"""
CAISO ATL_PUB fetcher
──────────────────────
Runs as a Docker service. Every 60 seconds:
  1. Query caiso_atl_pub for the last publication date → derive start/end window
  2. Fetch ATL_PUB from OASIS API (ZIP → CSV)
  3. Load rows into a temp table
  4. Upsert west_fin.caiso_atl_pub_xref
  5. Insert into west_fin.caiso_atl_pub
  6. Enqueue sub-scrape jobs into west_fin.scrape_queue (non-null sys_string rows)
  7. Sleep the remainder of the 60-second window

Queue draining is handled by queue_worker.py (a separate container).
"""

import json
import time
import logging
import zipfile
import io
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
import pytz
import csv

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("fetcher")

# ── Config ─────────────────────────────────────────────────────────────────────
CREDENTIALS_FILE = "/app/credentials.json"
CRED_SECTION     = "sandbox_su"
PST              = pytz.timezone("America/Los_Angeles")
MIN_LOOP_SECONDS = 60
OASIS_URL        = "http://oasis.caiso.com/oasisapi/SingleZip"
DEFAULT_PRIORITY = 3   # scrape_queue priority for enqueued jobs (1=high … 3=low).
                       # priority drives the retry budget (max_attempts) via a
                       # DB trigger: P1=10, P2=5, P3=3.


# ── Credentials / connection ───────────────────────────────────────────────────

def load_credentials() -> dict:
    with open(CREDENTIALS_FILE) as f:
        creds = json.load(f)
    return creds[CRED_SECTION]


def get_connection(creds: dict):
    return psycopg2.connect(
        host=creds["server"],
        dbname=creds["database"],
        port=int(creds["port"]),
        user=creds["user"],
        password=creds["password"],
    )


# ── Date-range helpers ─────────────────────────────────────────────────────────

def get_date_range(conn) -> tuple[str, str]:
    """
    lastpub  = max(publication_date) - 60 min  (yesterday if table empty)
    start    = lastpub - 1 h,  formatted PST8PDT
    end      = tomorrow midnight, formatted PST8PDT
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(
                MAX(publication_date) - INTERVAL '60 minutes',
                CURRENT_DATE - INTERVAL '1 day'
            ) AS last_pub
            FROM west_fin.caiso_atl_pub
        """)
        last_pub = cur.fetchone()[0]

    if not isinstance(last_pub, datetime):
        last_pub = datetime(last_pub.year, last_pub.month, last_pub.day)

    start_dt = last_pub - timedelta(hours=1)
    end_dt   = (
        datetime.now(tz=timezone.utc)
        .astimezone(PST)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=1)
    )

    def force_pst(dt):
        return PST.localize(dt) if dt.tzinfo is None else dt.astimezone(PST)

    fmt = "%Y%m%dT%H:%M%z"
    return force_pst(start_dt).strftime(fmt), force_pst(end_dt).strftime(fmt)


# ── OASIS fetch ────────────────────────────────────────────────────────────────

def fetch_oasis_data(startdatetime: str, enddatetime: str):
    """Returns (rows: list[dict], ts1: float)."""
    params = {
        "queryname":     "ATL_PUB",
        "resultformat":  "6",
        "oasis_section": "ALL",
        "status":        "ALL",
        "atlpubversion": "ALL",
        "startdatetime": startdatetime,
        "enddatetime":   enddatetime,
        "version":       "1",
    }
    log.info("Fetching OASIS | start=%s end=%s", startdatetime, enddatetime)
    ts1  = time.time()
    resp = requests.get(OASIS_URL, params=params, timeout=120)
    resp.raise_for_status()
    log.info("API responded in %.1f s | content-type: %s",
             time.time() - ts1, resp.headers.get("Content-Type", ""))

    rows = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            log.info("Parsing: %s", name)
            with zf.open(name) as f:
                for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
                    rows.append(row)

    log.info("Fetched %d rows", len(rows))
    return rows, ts1


# ── Row normalisation ──────────────────────────────────────────────────────────

def normalize_row(raw: dict, posted_at: datetime) -> dict:
    def to_int(v):
        try:
            return int(v) if v not in (None, "", "NULL") else None
        except (ValueError, TypeError):
            return None

    def to_ts(v):
        if not v or v in ("NULL", ""):
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                pass
        return None

    def to_date(v):
        if not v or v in ("NULL", ""):
            return None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                pass
        return None

    r = {k.lower(): v for k, v in raw.items()}
    return {
        "report_name":      r.get("report_name")    or r.get("reportname"),
        "oasis_section":    r.get("oasis_section")  or r.get("oasissection"),
        "market_run_id":    r.get("market_run_id")  or r.get("marketrunid"),
        "publication_id":   to_int(r.get("publication_id") or r.get("publicationid")),
        "status":           r.get("status"),
        "publication_date": to_ts(r.get("publication_date") or r.get("publicationdate")),
        "opr_dt":           to_date(r.get("opr_dt")  or r.get("oprdt")),
        "opr_hr":           to_int(r.get("opr_hr")   or r.get("oprhr")),
        "opr_interval":     to_int(r.get("opr_interval") or r.get("oprinterval")),
        "atlpubversion":    r.get("atlpubversion"),
        "posted_at":        posted_at,
    }


# ── Core upsert + queue population ────────────────────────────────────────────

def upsert_data(conn, rows: list[dict]) -> datetime:
    """
    Temp table → xref upsert → fact insert → enqueue sub-scrapes.
    Returns the posted_at timestamp used for this batch.
    """
    posted_at  = datetime.utcnow()
    normalized = [normalize_row(r, posted_at) for r in rows]

    with conn:
        with conn.cursor() as cur:

            # ── 1. Temp table ──────────────────────────────────────────────────
            cur.execute("""
                CREATE TEMP TABLE _tmp_atlpub_data (
                    report_name      text        NOT NULL,
                    oasis_section    text        NOT NULL,
                    market_run_id    text        NOT NULL,
                    publication_id   int4        NOT NULL,
                    status           text        NULL,
                    publication_date timestamp   NULL,
                    opr_dt           date        NULL,
                    opr_hr           int4        NULL,
                    opr_interval     int4        NULL,
                    atlpubversion    varchar     NULL,
                    posted_at        timestamp   NULL,
                    CONSTRAINT _tmp_atlpub_data_pkey
                        PRIMARY KEY (report_name, status, oasis_section,
                                     market_run_id, atlpubversion, publication_id)
                ) ON COMMIT DROP;
            """)

            # ── 2. Bulk insert into temp ───────────────────────────────────────
            cols = [
                "report_name","oasis_section","market_run_id","publication_id",
                "status","publication_date","opr_dt","opr_hr","opr_interval",
                "atlpubversion","posted_at",
            ]
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO _tmp_atlpub_data ({', '.join(cols)}) VALUES %s "
                "ON CONFLICT DO NOTHING",
                [[r[c] for c in cols] for r in normalized],
            )
            log.info("Loaded %d rows into temp table", cur.rowcount)

            # ── 3. Xref upsert ─────────────────────────────────────────────────
            cur.execute("""
                INSERT INTO west_fin.caiso_atl_pub_xref
                    (report_name, status, oasis_section, market_run_id, atlpubversion)
                SELECT DISTINCT
                    t.report_name, t.status, t.oasis_section,
                    t.market_run_id, t.atlpubversion
                FROM _tmp_atlpub_data t
                LEFT JOIN west_fin.caiso_atl_pub_xref apx
                    ON  apx.report_name   = t.report_name
                    AND apx.oasis_section = t.oasis_section
                    AND apx.market_run_id = t.market_run_id
                    AND apx.atlpubversion = t.atlpubversion
                    AND apx.status        = t.status
                WHERE apx.atl_pub_xref IS NULL
                ON CONFLICT (report_name, status, oasis_section, market_run_id, atlpubversion)
                DO NOTHING;
            """)
            log.info("Xref upsert: %d rows", cur.rowcount)

            # ── 4. Fact insert ─────────────────────────────────────────────────
            cur.execute("""
                INSERT INTO west_fin.caiso_atl_pub
                    (publication_id, atl_pub_xref, publication_date,
                     opr_dt, opr_hr, opr_interval, posted_at)
                SELECT
                    t.publication_id,
                    x.atl_pub_xref,
                    t.publication_date,
                    t.opr_dt,
                    t.opr_hr,
                    t.opr_interval,
                    t.posted_at
                FROM _tmp_atlpub_data t
                JOIN west_fin.caiso_atl_pub_xref x
                    ON  t.report_name   = x.report_name
                    AND t.status        = x.status
                    AND t.oasis_section = x.oasis_section
                    AND t.market_run_id = x.market_run_id
                    AND t.atlpubversion = x.atlpubversion
                ON CONFLICT (publication_id) DO NOTHING;
            """)
            log.info("Fact insert: %d rows", cur.rowcount)

            # ── 5. Enqueue sub-scrapes ─────────────────────────────────────────
            cur.execute("""
                INSERT INTO west_fin.scrape_queue
                    (sys_string, report_name, market_run_id,
                     opr_dt, opr_hr, opr_interval, source_posted_at,
                     status, attempt_count, priority)
                SELECT
                    a.sys_string,
                    a.report_name,
                    a.market_run_id,
                    a.opr_dt,
                    a.opr_hr,
                    a.opr_interval,
                    a.source_posted_at,
                    'pending',
                    0,
                    %(priority)s
                FROM (
                    SELECT
                        x.report_name,
                        x.market_run_id,
                        d.opr_dt,
                        d.opr_hr,
                        d.opr_interval,
                        d.posted_at AS source_posted_at,
                        x.sys_string
                            || ' -m ' || x.market_run_id
                            || ' -d ' || d.opr_dt
                            || ' -r ' || d.opr_hr
                            || ' -i ' || d.opr_interval
                            || ' -v ' || x.atlpubversion  AS sys_string
                    FROM west_fin.caiso_atl_pub_xref x
                    JOIN west_fin.caiso_atl_pub d
                        ON x.atl_pub_xref = d.atl_pub_xref
                    WHERE d.posted_at = %(posted_at)s
                ) a
                WHERE a.sys_string IS NOT NULL
                ON CONFLICT (sys_string, source_posted_at)
                    WHERE status IN ('pending', 'running')
                DO NOTHING;
            """, {"posted_at": posted_at, "priority": DEFAULT_PRIORITY})
            log.info("Enqueued %d sub-scrape job(s)", cur.rowcount)

    return posted_at


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    log.info("CAISO ATL_PUB fetcher starting")
    creds = load_credentials()

    while True:
        loop_start = time.time()
        try:
            conn = get_connection(creds)
            try:
                startdatetime, enddatetime = get_date_range(conn)
                rows, _ts1 = fetch_oasis_data(startdatetime, enddatetime)

                if rows:
                    upsert_data(conn, rows)
                else:
                    log.info("No rows from API — nothing to persist or enqueue")

            finally:
                conn.close()

        except Exception as exc:
            log.exception("Unhandled error in fetcher loop: %s", exc)

        elapsed = time.time() - loop_start
        wait    = max(0.0, MIN_LOOP_SECONDS - elapsed)
        log.info("Cycle complete in %.1f s — sleeping %.1f s", elapsed, wait)
        time.sleep(wait)


if __name__ == "__main__":
    main()
