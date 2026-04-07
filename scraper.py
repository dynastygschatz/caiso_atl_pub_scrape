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
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
CREDENTIALS_FILE = "/app/credentials.json"
CRED_SECTION = "sandbox_su"
PST = pytz.timezone("America/Los_Angeles")   # PST8PDT equivalent
MIN_LOOP_SECONDS = 60
OASIS_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_credentials() -> dict:
    with open(CREDENTIALS_FILE) as f:
        creds = json.load(f)
    return creds[CRED_SECTION]


def get_connection(creds: dict):
    return psycopg2.connect(
        host=creds["server"],
        dbname=creds["database"],
        port=creds["port"],
        user=creds["user"],
        password=creds["password"],
    )


def get_date_range(conn) -> tuple[str, str]:
    """
    Mirrors the R logic:
      lastpub  = max(publication_date) - 60 min  (or yesterday if table empty)
      start    = lastpub - 1 hour,  formatted in PST8PDT
      end      = tomorrow midnight, formatted in PST8PDT
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(
                MAX(publication_date) - INTERVAL '60 minutes',
                CURRENT_DATE - INTERVAL '1 day'
            ) AS last_pub
            FROM west_fin.caiso_atl_pub
            """
        )
        row = cur.fetchone()
        last_pub = row[0]  # datetime or date

    # Ensure it's a datetime
    if not isinstance(last_pub, datetime):
        last_pub = datetime(last_pub.year, last_pub.month, last_pub.day)

    start_dt = last_pub - timedelta(hours=1)
    end_dt   = datetime.now(tz=timezone.utc).astimezone(PST).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)

    # Force to PST timezone then format as YYYYMMDDThh:mm±hhmm
    if start_dt.tzinfo is None:
        start_dt = PST.localize(start_dt)
    else:
        start_dt = start_dt.astimezone(PST)

    if end_dt.tzinfo is None:
        end_dt = PST.localize(end_dt)
    else:
        end_dt = end_dt.astimezone(PST)

    fmt = "%Y%m%dT%H:%M%z"
    return start_dt.strftime(fmt), end_dt.strftime(fmt)


def fetch_oasis_data(startdatetime: str, enddatetime: str) -> list[dict]:
    """
    Hit the CAISO OASIS endpoint, unzip the response, parse the CSV.
    Returns a list of row dicts.
    """
    params = {
        "queryname":    "ATL_PUB",
        "resultformat": "6",
        "oasis_section":"ALL",
        "status":       "ALL",
        "atlpubversion":"ALL",
        "startdatetime": startdatetime,
        "enddatetime":   enddatetime,
        "version":       "1",
    }

    log.info("Fetching OASIS data | start=%s end=%s", startdatetime, enddatetime)
    ts1 = time.time()

    resp = requests.get(OASIS_URL, params=params, timeout=120)
    resp.raise_for_status()

    log.info("API responded in %.1f s  |  content-type: %s",
             time.time() - ts1, resp.headers.get("Content-Type", ""))

    # Unzip and read the inner CSV
    rows = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            log.info("Parsing file: %s", name)
            with zf.open(name) as csvfile:
                reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf-8"))
                for row in reader:
                    rows.append(row)

    log.info("Fetched %d rows from API", len(rows))
    return rows, ts1


def normalize_row(raw: dict, posted_at: datetime) -> dict:
    """Map raw CSV columns → DB columns, coercing types."""
    def to_int(v):
        try:
            return int(v) if v not in (None, "", "NULL") else None
        except ValueError:
            return None

    def to_ts(v):
        if not v or v in ("NULL", ""):
            return None
        # OASIS timestamps are typically ISO-8601 or MM/DD/YYYY HH:MM
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                continue
        return None

    def to_date(v):
        if not v or v in ("NULL", ""):
            return None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue
        return None

    # Column names in the CSV may be upper-cased
    r = {k.lower(): v for k, v in raw.items()}

    return {
        "report_name":    r.get("report_name") or r.get("reportname"),
        "oasis_section":  r.get("oasis_section") or r.get("oasissection"),
        "market_run_id":  r.get("market_run_id") or r.get("marketrunid"),
        "publication_id": to_int(r.get("publication_id") or r.get("publicationid")),
        "status":         r.get("status"),
        "publication_date": to_ts(r.get("publication_date") or r.get("publicationdate")),
        "opr_dt":         to_date(r.get("opr_dt") or r.get("oprdt")),
        "opr_hr":         to_int(r.get("opr_hr") or r.get("oprhr")),
        "opr_interval":   to_int(r.get("opr_interval") or r.get("oprinterval")),
        "atlpubversion":  r.get("atlpubversion"),
        "posted_at":      posted_at,
    }


def upsert_data(conn, rows: list[dict]) -> None:
    posted_at = datetime.utcnow()
    normalized = [normalize_row(r, posted_at) for r in rows]

    with conn:
        with conn.cursor() as cur:

            # ── 1. Temp table ─────────────────────────────────────────────────
            cur.execute("""
                CREATE TEMP TABLE _tmp_atlpub_data (
                    report_name     text        NOT NULL,
                    oasis_section   text        NOT NULL,
                    market_run_id   text        NOT NULL,
                    publication_id  int4        NOT NULL,
                    status          text        NULL,
                    publication_date timestamp  NULL,
                    opr_dt          date        NULL,
                    opr_hr          int4        NULL,
                    opr_interval    int4        NULL,
                    atlpubversion   varchar     NULL,
                    posted_at       timestamp   NULL,
                    CONSTRAINT _tmp_atlpub_data_pkey
                        PRIMARY KEY (report_name, status, oasis_section,
                                     market_run_id, atlpubversion, publication_id)
                ) ON COMMIT DROP;
            """)

            # ── 2. Bulk-insert into temp table ────────────────────────────────
            cols = [
                "report_name","oasis_section","market_run_id","publication_id",
                "status","publication_date","opr_dt","opr_hr","opr_interval",
                "atlpubversion","posted_at",
            ]
            psycopg2.extras.execute_values(
                cur,
                f"""
                INSERT INTO _tmp_atlpub_data ({', '.join(cols)})
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                [[r[c] for c in cols] for r in normalized],
            )
            log.info("Loaded %d rows into temp table", cur.rowcount)

            # ── 3. Upsert xref ────────────────────────────────────────────────
            cur.execute("""
                INSERT INTO west_fin.caiso_atl_pub_xref
                    (report_name, status, oasis_section, market_run_id, atlpubversion)
                SELECT DISTINCT
                    t.report_name, t.status, t.oasis_section,
                    t.market_run_id, t.atlpubversion
                FROM _tmp_atlpub_data t
                LEFT JOIN west_fin.caiso_atl_pub_xref apx
                    ON  apx.report_name    = t.report_name
                    AND apx.oasis_section  = t.oasis_section
                    AND apx.market_run_id  = t.market_run_id
                    AND apx.atlpubversion  = t.atlpubversion
                    AND apx.status         = t.status
                WHERE apx.atl_pub_xref IS NULL
                ON CONFLICT (report_name, status, oasis_section, market_run_id, atlpubversion)
                DO NOTHING;
            """)
            log.info("Xref upsert affected %d rows", cur.rowcount)

            # ── 4. Insert primary table ───────────────────────────────────────
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
                    ON  t.report_name    = x.report_name
                    AND t.status         = x.status
                    AND t.oasis_section  = x.oasis_section
                    AND t.market_run_id  = x.market_run_id
                    AND t.atlpubversion  = x.atlpubversion
                ON CONFLICT (publication_id)
                DO NOTHING;
            """)
            log.info("Primary table insert affected %d rows", cur.rowcount)


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    log.info("CAISO ATL_PUB scraper starting")
    creds = load_credentials()

    while True:
        loop_start = time.time()
        try:
            conn = get_connection(creds)
            try:
                startdatetime, enddatetime = get_date_range(conn)
                rows, ts1 = fetch_oasis_data(startdatetime, enddatetime)

                if rows:
                    upsert_data(conn, rows)
                else:
                    log.info("No rows returned from API — nothing to insert")

            finally:
                conn.close()

        except Exception as exc:
            log.exception("Error during scrape cycle: %s", exc)

        # ── Enforce 60-second minimum between API hits ─────────────────────
        elapsed = time.time() - loop_start
        wait    = max(0, MIN_LOOP_SECONDS - elapsed)
        log.info("Cycle complete in %.1f s — sleeping %.1f s", elapsed, wait)
        time.sleep(wait)


if __name__ == "__main__":
    main()
