"""
CAISO sub-scrape queue worker
─────────────────────────────
Runs as a separate Docker service alongside scraper.py.

Responsibilities:
  - Drain west_fin.scrape_queue sequentially (one subprocess at a time)
  - Recover stuck jobs (running > STUCK_THRESHOLD_MINUTES) at startup
    and every STUCK_CHECK_INTERVAL_SECONDS thereafter
  - Sleep between polls so an empty queue doesn't busy-spin

The fetcher (scraper.py) owns:
  - OASIS API calls
  - temp table → xref → fact inserts
  - Enqueuing new jobs into scrape_queue

This worker owns:
  - Everything after that (claiming, executing, marking done/failed/retry)
"""

import json
import time
import logging
import subprocess
import shlex
import psycopg2
import psycopg2.extras
from datetime import datetime

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("queue_worker")

# ── Config ─────────────────────────────────────────────────────────────────────
CREDENTIALS_FILE        = "/app/credentials.json"
CRED_SECTION            = "sandbox_su"
SUBPROCESS_TIMEOUT      = 300    # seconds before a sub-scrape is killed
POLL_INTERVAL_SECONDS   = 10     # how long to sleep when queue is empty
STUCK_THRESHOLD_MINUTES = 10     # jobs running longer than this are recovered
STUCK_CHECK_INTERVAL    = 300    # run stuck-job sweep every N seconds (5 min)


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


# ── Stuck-job recovery ─────────────────────────────────────────────────────────

def recover_stuck_jobs(conn) -> int:
    """
    Calls the DB-side recover_stuck_jobs() function.
    Returns the number of jobs reset.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT west_fin.recover_stuck_jobs(%s)",
                (STUCK_THRESHOLD_MINUTES,)
            )
            count = cur.fetchone()[0]
    if count:
        log.warning("Stuck-job recovery: reset %d job(s) back to pending", count)
    else:
        log.debug("Stuck-job check: no stuck jobs found")
    return count


# ── Queue operations ───────────────────────────────────────────────────────────

def claim_next_job(conn) -> dict | None:
    """
    Atomically claim the oldest pending job that has attempts remaining.
    Uses SELECT … FOR UPDATE SKIP LOCKED — safe when the fetcher and worker
    run as separate processes/containers.
    Returns the job dict (pre-increment attempt_count) or None.
    """
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM west_fin.scrape_queue
                WHERE status = 'pending'
                  AND attempt_count < max_attempts
                ORDER BY queued_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            row = cur.fetchone()
            if row is None:
                return None

            cur.execute("""
                UPDATE west_fin.scrape_queue
                SET status        = 'running',
                    started_at    = now(),
                    attempt_count = attempt_count + 1
                WHERE queue_id = %(queue_id)s
            """, {"queue_id": row["queue_id"]})

    return dict(row)


def mark_job_done(conn, queue_id: int) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE west_fin.scrape_queue
                SET status       = 'done',
                    completed_at = now()
                WHERE queue_id = %s
            """, (queue_id,))


def mark_job_failed_or_retry(conn, queue_id: int, error: str) -> None:
    """
    Exhausted attempts → 'failed'. Otherwise → back to 'pending' for retry.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE west_fin.scrape_queue
                SET status     = CASE
                                   WHEN attempt_count >= max_attempts THEN 'failed'
                                   ELSE 'pending'
                                 END,
                    last_error = %s,
                    started_at = NULL
                WHERE queue_id = %s
            """, (error[:2000], queue_id))


def get_queue_summary(conn) -> dict:
    """Returns a quick status count dict for logging."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT status, COUNT(*) AS n
            FROM west_fin.scrape_queue
            WHERE status IN ('pending', 'running', 'failed')
            GROUP BY status
        """)
        return {row[0]: row[1] for row in cur.fetchall()}


# ── Job execution ──────────────────────────────────────────────────────────────

def run_job(conn, job: dict) -> None:
    """Execute a single queue job as a subprocess, update its status."""
    qid     = job["queue_id"]
    cmd     = job["sys_string"]
    attempt = job["attempt_count"] + 1   # DB already incremented; this is for logging

    log.info("▶ queue_id=%-6d  attempt %d/%d  cmd: %s",
             qid, attempt, job["max_attempts"], cmd)

    try:
        result = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT,
        )

        if result.returncode == 0:
            mark_job_done(conn, qid)
            log.info("✔ queue_id=%-6d  done (exit 0)", qid)
        else:
            err = (
                f"exit={result.returncode} | "
                f"stderr={result.stderr.strip()[:500]}"
            )
            log.warning("✘ queue_id=%-6d  failed: %s", qid, err[:200])
            mark_job_failed_or_retry(conn, qid, err)

    except subprocess.TimeoutExpired:
        err = f"Timed out after {SUBPROCESS_TIMEOUT}s"
        log.warning("⏱ queue_id=%-6d  timed out", qid)
        mark_job_failed_or_retry(conn, qid, err)

    except Exception as exc:
        err = repr(exc)
        log.exception("💥 queue_id=%-6d  unexpected error: %s", qid, err)
        mark_job_failed_or_retry(conn, qid, err)


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    log.info("Queue worker starting")
    creds = load_credentials()

    last_stuck_check = 0.0   # force an immediate check on startup

    while True:
        try:
            conn = get_connection(creds)
            try:
                # ── Stuck-job recovery (periodic) ──────────────────────────────
                now = time.time()
                if now - last_stuck_check >= STUCK_CHECK_INTERVAL:
                    recover_stuck_jobs(conn)
                    last_stuck_check = now

                # ── Drain all pending jobs ─────────────────────────────────────
                drained = 0
                while True:
                    job = claim_next_job(conn)
                    if job is None:
                        break
                    run_job(conn, job)
                    drained += 1

                if drained:
                    summary = get_queue_summary(conn)
                    log.info(
                        "Drain complete — %d job(s) processed | queue: %s",
                        drained,
                        " | ".join(f"{k}={v}" for k, v in summary.items()) or "all clear"
                    )

            finally:
                conn.close()

        except psycopg2.OperationalError as exc:
            # DB connection dropped — log and retry after poll interval
            log.error("DB connection error: %s — retrying in %ds",
                      exc, POLL_INTERVAL_SECONDS)

        except Exception as exc:
            log.exception("Unhandled error in worker loop: %s", exc)

        # ── Poll interval — sleep between empty-queue checks ───────────────────
        log.debug("Sleeping %ds before next poll", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
