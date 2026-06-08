-- ─────────────────────────────────────────────────────────────────────────────
-- 004 — Job priority for west_fin.scrape_queue
--
-- Adds a `priority` knob (1 = highest … 3 = lowest, default 3) that drives:
--   1. How many times a job is retried (priority → max_attempts)
--        P1 → 10 attempts, P2 → 5, P3 (or unset) → 3
--   2. The order in which pending jobs are claimed (priority 1 before 2 before 3,
--        FIFO within a priority)
--
-- Backoff between retries is exponential and lives in queue_worker.py
-- (rescrape_target_time), so this migration also ensures that column exists.
-- ─────────────────────────────────────────────────────────────────────────────


-- ── 1. priority column ──────────────────────────────────────────────────────
ALTER TABLE west_fin.scrape_queue
    ADD COLUMN IF NOT EXISTS priority int4 NOT NULL DEFAULT 3;

ALTER TABLE west_fin.scrape_queue
    DROP CONSTRAINT IF EXISTS scrape_queue_priority_chk;
ALTER TABLE west_fin.scrape_queue
    ADD CONSTRAINT scrape_queue_priority_chk CHECK (priority IN (1, 2, 3));

COMMENT ON COLUMN west_fin.scrape_queue.priority IS
    'Scrape priority: 1 = highest (claimed first, retried most), 3 = lowest. '
    'Defaults to 3 when unset. Drives max_attempts via trg_sync_max_attempts.';

-- Backstop: the worker schedules exponential re-queue times here.
ALTER TABLE west_fin.scrape_queue
    ADD COLUMN IF NOT EXISTS rescrape_target_time timestamp NULL;


-- ── 2. priority → max retry attempts mapping ─────────────────────────────────
CREATE OR REPLACE FUNCTION west_fin.priority_to_max_attempts(p_priority int)
RETURNS int
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE COALESCE(p_priority, 3)
               WHEN 1 THEN 10
               WHEN 2 THEN 5
               ELSE      3        -- priority 3 (or anything unexpected) → default
           END;
$$;

COMMENT ON FUNCTION west_fin.priority_to_max_attempts IS
    'Maps scrape_queue.priority to the retry budget (max_attempts): '
    'P1=10, P2=5, P3/unset=3.';


-- ── 3. Keep max_attempts in sync with priority ───────────────────────────────
-- priority is the single source of truth for the retry budget. This fires only
-- when a row is inserted or when priority itself is updated, so the worker's
-- frequent status/attempt_count updates are unaffected.
CREATE OR REPLACE FUNCTION west_fin.sync_max_attempts()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.priority     := COALESCE(NEW.priority, 3);
    NEW.max_attempts := west_fin.priority_to_max_attempts(NEW.priority);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_max_attempts ON west_fin.scrape_queue;
CREATE TRIGGER trg_sync_max_attempts
    BEFORE INSERT OR UPDATE OF priority
    ON west_fin.scrape_queue
    FOR EACH ROW
    EXECUTE FUNCTION west_fin.sync_max_attempts();

-- Backfill existing rows so their retry budget matches their (default) priority.
UPDATE west_fin.scrape_queue
SET max_attempts = west_fin.priority_to_max_attempts(priority);


-- ── 4. Priority-aware claim index ────────────────────────────────────────────
-- Replaces the FIFO-only pending index so the worker can claim
-- ORDER BY priority, queued_at efficiently.
DROP INDEX IF EXISTS west_fin.idx_scrape_queue_pending;
CREATE INDEX IF NOT EXISTS idx_scrape_queue_pending
    ON west_fin.scrape_queue (priority, queued_at)
    WHERE status = 'pending';


-- ── 5. Surface priority in the monitoring views ──────────────────────────────
CREATE OR REPLACE VIEW west_fin.v_scrape_queue_stuck AS
SELECT
    queue_id,
    priority,
    report_name,
    market_run_id,
    opr_dt,
    opr_hr,
    sys_string,
    attempt_count,
    max_attempts,
    started_at,
    EXTRACT(EPOCH FROM (now() - started_at)) / 60  AS running_minutes,
    last_error
FROM west_fin.scrape_queue
WHERE status = 'running'
  AND started_at < now() - INTERVAL '10 minutes'
ORDER BY started_at;

CREATE OR REPLACE VIEW west_fin.v_scrape_queue_failed AS
SELECT
    queue_id,
    priority,
    report_name,
    market_run_id,
    opr_dt,
    opr_hr,
    opr_interval,
    sys_string,
    attempt_count,
    max_attempts,
    queued_at,
    last_error,
    completed_at
FROM west_fin.scrape_queue
WHERE status = 'failed'
ORDER BY priority, queued_at DESC;
