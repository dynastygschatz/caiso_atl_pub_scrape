-- ─────────────────────────────────────────────────────────────────────────────
-- Stuck-job recovery
-- Jobs stuck in 'running' beyond STUCK_THRESHOLD are reset to 'pending'
-- so the worker picks them up again on the next cycle.
--
-- Two options are provided:
--   A) pg_cron (if available on your RDS instance) — runs automatically
--   B) Manual / Python-side call — use if pg_cron is not available
-- ─────────────────────────────────────────────────────────────────────────────


-- ── Recovery function ─────────────────────────────────────────────────────────
-- Resets jobs that have been 'running' longer than p_stuck_threshold_minutes.
-- Jobs that have hit max_attempts are marked 'failed' instead of 'pending'.
-- Returns the number of rows recovered.

CREATE OR REPLACE FUNCTION west_fin.recover_stuck_jobs(
    p_stuck_threshold_minutes int DEFAULT 10
)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_recovered int;
BEGIN
    WITH recovered AS (
        UPDATE west_fin.scrape_queue
        SET
            status     = CASE
                             WHEN attempt_count >= max_attempts THEN 'failed'
                             ELSE 'pending'
                         END,
            started_at = NULL,
            last_error = format(
                'Recovered by stuck-job sweep at %s after %s min threshold '
                '(was running since %s, attempt %s/%s). Prior error: %s',
                now(),
                p_stuck_threshold_minutes,
                started_at,
                attempt_count,
                max_attempts,
                COALESCE(last_error, 'none')
            )
        WHERE status = 'running'
          AND started_at < now() - (p_stuck_threshold_minutes || ' minutes')::interval
        RETURNING queue_id
    )
    SELECT COUNT(*) INTO v_recovered FROM recovered;

    IF v_recovered > 0 THEN
        RAISE NOTICE 'recover_stuck_jobs: reset % job(s) stuck > % minutes',
            v_recovered, p_stuck_threshold_minutes;
    END IF;

    RETURN v_recovered;
END;
$$;

COMMENT ON FUNCTION west_fin.recover_stuck_jobs IS
    'Resets "running" jobs that have been in-flight longer than the threshold '
    'back to "pending" (or "failed" if attempts are exhausted). '
    'Safe to call repeatedly — idempotent. '
    'Default threshold: 10 minutes. '
    'Usage: SELECT west_fin.recover_stuck_jobs();  -- uses default '
    '        SELECT west_fin.recover_stuck_jobs(15); -- 15-minute threshold';


-- ── Option A: pg_cron schedule (RDS / Aurora with pg_cron extension) ──────────
-- Run every 5 minutes. Uncomment if pg_cron is enabled on your instance.
--
-- SELECT cron.schedule(
--     'recover-stuck-scrape-jobs',          -- job name (must be unique)
--     '*/5 * * * *',                        -- every 5 minutes
--     $$SELECT west_fin.recover_stuck_jobs(10)$$
-- );
--
-- To remove the schedule:
-- SELECT cron.unschedule('recover-stuck-scrape-jobs');
--
-- To verify:
-- SELECT * FROM cron.job WHERE jobname = 'recover-stuck-scrape-jobs';


-- ── Option B: Verify the function works manually ──────────────────────────────
-- Run this to test before setting up the schedule:
--
-- SELECT west_fin.recover_stuck_jobs(10);
--
-- Or with a very short threshold to force recovery during testing:
-- SELECT west_fin.recover_stuck_jobs(0);
