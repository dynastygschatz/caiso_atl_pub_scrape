-- ─────────────────────────────────────────────────────────────────────────────
-- Queue health monitoring
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Summary view: one row per status with key metrics ─────────────────────
CREATE OR REPLACE VIEW west_fin.v_scrape_queue_health AS
SELECT
    status,
    COUNT(*)                                            AS job_count,
    MIN(queued_at)                                      AS oldest_queued_at,
    MAX(queued_at)                                      AS newest_queued_at,
    ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - started_at))))
                                                        AS avg_runtime_secs,
    MAX(EXTRACT(EPOCH FROM (completed_at - started_at)))
                                                        AS max_runtime_secs,
    SUM(attempt_count)                                  AS total_attempts,
    COUNT(*) FILTER (WHERE attempt_count > 1)           AS jobs_with_retries
FROM west_fin.scrape_queue
GROUP BY status
ORDER BY
    CASE status
        WHEN 'running' THEN 1
        WHEN 'pending' THEN 2
        WHEN 'failed'  THEN 3
        WHEN 'done'    THEN 4
    END;

COMMENT ON VIEW west_fin.v_scrape_queue_health IS
    'Per-status summary of scrape_queue. Check this for at-a-glance queue health.';


-- ── 2. Stuck-job view: running jobs older than 10 minutes ────────────────────
CREATE OR REPLACE VIEW west_fin.v_scrape_queue_stuck AS
SELECT
    queue_id,
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

COMMENT ON VIEW west_fin.v_scrape_queue_stuck IS
    'Running jobs that have been in-flight for more than 10 minutes. '
    'These are candidates for stuck-job recovery.';


-- ── 3. Failed-job view: permanently failed jobs with full error detail ────────
CREATE OR REPLACE VIEW west_fin.v_scrape_queue_failed AS
SELECT
    queue_id,
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
ORDER BY queued_at DESC;

COMMENT ON VIEW west_fin.v_scrape_queue_failed IS
    'All permanently failed jobs. Use queue_id to manually requeue if needed.';


-- ── 4. Helper: manually requeue a failed job ──────────────────────────────────
-- Usage: SELECT west_fin.requeue_job(queue_id);
CREATE OR REPLACE FUNCTION west_fin.requeue_job(p_queue_id bigint)
RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_status text;
BEGIN
    SELECT status INTO v_status
    FROM west_fin.scrape_queue
    WHERE queue_id = p_queue_id;

    IF NOT FOUND THEN
        RETURN format('ERROR: queue_id %s not found', p_queue_id);
    END IF;

    UPDATE west_fin.scrape_queue
    SET status        = 'pending',
        attempt_count = 0,
        last_error    = NULL,
        started_at    = NULL,
        completed_at  = NULL
    WHERE queue_id = p_queue_id;

    RETURN format('OK: queue_id %s reset from %s → pending', p_queue_id, v_status);
END;
$$;

COMMENT ON FUNCTION west_fin.requeue_job IS
    'Manually reset any job back to pending with zeroed attempt count. '
    'Usage: SELECT west_fin.requeue_job(123);';
