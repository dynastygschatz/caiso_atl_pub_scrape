-- ─────────────────────────────────────────────────────────────────────────────
-- west_fin.scrape_queue
-- Durable job queue for sub-scrapes triggered by caiso_atl_pub_xref sys_string
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS west_fin.scrape_queue (
    queue_id        bigserial       PRIMARY KEY,

    -- The shell command to execute
    sys_string      text            NOT NULL,

    -- Source identifiers (for debugging / deduplication)
    report_name     text            NOT NULL,
    market_run_id   text            NOT NULL,
    opr_dt          date            NULL,
    opr_hr          int4            NULL,
    opr_interval    int4            NULL,

    -- The posted_at value from caiso_atl_pub that generated this job
    source_posted_at timestamp      NOT NULL,

    -- Lifecycle
    status          text            NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending', 'running', 'done', 'failed')),
    attempt_count   int4            NOT NULL DEFAULT 0,
    max_attempts    int4            NOT NULL DEFAULT 3,

    -- Timestamps
    queued_at       timestamp       NOT NULL DEFAULT now(),
    started_at      timestamp       NULL,
    completed_at    timestamp       NULL,
    last_error      text            NULL
);

-- Partial index: fast scan for work to do
CREATE INDEX IF NOT EXISTS idx_scrape_queue_pending
    ON west_fin.scrape_queue (queued_at)
    WHERE status = 'pending';

-- Deduplication: don't enqueue the same logical job twice for the same
-- source_posted_at batch if it's still pending or running
CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_queue_dedup
    ON west_fin.scrape_queue (sys_string, source_posted_at)
    WHERE status IN ('pending', 'running');

COMMENT ON TABLE west_fin.scrape_queue IS
    'Durable FIFO queue of sub-scrape jobs derived from caiso_atl_pub_xref.sys_string. '
    'Jobs are claimed with SELECT … FOR UPDATE SKIP LOCKED, run sequentially as subprocesses, '
    'retried up to max_attempts on failure, then marked done or failed.';
