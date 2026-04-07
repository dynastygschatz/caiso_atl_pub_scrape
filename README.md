# CAISO ATL_PUB Scraper — Docker Service

Continuously scrapes the CAISO OASIS `ATL_PUB` endpoint, persists publication
data into Postgres, and dispatches sub-scrapes via a durable job queue. Runs
as two coordinated Docker containers from a single shared image.

---

## Project layout

```
caiso-scraper/
├── scraper.py                        # Fetcher service: OASIS API → Postgres → enqueue
├── queue_worker.py                   # Worker service: drain queue → subprocess dispatch
├── Dockerfile                        # Shared image for both services
├── docker-compose.yml                # Two-service stack definition
├── requirements.txt
├── credentials.json.example
└── migrations/
    ├── 001_create_scrape_queue.sql   # Queue table + dedup indexes
    ├── 002_queue_health_view.sql     # Monitoring views + requeue helper
    └── 003_stuck_job_recovery.sql    # Stuck-job recovery function
```

---

## Architecture

```
┌──────────────────────────────┐     ┌──────────────────────────────────┐
│      caiso-fetcher           │     │       caiso-queue-worker         │
│  (scraper.py, every 60 s)    │     │  (queue_worker.py, polls 10 s)   │
│                              │     │                                  │
│  1. Query last pub date      │     │  1. Recover stuck jobs (5 min)   │
│  2. Fetch OASIS API (zip)    │     │  2. Claim oldest pending job     │
│  3. Load temp table          │     │     (SELECT FOR UPDATE SKIP      │
│  4. Upsert xref              │     │      LOCKED — crash-safe)        │
│  5. Insert fact table        │     │  3. Run sys_string as subprocess │
│  6. Enqueue sub-scrapes ─────┼────▶│  4. Mark done / retry / failed  │
│  7. Sleep remainder of 60 s  │     │  5. Loop until queue empty       │
└──────────────────────────────┘     └──────────────────────────────────┘
                │                                     │
                └──────────────┬──────────────────────┘
                               ▼
                     west_fin schema (Postgres)
                     ├── caiso_atl_pub
                     ├── caiso_atl_pub_xref
                     ├── scrape_queue
                     ├── v_scrape_queue_health
                     ├── v_scrape_queue_stuck
                     ├── v_scrape_queue_failed
                     └── recover_stuck_jobs()
```

The two containers share one image — `docker-compose.yml` overrides the
`command:` per service. The queue is deduplication-safe: if the fetcher fires
again before the worker drains, duplicate jobs are silently dropped by a
partial unique index on `(sys_string, source_posted_at)`.

---

## First-time setup

### 1. Run DB migrations

```bash
psql -h <host> -U <user> -d <database> \
    -f migrations/001_create_scrape_queue.sql \
    -f migrations/002_queue_health_view.sql \
    -f migrations/003_stuck_job_recovery.sql
```

### 2. Copy files to the server

```bash
scp -r caiso-scraper/ ec2-user@<your-server>:/opt/caiso-scraper/
```

### 3. Create credentials.json on the server

```bash
sudo cp /opt/caiso-scraper/credentials.json.example \
        /opt/caiso-scraper/credentials.json
sudo nano /opt/caiso-scraper/credentials.json   # fill in real values
sudo chmod 600 /opt/caiso-scraper/credentials.json
```

```json
{
  "sandbox_su": {
    "server":   "your-postgres-host.rds.amazonaws.com",
    "database": "your_database",
    "port":     5432,
    "user":     "your_user",
    "password": "your_password"
  }
}
```

### 4. Build and start both services

```bash
cd /opt/caiso-scraper
docker compose up -d --build
```

---

## Day-to-day operations

| Task | Command |
|------|---------|
| Status of both services | `docker compose ps` |
| All logs (both services) | `docker compose logs -f` |
| Fetcher logs only | `docker compose logs -f caiso-fetcher` |
| Worker logs only | `docker compose logs -f caiso-queue-worker` |
| Stop everything | `docker compose down` |
| Restart one service | `docker compose restart caiso-queue-worker` |
| Rebuild after code change | `docker compose up -d --build` |

---

## Queue monitoring

Connect to Postgres and run:

```sql
-- Overall health: counts, avg runtime, retry rates
SELECT * FROM west_fin.v_scrape_queue_health;

-- Jobs stuck in 'running' for more than 10 minutes
SELECT * FROM west_fin.v_scrape_queue_stuck;

-- All permanently failed jobs with error detail
SELECT * FROM west_fin.v_scrape_queue_failed;

-- Manually reset a specific job back to pending (zeroes attempt count)
SELECT west_fin.requeue_job(123);

-- Manually trigger stuck-job recovery (worker does this automatically)
SELECT west_fin.recover_stuck_jobs();       -- default 10-minute threshold
SELECT west_fin.recover_stuck_jobs(5);     -- stricter 5-minute threshold
```

---

## Stuck-job recovery

The worker runs `recover_stuck_jobs()` automatically every 5 minutes. Any job
that has been `running` for more than 10 minutes is reset to `pending` for
retry (or permanently `failed` if it has hit `max_attempts = 3`). The
`last_error` column records exactly when and why the reset happened.

If your RDS instance has `pg_cron` enabled, `003_stuck_job_recovery.sql`
contains a commented-out `cron.schedule()` call that adds a DB-side sweep as
a second layer of protection.

---

## How the fetcher works (scraper.py)

1. **Date range** — Queries `caiso_atl_pub` for the latest `publication_date`,
   subtracts 60 min, backs off another hour for the start window. End window
   is midnight tomorrow (PST / America/Los_Angeles).
2. **API call** — GETs the OASIS SingleZip endpoint, unzips, parses the CSV.
3. **Temp table** — Session-scoped `_tmp_atlpub_data`, auto-dropped at commit.
4. **Xref upsert** — New dimension combos inserted into `caiso_atl_pub_xref`.
5. **Fact insert** — New rows inserted into `caiso_atl_pub` (`ON CONFLICT DO NOTHING`).
6. **Enqueue** — For each row with a non-null `sys_string`, builds the full
   command (`sys_string -m … -d … -r … -i … -v …`) and inserts into
   `scrape_queue`. Duplicates for still-active jobs are silently skipped.
7. **Throttle** — Sleeps so total cycle time is never less than 60 seconds.

## How the worker works (queue_worker.py)

1. **Stuck-job sweep** — Calls `recover_stuck_jobs()` every 5 minutes.
2. **Claim** — `SELECT … FOR UPDATE SKIP LOCKED` atomically grabs the oldest
   pending job and flips it to `running`. Safe across container restarts.
3. **Dispatch** — Runs `sys_string` as a shell subprocess with a 5-minute
   timeout.
4. **Result** — Exit 0 → `done`. Non-zero / timeout → `pending` for retry, or
   `failed` after 3 attempts. Full error saved in `last_error`.
5. **Poll** — Sleeps 10 seconds between empty-queue checks.

---

## Logs

Both services write to stdout (JSON-file driver, 20 MB × 5 files per container).

**Fetcher:**
```
2025-01-15T08:00:01 [INFO] CAISO ATL_PUB fetcher starting
2025-01-15T08:00:01 [INFO] Fetching OASIS | start=20250114T23:00-0800 end=20250116T00:00-0800
2025-01-15T08:00:09 [INFO] API responded in 7.8 s | content-type: application/zip
2025-01-15T08:00:09 [INFO] Fetched 1420 rows
2025-01-15T08:00:09 [INFO] Loaded 1420 rows into temp table
2025-01-15T08:00:09 [INFO] Xref upsert: 3 rows
2025-01-15T08:00:09 [INFO] Fact insert: 1418 rows
2025-01-15T08:00:09 [INFO] Enqueued 12 sub-scrape job(s)
2025-01-15T08:00:09 [INFO] Cycle complete in 9.2 s — sleeping 50.8 s
```

**Worker:**
```
2025-01-15T08:00:10 [INFO] Queue worker starting
2025-01-15T08:00:10 [WARNING] Stuck-job recovery: reset 0 job(s) back to pending
2025-01-15T08:00:10 [INFO] ▶ queue_id=42      attempt 1/3  cmd: /usr/local/bin/my-scraper -m DAM ...
2025-01-15T08:00:14 [INFO] ✔ queue_id=42      done (exit 0)
2025-01-15T08:00:14 [INFO] ▶ queue_id=43      attempt 1/3  cmd: /usr/local/bin/my-scraper -m RTM ...
2025-01-15T08:00:31 [WARNING] ✘ queue_id=43   failed: exit=1 | stderr=Connection refused
2025-01-15T08:00:31 [INFO] Drain complete — 2 job(s) processed | queue: pending=10 | failed=1
```
