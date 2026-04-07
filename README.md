# CAISO ATL_PUB Scraper — Docker Service

Continuously scrapes the CAISO OASIS `ATL_PUB` endpoint and persists
publication data into `west_fin.caiso_atl_pub` / `west_fin.caiso_atl_pub_xref`
in your Postgres database. Each loop is throttled to a minimum of 60 seconds.

---

## Project layout

```
caiso-scraper/
├── scraper.py              # Main application
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── credentials.json.example
```

---

## First-time setup on your AWS Linux server

### 1. Copy files to the server

```bash
scp -r caiso-scraper/ ec2-user@<your-server>:/opt/caiso-scraper/
```

### 2. Create credentials.json on the server

```bash
sudo mkdir -p /opt/caiso-scraper
sudo cp /opt/caiso-scraper/credentials.json.example \
        /opt/caiso-scraper/credentials.json
sudo nano /opt/caiso-scraper/credentials.json   # fill in real values
sudo chmod 600 /opt/caiso-scraper/credentials.json
```

`credentials.json` structure:
```json
{
  "sandbox_su": {
    "server":   "your-postgres-host",
    "database": "your_database",
    "port":     5432,
    "user":     "your_user",
    "password": "your_password"
  }
}
```

### 3. Build and start

```bash
cd /opt/caiso-scraper
docker compose up -d --build
```

---

## Day-to-day operations

| Task | Command |
|------|---------|
| View live logs | `docker compose logs -f` |
| Stop | `docker compose down` |
| Restart | `docker compose restart` |
| Rebuild after code change | `docker compose up -d --build` |
| Check status | `docker compose ps` |

---

## How it works

1. **Date range** — Queries `west_fin.caiso_atl_pub` for the latest
   `publication_date`, subtracts 60 min, then backs off another hour for the
   start window. End window is midnight tomorrow (PST).

2. **API call** — GETs the OASIS SingleZip endpoint, unzips the response,
   parses the inner CSV.

3. **Temp table** — Loads all rows into a session-scoped temp table
   `_tmp_atlpub_data` (auto-dropped at commit).

4. **Xref upsert** — Inserts any new dimension combos into
   `west_fin.caiso_atl_pub_xref`.

5. **Fact insert** — Joins temp → xref and inserts new rows into
   `west_fin.caiso_atl_pub` (`ON CONFLICT DO NOTHING`).

6. **Throttle** — Sleeps so that total cycle time is never less than 60 s.

---

## Logs

Logs are written to stdout (JSON-file driver, 20 MB × 5 files):

```
2025-01-15T08:00:01 [INFO] CAISO ATL_PUB scraper starting
2025-01-15T08:00:01 [INFO] Fetching OASIS data | start=20250114T23:00-0800 end=20250116T00:00-0800
2025-01-15T08:00:09 [INFO] API responded in 7.8 s  |  content-type: application/zip
2025-01-15T08:00:09 [INFO] Fetched 1420 rows from API
2025-01-15T08:00:09 [INFO] Loaded 1420 rows into temp table
2025-01-15T08:00:09 [INFO] Xref upsert affected 3 rows
2025-01-15T08:00:09 [INFO] Primary table insert affected 1418 rows
2025-01-15T08:00:09 [INFO] Cycle complete in 9.2 s — sleeping 50.8 s
```
