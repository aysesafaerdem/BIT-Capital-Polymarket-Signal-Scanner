# PostgreSQL / Supabase Migration Guide (Step-by-Step)

This project currently runs on SQLite. Use this guide to move storage to Supabase Postgres while keeping the app running during migration.

## 1. Create Supabase project
1. Create a new Supabase project.
2. Copy the Postgres connection string from `Project Settings -> Database`.
3. Save it as `DATABASE_URL` in your `.env`.

Example:
```bash
DATABASE_URL=postgresql://postgres:<password>@db.<project>.supabase.co:5432/postgres
```

## 2. Install dependencies
Add Postgres driver and install requirements:
```bash
pip install -r requirements.txt
```

## 3. Initialize Postgres schema
Run either option:

Option A (Python helper):
```bash
python -m database.db_postgres
```

Option B (Supabase SQL Editor):
- Open `/database/schema_postgres.sql`
- Paste and run in Supabase SQL Editor

## 4. Migrate existing SQLite data
Make sure your local SQLite DB exists (default `data/polymarket_scanner.db`), then run:
```bash
export DB_PATH=data/polymarket_scanner.db
export DATABASE_URL=postgresql://...
python scripts/migrate_sqlite_to_postgres.py
```

What gets migrated:
- `markets` -> `markets` + `market_snapshots`
- `signals` -> `market_signals` + join tables + holding impacts
- `reports` -> `reports` + `report_items` from `top_signals`
- `portfolio_config` -> `portfolio_holdings` + themes
- `scheduler_log` -> `job_runs`

## 5. Verify data in Postgres
Run checks in Supabase SQL Editor:
```sql
select count(*) from markets;
select count(*) from market_snapshots;
select count(*) from market_signals where is_latest = true;
select count(*) from reports;
select count(*) from portfolio_holdings;
```

## 6. Start incremental app migration (recommended)
Do not swap every query at once.

Recommended order:
1. Read-only endpoints (`/api/stats`, `/api/earnings`, `/api/reports`)
2. Ingestion writes (`markets`, `market_snapshots`)
3. Analysis writes (`market_signals`, join tables)
4. Report generation writes (`reports`, `report_items`)
5. Scheduler log writes (`job_runs`)

## 7. Add a DB backend toggle (optional)
Use env var:
```bash
DB_BACKEND=sqlite   # local default
DB_BACKEND=postgres # when ready
```
Then route to `database.db` vs `database.db_postgres` in your app.

## 8. Important SQL differences (SQLite -> Postgres)
- Placeholders: SQLite uses `?`, psycopg uses `%s`
- `AUTOINCREMENT` -> `bigserial`
- JSON text -> `jsonb`
- Date/time text -> `timestamptz` / `date`
- `INSERT OR IGNORE` -> `INSERT ... ON CONFLICT DO NOTHING`

## 9. Suggested next code changes
- Replace `sqlite3` calls in `database/db.py` with a shared DB interface
- Version signal runs with `signal_runs`
- Write new snapshots on each ingestion cycle instead of overwriting history
- Query `market_signals` with `is_latest = true` for dashboards
