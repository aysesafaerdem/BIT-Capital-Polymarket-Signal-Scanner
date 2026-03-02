from __future__ import annotations
...
"""PostgreSQL connection helpers for Supabase / Postgres deployments.

This file is intentionally separate from database/db.py so the current SQLite app keeps working
while you migrate query code incrementally.
"""
import json
import os
from pathlib import Path

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_connection():
    """Return a psycopg connection configured to yield dict-like rows."""
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Add psycopg[binary] to requirements.")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, prepare_threshold=None)


def init_db_postgres(schema_path: str | None = None):
    """Initialize Postgres schema from schema_postgres.sql."""
    schema_file = Path(schema_path) if schema_path else Path(__file__).parent / "schema_postgres.sql"
    sql = schema_file.read_text()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print(f"[DB] PostgreSQL schema initialized from {schema_file}")


def dict_from_row(row):
    """Normalize row to dict and parse selected JSON strings if needed."""
    if row is None:
        return None
    d = dict(row)
    json_fields = {
        'subcategories', 'odds_history', 'related_markets',
        'raw_market', 'raw_data', 'raw_analysis', 'causal_chain',
        'what_to_watch_next', 'red_flags_or_unknowns', 'full_report_json'
    }
    for field in json_fields:
        if field in d and isinstance(d[field], str):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                pass
    return d


if __name__ == "__main__":
    init_db_postgres()
