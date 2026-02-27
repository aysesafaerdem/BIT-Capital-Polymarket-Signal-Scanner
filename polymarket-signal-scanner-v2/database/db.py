"""
Database initialization and connection management.
Supports SQLite (legacy/local) and PostgreSQL/Supabase (DB_BACKEND=postgres).
"""
import json
import os
from decimal import Decimal
from datetime import date, datetime
from pathlib import Path



def _normalize_jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, list):
        return [_normalize_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_normalize_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize_jsonable(v) for k, v in value.items()}
    return value

DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite").strip().lower()
DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent.parent / "data" / "polymarket_scanner.db"))

if DB_BACKEND == "postgres":
    from database.db_postgres import get_connection as _pg_get_connection, init_db_postgres as _pg_init_db

    def get_connection():
        return _pg_get_connection()

    def init_db():
        return _pg_init_db()

    def dict_from_row(row):
        if row is None:
            return None
        d = dict(row)
        # psycopg already returns Python objects for jsonb, but keep compatibility for text values.
        json_fields = [
            'subcategories', 'odds_history', 'related_markets',
            'primary_channels', 'key_geographies', 'trigger_keywords',
            'causal_chain', 'affected_holdings', 'portfolio_theme_fit',
            'what_to_watch_next', 'red_flags_or_unknowns', 'themes',
            'top_signals', 'full_report_json'
        ]
        for field in json_fields:
            if field in d and d[field] and isinstance(d[field], str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return _normalize_jsonable(d)
else:
    import sqlite3

    def get_connection():
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_db():
        schema_path = Path(__file__).parent / "schema.sql"
        conn = get_connection()
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            conn.executescript(schema_sql)
            conn.commit()
            print(f"[DB] Database initialized at {DB_PATH}")
        finally:
            conn.close()

    def dict_from_row(row):
        if row is None:
            return None
        d = dict(row)
        json_fields = [
            'subcategories', 'odds_history', 'related_markets',
            'primary_channels', 'key_geographies', 'trigger_keywords',
            'causal_chain', 'affected_holdings', 'portfolio_theme_fit',
            'what_to_watch_next', 'red_flags_or_unknowns', 'themes',
            'top_signals', 'full_report_json'
        ]
        for field in json_fields:
            if field in d and d[field] and isinstance(d[field], str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return _normalize_jsonable(d)

if __name__ == "__main__":
    init_db()
    print(f"[DB] Schema created successfully ({DB_BACKEND}).")
