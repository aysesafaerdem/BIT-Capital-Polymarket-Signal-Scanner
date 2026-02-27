#!/usr/bin/env python3
"""One-time migration: SQLite -> PostgreSQL (Supabase-compatible).

Usage:
  export DATABASE_URL=postgresql://...
  export DB_PATH=data/polymarket_scanner.db
  python scripts/migrate_sqlite_to_postgres.py
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
SQLITE_DB = os.environ.get("DB_PATH", str(ROOT / "data" / "polymarket_scanner.db"))
DATABASE_URL = os.environ.get("DATABASE_URL")

CHANNEL_LABELS = {
    "crypto": "Crypto",
    "ai_semis": "AI / Semiconductors",
    "rates": "Rates / Macro",
    "oil": "Oil / Energy",
    "cyber": "Cybersecurity",
    "fintech": "Fintech",
    "geopolitics": "Geopolitics",
    "fx": "FX",
}


def null_if_blank(v: Any):
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    return v


def to_jsonb_str(v: Any, default):
    parsed = parse_json(v)
    if isinstance(parsed, (dict, list)):
        return json.dumps(parsed)
    return json.dumps(default)


def parse_json(v: Any):
    if not v:
        return [] if v in (None, "") else v
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def sqlite_conn():
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    return conn


def pg_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_schema(conn):
    schema = (ROOT / "database" / "schema_postgres.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.commit()


def upsert_theme(cur, name: str) -> int:
    cur.execute(
        """
        insert into themes (name) values (%s)
        on conflict (name) do update set name = excluded.name
        returning id
        """,
        (name,),
    )
    return cur.fetchone()["id"]


def upsert_geography(cur, code: str) -> str:
    label = code
    cur.execute(
        "insert into geographies (code, label) values (%s, %s) on conflict (code) do nothing",
        (code, label),
    )
    return code


def migrate_portfolio(sqlc, pgc) -> dict[str, int]:
    rows = sqlc.execute("select * from portfolio_config order by id asc").fetchall()
    ticker_to_id: dict[str, int] = {}
    with pgc.cursor() as cur:
        for r in rows:
            key_ticker = r["ticker"] or ''
            cur.execute(
                """
                select id from portfolio_holdings
                where coalesce(ticker, '') = %s and fund = %s and name = %s
                limit 1
                """,
                (key_ticker, r["fund"], r["name"]),
            )
            existing = cur.fetchone()
            if existing:
                holding_id = existing["id"]
            else:
                cur.execute(
                    """
                    insert into portfolio_holdings (name, ticker, fund, weight_pct, sector, is_active, updated_at)
                    values (%s,%s,%s,%s,%s,%s,%s)
                    returning id
                    """,
                    (
                        r["name"], r["ticker"], r["fund"], r["weight"], r["sector"], bool(r["is_active"]), r["updated_at"],
                    ),
                )
                holding_id = cur.fetchone()["id"]
            if r["ticker"] and r["ticker"] not in ticker_to_id:
                ticker_to_id[r["ticker"]] = holding_id
            for theme_name in parse_json(r["themes"]) or []:
                if not theme_name:
                    continue
                theme_id = upsert_theme(cur, str(theme_name))
                cur.execute(
                    "insert into portfolio_holding_themes (holding_id, theme_id) values (%s,%s) on conflict do nothing",
                    (holding_id, theme_id),
                )
    pgc.commit()
    print(f"[migrate] portfolio_holdings: {len(rows)}")
    return ticker_to_id


def migrate_markets(sqlc, pgc):
    rows = sqlc.execute("select * from markets").fetchall()
    with pgc.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                insert into markets (id, question, description, category, subcategories, end_time, raw_market, created_at, updated_at)
                values (%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s)
                on conflict (id) do update set
                  question = excluded.question,
                  description = excluded.description,
                  category = excluded.category,
                  subcategories = excluded.subcategories,
                  end_time = excluded.end_time,
                  raw_market = excluded.raw_market,
                  updated_at = now()
                """,
                (
                    r["id"],
                    r["question"],
                    r["description"],
                    r["category"],
                    to_jsonb_str(r["subcategories"], []),
                    null_if_blank(r["end_date"]),
                    to_jsonb_str(r["raw_data"], {}),
                    null_if_blank(r["fetched_at"]),
                    null_if_blank(r["updated_at"]) or null_if_blank(r["fetched_at"]) or None,
                ),
            )
            cur.execute(
                """
                insert into market_snapshots (market_id, fetched_at, current_yes, current_no, volume_usd, liquidity_usd, odds_history, related_markets, raw_data)
                values (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
                on conflict (market_id, fetched_at) do nothing
                """,
                (
                    r["id"],
                    null_if_blank(r["fetched_at"]),
                    r["current_yes"],
                    r["current_no"],
                    r["volume"],
                    r["liquidity"],
                    to_jsonb_str(r["odds_history"], []),
                    to_jsonb_str(r["related_markets"], []),
                    to_jsonb_str(r["raw_data"], {}),
                ),
            )
    pgc.commit()
    print(f"[migrate] markets: {len(rows)}")


def create_import_signal_run(pgc) -> int:
    with pgc.cursor() as cur:
        cur.execute(
            """
            insert into signal_runs (run_type, model_name, prompt_version, status, metadata, started_at, completed_at)
            values (%s,%s,%s,%s,%s::jsonb, now(), now())
            returning id
            """,
            ("import", "sqlite_migration", "legacy", "success", json.dumps({"source": "sqlite"})),
        )
        run_id = cur.fetchone()["id"]
    pgc.commit()
    return run_id


def migrate_signals(sqlc, pgc, ticker_to_holding_id: dict[str, int], signal_run_id: int):
    rows = sqlc.execute("select * from signals").fetchall()
    market_to_signal_id: dict[str, int] = {}
    with pgc.cursor() as cur:
        for r in rows:
            # If re-running migration, mark existing latest false before inserting fresh latest.
            cur.execute("update market_signals set is_latest = false where market_id = %s and is_latest = true", (r["market_id"],))
            cur.execute(
                """
                insert into market_signals (
                  market_id, signal_run_id, relevance_label, relevance_score, one_sentence_verdict,
                  event_type, causal_chain, what_to_watch_next, red_flags_or_unknowns, raw_analysis,
                  analyzed_at, is_latest
                ) values (
                  %s,%s,%s,%s,%s,
                  %s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,
                  %s,true
                )
                returning id
                """,
                (
                    r["market_id"],
                    signal_run_id,
                    r["relevance_label"],
                    r["relevance_score"],
                    r["one_sentence_verdict"],
                    r["event_type"],
                    to_jsonb_str(r["causal_chain"], []),
                    to_jsonb_str(r["what_to_watch_next"], []),
                    to_jsonb_str(r["red_flags_or_unknowns"], []),
                    to_jsonb_str(r["raw_analysis"], {}),
                    null_if_blank(r["analyzed_at"]) or None,
                ),
            )
            signal_id = cur.fetchone()["id"]
            market_to_signal_id[r["market_id"]] = signal_id

            for ch in (parse_json(r["primary_channels"]) or []):
                if not ch:
                    continue
                code = str(ch)
                cur.execute("insert into channels (code, label) values (%s,%s) on conflict (code) do nothing", (code, CHANNEL_LABELS.get(code, code)))
                cur.execute("insert into market_signal_channels (market_signal_id, channel_code) values (%s,%s) on conflict do nothing", (signal_id, code))

            for geo in (parse_json(r["key_geographies"]) or []):
                if not geo:
                    continue
                code = str(geo)
                upsert_geography(cur, code)
                cur.execute("insert into market_signal_geographies (market_signal_id, geography_code) values (%s,%s) on conflict do nothing", (signal_id, code))

            for kw in (parse_json(r["trigger_keywords"]) or []):
                if not kw:
                    continue
                cur.execute("insert into market_signal_keywords (market_signal_id, keyword) values (%s,%s) on conflict do nothing", (signal_id, str(kw)))

            for theme_name in (parse_json(r["portfolio_theme_fit"]) or []):
                if not theme_name:
                    continue
                theme_id = upsert_theme(cur, str(theme_name))
                cur.execute("insert into market_signal_themes (market_signal_id, theme_id) values (%s,%s) on conflict do nothing", (signal_id, theme_id))

            for impact in (parse_json(r["affected_holdings"]) or []):
                if not isinstance(impact, dict):
                    continue
                ticker = impact.get("ticker_or_symbol")
                holding_id = ticker_to_holding_id.get(ticker) if ticker else None
                cur.execute(
                    """
                    insert into market_signal_holding_impacts (
                      market_signal_id, holding_id, ticker_or_symbol, name, direction, mechanism, time_horizon, confidence
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        signal_id,
                        holding_id,
                        ticker,
                        impact.get("name"),
                        (impact.get("direction") or "MIXED"),
                        impact.get("mechanism"),
                        impact.get("time_horizon"),
                        impact.get("confidence"),
                    ),
                )
    pgc.commit()
    print(f"[migrate] market_signals: {len(rows)}")
    return market_to_signal_id


def migrate_reports(sqlc, pgc, market_to_signal_id: dict[str, int]):
    rows = sqlc.execute("select * from reports order by id asc").fetchall()
    with pgc.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                insert into reports (
                  report_date, title, executive_summary, actionable_count, monitor_count, ignore_count,
                  full_report_html, full_report_json, generated_at
                ) values (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                returning id
                """,
                (
                    null_if_blank(r["report_date"]), r["title"], r["executive_summary"], r["actionable_count"],
                    r["monitor_count"], r["ignore_count"], r["full_report_html"],
                    r["full_report_json"] if r["full_report_json"] else json.dumps({}), null_if_blank(r["generated_at"]) or None,
                ),
            )
            report_id = cur.fetchone()["id"]
            top_signals = parse_json(r["top_signals"]) or []
            for rank, market_id in enumerate(top_signals, start=1):
                signal_id = market_to_signal_id.get(str(market_id))
                if not signal_id:
                    continue
                cur.execute(
                    "insert into report_items (report_id, market_signal_id, section, rank) values (%s,%s,%s,%s) on conflict do nothing",
                    (report_id, signal_id, "top_signals", rank),
                )
    pgc.commit()
    print(f"[migrate] reports: {len(rows)}")


def migrate_jobs(sqlc, pgc):
    rows = sqlc.execute("select * from scheduler_log order by id asc").fetchall()
    with pgc.cursor() as cur:
        for r in rows:
            cur.execute(
                "insert into job_runs (job_name, status, message, started_at, completed_at) values (%s,%s,%s,%s,%s)",
                (r["job_name"], r["status"], r["message"], null_if_blank(r["started_at"]), null_if_blank(r["completed_at"])),
            )
    pgc.commit()
    print(f"[migrate] job_runs: {len(rows)}")


def main():
    print(f"[migrate] SQLite source: {SQLITE_DB}")
    if not Path(SQLITE_DB).exists():
        raise SystemExit("SQLite DB file not found. Set DB_PATH correctly.")
    with sqlite_conn() as sq, pg_conn() as pg:
        ensure_schema(pg)
        ticker_map = migrate_portfolio(sq, pg)
        migrate_markets(sq, pg)
        run_id = create_import_signal_run(pg)
        market_signal_map = migrate_signals(sq, pg, ticker_map, run_id)
        migrate_reports(sq, pg, market_signal_map)
        migrate_jobs(sq, pg)
    print("[migrate] Done.")


if __name__ == "__main__":
    main()
