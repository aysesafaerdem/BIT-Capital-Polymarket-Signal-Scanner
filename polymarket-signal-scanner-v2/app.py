"""
Polymarket Signal Scanner — Main Flask Application
BIT Capital Equity & Macro Relevance Filter
"""
import json
import os
import sys
from datetime import datetime, timezone
from flask import Flask, jsonify, request, render_template_string, send_from_directory
from pathlib import Path
from urllib.parse import urlparse
from backend.agenda_watchlist import match_agenda_hints
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))

from database.db import get_connection, init_db, dict_from_row

app = Flask(__name__, static_folder="frontend/static")
app.config["JSON_SORT_KEYS"] = False

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite").strip().lower()


def _llm_status():
    if GROQ_API_KEY:
        return {"provider": "groq", "configured": True, "label": "Groq"}
    if GEMINI_API_KEY:
        return {"provider": "gemini", "configured": True, "label": "Gemini"}
    if ANTHROPIC_API_KEY:
        return {"provider": "anthropic", "configured": True, "label": "Claude"}
    return {"provider": "fallback", "configured": False, "label": "Rule-based"}


def _mask_db_target():
    if DB_BACKEND == "postgres":
        url = os.environ.get("DATABASE_URL", "")
        if not url:
            return "(missing DATABASE_URL)"
        try:
            p = urlparse(url)
            return f"postgres://{p.username}@{p.hostname}:{p.port}{p.path}"
        except Exception:
            return "postgres://***"
    return os.environ.get("DB_PATH", "data/polymarket_scanner.db")



# ============================================================
# STARTUP
# ============================================================

def startup():
    """Initialize DB and start scheduler."""
    if os.environ.get("INIT_DB_ON_STARTUP", "1") not in {"0", "false", "False"}:
        init_db()

    # In Postgres mode, skip the legacy SQLite bootstrap flow but still run the scheduler.
    # Manual ingestion/analysis/report paths are already Postgres-compatible.
    if DB_BACKEND == "postgres":
        print("[APP] Postgres mode enabled: skipping legacy bootstrap seed/analyze/report warmup; starting scheduler.")
        try:
            from backend.scheduler import start_scheduler
            start_scheduler()
        except Exception as e:
            print(f"[APP] Scheduler start error (postgres mode): {e}")
        return

    # Check if we need to seed data
    conn = get_connection()
    market_count = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
    conn.close()

    if market_count == 0:
        print("[APP] No markets found, running initial data load...")
        try:
            from backend.ingestion import run_ingestion, inject_sample_markets
            result = run_ingestion(max_markets=200)

            # Check again
            conn = get_connection()
            count = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
            conn.close()
            if count < 3:
                inject_sample_markets()
        except Exception as e:
            print(f"[APP] Ingestion error: {e}")
            try:
                from backend.ingestion import inject_sample_markets
                inject_sample_markets()
            except Exception as e2:
                print(f"[APP] Sample injection error: {e2}")

    # Run analysis on unanalyzed markets
    conn = get_connection()
    unanalyzed = conn.execute("""
        SELECT COUNT(*) as c FROM markets m
        LEFT JOIN signals s ON m.id = s.market_id
        WHERE s.market_id IS NULL
    """).fetchone()["c"]
    conn.close()

    if unanalyzed > 0:
        print(f"[APP] Analyzing {unanalyzed} unanalyzed markets...")
        try:
            from backend.analysis import analyze_markets
            analyze_markets(batch_size=min(unanalyzed, 30))
        except Exception as e:
            print(f"[APP] Analysis error: {e}")

    # Generate initial report if none exists
    conn = get_connection()
    report_count = conn.execute("SELECT COUNT(*) as c FROM reports").fetchone()["c"]
    conn.close()

    if report_count == 0:
        try:
            from backend.report_generator import generate_full_report
            generate_full_report()
        except Exception as e:
            print(f"[APP] Report generation error: {e}")

    # Start background scheduler
    try:
        from backend.scheduler import start_scheduler
        start_scheduler()
    except Exception as e:
        print(f"[APP] Scheduler start error: {e}")


# ============================================================
# API ROUTES
# ============================================================

@app.route("/api/stats")
def api_stats():
    """Dashboard statistics."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            total_markets = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
            analyzed = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true").fetchone()["c"]
            actionable = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true AND relevance_label='ACTIONABLE'").fetchone()["c"]
            monitor = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true AND relevance_label='MONITOR'").fetchone()["c"]
            ignore = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true AND relevance_label='IGNORE'").fetchone()["c"]
            reports = conn.execute("SELECT COUNT(*) as c FROM reports").fetchone()["c"]
            last_ingestion = conn.execute("""
                SELECT completed_at FROM job_runs
                WHERE job_name='ingestion' AND status='SUCCESS'
                ORDER BY started_at DESC LIMIT 1
            """).fetchone()

            channel_rows = conn.execute("""
                SELECT c.channel_code, COUNT(*) as c
                FROM market_signal_channels c
                JOIN market_signals s ON s.id = c.market_signal_id
                WHERE s.is_latest = true
                GROUP BY c.channel_code
            """).fetchall()
            channel_counts = {r["channel_code"]: r["c"] for r in channel_rows}

            theme_rows = conn.execute("""
                SELECT t.name, COUNT(*) as c
                FROM market_signal_themes st
                JOIN themes t ON t.id = st.theme_id
                JOIN market_signals s ON s.id = st.market_signal_id
                WHERE s.is_latest = true AND s.relevance_label IN ('ACTIONABLE','MONITOR')
                GROUP BY t.name
            """).fetchall()
            theme_counts = {r["name"]: r["c"] for r in theme_rows}
        else:
            total_markets = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
            analyzed = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
            actionable = conn.execute("SELECT COUNT(*) as c FROM signals WHERE relevance_label='ACTIONABLE'").fetchone()["c"]
            monitor = conn.execute("SELECT COUNT(*) as c FROM signals WHERE relevance_label='MONITOR'").fetchone()["c"]
            ignore = conn.execute("SELECT COUNT(*) as c FROM signals WHERE relevance_label='IGNORE'").fetchone()["c"]
            reports = conn.execute("SELECT COUNT(*) as c FROM reports").fetchone()["c"]
            last_ingestion = conn.execute("""
                SELECT completed_at FROM scheduler_log
                WHERE job_name='ingestion' AND status='SUCCESS'
                ORDER BY started_at DESC LIMIT 1
            """).fetchone()
            channel_rows = conn.execute("SELECT primary_channels FROM signals WHERE primary_channels IS NOT NULL").fetchall()
            channel_counts = {}
            for row in channel_rows:
                try:
                    for ch in json.loads(row["primary_channels"]):
                        channel_counts[ch] = channel_counts.get(ch, 0) + 1
                except Exception:
                    pass
            theme_rows = conn.execute("""
                SELECT portfolio_theme_fit FROM signals
                WHERE relevance_label IN ('ACTIONABLE','MONITOR') AND portfolio_theme_fit IS NOT NULL
            """).fetchall()
            theme_counts = {}
            for row in theme_rows:
                try:
                    for th in json.loads(row["portfolio_theme_fit"]):
                        theme_counts[th] = theme_counts.get(th, 0) + 1
                except Exception:
                    pass

        return jsonify({
            "total_markets": total_markets,
            "analyzed": analyzed,
            "actionable": actionable,
            "monitor": monitor,
            "ignore": ignore,
            "reports": reports,
            "signal_rate": round((actionable + monitor) / max(analyzed, 1) * 100, 1),
            "filter_rate": round(ignore / max(analyzed, 1) * 100, 1),
            "last_ingestion": last_ingestion["completed_at"] if last_ingestion else None,
            "channel_distribution": channel_counts,
            "theme_distribution": theme_counts,
            "api_key_configured": bool(GROQ_API_KEY or ANTHROPIC_API_KEY),
            "llm_provider": _llm_status()["provider"],
            "llm_label": _llm_status()["label"],
        })
    finally:
        conn.close()


@app.route("/api/markets")
def api_markets():
    """List markets with optional filtering."""
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    label_filter = request.args.get("label", "")
    channel_filter = request.args.get("channel", "")
    search = request.args.get("q", "")
    offset = (page - 1) * per_page

    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            conditions, params = [], []
            if label_filter:
                conditions.append("s.relevance_label = %s")
                params.append(label_filter)
            if channel_filter:
                conditions.append("exists (select 1 from market_signal_channels c where c.market_signal_id = s.id and c.channel_code = %s)")
                params.append(channel_filter)
            if search:
                conditions.append("(m.question ILIKE %s OR coalesce(m.description,'') ILIKE %s)")
                params.extend([f'%{search}%', f'%{search}%'])
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            total = conn.execute(f"""
                SELECT COUNT(*) as c
                FROM markets m
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                {where_clause}
            """, params).fetchone()["c"]
            rows = conn.execute(f"""
                SELECT m.id, m.question, m.category, m.end_time as end_date,
                       snap.current_yes, snap.current_no, snap.volume_usd as volume, snap.liquidity_usd as liquidity,
                       snap.fetched_at, snap.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type,
                       (SELECT json_agg(c.channel_code ORDER BY c.channel_code) FROM market_signal_channels c WHERE c.market_signal_id = s.id) as primary_channels,
                       (SELECT json_agg(json_build_object('ticker_or_symbol',hi.ticker_or_symbol,'name',hi.name,'direction',hi.direction,'mechanism',hi.mechanism,'time_horizon',hi.time_horizon,'confidence',hi.confidence)) FROM market_signal_holding_impacts hi WHERE hi.market_signal_id = s.id) as affected_holdings,
                       (SELECT json_agg(k.keyword ORDER BY k.keyword) FROM market_signal_keywords k WHERE k.market_signal_id = s.id) as trigger_keywords,
                       s.analyzed_at
                FROM markets m
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.liquidity_usd, ms.fetched_at, ms.odds_history
                    FROM market_snapshots ms WHERE ms.market_id = m.id
                    ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                {where_clause}
                ORDER BY COALESCE(s.relevance_score, -1) DESC, COALESCE(snap.volume_usd,0) DESC
                LIMIT %s OFFSET %s
            """, params + [per_page, offset]).fetchall()
        else:
            conditions, params = [], []
            if label_filter:
                conditions.append("s.relevance_label = ?")
                params.append(label_filter)
            if channel_filter:
                conditions.append("s.primary_channels LIKE ?")
                params.append(f'%{channel_filter}%')
            if search:
                conditions.append("(m.question LIKE ? OR m.description LIKE ?)")
                params.extend([f'%{search}%', f'%{search}%'])
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            total = conn.execute(f"""
                SELECT COUNT(*) as c FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                {where_clause}
            """, params).fetchone()["c"]
            rows = conn.execute(f"""
                SELECT m.id, m.question, m.category, m.end_date,
                       m.current_yes, m.current_no, m.volume, m.liquidity,
                       m.fetched_at, m.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type, s.primary_channels, s.affected_holdings,
                       s.trigger_keywords, s.analyzed_at
                FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                {where_clause}
                ORDER BY COALESCE(s.relevance_score, -1) DESC, m.volume DESC
                LIMIT ? OFFSET ?
            """, params + [per_page, offset]).fetchall()
        markets = [dict_from_row(r) for r in rows]
        return jsonify({"markets": markets, "total": total, "page": page, "per_page": per_page, "pages": (total + per_page - 1) // per_page})
    finally:
        conn.close()


@app.route("/api/markets/<market_id>")
def api_market_detail(market_id):
    """Get detailed market info including full analysis."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            row = conn.execute("""
                SELECT m.id, m.question, m.description, m.category, m.subcategories,
                       m.end_time as end_date, m.raw_market as raw_data, m.created_at, m.updated_at,
                       snap.current_yes, snap.current_no, snap.volume_usd as volume, snap.liquidity_usd as liquidity,
                       snap.fetched_at, snap.odds_history, snap.related_markets,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type,
                       (SELECT json_agg(c.channel_code ORDER BY c.channel_code) FROM market_signal_channels c WHERE c.market_signal_id = s.id) as primary_channels,
                       (SELECT json_agg(g.geography_code ORDER BY g.geography_code) FROM market_signal_geographies g WHERE g.market_signal_id = s.id) as key_geographies,
                       (SELECT json_agg(k.keyword ORDER BY k.keyword) FROM market_signal_keywords k WHERE k.market_signal_id = s.id) as trigger_keywords,
                       s.causal_chain,
                       (SELECT json_agg(json_build_object('ticker_or_symbol',hi.ticker_or_symbol,'name',hi.name,'direction',hi.direction,'mechanism',hi.mechanism,'time_horizon',hi.time_horizon,'confidence',hi.confidence)) FROM market_signal_holding_impacts hi WHERE hi.market_signal_id = s.id) as affected_holdings,
                       (SELECT json_agg(t.name ORDER BY t.name) FROM market_signal_themes st JOIN themes t ON t.id = st.theme_id WHERE st.market_signal_id = s.id) as portfolio_theme_fit,
                       s.what_to_watch_next, s.red_flags_or_unknowns, s.raw_analysis, s.analyzed_at
                FROM markets m
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.liquidity_usd, ms.fetched_at, ms.odds_history, ms.related_markets
                    FROM market_snapshots ms WHERE ms.market_id = m.id
                    ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                WHERE m.id = %s
            """, (market_id,)).fetchone()
        else:
            row = conn.execute("""
                SELECT m.*, s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type, s.primary_channels, s.key_geographies, s.trigger_keywords,
                       s.causal_chain, s.affected_holdings, s.portfolio_theme_fit,
                       s.what_to_watch_next, s.red_flags_or_unknowns, s.raw_analysis, s.analyzed_at
                FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                WHERE m.id = ?
            """, (market_id,)).fetchone()
        if not row:
            return jsonify({"error": "Market not found"}), 404
        return jsonify(dict_from_row(row))
    finally:
        conn.close()


@app.route("/api/signals")
def api_signals():
    """Get top signals for the dashboard."""
    label = request.args.get("label", "")
    limit = int(request.args.get("limit", 20))

    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            where = "WHERE s.is_latest = true AND s.relevance_label = %s" if label else "WHERE s.is_latest = true AND s.relevance_label IN ('ACTIONABLE','MONITOR')"
            params = [label] if label else []
            params.append(limit)
            rows = conn.execute(f"""
                SELECT m.id, m.question,
                       snap.current_yes, snap.current_no, snap.volume_usd as volume,
                       m.category, m.end_time as end_date, snap.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type,
                       (SELECT json_agg(c.channel_code ORDER BY c.channel_code) FROM market_signal_channels c WHERE c.market_signal_id = s.id) as primary_channels,
                       (SELECT json_agg(json_build_object('ticker_or_symbol',hi.ticker_or_symbol,'name',hi.name,'direction',hi.direction,'mechanism',hi.mechanism,'time_horizon',hi.time_horizon,'confidence',hi.confidence)) FROM market_signal_holding_impacts hi WHERE hi.market_signal_id = s.id) as affected_holdings,
                       s.causal_chain,
                       (SELECT json_agg(t.name ORDER BY t.name) FROM market_signal_themes st JOIN themes t ON t.id = st.theme_id WHERE st.market_signal_id = s.id) as portfolio_theme_fit,
                       (SELECT json_agg(k.keyword ORDER BY k.keyword) FROM market_signal_keywords k WHERE k.market_signal_id = s.id) as trigger_keywords,
                       s.what_to_watch_next,
                       s.analyzed_at
                FROM market_signals s
                JOIN markets m ON m.id = s.market_id
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.odds_history
                    FROM market_snapshots ms WHERE ms.market_id = m.id
                    ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                {where}
                ORDER BY s.relevance_score DESC, s.analyzed_at DESC, COALESCE(snap.volume_usd,0) DESC
                LIMIT %s
            """, params).fetchall()
            return jsonify([dict_from_row(r) for r in rows])
        else:
            where = "WHERE s.relevance_label = ?" if label else "WHERE s.relevance_label IN ('ACTIONABLE','MONITOR')"
            params = [label] if label else []
            params.append(limit)
            rows = conn.execute(f"""
                SELECT m.id, m.question, m.current_yes, m.current_no, m.volume,
                       m.category, m.end_date, m.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type, s.primary_channels, s.affected_holdings,
                       s.causal_chain, s.portfolio_theme_fit, s.trigger_keywords,
                       s.what_to_watch_next, s.analyzed_at
                FROM signals s
                JOIN markets m ON s.market_id = m.id
                {where}
                ORDER BY s.relevance_score DESC, s.analyzed_at DESC, COALESCE(m.volume,0) DESC
                LIMIT ?
            """, params).fetchall()
            return jsonify([dict_from_row(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/reports")
def api_reports():
    """List all generated reports."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            rows = conn.execute("""
                SELECT id, report_date, title, executive_summary,
                       actionable_count, monitor_count, ignore_count, generated_at
                FROM reports
                -- Cast protects ordering when legacy rows use text timestamps with mixed offsets.
                ORDER BY generated_at::timestamptz DESC, id DESC
                LIMIT 50
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT id, report_date, title, executive_summary,
                       actionable_count, monitor_count, ignore_count, generated_at
                FROM reports
                ORDER BY datetime(generated_at) DESC, id DESC
                LIMIT 50
            """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/reports/<int:report_id>")
def api_report_detail(report_id):
    """Get a specific report with full HTML and JSON."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM reports WHERE id = %s", (report_id,)).fetchone() if DB_BACKEND == "postgres" else conn.execute(
            "SELECT * FROM reports WHERE id = ?", (report_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "Report not found"}), 404
        return jsonify(dict_from_row(row))
    finally:
        conn.close()


@app.route("/api/reports/latest")
def api_latest_report():
    """Get the most recent report."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            row = conn.execute(
                "SELECT * FROM reports ORDER BY generated_at::timestamptz DESC, id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM reports ORDER BY datetime(generated_at) DESC, id DESC LIMIT 1"
            ).fetchone()
        if not row:
            return jsonify({"error": "No reports yet"}), 404
        return jsonify(dict_from_row(row))
    finally:
        conn.close()


@app.route("/api/portfolio")
def api_portfolio():
    """Get portfolio configuration."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            rows = conn.execute("""
                SELECT h.id, h.name, h.ticker, h.fund, h.weight_pct as weight, h.sector,
                       CASE WHEN h.is_active THEN 1 ELSE 0 END as is_active,
                       h.updated_at,
                       (SELECT json_agg(t.name ORDER BY t.name)
                        FROM portfolio_holding_themes pht
                        JOIN themes t ON t.id = pht.theme_id
                        WHERE pht.holding_id = h.id) as themes
                FROM portfolio_holdings h
                WHERE h.is_active = true
                ORDER BY h.fund, h.weight_pct DESC NULLS LAST
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM portfolio_config WHERE is_active = 1
                ORDER BY fund, weight DESC
            """).fetchall()
        return jsonify([dict_from_row(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/portfolio/<int:holding_id>", methods=["PUT"])
def api_update_holding(holding_id):
    """Update a portfolio holding configuration."""
    data = request.get_json() or {}
    conn = get_connection()
    try:
        now_iso = datetime.now(timezone.utc).isoformat()

        if DB_BACKEND == "postgres":
            is_active = bool(data.get("is_active", True))
            weight = data.get("weight", None)
            themes = data.get("themes", [])

            conn.execute("""
                UPDATE portfolio_holdings
                SET is_active = %s,
                    weight_pct = %s,
                    updated_at = %s
                WHERE id = %s
            """, (is_active, weight, now_iso, holding_id))

            # Replace theme mappings if provided
            if isinstance(themes, list):
                conn.execute("DELETE FROM portfolio_holding_themes WHERE holding_id = %s", (holding_id,))
                for theme_name in themes:
                    if not theme_name:
                        continue
                    # upsert theme
                    row = conn.execute("""
                        INSERT INTO themes (name)
                        VALUES (%s)
                        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                        RETURNING id
                    """, (str(theme_name),)).fetchone()
                    if row:
                        theme_id = row["id"]
                    else:
                        theme_id = conn.execute("SELECT id FROM themes WHERE name = %s", (str(theme_name),)).fetchone()["id"]

                    conn.execute("""
                        INSERT INTO portfolio_holding_themes (holding_id, theme_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (holding_id, theme_id))

            conn.commit()
            return jsonify({"status": "updated"})

        # SQLite path
        conn.execute("""
            UPDATE portfolio_config SET
                is_active = ?, weight = ?, themes = ?, updated_at = ?
            WHERE id = ?
        """, (
            data.get("is_active", 1),
            data.get("weight", 0),
            json.dumps(data.get("themes", [])) if isinstance(data.get("themes"), list) else data.get("themes"),
            now_iso,
            holding_id
        ))
        conn.commit()
        return jsonify({"status": "updated"})
    finally:
        conn.close()



@app.route("/api/actions/ingest", methods=["POST"])
def api_trigger_ingestion():
    """Manually trigger market ingestion."""
    import threading
    def _run():
        try:
            from backend.ingestion import run_ingestion, inject_sample_markets
            result = run_ingestion(max_markets=200)
            conn = get_connection()
            count = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
            conn.close()
            if count < 3:
                inject_sample_markets()
        except Exception as e:
            print(f"[API] Ingestion error: {e}")
    
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "Ingestion job triggered"})


@app.route("/api/actions/analyze", methods=["POST"])
def api_trigger_analysis():
    """Manually trigger market analysis."""
    import threading
    data = request.get_json() or {}
    batch_size = data.get("batch_size", 30)
    force = data.get("force", False)
    
    def _run():
        try:
            from backend.analysis import analyze_markets
            analyze_markets(batch_size=batch_size, force_reanalyze=force)
        except Exception as e:
            print(f"[API] Analysis error: {e}")
    
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": f"Analysis job triggered (batch={batch_size})"})


@app.route("/api/actions/report", methods=["POST"])
def api_trigger_report():
    """Manually trigger report generation."""
    import threading

    def _run():
        conn = get_connection()
        job_id = None
        started_at = datetime.now(timezone.utc).isoformat()
        try:
            if DB_BACKEND == "postgres":
                row = conn.execute(
                    "INSERT INTO job_runs (job_name, status, message, started_at) VALUES (%s,%s,%s,%s) RETURNING id",
                    ("report", "RUNNING", "Generating report...", started_at),
                ).fetchone()
                job_id = row["id"] if row else None
            else:
                cur = conn.execute(
                    "INSERT INTO scheduler_log (job_name, status, message, started_at) VALUES (?,?,?,?)",
                    ("report", "RUNNING", "Generating report...", started_at),
                )
                job_id = cur.lastrowid
            conn.commit()

            from backend.report_generator import generate_full_report
            result = generate_full_report()

            completed_at = datetime.now(timezone.utc).isoformat()
            done_msg = json.dumps(result) if isinstance(result, dict) else str(result or "ok")
            if DB_BACKEND == "postgres":
                conn.execute(
                    "UPDATE job_runs SET status=%s, message=%s, completed_at=%s WHERE id=%s",
                    ("SUCCESS", done_msg, completed_at, job_id),
                )
            else:
                conn.execute(
                    "UPDATE scheduler_log SET status=?, message=?, completed_at=? WHERE id=?",
                    ("SUCCESS", done_msg, completed_at, job_id),
                )
            conn.commit()
        except Exception as e:
            print(f"[API] Report error: {e}")
            completed_at = datetime.now(timezone.utc).isoformat()
            if job_id is not None:
                if DB_BACKEND == "postgres":
                    conn.execute(
                        "UPDATE job_runs SET status=%s, message=%s, completed_at=%s WHERE id=%s",
                        ("FAILED", str(e), completed_at, job_id),
                    )
                else:
                    conn.execute(
                        "UPDATE scheduler_log SET status=?, message=?, completed_at=? WHERE id=?",
                        ("FAILED", str(e), completed_at, job_id),
                    )
                conn.commit()
        finally:
            conn.close()
    
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "Report generation triggered"})


@app.route("/api/scheduler/status")
def api_scheduler_status():
    """Get scheduler job status."""
    try:
        from backend.scheduler import get_scheduler_status
        return jsonify(get_scheduler_status())
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/config")
def api_config():
    """Get app configuration (sanitized)."""
    return jsonify({
        "anthropic_api_configured": bool(ANTHROPIC_API_KEY),
        "groq_api_configured": bool(GROQ_API_KEY),
        "gemini_api_configured": bool(GEMINI_API_KEY),
        "llm_provider": _llm_status()["provider"],
        "ingestion_interval_secs": int(os.environ.get("INGESTION_INTERVAL_SECS", 3600)),
        "analysis_interval_secs": int(os.environ.get("ANALYSIS_INTERVAL_SECS", 1800)),
        "report_interval_secs": int(os.environ.get("REPORT_INTERVAL_SECS", 43200)),
        "db_backend": DB_BACKEND,
        "init_db_on_startup": os.environ.get("INIT_DB_ON_STARTUP", "1"),
        "db_path": os.environ.get("DB_PATH", "data/polymarket_scanner.db") if DB_BACKEND != "postgres" else None
    })


@app.route("/api/markets/live")
def api_markets_live():
    """All markets with live data — full unfiltered feed for analysts."""
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    sort = request.args.get("sort", "volume")
    order = request.args.get("order", "desc")
    search = request.args.get("q", "")
    category = request.args.get("category", "")
    offset = (page - 1) * per_page

    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            allowed_sorts = {"volume": "snap.volume_usd", "yes": "snap.current_yes", "end_date": "m.end_time", "fetched_at": "snap.fetched_at"}
            sort_col = allowed_sorts.get(sort, "snap.volume_usd")
            sort_dir = "DESC" if order == "desc" else "ASC"
            conditions, params = [], []
            if search:
                conditions.append("(m.question ILIKE %s OR coalesce(m.category,'') ILIKE %s)")
                params.extend([f'%{search}%', f'%{search}%'])
            if category:
                conditions.append("coalesce(m.category,'') ILIKE %s")
                params.append(f'%{category}%')
            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            total = conn.execute(f"SELECT COUNT(*) as c FROM markets m {where}", params).fetchone()["c"]
            rows = conn.execute(f"""
                SELECT m.id, m.question, m.category, m.end_time as end_date,
                       snap.current_yes, snap.current_no, snap.volume_usd as volume, snap.liquidity_usd as liquidity,
                       snap.fetched_at, m.updated_at,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       (SELECT json_agg(c.channel_code ORDER BY c.channel_code) FROM market_signal_channels c WHERE c.market_signal_id = s.id) as primary_channels,
                       (SELECT json_agg(json_build_object('ticker_or_symbol',hi.ticker_or_symbol,'name',hi.name,'direction',hi.direction,'mechanism',hi.mechanism,'time_horizon',hi.time_horizon,'confidence',hi.confidence)) FROM market_signal_holding_impacts hi WHERE hi.market_signal_id = s.id) as affected_holdings
                FROM markets m
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.liquidity_usd, ms.fetched_at
                    FROM market_snapshots ms WHERE ms.market_id = m.id ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                {where}
                ORDER BY {sort_col} {sort_dir} NULLS LAST
                LIMIT %s OFFSET %s
            """, params + [per_page, offset]).fetchall()
            cats = conn.execute("SELECT DISTINCT category FROM markets WHERE coalesce(category,'') != '' ORDER BY category").fetchall()
        else:
            allowed_sorts = {"volume": "m.volume", "yes": "m.current_yes", "end_date": "m.end_date", "fetched_at": "m.fetched_at"}
            sort_col = allowed_sorts.get(sort, "m.volume")
            sort_dir = "DESC" if order == "desc" else "ASC"
            conditions, params = [], []
            if search:
                conditions.append("(m.question LIKE ? OR m.category LIKE ?)")
                params.extend([f'%{search}%', f'%{search}%'])
            if category:
                conditions.append("m.category LIKE ?")
                params.append(f'%{category}%')
            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            total = conn.execute(f"SELECT COUNT(*) as c FROM markets m {where}", params).fetchone()["c"]
            rows = conn.execute(f"""
                SELECT m.id, m.question, m.category, m.end_date,
                       m.current_yes, m.current_no, m.volume, m.liquidity,
                       m.fetched_at, m.updated_at,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.primary_channels, s.affected_holdings
                FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                {where}
                ORDER BY {sort_col} {sort_dir}
                LIMIT ? OFFSET ?
            """, params + [per_page, offset]).fetchall()
            cats = conn.execute("SELECT DISTINCT category FROM markets WHERE category != '' ORDER BY category").fetchall()

        return jsonify({
            "markets": [dict_from_row(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "categories": [r["category"] for r in cats if r["category"]]
        })
    finally:
        conn.close()


@app.route("/api/watchlist")
def api_watchlist():
    """Weekly odds movers — markets with biggest probability changes."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            rows = conn.execute("""
                SELECT m.id, m.question, m.category, m.end_time as end_date,
                       snap.current_yes, snap.current_no, snap.volume_usd as volume, snap.odds_history,
                       prev.prev_yes, prev.prev_fetched_at,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       (SELECT json_agg(c.channel_code ORDER BY c.channel_code) FROM market_signal_channels c WHERE c.market_signal_id = s.id) as primary_channels,
                       (SELECT json_agg(json_build_object('ticker_or_symbol',hi.ticker_or_symbol,'name',hi.name,'direction',hi.direction,'mechanism',hi.mechanism,'time_horizon',hi.time_horizon,'confidence',hi.confidence)) FROM market_signal_holding_impacts hi WHERE hi.market_signal_id = s.id) as affected_holdings
                FROM markets m
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.odds_history, ms.fetched_at
                    FROM market_snapshots ms WHERE ms.market_id = m.id
                    ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes as prev_yes, ms.fetched_at as prev_fetched_at
                    FROM market_snapshots ms WHERE ms.market_id = m.id
                    ORDER BY ms.fetched_at DESC OFFSET 1 LIMIT 1
                ) prev ON true
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                WHERE snap.current_yes IS NOT NULL
                ORDER BY COALESCE(snap.volume_usd,0) DESC
                LIMIT 700
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT m.id, m.question, m.category, m.end_date,
                       m.current_yes, m.current_no, m.volume, m.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.primary_channels, s.affected_holdings
                FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                WHERE m.odds_history IS NOT NULL AND m.odds_history != '[]'
                ORDER BY m.volume DESC
                LIMIT 500
            """).fetchall()

        movers = []
        all_candidates = []
        no_history_fallback = []
        for row in rows:
            d = dict_from_row(row)
            agenda = match_agenda_hints(d.get("question", ""), d.get("category", ""))
            d["agenda_watchlist_hits"] = [m.get("label") for m in (agenda.get("matches") or [])[:2]]
            d["agenda_tags"] = agenda.get("tags", [])
            d["agenda_score"] = agenda.get("agenda_score", 0)
            d["agenda_relevant"] = bool(agenda.get("is_agenda_relevant"))

            history = d.get("odds_history", [])
            if isinstance(history, str):
                try:
                    history = json.loads(history)
                except Exception:
                    history = []
            # Fallback for Postgres snapshots with empty/short odds_history: derive 2-point history from latest snapshots
            if (not isinstance(history, list) or len(history) < 2) and DB_BACKEND == "postgres":
                cur_yes = d.get("current_yes")
                prev_yes = d.get("prev_yes")
                if cur_yes is not None and prev_yes is not None:
                    history = [
                        {"t": d.get("prev_fetched_at") or "", "yes": float(prev_yes)},
                        {"t": d.get("fetched_at") or "", "yes": float(cur_yes)},
                    ]
                elif isinstance(history, list) and len(history) == 1 and cur_yes is not None:
                    history = [history[0], {"t": d.get("fetched_at") or "", "yes": float(cur_yes)}]

            if isinstance(history, list) and len(history) >= 2:
                try:
                    first = float(history[0].get("yes", 0.5))
                    last = float(history[-1].get("yes", 0.5))
                except Exception:
                    first = last = None
                if first is not None and last is not None:
                    delta = round((last - first) * 100, 1)
                    abs_delta = abs(delta)
                    d["delta_pp"] = delta
                    d["abs_delta"] = abs_delta
                    d["direction"] = "UP" if delta > 0 else "DOWN"
                    d["first_yes"] = round(first * 100, 1)
                    d["last_yes"] = round(last * 100, 1)
                    all_candidates.append(d)
                    if abs_delta >= 3:
                        movers.append(d)
                    continue

            # Last resort: keep a curated fallback list so endpoint doesn't return [] on fresh installs
            cy = d.get("current_yes")
            if cy is not None:
                try:
                    cy = float(cy)
                    pseudo = round(abs(cy - 0.5) * 100, 1)
                    d["delta_pp"] = round((cy - 0.5) * 100, 1)
                    d["abs_delta"] = pseudo
                    d["direction"] = "UP" if cy >= 0.5 else "DOWN"
                    d["first_yes"] = 50.0
                    d["last_yes"] = round(cy * 100, 1)
                    d["watchlist_mode"] = "fallback_probability_extremes"
                    no_history_fallback.append(d)
                except Exception:
                    pass

        sort_key = lambda x: (-(1 if x.get("agenda_relevant") else 0), -x.get("agenda_score", 0), -float(x.get("abs_delta") or 0), -float(x.get("volume") or 0))
        movers.sort(key=sort_key)
        if movers:
            return jsonify(movers[:60])

        all_candidates.sort(key=sort_key)
        if all_candidates:
            for x in all_candidates[:60]:
                x["watchlist_mode"] = x.get("watchlist_mode") or "micro_movers"
            return jsonify(all_candidates[:60])

        no_history_fallback.sort(key=sort_key)
        return jsonify(no_history_fallback[:60])
    finally:
        conn.close()


@app.route("/api/earnings")
def api_earnings():
    """
    Earnings-related prediction markets — company milestones, revenue targets,
    product launches and quarterly events that matter before earnings calls.
    """
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            rows = conn.execute("""
                SELECT m.id, m.question, m.category, m.end_time as end_date,
                       snap.current_yes, snap.current_no, snap.volume_usd as volume, snap.liquidity_usd as liquidity,
                       snap.fetched_at, snap.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       (SELECT json_agg(c.channel_code ORDER BY c.channel_code) FROM market_signal_channels c WHERE c.market_signal_id = s.id) as primary_channels,
                       (SELECT json_agg(json_build_object('ticker_or_symbol',hi.ticker_or_symbol,'name',hi.name,'direction',hi.direction,'mechanism',hi.mechanism,'time_horizon',hi.time_horizon,'confidence',hi.confidence)) FROM market_signal_holding_impacts hi WHERE hi.market_signal_id = s.id) as affected_holdings,
                       s.causal_chain
                FROM markets m
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.liquidity_usd, ms.fetched_at, ms.odds_history
                    FROM market_snapshots ms WHERE ms.market_id = m.id
                    ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                ORDER BY COALESCE(snap.volume_usd,0) DESC
                LIMIT 1000
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT m.id, m.question, m.category, m.end_date,
                       m.current_yes, m.current_no, m.volume, m.liquidity,
                       m.fetched_at, m.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.primary_channels, s.affected_holdings, s.causal_chain
                FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                ORDER BY m.volume DESC
                LIMIT 1000
            """).fetchall()

        earnings_keywords = [
            "earnings", "revenue", "eps", "profit", "quarterly", "q1", "q2", "q3", "q4",
            "annual report", "guidance", "deliveries", "shipments", "subscribers",
            "monthly active", "market cap", "ipo", "merger", "acquisition", "split",
            "dividend", "buyback", "largest company", "trillion", "billion market",
            "tesla deliver", "nvidia revenue", "apple sales", "microsoft azure",
            "google cloud", "amazon aws", "meta users", "tsmc revenue",
            "iphone", "product launch", "announce", "beat", "miss", "forecast",
            "backpack", "spacex", "anthropic ipo", "discord ipo", "reddit", "stripe"
        ]

        results = []
        for row in rows:
            d = dict_from_row(row)
            q = f"{d.get('question') or ''} {d.get('category') or ''}".lower()
            matched = [kw for kw in earnings_keywords if kw in q]
            if not matched:
                continue
            etype = "Company Milestone"
            if any(k in q for k in ["earnings","revenue","eps","quarterly","q1","q2","q3","q4","guidance","beat","miss"]):
                etype = "Earnings / Financials"
            elif any(k in q for k in ["ipo","merger","acquisition","split","buyback","dividend"]):
                etype = "Corporate Action"
            elif any(k in q for k in ["deliver","shipment","subscriber","monthly active","users"]):
                etype = "Operating Metrics"
            elif any(k in q for k in ["largest company","trillion","market cap"]):
                etype = "Market Cap Race"
            elif any(k in q for k in ["launch","announce","product","iphone","model"]):
                etype = "Product Launch"
            d["event_type_label"] = etype
            d["matched_keywords"] = matched[:4]
            try:
                end_raw = d.get("end_date", "")
                end = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
                d["days_to_expiry"] = (end - datetime.now(end.tzinfo or timezone.utc)).days
            except Exception:
                d["days_to_expiry"] = None
            results.append(d)

        results.sort(key=lambda x: (x["days_to_expiry"] is None, (x["days_to_expiry"] is not None and x["days_to_expiry"] < 0), (x["days_to_expiry"] if (x["days_to_expiry"] is not None and x["days_to_expiry"] >= 0) else 9999), -float(x.get("volume") or 0), abs(x["days_to_expiry"]) if x["days_to_expiry"] is not None else 999999))
        return jsonify(results[:80])
    finally:
        conn.close()



@app.route("/api/job/status")
def api_job_status():
    """Poll-able endpoint for live job progress feedback."""
    conn = get_connection()
    try:
        jobs = {}
        for job in ["ingestion", "analysis", "report"]:
            if DB_BACKEND == "postgres":
                row = conn.execute("""
                    SELECT job_name, status, message, started_at, completed_at
                    FROM job_runs
                    WHERE job_name = %s
                    ORDER BY started_at DESC LIMIT 1
                """, (job,)).fetchone()
            else:
                row = conn.execute("""
                    SELECT job_name, status, message, started_at, completed_at
                    FROM scheduler_log
                    WHERE job_name = ?
                    ORDER BY started_at DESC LIMIT 1
                """, (job,)).fetchone()
            if row:
                jobs[job] = dict(row)

        if DB_BACKEND == "postgres":
            markets = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
            analyzed = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true").fetchone()["c"]
            reports = conn.execute("SELECT COUNT(*) as c FROM reports").fetchone()["c"]
        else:
            markets = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
            analyzed = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
            reports = conn.execute("SELECT COUNT(*) as c FROM reports").fetchone()["c"]

        pending = max(markets - analyzed, 0)

        return jsonify({
            "jobs": jobs,
            "counts": {"markets": markets, "analyzed": analyzed, "pending": pending, "reports": reports}
        })
    finally:
        conn.close()


@app.route("/api/portfolio/signals")
def api_portfolio_signals():
    """Fund-level signal radar: under each fund, which bets/signals are impacting holdings and what they imply."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            rows = conn.execute("""
                WITH latest_snap AS (
                    SELECT ms.market_id, ms.current_yes, ms.volume_usd
                    FROM market_snapshots ms
                    JOIN (
                        SELECT market_id, MAX(fetched_at) AS max_fetched_at
                        FROM market_snapshots
                        GROUP BY market_id
                    ) t ON t.market_id = ms.market_id AND t.max_fetched_at = ms.fetched_at
                )
                SELECT
                    COALESCE(h.fund, 'Unmapped') AS fund,
                    COALESCE(h.ticker, hi.ticker_or_symbol) AS ticker,
                    COALESCE(h.name, hi.name, hi.ticker_or_symbol, 'Unknown') AS holding_name,
                    m.id AS market_id,
                    m.question,
                    m.category,
                    m.end_time,
                    s.relevance_label,
                    s.relevance_score,
                    s.one_sentence_verdict,
                    s.event_type,
                    hi.direction,
                    hi.mechanism,
                    hi.time_horizon,
                    hi.confidence,
                    ls.current_yes,
                    ls.volume_usd
                FROM market_signal_holding_impacts hi
                JOIN market_signals s
                    ON s.id = hi.market_signal_id
                   AND s.is_latest = true
                JOIN markets m
                    ON m.id = s.market_id
                LEFT JOIN portfolio_holdings h
                    ON h.id = hi.holding_id
                LEFT JOIN latest_snap ls
                    ON ls.market_id = m.id
                WHERE s.relevance_label IN ('ACTIONABLE', 'MONITOR')
                ORDER BY
                    COALESCE(h.fund, 'Unmapped'),
                    CASE WHEN s.relevance_label = 'ACTIONABLE' THEN 0 ELSE 1 END,
                    s.relevance_score DESC,
                    COALESCE(ls.volume_usd, 0) DESC
            """).fetchall()

            funds = defaultdict(lambda: {
                "fund": "",
                "actionable": 0,
                "monitor": 0,
                "signals": [],
                "tickers": set(),
                "score_sum": 0.0,
                "score_n": 0,
            })

            for row in rows:
                d = dict_from_row(row)
                fund = d.get("fund") or "Unmapped"
                g = funds[fund]
                g["fund"] = fund

                label = str(d.get("relevance_label") or "").upper()
                if label == "ACTIONABLE":
                    g["actionable"] += 1
                elif label == "MONITOR":
                    g["monitor"] += 1

                ticker = d.get("ticker")
                if ticker:
                    g["tickers"].add(str(ticker))

                score = d.get("relevance_score")
                if isinstance(score, (int, float)):
                    g["score_sum"] += float(score)
                    g["score_n"] += 1

                g["signals"].append({
                    "market_id": d.get("market_id"),
                    "question": d.get("question"),
                    "category": d.get("category"),
                    "end_time": d.get("end_time"),
                    "label": label,
                    "score": d.get("relevance_score"),
                    "verdict": d.get("one_sentence_verdict"),
                    "event_type": d.get("event_type"),
                    "direction": d.get("direction"),
                    "mechanism": d.get("mechanism"),
                    "time_horizon": d.get("time_horizon"),
                    "confidence": d.get("confidence"),
                    "ticker": d.get("ticker"),
                    "holding_name": d.get("holding_name"),
                    "current_yes": d.get("current_yes"),
                    "volume_usd": d.get("volume_usd"),
                })

            result = []
            for _, g in funds.items():
                g["signals"].sort(key=lambda x: (
                    0 if x.get("label") == "ACTIONABLE" else 1,
                    -(x.get("score") or 0),
                    -(x.get("volume_usd") or 0),
                ))
                result.append({
                    "fund": g["fund"],
                    "actionable": g["actionable"],
                    "monitor": g["monitor"],
                    "avg_score": round(g["score_sum"] / g["score_n"], 1) if g["score_n"] else None,
                    "active_tickers_implicated": sorted(g["tickers"]),
                    "signals": g["signals"][:20],
                })

            result.sort(key=lambda x: (
                -(x["actionable"]),
                -(x["monitor"]),
                -(x["avg_score"] or 0),
                x["fund"],
            ))
            return jsonify(result)

        portfolio_rows = conn.execute("""
            SELECT ticker, name, fund
            FROM portfolio_config
            WHERE is_active = 1
        """).fetchall()

        fund_map = {}
        for r in portfolio_rows:
            d = dict_from_row(r)
            ticker = (d.get("ticker") or "").upper().strip()
            if ticker:
                fund_map[ticker] = {
                    "fund": d.get("fund") or "Unmapped",
                    "name": d.get("name") or ticker,
                }

        signal_rows = conn.execute("""
            SELECT
                s.market_id,
                m.question,
                s.event_type,
                s.relevance_label,
                s.relevance_score,
                s.one_sentence_verdict,
                s.affected_holdings,
                m.current_yes,
                m.volume
            FROM signals s
            JOIN markets m ON m.id = s.market_id
            WHERE s.relevance_label IN ('ACTIONABLE', 'MONITOR')
            ORDER BY CASE WHEN s.relevance_label='ACTIONABLE' THEN 0 ELSE 1 END, s.relevance_score DESC
        """).fetchall()

        funds = defaultdict(lambda: {
            "fund": "",
            "actionable": 0,
            "monitor": 0,
            "signals": [],
            "tickers": set(),
            "score_sum": 0.0,
            "score_n": 0,
        })

        for r in signal_rows:
            d = dict_from_row(r)
            affected = d.get("affected_holdings") or []
            if isinstance(affected, str):
                try:
                    affected = json.loads(affected)
                except Exception:
                    affected = []
            if not isinstance(affected, list):
                affected = []

            for h in affected:
                h = h or {}
                ticker = (h.get("ticker") or "").upper().strip()
                if not ticker or ticker not in fund_map:
                    continue

                meta = fund_map[ticker]
                fund = meta["fund"]
                g = funds[fund]
                g["fund"] = fund

                label = str(d.get("relevance_label") or "").upper()
                if label == "ACTIONABLE":
                    g["actionable"] += 1
                elif label == "MONITOR":
                    g["monitor"] += 1

                g["tickers"].add(ticker)

                score = d.get("relevance_score")
                if isinstance(score, (int, float)):
                    g["score_sum"] += float(score)
                    g["score_n"] += 1

                g["signals"].append({
                    "market_id": d.get("market_id"),
                    "question": d.get("question"),
                    "label": label,
                    "score": d.get("relevance_score"),
                    "verdict": d.get("one_sentence_verdict"),
                    "event_type": d.get("event_type"),
                    "ticker": ticker,
                    "holding_name": meta["name"],
                    "current_yes": d.get("current_yes"),
                    "volume_usd": d.get("volume"),
                    "direction": h.get("direction"),
                    "mechanism": h.get("mechanism"),
                    "time_horizon": h.get("time_horizon"),
                    "confidence": h.get("confidence"),
                })

        result = []
        for _, g in funds.items():
            g["signals"].sort(key=lambda x: (
                0 if x.get("label") == "ACTIONABLE" else 1,
                -(x.get("score") or 0),
                -(x.get("volume_usd") or 0),
            ))
            result.append({
                "fund": g["fund"],
                "actionable": g["actionable"],
                "monitor": g["monitor"],
                "avg_score": round(g["score_sum"] / g["score_n"], 1) if g["score_n"] else None,
                "active_tickers_implicated": sorted(g["tickers"]),
                "signals": g["signals"][:20],
            })

        result.sort(key=lambda x: (
            -(x["actionable"]),
            -(x["monitor"]),
            -(x["avg_score"] or 0),
            x["fund"],
        ))
        return jsonify(result)

    finally:
        conn.close()


# ============================================================
# FRONTEND ROUTES
# ============================================================

@app.route("/")
@app.route("/<path:path>")
def serve_frontend(path=""):
    """Serve the frontend HTML."""
    frontend_path = Path(__file__).parent / "frontend" / "index.html"
    if frontend_path.exists():
        with open(frontend_path, "r") as f:
            return f.read(), 200, {"Content-Type": "text/html"}
    return "<h1>Frontend not found. Run the app from the project root.</h1>", 404


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--no-scheduler", action="store_true")
    args = parser.parse_args()
    
    print("=" * 60)
    print("  BIT Capital Polymarket Signal Scanner")
    print("=" * 60)
    print(f"  DB Backend: {DB_BACKEND} | {_mask_db_target()}")
    llm = _llm_status()
    if llm["configured"]:
        print(f"  LLM: ✓ {llm['label']} API")
    else:
        print("  LLM: ✗ Rule-based fallback (set GROQ_API_KEY or GEMINI_API_KEY or ANTHROPIC_API_KEY)")
    print(f"  URL: http://localhost:{args.port}")
    if DB_BACKEND == "postgres":
        print("  Actions: Postgres write paths enabled (manual + API-tested)")
    print("=" * 60)
    
    if not args.no_scheduler:
        startup()
    else:
        if os.environ.get("INIT_DB_ON_STARTUP", "1") not in {"0", "false", "False"}:
            init_db()
    
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
