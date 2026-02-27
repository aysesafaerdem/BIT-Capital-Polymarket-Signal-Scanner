"""
Signal Report Generator
Generates HTML + JSON analyst reports from analyzed Polymarket signals.
Uses LLM for executive summary and insight generation.
"""
import json
import os
import sys
from datetime import datetime, timezone
from html import escape as html_escape
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import get_connection, dict_from_row, DB_BACKEND
from backend.agenda_watchlist import match_agenda_hints, get_upcoming_catalysts
try:
    from backend.analysis import (
        BIT_INTELLIGENT_FILTER_ROUTING,
        _match_specific_macro_recipes,
        _match_holding_trigger_routes,
        retrieve_external_evidence_for_market,
    )
except Exception:
    BIT_INTELLIGENT_FILTER_ROUTING = {}
    _match_specific_macro_recipes = None
    _match_holding_trigger_routes = None
    retrieve_external_evidence_for_market = None

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import google.generativeai as google_generativeai
    GEMINI_AVAILABLE = True
except ImportError:
    google_generativeai = None
    GEMINI_AVAILABLE = False


REPORT_SYSTEM_PROMPT = """You are a senior portfolio analyst at BIT Capital, a tech-focused investment fund.
Your job is to write a concise, insightful signal report for portfolio managers based on Polymarket prediction market data.

The report should:
1. Identify the top 3-5 most actionable signals for BIT Capital's portfolio
2. Explain what prediction markets are currently pricing in for each major theme (crypto, semis, rates, geopolitics)
3. Call out any divergences between prediction market odds and consensus views
4. Provide clear portfolio implications with specific ticker mentions
5. Flag risks or tail scenarios to watch

Write in clear, professional language. Be specific with numbers and probabilities.
Lead with the most important insight. Avoid hedging everything. Make calls.

Format as structured HTML with sections: Executive Summary, Top Signals, Theme Breakdown, Portfolio Implications, Risks to Watch.
Use <h2>, <h3>, <p>, <ul>, <li>, <strong>, <span class="bull">↑ bullish</span>, <span class="bear">↓ bearish</span> tags only."""


def build_analyst_action_center(signals: list[dict], stats: dict) -> str:
    """Deterministic insight block shown at the top of every report."""
    actionable = [s for s in signals if s.get("relevance_label") == "ACTIONABLE"]
    scoped = actionable if actionable else signals
    scoped = sorted(scoped, key=lambda s: ((s.get("relevance_score") or 0), (s.get("volume") or 0)), reverse=True)
    top = scoped[:8]

    def parse_list(v):
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                j = json.loads(v)
                return j if isinstance(j, list) else []
            except Exception:
                return []
        return []

    def infer_family(q: str) -> str:
        t = (q or "").lower()
        if any(k in t for k in ["fed", "fomc", "rate", "cpi", "pce", "yield", "dot plot"]):
            return "Rates / Macro"
        if any(k in t for k in ["iran", "israel", "war", "strike", "sanction", "opec", "oil", "hormuz"]):
            return "Geopolitics / Energy"
        if any(k in t for k in ["bitcoin", "ethereum", "crypto", "btc", "eth", "stablecoin", "coinbase"]):
            return "Crypto"
        if any(k in t for k in ["nvidia", "tsmc", "semiconductor", "ai model", "gpu", "hbm", "export control"]):
            return "AI / Semis"
        if any(k in t for k in ["election", "president", "senate", "party"]):
            return "Politics / Policy"
        return "Cross-Asset"

    def action_for_signal(yes_pct: int, family: str) -> str:
        if yes_pct >= 65:
            return f"Price in continuation risk for {family}; tighten downside hedges and bias toward beneficiaries."
        if yes_pct <= 20:
            return f"Market assigns low probability; keep small tactical exposure and wait for probability re-pricing."
        return f"Binary zone; run scenario sizing and execute only on confirmation (+/-10pp move)."

    family_counts = {}
    channel_counts = {}
    for s in scoped:
        fam = infer_family(s.get("question", ""))
        family_counts[fam] = family_counts.get(fam, 0) + 1
        for ch in parse_list(s.get("primary_channels"))[:2]:
            chs = str(ch)
            channel_counts[chs] = channel_counts.get(chs, 0) + 1

    family_rows = sorted(family_counts.items(), key=lambda kv: kv[1], reverse=True)[:6]
    max_family = max([c for _, c in family_rows], default=1)
    family_chart = "".join(
        f"""
<div style="display:grid;grid-template-columns:180px 1fr 42px;gap:8px;align-items:center;margin:6px 0">
  <div style="font-size:12px;color:#d1d5db">{html_escape(name)}</div>
  <div style="height:8px;background:#1f2937;border-radius:999px;overflow:hidden">
    <div style="height:100%;width:{int((count/max_family)*100)}%;background:linear-gradient(90deg,#0ea5e9,#22c55e)"></div>
  </div>
  <div style="font-family:monospace;font-size:11px;color:#94a3b8;text-align:right">{count}</div>
</div>
"""
        for name, count in family_rows
    )

    top_rows = []
    for s in top:
        q = html_escape((s.get("question") or "")[:120])
        yes_pct = round(float(s.get("current_yes", 0.5)) * 100)
        score = int(s.get("relevance_score", 0) or 0)
        holdings = parse_list(s.get("affected_holdings"))
        impacted = [h.get("ticker_or_symbol") or h.get("name") for h in holdings if isinstance(h, dict)]
        impacted = [x for x in impacted if x][:3]
        fam = infer_family(s.get("question", ""))
        action_text = action_for_signal(yes_pct, fam)
        top_rows.append(
            f"""
<tr>
  <td style="padding:8px;border-bottom:1px solid #1f2937;vertical-align:top">
    <div style="font-weight:600;color:#e5e7eb">{q}</div>
    <div style="margin-top:4px;font-size:11px;color:#94a3b8">{fam}</div>
  </td>
  <td style="padding:8px;border-bottom:1px solid #1f2937;font-family:monospace;color:#e5e7eb">{yes_pct}%</td>
  <td style="padding:8px;border-bottom:1px solid #1f2937;font-family:monospace;color:#e5e7eb">{score}</td>
  <td style="padding:8px;border-bottom:1px solid #1f2937;color:#cbd5e1">{html_escape(', '.join(impacted) if impacted else 'Watchlist basket')}</td>
  <td style="padding:8px;border-bottom:1px solid #1f2937;color:#cbd5e1">{html_escape(action_text)}</td>
</tr>
"""
        )

    top_channel = sorted(channel_counts.items(), key=lambda kv: kv[1], reverse=True)[:4]
    channel_line = ", ".join([f"{k} ({v})" for k, v in top_channel]) if top_channel else "n/a"

    return f"""
<h2>Analyst Action Center</h2>
<p><strong>Current posture:</strong> {stats.get('actionable', 0)} ACTIONABLE / {stats.get('monitor', 0)} MONITOR from {stats.get('total_markets', 0)} markets. <strong>Primary routing channels:</strong> {html_escape(channel_line)}.</p>

<h3>Signal Concentration (by Event Family)</h3>
<div style="padding:10px 12px;border:1px solid #1f2937;border-radius:8px;background:rgba(15,23,42,.45)">
{family_chart if family_chart else '<div style="font-size:12px;color:#94a3b8">No concentrated family detected.</div>'}
</div>

<h3>Top Actionable Signals and Recommended Move</h3>
<table style="width:100%;border-collapse:collapse;font-size:12px">
  <thead>
    <tr>
      <th style="text-align:left;padding:8px;border-bottom:1px solid #334155;color:#94a3b8">Market</th>
      <th style="text-align:left;padding:8px;border-bottom:1px solid #334155;color:#94a3b8">YES</th>
      <th style="text-align:left;padding:8px;border-bottom:1px solid #334155;color:#94a3b8">Score</th>
      <th style="text-align:left;padding:8px;border-bottom:1px solid #334155;color:#94a3b8">Impacted</th>
      <th style="text-align:left;padding:8px;border-bottom:1px solid #334155;color:#94a3b8">Analyst Play</th>
    </tr>
  </thead>
  <tbody>
    {''.join(top_rows) if top_rows else '<tr><td colspan="5" style="padding:10px;color:#94a3b8">No actionable signals in current run.</td></tr>'}
  </tbody>
</table>

<h3>Execution Playbook (Next 24h)</h3>
<ul>
  <li><strong>Trigger ladder:</strong> +10pp odds move = escalate to PM call; +15pp with volume acceleration = tactical position change.</li>
  <li><strong>Invalidation:</strong> -10pp move or policy headline reversal = cut conviction and revert to neutral sizing.</li>
  <li><strong>Risk control:</strong> Avoid stacking correlated bets across the same macro family in one rebalance window.</li>
  <li><strong>Monitoring cadence:</strong> Check odds/volume drift at least every 2-4 hours around catalyst windows.</li>
</ul>
"""


def get_top_signals(limit: int = 20) -> list[dict]:
    """Fetch top signals (ACTIONABLE + MONITOR) for report generation."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            rows = conn.execute("""
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
                       s.what_to_watch_next, s.red_flags_or_unknowns, s.analyzed_at,
                       s.id as market_signal_id
                FROM market_signals s
                JOIN markets m ON s.market_id = m.id
                LEFT JOIN LATERAL (
                    SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.odds_history
                    FROM market_snapshots ms WHERE ms.market_id = m.id ORDER BY ms.fetched_at DESC LIMIT 1
                ) snap ON true
                WHERE s.is_latest = true AND s.relevance_label IN ('ACTIONABLE', 'MONITOR')
                ORDER BY s.relevance_score DESC, s.analyzed_at DESC, COALESCE(snap.volume_usd,0) DESC
                LIMIT %s
            """, (limit,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT m.id, m.question, m.current_yes, m.current_no, m.volume,
                       m.category, m.end_date, m.odds_history,
                       s.relevance_label, s.relevance_score, s.one_sentence_verdict,
                       s.event_type, s.primary_channels, s.affected_holdings,
                       s.causal_chain, s.portfolio_theme_fit, s.trigger_keywords,
                       s.what_to_watch_next, s.red_flags_or_unknowns, s.analyzed_at
                FROM signals s
                JOIN markets m ON s.market_id = m.id
                WHERE s.relevance_label IN ('ACTIONABLE', 'MONITOR')
                ORDER BY s.relevance_score DESC, s.analyzed_at DESC, COALESCE(m.volume,0) DESC
                LIMIT ?
            """, (limit,)).fetchall()
        out = [dict_from_row(r) for r in rows]
        for s in out:
            s["agenda_hints"] = match_agenda_hints(s.get("question", ""), s.get("category", ""))
        return out
    finally:
        conn.close()


def get_summary_stats() -> dict:
    """Get summary statistics for the report header."""
    conn = get_connection()
    try:
        total_markets = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
        if DB_BACKEND == "postgres":
            total_signals = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true").fetchone()["c"]
            actionable = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true AND relevance_label='ACTIONABLE'").fetchone()["c"]
            monitor = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true AND relevance_label='MONITOR'").fetchone()["c"]
            ignore = conn.execute("SELECT COUNT(*) as c FROM market_signals WHERE is_latest = true AND relevance_label='IGNORE'").fetchone()["c"]
        else:
            total_signals = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
            actionable = conn.execute("SELECT COUNT(*) as c FROM signals WHERE relevance_label='ACTIONABLE'").fetchone()["c"]
            monitor = conn.execute("SELECT COUNT(*) as c FROM signals WHERE relevance_label='MONITOR'").fetchone()["c"]
            ignore = conn.execute("SELECT COUNT(*) as c FROM signals WHERE relevance_label='IGNORE'").fetchone()["c"]
        return {
            "total_markets": total_markets,
            "total_signals": total_signals,
            "actionable": actionable,
            "monitor": monitor,
            "ignore": ignore,
            "filter_rate": round((total_signals - actionable - monitor) / max(total_signals, 1) * 100, 1)
        }
    finally:
        conn.close()


def generate_llm_report_body(signals: list[dict], stats: dict) -> str:
    """Use LLM to generate the main report body."""
    if not ((GEMINI_AVAILABLE and GEMINI_API_KEY) or (ANTHROPIC_AVAILABLE and ANTHROPIC_API_KEY)):
        return generate_rule_based_report_body(signals, stats)
    
    # Prepare signal summary for LLM
    signal_summaries = []
    report_evidence_pack = []
    for s in signals[:10]:  # Top 10 for LLM
        holdings = s.get("affected_holdings", [])
        if isinstance(holdings, str):
            try:
                holdings = json.loads(holdings)
            except:
                holdings = []
        
        affected = ", ".join([f"{h.get('ticker_or_symbol', '')} ({h.get('direction', '')})" 
                             for h in holdings[:3]])
        
        signal_summaries.append({
            "question": s.get("question", ""),
            "probability_yes": f"{round(s.get('current_yes', 0.5) * 100)}%",
            "label": s.get("relevance_label", ""),
            "score": s.get("relevance_score", 0),
            "verdict": s.get("one_sentence_verdict", ""),
            "affected_holdings": affected,
            "channels": s.get("primary_channels", []),
            "volume_usd": f"${s.get('volume', 0):,.0f}",
            "trigger_keywords": (s.get("trigger_keywords", []) or [])[:8],
        })

        if retrieve_external_evidence_for_market is not None and len(report_evidence_pack) < 16:
            try:
                evs = retrieve_external_evidence_for_market(s, max_items=2) or []
                for ev in evs:
                    report_evidence_pack.append({
                        "market_question": s.get("question", "")[:160],
                        "headline": ev.get("headline"),
                        "snippet": ev.get("snippet"),
                        "source": ev.get("source"),
                        "published_at": ev.get("published_at"),
                        "url": ev.get("url"),
                    })
            except Exception:
                pass
    
    user_msg = f"""Generate an analyst signal report for BIT Capital's portfolio managers.

Date (UTC): {datetime.now(timezone.utc).strftime('%B %d, %Y')}

SIGNAL STATISTICS:
- Total Polymarket markets scanned: {stats['total_markets']}
- Markets analyzed: {stats['total_signals']}
- ACTIONABLE signals: {stats['actionable']}
- MONITOR signals: {stats['monitor']}
- Filtered out as irrelevant: {stats['ignore']}

TOP SIGNALS TO COVER:
{json.dumps(signal_summaries, indent=2)}

AGENDA WATCHLIST CONTEXT (soft hints; use only if signal text explicitly matches):
{json.dumps(get_upcoming_catalysts(), indent=2)}

EXTERNAL NEWS / EVIDENCE SNIPPETS (best-effort; use only when directly relevant to the market and not contradictory to resolution rules):
{json.dumps(report_evidence_pack[:16], indent=2)}

Write a professional analyst report covering these signals and their implications for BIT Capital's portfolio.
Focus on NVDA, TSMC, MU, IREN, HUT, RIOT, HOOD, GOOGL, META, PANW, LMND and other key holdings.
Be specific about probability changes and what they imply. Make this actionable."""
    
    try:
        if GEMINI_AVAILABLE and GEMINI_API_KEY:
            google_generativeai.configure(api_key=GEMINI_API_KEY)
            model = google_generativeai.GenerativeModel(
                GEMINI_MODEL,
                system_instruction=REPORT_SYSTEM_PROMPT,
            )
            response = model.generate_content(
                user_msg,
                generation_config={"temperature": 0.2},
            )
            text = getattr(response, "text", "") or ""
            if text:
                return text

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=3000,
            system=REPORT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}]
        )
        return response.content[0].text
    except Exception as e:
        print(f"[REPORT] LLM report generation failed: {e}")
        return generate_rule_based_report_body(signals, stats)


def generate_rule_based_report_body(signals: list[dict], stats: dict) -> str:
    """Generate a structured HTML report without LLM."""
    actionable = [s for s in signals if s.get("relevance_label") == "ACTIONABLE"]
    monitor = [s for s in signals if s.get("relevance_label") == "MONITOR"]
    
    # Group by channel
    channel_groups = {}
    for s in signals:
        channels = s.get("primary_channels", [])
        if isinstance(channels, str):
            try:
                channels = json.loads(channels)
            except:
                channels = []
        for ch in channels[:1]:  # Primary channel only
            if ch not in channel_groups:
                channel_groups[ch] = []
            channel_groups[ch].append(s)
    
    # Build affected tickers summary
    all_tickers = {}
    ticker_mechanisms = {}
    for s in actionable:
        holdings = s.get("affected_holdings", [])
        if isinstance(holdings, str):
            try:
                holdings = json.loads(holdings)
            except:
                holdings = []
        for h in holdings:
            ticker = h.get("ticker_or_symbol", "")
            if ticker:
                if ticker not in all_tickers:
                    all_tickers[ticker] = {"up": 0, "down": 0, "mixed": 0}
                    ticker_mechanisms[ticker] = []
                d = h.get("direction", "MIXED").upper()
                if d == "UP":
                    all_tickers[ticker]["up"] += 1
                elif d == "DOWN":
                    all_tickers[ticker]["down"] += 1
                else:
                    all_tickers[ticker]["mixed"] += 1
                mech = (h.get("mechanism") or "").strip()
                if mech and mech not in ticker_mechanisms[ticker]:
                    ticker_mechanisms[ticker].append(mech)

    # Recipe and holding-trigger routing summaries from signal text (for fallback-enhanced explainability)
    recipe_hits = {}
    routing_hits = {}
    for s in signals:
        text_blob = " ".join([
            str(s.get("question", "") or ""),
            str(s.get("one_sentence_verdict", "") or ""),
            json.dumps(s.get("trigger_keywords", []) if not isinstance(s.get("trigger_keywords"), str) else s.get("trigger_keywords")),
        ]).lower()

        if _match_specific_macro_recipes:
            try:
                for rm in (_match_specific_macro_recipes(text_blob) or [])[:2]:
                    recipe_hits.setdefault(rm.get("recipe", "unknown_recipe"), {"count": 0, "examples": [], "hit_terms": set()})
                    recipe_hits[rm["recipe"]]["count"] += 1
                    recipe_hits[rm["recipe"]]["hit_terms"].update(rm.get("hits", []))
                    if len(recipe_hits[rm["recipe"]]["examples"]) < 2:
                        recipe_hits[rm["recipe"]]["examples"].append(s.get("question", ""))
            except Exception:
                pass

        if _match_holding_trigger_routes:
            try:
                for hm in (_match_holding_trigger_routes(text_blob) or [])[:4]:
                    t = hm.get("ticker", "")
                    if not t:
                        continue
                    routing_hits.setdefault(t, {"count": 0, "alias_hits": set(), "trigger_hits": set()})
                    routing_hits[t]["count"] += 1
                    routing_hits[t]["alias_hits"].update(hm.get("alias_hits", []))
                    routing_hits[t]["trigger_hits"].update(hm.get("trigger_hits", []))
            except Exception:
                pass
    
    # Executive summary
    html = f"""
<h2>Executive Summary</h2>
<p>BIT Capital Polymarket Signal Scanner has processed <strong>{stats['total_markets']}</strong> active prediction 
markets. Of these, <strong>{stats['actionable']}</strong> are classified as <strong>ACTIONABLE</strong> and 
<strong>{stats['monitor']}</strong> as <strong>MONITOR</strong> — representing a signal/noise filter rate of 
<strong>{stats['filter_rate']}%</strong>.</p>

<p>Key transmission channels currently driving signal relevance: <strong>{', '.join(list(channel_groups.keys())[:4]) or 'mixed channels'}</strong>. 
The current signal set is most sensitive to portfolio transmission via rates/duration, crypto liquidity, AI/semiconductor supply chain, and policy/regulatory catalysts.</p>
"""

    # Current macro/policy catalysts in focus (recipe-driven)
    if recipe_hits:
        pretty_recipe = {
            "iran_nuclear_sanctions_oil": "US-Iran Nuclear Talks / Sanctions / Oil Risk Premium",
            "tariff_turmoil_trade_volatility": "Tariff Turmoil / Trade Volatility",
            "fomc_march_2026_sep_dot_plot": "March FOMC / SEP / Dot Plot",
            "us_crypto_market_structure_bill": "US Crypto Market Structure Legislation / Stablecoin Rules",
        }
        html += "<h2>Current Macro & Policy Catalysts in Focus</h2>\n"
        html += "<ul>\n"
        for key, meta in sorted(recipe_hits.items(), key=lambda kv: -kv[1]["count"])[:6]:
            terms = ", ".join(list(meta["hit_terms"])[:6])
            ex = " | ".join(meta["examples"][:2])
            html += f"<li><strong>{pretty_recipe.get(key, key.replace('_',' ').title())}</strong> — matched in <strong>{meta['count']}</strong> signal(s). "
            html += f"Observed trigger phrases: {terms or 'n/a'}."
            if ex:
                html += f"<br><strong>Example markets:</strong> {ex}"
            html += "</li>\n"
        html += "</ul>\n"
    
    # Top actionable signals
    if actionable:
        html += "<h2>Top Actionable Signals</h2>\n"
        for s in actionable[:5]:
            holdings = s.get("affected_holdings", [])
            if isinstance(holdings, str):
                try:
                    holdings = json.loads(holdings)
                except:
                    holdings = []
            
            affected_str = " | ".join([
                f"<strong>{h.get('ticker_or_symbol','')}</strong> "
                f"<span class=\"{'bull' if h.get('direction')=='UP' else 'bear' if h.get('direction')=='DOWN' else 'neutral'}\">"
                f"{'↑' if h.get('direction')=='UP' else '↓' if h.get('direction')=='DOWN' else '↔'} {h.get('direction','')}</span>"
                for h in holdings[:3]
            ])
            
            yes_pct = round(s.get("current_yes", 0.5) * 100)
            
            chain = s.get("causal_chain", [])
            if isinstance(chain, str):
                try:
                    chain = json.loads(chain)
                except Exception:
                    chain = []
            trigger_keywords = s.get("trigger_keywords", [])
            if isinstance(trigger_keywords, str):
                try:
                    trigger_keywords = json.loads(trigger_keywords)
                except Exception:
                    trigger_keywords = []
            chain_summary = ""
            if chain:
                chain_parts = [
                    f"{c.get('from','')} to {c.get('to','')}"
                    for c in chain[:3]
                    if isinstance(c, dict)
                ]
                if chain_parts:
                    chain_summary = " \u2192 ".join(chain_parts)
            html += f"""
<div style="border-left: 3px solid #ef4444; padding: 12px; margin: 12px 0; background: rgba(239,68,68,0.05); border-radius: 4px;">
    <h3>{s.get('question', '')[:100]}</h3>
    <p><strong>Implied probability:</strong> {yes_pct}% YES | <strong>Score:</strong> {s.get('relevance_score', 0)}/100</p>
    <p>{s.get('one_sentence_verdict', '')}</p>
    <p><strong>Portfolio impact:</strong> {affected_str}</p>
    {f"<p><strong>Trigger keywords:</strong> {', '.join(trigger_keywords[:10])}</p>" if trigger_keywords else ""}
    {f"<p><strong>Causal transmission:</strong> {chain_summary}</p>" if chain_summary else ""}
</div>
"""
    
    # Monitor signals
    if monitor:
        html += "<h2>Monitor Signals</h2>\n<ul>\n"
        for s in monitor[:5]:
            yes_pct = round(s.get("current_yes", 0.5) * 100)
            html += f"<li><strong>{s.get('question','')[:90]}...</strong> — {yes_pct}% YES | {s.get('one_sentence_verdict','')}</li>\n"
        html += "</ul>\n"
    
    # Agenda-aligned watchlist (soft phrase/entity matches only)
    agenda_bucket = {}
    for s in signals:
        ah = s.get("agenda_hints", {}) or {}
        for m in (ah.get("matches") or [])[:2]:
            agenda_bucket.setdefault(m.get("label", "Agenda Catalyst"), []).append((s, m))

    if agenda_bucket:
        html += "<h2>Agenda-Aligned Watchlist</h2>\n"
        html += "<p>Signals aligned to the current BIT-relevant macro/geopolitical agenda using explicit phrase/entity matches (soft hints, not proof).</p>\n"
        ranked = sorted(agenda_bucket.items(), key=lambda kv: -max((pm[1].get("priority", 0) for pm in kv[1]), default=0))
        for label, pairs in ranked[:6]:
            html += f"<h3>{label}</h3>\n<ul>\n"
            for s, m in pairs[:3]:
                yes_pct = round(float(s.get("current_yes", 0.5)) * 100)
                html += f"<li><strong>{yes_pct}% YES</strong> — {s.get('question','')[:120]}<br><strong>Matched:</strong> {', ' .join(m.get('hits', [])[:3])}<br><strong>Transmission:</strong> {m.get('transmission', '')}</li>\n"
            html += "</ul>\n"

    # Theme breakdown
    html += "<h2>Theme Breakdown</h2>\n"
    
    theme_names = {
        "crypto": "Crypto & Digital Assets",
        "ai_semis": "AI & Semiconductors", 
        "rates": "Macro & Rates",
        "oil": "Energy & Oil",
        "cyber": "Cybersecurity",
        "fintech": "Fintech & Regulation",
        "geopolitics": "Geopolitics",
        "rates_duration": "Rates Duration",
        "crypto_liquidity": "Crypto Liquidity",
        "equities_factor": "Equity Factor Rotation",
        "equities_risk_on_off": "Equity Risk Regime",
        "commodities_energy": "Energy Commodities",
        "inflation_expectations": "Inflation Expectations",
    }
    
    for channel, ch_signals in channel_groups.items():
        if not ch_signals:
            continue
        theme_name = theme_names.get(channel, channel.replace("_", " ").title())
        html += f"<h3>{theme_name}</h3>\n"
        for s in ch_signals[:3]:
            yes_pct = round(s.get("current_yes", 0.5) * 100)
            label = s.get("relevance_label", "")
            label_text = "ACTIONABLE" if label == "ACTIONABLE" else "MONITOR" if label == "MONITOR" else "IGNORE"
            html += f"<p><strong>{label_text}</strong> | <strong>{yes_pct}%</strong> — {s.get('question','')[:100]}</p>\n"

    # Holding-level trigger routing map (which stock is linked to what catalysts)
    if routing_hits:
        html += "<h2>Holding-Level Trigger Routing Map</h2>\n"
        html += "<p>The following holdings are repeatedly linked to explicit alias/trigger keyword matches in the current signal set, improving routing confidence for relevance scoring.</p>\n"
        html += "<ul>\n"
        for ticker, meta in sorted(routing_hits.items(), key=lambda kv: -kv[1]["count"])[:12]:
            aliases = ", ".join(list(meta["alias_hits"])[:3]) or ticker
            triggers = ", ".join(list(meta["trigger_hits"])[:6]) or "keyword overlap"
            html += f"<li><strong>{ticker}</strong> — matched in <strong>{meta['count']}</strong> signal(s). "
            html += f"<strong>Aliases observed:</strong> {aliases}. "
            html += f"<strong>Catalysts observed:</strong> {triggers}.</li>\n"
        html += "</ul>\n"
    
    # Portfolio implications
    if all_tickers:
        html += "<h2>Portfolio Implications</h2>\n"
        html += "<p>Based on ACTIONABLE signals, the following holdings show elevated signal activity:</p>\n<ul>\n"
        for ticker, counts in sorted(all_tickers.items(), key=lambda x: -(x[1]["up"] + x[1]["down"] + x[1]["mixed"]))[:8]:
            direction = "↑ Bullish bias" if counts["up"] > counts["down"] else "↓ Bearish bias" if counts["down"] > counts["up"] else "↔ Mixed signals"
            mechs = ticker_mechanisms.get(ticker, [])
            mechanism_line = f" <br><strong>Why it is affected:</strong> {' | '.join(mechs[:2])}" if mechs else ""
            html += f"<li><strong>{ticker}</strong>: {direction} ({counts['up']} bullish, {counts['down']} bearish, {counts['mixed']} mixed signals){mechanism_line}</li>\n"
        html += "</ul>\n"
    
    # Upcoming catalysts
    html += "<h2>Upcoming Catalyst Calendar</h2>\n<ul>\n"
    for c in get_upcoming_catalysts():
        html += f"<li><strong>{c.get('label')}</strong> — {c.get('window')} | tags: {', '.join(c.get('tags', []))}</li>\n"
    html += "</ul>\n"

    # Risks
    html += """<h2>Risks to Watch</h2>
<ul>
<li>Prediction market odds can move rapidly on news — monitor ACTIONABLE signals for ±10pp 24h moves</li>
<li>Low liquidity markets may not reflect true consensus — prioritize signals with >$500k volume</li>
<li>Correlation risk: multiple signals may be driven by the same macro factor (e.g., rates affect all growth)</li>
</ul>
"""
    
    return html


def generate_full_report() -> dict:
    """Generate and store a complete signal report."""
    print("[REPORT] Generating signal report...")
    
    signals = get_top_signals(limit=20)
    stats = get_summary_stats()
    
    if not signals:
        print("[REPORT] No signals to report on yet. Run analysis first.")
        return {"status": "no_signals"}
    
    # Generate report body
    action_center_html = build_analyst_action_center(signals, stats)
    report_html_body = action_center_html + "\n" + generate_llm_report_body(signals, stats)
    
    # Wrap in full HTML template
    now = datetime.now(timezone.utc)
    report_date = now.strftime("%Y-%m-%d")
    report_title = f"BIT Capital Signal Report — {now.strftime('%B %d, %Y')} UTC"
    
    top_signal_ids = [s["id"] for s in signals[:5]]
    
    # Full report JSON for API consumption
    report_json = {
        "generated_at": now.isoformat(),
        "stats": stats,
        "actionable_signals": [
            {
                "question": s["question"],
                "probability_yes": s.get("current_yes", 0.5),
                "score": s.get("relevance_score", 0),
                "verdict": s.get("one_sentence_verdict", ""),
                "affected_holdings": s.get("affected_holdings", []),
                "channels": s.get("primary_channels", [])
            }
            for s in signals if s.get("relevance_label") == "ACTIONABLE"
        ],
        "monitor_signals": [
            {
                "question": s["question"],
                "probability_yes": s.get("current_yes", 0.5),
                "score": s.get("relevance_score", 0),
                "verdict": s.get("one_sentence_verdict", "")
            }
            for s in signals if s.get("relevance_label") == "MONITOR"
        ]
    }
    
    # Store in database
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            cur = conn.execute("""
                INSERT INTO reports
                    (report_date, title, executive_summary, actionable_count, monitor_count,
                     ignore_count, full_report_html, full_report_json, generated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                RETURNING id
            """, (
                report_date,
                report_title,
                f"{stats['actionable']} ACTIONABLE, {stats['monitor']} MONITOR signals from {stats['total_markets']} markets scanned.",
                stats["actionable"], stats["monitor"], stats["ignore"],
                report_html_body, json.dumps(report_json), now.isoformat()
            ))
            report_id = cur.fetchone()["id"]
            for rank, s in enumerate(signals[:5], start=1):
                signal_id = s.get("market_signal_id")
                if not signal_id:
                    row = conn.execute("SELECT id FROM market_signals WHERE market_id = %s AND is_latest = true", (s["id"],)).fetchone()
                    signal_id = row["id"] if row else None
                if signal_id:
                    conn.execute(
                        "INSERT INTO report_items (report_id, market_signal_id, section, rank) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (report_id, signal_id, "top_signals", rank)
                    )
        else:
            cur = conn.execute("""
                INSERT INTO reports
                    (report_date, title, executive_summary, actionable_count, monitor_count,
                     ignore_count, top_signals, full_report_html, full_report_json, generated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                report_date,
                report_title,
                f"{stats['actionable']} ACTIONABLE, {stats['monitor']} MONITOR signals from {stats['total_markets']} markets scanned.",
                stats["actionable"], stats["monitor"], stats["ignore"],
                json.dumps(top_signal_ids), report_html_body, json.dumps(report_json), now.isoformat()
            ))
            report_id = cur.lastrowid
        conn.commit()
        print(f"[REPORT] Report #{report_id} saved to database.")
        return {"status": "success", "report_id": report_id, "stats": stats}
    finally:
        conn.close()


if __name__ == "__main__":
    from database.db import init_db
    init_db()
    result = generate_full_report()
    print(f"Report generation result: {result}")
