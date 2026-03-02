"""
Polymarket Data Ingestion Pipeline
Fetches active markets from Polymarket's Gamma API and stores them in the database.
Runs as a scheduled job.
"""
import requests
import json
import sqlite3
from datetime import datetime, timezone
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import get_connection, DB_BACKEND

POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

FETCH_LIMIT = 200  # markets per fetch
ACTIVE_ONLY = True


def _json_loads_safe(v, default):
    if v is None:
        return default
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return default
    return default

def _null_if_blank(v):
    if isinstance(v, str) and not v.strip():
        return None
    return v


def fetch_markets(limit: int = FETCH_LIMIT, offset: int = 0) -> list[dict]:
    """Fetch active markets from Polymarket Gamma API."""
    params = {
        "active": "true",
        "closed": "false",
        "limit": limit,
        "offset": offset,
        "order": "volume",
        "ascending": "false"
    }
    try:
        resp = requests.get(
            f"{POLYMARKET_GAMMA_API}/markets",
            params=params,
            timeout=30,
            headers={"Accept": "application/json"}
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "data" in data:
            return data["data"]
        return []
    except Exception as e:
        print(f"[INGESTION] Error fetching markets: {e}")
        return []


def fetch_market_detail(condition_id: str) -> dict:
    """Fetch detailed odds history for a specific market."""
    try:
        resp = requests.get(
            f"{POLYMARKET_GAMMA_API}/markets/{condition_id}",
            timeout=15,
            headers={"Accept": "application/json"}
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        pass
    return {}


def parse_market(raw: dict) -> dict:
    """Parse a raw Polymarket market object into our schema."""
    # Extract outcomes/prices
    outcomes = []
    prices = {}
    
    try:
        outcomes_raw = raw.get("outcomes", "[]")
        if isinstance(outcomes_raw, str):
            outcomes = json.loads(outcomes_raw)
        elif isinstance(outcomes_raw, list):
            outcomes = outcomes_raw
    except:
        outcomes = ["YES", "NO"]
    
    try:
        prices_raw = raw.get("outcomePrices", raw.get("outcome_prices", "[]"))
        if isinstance(prices_raw, str):
            price_list = json.loads(prices_raw)
        elif isinstance(prices_raw, list):
            price_list = prices_raw
        else:
            price_list = []
        
        for i, outcome in enumerate(outcomes):
            if i < len(price_list):
                prices[outcome] = float(price_list[i]) if price_list[i] else 0.5
    except:
        prices = {"YES": 0.5, "NO": 0.5}
    
    # Current odds
    current_yes = prices.get("YES", prices.get(outcomes[0] if outcomes else "YES", 0.5))
    current_no = prices.get("NO", 1.0 - current_yes)
    
    # Volume and liquidity
    volume = float(raw.get("volume", raw.get("volume24hr", 0)) or 0)
    liquidity = float(raw.get("liquidity", 0) or 0)
    
    # Subcategories / tags
    tags = raw.get("tags", [])
    if isinstance(tags, list):
        subcategories = [t.get("label", t) if isinstance(t, dict) else str(t) for t in tags]
    else:
        subcategories = []
    
    # End date
    end_date = raw.get("endDate", raw.get("end_date_iso", raw.get("endDateIso", "")))
    
    # Market ID — prefer conditionId or id
    market_id = raw.get("conditionId", raw.get("id", raw.get("condition_id", "")))
    
    # Build odds history from daily prices if available
    history_raw = raw.get("history", raw.get("oneDayPriceChange", None))
    odds_history = []
    if isinstance(history_raw, list):
        for point in history_raw[-50:]:  # Keep last 50 points
            if isinstance(point, dict):
                odds_history.append({
                    "t": point.get("t", point.get("timestamp", "")),
                    "yes": float(point.get("p", point.get("yes", 0.5))),
                    "no": 1.0 - float(point.get("p", point.get("yes", 0.5)))
                })
    
    return {
        "id": str(market_id),
        "question": raw.get("question", ""),
        "description": raw.get("description", "")[:2000],  # cap size
        "category": raw.get("category", raw.get("groupItemTitle", "")),
        "subcategories": json.dumps(subcategories),
        "end_date": str(end_date),
        "current_yes": current_yes,
        "current_no": current_no,
        "volume": volume,
        "liquidity": liquidity,
        "odds_history": json.dumps(odds_history),
        "related_markets": json.dumps([]),
        "raw_data": json.dumps(raw)[:10000],  # cap raw data size
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }


def store_markets(markets: list[dict]) -> int:
    """Store parsed markets in the database. Returns count stored."""
    if not markets:
        return 0

    conn = get_connection()
    stored = 0
    try:
        for market in markets:
            if not market.get("id") or not market.get("question"):
                continue
            try:
                if DB_BACKEND == "postgres":
                    subcategories = _json_loads_safe(market.get("subcategories"), [])
                    odds_history = _json_loads_safe(market.get("odds_history"), [])
                    related_markets = _json_loads_safe(market.get("related_markets"), [])
                    raw_data = _json_loads_safe(market.get("raw_data"), {})
                    conn.execute(
                        """
                        INSERT INTO markets (id, question, description, category, subcategories, end_time, raw_market, created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s)
                        ON CONFLICT (id) DO UPDATE SET
                          question = EXCLUDED.question,
                          description = EXCLUDED.description,
                          category = EXCLUDED.category,
                          subcategories = EXCLUDED.subcategories,
                          end_time = COALESCE(EXCLUDED.end_time, markets.end_time),
                          raw_market = EXCLUDED.raw_market,
                          updated_at = EXCLUDED.updated_at
                        """,
                        (
                            market["id"], market.get("question", ""), market.get("description", ""), market.get("category", ""),
                            json.dumps(subcategories), _null_if_blank(market.get("end_date")), json.dumps(raw_data),
                            _null_if_blank(market.get("fetched_at")), _null_if_blank(market.get("updated_at")),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO market_snapshots (market_id, fetched_at, current_yes, current_no, volume_usd, liquidity_usd, odds_history, related_markets, raw_data)
                        VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
                        ON CONFLICT (market_id, fetched_at) DO UPDATE SET
                          current_yes = EXCLUDED.current_yes,
                          current_no = EXCLUDED.current_no,
                          volume_usd = EXCLUDED.volume_usd,
                          liquidity_usd = EXCLUDED.liquidity_usd,
                          odds_history = EXCLUDED.odds_history,
                          related_markets = EXCLUDED.related_markets,
                          raw_data = EXCLUDED.raw_data
                        """,
                        (
                            market["id"], _null_if_blank(market.get("fetched_at")), market.get("current_yes"), market.get("current_no"),
                            market.get("volume", 0), market.get("liquidity", 0), json.dumps(odds_history), json.dumps(related_markets), json.dumps(raw_data),
                        ),
                    )
                else:
                    existing = conn.execute(
                        "SELECT id FROM markets WHERE id = ?", (market["id"],)
                    ).fetchone()
                    if existing:
                        conn.execute("""
                            UPDATE markets SET
                                current_yes=?, current_no=?, volume=?, liquidity=?,
                                odds_history=?, updated_at=?, raw_data=?
                            WHERE id=?
                        """, (
                            market["current_yes"], market["current_no"],
                            market["volume"], market["liquidity"],
                            market["odds_history"], market["updated_at"],
                            market["raw_data"], market["id"]
                        ))
                    else:
                        conn.execute("""
                            INSERT INTO markets
                                (id, question, description, category, subcategories, end_date,
                                 current_yes, current_no, volume, liquidity, odds_history,
                                 related_markets, raw_data, fetched_at, updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            market["id"], market["question"], market["description"],
                            market["category"], market["subcategories"], market["end_date"],
                            market["current_yes"], market["current_no"],
                            market["volume"], market["liquidity"],
                            market["odds_history"], market["related_markets"],
                            market["raw_data"], market["fetched_at"], market["updated_at"]
                        ))
                stored += 1
            except Exception as e:
                print(f"[INGESTION] Error storing market {market.get('id')}: {e}")
        conn.commit()
    finally:
        conn.close()
    return stored


def run_ingestion(max_markets: int = 500) -> dict:
    """Main ingestion job: fetch markets from Polymarket and store in DB."""
    from database.db import get_connection as _gc
    
    log_conn = _gc()
    log_id = None
    started_at = datetime.now(timezone.utc).isoformat()
    
    try:
        if DB_BACKEND == "postgres":
            cur = log_conn.execute(
                "INSERT INTO job_runs (job_name, status, message, started_at) VALUES (%s,%s,%s,%s) RETURNING id",
                ("ingestion", "RUNNING", "Starting market fetch", started_at)
            )
            log_id = cur.fetchone()["id"]
        else:
            cur = log_conn.execute(
                "INSERT INTO scheduler_log (job_name, status, message, started_at) VALUES (?,?,?,?)",
                ("ingestion", "RUNNING", "Starting market fetch", started_at)
            )
            log_id = cur.lastrowid
        log_conn.commit()
    except:
        pass
    finally:
        log_conn.close()
    
    print(f"[INGESTION] Starting Polymarket data fetch at {started_at}")
    
    total_fetched = 0
    total_stored = 0
    offset = 0
    batch_size = 100
    
    while total_fetched < max_markets:
        raw_markets = fetch_markets(limit=min(batch_size, max_markets - total_fetched), offset=offset)
        if not raw_markets:
            print(f"[INGESTION] No more markets at offset {offset}")
            break
        
        parsed = []
        for raw in raw_markets:
            try:
                p = parse_market(raw)
                if p["id"] and p["question"]:
                    parsed.append(p)
            except Exception as e:
                print(f"[INGESTION] Parse error: {e}")
        
        stored = store_markets(parsed)
        total_fetched += len(raw_markets)
        total_stored += stored
        offset += len(raw_markets)
        
        print(f"[INGESTION] Batch: fetched={len(raw_markets)}, stored={stored}, total={total_fetched}")
        
        if len(raw_markets) < batch_size:
            break
    
    result = {
        "status": "SUCCESS",
        "total_fetched": total_fetched,
        "total_stored": total_stored,
        "completed_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Update log
    log_conn = get_connection()
    try:
        if log_id:
            if DB_BACKEND == "postgres":
                log_conn.execute(
                    "UPDATE job_runs SET status=%s, message=%s, completed_at=%s WHERE id=%s",
                    ("SUCCESS", json.dumps(result), result["completed_at"], log_id)
                )
            else:
                log_conn.execute(
                    "UPDATE scheduler_log SET status=?, message=?, completed_at=? WHERE id=?",
                    ("SUCCESS", json.dumps(result), result["completed_at"], log_id)
                )
        log_conn.commit()
    finally:
        log_conn.close()
    
    print(f"[INGESTION] Complete: {result}")
    return result


def inject_sample_markets():
    """Inject sample/mock markets for demo if Polymarket is unreachable."""
    sample_markets = [
        {
            "id": "demo-001",
            "question": "Will the Federal Reserve cut interest rates by at least 25 bps at the March 2025 FOMC meeting?",
            "description": "Resolves YES if FOMC cuts federal funds rate target by ≥25 bps in March 2025 meeting.",
            "category": "Economics",
            "subcategories": json.dumps(["Fed", "rates", "FOMC", "monetary policy"]),
            "end_date": "2025-03-22",
            "current_yes": 0.68,
            "current_no": 0.32,
            "volume": 5200000,
            "liquidity": 980000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.45, "no": 0.55},
                {"t": "2025-01-15", "yes": 0.52, "no": 0.48},
                {"t": "2025-02-01", "yes": 0.61, "no": 0.39},
                {"t": "2025-02-15", "yes": 0.68, "no": 0.32},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-002",
            "question": "Will the US impose additional tariffs on Chinese semiconductors above 50% in 2025?",
            "description": "Resolves YES if the US government announces or implements tariffs >50% on Chinese semiconductor imports in 2025.",
            "category": "Politics",
            "subcategories": json.dumps(["tariffs", "China", "semiconductors", "trade policy", "export controls"]),
            "end_date": "2025-12-31",
            "current_yes": 0.41,
            "current_no": 0.59,
            "volume": 3100000,
            "liquidity": 620000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.30, "no": 0.70},
                {"t": "2025-01-20", "yes": 0.38, "no": 0.62},
                {"t": "2025-02-10", "yes": 0.41, "no": 0.59},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-003",
            "question": "Will Bitcoin reach $100,000 USD before June 2025?",
            "description": "Resolves YES if BTC/USD price on any major exchange exceeds $100,000 before June 1, 2025.",
            "category": "Crypto",
            "subcategories": json.dumps(["Bitcoin", "BTC", "crypto", "price target"]),
            "end_date": "2025-06-01",
            "current_yes": 0.73,
            "current_no": 0.27,
            "volume": 8900000,
            "liquidity": 1500000,
            "odds_history": json.dumps([
                {"t": "2024-12-01", "yes": 0.55, "no": 0.45},
                {"t": "2025-01-01", "yes": 0.62, "no": 0.38},
                {"t": "2025-02-01", "yes": 0.70, "no": 0.30},
                {"t": "2025-02-15", "yes": 0.73, "no": 0.27},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-004",
            "question": "Will NVIDIA announce a next-generation GPU architecture (post-Blackwell) in 2025?",
            "description": "Resolves YES if NVIDIA publicly announces a new GPU architecture generation beyond Blackwell at any major conference or press release in 2025.",
            "category": "Technology",
            "subcategories": json.dumps(["NVIDIA", "GPU", "AI chips", "semiconductor", "data center"]),
            "end_date": "2025-12-31",
            "current_yes": 0.55,
            "current_no": 0.45,
            "volume": 1200000,
            "liquidity": 280000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.40, "no": 0.60},
                {"t": "2025-02-01", "yes": 0.55, "no": 0.45},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-005",
            "question": "Will the US Congress pass stablecoin legislation in 2025?",
            "description": "Resolves YES if the US Congress passes any bill specifically regulating stablecoins and it is signed into law in 2025.",
            "category": "Crypto",
            "subcategories": json.dumps(["stablecoin", "crypto regulation", "Congress", "SEC", "CFTC", "legislation"]),
            "end_date": "2025-12-31",
            "current_yes": 0.38,
            "current_no": 0.62,
            "volume": 2700000,
            "liquidity": 540000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.28, "no": 0.72},
                {"t": "2025-01-15", "yes": 0.33, "no": 0.67},
                {"t": "2025-02-15", "yes": 0.38, "no": 0.62},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-006",
            "question": "Will the US impose new AI export controls on GPU chips to China in 2025?",
            "description": "Resolves YES if the US government announces new export restrictions specifically targeting AI/GPU chips to China beyond existing BIS rules.",
            "category": "Technology",
            "subcategories": json.dumps(["export controls", "AI regulation", "China", "GPU", "semiconductor", "BIS"]),
            "end_date": "2025-12-31",
            "current_yes": 0.62,
            "current_no": 0.38,
            "volume": 4100000,
            "liquidity": 780000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.48, "no": 0.52},
                {"t": "2025-01-20", "yes": 0.55, "no": 0.45},
                {"t": "2025-02-10", "yes": 0.62, "no": 0.38},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-007",
            "question": "Will oil (Brent crude) exceed $100/barrel before end of 2025?",
            "description": "Resolves YES if Brent crude oil futures price exceeds $100/barrel at any point before December 31, 2025.",
            "category": "Economics",
            "subcategories": json.dumps(["oil", "Brent", "OPEC", "energy", "inflation"]),
            "end_date": "2025-12-31",
            "current_yes": 0.22,
            "current_no": 0.78,
            "volume": 6300000,
            "liquidity": 1100000,
            "odds_history": json.dumps([
                {"t": "2024-12-01", "yes": 0.30, "no": 0.70},
                {"t": "2025-01-01", "yes": 0.25, "no": 0.75},
                {"t": "2025-02-01", "yes": 0.22, "no": 0.78},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-008",
            "question": "Will the Super Bowl LIX be won by the Philadelphia Eagles?",
            "description": "Resolves YES if the Philadelphia Eagles win Super Bowl LIX.",
            "category": "Sports",
            "subcategories": json.dumps(["NFL", "Super Bowl", "sports", "football"]),
            "end_date": "2025-02-10",
            "current_yes": 0.52,
            "current_no": 0.48,
            "volume": 15000000,
            "liquidity": 2000000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.30, "no": 0.70},
                {"t": "2025-01-20", "yes": 0.45, "no": 0.55},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-009",
            "question": "Will Taiwan Strait military tensions escalate to armed conflict in 2025?",
            "description": "Resolves YES if armed military exchange occurs between PRC and Taiwan/US forces in or around the Taiwan Strait in 2025.",
            "category": "Geopolitics",
            "subcategories": json.dumps(["Taiwan", "China", "military", "semiconductor", "TSMC", "geopolitics"]),
            "end_date": "2025-12-31",
            "current_yes": 0.07,
            "current_no": 0.93,
            "volume": 3500000,
            "liquidity": 650000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.06, "no": 0.94},
                {"t": "2025-02-01", "yes": 0.07, "no": 0.93},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-010",
            "question": "Will a major cybersecurity breach affect a US financial institution in Q1 2025?",
            "description": "Resolves YES if a publicly disclosed cybersecurity breach causes material disruption to a top-20 US financial institution in Q1 2025.",
            "category": "Technology",
            "subcategories": json.dumps(["cybersecurity", "breach", "financial", "ransomware", "CISA"]),
            "end_date": "2025-03-31",
            "current_yes": 0.31,
            "current_no": 0.69,
            "volume": 890000,
            "liquidity": 175000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.28, "no": 0.72},
                {"t": "2025-02-01", "yes": 0.31, "no": 0.69},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-011",
            "question": "Will the US CPI inflation rate fall below 2.5% year-over-year before July 2025?",
            "description": "Resolves YES if any reported monthly CPI YoY reading comes in below 2.5% before July 2025 data release.",
            "category": "Economics",
            "subcategories": json.dumps(["CPI", "inflation", "Fed", "rates", "monetary policy"]),
            "end_date": "2025-07-15",
            "current_yes": 0.44,
            "current_no": 0.56,
            "volume": 4800000,
            "liquidity": 920000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.55, "no": 0.45},
                {"t": "2025-01-20", "yes": 0.50, "no": 0.50},
                {"t": "2025-02-10", "yes": 0.44, "no": 0.56},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-012",
            "question": "Will Elon Musk host a major entertainment event in 2025?",
            "description": "Resolves YES if Elon Musk hosts a televised entertainment or awards-style event in 2025.",
            "category": "Culture",
            "subcategories": json.dumps(["celebrity", "entertainment", "Elon Musk"]),
            "end_date": "2025-12-31",
            "current_yes": 0.15,
            "current_no": 0.85,
            "volume": 200000,
            "liquidity": 50000,
            "odds_history": json.dumps([{"t": "2025-01-01", "yes": 0.15, "no": 0.85}]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-013",
            "question": "Will the SEC approve an Ethereum spot ETF in the United States in 2025?",
            "description": "Resolves YES if the SEC approves an Ethereum spot ETF for US trading in 2025.",
            "category": "Crypto",
            "subcategories": json.dumps(["Ethereum", "ETH", "ETF", "SEC", "crypto regulation", "staking"]),
            "end_date": "2025-12-31",
            "current_yes": 0.71,
            "current_no": 0.29,
            "volume": 7200000,
            "liquidity": 1400000,
            "odds_history": json.dumps([
                {"t": "2024-12-01", "yes": 0.52, "no": 0.48},
                {"t": "2025-01-01", "yes": 0.61, "no": 0.39},
                {"t": "2025-02-01", "yes": 0.68, "no": 0.32},
                {"t": "2025-02-15", "yes": 0.71, "no": 0.29},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-014",
            "question": "Will OPEC+ announce a production increase greater than 500k bpd in H1 2025?",
            "description": "Resolves YES if OPEC+ formally announces an aggregate production increase of more than 500,000 barrels per day in any single adjustment in H1 2025.",
            "category": "Economics",
            "subcategories": json.dumps(["OPEC", "oil", "production", "energy", "Brent", "WTI", "inflation"]),
            "end_date": "2025-06-30",
            "current_yes": 0.29,
            "current_no": 0.71,
            "volume": 1900000,
            "liquidity": 380000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.22, "no": 0.78},
                {"t": "2025-02-01", "yes": 0.29, "no": 0.71},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        },
        {
            "id": "demo-015",
            "question": "Will TSMC announce a US fab expansion beyond Arizona in 2025?",
            "description": "Resolves YES if TSMC publicly announces plans to build or expand manufacturing facilities in a US state other than Arizona in 2025.",
            "category": "Technology",
            "subcategories": json.dumps(["TSMC", "semiconductor", "foundry", "US manufacturing", "chips", "CHIPS Act"]),
            "end_date": "2025-12-31",
            "current_yes": 0.33,
            "current_no": 0.67,
            "volume": 950000,
            "liquidity": 190000,
            "odds_history": json.dumps([
                {"t": "2025-01-01", "yes": 0.25, "no": 0.75},
                {"t": "2025-02-01", "yes": 0.33, "no": 0.67},
            ]),
            "related_markets": json.dumps([]),
            "raw_data": json.dumps({}),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
    ]
    
    stored = store_markets(sample_markets)
    print(f"[INGESTION] Injected {stored} sample markets for demo")
    return stored


if __name__ == "__main__":
    from database.db import init_db
    init_db()
    
    print("[INGESTION] Attempting to fetch from Polymarket API...")
    result = run_ingestion(max_markets=200)
    
    # If we got very few markets, inject samples too
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
    conn.close()
    
    if count < 5:
        print("[INGESTION] Few markets fetched, injecting sample data for demo...")
        inject_sample_markets()
