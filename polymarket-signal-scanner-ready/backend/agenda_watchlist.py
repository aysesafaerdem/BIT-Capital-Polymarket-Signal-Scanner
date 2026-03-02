"""BIT Capital agenda watchlist hints (soft matching only).

These hints should guide normalization/priority when market text explicitly matches.
Never infer a match without textual evidence in the market input.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AgendaTheme:
    key: str
    label: str
    tags: tuple[str, ...]
    catch_phrases: tuple[str, ...]
    transmission: str
    bit_why: str
    priority: int  # 1-5 (5 highest)
    scheduled: bool = False


AGENDA_THEMES: tuple[AgendaTheme, ...] = (
    AgendaTheme(
        key="middle_east_oil_risk",
        label="Middle East / Iran -> Oil Risk Premium -> Inflation/Rates -> Tech Multiples",
        tags=("ENERGY_OIL", "SHIPPING_CHOKEPOINT", "GEO_SANCTIONS", "MACRO_RATES"),
        catch_phrases=(
            "us strikes iran", "iran", "irgc", "nuclear talks", "geneva talks",
            "tanker seizure", "shipping insurance", "strait of hormuz", "red sea", "suez",
            "sanctions relief", "sanctions on iran", "ofac"
        ),
        transmission="ENERGY_OIL + SHIPPING_CHOKEPOINT + GEO_SANCTIONS -> Brent up -> inflation expectations up -> yields up -> growth de-rating",
        bit_why="Rate-sensitive tech basket and AI infra/mining risk appetite are directly exposed to oil/rates repricing.",
        priority=5,
    ),
    AgendaTheme(
        key="us_trade_tariffs",
        label="US Trade/Tariff Shocks -> Inflation Path / USD / Risk-On-Off",
        tags=("TRADE_TARIFFS", "MACRO_RATES", "FX_USD"),
        catch_phrases=("universal tariff", "tariff hike", "supreme court tariff ruling", "cbp halts collection", "trade policy"),
        transmission="TRADE_TARIFFS -> inflation impulse repriced -> Fed path repriced -> growth/crypto risk-on-off",
        bit_why="Growth, semis, and crypto beta names are highly sensitive to rates + policy volatility.",
        priority=5,
    ),
    AgendaTheme(
        key="fed_path_hard_dates",
        label="Fed Path + Next Hard Dates (Jobs/FOMC)",
        tags=("MACRO_RATES", "FED_PATH", "SCHEDULED_CATALYST"),
        catch_phrases=("march fomc", "fomc", "jobs report", "march 6", "pause", "rate cut", "term premium", "fed speakers"),
        transmission="MACRO_RATES -> discount rate / term premium -> growth multiple repricing",
        bit_why="BIT's core tech holdings are duration-sensitive and react sharply to Fed path repricing.",
        priority=5,
        scheduled=True,
    ),
    AgendaTheme(
        key="ai_semis_export_controls",
        label="AI / Semis / Export Controls (US-China)",
        tags=("AI_SEMIS", "GEO_SANCTIONS", "EXPORT_CONTROLS"),
        catch_phrases=("ai chip export policy", "h200", "licensing", "cloud gpu rentals", "chipmaking tools", "export control", "cloud loopholes"),
        transmission="AI_SEMIS + GEO_SANCTIONS -> revenue/supply chain risk -> AI capex pacing -> semis repricing",
        bit_why="Direct exposure to NVDA/TSM/MU and second-order effects to AI infra capex beneficiaries.",
        priority=5,
    ),
    AgendaTheme(
        key="taiwan_ai_server_exports",
        label="Taiwan AI Server / Export Boom",
        tags=("AI_SEMIS", "TAIWAN_EXPORTS", "AI_CAPEX"),
        catch_phrases=("taiwan exports", "ai servers", "tsmc record", "capex", "nvidia earnings spillover"),
        transmission="TAIWAN_EXPORTS + AI_CAPEX -> TSMC complex confidence -> semis earnings/capex sentiment",
        bit_why="Supports or weakens confidence in AI supply chain throughput and demand sustainability.",
        priority=4,
    ),
    AgendaTheme(
        key="crypto_market_structure",
        label="US Crypto Market Structure / Legislation / Tokenization",
        tags=("CRYPTO_REGULATION", "CRYPTO_MARKET", "MARKET_PLUMBING"),
        catch_phrases=("clarity act", "sec/cftc jurisdiction", "token taxonomy", "tokenized securities", "24/7 trading", "stablecoin", "tokenized treasury", "define crypto market rules"),
        transmission="CRYPTO_REGULATION + CRYPTO_MARKET -> liquidity/participation -> miners/brokers/crypto financials repriced",
        bit_why="Direct relevance to HOOD/miners/crypto beta and market structure sentiment.",
        priority=5,
        scheduled=True,
    ),
    AgendaTheme(
        key="cyber_ransomware_pressure",
        label="Cyber / Ransomware Pressure -> Security Spend",
        tags=("CYBER_EVENT", "SECURITY_SPEND"),
        catch_phrases=("ransomware", "zero-day", "supply chain attack", "cisa", "nist mandate", "fcc urges", "cybersecurity outlook"),
        transmission="CYBER_EVENT -> enterprise urgency up -> security budget reallocation -> PANW/CRWD demand tailwind",
        bit_why="Direct linkage to cybersecurity exposure and risk-event repricing.",
        priority=4,
    ),
    AgendaTheme(
        key="ukraine_geopolitics_energy",
        label="Russia-Ukraine Geopolitics -> Energy / Risk Premium",
        tags=("GEOPOLITICS", "ENERGY_OIL", "RISK_PREMIUM"),
        catch_phrases=("russia ukraine", "ukraine talks", "ceasefire talks", "gas flows", "war anniversary"),
        transmission="GEOPOLITICS -> energy / risk premium -> inflation / risk sentiment -> cross-asset repricing",
        bit_why="Can reinforce oil/rates narrative and broad risk appetite shifts for BIT's tech-heavy book.",
        priority=4,
    ),
)

UPCOMING_CATALYSTS: tuple[dict[str, Any], ...] = (
    {
        "label": "Macro data clusters (inflation/GDP/central bank speakers)",
        "window": "next 4-10 weeks",
        "tags": ["MACRO_RATES", "VOL_CLUSTER"],
        "priority": 4,
    },
    {
        "label": "FOMC Mar 17-18, 2026 (then Apr 28-29)",
        "window": "scheduled",
        "tags": ["FED_PATH", "SCHEDULED_CATALYST"],
        "priority": 5,
    },
    {
        "label": "OPEC+/sanctions/supply disruption narrative",
        "window": "ongoing",
        "tags": ["ENERGY_OIL", "GEO_SANCTIONS"],
        "priority": 4,
    },
    {
        "label": "Crypto rulemaking / tokenization / 24-7 trading plumbing",
        "window": "ongoing",
        "tags": ["CRYPTO_REGULATION", "MARKET_PLUMBING"],
        "priority": 4,
    },
)


def _text_blob(*parts: Any) -> str:
    return " ".join(str(p or "") for p in parts).lower()


def match_agenda_hints(*parts: Any) -> dict[str, Any]:
    """Return soft agenda matches/tags if market text explicitly contains catch phrases."""
    text = _text_blob(*parts)
    matches = []
    tags = []
    score = 0
    for theme in AGENDA_THEMES:
        hits = [p for p in theme.catch_phrases if p in text]
        if not hits:
            continue
        matches.append({
            "key": theme.key,
            "label": theme.label,
            "hits": hits[:6],
            "tags": list(theme.tags),
            "transmission": theme.transmission,
            "bit_why": theme.bit_why,
            "priority": theme.priority,
            "scheduled": theme.scheduled,
        })
        tags.extend(theme.tags)
        score += theme.priority * (1 + min(len(hits), 2))
    # de-dupe while preserving order
    seen = set()
    uniq_tags = []
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            uniq_tags.append(tag)
    matches.sort(key=lambda m: (-m["priority"], m["label"]))
    return {
        "matches": matches,
        "tags": uniq_tags,
        "agenda_score": score,
        "is_agenda_relevant": bool(matches),
    }


def get_upcoming_catalysts() -> list[dict[str, Any]]:
    return [dict(x) for x in UPCOMING_CATALYSTS]
