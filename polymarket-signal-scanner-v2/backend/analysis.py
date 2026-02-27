"""
LLM-Powered Signal Analysis Pipeline — BIT Capital Polymarket Signal Scanner
Professional taxonomy: driver_category × market_channel × macro_regime × event_cadence
Deeply wired to BIT Capital fund structure and holdings.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from html import unescape
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import get_connection, dict_from_row, DB_BACKEND
from backend.agenda_watchlist import match_agenda_hints

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

try:
    import google.generativeai as google_generativeai
    GEMINI_AVAILABLE = True
except ImportError:
    google_generativeai = None
    GEMINI_AVAILABLE = False

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
LLM_RETRY_MAX = int(os.environ.get("LLM_RETRY_MAX", "2"))
LLM_RETRY_BASE_SLEEP_SECS = float(os.environ.get("LLM_RETRY_BASE_SLEEP_SECS", "1.5"))
EVIDENCE_RETRIEVER_ENABLED = os.environ.get("EVIDENCE_RETRIEVER_ENABLED", "1") not in {"0", "false", "False"}
EVIDENCE_RETRIEVER_MAX_ITEMS = int(os.environ.get("EVIDENCE_RETRIEVER_MAX_ITEMS", "4"))
REANALYZE_AFTER_HOURS = max(1, int(os.environ.get("REANALYZE_AFTER_HOURS", "24")))

# Global cooldown to avoid repeatedly hammering providers when daily quota is exhausted.
LLM_COOLDOWN_UNTIL_TS = 0.0

# ══════════════════════════════════════════════════════════════════════════════
# TAXONOMY — the canonical classification vocabulary fed to the LLM
# ══════════════════════════════════════════════════════════════════════════════

TAXONOMY = {
    "driver_category": [
        "monetary_policy_central_banks",      # Fed, ECB, BOJ rate path / QT / forward guidance
        "inflation_and_prices",               # CPI, PCE, PPI, wage data, energy pass-through
        "growth_and_labor",                   # GDP, NFP, PMI, earnings recession risk
        "fiscal_and_sovereign",               # US deficit, debt ceiling, sovereign spreads
        "trade_and_tariffs",                  # Universal tariff, WTO, CBP rulings, trade deals
        "geopolitics_and_conflict",           # Russia-Ukraine, Middle East, Taiwan Strait, Korea
        "sanctions_and_export_controls",      # OFAC sanctions, BIS export controls, chip bans
        "energy_and_supply_disruptions",      # Oil/gas supply, OPEC+, shipping chokepoints
        "technology_cycle_ai_semis",          # AI capex, chip demand, foundry utilization, HBM
        "crypto_policy_and_market_structure", # SEC/CFTC, stablecoin bills, 24/7 trading, tokenization
        "cybersecurity_and_digital_risk",     # Breaches, mandates, CISA, ransomware, zero-days
        "company_specific_corporate_actions", # Earnings beats/misses, M&A, splits, guidance
    ],
    "market_channel": [
        "rates_duration",          # 2y/10y yield, SOFR, term premium → discount rate repricing
        "inflation_expectations",  # Breakevens, TIPS, commodity pass-through → real yield moves
        "fx_usd_liquidity",        # DXY strength, USD funding stress, EM FX → risk appetite
        "equities_risk_on_off",    # Broad beta, VIX, SPX → risk-on / risk-off for all names
        "equities_factor",         # Growth vs Value, duration factor, quality, small vs large cap
        "credit_spreads",          # IG/HY OAS, CDS → cost of capital for leveraged names
        "commodities_energy",      # Brent, WTI, nat gas, coal → inflation + miner energy costs
        "commodities_metals",      # Copper, gold, cobalt → capex costs, safe haven demand
        "shipping_chokepoints",    # Suez, Hormuz, Panama → supply chain costs + inflation
        "crypto_liquidity",        # BTC/ETH spot, stablecoin flows, funding rates → miner/broker P&L
    ],
    "macro_regime": [
        "disinflation_soft_landing",     # Core CPI falling, Fed on hold/cut, growth OK → risk-on
        "reacceleration_overheating",    # Growth/inflation surprising higher → Fed stays higher
        "growth_scare_recession",        # PMI/NFP collapsing, yield curve inversion → risk-off
        "stagflation_oil_shock",         # Oil spike + weak growth → worst of both worlds
        "policy_surprise_hawkish",       # Fed dots, FOMC statement more hawkish than priced
        "policy_surprise_dovish",        # Fed pivot signal, cut more than priced → risk-on
        "tariff_inflation_shock",        # Tariff announcement → CPI spike → Fed re-pricing
        "liquidity_crunch_funding_stress",  # Repo stress, credit events, funding market dislocation
        "geopolitical_risk_premium",     # War escalation, sanctions expansion → safe haven bid
    ],
    "event_cadence": [
        "scheduled_data_release",          # CPI, NFP, GDP — known dates, vol clustered around them
        "scheduled_policy_meeting",        # FOMC, ECB, BOJ — hard binary outcomes
        "unscheduled_breaking_event",      # Military action, surprise default, flash crash
        "binary_legal_regulatory_deadline",# Court ruling, legislation vote, regulatory deadline
        "company_calendar_event",          # Earnings call, investor day, product launch, delivery
    ],
    "overlay_mode": ["GICS_11", "ICB", "BIT_THEMES"],
}

# ══════════════════════════════════════════════════════════════════════════════
# BIT CAPITAL FUND + HOLDINGS — authoritative reference injected into every prompt
# ══════════════════════════════════════════════════════════════════════════════

BIT_PORTFOLIO = {
    "funds": {
        "BIT Global Technology Leaders": {
            "focus": "High-conviction global tech: AI infrastructure, semis, fintech/insurtech, clean energy",
            "style": "Growth / high-duration — acutely sensitive to rates and AI capex cycle",
            "holdings": [
                {"name": "IREN (Iris Energy)",  "ticker": "IREN",  "weight": 9.5,  "sector": "AI Infrastructure",
                 "gics": "Information Technology", "themes": ["AI compute", "bitcoin mining", "data center", "power"],
                 "sensitivities": ["BTC price", "power costs", "AI capex", "hashrate difficulty"],
                 "driver_categories": ["technology_cycle_ai_semis", "crypto_policy_and_market_structure", "energy_and_supply_disruptions"],
                 "channels": ["crypto_liquidity", "commodities_energy", "rates_duration"]},
                {"name": "Auto1 Group",          "ticker": "AUTO1", "weight": 8.0,  "sector": "Consumer Internet",
                 "gics": "Consumer Discretionary", "themes": ["e-commerce", "automotive", "marketplace"],
                 "sensitivities": ["consumer confidence", "auto demand", "European growth"],
                 "driver_categories": ["growth_and_labor", "monetary_policy_central_banks"],
                 "channels": ["equities_risk_on_off", "rates_duration"]},
                {"name": "Bloom Energy",         "ticker": "BE",    "weight": 7.0,  "sector": "Clean Energy",
                 "gics": "Utilities", "themes": ["fuel cell", "power", "energy infrastructure"],
                 "sensitivities": ["natural gas prices", "energy policy", "data center power demand"],
                 "driver_categories": ["energy_and_supply_disruptions", "technology_cycle_ai_semis"],
                 "channels": ["commodities_energy", "equities_factor"]},
                {"name": "Alphabet",             "ticker": "GOOGL", "weight": 5.5,  "sector": "Consumer Internet",
                 "gics": "Communication Services", "themes": ["AI", "search", "cloud", "advertising"],
                 "sensitivities": ["ad spend", "AI competition", "cloud growth", "antitrust"],
                 "driver_categories": ["growth_and_labor", "technology_cycle_ai_semis", "monetary_policy_central_banks"],
                 "channels": ["equities_factor", "rates_duration", "equities_risk_on_off"]},
                {"name": "Micron Technology",    "ticker": "MU",    "weight": 5.0,  "sector": "Semiconductors",
                 "gics": "Information Technology", "themes": ["DRAM", "HBM", "memory", "AI semis"],
                 "sensitivities": ["HBM demand", "NAND pricing", "China exposure", "export controls"],
                 "driver_categories": ["technology_cycle_ai_semis", "sanctions_and_export_controls"],
                 "channels": ["equities_factor", "rates_duration"]},
                {"name": "Nvidia",               "ticker": "NVDA",  "weight": 5.0,  "sector": "Semiconductors",
                 "gics": "Information Technology", "themes": ["GPU", "AI", "data center", "CUDA"],
                 "sensitivities": ["AI capex", "China revenue", "export controls", "hyperscaler demand"],
                 "driver_categories": ["technology_cycle_ai_semis", "sanctions_and_export_controls", "monetary_policy_central_banks"],
                 "channels": ["equities_factor", "rates_duration", "equities_risk_on_off"]},
                {"name": "TSMC",                 "ticker": "TSM",   "weight": 5.0,  "sector": "Semiconductors",
                 "gics": "Information Technology", "themes": ["foundry", "EUV", "Taiwan", "chips"],
                 "sensitivities": ["Taiwan geopolitics", "chip demand cycle", "export controls", "capex"],
                 "driver_categories": ["geopolitics_and_conflict", "sanctions_and_export_controls", "technology_cycle_ai_semis"],
                 "channels": ["equities_factor", "equities_risk_on_off"]},
                {"name": "Lemonade",             "ticker": "LMND",  "weight": 4.5,  "sector": "Insurtech",
                 "gics": "Financials", "themes": ["insurance", "AI underwriting", "insurtech"],
                 "sensitivities": ["insurance regulation", "loss ratios", "interest rates"],
                 "driver_categories": ["monetary_policy_central_banks", "company_specific_corporate_actions"],
                 "channels": ["rates_duration", "equities_factor"]},
                {"name": "Robinhood",            "ticker": "HOOD",  "weight": 4.5,  "sector": "Fintech",
                 "gics": "Financials", "themes": ["brokerage", "crypto", "retail investing", "PFOF"],
                 "sensitivities": ["retail trading volume", "crypto regulation", "PFOF rules", "interest income"],
                 "driver_categories": ["crypto_policy_and_market_structure", "monetary_policy_central_banks"],
                 "channels": ["crypto_liquidity", "rates_duration", "equities_risk_on_off"]},
                {"name": "Hinge Health",         "ticker": "HNGE",  "weight": 5.0,  "sector": "Digital Health",
                 "gics": "Health Care", "themes": ["digital health", "MSK", "physical therapy", "AI"],
                 "sensitivities": ["employer benefits spending", "ACA stability", "rates"],
                 "driver_categories": ["growth_and_labor", "monetary_policy_central_banks"],
                 "channels": ["equities_factor", "rates_duration"]},
            ],
        },
        "BIT Global Crypto Leaders": {
            "focus": "Pure-play crypto infrastructure: miners, digital asset finance, Ethereum",
            "style": "High-beta crypto — directly tied to BTC/ETH price, hashrate, regulation, and energy costs",
            "holdings": [
                {"name": "Hut 8",           "ticker": "HUT",  "weight": 10.0, "sector": "Crypto Mining",
                 "gics": "Information Technology", "themes": ["bitcoin mining", "hashrate", "data center"],
                 "sensitivities": ["BTC price", "mining difficulty", "power costs", "BTC halving"],
                 "driver_categories": ["crypto_policy_and_market_structure", "energy_and_supply_disruptions"],
                 "channels": ["crypto_liquidity", "commodities_energy"]},
                {"name": "Terawulf",        "ticker": "WULF", "weight": 5.0,  "sector": "Crypto Mining",
                 "gics": "Information Technology", "themes": ["bitcoin mining", "nuclear power", "hashrate"],
                 "sensitivities": ["BTC price", "nuclear power costs", "mining difficulty"],
                 "driver_categories": ["crypto_policy_and_market_structure", "energy_and_supply_disruptions"],
                 "channels": ["crypto_liquidity", "commodities_energy"]},
                {"name": "Cipher Mining",   "ticker": "CIFR", "weight": 5.0,  "sector": "Crypto Mining",
                 "gics": "Information Technology", "themes": ["bitcoin mining", "hashrate"],
                 "sensitivities": ["BTC price", "energy costs", "hashrate"],
                 "driver_categories": ["crypto_policy_and_market_structure", "energy_and_supply_disruptions"],
                 "channels": ["crypto_liquidity", "commodities_energy"]},
                {"name": "Riot Platforms",  "ticker": "RIOT", "weight": 5.0,  "sector": "Crypto Mining",
                 "gics": "Information Technology", "themes": ["bitcoin mining", "hashrate", "data center"],
                 "sensitivities": ["BTC price", "power costs", "mining difficulty"],
                 "driver_categories": ["crypto_policy_and_market_structure", "energy_and_supply_disruptions"],
                 "channels": ["crypto_liquidity", "commodities_energy"]},
                {"name": "Applied Digital",  "ticker": "APLD", "weight": 5.0,  "sector": "AI Infrastructure",
                 "gics": "Information Technology", "themes": ["data center", "HPC", "AI compute"],
                 "sensitivities": ["AI capex cycle", "power costs", "hyperscaler contracts"],
                 "driver_categories": ["technology_cycle_ai_semis", "energy_and_supply_disruptions"],
                 "channels": ["equities_factor", "commodities_energy"]},
                {"name": "Galaxy Digital",   "ticker": "GLXY", "weight": 4.5,  "sector": "Crypto Finance",
                 "gics": "Financials", "themes": ["crypto", "DeFi", "institutional", "trading"],
                 "sensitivities": ["BTC/ETH price", "institutional flows", "crypto regulation", "trading volume"],
                 "driver_categories": ["crypto_policy_and_market_structure", "monetary_policy_central_banks"],
                 "channels": ["crypto_liquidity", "equities_risk_on_off"]},
                {"name": "Ethereum",         "ticker": "ETH",  "weight": 4.5,  "sector": "Crypto",
                 "gics": "N/A (Digital Asset)", "themes": ["DeFi", "staking", "Layer2", "smart contracts"],
                 "sensitivities": ["ETH staking yield", "DeFi activity", "regulation", "BTC correlation"],
                 "driver_categories": ["crypto_policy_and_market_structure"],
                 "channels": ["crypto_liquidity", "equities_risk_on_off"]},
            ],
        },
        "BIT Defensive Growth": {
            "focus": "Quality compounder tech with durable moats: cloud, cybersecurity, China internet",
            "style": "Growth but more defensive — lower duration beta, stronger earnings visibility",
            "holdings": [
                {"name": "Palo Alto Networks", "ticker": "PANW", "weight": 3.5, "sector": "Cybersecurity",
                 "gics": "Information Technology", "themes": ["cybersecurity", "zero-trust", "SASE", "government"],
                 "sensitivities": ["enterprise IT spend", "cyber incidents", "government contracts", "platform consolidation"],
                 "driver_categories": ["cybersecurity_and_digital_risk", "growth_and_labor"],
                 "channels": ["equities_factor", "equities_risk_on_off"]},
                {"name": "Amazon",             "ticker": "AMZN", "weight": 4.0, "sector": "Consumer Internet",
                 "gics": "Consumer Discretionary", "themes": ["cloud", "e-commerce", "AI", "AWS"],
                 "sensitivities": ["AWS growth", "consumer spending", "AI investment returns", "margin expansion"],
                 "driver_categories": ["growth_and_labor", "technology_cycle_ai_semis", "monetary_policy_central_banks"],
                 "channels": ["equities_factor", "rates_duration"]},
                {"name": "Microsoft",          "ticker": "MSFT", "weight": 4.0, "sector": "Consumer Internet",
                 "gics": "Information Technology", "themes": ["cloud", "AI", "Azure", "Office365"],
                 "sensitivities": ["Azure growth", "Copilot monetization", "enterprise spend", "AI ROI"],
                 "driver_categories": ["technology_cycle_ai_semis", "monetary_policy_central_banks", "growth_and_labor"],
                 "channels": ["equities_factor", "rates_duration"]},
                {"name": "Alibaba",            "ticker": "BABA", "weight": 4.0, "sector": "Consumer Internet",
                 "gics": "Consumer Discretionary", "themes": ["China", "e-commerce", "cloud", "AI"],
                 "sensitivities": ["China consumer recovery", "US-China relations", "PBOC policy", "tech regulation"],
                 "driver_categories": ["geopolitics_and_conflict", "sanctions_and_export_controls", "growth_and_labor"],
                 "channels": ["equities_risk_on_off", "fx_usd_liquidity"]},
            ],
        },
        "BIT Global Multi Asset": {
            "focus": "Cross-asset diversification layer: semis equipment, cybersecurity, real assets",
            "style": "Multi-factor — blend of growth and defensive characteristics",
            "holdings": [
                {"name": "Broadcom",         "ticker": "AVGO", "weight": 2.0, "sector": "Semiconductors",
                 "gics": "Information Technology", "themes": ["ASIC", "networking", "AI chips", "enterprise"],
                 "sensitivities": ["hyperscaler custom silicon", "VMware integration", "AI networking"],
                 "driver_categories": ["technology_cycle_ai_semis", "growth_and_labor"],
                 "channels": ["equities_factor", "rates_duration"]},
                {"name": "CrowdStrike",      "ticker": "CRWD", "weight": 2.0, "sector": "Cybersecurity",
                 "gics": "Information Technology", "themes": ["endpoint security", "cloud", "XDR"],
                 "sensitivities": ["enterprise security spend", "cyber incidents", "platform wins"],
                 "driver_categories": ["cybersecurity_and_digital_risk", "growth_and_labor"],
                 "channels": ["equities_factor"]},
                {"name": "KLA Corporation",  "ticker": "KLAC", "weight": 2.0, "sector": "Semiconductors",
                 "gics": "Information Technology", "themes": ["process control", "EUV", "wafer inspection"],
                 "sensitivities": ["fab utilization", "chip capex cycle", "China restrictions"],
                 "driver_categories": ["technology_cycle_ai_semis", "sanctions_and_export_controls"],
                 "channels": ["equities_factor"]},
            ],
        },
        "BIT Global Fintech Leaders": {
            "focus": "Global fintech and emerging market financial services",
            "style": "Growth with EM tilt — sensitive to rates, EM risk appetite, regulation",
            "holdings": [
                {"name": "Kaspi.kz",  "ticker": "KSPI", "weight": 4.0, "sector": "Fintech",
                 "gics": "Financials", "themes": ["payments", "Kazakhstan", "emerging markets", "super app"],
                 "sensitivities": ["KZT stability", "Central Asia geopolitics", "payment regulation", "EM risk"],
                 "driver_categories": ["monetary_policy_central_banks", "geopolitics_and_conflict"],
                 "channels": ["fx_usd_liquidity", "equities_risk_on_off"]},
                {"name": "Credicorp", "ticker": "BAP",  "weight": 5.0, "sector": "Fintech",
                 "gics": "Financials", "themes": ["banking", "Peru", "LatAm", "financial services"],
                 "sensitivities": ["Peru political stability", "LatAm rates", "credit cycle", "commodity exposure"],
                 "driver_categories": ["monetary_policy_central_banks", "fiscal_and_sovereign", "geopolitics_and_conflict"],
                 "channels": ["rates_duration", "fx_usd_liquidity", "equities_risk_on_off"]},
                {"name": "Oscar Health", "ticker": "OSCR", "weight": 4.0, "sector": "Insurtech",
                 "gics": "Health Care", "themes": ["health insurance", "insurtech", "ACA"],
                 "sensitivities": ["ACA stability", "MLR regulation", "premium pricing", "healthcare inflation"],
                 "driver_categories": ["company_specific_corporate_actions", "growth_and_labor"],
                 "channels": ["equities_factor", "rates_duration"]},
            ],
        },
        "BIT Global Leaders": {
            "focus": "Global large-cap consumer internet and social media platforms",
            "style": "High-quality growth — ad cycle and consumer discretionary sensitivity",
            "holdings": [
                {"name": "Meta Platforms", "ticker": "META", "weight": 4.0, "sector": "Consumer Internet",
                 "gics": "Communication Services", "themes": ["social media", "advertising", "AI", "metaverse"],
                 "sensitivities": ["digital ad spend", "AI monetization", "regulatory scrutiny", "user growth"],
                 "driver_categories": ["growth_and_labor", "technology_cycle_ai_semis"],
                 "channels": ["equities_factor", "equities_risk_on_off"]},
                {"name": "Reddit",         "ticker": "RDDT", "weight": 4.0, "sector": "Consumer Internet",
                 "gics": "Communication Services", "themes": ["social media", "advertising", "community"],
                 "sensitivities": ["ad market", "AI data licensing", "user growth"],
                 "driver_categories": ["growth_and_labor"],
                 "channels": ["equities_factor", "equities_risk_on_off"]},
            ],
        },
    }
}

# Flat lookup: ticker → holding metadata (for rule-based fallback)
_TICKER_MAP: dict[str, dict] = {}
for _fund, _fdata in BIT_PORTFOLIO["funds"].items():
    for _h in _fdata["holdings"]:
        _TICKER_MAP[_h["ticker"]] = {**_h, "fund": _fund, "fund_focus": _fdata["focus"]}


# Extended keyword ontology to improve LLM trigger keyword precision and rule-based fallback
# coverage for BIT Capital fund names, holding aliases and macro/thematic catalysts.
BIT_KEYWORD_ONTOLOGY = {
    "bit_brand_and_funds": {
        "brand": ["BIT Capital", "BIT Capital GmbH", "bitcap", "Jan Beckers", "Carlos Bielsa"],
        "fund_sections": [
            "BIT Global Technology Leaders",
            "BIT Global Leaders",
            "BIT Global Fintech Leaders",
            "BIT Global Crypto Leaders",
            "BIT Defensive Growth",
            "BIT Global Multi Asset",
        ],
    },
    "core_holdings_keywords_by_fund": {
        "technology_leaders_core": [
            "IREN|Iris Energy", "AUTO1|Auto1 Group", "BE|Bloom Energy",
            "GOOGL|GOOG|Alphabet|Google", "MU|Micron", "NVDA|NVIDIA",
            "TSM|TSMC|Taiwan Semiconductor", "Hinge Health", "LMND|Lemonade",
            "HOOD|Robinhood", "RDDT|Reddit", "DDOG|Datadog",
        ],
        "global_leaders_core": [
            "IREN|Iris Energy", "AUTO1|Auto1 Group", "BE|Bloom Energy",
            "GOOGL|GOOG|Alphabet|Google", "MU|Micron", "NVDA|NVIDIA",
            "TSM|TSMC|Taiwan Semiconductor", "META|Meta Platforms", "RDDT|Reddit",
            "DDOG|Datadog", "Hinge Health",
        ],
        "fintech_leaders_core": [
            "AUTO1|Auto1 Group", "IREN|Iris Energy", "LMND|Lemonade", "HOOD|Robinhood",
            "KSPI|Kaspi.kz", "BAP|Credicorp", "OSCR|Oscar Health",
            "GOOGL|Alphabet|Google", "MU|Micron", "HUT|Hut 8",
        ],
        "crypto_leaders_core": [
            "IREN|Iris Energy", "HUT|Hut 8", "COIN|Coinbase", "ETH|Ethereum|Ether",
            "RIOT|Riot Platforms", "CIFR|Cipher Mining", "WULF|TeraWulf",
            "APLD|Applied Digital", "GLXY|Galaxy Digital", "Figure Technology Solutions|Figure",
        ],
    },
    "macro_and_rates": {
        "central_bank": ["Fed", "FOMC", "dot plot", "SEP|Summary of Economic Projections", "rate cut", "rate hike", "pause", "QT", "QE"],
        "data_releases": ["CPI", "PCE", "NFP|jobs report|payrolls", "unemployment rate", "wage growth", "PMI", "GDP"],
        "market_vars": ["UST 2Y", "UST 10Y", "yields", "term premium", "breakevens", "real yields", "DXY", "USD liquidity"],
    },
    "energy_oil_geopolitics": {
        "entities": ["Iran", "Venezuela", "OPEC", "OPEC+", "OFAC", "sanctions relief", "embargo", "secondary sanctions"],
        "chokepoints_shipping": ["Strait of Hormuz|Hormuz", "Red Sea", "Suez", "tanker seizure", "shipping insurance", "freight rates"],
        "oil_markets": ["Brent", "WTI", "oil supply disruption", "production quota", "output hike", "SPR|strategic petroleum reserve"],
    },
    "trade_tariffs_industrial_policy": {
        "keywords": ["tariffs", "global tariff", "trade war", "import ban", "export ban", "industrial policy", "subsidies", "reshoring", "supply chain relocation"],
    },
    "ai_semis_and_datacenters": {
        "ai_compute": ["AI capex", "data center capex", "hyperscalers", "GPU", "CUDA", "HBM", "DRAM", "memory cycle"],
        "supply_chain_controls": ["export controls", "chip ban", "licensing", "China tech policy", "foundry", "EUV", "advanced packaging"],
        "power_grid": ["power price", "grid constraint", "megawatt", "generation capacity", "gas turbine", "nuclear", "interconnection queue"],
    },
    "crypto_regulation_and_market_structure": {
        "regulators": ["SEC", "CFTC", "MiCA", "stablecoin bill", "market structure bill", "digital commodity exchange"],
        "market_terms": ["spot ETF", "ETF approval", "staking rules", "custody", "DeFi", "DEX", "tokenized securities", "24/7 trading"],
    },
    "fintech_insurtech_banking": {
        "fintech": ["KYC", "AML", "payments rules", "interchange", "PFOF|payment for order flow", "brokerage rules"],
        "banking": ["Basel", "capital requirements", "stress tests", "deposit flight", "bank regulation"],
        "insurance": ["underwriting", "reinsurance", "claims severity", "loss ratio", "rate filings"],
    },
    "cybersecurity": {
        "keywords": ["ransomware", "zero-day", "supply chain attack", "CISA", "NIST", "mandate", "critical infrastructure"],
    },
    "consumer_ads_cycle": {
        "keywords": ["ad spend", "digital advertising", "e-commerce demand", "consumer confidence", "recession", "soft landing", "hard landing"],
    },
    "macro_regime_tags_for_reasoning": [
        "disinflation_soft_landing", "reacceleration_overheating", "growth_scare_recession",
        "stagflation_oil_shock", "policy_surprise_hawkish", "policy_surprise_dovish",
        "tariff_inflation_shock", "liquidity_crunch_funding_stress", "geopolitical_risk_premium",
    ],
    "negative_filters_to_ignore_noise": [
        "celebrity", "sports", "award show", "relationship drama", "local election",
    ],
}

# Intelligent routing layer (holding-level alias/trigger maps + specific macro event recipes)
# used for LLM prompt enrichment and rule-based routing/scoring.
BIT_INTELLIGENT_FILTER_ROUTING = {
    "as_of": "2026-02-26",
    "specific_macro_event_recipes": {
        "iran_nuclear_sanctions_oil": [
            "us-iran nuclear talks", "iran nuclear talks", "geneva talks", "nuclear talks",
            "sanctions relief", "ofac relief", "brent above $70", "oil rises", "oil risk premium",
        ],
        "tariff_turmoil_trade_volatility": [
            "global tariff uncertainty", "tariff chaos", "tariff turmoil", "trade volatility",
            "supreme court tariff ruling", "tariff ruling",
        ],
        "fomc_march_2026_sep_dot_plot": [
            "march fomc", "fomc march 17-18", "fomc march 17 18", "sep meeting", "dot plot",
        ],
        "us_crypto_market_structure_bill": [
            "crypto market structure bill", "define crypto market rules", "cftc oversight",
            "stablecoin interest", "stablecoin bill", "market structure bill",
        ],
    },
    "global_macro_triggers": {
        "rates_duration": ["Fed", "FOMC", "dot plot", "SEP", "rate cut", "rate hike", "pause", "QT", "QE", "10Y yield", "real yields", "term premium"],
        "inflation": ["CPI", "PCE", "inflation expectations", "breakevens", "wage growth"],
        "usd_liquidity_fx": ["DXY", "USD liquidity", "funding stress", "swap spreads", "cross-currency basis"],
        "oil_energy_geopolitics": ["Brent", "WTI", "OPEC", "OPEC+", "sanctions", "OFAC", "Hormuz", "Red Sea", "tanker seizure", "shipping insurance", "oil supply disruption"],
        "trade_tariffs": ["tariff", "trade war", "export ban", "import ban", "industrial policy", "subsidy", "reshoring"],
        "risk_sentiment": ["VIX", "risk-on", "risk-off", "equity selloff", "credit spreads", "HY spreads"],
    },
    "holdings_trigger_keywords": {
        "IREN": {"aliases": ["IREN", "Iris Energy", "bitcoin miner", "HPC data center", "AI compute hosting"], "triggers": ["Bitcoin", "BTC price", "ETF inflows", "halving", "hashrate", "mining difficulty", "block reward", "fee revenue", "miner capitulation", "electricity price", "PPA", "grid curtailment", "Texas power", "megawatt capacity", "GPU cluster", "NVIDIA GPUs", "AI data center", "colocation", "AI hosting", "HPC buildout", "mining ban"]},
        "AUTO1": {"aliases": ["AUTO1", "Auto1 Group", "used car platform", "used-car marketplace"], "triggers": ["consumer confidence", "recession", "disposable income", "EU demand", "interest rates", "auto loans", "credit availability", "financing costs", "used car prices", "auction prices", "EV adoption", "EV pricing pressure", "guidance raise", "guidance cut", "take rate"]},
        "Hinge_Health": {"aliases": ["Hinge Health", "digital health", "MSK care", "telehealth"], "triggers": ["reimbursement", "payer contracts", "Medicare", "Medicaid", "ACA", "employer benefits", "health plan costs", "IPO filing", "S-1", "acquisition", "member growth", "retention", "ARPU"]},
        "TSM": {"aliases": ["TSM", "TSMC", "Taiwan Semiconductor", "foundry", "CoWoS", "advanced packaging"], "triggers": ["AI server demand", "GPU supply", "HBM", "advanced packaging", "chip shortage", "foundry capex", "wafer starts", "utilization", "US export controls", "China restrictions", "licensing", "EUV", "ASML", "Taiwan Strait", "China-Taiwan tension", "military drills", "fab outage"]},
        "MU": {"aliases": ["MU", "Micron", "DRAM", "NAND", "HBM memory"], "triggers": ["DRAM prices", "NAND prices", "memory upcycle", "inventory correction", "HBM demand", "AI servers", "GPU shipments", "data center capex", "China restrictions", "sanctions", "export licensing", "capex cut", "capex raise", "fab expansion"]},
        "NVDA": {"aliases": ["NVDA", "NVIDIA", "GPU", "CUDA", "AI accelerators"], "triggers": ["hyperscaler capex", "data center spend", "AI training", "inference demand", "GPU backlog", "Blackwell", "Hopper", "H200", "GB200", "China export ban", "US licensing", "restricted SKUs", "TSMC packaging", "CoWoS capacity", "HBM supply", "datacenter revenue"]},
        "GOOGL": {"aliases": ["GOOGL", "GOOG", "Alphabet", "Google"], "triggers": ["digital ad spend", "ad pricing", "marketing budgets", "recession ad pullback", "AI search", "LLM", "AI assistants", "cloud AI", "antitrust", "DMA", "DOJ case", "privacy regulation", "cloud growth", "AI cloud revenue"]},
        "META": {"aliases": ["META", "Meta", "Facebook", "Instagram"], "triggers": ["ad spend", "CPM", "reels monetization", "performance ads", "AI capex", "data center buildout", "GPU purchases", "privacy", "DMA", "platform regulation", "engagement", "user growth", "ARPU"]},
        "RDDT": {"aliases": ["RDDT", "Reddit", "social platform"], "triggers": ["ad demand", "brand ads", "performance ads", "data licensing", "AI training data", "API pricing", "LLM partnerships", "DAU", "MAU", "engagement", "content moderation"]},
        "DDOG": {"aliases": ["DDOG", "Datadog", "observability", "APM", "cloud monitoring"], "triggers": ["cloud optimization", "consumption-based pricing", "IT budgets", "AI workload monitoring", "GPU observability", "MLOps", "Splunk", "Dynatrace", "New Relic", "net retention", "ARR", "guidance"]},
        "LMND": {"aliases": ["LMND", "Lemonade", "insurtech"], "triggers": ["premium increases", "rate filings", "pricing adequacy", "hurricanes", "wildfires", "cat losses", "claims severity", "reinsurance costs", "reinsurance renewal", "bond yields", "insurance regulator"]},
        "HOOD": {"aliases": ["HOOD", "Robinhood", "broker", "retail trading"], "triggers": ["retail trading volumes", "options volume", "meme stocks", "market volatility", "crypto trading volume", "BTC", "ETH", "altcoins", "SEC", "PFOF", "best execution", "broker rules", "net interest revenue"]},
        "BE": {"aliases": ["BE", "Bloom Energy", "fuel cell", "distributed generation"], "triggers": ["data center power", "grid constraint", "on-site power", "microgrid", "IRA tax credits", "DOE funding", "clean energy incentives", "natural gas prices", "LNG", "spark spread", "backlog", "new orders"]},
        "PANW": {"aliases": ["PANW", "Palo Alto Networks", "cybersecurity"], "triggers": ["ransomware", "zero-day", "major breach", "supply chain attack", "CISA", "NIST", "critical infrastructure rules", "security budgets", "platform consolidation", "billings", "ARR"]},
        "CRWD": {"aliases": ["CRWD", "CrowdStrike", "endpoint security"], "triggers": ["ransomware", "breach", "incident response", "CISA", "NIST", "federal procurement", "endpoint spend", "cloud security", "ARR", "net retention"]},
        "AXON": {"aliases": ["AXON", "Axon", "body camera", "TASER", "public safety SaaS"], "triggers": ["police procurement", "government budget", "contract award", "law enforcement reform", "body-cam mandates", "subscription growth", "evidence management"]},
        "BABA": {"aliases": ["BABA", "Alibaba"], "triggers": ["China regulation", "platform crackdown", "stimulus", "property sector", "China consumption", "e-commerce GMV", "ADR delisting risk", "export controls", "geopolitics"]},
        "KSPI": {"aliases": ["KSPI", "Kaspi", "Kaspi.kz", "Kazakhstan fintech"], "triggers": ["KZT FX", "capital controls", "EM risk-off", "Russia sanctions spillover", "trade routes", "local banking rules", "payments regulation", "Kazakhstan consumer demand"]},
        "BAP": {"aliases": ["BAP", "Credicorp", "Peru bank"], "triggers": ["Peru GDP", "inflation", "rates", "political instability", "PEN FX", "EM risk-off", "NPLs", "loan growth", "deposit growth"]},
        "OSCR": {"aliases": ["OSCR", "Oscar Health", "health insurance"], "triggers": ["ACA", "Medicaid", "CMS", "reimbursement", "medical loss ratio", "claims trend", "utilization", "investment income", "yields"]},
        "Rubrik": {"aliases": ["Rubrik", "data backup", "ransomware recovery"], "triggers": ["ransomware wave", "data breach", "backup modernization", "cloud migration", "data retention rules", "critical infrastructure mandates"]},
        "COIN": {"aliases": ["COIN", "Coinbase", "crypto exchange"], "triggers": ["Bitcoin", "Ethereum", "crypto rally", "crypto crash", "trading volume", "institutional flows", "ETF flows", "SEC enforcement", "CFTC", "stablecoin law", "market structure bill"]},
        "ETH": {"aliases": ["ETH", "Ethereum", "Ether"], "triggers": ["spot ETH ETF", "ETF inflows/outflows", "staking yield", "staking rules", "validator", "DeFi TVL", "DEX volume", "L2 activity", "SEC", "CFTC", "MiCA"]},
        "HUT": {"aliases": ["HUT", "Hut 8", "bitcoin miner"], "triggers": ["Bitcoin", "risk-on", "ETF flows", "hashrate", "difficulty", "energy costs", "data center hosting", "HPC", "AI compute"]},
        "RIOT": {"aliases": ["RIOT", "Riot Platforms", "bitcoin miner"], "triggers": ["Bitcoin", "ETF flows", "difficulty", "hashrate", "power costs", "mining regulation", "power grid rules"]},
        "CIFR": {"aliases": ["CIFR", "Cipher Mining", "bitcoin miner"], "triggers": ["Bitcoin", "risk-on", "difficulty", "hashrate", "power prices"]},
        "WULF": {"aliases": ["WULF", "TeraWulf", "bitcoin miner", "data center"], "triggers": ["Bitcoin", "electricity costs", "power capacity", "HPC hosting", "AI workloads"]},
        "APLD": {"aliases": ["APLD", "Applied Digital", "data center", "HPC hosting"], "triggers": ["AI data center", "GPU hosting", "colocation demand", "power availability", "grid interconnection", "financing", "project finance", "credit spreads"]},
        "GLXY": {"aliases": ["GLXY", "Galaxy Digital", "crypto financials"], "triggers": ["Bitcoin", "Ethereum", "risk-on/off", "ETF flows", "institutional adoption", "crypto regulation", "market structure"]},
        "Figure": {"aliases": ["Figure", "Figure Technology", "tokenization", "HELOC"], "triggers": ["tokenized securities", "on-chain lending", "digitization of credit", "SEC", "fintech regulation", "bank charters", "mortgage rates", "housing credit"]},
        "AVGO": {"aliases": ["AVGO", "Broadcom"], "triggers": ["AI networking", "switches", "datacenter interconnect", "semiconductor demand", "enterprise capex", "VMware integration", "deal synergies"]},
        "IFX": {"aliases": ["IFX", "Infineon"], "triggers": ["auto demand", "EV semis", "industrial cycle", "EU PMI", "Germany industrial output", "electricity costs", "EU energy policy"]},
        "KLAC": {"aliases": ["KLAC", "KLA", "semiconductor equipment"], "triggers": ["foundry capex", "wafer fab equipment", "process control", "advanced nodes", "yield learning", "HBM packaging", "tools export controls", "China restrictions"]},
        "Xetra_Gold": {"aliases": ["Xetra Gold", "gold ETC", "gold"], "triggers": ["geopolitical risk", "equity selloff", "flight to safety", "real yields", "Fed dovish", "inflation expectations", "DXY down", "USD weakness"]},
    },
}


def _expand_alias_patterns(values) -> list[str]:
    """Expand 'A|B|C' alias notation into lowercase terms."""
    out: list[str] = []
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return out
    for item in values:
        if not item:
            continue
        for part in str(item).split("|"):
            p = part.strip().lower()
            if p and p not in out:
                out.append(p)
    return out


def _iter_ontology_terms(node):
    """Yield flattened lowercase ontology terms from nested dict/list structures."""
    if isinstance(node, dict):
        for v in node.values():
            yield from _iter_ontology_terms(v)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_ontology_terms(item)
    elif isinstance(node, str):
        for t in _expand_alias_patterns(node):
            yield t


_ONTOLOGY_TERMS = list(dict.fromkeys(_iter_ontology_terms(BIT_KEYWORD_ONTOLOGY)))
_NEGATIVE_NOISE_TERMS = set(_expand_alias_patterns(BIT_KEYWORD_ONTOLOGY.get("negative_filters_to_ignore_noise", [])))
_FUND_NAME_TERMS: set[str] = set()
_HOLDING_TICKER_TERMS: set[str] = set()
_HOLDING_NAME_TERMS: set[str] = set()
_ROUTING_ALIAS_TERMS: set[str] = set()
_ROUTING_TRIGGER_TERMS: set[str] = set()
_ROUTING_MACRO_TERMS: set[str] = set()
_ROUTING_HOLDING_INDEX: dict[str, dict] = {}
_ROUTING_CANONICAL_TICKER_MAP = {
    "HINGE_HEALTH": "HNGE",
    "XETRA_GOLD": "XETRA_GOLD",  # may not exist in current portfolio map; kept for keyword routing only
}

# Include dynamic portfolio fund names and holdings names/tickers so the ontology remains aligned
# with the in-code portfolio map even as holdings evolve.
for _fund_name, _fdata in BIT_PORTFOLIO["funds"].items():
    _fund_terms = _expand_alias_patterns([_fund_name])
    _FUND_NAME_TERMS.update(_fund_terms)
    _ONTOLOGY_TERMS.extend(_fund_terms)
    for _h in _fdata["holdings"]:
        _ticker_terms = _expand_alias_patterns([_h.get("ticker", "")])
        _name_terms = _expand_alias_patterns([_h.get("name", "")])
        _HOLDING_TICKER_TERMS.update(_ticker_terms)
        _HOLDING_NAME_TERMS.update(_name_terms)
        _ONTOLOGY_TERMS.extend(_ticker_terms + _name_terms)
        for _theme in _h.get("themes", []):
            _ONTOLOGY_TERMS.extend(_expand_alias_patterns([_theme]))

# Load routing aliases/triggers into ontology and searchable indexes
for _routing_key, _rdef in BIT_INTELLIGENT_FILTER_ROUTING.get("holdings_trigger_keywords", {}).items():
    _canonical = _ROUTING_CANONICAL_TICKER_MAP.get(_routing_key.upper(), _routing_key.split("_")[0].upper())
    _aliases = [a.lower() for a in (_rdef.get("aliases") or []) if a]
    _triggers = [t.lower() for t in (_rdef.get("triggers") or []) if t]
    _ROUTING_HOLDING_INDEX[_canonical] = {"aliases": _aliases, "triggers": _triggers, "routing_key": _routing_key}
    _ROUTING_ALIAS_TERMS.update(_aliases)
    _ROUTING_TRIGGER_TERMS.update(_triggers)
    _ONTOLOGY_TERMS.extend(_aliases + _triggers)
for _terms in BIT_INTELLIGENT_FILTER_ROUTING.get("specific_macro_event_recipes", {}).values():
    _expanded = [t.lower() for t in _terms if t]
    _ROUTING_MACRO_TERMS.update(_expanded)
    _ONTOLOGY_TERMS.extend(_expanded)
for _bucket_terms in BIT_INTELLIGENT_FILTER_ROUTING.get("global_macro_triggers", {}).values():
    _expanded = [t.lower() for t in _bucket_terms if t]
    _ROUTING_MACRO_TERMS.update(_expanded)
    _ONTOLOGY_TERMS.extend(_expanded)
_ONTOLOGY_TERMS = list(dict.fromkeys(t for t in _ONTOLOGY_TERMS if t))


def _contains_term(text: str, term: str) -> bool:
    """Word-aware containment check (fallbacks to substring for punctuated tokens)."""
    if not text or not term:
        return False
    if re.search(r"[a-z0-9]", term):
        pattern = r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])"
        return re.search(pattern, text) is not None
    return term in text


def _extract_trigger_keywords(text: str, limit: int = 20) -> list[str]:
    """
    Hybrid keyword extraction:
    1) explicit driver/category keywords used by fallback logic
    2) ontology terms (fund names, holdings aliases, macro catalysts)
    """
    scored: list[tuple[int, int, str]] = []
    seen: set[str] = set()
    order = 0

    def add_kw(term: str, score: int):
        nonlocal order
        if not term or term in seen:
            return
        seen.add(term)
        scored.append((score, order, term))
        order += 1

    def score_ontology_term(term: str) -> int:
        # Highest priority: exact BIT fund names / portfolio tickers / holding names
        if term in _FUND_NAME_TERMS:
            return 120
        if term in _HOLDING_TICKER_TERMS:
            return 115
        if term in _HOLDING_NAME_TERMS:
            return 108

        if term in _ROUTING_ALIAS_TERMS:
            return 112
        if term in _ROUTING_TRIGGER_TERMS:
            return 102
        if term in _ROUTING_MACRO_TERMS:
            return 98

        # Strong thematic entities and policy terms
        high_signal_substrings = (
            "fomc", "cpi", "pce", "nfp", "payroll", "opec", "opec+",
            "hormuz", "red sea", "suez", "ofac", "export control", "chip ban",
            "stablecoin", "mica", "sec", "cftc", "spot etf", "staking",
            "hbm", "cuda", "euv", "advanced packaging", "data center capex",
            "power price", "grid constraint", "pfof", "payment for order flow",
            "ransomware", "zero-day", "supply chain attack",
        )
        if any(s in term for s in high_signal_substrings):
            return 96

        # Medium priority: compound multi-word ontology phrases
        if " " in term:
            return 82

        # Lower priority: generic single tokens (still useful)
        return 68

    for cat_kws in _DRIVER_KEYWORDS.values():
        for raw in cat_kws:
            if _contains_term(text, raw.lower()):
                add_kw(raw, 55)
    for term in _ONTOLOGY_TERMS:
        if term in _NEGATIVE_NOISE_TERMS:
            continue
        if len(term) < 3 and term not in {"ai", "qt", "qe"}:
            continue
        if _contains_term(text, term):
            add_kw(term, score_ontology_term(term))

    # Promote exact ticker uppercase rendering when possible for portfolio tickers.
    normalized_out: list[str] = []
    ticker_lookup_upper = {t.lower(): t for t in _TICKER_MAP.keys()}
    for _, _, term in sorted(scored, key=lambda x: (-x[0], x[1])):
        rendered = ticker_lookup_upper.get(term, term)
        if rendered not in normalized_out:
            normalized_out.append(rendered)
        if len(normalized_out) >= limit:
            break
    return normalized_out[:limit]


def _match_holding_trigger_routes(text: str) -> list[dict]:
    """Find holding aliases and trigger clusters explicitly present in market text."""
    matches: list[dict] = []
    for ticker, rdef in _ROUTING_HOLDING_INDEX.items():
        alias_hits = [a for a in rdef["aliases"] if _contains_term(text, a)]
        trigger_hits = [t for t in rdef["triggers"] if _contains_term(text, t)]
        if alias_hits or len(trigger_hits) >= 2:
            matches.append({
                "ticker": ticker,
                "alias_hits": alias_hits[:4],
                "trigger_hits": trigger_hits[:6],
                "match_score": (len(alias_hits) * 8) + (len(trigger_hits) * 3),
            })
    return sorted(matches, key=lambda x: x["match_score"], reverse=True)


def _match_specific_macro_recipes(text: str) -> list[dict]:
    out: list[dict] = []
    for key, terms in BIT_INTELLIGENT_FILTER_ROUTING.get("specific_macro_event_recipes", {}).items():
        hits = [t for t in terms if _contains_term(text, t.lower())]
        if hits:
            out.append({"recipe": key, "hits": hits[:6], "score": len(hits)})
    return sorted(out, key=lambda x: x["score"], reverse=True)


def _recipe_driven_override(
    text: str,
    recipe_match: dict | None,
    driver_category: str,
    channels: list[str],
    macro_regime: str,
    holdings: list[dict],
    causal_chain: list[dict],
    score: int,
) -> tuple[str, list[str], str, list[dict], list[dict], int, str | None]:
    """
    Apply deterministic causal-chain templates for high-value macro recipes.
    This improves fallback reasoning quality when LLM is unavailable/rate-limited.
    """
    if not recipe_match:
        return driver_category, channels, macro_regime, holdings, causal_chain, score, None

    recipe = str(recipe_match.get("recipe") or "")
    hits = [str(h).lower() for h in (recipe_match.get("hits") or [])]
    event_type: str | None = None

    def ensure_channels(*chs: str) -> list[str]:
        out = list(channels)
        for ch in chs:
            if ch and ch not in out:
                out.insert(0, ch)
        return list(dict.fromkeys(out))[:3]

    def add_holding(ticker: str, direction: str, horizon: str, mechanism: str, confidence: float):
        if ticker not in _TICKER_MAP:
            return
        h = _TICKER_MAP[ticker]
        holdings.append({
            "name": h["name"],
            "ticker_or_symbol": ticker,
            "fund": h["fund"],
            "direction": direction,
            "time_horizon": horizon,
            "mechanism": mechanism,
            "confidence": confidence,
        })

    # 1) US-Iran nuclear talks / sanctions / Brent risk premium
    if recipe == "iran_nuclear_sanctions_oil":
        oil_up = any(k in text for k in ["oil rises", "brent above", "risk premium", "tanker seizure", "red sea"]) or "brent" in text
        sanctions_relief = "sanctions relief" in text
        driver_category = "energy_and_supply_disruptions"
        macro_regime = "stagflation_oil_shock" if oil_up else "geopolitical_risk_premium"
        channels = ensure_channels("commodities_energy", "inflation_expectations", "rates_duration")
        event_type = "Iran Nuclear / Sanctions Oil Risk Repricing"
        causal_chain = [
            {"step": 1, "from": "US-Iran nuclear talks / sanctions path", "to": "Perceived Iran supply path and geopolitical oil risk premium",
             "why": "Talk progress, setbacks, or sanctions relief expectations directly reprice expected Iranian crude availability and regional risk premia."},
            {"step": 2, "from": "Brent/WTI repricing", "to": "Inflation expectations and real yield path",
             "why": "Energy price moves feed breakevens and can shift expected central bank reaction functions."},
            {"step": 3, "from": "Rates + energy costs", "to": "BIT growth duration and mining economics",
             "why": "Higher yields pressure high-duration tech multiples while electricity and energy-linked costs affect miners and power-sensitive infrastructure names."},
        ]
        if oil_up:
            causal_chain.append({
                "step": 4, "from": "Oil risk premium / Brent > threshold", "to": "Stagflation-style pressure on BIT Tech + Crypto leaders",
                "why": "Oil spikes tighten financial conditions and raise operating cost pressure for crypto miners and data center economics."
            })
            add_holding("BE", "UP", "weeks", "Higher energy-system stress can increase demand for distributed/on-site power solutions.", 0.66)
            add_holding("IREN", "DOWN", "weeks", "Energy and power economics are key inputs to mining and AI-hosting economics.", 0.76)
            add_holding("HUT", "DOWN", "weeks", "Mining margins compress when energy/power costs rise and risk sentiment weakens.", 0.78)
            add_holding("RIOT", "DOWN", "weeks", "Oil/geopolitical shocks often pressure miner beta via power-cost and risk-off transmission.", 0.72)
            add_holding("NVDA", "DOWN", "weeks", "Higher real yields from inflation/risk premium repricing pressure high-duration AI semis.", 0.63)
        else:
            # sanctions relief / supply normalization can reduce oil, easing inflation pressure
            causal_chain.append({
                "step": 4, "from": "Sanctions relief / supply normalization expectations", "to": "Lower oil inflation impulse and less hawkish rates pressure",
                "why": "Improved supply expectations can compress oil risk premia, easing inflation pressure and supporting risk assets."
            })
            add_holding("BE", "MIXED", "months", "Lower energy stress may reduce urgency for distributed generation, but broader risk sentiment may improve.", 0.52)
            add_holding("IREN", "UP", "weeks", "Lower energy/oil inflation pressure supports both rates and mining economics at the margin.", 0.64)
            add_holding("NVDA", "UP", "weeks", "Lower inflation/rates pressure can support duration-sensitive AI leaders.", 0.58)
        score = min(score + 12, 95)

    # 2) Tariff turmoil / trade volatility / Supreme Court tariff ruling
    elif recipe == "tariff_turmoil_trade_volatility":
        driver_category = "trade_and_tariffs"
        macro_regime = "tariff_inflation_shock"
        channels = ensure_channels("inflation_expectations", "rates_duration", "equities_factor")
        event_type = "Tariff / Trade Policy Shock"
        causal_chain = [
            {"step": 1, "from": "Tariff policy uncertainty / court ruling", "to": "Import cost and supply-chain policy uncertainty",
             "why": "Trade policy changes directly affect landed costs, sourcing decisions, and corporate planning visibility."},
            {"step": 2, "from": "Trade-cost uncertainty", "to": "Inflation path and Fed reaction function repricing",
             "why": "Tariffs can generate near-term inflation pressure and reinforce higher-for-longer expectations."},
            {"step": 3, "from": "Rates and risk premium repricing", "to": "BIT growth/semis and trade-sensitive holdings",
             "why": "Duration-sensitive tech and cross-border business models re-rate when policy uncertainty and yields rise."},
        ]
        add_holding("NVDA", "DOWN", "months", "Tariff/trade volatility can worsen supply-chain and valuation uncertainty for semis leaders.", 0.67)
        add_holding("TSM", "DOWN", "months", "Trade frictions and export-policy volatility pressure foundry demand visibility and geopolitics premium.", 0.64)
        add_holding("AUTO1", "DOWN", "months", "Cross-border auto/consumer demand and financing conditions are vulnerable to tariff volatility.", 0.63)
        add_holding("BABA", "DOWN", "months", "US-China trade/policy volatility weighs on sentiment and ADR/geopolitics risk premium.", 0.69)
        score = min(score + 10, 95)

    # 3) FOMC March 17–18, 2026 / SEP / dot plot
    elif recipe == "fomc_march_2026_sep_dot_plot":
        driver_category = "monetary_policy_central_banks"
        dovish = any(k in text for k in ["rate cut", "dovish", "cuts", "lower rates", "pause"]) and not any(k in text for k in ["hike", "higher for longer", "hawkish"])
        macro_regime = "policy_surprise_dovish" if dovish else "policy_surprise_hawkish"
        channels = ensure_channels("rates_duration", "equities_factor", "crypto_liquidity")
        event_type = "FOMC / SEP / Dot Plot"
        causal_chain = [
            {"step": 1, "from": "March FOMC / SEP / dot plot", "to": "Policy-path expectations and terminal-rate distribution",
             "why": "The statement, SEP projections, and dot plot jointly reprice the policy path and market-implied cuts/hikes."},
            {"step": 2, "from": "Real yields and duration repricing", "to": "Growth equity multiple compression/expansion",
             "why": "BIT portfolios have significant duration exposure via AI, software, fintech, and internet leaders."},
            {"step": 3, "from": "Policy-path signal", "to": "Crypto liquidity and miner beta",
             "why": "Looser financial conditions typically support crypto risk appetite; tighter conditions weigh on miners and trading activity."},
        ]
        if dovish:
            add_holding("NVDA", "UP", "days", "Dovish dot-plot / SEP repricing lowers real yields and supports duration-sensitive AI leaders.", 0.78)
            add_holding("IREN", "UP", "days", "Dovish policy repricing improves risk sentiment and crypto/AI-infra beta.", 0.76)
            add_holding("HOOD", "UP", "days", "Lower-rate/risk-on conditions typically support trading activity and valuation multiples.", 0.72)
            add_holding("LMND", "UP", "days", "Duration-sensitive insurtech valuation benefits from lower real-yield expectations.", 0.68)
            add_holding("HUT", "UP", "days", "Dovish macro repricing often supports crypto beta and miner equities.", 0.73)
        else:
            add_holding("NVDA", "DOWN", "days", "Hawkish SEP/dot-plot repricing raises real yields and pressures duration-heavy semis.", 0.80)
            add_holding("IREN", "DOWN", "days", "Tighter conditions pressure both crypto beta and high-duration infrastructure multiples.", 0.77)
            add_holding("HOOD", "DOWN", "days", "Higher-for-longer rates can pressure valuation and risk-taking activity.", 0.70)
            add_holding("LMND", "DOWN", "days", "Hawkish rates repricing compresses long-duration growth multiples.", 0.70)
            add_holding("HUT", "DOWN", "days", "Risk-off / tighter conditions typically weigh on miners via crypto-beta channel.", 0.72)
        score = min(score + 14, 95)

    # 4) US crypto market structure bill / CFTC / stablecoin framework
    elif recipe == "us_crypto_market_structure_bill":
        driver_category = "crypto_policy_and_market_structure"
        restrictive = any(k in text for k in ["ban", "restrict", "crackdown", "enforcement only", "reject"])
        macro_regime = "disinflation_soft_landing" if not restrictive else "geopolitical_risk_premium"
        channels = ensure_channels("crypto_liquidity", "equities_risk_on_off", "rates_duration")
        event_type = "US Crypto Market Structure Legislation"
        causal_chain = [
            {"step": 1, "from": "US crypto market-structure / stablecoin bill developments", "to": "Regulatory clarity on exchange, custody, and token classification",
             "why": "Legislative language can define jurisdiction, market conduct rules, and operating certainty across the crypto ecosystem."},
            {"step": 2, "from": "Regulatory clarity (or restriction)", "to": "Institutional participation, volumes, and asset-pricing confidence",
             "why": "Constructive rule clarity typically supports participation and liquidity; restrictive outcomes reduce activity and risk appetite."},
            {"step": 3, "from": "Crypto liquidity and volumes", "to": "BIT crypto leaders, miners, and Robinhood/Coinbase-linked exposures",
             "why": "Mining equities, exchanges, and crypto financials are highly sensitive to crypto price/volume and policy posture."},
        ]
        if restrictive:
            for t in ["COIN", "HUT", "RIOT", "CIFR", "WULF", "IREN", "GLXY", "HOOD"]:
                add_holding(t, "DOWN", "weeks", "Restrictive crypto-market-structure outcomes would pressure liquidity, volumes, and crypto-beta exposures.", 0.70 if t in {"COIN","HUT","IREN"} else 0.64)
            add_holding("ETH", "DOWN", "weeks", "Restrictive policy posture typically weighs on ETH participation and DeFi sentiment.", 0.66)
        else:
            for t in ["COIN", "HUT", "RIOT", "CIFR", "WULF", "IREN", "GLXY", "HOOD"]:
                add_holding(t, "UP", "weeks", "Constructive market-structure clarity can support crypto liquidity, volumes, and valuation multiples.", 0.74 if t in {"COIN","HUT","IREN"} else 0.68)
            add_holding("ETH", "UP", "weeks", "Constructive legislation improves institutional confidence and market-structure visibility for ETH.", 0.70)
        score = min(score + 16, 95)

    return driver_category, channels, macro_regime, holdings, causal_chain, score, event_type


def _portfolio_context_block() -> str:
    """Generate the compact portfolio context block injected into the LLM system prompt."""
    lines = ["BIT CAPITAL FUND STRUCTURE AND HOLDINGS:\n"]
    for fund, fdata in BIT_PORTFOLIO["funds"].items():
        lines.append(f"┌─ {fund}")
        lines.append(f"│  Style: {fdata['style']}")
        holdings_str = ", ".join(
            f"{h['ticker']} ~{h['weight']}% [{', '.join(h['themes'][:2])}]"
            for h in fdata["holdings"]
        )
        lines.append(f"│  Holdings: {holdings_str}")
        lines.append("")
    return "\n".join(lines)


def _taxonomy_context_block() -> str:
    """Generate the taxonomy reference block injected into the LLM system prompt."""
    return f"""CLASSIFICATION TAXONOMY (use EXACTLY these values in your output):

driver_category (pick the primary macro/thematic driver):
  {chr(10).join(f"  • {v}" for v in TAXONOMY["driver_category"])}

market_channel (pick 1-3 transmission channels to BIT holdings):
  {chr(10).join(f"  • {v}" for v in TAXONOMY["market_channel"])}

macro_regime (which regime does this event reinforce or challenge?):
  {chr(10).join(f"  • {v}" for v in TAXONOMY["macro_regime"])}

event_cadence (what is the timing structure of the event?):
  {chr(10).join(f"  • {v}" for v in TAXONOMY["event_cadence"])}
"""


def _keyword_intelligence_block() -> str:
    """Compact keyword intelligence block for the LLM prompt."""
    funds = BIT_KEYWORD_ONTOLOGY["bit_brand_and_funds"]["fund_sections"]
    brand_terms = BIT_KEYWORD_ONTOLOGY["bit_brand_and_funds"]["brand"]
    core = BIT_KEYWORD_ONTOLOGY["core_holdings_keywords_by_fund"]
    macro = BIT_KEYWORD_ONTOLOGY["macro_and_rates"]
    ai_semis = BIT_KEYWORD_ONTOLOGY["ai_semis_and_datacenters"]
    crypto = BIT_KEYWORD_ONTOLOGY["crypto_regulation_and_market_structure"]
    fintech = BIT_KEYWORD_ONTOLOGY["fintech_insurtech_banking"]
    energy = BIT_KEYWORD_ONTOLOGY["energy_oil_geopolitics"]
    recipes = BIT_INTELLIGENT_FILTER_ROUTING["specific_macro_event_recipes"]
    return f"""KEYWORD INTELLIGENCE (optimize trigger_keywords extraction using these aliases/themes):
BIT brand / PMs: {", ".join(brand_terms)}
BIT funds: {", ".join(funds)}
Core holdings aliases (Tech): {", ".join(core["technology_leaders_core"][:10])}
Core holdings aliases (Crypto): {", ".join(core["crypto_leaders_core"][:10])}
Core holdings aliases (Fintech): {", ".join(core["fintech_leaders_core"][:10])}
Macro & rates: {", ".join(macro["central_bank"] + macro["data_releases"] + macro["market_vars"])}
Energy / geopolitics: {", ".join(energy["entities"] + energy["chokepoints_shipping"] + energy["oil_markets"])}
AI / semis / data centers: {", ".join(ai_semis["ai_compute"] + ai_semis["supply_chain_controls"] + ai_semis["power_grid"])}
Crypto regulation & market structure: {", ".join(crypto["regulators"] + crypto["market_terms"])}
Fintech / banking / insurance: {", ".join(fintech["fintech"] + fintech["banking"] + fintech["insurance"])}
Specific macro recipes to prioritize when explicitly matched:
- US–Iran / sanctions relief / oil risk premium: {", ".join(recipes["iran_nuclear_sanctions_oil"][:6])}
- Tariff turmoil / trade volatility: {", ".join(recipes["tariff_turmoil_trade_volatility"])}
- March FOMC / SEP / dot plot: {", ".join(recipes["fomc_march_2026_sep_dot_plot"])}
- US crypto market structure bill / CFTC / stablecoin: {", ".join(recipes["us_crypto_market_structure_bill"])}
Noise filters (usually IGNORE unless direct macro/commodity/regulatory link): {", ".join(BIT_KEYWORD_ONTOLOGY["negative_filters_to_ignore_noise"])}
Trigger keyword rules:
- Prefer exact entities/tickers/fund names that appear in the market text.
- Include 5-12 high-signal keywords; avoid generic filler.
- Use aliases only when explicitly present (e.g. 'NVDA' or 'NVIDIA'; do not invent unseen aliases).
"""


# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT — injected once per session; references taxonomy + full portfolio
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = f"""You are the "Equity & Macro Relevance Filter" for BIT Capital's Polymarket Signal Scanner.

ROLE: Given a Polymarket prediction market, determine if it is economically relevant to BIT Capital's investable universe and portfolio exposures. If relevant, explain the full causal chain (driver → transmission channel → macro regime → affected holdings → directionality).

STRICTNESS: Be strict. The majority of Polymarket markets are NOT relevant. Only pass through markets with a plausible, direct or first-order-indirect transmission to BIT Capital holdings within 12 months. Speculative distant connections should score low.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{_portfolio_context_block()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FUND-LEVEL SENSITIVITIES SUMMARY:
• BIT Global Technology Leaders: Most sensitive to (1) AI capex cycle, (2) Fed rate path, (3) crypto/BTC price, (4) clean energy policy. IREN + Bloom + miners = large weight — power costs and BTC matter enormously.
• BIT Global Crypto Leaders: Directly tied to BTC/ETH price, mining economics, crypto regulation. Any US crypto legislation, SEC/CFTC action, BTC halving dynamics, or miner energy costs = potentially ACTIONABLE.
• BIT Defensive Growth: Sensitive to enterprise IT spend, cybersecurity incidents, China-US relations. Lower duration than Tech Leaders, but BABA carries significant China political risk.
• BIT Global Multi Asset: Semiconductor cycle (KLAC/AVGO) and cybersecurity (CRWD) — more stable but exposed to chip export restrictions.
• BIT Global Fintech Leaders: EM risk appetite, local interest rates, fintech regulation. BAP sensitive to LatAm political risk; KSPI to Central Asia geopolitics.
• BIT Global Leaders: Ad cycle sensitivity (META/RDDT). Falls with consumer recession risk or digital ad market contraction.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{_taxonomy_context_block()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{_keyword_intelligence_block()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RELEVANCE SCORING RUBRIC:
• ACTIONABLE (score ≥70): Clear, high-conviction causal chain to ≥1 BIT holding. Event plausibly moves the holding within 0-3 months. At least one channel with strong transmission.
• MONITOR (score 35-69): Plausible but indirect, multi-step, or slow-moving. Could become actionable if probability shifts significantly. Watch closely.
• IGNORE (score <35): No clear economic linkage to BIT portfolio. Includes sports, entertainment, unrelated geopolitics, or markets where BIT has no exposure path.

CAUSAL CHAIN TEMPLATES (these are the canonical patterns — prioritize specificity):

1. OIL SUPPLY SHOCK → STAGFLATION:
   oil supply disruption → Brent ↑ → inflation expectations ↑ → Fed holds longer → yields ↑ → growth multiples ↓ → IREN/NVDA/GOOGL/HOOD DOWN; Bloom Energy UP (energy security)
   Also: energy costs ↑ → miner P&L squeeze → HUT/RIOT/IREN/WULF/CIFR DOWN
   Regime: stagflation_oil_shock

2. FED PATH REPRICING (hawkish surprise):
   economic data beats OR tariff inflation → Fed higher for longer → real yields ↑ → equities_factor DOWN (growth vs value) → ALL high-duration BIT names repriced DOWN
   Most sensitive: IREN, NVDA, LMND, HOOD, AUTO1 (highest duration exposure)
   Regime: policy_surprise_hawkish OR reacceleration_overheating

3. FED PATH REPRICING (dovish pivot):
   inflation falling + weak jobs → Fed cuts → real yields ↓ → risk-on → growth equities UP → crypto UP (BTC/ETH ↑) → miners UP → BIT Tech + Crypto funds rally
   Regime: policy_surprise_dovish OR disinflation_soft_landing

4. AI / SEMIS EXPORT CONTROLS:
   US tightens chip export rules → NVDA China revenue risk → TSMC order risk → MU HBM pricing uncertainty → AI capex pacing → IREN/APLD data center growth slows
   Regime: geopolitical_risk_premium → technology_cycle_ai_semis
   Channel: equities_factor + rates_duration

5. CRYPTO REGULATION (constructive):
   SEC/CFTC clarity OR stablecoin bill passes → institutional crypto participation ↑ → BTC/ETH ↑ → miners P&L ↑ → HUT/RIOT/IREN/WULF/CIFR UP → HOOD crypto volume UP → GLXY UP
   Regime: disinflation_soft_landing (risk-on enabler)
   Channel: crypto_liquidity

6. CRYPTO REGULATION (restrictive):
   crackdown, ban, or negative court ruling → BTC/ETH ↓ → mining economics deteriorate → entire Crypto Leaders fund DOWN
   Channel: crypto_liquidity (negative)

7. TAIWAN / CHINA GEOPOLITICS:
   military escalation OR sanctions expansion → TSMC supply chain risk → global chip shortage → NVDA/MU downstream risk → AI capex disruption → IREN/APLD AI infrastructure pipeline at risk
   Regime: geopolitical_risk_premium
   Channel: equities_risk_on_off + equities_factor

8. TARIFF INFLATION SHOCK:
   tariff announcement → import price inflation → CPI re-rates → Fed stays hawkish → tech multiples DOWN → supply chain costs ↑ → Auto1 (European exposure to US tariffs)
   Regime: tariff_inflation_shock
   Channel: inflation_expectations → rates_duration → equities_factor

9. CYBERSECURITY INCIDENT / MANDATE:
   major breach OR government mandate → enterprise security urgency ↑ → PANW platform consolidation ↑ → CRWD endpoint spend ↑
   Regime: unaffected (idiosyncratic)
   Channel: equities_factor (positive for PANW/CRWD)

10. CONSUMER / AD CYCLE:
    recession risk OR consumer slowdown → digital ad spend contracts → META/RDDT (BIT Global Leaders) DOWN; GOOGL DOWN
    Regime: growth_scare_recession
    Channel: equities_risk_on_off → equities_factor

AGENDA WATCHLIST HINTING:
- Agenda hint matches are SOFT PRIORITY SIGNALS — use only when market text explicitly contains matched entities/phrases.
- Treat as normalization and scoring boosters, never as standalone proof of relevance.
- When matched, strengthen causal chain specificity using the transmission templates above.

OUTPUT: Return ONLY valid JSON with this exact schema (no markdown, no preamble):
{{
  "market_id": "...",
  "relevance_label": "IGNORE|MONITOR|ACTIONABLE",
  "relevance_score": 0-100,
  "one_sentence_verdict": "Concise analyst-quality verdict linking event → channel → BIT holding(s)",
  "driver_category": "exactly_one_value_from_taxonomy",
  "market_channels": ["1-3 values from market_channel taxonomy"],
  "macro_regime": "exactly_one_value_from_taxonomy",
  "event_cadence": "exactly_one_value_from_taxonomy",
  "event_type": "human-readable short label (e.g. 'Fed rate decision', 'BTC mining regulation')",
  "primary_channels": ["legacy field — use same as market_channels for compatibility"],
  "key_geographies": ["US", "China", "Middle East", "LatAm", "Europe", "Global", "Taiwan", "Kazakhstan"],
  "trigger_keywords": ["keyword1", "keyword2", "..."],
  "causal_chain": [
    {{"step": 1, "from": "...", "to": "...", "why": "..."}}
  ],
  "affected_holdings": [
    {{
      "name": "...",
      "ticker_or_symbol": "...",
      "fund": "BIT fund name",
      "direction": "UP|DOWN|MIXED",
      "time_horizon": "days|weeks|months|quarters",
      "mechanism": "specific mechanism sentence",
      "confidence": 0.0-1.0
    }}
  ],
  "portfolio_theme_fit": ["theme string from BIT themes"],
  "fund_level_impact": [
    {{"fund": "BIT fund name", "net_direction": "UP|DOWN|MIXED", "magnitude": "HIGH|MEDIUM|LOW", "rationale": "..."}}
  ],
  "what_to_watch_next": [
    {{"signal": "...", "metric": "...", "threshold": "...", "why": "..."}}
  ],
  "red_flags_or_unknowns": ["..."]
}}"""


# ══════════════════════════════════════════════════════════════════════════════
# USER PROMPT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _extract_text_evidence_query_terms(market: dict) -> list[str]:
    q = str(market.get("question", "") or "")
    desc = str(market.get("description", "") or "")
    raw_text = " ".join([q, desc])[:2000]
    keywords = _extract_trigger_keywords(raw_text, limit=8)
    candidates = []
    for k in keywords:
        kk = str(k).strip()
        if not kk:
            continue
        if len(kk) < 3:
            continue
        candidates.append(kk)
    # add high-signal terms from question text
    for t in re.findall(r"\b[A-Z]{2,5}\b", q):
        if t not in candidates:
            candidates.append(t)
    return candidates[:10]


def retrieve_external_evidence_for_market(market: dict, max_items: int | None = None) -> list[dict]:
    """Best-effort public news evidence retriever (Google News RSS)."""
    if not EVIDENCE_RETRIEVER_ENABLED:
        return []
    try:
        import requests
    except Exception:
        return []

    max_items = max_items or EVIDENCE_RETRIEVER_MAX_ITEMS
    q = str(market.get("question", "") or "").strip()
    if not q:
        return []

    terms = _extract_text_evidence_query_terms(market)
    query_parts = [q]
    if terms:
        query_parts.append(" ".join(terms[:4]))
    query = " ".join(query_parts)
    url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200 or not resp.text:
            return []
        root = ET.fromstring(resp.text)
    except Exception:
        return []

    out: list[dict] = []
    for item in root.findall(".//item")[: max_items * 2]:
        title = unescape((item.findtext("title") or "").strip())
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = unescape((item.findtext("description") or "").strip())
        snippet = re.sub(r"<[^>]+>", " ", desc)
        snippet = re.sub(r"\s+", " ", snippet).strip()
        if not title:
            continue
        out.append({
            "source": "Google News RSS",
            "source_type": "news_rss",
            "headline": title[:280],
            "snippet": snippet[:420],
            "url": link[:1000] if link else None,
            "published_at": pub or None,
            "evidence_kind": "news_snippet",
            "query_used": query[:300],
        })
        if len(out) >= max_items:
            break
    return out


def build_user_prompt(market: dict, evidence_items: list[dict] | None = None) -> str:
    """Build the market-specific user prompt."""
    odds_history = market.get("odds_history", [])
    if isinstance(odds_history, str):
        try:
            odds_history = json.loads(odds_history)
        except Exception:
            odds_history = []

    odds_change_7d = None
    if len(odds_history) >= 2:
        try:
            odds_change_7d = round(float(odds_history[-1]["yes"]) - float(odds_history[-2]["yes"]), 3)
        except Exception:
            pass

    subcategories = market.get("subcategories", [])
    if isinstance(subcategories, str):
        try:
            subcategories = json.loads(subcategories)
        except Exception:
            subcategories = []

    agenda_hints = match_agenda_hints(
        market.get("question", ""),
        market.get("description", ""),
        market.get("category", ""),
        market.get("subcategories", ""),
    )

    polymarket_data = {
        "id": market.get("id", ""),
        "question": market.get("question", ""),
        "description": market.get("description", "")[:500],
        "category": market.get("category", ""),
        "subcategories": subcategories[:10],
        "end_date": market.get("end_date", ""),
        "current_odds": {
            "YES": market.get("current_yes", 0.5),
            "NO": market.get("current_no", 0.5),
        },
        "odds_history_tail": odds_history[-5:],
        "odds_change_7d_pp": round(odds_change_7d * 100, 2) if odds_change_7d is not None else None,
        "volume_usd": market.get("volume", 0),
        "liquidity_usd": market.get("liquidity", 0),
    }

    prompt_parts = [
        "Analyze the following Polymarket market for BIT Capital relevance.",
        "Apply the professional taxonomy strictly. Agenda hints below are soft signals only if text explicitly matches.\n",
        f"MARKET DATA:\n{json.dumps(polymarket_data, indent=2)}",
    ]

    if agenda_hints.get("is_agenda_relevant"):
        prompt_parts.append(
            f"\nAGENDA_HINT_MATCHES (soft — use only if market text explicitly matches):\n"
            f"{json.dumps(agenda_hints, indent=2)}"
        )

    if evidence_items:
        safe_evidence = [
            {
                "source": e.get("source"),
                "headline": e.get("headline"),
                "snippet": e.get("snippet"),
                "published_at": e.get("published_at"),
                "url": e.get("url"),
            }
            for e in evidence_items[:6]
            if isinstance(e, dict)
        ]
        prompt_parts.append(
            "EXTERNAL EVIDENCE PACK (optional, use only if directly relevant to the market text and resolution mechanics):\n"
            + json.dumps(safe_evidence, indent=2)
        )

    return "\n\n".join(prompt_parts)


# ══════════════════════════════════════════════════════════════════════════════
# LLM CALL
# ══════════════════════════════════════════════════════════════════════════════

class LLMRateLimitExceeded(RuntimeError):
    """LLM provider rate limit reached."""

    def __init__(self, message: str, *, per_day: bool = False, retry_after_secs: float | None = None):
        super().__init__(message)
        self.per_day = per_day
        self.retry_after_secs = retry_after_secs


def _parse_retry_after_secs_from_error_text(msg: str) -> float | None:
    txt = (msg or "").lower()
    # Examples: "Please try again in 2.06s", "950ms", "26m28.032s"
    m = re.search(r"please try again in ([0-9.]+)ms", txt)
    if m:
        return max(float(m.group(1)) / 1000.0, 0.2)
    m = re.search(r"please try again in ([0-9.]+)s", txt)
    if m:
        return max(float(m.group(1)), 0.2)
    m = re.search(r"please try again in ([0-9]+)m([0-9.]+)s", txt)
    if m:
        return max((int(m.group(1)) * 60) + float(m.group(2)), 1.0)
    return None


def _raise_if_rate_limited(exc: Exception) -> None:
    msg = str(exc)
    low = msg.lower()
    if (
        "rate_limit_exceeded" not in low
        and "rate limit reached" not in low
        and "quota exceeded" not in low
        and "you exceeded your current quota" not in low
    ):
        return
    per_day = (
        "tokens per day" in low
        or "(tpd)" in low
        or " tpd" in low
        or "perday" in low
        or "requestsperday" in low
        or "freetier" in low and "requestsperday" in low
        or "limit: 0" in low
    )
    retry_after = _parse_retry_after_secs_from_error_text(msg)
    raise LLMRateLimitExceeded(msg, per_day=per_day, retry_after_secs=retry_after)


def _should_skip_llm_for_market(market: dict) -> bool:
    """Cheap prefilter to avoid spending LLM tokens on obvious non-finance/noise markets."""
    q = str(market.get("question", "") or "").lower()
    d = str(market.get("description", "") or "").lower()
    c = str(market.get("category", "") or "").lower()
    text = " | ".join([q, d, c])

    finance_hooks = [
        "fed", "fomc", "cpi", "pce", "nfp", "inflation", "rates", "yield", "tariff",
        "iran", "sanctions", "opec", "brent", "wti", "oil", "bitcoin", "ethereum",
        "crypto", "stablecoin", "cftc", "sec", "nvidia", "nvda", "tsm", "tsmc",
        "micron", "earnings", "guidance", "amazon", "apple", "microsoft", "google",
        "alphabet", "meta", "coinbase", "robinhood", "datadog", "lemonade", "crowdstrike",
        "palo alto", "broadcom", "trade war", "supreme court tariff"
    ]
    if any(k in text for k in finance_hooks):
        return False

    obvious_noise_markers = [
        "vs.", " vs ", "over/under", "both teams to score", "map 2 winner", "first blood",
        "season", "win the traitors", "award", "celebrity", "dating", "melodifestivalen"
    ]
    noisy_categories = ["sports", "entertainment", "culture", "gaming", "esports"]
    if c in noisy_categories or any(n in text for n in obvious_noise_markers):
        return True
    return False

def analyze_with_llm(market: dict) -> dict:
    """Analyze a single market using Groq/Claude; fall back to rule-based if unavailable."""
    if time.time() < LLM_COOLDOWN_UNTIL_TS:
        return rule_based_fallback(market)
    if not ((GROQ_AVAILABLE and GROQ_API_KEY) or (GEMINI_AVAILABLE and GEMINI_API_KEY) or (ANTHROPIC_AVAILABLE and ANTHROPIC_API_KEY)):
        return rule_based_fallback(market)

    def _parse_llm_json(content: str) -> dict:
        content = (content or "").strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        result = json.loads(content)
        result["market_id"] = market["id"]
        if "market_channels" in result and "primary_channels" not in result:
            result["primary_channels"] = result["market_channels"]
        if "primary_channels" in result and "market_channels" not in result:
            result["market_channels"] = result["primary_channels"]
        return result

    evidence_items = retrieve_external_evidence_for_market(market)
    user_prompt = build_user_prompt(market, evidence_items=evidence_items)
    gemini_model_raw = str(GEMINI_MODEL or "").strip() or "gemini-2.0-flash"
    gemini_model_bare = gemini_model_raw.replace("models/", "", 1) if gemini_model_raw.startswith("models/") else gemini_model_raw
    gemini_model_candidates = []
    for cand in [
        gemini_model_bare,
        f"models/{gemini_model_bare}",
        gemini_model_raw,
    ]:
        if cand and cand not in gemini_model_candidates:
            gemini_model_candidates.append(cand)

    def _with_evidence(result: dict) -> dict:
        if evidence_items:
            result["evidence_items"] = evidence_items
        return result

    attempts = max(0, LLM_RETRY_MAX) + 1
    for attempt in range(1, attempts + 1):
        last_rate_limit: LLMRateLimitExceeded | None = None
        had_non_rate_provider_error = False
        # Priority: Groq, Gemini, Anthropic, then fallback
        if GROQ_AVAILABLE and GROQ_API_KEY:
            try:
                client = Groq(api_key=GROQ_API_KEY)
                response = client.chat.completions.create(
                    model=GROQ_MODEL,
                    temperature=0.1,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                content = (response.choices[0].message.content or "").strip()
                return _with_evidence(_parse_llm_json(content))
            except json.JSONDecodeError as e:
                print(f"[ANALYSIS] JSON parse error for market {market.get('id')} (groq): {e}")
                return rule_based_fallback(market)
            except Exception as e:
                try:
                    _raise_if_rate_limited(e)
                except LLMRateLimitExceeded as rl:
                    last_rate_limit = rl
                    if not rl.per_day and attempt < attempts:
                        sleep_s = rl.retry_after_secs if rl.retry_after_secs is not None else (LLM_RETRY_BASE_SLEEP_SECS * attempt)
                        sleep_s = min(max(sleep_s, 0.2), 8.0)
                        print(f"[ANALYSIS] Groq rate-limited (TPM) for market {market.get('id')}; retrying in {sleep_s:.2f}s (attempt {attempt}/{attempts})")
                        time.sleep(sleep_s)
                        continue
                    if rl.per_day:
                        print(f"[ANALYSIS] Groq TPD exhausted for market {market.get('id')}; trying next provider.")
                    else:
                        print(f"[ANALYSIS] Groq LLM error for market {market.get('id')}: {e}")
                else:
                    print(f"[ANALYSIS] Groq LLM error for market {market.get('id')}: {e}")
                    had_non_rate_provider_error = True

        if GEMINI_AVAILABLE and GEMINI_API_KEY:
            try:
                google_generativeai.configure(api_key=GEMINI_API_KEY)
                gemini_last_exc = None
                for gem_model_name in gemini_model_candidates:
                    try:
                        model = google_generativeai.GenerativeModel(
                            model_name=gem_model_name,
                            system_instruction=SYSTEM_PROMPT,
                        )
                        response = model.generate_content(
                            user_prompt,
                            generation_config={"temperature": 0.1},
                        )
                        content = getattr(response, "text", "") or ""
                        if not content and getattr(response, "candidates", None):
                            parts = []
                            for cand in response.candidates:
                                for p in getattr(getattr(cand, "content", None), "parts", []) or []:
                                    txt = getattr(p, "text", None)
                                    if txt:
                                        parts.append(txt)
                            content = "\n".join(parts)
                        return _with_evidence(_parse_llm_json((content or "").strip()))
                    except Exception as ge:
                        gemini_last_exc = ge
                        msg = str(ge).lower()
                        if "unexpected model name format" in msg or "not found" in msg or "unsupported" in msg:
                            continue
                        raise
                if gemini_last_exc is not None:
                    raise gemini_last_exc
            except json.JSONDecodeError as e:
                print(f"[ANALYSIS] JSON parse error for market {market.get('id')} (gemini): {e}")
                return rule_based_fallback(market)
            except Exception as e:
                try:
                    _raise_if_rate_limited(e)
                except LLMRateLimitExceeded as rl:
                    last_rate_limit = rl
                    if not rl.per_day and attempt < attempts:
                        sleep_s = rl.retry_after_secs if rl.retry_after_secs is not None else (LLM_RETRY_BASE_SLEEP_SECS * attempt)
                        sleep_s = min(max(sleep_s, 0.2), 8.0)
                        print(f"[ANALYSIS] Gemini rate-limited (TPM) for market {market.get('id')}; retrying in {sleep_s:.2f}s (attempt {attempt}/{attempts})")
                        time.sleep(sleep_s)
                        continue
                    if rl.per_day:
                        print(f"[ANALYSIS] Gemini TPD exhausted for market {market.get('id')}; trying next provider.")
                    else:
                        print(f"[ANALYSIS] Gemini LLM error for market {market.get('id')}: {e}")
                else:
                    print(f"[ANALYSIS] Gemini LLM error for market {market.get('id')}: {e}")
                    had_non_rate_provider_error = True

        if ANTHROPIC_AVAILABLE and ANTHROPIC_API_KEY:
            try:
                client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                response = client.messages.create(
                    model=ANTHROPIC_MODEL,
                    max_tokens=2500,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                content = response.content[0].text.strip()
                return _with_evidence(_parse_llm_json(content))
            except json.JSONDecodeError as e:
                print(f"[ANALYSIS] JSON parse error for market {market.get('id')} (anthropic): {e}")
                return rule_based_fallback(market)
            except Exception as e:
                try:
                    _raise_if_rate_limited(e)
                except LLMRateLimitExceeded as rl:
                    last_rate_limit = rl
                    if not rl.per_day and attempt < attempts:
                        sleep_s = rl.retry_after_secs if rl.retry_after_secs is not None else (LLM_RETRY_BASE_SLEEP_SECS * attempt)
                        sleep_s = min(max(sleep_s, 0.2), 8.0)
                        print(f"[ANALYSIS] Claude rate-limited (TPM) for market {market.get('id')}; retrying in {sleep_s:.2f}s (attempt {attempt}/{attempts})")
                        time.sleep(sleep_s)
                        continue
                    if rl.per_day:
                        print(f"[ANALYSIS] Claude TPD exhausted for market {market.get('id')}.")
                    else:
                        print(f"[ANALYSIS] Claude LLM error for market {market.get('id')}: {e}")
                else:
                    print(f"[ANALYSIS] Claude LLM error for market {market.get('id')}: {e}")
                    had_non_rate_provider_error = True

        if last_rate_limit and last_rate_limit.per_day and not had_non_rate_provider_error:
            raise last_rate_limit
        return rule_based_fallback(market)
    return rule_based_fallback(market)


# ══════════════════════════════════════════════════════════════════════════════
# RULE-BASED FALLBACK — mirrors the taxonomy structure used in the LLM prompt
# ══════════════════════════════════════════════════════════════════════════════

# driver_category keyword mapping
_DRIVER_KEYWORDS: dict[str, list[str]] = {
    "monetary_policy_central_banks": [
        "federal reserve", "fed", "fomc", "ecb", "boj", "rate cut", "rate hike",
        "interest rate", "monetary policy", "qt", "quantitative tightening", "forward guidance",
        "fed chair", "powell", "lagarde", "fed pivot", "terminal rate",
    ],
    "inflation_and_prices": [
        "inflation", "cpi", "core pce", "ppi", "deflation", "disinflation",
        "price index", "cost of living", "wage growth", "core inflation",
    ],
    "growth_and_labor": [
        "gdp", "recession", "nfp", "jobs report", "unemployment", "pmi", "ism",
        "consumer confidence", "retail sales", "growth", "earnings recession",
    ],
    "fiscal_and_sovereign": [
        "debt ceiling", "deficit", "us budget", "sovereign debt", "fiscal", "treasury auction",
        "doge", "government spending", "continuing resolution",
    ],
    "trade_and_tariffs": [
        "tariff", "trade war", "wto", "customs duty", "import tax", "trade deal",
        "cbp", "universal tariff", "section 232", "section 301", "trade policy",
    ],
    "geopolitics_and_conflict": [
        "taiwan", "taiwan strait", "ukraine", "russia", "china military", "south china sea",
        "middle east", "iran", "north korea", "ceasefire", "war", "invasion",
        "military operation", "nato", "red sea attack", "houthi",
    ],
    "sanctions_and_export_controls": [
        "sanction", "ofac", "export control", "chip ban", "bes", "entity list",
        "trade restriction", "huawei", "smic", "blacklist", "arms embargo",
    ],
    "energy_and_supply_disruptions": [
        "oil", "brent", "wti", "opec", "natural gas", "lng", "refinery",
        "energy supply", "strait of hormuz", "suez", "pipeline", "barrel", "crude",
        "energy prices", "power grid",
    ],
    "technology_cycle_ai_semis": [
        "nvidia", "tsmc", "gpu", "semiconductor", "chip", "ai capex", "data center",
        "foundry", "micron", "memory", "hbm", "dram", "nand", "blackwell",
        "artificial intelligence", "machine learning", "inference", "chips act",
        "fab", "wafer", "broadcom", "amd", "hyperscaler", "compute",
    ],
    "crypto_policy_and_market_structure": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "stablecoin", "sec crypto",
        "cftc", "token", "defi", "nft", "mining", "hashrate", "blockchain",
        "digital asset", "coinbase", "crypto regulation", "clarity act",
        "tokenization", "24/7 trading", "crypto etf", "spot btc",
    ],
    "cybersecurity_and_digital_risk": [
        "cybersecurity", "breach", "ransomware", "hack", "zero-day",
        "cisa", "critical infrastructure", "data breach", "cyberattack",
        "supply chain attack", "malware", "nist", "security mandate",
    ],
    "company_specific_corporate_actions": [
        "earnings", "revenue miss", "revenue beat", "guidance", "merger", "acquisition",
        "ipo", "buyback", "dividend", "ceo change", "product launch", "quarterly",
    ],
}

# market_channel detection from driver + context
_CHANNEL_MAP: dict[str, list[str]] = {
    "rates_duration":          ["fed", "fomc", "rate", "yield", "treasury", "duration", "10-year", "sofr"],
    "inflation_expectations":  ["inflation", "cpi", "pce", "breakeven", "tips", "wage", "price index"],
    "fx_usd_liquidity":        ["dollar", "dxy", "usd", "em fx", "currency", "yuan", "yen", "eur/usd"],
    "equities_risk_on_off":    ["risk-on", "risk-off", "vix", "equity market", "broad market", "spx"],
    "equities_factor":         ["growth vs value", "factor", "quality", "duration equity", "sector rotation"],
    "credit_spreads":          ["credit spread", "hy spread", "ig spread", "cds", "default", "leveraged"],
    "commodities_energy":      ["oil", "brent", "wti", "natural gas", "energy", "opec", "crude", "barrel"],
    "commodities_metals":      ["copper", "gold", "silver", "cobalt", "aluminum", "iron ore", "metal"],
    "shipping_chokepoints":    ["suez", "hormuz", "panama canal", "shipping", "chokepoint", "freight rate", "red sea"],
    "crypto_liquidity":        ["bitcoin", "btc", "crypto", "defi", "stablecoin", "funding rate", "mining"],
}

# macro_regime detection
_REGIME_MAP: dict[str, list[str]] = {
    "disinflation_soft_landing":     ["soft landing", "disinflation", "inflation falling", "rate cut", "goldilocks"],
    "reacceleration_overheating":    ["reacceleration", "overheating", "above trend", "hot economy", "no landing"],
    "growth_scare_recession":        ["recession", "hard landing", "contraction", "pmi below 50", "layoffs surge"],
    "stagflation_oil_shock":         ["stagflation", "oil spike", "energy crisis", "supply shock + inflation"],
    "policy_surprise_hawkish":       ["hawkish surprise", "higher for longer", "fed holds", "no cut", "rate hike"],
    "policy_surprise_dovish":        ["dovish pivot", "rate cut surprise", "emergency cut", "fed pivot"],
    "tariff_inflation_shock":        ["tariff", "trade war", "import tax", "inflation from tariff"],
    "liquidity_crunch_funding_stress": ["credit crunch", "repo stress", "funding stress", "bank failure", "liquidity crisis"],
    "geopolitical_risk_premium":     ["war escalation", "military conflict", "sanctions", "taiwan crisis", "iran strike"],
}

# event_cadence detection
_CADENCE_MAP: dict[str, list[str]] = {
    "scheduled_data_release":       ["cpi report", "jobs report", "gdp release", "pmi", "earnings report", "monthly data"],
    "scheduled_policy_meeting":     ["fomc", "ecb meeting", "boj meeting", "opec meeting", "g20", "g7"],
    "unscheduled_breaking_event":   ["surprise", "breaking", "emergency", "flash", "sudden", "unexpected"],
    "binary_legal_regulatory_deadline": ["court ruling", "vote", "deadline", "legislation", "sec decision", "cftc ruling"],
    "company_calendar_event":       ["earnings", "product launch", "investor day", "quarterly", "delivery", "guidance"],
}

# Holdings to flag for each driver category (order = priority)
_DRIVER_TO_HOLDINGS: dict[str, list[str]] = {
    "monetary_policy_central_banks": ["NVDA", "IREN", "HOOD", "LMND", "AUTO1", "GOOGL", "META", "BAP", "KSPI"],
    "inflation_and_prices":          ["IREN", "HUT", "RIOT", "WULF", "CIFR", "BE", "NVDA"],
    "growth_and_labor":              ["META", "GOOGL", "RDDT", "AMZN", "MSFT", "AUTO1", "BAP"],
    "fiscal_and_sovereign":          ["BAP", "KSPI", "HOOD", "LMND"],
    "trade_and_tariffs":             ["NVDA", "TSM", "MU", "AUTO1", "BABA", "AVGO"],
    "geopolitics_and_conflict":      ["TSM", "NVDA", "MU", "BABA", "KSPI"],
    "sanctions_and_export_controls": ["NVDA", "TSM", "MU", "AVGO", "KLAC", "BABA"],
    "energy_and_supply_disruptions": ["IREN", "HUT", "RIOT", "WULF", "CIFR", "APLD", "BE"],
    "technology_cycle_ai_semis":     ["NVDA", "TSM", "MU", "IREN", "APLD", "AVGO", "KLAC", "GOOGL", "MSFT", "AMZN"],
    "crypto_policy_and_market_structure": ["HUT", "RIOT", "WULF", "CIFR", "IREN", "ETH", "GLXY", "HOOD"],
    "cybersecurity_and_digital_risk":["PANW", "CRWD"],
    "company_specific_corporate_actions": [],  # handled per-mention
}


def _detect_category(text: str) -> tuple[str, int]:
    """Return (driver_category, base_score)."""
    best_cat, best_score = "company_specific_corporate_actions", 0
    for cat, keywords in _DRIVER_KEYWORDS.items():
        hits = sum(1 for k in keywords if k in text)
        weight = {"monetary_policy_central_banks": 6, "trade_and_tariffs": 6,
                  "geopolitics_and_conflict": 5, "sanctions_and_export_controls": 7,
                  "energy_and_supply_disruptions": 5, "technology_cycle_ai_semis": 6,
                  "crypto_policy_and_market_structure": 6, "cybersecurity_and_digital_risk": 7}.get(cat, 4)
        s = hits * weight
        if s > best_score:
            best_score, best_cat = s, cat
    base = min(25 + best_score * 2, 45)
    return best_cat, base


def _detect_channels(text: str, category: str) -> list[str]:
    channels = []
    for ch, kws in _CHANNEL_MAP.items():
        if any(k in text for k in kws):
            channels.append(ch)
    # default channel by category
    defaults = {
        "monetary_policy_central_banks": "rates_duration",
        "inflation_and_prices": "inflation_expectations",
        "trade_and_tariffs": "inflation_expectations",
        "geopolitics_and_conflict": "equities_risk_on_off",
        "sanctions_and_export_controls": "equities_factor",
        "energy_and_supply_disruptions": "commodities_energy",
        "technology_cycle_ai_semis": "equities_factor",
        "crypto_policy_and_market_structure": "crypto_liquidity",
        "cybersecurity_and_digital_risk": "equities_factor",
        "growth_and_labor": "equities_risk_on_off",
        "fiscal_and_sovereign": "rates_duration",
        "company_specific_corporate_actions": "equities_factor",
    }
    if defaults.get(category) and defaults[category] not in channels:
        channels.insert(0, defaults[category])
    return list(dict.fromkeys(channels))[:3]  # dedupe, max 3


def _detect_regime(text: str, category: str) -> str:
    for regime, kws in _REGIME_MAP.items():
        if any(k in text for k in kws):
            return regime
    # infer from category
    cat_to_regime = {
        "monetary_policy_central_banks": "policy_surprise_hawkish",
        "inflation_and_prices": "reacceleration_overheating",
        "growth_and_labor": "growth_scare_recession",
        "trade_and_tariffs": "tariff_inflation_shock",
        "geopolitics_and_conflict": "geopolitical_risk_premium",
        "sanctions_and_export_controls": "geopolitical_risk_premium",
        "energy_and_supply_disruptions": "stagflation_oil_shock",
        "technology_cycle_ai_semis": "disinflation_soft_landing",
        "crypto_policy_and_market_structure": "disinflation_soft_landing",
        "cybersecurity_and_digital_risk": "disinflation_soft_landing",
        "company_specific_corporate_actions": "disinflation_soft_landing",
    }
    return cat_to_regime.get(category, "disinflation_soft_landing")


def _detect_cadence(text: str) -> str:
    for cadence, kws in _CADENCE_MAP.items():
        if any(k in text for k in kws):
            return cadence
    return "unscheduled_breaking_event"


def _build_holdings_for_category(category: str, text: str, regime: str) -> tuple[list[dict], list[dict], int]:
    """Return (holdings, causal_chain, score_boost)."""
    holdings: list[dict] = []
    causal_chain: list[dict] = []
    score_boost = 0

    # Direction heuristic: for hawkish/tariff/geo regimes → growth DOWN; dovish/risk-on → UP
    hawkish_regimes = {"policy_surprise_hawkish", "reacceleration_overheating", "stagflation_oil_shock",
                       "tariff_inflation_shock", "geopolitical_risk_premium", "liquidity_crunch_funding_stress"}
    dovish_regimes  = {"policy_surprise_dovish", "disinflation_soft_landing"}
    risk_off = regime in hawkish_regimes

    # ── CRYPTO ──
    if category == "crypto_policy_and_market_structure":
        is_positive = any(k in text for k in [
            "clarity", "approve", "pass", "legal", "etf approved", "regulator approve",
            "stablecoin bill pass", "legislation signed", "institutional", "regulated",
        ])
        is_negative = any(k in text for k in [
            "ban", "crackdown", "illegal", "reject", "deny", "criminal", "seizure", "shutdown",
        ])
        dir_ = "UP" if is_positive else ("DOWN" if is_negative else "MIXED")
        causal_chain = [
            {"step": 1, "from": "Crypto regulatory event", "to": "Institutional confidence in digital assets",
             "why": "Regulatory clarity unlocks / closes institutional participation"},
            {"step": 2, "from": "BTC/ETH price movement", "to": "Mining revenue and crypto-finance P&L",
             "why": "Mining revenue = hashrate × (BTC price / difficulty); broker volume moves with BTC"},
        ]
        miner_tickers = ["HUT", "RIOT", "WULF", "CIFR", "IREN"]
        for t in miner_tickers:
            h = _TICKER_MAP[t]
            holdings.append({
                "name": h["name"], "ticker_or_symbol": t, "fund": h["fund"],
                "direction": dir_, "time_horizon": "weeks",
                "mechanism": f"BTC price movement directly affects {h['name']} mining revenue (revenue = hashrate × BTC / difficulty)",
                "confidence": 0.82,
            })
        holdings.append({
            "name": "Ethereum", "ticker_or_symbol": "ETH", "fund": "BIT Global Crypto Leaders",
            "direction": dir_, "time_horizon": "weeks",
            "mechanism": "ETH moves with broader crypto sentiment and regulatory posture",
            "confidence": 0.75,
        })
        holdings.append({
            "name": "Robinhood", "ticker_or_symbol": "HOOD", "fund": "BIT Global Technology Leaders",
            "direction": dir_, "time_horizon": "weeks",
            "mechanism": "Crypto trading volume on Robinhood is a key P&L driver; regulation expands or shrinks TAM",
            "confidence": 0.72,
        })
        holdings.append({
            "name": "Galaxy Digital", "ticker_or_symbol": "GLXY", "fund": "BIT Global Crypto Leaders",
            "direction": dir_, "time_horizon": "weeks",
            "mechanism": "Galaxy's trading and asset management revenues tied to crypto asset values and activity",
            "confidence": 0.70,
        })
        score_boost = 35 if dir_ != "MIXED" else 20

    # ── MONETARY POLICY / RATES ──
    elif category == "monetary_policy_central_banks":
        is_cut = any(k in text for k in ["cut", "lower", "ease", "pivot", "dovish"])
        dir_ = "UP" if is_cut else "DOWN"
        causal_chain = [
            {"step": 1, "from": "Fed rate decision / guidance", "to": "Risk-free rate and term premium",
             "why": "Fed directly sets the cost of capital; forward guidance moves long-end yields"},
            {"step": 2, "from": "Real yields change", "to": "Growth equity multiple repricing",
             "why": "High-duration growth stocks (NVDA, IREN, LMND, HOOD) most sensitive to discount rate"},
            {"step": 3, "from": "Risk appetite shift", "to": "Crypto and mining stocks",
             "why": "Rate cuts = risk-on = BTC/ETH rally = miner revenue expansion"},
        ]
        rate_sensitive = [("NVDA", "BIT Global Technology Leaders"),
                          ("IREN", "BIT Global Technology Leaders"),
                          ("LMND", "BIT Global Technology Leaders"),
                          ("HOOD", "BIT Global Technology Leaders"),
                          ("AUTO1", "BIT Global Technology Leaders"),
                          ("GOOGL", "BIT Global Technology Leaders")]
        for t, fund in rate_sensitive:
            h = _TICKER_MAP[t]
            holdings.append({
                "name": h["name"], "ticker_or_symbol": t, "fund": fund,
                "direction": dir_, "time_horizon": "weeks",
                "mechanism": f"High-duration growth stock; discount rate repricing directly affects {h['name']} multiple",
                "confidence": 0.75,
            })
        score_boost = 30

    # ── AI / SEMIS ──
    elif category == "technology_cycle_ai_semis":
        is_export_control = any(k in text for k in ["export control", "chip ban", "sanction", "entity list", "restriction"])
        causal_chain = [
            {"step": 1, "from": "AI/semiconductor market event", "to": "Chip demand/supply/pricing",
             "why": "Directly affects semiconductor revenue and capex decisions"},
            {"step": 2, "from": "Chip supply chain change", "to": "AI infrastructure investment pacing",
             "why": "NVDA/TSM availability gates AI capex; restrictions or shortages ripple to data centers"},
        ]
        if is_export_control:
            causal_chain.append({
                "step": 3, "from": "Export restrictions on advanced chips", "to": "China revenue loss for NVDA/TSM/MU",
                "why": "China represents ~15-25% of revenue for key semis names",
            })
        ai_names = [("NVDA", "GPU/AI chip direct exposure"), ("TSM", "foundry supply chain"),
                    ("MU", "HBM/AI memory pricing"), ("IREN", "AI compute/data center buildout"),
                    ("APLD", "AI HPC infrastructure"), ("AVGO", "custom AI ASICs")]
        for t, mech in ai_names:
            h = _TICKER_MAP.get(t)
            if not h:
                continue
            dir_ = "DOWN" if is_export_control else "MIXED"
            holdings.append({
                "name": h["name"], "ticker_or_symbol": t, "fund": h["fund"],
                "direction": dir_, "time_horizon": "months",
                "mechanism": mech,
                "confidence": 0.80 if t in ["NVDA", "TSM"] else 0.70,
            })
        score_boost = 30 + (15 if is_export_control else 0)

    # ── ENERGY / OIL ──
    elif category == "energy_and_supply_disruptions":
        causal_chain = [
            {"step": 1, "from": "Oil/energy supply disruption", "to": "Brent/WTI price ↑",
             "why": "Supply shock directly raises spot and forward oil prices"},
            {"step": 2, "from": "Energy price spike", "to": "Inflation expectations ↑ → Fed holds / raises",
             "why": "Energy is a primary driver of CPI; Fed cannot cut while energy is inflationary"},
            {"step": 3, "from": "Higher for longer rates", "to": "Growth tech multiples ↓",
             "why": "Discount rate reprices all high-duration names in BIT portfolio"},
            {"step": 4, "from": "Power/energy costs ↑", "to": "Mining P&L deteriorates",
             "why": "Miners operate on thin margins; electricity cost is the primary operating expense"},
        ]
        holdings = [
            {**{k: v for k, v in _TICKER_MAP["BE"].items() if k in ["name", "fund"]},
             "ticker_or_symbol": "BE", "direction": "UP", "time_horizon": "months",
             "mechanism": "Energy price shock boosts demand for alternative power solutions (fuel cells)", "confidence": 0.65},
            {**{k: v for k, v in _TICKER_MAP["IREN"].items() if k in ["name", "fund"]},
             "ticker_or_symbol": "IREN", "direction": "DOWN", "time_horizon": "weeks",
             "mechanism": "Higher energy costs squeeze IREN data center and mining margins", "confidence": 0.72},
            {**{k: v for k, v in _TICKER_MAP["HUT"].items() if k in ["name", "fund"]},
             "ticker_or_symbol": "HUT", "direction": "DOWN", "time_horizon": "weeks",
             "mechanism": "Electricity is primary mining OpEx; rising power costs compress mining economics", "confidence": 0.75},
        ]
        score_boost = 25

    # ── GEOPOLITICS / TAIWAN ──
    elif category == "geopolitics_and_conflict":
        is_taiwan = any(k in text for k in ["taiwan", "tsmc", "pla", "strait", "beijing", "china military"])
        causal_chain = [
            {"step": 1, "from": "Geopolitical escalation", "to": "Risk premium expansion",
             "why": "Conflict risk raises uncertainty discount across all assets"},
        ]
        if is_taiwan:
            causal_chain += [
                {"step": 2, "from": "Taiwan conflict risk ↑", "to": "TSMC supply chain threatened",
                 "why": "TSMC manufactures ~90% of advanced chips globally; disruption = semiconductor shortage"},
                {"step": 3, "from": "Chip supply shock", "to": "AI capex halted; all BIT tech holdings repriced",
                 "why": "Modern AI/tech infrastructure cannot function without TSMC-sourced chips"},
            ]
            holdings = [
                {"name": "TSMC", "ticker_or_symbol": "TSM", "fund": "BIT Global Technology Leaders",
                 "direction": "DOWN", "time_horizon": "days",
                 "mechanism": "Direct Taiwan conflict risk to foundry operations", "confidence": 0.92},
                {"name": "Nvidia", "ticker_or_symbol": "NVDA", "fund": "BIT Global Technology Leaders",
                 "direction": "DOWN", "time_horizon": "days",
                 "mechanism": "GPU supply chain entirely dependent on TSMC advanced nodes", "confidence": 0.88},
                {"name": "Micron Technology", "ticker_or_symbol": "MU", "fund": "BIT Global Technology Leaders",
                 "direction": "DOWN", "time_horizon": "days",
                 "mechanism": "Taiwan operations at risk; secondary supply chain concern", "confidence": 0.80},
            ]
            score_boost = 40
        else:
            score_boost = 15

    # ── TARIFFS ──
    elif category == "trade_and_tariffs":
        causal_chain = [
            {"step": 1, "from": "Tariff announcement / escalation", "to": "Import price inflation",
             "why": "Tariffs directly raise costs of imported goods and components"},
            {"step": 2, "from": "Inflation impulse", "to": "Fed rate path repriced hawkish",
             "why": "Tariff-driven CPI forces Fed to hold rates higher"},
            {"step": 3, "from": "Higher for longer", "to": "Growth multiple compression",
             "why": "Discount rate directly affects all high-duration BIT names"},
        ]
        holdings = [
            {"name": "Nvidia", "ticker_or_symbol": "NVDA", "fund": "BIT Global Technology Leaders",
             "direction": "DOWN", "time_horizon": "months",
             "mechanism": "Supply chain cost + China revenue risk from tariff escalation", "confidence": 0.70},
            {"name": "Auto1 Group", "ticker_or_symbol": "AUTO1", "fund": "BIT Global Technology Leaders",
             "direction": "DOWN", "time_horizon": "months",
             "mechanism": "European auto e-commerce vulnerable to US-EU trade friction", "confidence": 0.60},
            {"name": "Alibaba", "ticker_or_symbol": "BABA", "fund": "BIT Defensive Growth",
             "direction": "DOWN", "time_horizon": "months",
             "mechanism": "US-China trade war escalation directly threatens Alibaba's US market access and investor sentiment", "confidence": 0.65},
        ]
        score_boost = 28

    # ── SANCTIONS / EXPORT CONTROLS ──
    elif category == "sanctions_and_export_controls":
        causal_chain = [
            {"step": 1, "from": "Export control / sanction announcement", "to": "China revenue at risk for semis names",
             "why": "BIS entity list additions and licensing requirements cut off China market access"},
            {"step": 2, "from": "Revenue risk", "to": "Guidance cuts + multiple compression",
             "why": "Market reprices growth outlook when a 15-25% revenue segment is at risk"},
        ]
        holdings = [
            {"name": "Nvidia", "ticker_or_symbol": "NVDA", "fund": "BIT Global Technology Leaders",
             "direction": "DOWN", "time_horizon": "weeks",
             "mechanism": "China represents ~15% of NVDA revenue; export restrictions directly threaten this", "confidence": 0.82},
            {"name": "TSMC", "ticker_or_symbol": "TSM", "fund": "BIT Global Technology Leaders",
             "direction": "DOWN", "time_horizon": "months",
             "mechanism": "Export controls force TSMC to stop serving restricted Chinese fabs", "confidence": 0.75},
            {"name": "Micron Technology", "ticker_or_symbol": "MU", "fund": "BIT Global Technology Leaders",
             "direction": "DOWN", "time_horizon": "months",
             "mechanism": "Micron has significant China sales exposure; sanctions reduce accessible market", "confidence": 0.72},
            {"name": "KLA Corporation", "ticker_or_symbol": "KLAC", "fund": "BIT Global Multi Asset",
             "direction": "DOWN", "time_horizon": "months",
             "mechanism": "Semiconductor equipment export controls directly affect KLAC China sales", "confidence": 0.68},
        ]
        score_boost = 35

    # ── CYBERSECURITY ──
    elif category == "cybersecurity_and_digital_risk":
        causal_chain = [
            {"step": 1, "from": "Cyber incident / mandate / breach", "to": "Enterprise security urgency ↑",
             "why": "High-profile incidents force boards to accelerate security spending"},
            {"step": 2, "from": "Security spend acceleration", "to": "PANW/CRWD demand pipeline",
             "why": "Platform consolidation trend benefits market leaders PANW and CRWD"},
        ]
        holdings = [
            {"name": "Palo Alto Networks", "ticker_or_symbol": "PANW", "fund": "BIT Defensive Growth",
             "direction": "UP", "time_horizon": "weeks",
             "mechanism": "Cyber incidents drive platform consolidation and accelerate PANW enterprise deals", "confidence": 0.78},
            {"name": "CrowdStrike", "ticker_or_symbol": "CRWD", "fund": "BIT Global Multi Asset",
             "direction": "UP", "time_horizon": "weeks",
             "mechanism": "Security incidents drive endpoint protection and identity security demand for CRWD", "confidence": 0.75},
        ]
        score_boost = 32

    # ── GROWTH / LABOR / CONSUMER ──
    elif category == "growth_and_labor":
        dir_ = "DOWN" if any(k in text for k in ["recession", "weak", "miss", "contraction", "layoff"]) else "MIXED"
        causal_chain = [
            {"step": 1, "from": "Growth / labor market signal", "to": "Consumer and enterprise spending outlook",
             "why": "NFP/GDP drives ad spend, enterprise IT budgets, consumer discretionary"},
        ]
        holdings = [
            {"name": "Meta Platforms", "ticker_or_symbol": "META", "fund": "BIT Global Leaders",
             "direction": dir_, "time_horizon": "months",
             "mechanism": "Digital ad spend tracks consumer confidence and GDP closely", "confidence": 0.70},
            {"name": "Alphabet", "ticker_or_symbol": "GOOGL", "fund": "BIT Global Technology Leaders",
             "direction": dir_, "time_horizon": "months",
             "mechanism": "Search and YouTube ad revenue are pro-cyclical", "confidence": 0.68},
            {"name": "Auto1 Group", "ticker_or_symbol": "AUTO1", "fund": "BIT Global Technology Leaders",
             "direction": dir_, "time_horizon": "months",
             "mechanism": "Automotive e-commerce is discretionary; demand falls in recession", "confidence": 0.65},
        ]
        score_boost = 22

    return holdings, causal_chain, score_boost


def rule_based_fallback(market: dict) -> dict:
    """
    Rule-based fallback implementing the professional taxonomy structure.
    Mirrors the LLM prompt's classification logic using keyword matching.
    """
    question = (
        market.get("question", "") + " " +
        market.get("description", "") + " " +
        market.get("category", "") + " " +
        str(market.get("subcategories", ""))
    ).lower()

    agenda_hints = match_agenda_hints(
        market.get("question", ""),
        market.get("description", ""),
        market.get("category", ""),
        market.get("subcategories", ""),
    )

    # 1. Classify driver_category and get base score
    driver_category, base_score = _detect_category(question)

    # 2. Detect channels, regime, cadence
    channels = _detect_channels(question, driver_category)
    macro_regime = _detect_regime(question, driver_category)
    event_cadence = _detect_cadence(question)

    # 3. Build holdings and causal chain
    holdings, causal_chain, score_boost = _build_holdings_for_category(driver_category, question, macro_regime)
    routing_matches = _match_holding_trigger_routes(question)
    macro_recipe_matches = _match_specific_macro_recipes(question)

    score = min(base_score + score_boost, 95)
    recipe_event_type_override = None
    if macro_recipe_matches:
        driver_category, channels, macro_regime, holdings, causal_chain, score, recipe_event_type_override = _recipe_driven_override(
            question,
            macro_recipe_matches[0],
            driver_category,
            channels,
            macro_regime,
            holdings,
            causal_chain,
            score,
        )
    if routing_matches:
        score = min(score + min(20, 6 + sum(m["match_score"] for m in routing_matches[:2]) // 4), 95)
    if macro_recipe_matches:
        score = min(score + min(12, 3 * len(macro_recipe_matches[0]["hits"])), 95)

    # 4. Agenda watchlist soft boost
    if agenda_hints.get("is_agenda_relevant"):
        agenda_score_add = min(20, int(agenda_hints.get("agenda_score", 0) * 0.5))
        score = min(score + agenda_score_add, 95)
        for m in agenda_hints.get("matches", [])[:2]:
            causal_chain.append({
                "step": len(causal_chain) + 1,
                "from": m.get("label", "Agenda catalyst"),
                "to": "BIT portfolio risk factors",
                "why": m.get("transmission", "Agenda watchlist transmission template"),
            })

    # 5. Penalise clearly irrelevant markets
    irrelevant_markers = [
        "super bowl", "nfl", "nba", "celebrity", "entertainment", "award", "movie",
        "music", "kardashian", "sport", "football", "baseball", "basketball", "soccer",
        "emmy", "grammy", "oscar ceremony", "reality show", "dating", "love island",
    ] + sorted(_NEGATIVE_NOISE_TERMS)
    if any(k in question for k in irrelevant_markers):
        score = min(score, 12)
        holdings, causal_chain = [], []

    # 6. Label
    if score >= 70:
        label = "ACTIONABLE"
    elif score >= 35:
        label = "MONITOR"
    else:
        label = "IGNORE"
        holdings = []
        if not causal_chain:
            causal_chain = [{"step": 1, "from": "Market event", "to": "No relevant BIT holding",
                             "why": "No clear economic transmission path identified"}]

    # 7. Deduplicate holdings
    seen: set[str] = set()
    unique_holdings = []
    for h in holdings:
        t = h.get("ticker_or_symbol", "")
        if t not in seen:
            seen.add(t)
            unique_holdings.append(h)
    # Add explicit holding-route matches if LLM-style fallback logic missed them
    for rm in routing_matches[:4]:
        t = rm["ticker"]
        if t in seen or t not in _TICKER_MAP:
            continue
        h = _TICKER_MAP[t]
        unique_holdings.append({
            "name": h["name"],
            "ticker_or_symbol": t,
            "fund": h["fund"],
            "direction": "MIXED",
            "time_horizon": "weeks",
            "mechanism": f"Matched holding-level trigger routing via aliases {', '.join(rm['alias_hits'][:2] or [t])} and catalysts {', '.join(rm['trigger_hits'][:2]) if rm['trigger_hits'] else 'keyword overlap'}",
            "confidence": 0.62 if rm["alias_hits"] else 0.52,
        })
        seen.add(t)
    unique_holdings = unique_holdings[:6]

    # 8. Extract trigger keywords
    keywords_found = _extract_trigger_keywords(question, limit=24)
    for rm in routing_matches[:3]:
        for kw in rm["alias_hits"] + rm["trigger_hits"]:
            if kw not in keywords_found:
                keywords_found.insert(0, kw)
    for mr in macro_recipe_matches[:2]:
        for kw in mr["hits"]:
            if kw not in keywords_found:
                keywords_found.insert(0, kw)
    keywords_found = keywords_found[:24]

    # 9. Portfolio theme fit
    theme_map = {
        "technology_cycle_ai_semis": ["AI infrastructure & power scarcity", "semis supply chain"],
        "crypto_policy_and_market_structure": ["crypto market structure"],
        "cybersecurity_and_digital_risk": ["cybersecurity"],
        "monetary_policy_central_banks": ["macro rates sensitivity"],
        "energy_and_supply_disruptions": ["AI infrastructure & power scarcity", "clean energy"],
        "geopolitics_and_conflict": ["semis supply chain", "macro rates sensitivity"],
        "trade_and_tariffs": ["macro rates sensitivity"],
        "company_specific_corporate_actions": ["company-specific event"],
        "growth_and_labor": ["consumer internet"],
    }
    theme_fit = list(dict.fromkeys(theme_map.get(driver_category, [])))[:3]

    # 10. Fund-level impact
    fund_impact: list[dict] = []
    affected_funds: set[str] = set()
    for h in unique_holdings:
        if "fund" in h:
            affected_funds.add(h["fund"])
    for fund in affected_funds:
        fund_holdings = [h for h in unique_holdings if h.get("fund") == fund]
        directions = [h.get("direction") for h in fund_holdings]
        net = "DOWN" if directions.count("DOWN") > directions.count("UP") else ("UP" if directions.count("UP") > directions.count("DOWN") else "MIXED")
        fund_impact.append({"fund": fund, "net_direction": net, "magnitude": "HIGH" if score >= 70 else "MEDIUM", "rationale": f"{len(fund_holdings)} holding(s) affected via {driver_category}"})

    # 11. Verdict
    if not unique_holdings or label == "IGNORE":
        verdict = "No clear economic transmission to BIT Capital portfolio holdings."
    else:
        main = unique_holdings[0]
        verdict = (
            f"{driver_category.replace('_', ' ').title()} event affects {main['name']} "
            f"({main['direction'].lower()}) via {channels[0].replace('_', ' ')} channel; "
            f"regime: {macro_regime.replace('_', ' ')}."
        )

    return {
        "market_id": market.get("id", ""),
        "relevance_label": label,
        "relevance_score": score,
        "one_sentence_verdict": verdict,
        "driver_category": driver_category,
        "market_channels": channels,
        "macro_regime": macro_regime,
        "event_cadence": event_cadence,
        "event_type": recipe_event_type_override or driver_category.replace("_", " ").title(),
        "primary_channels": channels,  # legacy compat
        "key_geographies": ["US"],
        "trigger_keywords": keywords_found,
        "causal_chain": causal_chain[:6],
        "affected_holdings": unique_holdings,
        "portfolio_theme_fit": theme_fit,
        "fund_level_impact": fund_impact,
        "what_to_watch_next": [
            {"signal": "odds", "metric": "YES probability", "threshold": "move ±10pp in 24h",
             "why": "Significant odds movement signals new information from market participants"},
        ],
        "red_flags_or_unknowns": (
            ["Rule-based analysis — LLM API unavailable; taxonomy applied via keyword matching."]
            + ([f"Holding trigger routes matched: {', '.join(m['ticker'] for m in routing_matches[:4])}"] if routing_matches else [])
            + ([f"Specific macro recipe matched: {macro_recipe_matches[0]['recipe']}"] if macro_recipe_matches else [])
            + ([f"Agenda watchlist matches: {len(agenda_hints.get('matches', []))} theme(s)"]
               if agenda_hints.get("is_agenda_relevant") else [])
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION — analyze_markets + store_signal (unchanged interface)
# ══════════════════════════════════════════════════════════════════════════════

def analyze_markets(batch_size: int = 10, force_reanalyze: bool = False) -> dict:
    """Main analysis job: analyze unanalyzed markets using LLM or rule-based fallback."""
    global LLM_COOLDOWN_UNTIL_TS
    llm_cooldown_active = (time.time() < LLM_COOLDOWN_UNTIL_TS) and (not force_reanalyze)
    if llm_cooldown_active:
        remaining = max(0, int(LLM_COOLDOWN_UNTIL_TS - time.time()))
        print(f"[ANALYSIS] LLM cooldown active ({remaining}s remaining) — continuing with rule-based analysis.")

    conn = get_connection()
    log_id = None
    started_at = datetime.now(timezone.utc).isoformat()

    try:
        if DB_BACKEND == "postgres":
            cur = conn.execute(
                "INSERT INTO job_runs (job_name, status, message, started_at) VALUES (%s,%s,%s,%s) RETURNING id",
                ("analysis", "RUNNING", "Starting LLM analysis", started_at),
            )
            log_id = cur.fetchone()["id"]
        else:
            cur = conn.execute(
                "INSERT INTO scheduler_log (job_name, status, message, started_at) VALUES (?,?,?,?)",
                ("analysis", "RUNNING", "Starting LLM analysis", started_at),
            )
            log_id = cur.lastrowid
        conn.commit()
    except Exception:
        pass

    if DB_BACKEND == "postgres":
        base_select = """
            SELECT m.id, m.question, m.description, m.category, m.subcategories,
                   m.end_time as end_date,
                   snap.current_yes, snap.current_no, snap.volume_usd as volume,
                   snap.liquidity_usd as liquidity, snap.odds_history, snap.fetched_at
            FROM markets m
            LEFT JOIN LATERAL (
                SELECT ms.current_yes, ms.current_no, ms.volume_usd, ms.liquidity_usd,
                       ms.odds_history, ms.fetched_at
                FROM market_snapshots ms WHERE ms.market_id = m.id
                ORDER BY ms.fetched_at DESC LIMIT 1
            ) snap ON true
        """
        if force_reanalyze:
            query = base_select + " ORDER BY COALESCE(snap.volume_usd,0) DESC LIMIT %s"
            query_params = (batch_size,)
        else:
            query = base_select + """
                LEFT JOIN market_signals s ON s.market_id = m.id AND s.is_latest = true
                WHERE s.id IS NULL OR s.analyzed_at < (NOW() - (%s * INTERVAL '1 hour'))
                ORDER BY COALESCE(snap.volume_usd,0) DESC LIMIT %s
            """
            query_params = (REANALYZE_AFTER_HOURS, batch_size)
    else:
        if force_reanalyze:
            query = "SELECT m.* FROM markets m ORDER BY m.volume DESC LIMIT ?"
            query_params = (batch_size,)
        else:
            query = """
                SELECT m.* FROM markets m
                LEFT JOIN signals s ON m.id = s.market_id
                WHERE s.market_id IS NULL OR datetime(s.analyzed_at) < datetime('now', ?)
                ORDER BY m.volume DESC LIMIT ?
            """
            query_params = (f"-{REANALYZE_AFTER_HOURS} hours", batch_size)

    markets_raw = conn.execute(query, query_params).fetchall()
    conn.close()
    markets = [dict_from_row(r) for r in markets_raw]
    print(f"[ANALYSIS] Analyzing {len(markets)} markets (batch_size={batch_size})...")

    results = {"analyzed": 0, "actionable": 0, "monitor": 0, "ignore": 0, "errors": 0, "llm_skipped_prefilter": 0, "stopped_rate_limited": False}
    for i, market in enumerate(markets):
        print(f"[ANALYSIS] [{i+1}/{len(markets)}] {market.get('question', '')[:80]}…")
        try:
            if llm_cooldown_active:
                analysis = rule_based_fallback(market)
                results["llm_skipped_prefilter"] += 1
            elif _should_skip_llm_for_market(market):
                analysis = rule_based_fallback(market)
                results["llm_skipped_prefilter"] += 1
            else:
                analysis = analyze_with_llm(market)
            store_signal(market["id"], analysis)
            results["analyzed"] += 1
            lbl = analysis.get("relevance_label", "IGNORE")
            results["actionable" if lbl == "ACTIONABLE" else ("monitor" if lbl == "MONITOR" else "ignore")] += 1
            if (GROQ_API_KEY or GEMINI_API_KEY or ANTHROPIC_API_KEY) and i < len(markets) - 1 and not llm_cooldown_active:
                time.sleep(1.0)
        except LLMRateLimitExceeded as e:
            print(f"[ANALYSIS] Provider rate limit hit; stopping remaining batch. per_day={e.per_day} retry_after={e.retry_after_secs}")
            results["stopped_rate_limited"] = True
            if e.per_day:
                cooldown_secs = e.retry_after_secs if e.retry_after_secs is not None else 1800.0
                LLM_COOLDOWN_UNTIL_TS = max(LLM_COOLDOWN_UNTIL_TS, time.time() + max(60.0, cooldown_secs))
            # Do not waste the batch when provider quota is exhausted:
            # fallback current and remaining markets to rule-based analysis.
            try:
                analysis = rule_based_fallback(market)
                store_signal(market["id"], analysis)
                results["analyzed"] += 1
                results["llm_skipped_prefilter"] += 1
                lbl = analysis.get("relevance_label", "IGNORE")
                results["actionable" if lbl == "ACTIONABLE" else ("monitor" if lbl == "MONITOR" else "ignore")] += 1
            except Exception as fallback_err:
                print(f"[ANALYSIS] Fallback error on market {market.get('id')}: {fallback_err}")
                results["errors"] += 1
            llm_cooldown_active = True
            continue
        except Exception as e:
            print(f"[ANALYSIS] Error on market {market.get('id')}: {e}")
            results["errors"] += 1

    conn = get_connection()
    try:
        if log_id:
            if DB_BACKEND == "postgres":
                conn.execute(
                    "UPDATE job_runs SET status=%s, message=%s, completed_at=%s WHERE id=%s",
                    ("SUCCESS", json.dumps(results), datetime.now(timezone.utc).isoformat(), log_id),
                )
            else:
                conn.execute(
                    "UPDATE scheduler_log SET status=?, message=?, completed_at=? WHERE id=?",
                    ("SUCCESS", json.dumps(results), datetime.now(timezone.utc).isoformat(), log_id),
                )
        conn.commit()
    finally:
        conn.close()

    print(f"[ANALYSIS] Complete: {results}")
    return results


def store_signal(market_id: str, analysis: dict) -> None:
    """Store analysis result in signals table (SQLite) or normalized signal tables (Postgres)."""
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            analyzed_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE market_signals SET is_latest = false WHERE market_id = %s AND is_latest = true",
                (market_id,),
            )
            cur = conn.execute(
                """
                INSERT INTO market_signals (
                    market_id, relevance_label, relevance_score, one_sentence_verdict,
                    event_type, causal_chain, what_to_watch_next, red_flags_or_unknowns,
                    raw_analysis, analyzed_at, is_latest
                ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s,true)
                RETURNING id
                """,
                (
                    market_id,
                    analysis.get("relevance_label", "IGNORE"),
                    analysis.get("relevance_score", 0),
                    analysis.get("one_sentence_verdict", ""),
                    analysis.get("event_type", ""),
                    json.dumps(analysis.get("causal_chain", []) or []),
                    json.dumps(analysis.get("what_to_watch_next", []) or []),
                    json.dumps(analysis.get("red_flags_or_unknowns", []) or []),
                    json.dumps(analysis or {}),
                    analyzed_at,
                ),
            )
            signal_id = cur.fetchone()["id"]
            # Store channels (use market_channels if primary_channels missing)
            for ch in (analysis.get("primary_channels") or analysis.get("market_channels") or []):
                if not ch:
                    continue
                conn.execute("INSERT INTO channels (code, label) VALUES (%s,%s) ON CONFLICT (code) DO NOTHING", (str(ch), str(ch)))
                conn.execute("INSERT INTO market_signal_channels (market_signal_id, channel_code) VALUES (%s,%s) ON CONFLICT DO NOTHING", (signal_id, str(ch)))
            for geo in (analysis.get("key_geographies", []) or []):
                if not geo:
                    continue
                conn.execute("INSERT INTO geographies (code, label) VALUES (%s,%s) ON CONFLICT (code) DO NOTHING", (str(geo), str(geo)))
                conn.execute("INSERT INTO market_signal_geographies (market_signal_id, geography_code) VALUES (%s,%s) ON CONFLICT DO NOTHING", (signal_id, str(geo)))
            for kw in (analysis.get("trigger_keywords", []) or []):
                if kw:
                    conn.execute("INSERT INTO market_signal_keywords (market_signal_id, keyword) VALUES (%s,%s) ON CONFLICT DO NOTHING", (signal_id, str(kw)))
            for theme in (analysis.get("portfolio_theme_fit", []) or []):
                if not theme:
                    continue
                cur2 = conn.execute(
                    "INSERT INTO themes (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
                    (str(theme),),
                )
                theme_id = cur2.fetchone()["id"]
                conn.execute("INSERT INTO market_signal_themes (market_signal_id, theme_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (signal_id, theme_id))
            for h in (analysis.get("affected_holdings", []) or []):
                if not isinstance(h, dict):
                    continue
                conn.execute(
                    """
                    INSERT INTO market_signal_holding_impacts (
                        market_signal_id, ticker_or_symbol, name, direction, mechanism, time_horizon, confidence
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (signal_id, h.get("ticker_or_symbol"), h.get("name"),
                     h.get("direction") or "MIXED", h.get("mechanism"),
                     h.get("time_horizon"), h.get("confidence")),
                )

            # Materialize normalized event row for queryable analytics
            event_channels = (analysis.get("primary_channels") or analysis.get("market_channels") or [])
            primary_channel = event_channels[0] if event_channels else None
            entities = analysis.get("entities") or []
            if not entities:
                entities = [g for g in (analysis.get("key_geographies") or []) if g]
            polarity = analysis.get("polarity")
            if not polarity:
                dirs = [str((h or {}).get("direction", "")).upper() for h in (analysis.get("affected_holdings") or []) if isinstance(h, dict)]
                if dirs:
                    up_n = sum(1 for d in dirs if d == "UP")
                    down_n = sum(1 for d in dirs if d == "DOWN")
                    polarity = "UP" if up_n > down_n else ("DOWN" if down_n > up_n else "MIXED")
                else:
                    polarity = "MIXED"
            conn.execute("SAVEPOINT sp_optional_normalized_events")
            try:
                conn.execute(
                    """
                    INSERT INTO normalized_events (
                        market_signal_id, market_id, driver_category, primary_channel, macro_regime,
                        event_cadence, event_type, polarity, entities, metadata
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
                    ON CONFLICT (market_signal_id) DO UPDATE SET
                        driver_category = EXCLUDED.driver_category,
                        primary_channel = EXCLUDED.primary_channel,
                        macro_regime = EXCLUDED.macro_regime,
                        event_cadence = EXCLUDED.event_cadence,
                        event_type = EXCLUDED.event_type,
                        polarity = EXCLUDED.polarity,
                        entities = EXCLUDED.entities,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        signal_id,
                        market_id,
                        analysis.get("driver_category"),
                        primary_channel,
                        analysis.get("macro_regime"),
                        analysis.get("event_cadence"),
                        analysis.get("event_type"),
                        polarity,
                        json.dumps(entities if isinstance(entities, list) else [str(entities)]),
                        json.dumps({
                            "relevance_label": analysis.get("relevance_label"),
                            "relevance_score": analysis.get("relevance_score"),
                        }),
                    ),
                )
                conn.execute("RELEASE SAVEPOINT sp_optional_normalized_events")
            except Exception as e:
                if "normalized_events" not in str(e):
                    raise
                conn.execute("ROLLBACK TO SAVEPOINT sp_optional_normalized_events")
                conn.execute("RELEASE SAVEPOINT sp_optional_normalized_events")
                print("[ANALYSIS] Optional table missing: normalized_events (run Postgres schema migration)")

            # Materialize fund routes if analysis provided fund-level impact
            for fr in (analysis.get("fund_level_impact") or []):
                if not isinstance(fr, dict):
                    continue
                fund_name = str(fr.get("fund") or "").strip() or None
                if not fund_name:
                    continue
                fund_code_guess = None
                f_low = fund_name.lower()
                if "technology leaders" in f_low: fund_code_guess = "TL"
                elif "fintech leaders" in f_low: fund_code_guess = "FL"
                elif "crypto leaders" in f_low: fund_code_guess = "CL"
                elif "defensive growth" in f_low: fund_code_guess = "DG"
                elif "multi asset" in f_low: fund_code_guess = "MA"
                elif "global leaders" in f_low: fund_code_guess = "GL"
                magnitude = str(fr.get("magnitude") or "").upper()
                mag_score = {"LOW": 1.0, "MEDIUM": 2.0, "HIGH": 3.0}.get(magnitude, 1.5)
                base_score = float(analysis.get("relevance_score") or 0)
                conn.execute("SAVEPOINT sp_optional_fund_routes")
                try:
                    conn.execute(
                        """
                        INSERT INTO fund_routes (
                            market_signal_id, market_id, fund_code, fund_name, fund_score,
                            label, net_direction, rationale, metadata
                        ) VALUES (%s,%s,%s,%s,%s,%s::relevance_label,%s,%s,%s::jsonb)
                        """,
                        (
                            signal_id,
                            market_id,
                            fund_code_guess,
                            fund_name,
                            round(base_score * mag_score / 3.0, 3),
                            analysis.get("relevance_label", "IGNORE"),
                            fr.get("net_direction"),
                            fr.get("rationale"),
                            json.dumps(fr),
                        ),
                    )
                    conn.execute("RELEASE SAVEPOINT sp_optional_fund_routes")
                except Exception as e:
                    if "fund_routes" not in str(e):
                        raise
                    conn.execute("ROLLBACK TO SAVEPOINT sp_optional_fund_routes")
                    conn.execute("RELEASE SAVEPOINT sp_optional_fund_routes")
                    print("[ANALYSIS] Optional table missing: fund_routes (run Postgres schema migration)")

            # Persist evidence pack when available (future retriever / rules-risk integration)
            evidence_items = analysis.get("evidence_items")
            if evidence_items is None and isinstance(analysis.get("raw_analysis"), dict):
                evidence_items = analysis["raw_analysis"].get("evidence_items")
            for ev in (evidence_items or []):
                if not isinstance(ev, dict):
                    continue
                conn.execute("SAVEPOINT sp_optional_evidence_items")
                try:
                    conn.execute(
                        """
                        INSERT INTO evidence_items (
                            market_signal_id, market_id, source, source_type, headline, snippet,
                            url, published_at, evidence_kind, metadata
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                        """,
                        (
                            signal_id,
                            market_id,
                            ev.get("source"),
                            ev.get("source_type"),
                            ev.get("headline"),
                            ev.get("snippet"),
                            ev.get("url"),
                            ev.get("published_at"),
                            ev.get("evidence_kind"),
                            json.dumps(ev),
                        ),
                    )
                    conn.execute("RELEASE SAVEPOINT sp_optional_evidence_items")
                except Exception as e:
                    if "evidence_items" not in str(e):
                        raise
                    conn.execute("ROLLBACK TO SAVEPOINT sp_optional_evidence_items")
                    conn.execute("RELEASE SAVEPOINT sp_optional_evidence_items")
                    print("[ANALYSIS] Optional table missing: evidence_items (run Postgres schema migration)")
            conn.commit()
            return

        # ── SQLite path ──
        def _j(val):
            return json.dumps(val) if isinstance(val, (list, dict)) else (str(val) if val is not None else None)

        conn.execute(
            """
            INSERT OR REPLACE INTO signals
                (market_id, relevance_label, relevance_score, one_sentence_verdict,
                 event_type, primary_channels, key_geographies, trigger_keywords,
                 causal_chain, affected_holdings, portfolio_theme_fit,
                 what_to_watch_next, red_flags_or_unknowns, raw_analysis, analyzed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                market_id,
                analysis.get("relevance_label", "IGNORE"),
                analysis.get("relevance_score", 0),
                analysis.get("one_sentence_verdict", ""),
                analysis.get("event_type", ""),
                _j(analysis.get("primary_channels") or analysis.get("market_channels") or []),
                _j(analysis.get("key_geographies", [])),
                _j(analysis.get("trigger_keywords", [])),
                _j(analysis.get("causal_chain", [])),
                _j(analysis.get("affected_holdings", [])),
                _j(analysis.get("portfolio_theme_fit", [])),
                _j(analysis.get("what_to_watch_next", [])),
                _j(analysis.get("red_flags_or_unknowns", [])),
                json.dumps(analysis),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    from database.db import init_db
    init_db()
    results = analyze_markets(batch_size=20)
    print(f"Analysis results: {results}")
