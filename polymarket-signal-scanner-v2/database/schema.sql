-- Polymarket Signal Scanner Database Schema
-- BIT Capital Equity & Macro Relevance Filter

CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY,
    question TEXT NOT NULL,
    description TEXT,
    category TEXT,
    subcategories TEXT, -- JSON array as text
    end_date TEXT,
    current_yes REAL,
    current_no REAL,
    volume REAL DEFAULT 0,
    liquidity REAL DEFAULT 0,
    odds_history TEXT, -- JSON array as text
    related_markets TEXT, -- JSON array as text
    raw_data TEXT, -- full raw JSON from Polymarket
    fetched_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL REFERENCES markets(id),
    relevance_label TEXT NOT NULL CHECK(relevance_label IN ('IGNORE','MONITOR','ACTIONABLE')),
    relevance_score INTEGER NOT NULL CHECK(relevance_score BETWEEN 0 AND 100),
    one_sentence_verdict TEXT,
    event_type TEXT,
    primary_channels TEXT, -- JSON array
    key_geographies TEXT, -- JSON array
    trigger_keywords TEXT, -- JSON array
    causal_chain TEXT, -- JSON array of steps
    affected_holdings TEXT, -- JSON array
    portfolio_theme_fit TEXT, -- JSON array
    what_to_watch_next TEXT, -- JSON array
    red_flags_or_unknowns TEXT, -- JSON array
    raw_analysis TEXT, -- full LLM output JSON
    analyzed_at TEXT NOT NULL,
    UNIQUE(market_id)
);

CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date TEXT NOT NULL,
    title TEXT NOT NULL,
    executive_summary TEXT,
    actionable_count INTEGER DEFAULT 0,
    monitor_count INTEGER DEFAULT 0,
    ignore_count INTEGER DEFAULT 0,
    top_signals TEXT, -- JSON array of market_ids
    full_report_html TEXT,
    full_report_json TEXT,
    generated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    ticker TEXT,
    fund TEXT NOT NULL,
    weight REAL,
    sector TEXT,
    themes TEXT, -- JSON array
    is_active INTEGER DEFAULT 1,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduler_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT
);

-- Default portfolio configuration based on BIT Capital holdings
INSERT OR IGNORE INTO portfolio_config (name, ticker, fund, weight, sector, themes, is_active, updated_at) VALUES
('IREN (Iris Energy)', 'IREN', 'BIT Global Technology Leaders', 9.5, 'AI Infrastructure', '["AI compute","bitcoin mining","data center","power"]', 1, datetime('now')),
('Auto1 Group', 'AUTO1', 'BIT Global Technology Leaders', 8.0, 'Consumer Internet', '["e-commerce","automotive","marketplace"]', 1, datetime('now')),
('Bloom Energy', 'BE', 'BIT Global Technology Leaders', 7.0, 'Clean Energy', '["fuel cell","power","energy infrastructure"]', 1, datetime('now')),
('Alphabet', 'GOOGL', 'BIT Global Technology Leaders', 5.5, 'Consumer Internet', '["AI","search","cloud","advertising"]', 1, datetime('now')),
('Micron Technology', 'MU', 'BIT Global Technology Leaders', 5.0, 'Semiconductors', '["DRAM","HBM","memory","AI semis"]', 1, datetime('now')),
('Nvidia', 'NVDA', 'BIT Global Technology Leaders', 5.0, 'Semiconductors', '["GPU","AI","data center","CUDA"]', 1, datetime('now')),
('TSMC', 'TSM', 'BIT Global Technology Leaders', 5.0, 'Semiconductors', '["foundry","EUV","Taiwan","chips"]', 1, datetime('now')),
('Lemonade', 'LMND', 'BIT Global Technology Leaders', 4.5, 'Insurtech', '["insurance","AI underwriting","insurtech"]', 1, datetime('now')),
('Robinhood', 'HOOD', 'BIT Global Technology Leaders', 4.5, 'Fintech', '["brokerage","crypto","retail investing","PFOF"]', 1, datetime('now')),
('Meta Platforms', 'META', 'BIT Global Leaders', 4.0, 'Consumer Internet', '["social media","advertising","AI","metaverse"]', 1, datetime('now')),
('Reddit', 'RDDT', 'BIT Global Leaders', 4.0, 'Consumer Internet', '["social media","advertising","community"]', 1, datetime('now')),
('Hut 8', 'HUT', 'BIT Global Crypto Leaders', 10.0, 'Crypto Mining', '["bitcoin mining","hashrate","data center"]', 1, datetime('now')),
('Applied Digital', 'APLD', 'BIT Global Crypto Leaders', 5.0, 'AI Infrastructure', '["data center","HPC","AI compute"]', 1, datetime('now')),
('Terawulf', 'WULF', 'BIT Global Crypto Leaders', 5.0, 'Crypto Mining', '["bitcoin mining","nuclear power","hashrate"]', 1, datetime('now')),
('Cipher Mining', 'CIFR', 'BIT Global Crypto Leaders', 5.0, 'Crypto Mining', '["bitcoin mining","hashrate"]', 1, datetime('now')),
('Riot Platforms', 'RIOT', 'BIT Global Crypto Leaders', 5.0, 'Crypto Mining', '["bitcoin mining","hashrate","data center"]', 1, datetime('now')),
('Galaxy Digital', 'GLXY', 'BIT Global Crypto Leaders', 4.5, 'Crypto Finance', '["crypto","DeFi","institutional","trading"]', 1, datetime('now')),
('Palo Alto Networks', 'PANW', 'BIT Defensive Growth', 3.5, 'Cybersecurity', '["cybersecurity","zero-trust","SASE","government"]', 1, datetime('now')),
('Amazon', 'AMZN', 'BIT Defensive Growth', 4.0, 'Consumer Internet', '["cloud","e-commerce","AI","AWS"]', 1, datetime('now')),
('Microsoft', 'MSFT', 'BIT Defensive Growth', 4.0, 'Consumer Internet', '["cloud","AI","Azure","Office365"]', 1, datetime('now')),
('Broadcom', 'AVGO', 'BIT Global Multi Asset', 2.0, 'Semiconductors', '["ASIC","networking","AI chips","enterprise"]', 1, datetime('now')),
('CrowdStrike', 'CRWD', 'BIT Global Multi Asset', 2.0, 'Cybersecurity', '["endpoint security","cloud","XDR"]', 1, datetime('now')),
('Kaspi.kz', 'KSPI', 'BIT Global Fintech Leaders', 4.0, 'Fintech', '["payments","Kazakhstan","emerging markets","super app"]', 1, datetime('now')),
('Credicorp', 'BAP', 'BIT Global Fintech Leaders', 5.0, 'Fintech', '["banking","Peru","LatAm","financial services"]', 1, datetime('now')),
('Oscar Health', 'OSCR', 'BIT Global Fintech Leaders', 4.0, 'Insurtech', '["health insurance","insurtech","ACA"]', 1, datetime('now')),
('Hinge Health', 'HNGE', 'BIT Global Technology Leaders', 5.0, 'Digital Health', '["digital health","MSK","physical therapy","AI"]', 1, datetime('now')),
('Ethereum', 'ETH', 'BIT Global Crypto Leaders', 4.5, 'Crypto', '["DeFi","staking","Layer2","smart contracts"]', 1, datetime('now')),
('Alibaba', 'BABA', 'BIT Defensive Growth', 4.0, 'Consumer Internet', '["China","e-commerce","cloud","AI"]', 1, datetime('now')),
('KLA Corporation', 'KLAC', 'BIT Global Multi Asset', 2.0, 'Semiconductors', '["process control","EUV","wafer inspection"]', 1, datetime('now'));
