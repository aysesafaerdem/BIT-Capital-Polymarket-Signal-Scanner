-- PostgreSQL / Supabase schema for Polymarket Signal Scanner
-- Normalized for queryability: markets, snapshots, signals, reports, portfolio, jobs

begin;

create extension if not exists pgcrypto;

-- Enums
DO $$ BEGIN
  CREATE TYPE relevance_label AS ENUM ('IGNORE','MONITOR','ACTIONABLE');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Utility trigger for updated_at
create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

-- Core market entity (stable fields)
create table if not exists markets (
  id text primary key,
  question text not null,
  description text,
  category text,
  subcategories jsonb default '[]'::jsonb,
  end_time timestamptz,
  status text not null default 'active',
  raw_market jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_markets_updated_at on markets;
create trigger trg_markets_updated_at
before update on markets
for each row execute function set_updated_at();

-- Market snapshots (time series)
create table if not exists market_snapshots (
  id bigserial primary key,
  market_id text not null references markets(id) on delete cascade,
  fetched_at timestamptz not null,
  current_yes numeric(7,4),
  current_no numeric(7,4),
  volume_usd numeric(18,2) default 0,
  liquidity_usd numeric(18,2) default 0,
  odds_history jsonb,
  related_markets jsonb,
  raw_data jsonb,
  created_at timestamptz not null default now(),
  unique (market_id, fetched_at)
);

-- Analysis dimensions
create table if not exists channels (
  code text primary key,
  label text not null
);

create table if not exists geographies (
  code text primary key,
  label text not null
);

create table if not exists themes (
  id bigserial primary key,
  name text not null unique
);

-- Analysis job/run metadata
create table if not exists signal_runs (
  id bigserial primary key,
  run_type text not null,
  model_name text,
  prompt_version text,
  status text not null default 'running',
  metadata jsonb,
  started_at timestamptz not null default now(),
  completed_at timestamptz
);

-- Analysis result (versioned)
create table if not exists market_signals (
  id bigserial primary key,
  market_id text not null references markets(id) on delete cascade,
  signal_run_id bigint references signal_runs(id) on delete set null,
  relevance_label relevance_label not null,
  relevance_score int not null check (relevance_score between 0 and 100),
  one_sentence_verdict text,
  event_type text,
  causal_chain jsonb,
  what_to_watch_next jsonb,
  red_flags_or_unknowns jsonb,
  raw_analysis jsonb,
  analyzed_at timestamptz not null default now(),
  is_latest boolean not null default true
);

-- Ensure only one latest signal per market
create unique index if not exists uq_market_signals_latest_per_market
  on market_signals (market_id)
  where is_latest = true;

-- Join tables for queryable dimensions
create table if not exists market_signal_channels (
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  channel_code text not null references channels(code),
  primary key (market_signal_id, channel_code)
);

create table if not exists market_signal_geographies (
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  geography_code text not null references geographies(code),
  primary key (market_signal_id, geography_code)
);

create table if not exists market_signal_keywords (
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  keyword text not null,
  primary key (market_signal_id, keyword)
);

create table if not exists market_signal_themes (
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  theme_id bigint not null references themes(id) on delete cascade,
  primary key (market_signal_id, theme_id)
);

-- Portfolio holdings + themes
create table if not exists portfolio_holdings (
  id bigserial primary key,
  name text not null,
  ticker text,
  fund text not null,
  weight_pct numeric(8,3),
  sector text,
  is_active boolean not null default true,
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_portfolio_holdings_updated_at on portfolio_holdings;
create trigger trg_portfolio_holdings_updated_at
before update on portfolio_holdings
for each row execute function set_updated_at();

create unique index if not exists uq_portfolio_holdings_ticker_fund
  on portfolio_holdings (coalesce(ticker, ''), fund, name);

create table if not exists portfolio_holding_themes (
  holding_id bigint not null references portfolio_holdings(id) on delete cascade,
  theme_id bigint not null references themes(id) on delete cascade,
  primary key (holding_id, theme_id)
);

-- Signal impact on holdings (normalized)
create table if not exists market_signal_holding_impacts (
  id bigserial primary key,
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  holding_id bigint references portfolio_holdings(id) on delete set null,
  ticker_or_symbol text,
  name text,
  direction text not null,
  mechanism text,
  time_horizon text,
  confidence numeric(5,4)
);

-- Normalized event abstraction (query-friendly summary of analysis result)
create table if not exists normalized_events (
  id bigserial primary key,
  market_signal_id bigint not null unique references market_signals(id) on delete cascade,
  market_id text not null references markets(id) on delete cascade,
  driver_category text,
  primary_channel text,
  macro_regime text,
  event_cadence text,
  event_type text,
  polarity text,
  entities jsonb default '[]'::jsonb,
  metadata jsonb,
  created_at timestamptz not null default now()
);

-- Fund routing (materialized from signal analysis for fast dashboard/report queries)
create table if not exists fund_routes (
  id bigserial primary key,
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  market_id text not null references markets(id) on delete cascade,
  fund_code text,
  fund_name text,
  fund_score numeric(8,3),
  label relevance_label,
  net_direction text,
  rationale text,
  metadata jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists uq_fund_routes_signal_fund
  on fund_routes (market_signal_id, coalesce(fund_code, ''), coalesce(fund_name, ''));

-- External evidence pack (news/retriever/rules-risk provenance)
create table if not exists evidence_items (
  id bigserial primary key,
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  market_id text not null references markets(id) on delete cascade,
  source text,
  source_type text,
  headline text,
  snippet text,
  url text,
  published_at timestamptz,
  evidence_kind text,
  metadata jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_evidence_items_market_signal on evidence_items(market_signal_id, created_at desc);
create index if not exists idx_evidence_items_market on evidence_items(market_id, created_at desc);

-- Reports (artifact + structured membership)
create table if not exists reports (
  id bigserial primary key,
  report_date date not null,
  title text not null,
  executive_summary text,
  actionable_count int not null default 0,
  monitor_count int not null default 0,
  ignore_count int not null default 0,
  full_report_html text,
  full_report_json jsonb,
  generated_at timestamptz not null default now()
);

create table if not exists report_items (
  report_id bigint not null references reports(id) on delete cascade,
  market_signal_id bigint not null references market_signals(id) on delete cascade,
  section text,
  rank int,
  primary key (report_id, market_signal_id)
);

-- Scheduler / jobs
create table if not exists job_runs (
  id bigserial primary key,
  job_name text not null,
  status text not null,
  message text,
  started_at timestamptz not null,
  completed_at timestamptz
);

-- Analyst alert rules (fund/user thresholds for routing-triggered notifications)
create table if not exists alerts (
  id bigserial primary key,
  user_key text,
  fund_code text,
  fund_name text,
  is_active boolean not null default true,
  threshold_odds_cross numeric(7,4),
  threshold_delta_24h_pp numeric(8,3),
  threshold_time_to_resolution_days int,
  threshold_relevance_score int,
  channels jsonb default '[]'::jsonb,
  delivery_targets jsonb default '[]'::jsonb,
  last_triggered_at timestamptz,
  metadata jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_alerts_updated_at on alerts;
create trigger trg_alerts_updated_at
before update on alerts
for each row execute function set_updated_at();

-- Helpful indexes
create index if not exists idx_markets_end_time on markets(end_time);
create index if not exists idx_market_snapshots_market_fetched on market_snapshots(market_id, fetched_at desc);
create index if not exists idx_market_signals_latest_label on market_signals(is_latest, relevance_label, analyzed_at desc);
create index if not exists idx_market_signals_market_latest on market_signals(market_id, is_latest, analyzed_at desc);
create index if not exists idx_normalized_events_driver_channel on normalized_events(driver_category, primary_channel, macro_regime);
create index if not exists idx_fund_routes_fund_label on fund_routes(fund_code, label, fund_score desc);
create index if not exists idx_report_items_report_rank on report_items(report_id, rank);
create index if not exists idx_reports_generated_at on reports(generated_at desc);
create index if not exists idx_market_signal_impacts_ticker on market_signal_holding_impacts(ticker_or_symbol);

-- JSONB indexes (optional but useful)
create index if not exists idx_market_snapshots_raw_data_gin on market_snapshots using gin (raw_data);
create index if not exists idx_market_signals_raw_analysis_gin on market_signals using gin (raw_analysis);

-- Seed common channels
insert into channels (code, label) values
  ('crypto', 'Crypto'),
  ('ai_semis', 'AI / Semiconductors'),
  ('rates', 'Rates / Macro'),
  ('oil', 'Oil / Energy'),
  ('cyber', 'Cybersecurity'),
  ('fintech', 'Fintech'),
  ('geopolitics', 'Geopolitics'),
  ('fx', 'FX')
on conflict (code) do nothing;

commit;
