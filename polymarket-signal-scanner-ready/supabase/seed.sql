-- Sample seed data for reviewer/demo runs.
-- Designed to be idempotent for repeated `supabase db reset` runs.

begin;

insert into public.channels (code, label) values
  ('crypto', 'Crypto'),
  ('ai_semis', 'AI / Semiconductors'),
  ('rates', 'Rates / Macro'),
  ('oil', 'Oil / Energy'),
  ('cyber', 'Cybersecurity'),
  ('fintech', 'Fintech'),
  ('geopolitics', 'Geopolitics'),
  ('fx', 'FX')
on conflict (code) do update set label = excluded.label;

insert into public.markets (
  id, question, description, category, subcategories, end_time, status, raw_market
) values
  (
    'sample-fed-cut',
    'Will the Fed cut rates by 25 bps at the next meeting?',
    'Sample macro policy market for reviewer validation.',
    'macro',
    '["rates","fomc"]'::jsonb,
    now() + interval '21 days',
    'active',
    '{"source":"seed"}'::jsonb
  ),
  (
    'sample-ai-export-controls',
    'Will the US expand AI chip export controls this quarter?',
    'Sample AI/semis regulation market for reviewer validation.',
    'policy',
    '["ai","semis","regulation"]'::jsonb,
    now() + interval '30 days',
    'active',
    '{"source":"seed"}'::jsonb
  )
on conflict (id) do update set
  question = excluded.question,
  description = excluded.description,
  category = excluded.category,
  subcategories = excluded.subcategories,
  end_time = excluded.end_time,
  status = excluded.status,
  raw_market = excluded.raw_market,
  updated_at = now();

insert into public.market_snapshots (
  market_id, fetched_at, current_yes, current_no, volume_usd, liquidity_usd, raw_data
) values
  ('sample-fed-cut', now() - interval '2 hours', 0.41, 0.59, 250000, 1250000, '{"source":"seed"}'::jsonb),
  ('sample-ai-export-controls', now() - interval '2 hours', 0.57, 0.43, 410000, 1890000, '{"source":"seed"}'::jsonb)
on conflict (market_id, fetched_at) do nothing;

insert into public.signal_runs (id, run_type, model_name, prompt_version, status, metadata, started_at, completed_at)
values
  (9001, 'analysis', 'rule_based_seed', 'v1', 'completed', '{"source":"seed"}'::jsonb, now() - interval '90 minutes', now() - interval '85 minutes')
on conflict (id) do update set
  status = excluded.status,
  completed_at = excluded.completed_at,
  metadata = excluded.metadata;

insert into public.market_signals (
  id, market_id, signal_run_id, relevance_label, relevance_score,
  one_sentence_verdict, event_type, causal_chain, what_to_watch_next,
  red_flags_or_unknowns, raw_analysis, analyzed_at, is_latest
) values
  (
    9101,
    'sample-fed-cut',
    9001,
    'ACTIONABLE',
    87,
    'Rate-cut repricing can shift duration-sensitive tech multiples and risk appetite.',
    'macro_policy',
    '["Fed guidance softens","Front-end yields fall","Growth-duration equities re-rate"]'::jsonb,
    '["FOMC statement language","Dot-plot median change","2Y yield reaction"]'::jsonb,
    '["Path dependency on inflation surprise"]'::jsonb,
    '{"source":"seed"}'::jsonb,
    now() - interval '80 minutes',
    true
  ),
  (
    9102,
    'sample-ai-export-controls',
    9001,
    'ACTIONABLE',
    91,
    'Tighter export controls can pressure semi supply chains and AI capacity assumptions.',
    'regulatory',
    '["Export scope expands","China demand mix shifts","Semi supply chain reprices"]'::jsonb,
    '["US BIS announcement","Vendor guidance updates","Lead-time commentary"]'::jsonb,
    '["Policy wording ambiguity"]'::jsonb,
    '{"source":"seed"}'::jsonb,
    now() - interval '75 minutes',
    true
  )
on conflict (id) do update set
  relevance_label = excluded.relevance_label,
  relevance_score = excluded.relevance_score,
  one_sentence_verdict = excluded.one_sentence_verdict,
  event_type = excluded.event_type,
  causal_chain = excluded.causal_chain,
  what_to_watch_next = excluded.what_to_watch_next,
  red_flags_or_unknowns = excluded.red_flags_or_unknowns,
  raw_analysis = excluded.raw_analysis,
  analyzed_at = excluded.analyzed_at,
  is_latest = excluded.is_latest;

insert into public.normalized_events (
  market_signal_id, market_id, driver_category, primary_channel, macro_regime,
  event_cadence, event_type, polarity, entities, metadata
) values
  (
    9101,
    'sample-fed-cut',
    'rates_duration',
    'rates',
    'policy_surprise_dovish',
    'scheduled',
    'macro_policy',
    'risk_on',
    '["Fed","US rates"]'::jsonb,
    '{"source":"seed"}'::jsonb
  ),
  (
    9102,
    'sample-ai-export-controls',
    'trade_tariffs',
    'ai_semis',
    'geopolitical_risk_premium',
    'event_driven',
    'regulatory',
    'risk_off',
    '["US BIS","AI semiconductors"]'::jsonb,
    '{"source":"seed"}'::jsonb
  )
on conflict (market_signal_id) do update set
  driver_category = excluded.driver_category,
  primary_channel = excluded.primary_channel,
  macro_regime = excluded.macro_regime,
  event_cadence = excluded.event_cadence,
  event_type = excluded.event_type,
  polarity = excluded.polarity,
  entities = excluded.entities,
  metadata = excluded.metadata;

insert into public.portfolio_holdings (
  id, name, ticker, fund, weight_pct, sector, is_active
) values
  (9201, 'NVIDIA Corp', 'NVDA', 'BIT Global Technology Leaders', 8.200, 'Semiconductors', true),
  (9202, 'Taiwan Semiconductor', 'TSM', 'BIT Global Technology Leaders', 6.900, 'Semiconductors', true),
  (9203, 'Coinbase', 'COIN', 'BIT Global Crypto Leaders', 5.100, 'Crypto Infrastructure', true)
on conflict (id) do update set
  name = excluded.name,
  ticker = excluded.ticker,
  fund = excluded.fund,
  weight_pct = excluded.weight_pct,
  sector = excluded.sector,
  is_active = excluded.is_active,
  updated_at = now();

insert into public.market_signal_holding_impacts (
  market_signal_id, holding_id, ticker_or_symbol, name, direction, mechanism, time_horizon, confidence
)
select 9101, 9201, 'NVDA', 'NVIDIA Corp', 'UP', 'Lower discount-rate pressure supports duration-sensitive valuation', '0-3m', 0.76
where not exists (
  select 1 from public.market_signal_holding_impacts
  where market_signal_id = 9101 and ticker_or_symbol = 'NVDA'
);

insert into public.market_signal_holding_impacts (
  market_signal_id, holding_id, ticker_or_symbol, name, direction, mechanism, time_horizon, confidence
)
select 9102, 9202, 'TSM', 'Taiwan Semiconductor', 'DOWN', 'Export scope expansion may reduce accessible demand in restricted geographies', '0-6m', 0.81
where not exists (
  select 1 from public.market_signal_holding_impacts
  where market_signal_id = 9102 and ticker_or_symbol = 'TSM'
);

insert into public.fund_routes (
  market_signal_id, market_id, fund_code, fund_name, fund_score, label, net_direction, rationale, metadata
)
select 9101, 'sample-fed-cut', 'BIT_TL', 'BIT Global Technology Leaders', 82.4, 'ACTIONABLE', 'UP', 'Rate repricing transmits through duration channel to core tech names', '{"source":"seed"}'::jsonb
where not exists (
  select 1 from public.fund_routes
  where market_signal_id = 9101 and fund_code = 'BIT_TL' and fund_name = 'BIT Global Technology Leaders'
);

insert into public.fund_routes (
  market_signal_id, market_id, fund_code, fund_name, fund_score, label, net_direction, rationale, metadata
)
select 9102, 'sample-ai-export-controls', 'BIT_TL', 'BIT Global Technology Leaders', 88.9, 'ACTIONABLE', 'DOWN', 'Policy shock routes through AI/semi channel to foundry and GPU supply chain', '{"source":"seed"}'::jsonb
where not exists (
  select 1 from public.fund_routes
  where market_signal_id = 9102 and fund_code = 'BIT_TL' and fund_name = 'BIT Global Technology Leaders'
);

insert into public.evidence_items (
  market_signal_id, market_id, source, source_type, headline, snippet, url, published_at, evidence_kind, metadata
)
select
  9102,
  'sample-ai-export-controls',
  'seed_news',
  'news',
  'US officials signal tighter advanced-chip export controls',
  'Policy commentary suggests broader licensing scope for selected AI accelerators.',
  'https://example.com/seed/export-controls',
  now() - interval '1 day',
  'policy_headline',
  '{"source":"seed"}'::jsonb
where not exists (
  select 1 from public.evidence_items
  where market_signal_id = 9102 and headline = 'US officials signal tighter advanced-chip export controls'
);

insert into public.reports (
  id, report_date, title, executive_summary,
  actionable_count, monitor_count, ignore_count,
  full_report_html, full_report_json, generated_at
) values (
  9301,
  current_date,
  'Sample Signal Report (Seed)',
  'Seeded analyst report: policy-rate and export-control scenarios are currently flagged as actionable.',
  2,
  0,
  0,
  '<h3>Analyst Action Center</h3><p>Review rate and export-control channels for semis sensitivity.</p>',
  '{"source":"seed","top_actionable":["sample-fed-cut","sample-ai-export-controls"]}'::jsonb,
  now() - interval '60 minutes'
)
on conflict (id) do update set
  report_date = excluded.report_date,
  title = excluded.title,
  executive_summary = excluded.executive_summary,
  actionable_count = excluded.actionable_count,
  monitor_count = excluded.monitor_count,
  ignore_count = excluded.ignore_count,
  full_report_html = excluded.full_report_html,
  full_report_json = excluded.full_report_json,
  generated_at = excluded.generated_at;

insert into public.report_items (report_id, market_signal_id, section, rank) values
  (9301, 9102, 'top_actionable', 1),
  (9301, 9101, 'top_actionable', 2)
on conflict (report_id, market_signal_id) do update set
  section = excluded.section,
  rank = excluded.rank;

insert into public.job_runs (id, job_name, status, message, started_at, completed_at)
values
  (9401, 'ingestion', 'SUCCESS', 'Seeded ingestion job', now() - interval '2 hours', now() - interval '119 minutes'),
  (9402, 'analysis', 'SUCCESS', 'Seeded analysis job', now() - interval '110 minutes', now() - interval '108 minutes'),
  (9403, 'report', 'SUCCESS', 'Seeded report job', now() - interval '65 minutes', now() - interval '60 minutes')
on conflict (id) do update set
  status = excluded.status,
  message = excluded.message,
  started_at = excluded.started_at,
  completed_at = excluded.completed_at;

commit;
