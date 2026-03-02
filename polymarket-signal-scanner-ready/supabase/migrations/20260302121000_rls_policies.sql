begin;

-- Enable RLS on analyst-facing tables for API safety.
alter table if exists public.markets enable row level security;
alter table if exists public.market_snapshots enable row level security;
alter table if exists public.market_signals enable row level security;
alter table if exists public.normalized_events enable row level security;
alter table if exists public.fund_routes enable row level security;
alter table if exists public.evidence_items enable row level security;
alter table if exists public.reports enable row level security;
alter table if exists public.report_items enable row level security;
alter table if exists public.portfolio_holdings enable row level security;
alter table if exists public.market_signal_holding_impacts enable row level security;
alter table if exists public.channels enable row level security;
alter table if exists public.job_runs enable row level security;

-- Read-only policies for anon/authenticated clients (dashboard consumption).
drop policy if exists markets_read_all on public.markets;
create policy markets_read_all on public.markets
for select to anon, authenticated using (true);

drop policy if exists market_snapshots_read_all on public.market_snapshots;
create policy market_snapshots_read_all on public.market_snapshots
for select to anon, authenticated using (true);

drop policy if exists market_signals_read_all on public.market_signals;
create policy market_signals_read_all on public.market_signals
for select to anon, authenticated using (true);

drop policy if exists normalized_events_read_all on public.normalized_events;
create policy normalized_events_read_all on public.normalized_events
for select to anon, authenticated using (true);

drop policy if exists fund_routes_read_all on public.fund_routes;
create policy fund_routes_read_all on public.fund_routes
for select to anon, authenticated using (true);

drop policy if exists evidence_items_read_all on public.evidence_items;
create policy evidence_items_read_all on public.evidence_items
for select to anon, authenticated using (true);

drop policy if exists reports_read_all on public.reports;
create policy reports_read_all on public.reports
for select to anon, authenticated using (true);

drop policy if exists report_items_read_all on public.report_items;
create policy report_items_read_all on public.report_items
for select to anon, authenticated using (true);

drop policy if exists portfolio_holdings_read_all on public.portfolio_holdings;
create policy portfolio_holdings_read_all on public.portfolio_holdings
for select to anon, authenticated using (true);

drop policy if exists market_signal_holding_impacts_read_all on public.market_signal_holding_impacts;
create policy market_signal_holding_impacts_read_all on public.market_signal_holding_impacts
for select to anon, authenticated using (true);

drop policy if exists channels_read_all on public.channels;
create policy channels_read_all on public.channels
for select to anon, authenticated using (true);

drop policy if exists job_runs_read_all on public.job_runs;
create policy job_runs_read_all on public.job_runs
for select to anon, authenticated using (true);

commit;
