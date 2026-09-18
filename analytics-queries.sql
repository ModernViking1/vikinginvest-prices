-- Viking Invest — usage analytics query reference
-- Source: public.usage_events (first-party, cookieless). Written client-side from the
-- dashboard; read here in the Supabase SQL editor (service role — clients cannot SELECT).
--
-- Event vocabulary:
--   event='page_load'  target='dashboard'   -- one per dashboard session boot
--   event='page_view'  target IN ('bt','dash','performance','investor')
--                                            -- a tab was opened; 'bt' = the Backtest / 3-Year page
--   event='panel_view' target='backtest_3y' -- the 3-Year Track Record panel was actually on screen
--
-- Columns: created_at, event, target, user_id (signed-in investor, null if anon), session_id, meta
-- Owner's own browsing (kmma@vikinginvest.org) is included; filter it out via the join in Q5 if wanted.

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1. THE HEADLINE: how many people look at the backtesting page?
--     Unique sessions and unique signed-in users that opened the Backtest tab.
select
  count(*)                                             as backtest_tab_opens,
  count(distinct session_id)                           as unique_sessions,
  count(distinct user_id) filter (where user_id is not null) as unique_signed_in_users
from public.usage_events
where event = 'page_view' and target = 'bt';

-- Q2. ENGAGEMENT FUNNEL: of everyone who opened the dashboard, how many reached
--     the Backtest tab, and how many actually saw the 3-Year panel render?
with sess as (
  select distinct session_id from public.usage_events where event = 'page_load'
), opened_bt as (
  select distinct session_id from public.usage_events where event = 'page_view' and target = 'bt'
), saw_panel as (
  select distinct session_id from public.usage_events where event = 'panel_view' and target = 'backtest_3y'
)
select
  (select count(*) from sess)      as sessions,
  (select count(*) from opened_bt) as opened_backtest_tab,
  (select count(*) from saw_panel) as saw_3y_panel,
  round(100.0 * (select count(*) from opened_bt) / nullif((select count(*) from sess),0), 1) as pct_opened_backtest;

-- Q3. TREND: Backtest-tab views per day (last 30 days).
select date_trunc('day', created_at)::date as day,
       count(*)                            as backtest_opens,
       count(distinct session_id)          as unique_sessions
from public.usage_events
where event = 'page_view' and target = 'bt'
  and created_at >= now() - interval '30 days'
group by 1 order by 1 desc;

-- Q4. TAB POPULARITY: which pages get the most attention overall?
select target as tab,
       count(*)                   as views,
       count(distinct session_id) as unique_sessions
from public.usage_events
where event = 'page_view'
group by 1 order by 2 desc;

-- Q5. NAMED INVESTORS who opened the Backtest tab (joins profiles/auth for the email).
--     Excludes the owner. Requires read access to auth.users (service role).
select coalesce(p.email, u.email) as investor_email,
       count(*)                   as backtest_opens,
       max(e.created_at)          as last_opened
from public.usage_events e
left join public.profiles p on p.id = e.user_id
left join auth.users     u on u.id = e.user_id
where e.event = 'page_view' and e.target = 'bt'
  and e.user_id is not null
  and coalesce(p.email, u.email) <> 'kmma@vikinginvest.org'
group by 1 order by 2 desc;

-- Q6. SIGNED-IN vs ANONYMOUS split for the backtesting page.
select case when user_id is null then 'anonymous' else 'signed_in' end as who,
       count(*)                   as opens,
       count(distinct session_id) as unique_sessions
from public.usage_events
where event = 'page_view' and target = 'bt'
group by 1;
