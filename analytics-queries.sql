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

-- ─────────────────────────────────────────────────────────────────────────────
-- RPC for the in-dashboard owner-only "Usage Analytics" panel.
-- Run this ONCE in the Supabase SQL editor. It is the ONLY read path into
-- usage_events: the table stays insert-only to clients, and this function returns
-- data solely when the caller's JWT email is the owner (checked inside, so even a
-- crafted call by another signed-in user gets nothing). SECURITY DEFINER so it can
-- read usage_events + resolve investor emails without loosening any table RLS.
create or replace function public.usage_analytics(p_days int default 14)
returns jsonb
language plpgsql
security definer
set search_path = public, auth
as $$
declare
  v_email text := lower(coalesce(auth.jwt() ->> 'email', ''));
  v_from  timestamptz := now() - make_interval(days => greatest(coalesce(p_days,14), 1));
  v_out   jsonb;
begin
  if v_email <> 'kmma@vikinginvest.org' then
    raise exception 'not authorized';
  end if;

  with ev as (
    select * from public.usage_events where created_at >= v_from
  ),
  bt as (
    select * from ev where event = 'page_view' and target = 'bt'
  ),
  panel_sessions as (
    select distinct session_id from ev where event = 'panel_view' and target = 'backtest_3y'
  ),
  tiles as (
    select
      (select count(*) from bt)                                                  as bt_opens,
      (select count(distinct coalesce(user_id::text, session_id)) from bt)       as unique_viewers,
      (select count(distinct user_id) from bt where user_id is not null)         as signed_in_viewers,
      (select count(distinct session_id) from bt where user_id is null)          as anon_sessions,
      (select count(distinct session_id) from ev where event = 'page_load')      as sessions,
      (select count(distinct session_id) from bt)                                as opened_bt,
      (select count(*) from panel_sessions)                                      as saw_panel
  ),
  trend as (
    select date_trunc('day', created_at)::date as day, count(*) as opens
    from bt group by 1 order by 1
  ),
  tabs as (
    select target as tab, count(*) as views
    from ev where event = 'page_view' and target is not null group by 1 order by 2 desc
  ),
  investors as (
    -- email lives in auth.users (public.profiles has no email column)
    select coalesce(u.email, 'user ' || left(bt.user_id::text, 8)) as email,
           count(*)                                                 as opens,
           bool_or(ps.session_id is not null)                       as saw_panel,
           max(bt.created_at)                                       as last_opened
    from bt
    left join auth.users     u  on u.id = bt.user_id
    left join panel_sessions ps on ps.session_id = bt.session_id
    where bt.user_id is not null
      and coalesce(u.email, '') <> 'kmma@vikinginvest.org'
    group by 1 order by 2 desc limit 50
  )
  select jsonb_build_object(
    'range_days', greatest(coalesce(p_days,14),1),
    'generated',  now(),
    'tiles',      (select to_jsonb(t) from tiles t),
    'trend',      coalesce((select jsonb_agg(jsonb_build_object('day', day, 'opens', opens) order by day) from trend), '[]'::jsonb),
    'tabs',       coalesce((select jsonb_agg(jsonb_build_object('tab', tab, 'views', views) order by views desc) from tabs), '[]'::jsonb),
    'investors',  coalesce((select jsonb_agg(jsonb_build_object('email', email, 'opens', opens, 'saw_panel', saw_panel, 'last_opened', last_opened) order by opens desc) from investors), '[]'::jsonb)
  ) into v_out;

  return v_out;
end;
$$;

revoke all     on function public.usage_analytics(int) from public, anon;
grant  execute on function public.usage_analytics(int) to authenticated;
