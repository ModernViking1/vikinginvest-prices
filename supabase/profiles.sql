-- Run this in Supabase SQL Editor after admin_signup_stats.sql.
-- Creates the profiles table + RLS + the admin_profile_stats() RPC.
--
-- Schema notes:
--   id              FK to auth.users — one row per user, deleted on
--                   account deletion (cascade)
--   first/last_name PII, optional, displayed back to the user in the
--                   topbar greeting if set
--   country         free text for v1 (United Kingdom / Denmark / etc).
--                   Switch to ISO 3166-1 alpha-2 later if we add
--                   region-specific behaviour
--   company         optional, used for consulting lead qualification
--   role            constrained enum — drives lead segmentation
--   experience      constrained enum — drives signal-explanation depth
--   telegram_handle mirrors auth.users.raw_user_meta_data.telegram_id
--                   so the profile is a self-contained read for admin
--                   surfaces (stats.html etc) without joining metadata
--   newsletter_opt_in explicit consent for marketing emails (lawful
--                   basis: consent)
--   consent_version + consent_at track which version of the privacy
--                   notice the user accepted. Bump consent_version
--                   here AND in the dashboard modal whenever the
--                   privacy text materially changes; the dashboard
--                   re-prompts users whose stored consent_version
--                   doesn't match the current code
--   dismissed_at    set by "Remind me later" so the modal doesn't
--                   nag every sign-in. Cleared when profile is saved
--   created_at /
--   updated_at      bookkeeping; updated_at is maintained by trigger
--
-- RLS posture: a user can SELECT / INSERT / UPDATE only their own
-- row, keyed on id = auth.uid(). No DELETE policy — deletion goes
-- via account removal (cascade from auth.users).

create table if not exists public.profiles (
  id                uuid primary key references auth.users(id) on delete cascade,
  first_name        text,
  last_name         text,
  country           text,
  company           text,
  role              text check (role in (
                      'portfolio_manager','day_trader','risk_analyst',
                      'treasury','quant','consultant','student','other')),
  experience        text check (experience in (
                      'beginner','intermediate','advanced','professional')),
  telegram_handle   text,
  newsletter_opt_in boolean not null default false,
  -- Instrument-class Telegram alert subscriptions. Valid values: any
  -- subset of ('major','minor','comm','index','crypto','sr_5_5',
  -- 'sr_4_5','sr_3_5'). The first five are the instrument-class
  -- buckets; the last three are School Run tier opt-ins for DE40/DJ30
  -- that route around the normal class filter when the opening-range
  -- state machine fires. No CHECK constraint on the column — keep the
  -- valid-value list in sync with VIPRO_CLASS_KEYS in the dashboard
  -- HTML and SR_TIER_TO_KIND in detect_triggers.py. The default
  -- inserts all five instrument classes so newly-created rows match
  -- the legacy "send everything" behaviour (SR tiers are opt-in only).
  -- An EMPTY array (user explicitly unchecked everything in the
  -- profile modal) is also a valid state and means "do not send me
  -- Telegram alerts" — the per-user routing code treats
  -- `array_length(alert_classes, 1) IS NULL` as "user opted out" and
  -- skips them, even though their telegram_handle is still on file.
  alert_classes     text[] not null default array['major','minor','comm','index','crypto'],
  consent_version   text,
  consent_at        timestamptz,
  dismissed_at      timestamptz,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

-- Forward-compatible: add the column on an existing table too, so
-- re-running this script against a schema created before 2026-06-04
-- backfills the column without recreating the table.
alter table public.profiles
  add column if not exists alert_classes text[] not null
  default array['major','minor','comm','index','crypto'];

alter table public.profiles enable row level security;

-- A user can read their own row.
drop policy if exists "profiles read own" on public.profiles;
create policy "profiles read own"
  on public.profiles for select
  using (id = auth.uid());

-- A user can insert their own row (id must match the authenticated uid).
drop policy if exists "profiles insert own" on public.profiles;
create policy "profiles insert own"
  on public.profiles for insert
  with check (id = auth.uid());

-- A user can update their own row. The WITH CHECK guard prevents
-- changing id to someone else's uid via UPDATE.
drop policy if exists "profiles update own" on public.profiles;
create policy "profiles update own"
  on public.profiles for update
  using (id = auth.uid())
  with check (id = auth.uid());

-- Keep updated_at honest without trusting the client.
create or replace function public.profiles_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists profiles_updated_at on public.profiles;
create trigger profiles_updated_at
  before update on public.profiles
  for each row execute function public.profiles_set_updated_at();


-- ── Admin profile stats RPC ──────────────────────────────────────
-- Same SECURITY DEFINER + email-allowlist pattern as
-- admin_signup_stats(). Returns aggregate completion stats for the
-- /stats.html profile panel; never exposes individual rows.
--
-- Field completion is "row has the field populated", not "field is
-- non-empty after trim". An empty-string company counts as filled
-- (the table-level constraint allows it). Add a trim filter here if
-- you start seeing junk values dominate the stats.
create or replace function public.admin_profile_stats()
returns json
language plpgsql
security definer
set search_path = public
as $$
declare
  caller_email text;
  v_total           bigint;
  v_completed       bigint;
  v_first_name      bigint;
  v_last_name       bigint;
  v_country         bigint;
  v_company         bigint;
  v_role            bigint;
  v_experience      bigint;
  v_telegram        bigint;
  v_newsletter      bigint;
  v_cls_major       bigint;
  v_cls_minor       bigint;
  v_cls_comm        bigint;
  v_cls_index       bigint;
  v_cls_crypto      bigint;
begin
  caller_email := lower(coalesce((auth.jwt() ->> 'email'), ''));
  if caller_email not in ('kmma@vikinginvest.org') then
    raise exception 'not_authorised'
      using errcode = '42501';
  end if;

  select count(*) into v_total from public.profiles;
  select count(*) into v_first_name from public.profiles where first_name is not null;
  select count(*) into v_last_name  from public.profiles where last_name  is not null;
  select count(*) into v_country    from public.profiles where country    is not null;
  select count(*) into v_company    from public.profiles where company    is not null;
  select count(*) into v_role       from public.profiles where role       is not null;
  select count(*) into v_experience from public.profiles where experience is not null;
  select count(*) into v_telegram   from public.profiles where telegram_handle is not null;
  select count(*) into v_newsletter from public.profiles where newsletter_opt_in = true;
  -- Alert-class subscription counts. Each profile is counted in every
  -- class it subscribes to, so the five numbers sum to >= total
  -- (anyone subscribed to >1 class is counted multiple times).
  select count(*) into v_cls_major  from public.profiles where 'major'  = any(alert_classes);
  select count(*) into v_cls_minor  from public.profiles where 'minor'  = any(alert_classes);
  select count(*) into v_cls_comm   from public.profiles where 'comm'   = any(alert_classes);
  select count(*) into v_cls_index  from public.profiles where 'index'  = any(alert_classes);
  select count(*) into v_cls_crypto from public.profiles where 'crypto' = any(alert_classes);

  -- "Completed" = the four fields a sales person would actually use
  -- to qualify a lead. Adjust this definition if the modal's
  -- mandatory set changes.
  select count(*) into v_completed
    from public.profiles
   where first_name is not null
     and country    is not null
     and role       is not null
     and experience is not null;

  return json_build_object(
    'total',           v_total,
    'completed',       v_completed,
    'fields', json_build_object(
      'first_name',  v_first_name,
      'last_name',   v_last_name,
      'country',     v_country,
      'company',     v_company,
      'role',        v_role,
      'experience',  v_experience,
      'telegram',    v_telegram,
      'newsletter',  v_newsletter
    ),
    'alert_classes', json_build_object(
      'major',  v_cls_major,
      'minor',  v_cls_minor,
      'comm',   v_cls_comm,
      'index',  v_cls_index,
      'crypto', v_cls_crypto
    ),
    'updated',         now()
  );
end;
$$;

grant execute on function public.admin_profile_stats() to authenticated;
revoke execute on function public.admin_profile_stats() from anon;

-- Force PostgREST to reload its schema cache so the column added above
-- becomes visible to the dashboard's /rest/v1/profiles upsert path.
-- Symptom this fixes: "Could not find the 'alert_classes' column of
-- 'profiles' in the schema cache" when saving the profile modal. Safe
-- to run on every re-execution — it's just a signal.
notify pgrst, 'reload schema';
