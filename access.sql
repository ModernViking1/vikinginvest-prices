-- ═══════════════════════════════════════════════════════════════════════
-- Tab access-control migration for the Viking Invest dashboard.
-- Run ONCE in the Supabase SQL editor (Dashboard → SQL Editor → New query).
--
-- Adds an owner-approval gate on the Performance / Investor / Backtest tabs:
--   • a signed-in user can request access (sets access_requested_at)
--   • only the OWNER can grant it (sets access_approved = true)
--   • users cannot approve themselves (enforced by the trigger below)
--
-- If the owner email ever changes, update it in all three places below AND in
-- the dashboard's OWNER_EMAIL constant.
-- ═══════════════════════════════════════════════════════════════════════

-- 1) Columns on the existing profiles table
alter table public.profiles
  add column if not exists access_approved     boolean     not null default false,
  add column if not exists access_requested_at timestamptz;

-- 2) Owner can read every profile (to see pending requests) and update approval.
drop policy if exists "owner reads all profiles" on public.profiles;
create policy "owner reads all profiles" on public.profiles
  for select using ( (auth.jwt() ->> 'email') = 'kmma@vikinginvest.org' );

drop policy if exists "owner updates profiles" on public.profiles;
create policy "owner updates profiles" on public.profiles
  for update using ( (auth.jwt() ->> 'email') = 'kmma@vikinginvest.org' );

-- 3) Prevent self-approval. A non-owner can set access_requested_at and edit
--    their own profile fields as before, but can NEVER set access_approved —
--    it is forced to false on insert and frozen to its prior value on update.
create or replace function public.guard_access_approved()
returns trigger language plpgsql security definer as $$
begin
  if coalesce((auth.jwt() ->> 'email'), '') <> 'kmma@vikinginvest.org' then
    if tg_op = 'INSERT' then
      new.access_approved := false;
    elsif new.access_approved is distinct from old.access_approved then
      new.access_approved := old.access_approved;
    end if;
  end if;
  return new;
end;
$$;

drop trigger if exists trg_guard_access_approved on public.profiles;
create trigger trg_guard_access_approved
  before insert or update on public.profiles
  for each row execute function public.guard_access_approved();

-- 4) Own-row access so a signed-in user can create/read/update THEIR OWN profile
--    row (this is how the "Request access" button writes access_requested_at).
--    Self-approval is still blocked by the trigger above, so an own-row update
--    can never set access_approved. Safe/idempotent — if your profiles.sql
--    already defines equivalent policies, these simply coexist.
drop policy if exists "own profile read"   on public.profiles;
create policy "own profile read"   on public.profiles for select using (auth.uid() = id);

drop policy if exists "own profile insert" on public.profiles;
create policy "own profile insert" on public.profiles for insert with check (auth.uid() = id);

drop policy if exists "own profile update" on public.profiles;
create policy "own profile update" on public.profiles for update using (auth.uid() = id);

-- Done. Reload the dashboard: guarded tabs now show a "Request access" overlay
-- for signed-in users, and you (the owner) get a floating "Access requests"
-- panel bottom-right to Approve / Revoke.
