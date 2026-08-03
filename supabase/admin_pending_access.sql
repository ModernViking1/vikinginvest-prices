-- Run this once in the Supabase SQL Editor (Dashboard → project
-- opwdsuusdmsaicoyqxti → SQL Editor → New query).
--
-- Backs the "Access requests" queue in /stats.html. Returns every profile
-- that has requested tab access, WITH the account email — which lives in
-- auth.users, a schema the anon/authenticated role cannot read directly.
--
-- Why an RPC and not a plain select: the queue used to read public.profiles
-- via the owner RLS policy, but profiles has no email column, so a signed-in
-- visitor who never filled in a name rendered as "(no name)" with no way to
-- tell who they were. This function joins auth.users to surface the email as
-- the identifier. It is SECURITY DEFINER (runs as the function owner, which
-- can read auth.users) and enforces the admin gate INSIDE the body, exactly
-- like admin_signup_stats(). Email is never exposed to any client-readable
-- table — only this admin-gated RPC returns it.
--
-- ADD a new admin? Append to the array literal in the IF clause AND to the
-- ADMIN_EMAILS array in /stats.html — both lists must agree.

create or replace function public.admin_pending_access()
returns json
language plpgsql
security definer
set search_path = public
as $$
declare
  caller_email text;
  result       json;
begin
  caller_email := lower(coalesce((auth.jwt() ->> 'email'), ''));
  if caller_email not in ('kmma@vikinginvest.org') then
    raise exception 'not_authorised'
      using errcode = '42501';
  end if;

  select coalesce(
           json_agg(row_to_json(t) order by t.access_requested_at desc),
           '[]'::json)
    into result
  from (
    select p.id,
           u.email,
           p.first_name,
           p.last_name,
           p.company,
           p.role,
           p.country,
           p.access_requested_at,
           p.access_approved
      from public.profiles p
      join auth.users u on u.id = p.id
     where p.access_requested_at is not null
  ) t;

  return result;
end;
$$;

-- Authenticated users may call it; the body still rejects non-admins.
grant execute on function public.admin_pending_access() to authenticated;

-- Belt + braces: unauthenticated callers fail at the API layer.
revoke execute on function public.admin_pending_access() from anon;
