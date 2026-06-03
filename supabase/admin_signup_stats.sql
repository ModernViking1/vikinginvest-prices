-- Run this once in Supabase SQL Editor (https://supabase.com/dashboard
-- → project opwdsuusdmsaicoyqxti → SQL Editor → New query).
--
-- Creates a SECURITY DEFINER function that the anon client (used by
-- /stats.html) can call via sb.rpc('admin_signup_stats'). The function
-- runs with privileges of the function owner (postgres role), so it
-- can read auth.users — which the anon role normally cannot.
--
-- The admin gate is enforced INSIDE the function: it inspects the
-- caller's JWT and rejects everyone except the email allowlist below,
-- mirroring the ADMIN_EMAILS constant in /stats.html. Anyone else
-- calling this RPC gets a 'not_authorised' error.
--
-- ADD a new admin? Append to the array literal in the IF clause AND to
-- the ADMIN_EMAILS array in /stats.html — both lists must agree or
-- the gate falls through inconsistently.

create or replace function public.admin_signup_stats()
returns json
language plpgsql
security definer
set search_path = public
as $$
declare
  caller_email text;
  v_total      bigint;
  v_last_7d    bigint;
  v_last_30d   bigint;
  v_latest     timestamptz;
begin
  caller_email := lower(coalesce((auth.jwt() ->> 'email'), ''));
  if caller_email not in ('kmma@vikinginvest.org') then
    raise exception 'not_authorised'
      using errcode = '42501';
  end if;

  select count(*) into v_total from auth.users;
  select count(*) into v_last_7d
    from auth.users
   where created_at >= now() - interval '7 days';
  select count(*) into v_last_30d
    from auth.users
   where created_at >= now() - interval '30 days';
  select max(created_at) into v_latest from auth.users;

  return json_build_object(
    'total',         v_total,
    'last_7d',       v_last_7d,
    'last_30d',      v_last_30d,
    'latest_signup', v_latest,
    'updated',       now()
  );
end;
$$;

-- Allow authenticated users to call it. The function still rejects
-- non-admins inside its body — grant just opens the RPC endpoint.
grant execute on function public.admin_signup_stats() to authenticated;

-- Optional: revoke from anon so unauthenticated calls fail fast at
-- the API layer rather than running the function and rejecting on
-- the email check. Belt + braces.
revoke execute on function public.admin_signup_stats() from anon;
