# Visitor / usage tracking — reference

First-party, cookieless analytics for the dashboard (`Viking_Invest_Trading_v69.html`).
Added 2026-09-18. This note is the single source of truth so a session doesn't have to
re-read the HTML to understand it.

## What is tracked

Client-side JS logs three event types to Supabase table **`public.usage_events`**
(fire-and-forget, wrapped so a failure can never affect the dashboard):

| event        | target                                  | fires when |
|--------------|-----------------------------------------|-----------|
| `page_load`  | `dashboard`                             | once per dashboard boot (one per session load) |
| `page_view`  | `bt` / `dash` / `performance` / `investor` | a nav tab is opened. **`bt` = the Backtest / 3-Year Track Record page** |
| `panel_view` | `backtest_3y`                           | the 3-Year panel actually renders ≥50% on screen (opened bt AND scrolled to it) |

Row columns: `created_at, event, target, user_id, session_id, meta`.

- **`session_id`** — anonymous, cookieless per-browser id in `localStorage` (`vi_sid`).
  Counts unique visitors without PII. **Resets on cleared storage / new browser /
  private window**, so it slightly *over-counts* new/unique visitors (upper bound).
- **`user_id`** — the Supabase auth user id, attached only when signed in (null for anon).
- Owner's own browsing (`kmma@vikinginvest.org`) **is** logged; filter it out when needed.

Tracker code lives in the two `<script>` blocks near the end of the HTML
(search `viTrack` / `usage_events`). `window.viTrack(event, target, meta)` is the public
API; `switchPage()` calls it with the resolved route (after `log`→`bt`, `journal`→`dash`,
`newsletter`→`investor` aliasing).

## Where the data lives

- Supabase project **`opwdsuusdmsaicoyqxti`** (`https://opwdsuusdmsaicoyqxti.supabase.co`).
- Table `public.usage_events` is **insert-only to clients** (RLS). The anon key embedded
  in the HTML can **only INSERT** — it cannot SELECT.

## How to READ it (two paths)

1. **Owner panel, in-dashboard** — sign in as `kmma@vikinginvest.org`, open the **Backtest
   tab**. An owner-only "Usage Analytics" panel renders there, calling the RPC
   `public.usage_analytics(p_days)`. Shows tiles (sessions, opened_bt, saw_panel, bt_opens,
   unique/signed-in/anon viewers), a per-day trend, tab popularity, and named investors.
   The RPC is `security definer` and checks the caller's JWT email == owner, so no data
   leaks even if the client guard is bypassed.
2. **Supabase SQL editor** (service role) — run the queries in **`analytics-queries.sql`**.
   Q1 headline, Q2 funnel, Q3 daily trend, Q4 tab popularity, Q5 named investors,
   Q6 signed-in vs anon, **Q7 new-vs-returning (site)**, **Q8 new-vs-returning (backtest
   tab)**, Q9 revisit-frequency buckets. The `usage_analytics` RPC definition also lives at
   the bottom of that file (run once to (re)install).

## new vs returning

- **Built into the owner panel** (2026-09-30): the `usage_analytics` RPC returns
  `new_visitors` / `returning_visitors` and the Backtest-tab panel shows them as two tiles
  ("New visitors · first-ever", "Returning · % of active"). **Re-run the RPC** from
  `analytics-queries.sql` in the Supabase SQL editor after deploy or the tiles read 0.
- Ad-hoc SQL for the same numbers: **Q7** (site-wide), **Q8** (backtest tab), **Q9**
  (revisit-frequency buckets) in `analytics-queries.sql`.
- Definition: visitor key = `coalesce(user_id, session_id)`; **new** = first-ever event
  inside the window (from an ALL-TIME `first_seen`, not windowed); **returning** = active in
  the window but first seen before it. Anon `session_id` resets on cleared storage / new
  browser, so new-visitor counts are an upper bound.

## Constraint for agent sessions

**A cloud agent session cannot pull live numbers itself.** Supabase egress is blocked by
the agent proxy, AND the only read paths require the owner's login (RPC, `authenticated`
+ JWT email check) or the service role (SQL editor) — the embedded anon key is insert-only.
So: hand the user the SQL (Q1–Q9) to run in the Supabase SQL editor, or point them to the
owner panel on the Backtest tab. Don't attempt to query Supabase from a session.
