# Swing signal pipeline — data freshness, execution gating, and alerts

This is the reference for how a swing setup becomes (or fails to become) a cTrader
fill, and why a Telegram alert can fire with no corresponding trade. It exists so we
don't have to re-derive this every time "strategy X alerted but didn't execute" comes
up. The intraday path (`signals.json` / `VikingInvestSignalBridge.cs`) is separate and
does **not** share these problems — it's fed by `fetch-data.yml` every ~5 min.

## The chain

```
fetch_historical_ohlc.py  (publish-historical-ohlc.yml, 6-HOURLY)
      │  full rebuild: 365d × {m15,h1,daily} × all pairs → historical-ohlc.json  (committed, ~60 MB)
      ▼
swing-signals-feed.yml  (every 10 min + repository_dispatch pinger)
   1. freshen_h1_tail.py     → splice recent H1  onto tail   (transient, discarded after run)
   2. freshen_daily_tail.py  → splice recent daily onto tail (transient)
   3. freshen_m15_tail.py    → splice recent m15  onto tail  (transient)
   4. swing_signals.py       → swing-signals.json            (COMMITTED + CDN-purged)
      ▼
jsdelivr CDN  (@main)  ──poll──►  VikingSwingBridge.cs  (cTrader, DEMO account 9563532)
      │                                    │ executes if it passes every gate below
      ▼                                    ▼
crypto_alert.py (in fetch-data.yml)   swing-executions.json  (via repository_dispatch → ingest-swing-execution.yml)
   → Telegram alert (independent of execution!)
```

## Why the tail-fresheners exist (the recurring bug)

`historical-ohlc.json` is rebuilt in full only every **6 hours**. Between rebuilds its
tail goes stale. The swing cBot rejects any signal whose **trigger bar** is older than
`MaxSignalAgeMin` (120 min) — so a signal built on a stale bar is *born* past the cap and
never fills. Each freshener does the cheap half of the backfill on the feed's tight
cadence, splicing just the recent tail of one layer onto `historical-ohlc.json` for that
run only (then the file is `git checkout`-reverted so the 6-hourly job stays the
committed source of truth). All three are **fail-open**: any error leaves the file
unchanged and the feed runs on existing data.

| Freshener | Layer | Who needs it |
|-----------|-------|--------------|
| `freshen_h1_tail.py`    | H1    | H1/4H-triggered swing setups |
| `freshen_daily_tail.py` | daily | cam_rev — its Camarilla levels come from the **prior day's** daily bar; a stale daily layer means today has no levels → 0 cam_rev emitted |
| `freshen_m15_tail.py`   | m15   | m15-triggered setups: **cam_rev's rejection candle** and the 24/7 **crypto** detectors (twob / engulf_manip / mmove). Crypto is the acute case (trades weekends, so it keeps printing bars the 6-hourly job can't keep up with) |

**If you add a strategy that triggers on a new timeframe, it needs a freshener for that
layer** or its signals will be born stale and never fill.

cam_rev additionally needs a synthesized current-day daily bar (`_synth_current_daily`
in `swing_signals.py`) because in live data today's daily bar hasn't closed yet; today's
levels still come from the real prior-day close.

## Execution gates (VikingSwingBridge.cs, in order)

A signal in `swing-signals.json` fills **only if it clears all of these**:

1. **Not already seen / in-flight** (`_seenIds`, `_inFlight`) — dedup.
2. **`demo_only` + `Account.IsLive`** → skip. On the current **DEMO** account this gate
   **never fires**, so demo_only strategies (e.g. `fma_gold`, and the cam_rev crypto
   pilot) **do execute on demo**. `demo_only` only blocks on a **LIVE** account.
3. **Broker-unavailable pair** (`_brokerUnavailable` = tao/sui/near/aster/lighter …) →
   skip. The broker has these instruments disabled; the order would bounce.
4. **Expiry** (`expiry_ts` past) → skip.
5. **`MaxSignalAgeMin`** (120 min, measured from `trigger_ts`) → skip if older.
6. **Duplicate open** (same pair+strategy+direction already open) → skip;
   opposite-direction same-strategy position gets flattened first (reversal).

There is **no per-strategy whitelist** — the cBot executes whatever strategy string the
feed emits, subject only to the gates above.

## Telegram alerts are independent of execution

`crypto_alert.py` (run at the end of `fetch-data.yml`, ~every 5 min) reads
`swing-signals.json` + `signals.json`, keeps `class:crypto` + `state:triggered` from any
strategy that is **LIVE (promoted) and has positive forward expectancy** (measured via
`observer_review`), and Telegrams it once. It does **NOT** look at `demo_only`, the age
cap, broker availability, or the CDN — its footer says *"Demo/observed — not advice."*

**Consequence:** a Telegram alert firing with **no cTrader fill is expected and not a
bug** whenever the signal is demo_only-on-live, on a broker-unavailable pair, aged past
the 120-min cap, or simply not yet polled. The alert is the near-instant server-side
view; the fill depends on the cBot polling a CDN-fresh feed inside the age window.

## CDN / timing caveat

The feed commits and calls `purge.jsdelivr.net`, but **purges are rate-limited** (seen
throttled with a ~30-min reset). Until the edge revalidates, the cBot may poll a stale
feed — which can push a fresh-but-borderline signal past the 120-min cap before the bot
ever sees it. The 10-min feed cron re-purges each cycle, so it self-heals, but it eats
into the freshness margin. If borderline fills are being missed, the levers are: raise
`MaxSignalAgeMin`, or shorten the CDN exposure (e.g. raw.githubusercontent fallback).

## cam_rev quick reference

- Camarilla pivot **reversal**; daily R3/S3 levels from the prior day; **m15** rejection
  trigger; RR 1:1; session-filtered (`CAM_SESS_OPEN`/`CLOSE` in `unified_shadow_harness`).
- **Fully LIVE across all its classes** — major/minor/index/comm **and** the crypto
  pilot (btc/eth/xrp/sol via `CAM_CRYPTO_PILOT`). None are `demo_only`. Crypto was
  promoted from demo-only to live execution on **2026-09-19** at the user's direction
  (3-yr crypto expectancy was marginal/negative, so watch the live-vs-observer gap on
  those pairs); the observer tracks them in parallel as the clean benchmark.
- Historical 0 fills were because cam_rev emitted **0 signals** at all until the daily +
  m15 freshness fixes (Sept 2026); there was never a per-strategy execution block.
