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

### Market vs LIMIT entries (`entry_mode`)

Most strategies emit `entry_mode: "market"` — the cBot fills at market the moment it sees
a fresh signal. **cam_rev emits `entry_mode: "limit"`** because it is a level *fade*: it
must fill **at** the pivot (`ref_entry`), not chase the bounce. For a limit signal the cBot
rests a **pending limit order** at `ref_entry` (via `PlaceLimitEntry`), self-expiring at the
signal's `ExpiryTs`, and **skips the market-only gates** — the `MaxSignalAgeMin` age cap,
the market-price wrong-side check, and the entry-drift cap. It keeps the protective ones
(demo_only, broker-unavailable, expiry, stop-invalidation, min-stop, and a pending-order
dedup so re-triggers don't stack). The fill is recorded by a `Positions.Opened` handler
(keyed on `_pendingLimitIds`, so market fills are never double-written).

**Why this matters:** market-mode cam_rev filled **zero** times ever. Its signals are born
8–14h old (a fade's rejection candle is hours behind live), so the 120-min age cap rejected
every one; and even when fresh, the entry-drift cap refused the late market chase off the
level. Both are correct for market orders and both are moot for a resting limit. A limit at
the level also matches the observer's precise-close backtest, so live WR should track the
model instead of lagging it. **Requires a cBot rebuild in cTrader to take effect** — the
feed change alone does nothing until the deployed binary parses `entry_mode`.

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
- **LIVE for major/minor/index/comm; crypto is DEMO-ONLY.** The 3-yr limit-execution
  backtest (`cam_limit_backtest.py`, 28k signals) settled it: at prompt placement the limit
  reproduces the observer baseline (FX/index/comm +0.46..+0.55R, robust in both OOS halves),
  but crypto (btc/eth/xrp/sol via `CAM_CRYPTO_PILOT`) is break-even at lag 0 and **negative
  under any placement latency** (−0.12R@30m .. −0.23R@2h, both OOS halves red). So crypto is
  emitted `demo_only=True` — observed on demo, never live. (It briefly went live 2026-09-19
  before this backtest existed; reverted 2026-09-21 on the evidence.)
- Historical 0 fills had two stacked causes: it emitted **0 signals** until the daily + m15
  freshness fixes, and once emitting, its **market** orders were rejected every time (born
  8–14h old → age cap; late chase off the level → entry-drift cap). Fixed by emitting cam_rev
  as a **LIMIT** at `ref_entry` (see "Market vs LIMIT entries" above) — needs a cBot rebuild.
