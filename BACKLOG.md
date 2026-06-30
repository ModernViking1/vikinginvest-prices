# Viking Invest — Backlog & Parked Items

Working tracker for things deliberately deferred + the plan while the live
intraday win-rate matures. Not investor-facing. Keep it short.

## ⏸ Parked — revisit when LIVE intraday WR has matured (n ≥ ~30/cohort)

- **Investor-facing "why live ≠ backtest" explainer.** Short, IP-shielded
  note (LIVE panel and/or Investor brief) pre-empting the obvious investor
  question. Substance: live = backtest edge *net of execution friction*
  (spread/slippage round-trip, fill latency), small-sample noise, and a
  more conservative live selection (risk gates). Framing: an honest
  live-below-backtest gap is what a sophisticated allocator expects;
  publishing both side-by-side (the "Δ vs sim" metric) is the credibility
  asset. Expectation to set: live settles ~6–12pp below backtest, not
  parity. **Do not anchor on current week's figure** — it's depressed by
  bugs since fixed (degenerate stops, macdp-minors spread bleed, label
  mis-classification) plus tiny sample.

- **Live broker fills on the Performance tab.** Add a "Live fills" view
  (Simulated / Live toggle, or a distinct badged block) so real cTrader
  trades show alongside the simulated trade log. Reads `executions.json`
  (same source as the LIVE panel). Hold until live WR is presentable.

## ⏸ Parked — revisit later (operational)

- **VPS migration for the cBot.** Currently runs on a laptop → battery
  death / sleep / Wi-Fi drop = silent gaps in `executions.json` (the cBot
  only captures `Positions.Closed` while running) + paused position
  management. Non-negotiable before going live. Cheapest path: ask IC
  Markets for a free broker VPS. Revisit when ready to harden / go live.

## 🔧 PRIORITY post-freeze fix — max-entry-deviation gate (cBot)

**The single highest-leverage fix the execution-cost report exposed.**
The cBot market-fills a triggered signal up to 30s–60min after the
signal's trigger price. On fast instruments the market has drifted far
by fill time: SPX500 entries landed ~189% of R (one fill 28 pts off a
6.7-pt stop), XRPUSD ~140%. The backtest assumes entry AT the signal
price; live enters wherever the market drifted to — this decoupling is
the dominant live-vs-sim WR gap, NOT broker spread.

Fix (cBot, needs rebuild): before `ExecuteMarketOrder`, compare the
current market to `sig.Entry`. If it has moved more than ~25–30% of R
(`|market - sig.Entry| / |sig.Entry - sig.Stop|`) in the adverse
direction, SKIP — the R structure the signal modelled no longer
exists. Cheap, principled, kills the catastrophic fills while keeping
the well-behaved ones (eurusd ~9%).

Proper long-term alternative: place a LIMIT/STOP order AT `sig.Entry`
instead of a market order — fills only at the modelled price (or
better) or not at all, which is exactly what the backtest assumes.
Eliminates drift regardless of latency.

Note on "go real-time": faster data cadence would *reduce* the drift
but NOT remove it — a market order fills at "now", never at the signal
price, so any non-zero latency leaves a gap. Matching the ORDER TYPE
to the backtest (limit at entry) closes it without rebuilding the
batch pipeline into a streaming system. Real-time is a big infra
project; the deviation gate / limit order solves this defect for ~30
lines of cBot.

## ▶ Recommended next steps WHILE awaiting more live data

1. **Change-freeze on detector logic (~1–2 weeks).** Highest priority and
   counterintuitive: stop tweaking rules. Every detector change (H5, H7,
   stop floor, cool-off all shipped 2026-06-25→30) resets the clean-data
   clock. Let the post-fix sample accumulate so we can actually *measure*
   whether the fixes worked. Monitor only.
2. **Per-pair execution-cost report (do now — data already captured).**
   The cBot records `slippage_pips` on every placement + `entry_attempt`
   vs `entry_filled`. Aggregate per pair to quantify exactly how much
   spread/slippage each pair bleeds — the mechanical driver of the
   backtest↔live gap. Lets us surgically gate the next macdp-minors-style
   offender on evidence, not guesswork.
3. **Publish-health / heartbeat indicator.** Surface "last successful
   execution-publish" + "last detector run" on the dashboard so silent
   breaks (the 422 dispatch bug, laptop-sleep gaps) are visible at a
   glance instead of discovered days later.

## ✅ Recently shipped (baking — do NOT re-touch without cause)

- 2026-06-30 — LIVE panel: TP-mislabel fix (direction-aware close reason,
  cBot + client-side re-derivation), close-source hyphen normalisation,
  Today/Week/All window toggle.
- 2026-06-29 — H7: macd-primary on minors gated to confluence ≥ 2.
  Ingest workflow reset-reapply (stop losing execution events).
  Degenerate-stop floor 3→5 bps + applied to divergence. Inspector H5
  honesty fix (4H-cloud definition matches live).
- 2026-06-25 — H5: 4H-trend filter on macd-primary (divergence exempt).
  Per-pair cool-off after 2 losses. School Run suspended (research-only).
  Catastrophe guards in cBot (MinStopPips, MaxPositionLots, post-fill SL
  verification). Password auth. Per-pair consolidated diagnosis + H5–H8.
