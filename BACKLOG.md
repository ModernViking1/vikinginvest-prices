# Viking Invest — Backlog & Parked Items

Working tracker for things deliberately deferred + the plan while the live
intraday win-rate matures. Not investor-facing. Keep it short.

## ❌ REJECTED 2026-07-01 — index add-on strategies (research)

Investigated add-ons to diversify the all-momentum index engine (US indices
softened to ~67-69% WR). THREE candidates tested, all rejected:
- **Overnight drift** — strongest academic anomaly, but does NOT transfer:
  our indices are 24h CFDs, so gains accrue INTRADAY (open→close +9-52% ann)
  not overnight (close→open flat/neg). No closed cash session to exploit.
- **RSI(2) mean-reversion** — looked promising raw (5/6 positive) but that was
  an artifact of unbounded downside. With any risk model it's net-negative:
  H1 aggregate EV −0.098R, PF 0.72; consistently negative across both
  time-halves and every parameter combo; stop-width sweep shows the edge
  only appears with NO stop (PF 1.09, and the −650bps tails from pass 1).
  Won't survive live execution costs. `research_rsi2_battery.py`.
- **TSMOM long-bias filter** — FAILS the adversarial check. Every variant
  (21/63/126d align, pure long-only) drops a ≥51%-WR cohort, cutting net-R
  Δ−3 to −10. Hypothesis was wrong: index SHORTS are profitable (55.4% WR,
  +8R) and counter-trend signals are the BEST cohort (59.6%). The engine's
  EW/H5/confluence already handles trend-alignment; TSMOM is redundant and
  only removes good trades. `backtest_tsmom_index.py`.
- VWAP: infeasible (no volume in the feed).

- **Opening Range Breakout** — rejected. Net-negative across all exit
  variants (1:1 EV −0.061R/PF 0.88; 2R PF 0.89; hold-to-close PF 0.96). No
  robustness: the odd index that flickers positive (Nikkei/Dow) is
  small-sample and doesn't hold across variants; NAS100 a consistent loser
  (PF 0.55). Wouldn't survive live spread. Matches the mixed literature (big
  ORB results rely on leverage + 10R/close exits; MNQ falsification found
  nothing). `research_orb_index.py`.

**CONCLUSION — index add-on research CLOSED (4/4 rejected).** The index
engine is already well-constructed; the sub-70% cards are metric
normalisation, NOT decay — a recent-window WR diagnosis showed the engine
stable-to-improving (2nd half 58.4% > 1st 53.5%), no regime decline. One
index to keep a passive eye on: **NAS100** (only index softening in the
recent split, 65%→42%; also lowest card at 66.8%). No action; each index
still net +150-200R. Revisit add-ons only if a real, sustained decay appears.

## ✅ SHIPPED 2026-07-01 — H4 time-of-day filter (FX)

Deployed: FX macd-primary entries now skip 07:00–09:59 UTC (London open)
and 21:00–21:59 UTC (daily rollover). Head-to-head backtest
(`backtest_h4_timeofday.py`): those hours were a 42.6%-WR cohort; skipping
them lifted net-R +47→+63 (+34%) and WR 53.9%→56.5% without culling
winners. Asian-hours and concentrate-to-overlap variants FAILED the
adversarial check (dropped >50% cohorts) and were rejected. Mechanism =
open-whipsaw + rollover spread clipping the tight 1:1 stop; matches
session-liquidity research and the live log. Server flag `H4_TOD_FILTER` /
`H4_SKIP_HOURS_UTC`, mirrored in the JS replay, RULES_VERSION 2026-07-01b.
Watch: confirm live loss-rate drops in those windows over the coming week.

## ⏸ PARKED 2026-07-01 — USD-directional weekend-thesis test

Follow-up from the loss-timing analysis (`analyze_loss_timing.py`). The
strategy's WR is strongest on Fridays historically (68.8%, +36R), which is
*loosely* consistent with the "USD strengthens into the weekend" idea — but
our WR is a both-directions, multi-pair number, not a USD-directional
measure, so it can't confirm the thesis. To actually settle it: isolate
USD-pair trades, split by long-USD vs short-USD, and test whether long-USD
outperforms short-USD as the week progresses toward Friday. Industry view
is contested (Levi 1978 said USD *falls* Friday; liquidity hypothesis says
it strengthens) and the day-of-week effect largely decayed post-1990s — so
low expected payoff; parked. Revisit only if a weekday overlay looks worth
pursuing after the H4 time-of-day work.

## ✅ RESOLVED 2026-07-01 — H7 replaced by the EW-disagreement veto

Decision taken (both variants backtested head-to-head; see below). **H7
reverted** (minors conf≥2 → conf≥1) and a **blended-EW-disagreement veto**
deployed on FX (major+minor): a macd-primary trade whose direction fights
a clear EW read is now blocked. Head-to-head on minors: H7 was −6R (culled
a net-winning cohort); veto-blend +19R (removes a 38.5%-WR cohort); across
all FX the veto lifts +39R→+57R. Shipped in `detect_triggers.py`
(`EW_DISAGREE_VETO`), mirrored in the HTML backtest replay, RULES_VERSION
bumped to 2026-07-01a. Harnesses: `backtest_ew_veto_vs_h7.py`,
`backtest_ew_accuracy.py`. Panel EW-track-record stat shipped.
**Watch:** live minor WR over the coming week now that the veto is live.
Original winner-check analysis retained below for the record.

## 🔬 (superseded) H7 (macdp minors gate) winner-check

Ran a confluence-bucketed replay of macd-primary over the historical
window for the 5 minors flagged in the consolidated-diagnosis panels
(CADJPY, USDSGD, EURAUD, NZDJPY, EURSGD), reusing the production detector
functions (same MACD/confluence/H5 filter + the new pip-floor).

**What H7 does:** minor-class entry filter — was confluence ≥ 1, H7
raised it to ≥ 2. Its *only* marginal effect is deleting the
**confluence-1 bucket**. So "does H7 cost winners?" = "is conf-1
winners or losers?"

**Finding (frictionless 1:1 replay — bucket H7 removes = conf-1):**

| Pair    | conf-1 (H7 kills) | conf-2 (H7 keeps) | net W removed | verdict |
|---------|-------------------|-------------------|---------------|---------|
| CADJPY  | 3W/5L (37%)       | 11W/2L (85%)      | −2            | H7 helps |
| USDSGD  | 9W/5L (64%)       | 4W/6L (40%)       | +4            | H7 hurts |
| EURAUD  | 10W/6L (63%)      | 11W/16L (41%)     | +4            | H7 hurts |
| NZDJPY  | 9W/7L (56%)       | 7W/12L (37%)      | +2            | H7 hurts |
| EURSGD  | 6W/5L (55%)       | 5W/5L (50%)       | +1            | ~neutral |
| **Agg** | **37W/28L (57%)** | 38W/41L (48%)     | **+9**        | net-neg |

So H7 removes ~9 more winners than losers across these 5 (only CADJPY
behaves as H7 assumes). The inspector showed H7 firing on USDSGD's 3
recent *losses* but was blind to the 9 conf-1 *winners* it also removes.

**Key caveat:** the replay is frictionless and comes out ~48–57% WR — it
does NOT reproduce the ~32% live minor WR. So confluence level is not the
thing separating live winners from losers; **execution is** (spread +
entry-drift on the tight 1:1 stop). H7 papers over an execution leak by
discarding signal quality. The real leak is now addressed by the
pip-floor (shipped 30 Jun) + entry-deviation gate.

**Options for the end-of-week decision:**
1. Scope H7 to CADJPY only (keep it where it's earned).
2. Revert H7 to conf ≥ 1 across minors; rely on pip-floor +
   entry-deviation gate for the execution leak, then re-measure.
   (Leaning #2 — cleaner; isolates whether the execution fixes alone
   close the live gap before we start filtering signal quality.)
3. Leave as-is, keep collecting live data.

**Revisit trigger:** end of this week, alongside the live-WR re-check
(entry-deviation gate + pip-floor now live). Re-run the bucket replay
with more live closes and confirm the direction holds before changing
the rule. Re-run harness: `python3 backtest_h7_confluence.py`.

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

## ✅ SHIPPED 2026-06-30 — max-entry-deviation gate (cBot, needs rebuild)

Built. cBot param `Max entry deviation (% of R, 0=off)`, default 0.30.
Skips a signal when the market has drifted > that fraction of R from
sig.Entry by fill time (Ask for a buy, Bid for a sell). Verified: the
SPX500 81%-of-R drift would be skipped; clean FX fills pass. Tunable
per-instance; watch the skip-rate vs execution-cost improvement and
adjust 0.30 up/down. **Operator: rebuild the cBot to activate.**
Original analysis below for reference.

## 🔧 (shipped) Original rationale — max-entry-deviation gate

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
