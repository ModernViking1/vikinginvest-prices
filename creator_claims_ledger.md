# Creator "Claimed vs Measured" Ledger (internal)

A one-glance record of social-media / course trading strategies we've stress-tested:
what the creator **claimed**, what we **measured** under realistic, cost-applied,
out-of-sample testing, and where each one is **monitored forward** if it earned a
scoped slot.

Method for every row: full 39-pair universe, realistic fills (market/stop, not
favourable limits), fixed dealing cost, chronological OOS split (both halves must be
positive), per asset class. We separate durable edges from backtest illusions — most
headline numbers are gross, idealised-fill, or curated-subset figures that do not
reproduce.

**How the wired ones are monitored:** any strategy with a "Wired as" tag runs in the
shadow forward-test harness (`unified_shadow_harness.py` → `swing-shadow-log.json`)
and its live demo-vs-model split shows on the dashboard swing panel. Check those rows
for the ongoing forward numbers vs the claim.

---

## Paul Bratby / xBratAI

- **Claimed (weekly report):** 63.44% win rate · "+5,304.68% total profit" · simulated
  $1,000 → $6,305 · profit factor implied "exceptional". Method: multi-layer
  confluence + RSI, "Trade the Fifth" Elliott-wave pullbacks.
- **Measured (his own 372-trade export + our replication):**
  - Raw (unleveraged) edge is **real but tiny**: +0.057% price move captured per trade
    (t≈3.37, statistically non-zero), win/loss ratio ~1.24.
  - The **"+5,304%" is a nonsense metric** — a sum of leveraged per-trade percentages
    (leverage ranged 43×–2,469×, median ~466×); it is not an achievable return.
  - **Cost-fragile:** ~25% of his winners cleared less than 0.02% (inside a typical
    spread). After realistic costs the +0.057% gross drops toward **breakeven**.
  - Our own rigorous test of his confluence / Trade-the-Fifth method was our **weakest
    candidate** — aggregate 4H breakeven, only comm/crypto positive on both OOS halves.
- **Verdict:** legitimate *small* edge, **dishonestly marketed**. Headline returns are
  unachievable (summed leveraged % + fantasy leverage + gross fills).
- **Wired as:** `w5_pullback` (Elliott wave-5 pullback, comm/crypto 4H only) — observe.

## Denislav Dantev — Fibonacci method

- **Claimed (internal dashboard):** profit factor **2.39** · +0.70R/trade all-time
  (541 trades) · 52.5% win rate · 2.21× win/loss · avg win +2.14R, avg loss −0.97R ·
  ~6.5-day swing hold. Method: retrace into the 50–61.8% Fibonacci "golden zone".
- **Measured (39 pairs, all TFs, realistic fills, OOS):**
  - Profit factor across the universe was mostly **0.6–0.9 (a losing system)**, negative
    expectancy on majors, minors, 4H and H1.
  - At a genuine 2:1 target on realistic fills, win rate came in **~30%, not 52.5%** —
    the golden-zone limit entry (favourable fill) is what inflates the claimed WR.
  - **Exactly one** walk-forward-robust cell survived: **commodities · H1 · 2:1**,
    **PF ~1.1, +0.06R**, both OOS halves positive. (Their claimed "best on minor FX"
    cell *failed* our OOS test.)
- **Verdict:** the 2.39 PF / +0.70R headline **does not reproduce** — same pattern as
  Bratby (idealised-fill + curated subset). The real edge is a thin commodities-only
  sliver, ~1.1 PF, not 2.39.
- **Wired as:** `fib_gz` (Dantev golden-zone reversal, **commodities-only**, H1, 2:1) —
  observe.

## "Millionaire trader" — ICT FVG-inversion + CISD

- **Claimed:** "the ONLY strategy you need to win in 2026." A stacked-confluence ICT
  model: draw-on-liquidity (1H FVG) → liquidity sweep → CISD (change in state of
  delivery) → FVG inversion (iFVG, price *closes through* a fair-value gap) → lower-TF
  entry, targeting the opposite liquidity. No hard performance numbers given — sold on
  the framework and hand-picked chart examples.
- **Measured (39 pairs, ALL timeframes 15m/h1/4h/daily, realistic close-through fill, OOS):**
  - **Negative in every single cell** — every timeframe, every target (opposite-liquidity
    *and* fixed 1.5/2/3:1), every asset class: **−0.13R to −0.62R**.
  - Not a fill illusion — the entry is a realistic momentum close-through (no favourable
    limit) and it still loses. The iFVG/CISD trigger fires ~9,000×/yr on 15m/h1 and does
    not predict continuation.
  - Same sweep/FVG-reversal family as our own tests (Flow Model, Liquidity Sniper,
    NY-fakeout-FVG). The **only** ingredient that ever gave this family an edge was a
    **NY-open session filter on FX** — already captured by `flowmodel`. This all-hours,
    all-pair formulation adds nothing.
- **Verdict:** no mechanical edge anywhere. Coherent framework, cherry-picked examples,
  zero measured edge — the strongest "claimed vs measured" gap of the three.
- **Wired as:** nothing (rejected).

## TJR — ICT sweep/OB/FVG model (sweep → BOS → retrace-to-zone)

- **Claimed:** TJR's mechanical ICT ruleset (no hard performance numbers published):
  trend context + liquidity sweep + reclaim → break of
  structure → retrace into an FVG/OB/Breaker zone → enter, SL beyond the sweep, TP =
  next unswept liquidity. Sold as deterministic — "a trade is valid only when all
  conditions fire *in sequence*."
- **Measured (39 pairs, all TFs 15m/h1/4h/daily, dual-fill, OOS):**
  - Negative on 15m / h1 / daily, and negative on its OWN stated target (next-unswept-
    liquidity) at *every* timeframe — the marketed exit is the part that fails.
  - The only positive is 4H with a **fixed 2R** (not the stated target): +0.04R (market)
    / +0.06R (limit), both OOS halves +, and the realistic fill survives (not an illusion).
  - Per class that thin 4H edge — crypto +0.10R, minor +0.08R, index +0.06R — is a
    **weaker duplicate** of edges already wired: `engulf_manip` (crypto-4H +0.14R) and
    `sweeprev` (minor-4H +0.10R). Same sweep-reversal pockets, captured better elsewhere.
- **Verdict:** no *new* deployable edge — a redundant, thinner slice of the ICT
  sweep-reversal family; stated target doesn't work. This is the **8th** strategy from
  this family we've tested (Flow Model, Liquidity Sniper, NY-fakeout-FVG, iFVG/CISD,
  sweeprev, engulf_manip, liquidity-sweep-retest, this) — all converge to the same
  place: a ≤0.1–0.2R 4H reversal whisper in crypto/minor/index/gold-session FX, never
  with the marketed liquidity target, always better captured by a narrowly-scoped variant.
- **Wired as:** nothing (redundant with `engulf_manip` / `sweeprev`).

## TJR — OB+FVG day-range strategy

- **Claimed:** TJR's other signature setup: mark Day High / Day Low; a strong impulse
  leaves a stacked Order Block + FVG (supply/demand); price retraces into the zone →
  enter, SL beyond the OB, target the opposite daily extreme. Sold via clean annotated
  chart examples (no hard numbers).
- **Measured (39 pairs, 15m/h1/4h, dual-fill, OOS):**
  - **Textbook fill illusion.** On a FAVOURABLE LIMIT fill at the zone edge it looks
    strong — h1 day-target +0.136R, 4H day-target **+0.222R**, both PASS.
  - On a REALISTIC market fill it flips **negative** — h1/4H day-target ≈ −0.07R,
    every market + fixed-RR cell negative. The ~0.29R limit-vs-market gap is pure
    adverse selection (same as supply/demand, engulf-imbalance).
  - Only thin crypto/index 4H cells survive a realistic fill, redundant with existing
    candidates.
- **Verdict:** no real edge — the retrace-into-zone *limit* entry flatters the
  backtest; honest fills lose. The high win rate (46–54% at the day-target) is a
  small-target-at-a-favourable-price artifact.
- **Wired as:** nothing (rejected).

## Tom Hougaard — 'situational analysis' Friday → Monday revisit

- **Claimed:** if Friday fails to trade above Thursday's high, the following Monday
  revisits Friday's low with "overwhelmingly high" probability — ~21 of 24 instances,
  or "roughly 95%", from manual analysis of years of Dow Jones data. Framed as a
  situational directional bias, target = Friday's low.
- **Measured (39 pairs, daily setup + h1-resolved trade, weekend-gap session detection,
  ~1yr):**
  - The **95% does NOT reproduce** — the actual Friday-low revisit rate under the setup
    is **~50–54%** (indices highest), not 95%.
  - There **is a real situational LIFT**, though: the setup adds **+10.5pp overall**
    (indices +18pp, commodities +12pp, majors +11pp) to the revisit probability vs the
    base rate when Friday *did* exceed Thursday's high — so the directional observation
    is genuine, just far weaker than marketed.
  - **Not tradeable as-is:** short Monday open → target Friday's low, stop Thursday's
    (or Friday's) high nets **~0 to slightly negative** expectancy everywhere
    (all-pairs −0.02 to −0.08R). A ~65% target-hit rate monetises to nothing because
    the target is near and the stop far — a low-RR mirage. Only DJ30 (the Dow Hougaard
    cites) is positive (+0.26R) but on n=17 trades, and neighbouring indices contradict
    (multiple-testing noise).
- **Verdict:** large claimed-vs-measured gap on the headline number; the underlying
  bias is real but modest and not profitably tradeable with the natural target/stop
  geometry. Useful only as a weak index directional-context flag, not a signal.
- **Wired as:** nothing (candidate read-only index bias tag only; not a trade).
- **Also tested (Hougaard 'add to a winner'):** opening-range-breakout probe + one
  pyramid add at +1R with structure confirmation (breakeven the first unit, trail
  both), on DAX/DJI/FTSE m15. No hard marketed number, so no separate row — but the
  result is clean: **adding does NOT beat the plain probe** (pooled +0.082R -> +0.057R,
  a −0.025R delta; the add hurt in 6 of 8 index/open-hour variants). A deeper 21-config
  sweep (3 indices × 2–3 opens × 15/30/60-min opening ranges) measuring the per-trade
  add-vs-probe DELTA found it positive in both OOS halves in **0 of 21** configs — the
  couple of positive-delta cells (FTSE 07:00, DJI 14:30) are driven entirely by their
  second OOS half (first half negative), i.e. noise. The base ORB probe is itself
  fragile (fails OOS, highly sensitive to the session-open hour). Rejected.
  See `hougaard_pyramid_research.py`.

## '90% win rate' VWAP + STDV band sweep (social-media, gold desk)

- **Claimed:** ~90% win rate. Anchored VWAP with 2σ/3σ standard-deviation bands; fade a
  liquidity sweep of the outer band back to the mean (VWAP). RR shown 3.0–5.7.
- **Measured (39 pairs, m15/h1/4h, 2σ & 3σ, realistic fills, per-trade RR=mean, OOS;
  VWAP approximated equal-weighted — our OHLC carries no volume):**
  - The **90% does NOT reproduce** — actual win rate is **27–40%** everywhere (median RR
    ~2). Nearly every cell is negative in both OOS halves on huge samples (pooled h1 ±2σ
    −0.069R, n=19,135).
  - The single apparent PASS (4h ±3σ indices, N=30, +0.162R) is fragile: it passes only
    at that one window (N=20/40/50 all fail), only DJ30 is per-index robust, DE40 and
    JP225 are negative, and the pool is carried by a fat-tailed NAS100 — curve-fit +
    multiple-testing noise, not an edge.
- **Verdict:** large claimed-vs-measured gap; fading σ-band extremes back to the mean has
  no robust edge — the losing mirror of continuation entries (same lesson as the 7AM
  '50% isolated leg' fade). Rejected. See `vwap_stdv_research.py`.
- **Wired as:** nothing (rejected).

---

## Alex Morris / Trading Cafe — supply/demand + order blocks

- **Claimed (course marketing + student results):** 60.72% avg win rate · 154.16% avg
  return · 8,400 student trades; one student's PDF log = 300 trades, 76.7% WR, +1.90R avg,
  +568.7R total (not fat-tailed). Method: fresh HTF supply/demand zone / 4H "bullish order
  block", top-down weekly/daily bias, retest entry, stop beyond the zone, 2:1+ (runners).
- **Measured (systematic replication, our full universe):**
  - The student log is a **manual BarReplay backtest** on FX minors/majors — hand-placed
    limit entries at zone edges + discretionary bias/quality filtering (the fill illusion
    plus survivorship).
  - **Tight-base S&D zone, market fills** (`alex_morris_sd_research.py`): 4h FX negative
    (−0.05R base, and the top-down-bias filter makes it *worse*, −0.22R); the lone daily
    n≥40 pass (+0.35R) is 1–5 trades/pair pooled across 18 pairs — small-sample noise.
  - **His exact 4H order-block method, market fills** (`alex_morris_ob_research.py`): FX
    negative at every target (4h RR2 −0.219R; h1 RR2 −0.214R with the recent OOS half
    strongly negative), and the RR5 "let it run" target he showcases makes FX *worse*.
    Order blocks only work on crypto (already live as `obfvg`) — not the FX he trades.
  - The posted "winning setups" are a **selected-winner highlight reel** (a #trading-results
    channel of winners) — no losers, no denominator, so they cannot move a base rate.
- **Verdict:** skilled discretionary trader; the *judgement* about which zones to take does
  the work and does not survive mechanisation on FX with realistic fills. Rejected for the
  live feed. Because Trading Cafe actively markets it, kept under forward observation.
- **Wired as:** shadow observer `obfvg_fx4` (4H order block on FX minor+major, logged NOT
  fed to the cBot) — accumulates forward, out-of-sample evidence on his exact method to
  revisit on live data rather than backtest.

---

*Extensible: add a row per creator/strategy as tested. The broader batch (supply &
demand, Bollinger trend-continuation, Liquidity Sniper, John Wick box, engulf-imbalance,
Sid RSI+MACD, W/M reversal, Asian-session gold, NY-open flow model, …) lives in the
`*_research.py` scripts; only the ones with hard marketed numbers get a claimed-vs-measured
row here.*
