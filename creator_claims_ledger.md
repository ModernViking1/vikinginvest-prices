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

## ICT "sequential confluence" model (sweep → BOS → retrace-to-zone)

- **Claimed:** an anonymous mechanical ICT ruleset (screenshots / scribd, no named
  creator, no hard numbers): trend context + liquidity sweep + reclaim → break of
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

---

*Extensible: add a row per creator/strategy as tested. The broader batch (supply &
demand, Bollinger trend-continuation, Liquidity Sniper, John Wick box, engulf-imbalance,
Sid RSI+MACD, W/M reversal, Asian-session gold, NY-open flow model, …) lives in the
`*_research.py` scripts; only the ones with hard marketed numbers get a claimed-vs-measured
row here.*
