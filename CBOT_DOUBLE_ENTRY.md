# cBot double-entry (idempotency) bug

**Status:** open — cBot-side fix needed. Dashboard mitigated (see below).
**Found:** 2026-08-11, while rechecking the swing stop-loss count.

## Symptom

The cBot occasionally opens **two positions for a single signal**. Both are real
positions (real demo P&L) that usually hit the same stop/exit within a few
seconds of each other, so they inflate closed-trade counts (especially stop-loss
counts and win-rate denominators) and — more importantly — **double the intended
risk** on the affected setup (2 × `r_pct` instead of 1).

The two positions are **not** a double-logged close (that's a separate, already-
handled issue — `OnPositionClosed` firing twice writes an identical row, deduped
by `position_id`). These are **genuinely two positions**: different
`position_id`, different `volume_units` (each sized off slightly different account
equity at placement time), sometimes one carries the `signal_id` and the other has
`signal_id: null`.

## Evidence (swing-executions.json, as of 2026-08-11)

69 closed positions = **64 distinct setups + 5 duplicate positions**. All 5
duplicates are losses:

| pair | exit price | positions (strategy, position_id, volume, R) |
|------|-----------|----------------------------------------------|
| usdzar | 16.41428 | s5_rsi (653306823, 1169000, −1.039) + s5_rsi_wide (653306825, 1168000, −1.039) |
| ethusd | 1851.57 | wm (656675798, 10, −1.074) + wm (656675765, 10, −1.075) |
| xauusd | 4139.11 | ? (656071916, 146, −0.945) + ? (655934965, 159, −0.938) |
| ethusd | 1894.83 | ? (658346752, 10, −1.008) + ? (658320630, 10, −1.002) |
| xauusd | 4350.9 | asianglitch (659540573, 250, −1.013) + asianglitch (659540451, 248, −1.014) |

Two distinct sub-causes are visible:

1. **Cross-tag double** (`usdzar`): `s5_rsi` and `s5_rsi_wide` both fired on the
   same pair. `swing_signals.py` de-duplicates one live position per pair
   (`by_pair` → highest-`PRIORITY` primary), so only **one** row should reach the
   feed. Two positions filling means either the feed carried both (dedup gap) or
   the cBot placed both. Worth confirming the emitted `swing-signals.json` for
   that cycle only had one `usdzar` row.
2. **Same-signal re-placement** (`xauusd`/`ethusd`/`asianglitch`): the same
   strategy on the same pair, one position tagged with `signal_id` and one with
   `signal_id: null`. This looks like the cBot **re-placing a signal it already
   acted on** on a later poll (e.g. after a restart, or when the signal_id wasn't
   recorded against the open position), i.e. an **idempotency** gap.

## Likely fix (cBot side)

The cBot should treat each `signal.id` as **place-once**:

- Maintain a persisted set of `signal_id`s it has already placed (survives
  restarts). On each poll, skip any signal whose id is already in that set **or**
  for which it already holds an open position on that pair from this strategy.
- Guard against the "lost signal_id" case: when opening, stamp the position's
  label/comment with the `signal_id` so a restart can reconcile open positions to
  signals and not re-enter.
- Optionally enforce the one-position-per-(pair,strategy) rule broker-side as a
  belt-and-braces cap (the emitter already enforces one live row per pair).

This mirrors the guard we added on the signal-emitter side for the intraday
strategies (`absorb_live` / `mmove_live` persist a state file and never re-emit a
resolved/duplicate id) — the swing cBot needs the equivalent on the placement
side.

## Dashboard mitigation (already shipped)

`Viking_Invest_Trading_v69.html` now collapses double-entries for **display**:
closed trades are grouped by `(pair, exit_price)` and any that closed within 5s of
a kept one are treated as the same setup. Applied to both the swing and intraday
panels, so tiles / win-rate / realised-R count **distinct trade decisions**, not
raw positions. This is cosmetic only — it does **not** change the real account
exposure, which is why the cBot fix above is still needed (the doubled risk is
real).
