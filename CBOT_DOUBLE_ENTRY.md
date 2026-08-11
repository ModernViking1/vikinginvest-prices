# cBot double-entry (idempotency) bug

**Status:** FIXED in `VikingSwingBridge.cs` (2026-08-11). The intraday bridge
(`VikingInvestSignalBridge.cs`) was already protected. Dashboard also de-dups for
display. **Requires recompiling + redeploying the swing cBot in cTrader to take effect.**
**Found:** 2026-08-11, while rechecking the swing stop-loss count.

## Fix applied (swing bridge)

Root cause: `_seenIds` (the place-once set) is marked only *after* the order fills,
and `_positionIdToSignalId` was in-memory only — so a bot restart, or a failed
seen-file append, in the window between placing and marking let the same signal
re-enter. The open broker position survived the restart but the bot no longer knew
it belonged to that signal.

- **`SignalIdOf(Position)`** — new helper: recovers a position's signal id from the
  in-memory map, falling back to the position `Comment` (`"SwingTrade | {strategy} |
  {id}"`), so it works after a restart.
- **Anti-double-entry guard in `Consider`** (before the order): refuse to open if we
  already hold (a) a position with the **exact same signal id**, or (b) a position of
  the **same (pair, strategy, direction)** — except intentional scaled gold legs
  (`…:tN`, 1/3 risk each), which are allowed to stack. Because it reads live
  `Positions` (which survive a restart) it closes the mark-after-fill window.
- **`OnPositionClosed`** now uses `SignalIdOf(p)` so a position opened before a
  restart still logs its real `signal_id` (not `null`) on close — the source of the
  `signal_id: null` rows in the duplicates below.

The intraday bridge already does the equivalent (restores the position→signal map
from disk on restart, guards by signal-id-from-comment, and caps one position per
symbol), so no change was needed there.

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

## Dashboard mitigation (already shipped)

`Viking_Invest_Trading_v69.html` now collapses double-entries for **display**:
closed trades are grouped by `(pair, exit_price)` and any that closed within 5s of
a kept one are treated as the same setup. Applied to both the swing and intraday
panels, so tiles / win-rate / realised-R count **distinct trade decisions**, not
raw positions. This is cosmetic only — it does **not** change the real account
exposure, which is why the cBot fix above is still needed (the doubled risk is
real).
