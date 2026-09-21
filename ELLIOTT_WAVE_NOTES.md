# Elliott Wave — what we track, and what we don't

## What our "EW observer" actually is

- **`detect_w5pb` / `w5_pullback`** (in `unified_shadow_harness.py`, observed via the swing
  shadow harness): mechanizes ONE narrow Elliott setup — a completed 0-1-2-3-4 pivot impulse,
  enter the **5th wave** (Bratby "Trade the Fifth"), stop below the wave-4 low, RR2, on 4H,
  scoped to comm/crypto. Flagged in-code as the **weakest** candidate (4H break-even, failed
  walk-forward universe-wide).
- **`elliott_research.py`**: a standalone backtest of mechanical 3rd- and 5th-wave entries off
  pivot impulses.

That is the whole of our automated Elliott capability. It is a fractal/pivot approximation of a
single wave-entry, not an Elliott *analyst*.

## What it does NOT do (and why external analyst charts aren't captured)

Full discretionary Elliott Wave analysis — the kind in the WICKTATORFX charts (USDJPY, GBPUSD,
NZDUSD) — is **not** in our system and cannot be produced by our detectors:

- Corrective **W-X-Y-Z** structures, **triangles (A-B-C-D-E)**, flats, zigzags, ending diagonals.
- **Multi-degree / nested** counts (a wave (4) of a (1/A) of a larger degree).
- Top-down context, fib-confluence **sell/buy zones**, **invalidation levels**, multi-target
  take-profits drawn by hand.

Wave labeling is inherently subjective, so these counts are **not mechanizable** from bars alone.
Our detector would never "see" these setups the way the analyst draws them.

## Decision: track external EW calls forward (observe-only)

Since we can't reproduce the analysis, we **log the external calls and track their outcomes** —
the same observe-only pattern as the quad bull-flag observer. No capital, no orders.

- **`elliott-watchlist.json`** — the logged external EW calls (pair, direction, entry/zone,
  invalidation, take-profit targets, wave label, source, date). Add new analyst calls here.
- **`quad_bullflag_observer.py`** sibling: **`elliott_observer.py`** reads the watchlist, fetches
  recent price, and marks each call `watching` → `active` (price reached the zone) →
  `target_hit` / `invalidated`, recording the realised R against the call's own entry/stop/target.
- **`.github/workflows/elliott-observer.yml`** runs it on a schedule and commits the updated
  watchlist.

### Important caveat on the seeded calls

The three seeded calls were **read off screenshots of someone else's charts** — the levels are a
best-effort transcription and should be **verified/corrected** by whoever follows the analyst. The
tracker only scores what it's given, so a wrong level scores wrong. This is a record of an external
discretionary source, not a signal we generate or endorse.
