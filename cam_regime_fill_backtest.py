"""cam_rev limit FILL-RATE by regime: do strong-trend (R+) fades fill LESS than range (R-) under the
live close-limit? Hypothesis: in a trend price runs away from the entry, so the resting limit misses
more R+ signals (the immediate-runners the owner saw alert-but-not-fill).

Replicates live: regime from build_regime(h1) + CHOP_LO; close-limit fill = price returns to ref_entry
within the 2h freshness window (8 m15 bars). Reports fill% + WR/exp(on filled) by regime and side.
Commits nothing.

  python cam_regime_fill_backtest.py --hist deep-ohlc.json
"""
import argparse, bisect, json
from collections import defaultdict
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from trend_regime import build_regime, _at, CHOP_LO

CLASSES = {'major', 'minor', 'index', 'comm'}
FILL_BARS = 8          # 2h — the live cam_rev freshness window
HOLD = H.CAM_HOLD


def sim_close_limit(bars, ts, s):
    """Live close-limit: fill if price returns to ref_entry within FILL_BARS; then score the bracket.
    Returns (filled_bool, status, R|None)."""
    ref, stop, tgt, d = s['entry'], s['stop'], s['target'], s['dir']
    R = abs(ref - stop)
    if R <= 0:
        return (False, 'nofill', None)
    i0 = bisect.bisect_left(ts, s['entry_ts'])
    fill = None
    for j in range(i0, min(i0 + FILL_BARS, len(bars))):
        b = bars[j]
        if d == 'bull' and b['l'] <= ref:
            fill = j; break
        if d == 'bear' and b['h'] >= ref:
            fill = j; break
    if fill is None:
        return (False, 'nofill', None)
    end = min(fill + HOLD, len(bars))
    for j in range(fill, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:   return (True, 'resolved', -1.0)
            if b['h'] >= tgt:    return (True, 'resolved', (tgt - ref) / R)
        else:
            if b['h'] >= stop:   return (True, 'resolved', -1.0)
            if b['l'] <= tgt:    return (True, 'resolved', (ref - tgt) / R)
    return (True, 'expired', None)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    # stats[regime] = [n_sig, n_fill, resolved_R list]
    stats = defaultdict(lambda: [0, 0, []])
    side = defaultdict(lambda: [0, 0])   # (regime,dir) -> [n_sig, n_fill]
    for pk, layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or not isinstance(layers, dict):
            continue
        m15 = _bars_norm(layers.get('m15') or []); daily = _bars_norm(layers.get('daily') or [])
        h1 = _bars_norm(layers.get('h1') or [])
        if len(m15) < 500 or len(daily) < 30 or len(h1) < 60:
            continue
        ts = [b['_ts'] for b in m15]
        h1reg = build_regime(h1)
        for s in H.detect_cam_rev(pk, m15, daily):
            chop = _at(h1reg, s['entry_ts'])[1]
            if chop is None:
                continue
            reg = 'strong(R+)' if chop < CHOP_LO else 'range(R-)'
            filled, st, o = sim_close_limit(m15, ts, s)
            stats[reg][0] += 1
            side[(reg, s['dir'])][0] += 1
            if filled:
                stats[reg][1] += 1; side[(reg, s['dir'])][1] += 1
            if st == 'resolved':
                stats[reg][2].append(o)
    print(f"=== cam_rev · close-limit FILL RATE by regime · {args.hist} ===")
    print(f"(fill = price returns to ref_entry within {FILL_BARS} bars / 2h; the live window)\n")
    for reg in ('strong(R+)', 'range(R-)'):
        n, f, rs = stats[reg]
        if not n:
            print(f"  {reg:12} (n<1)"); continue
        wr = 100.0*sum(1 for r in rs if r > 0)/len(rs) if rs else 0
        exp = sum(rs)/len(rs) if rs else 0
        print(f"  {reg:12} signals={n:<6} FILL={100.0*f/n:4.0f}%  (resolved={len(rs)})  WR={wr:3.0f}%  exp={exp:+.3f}R")
    print("\n  by side (fill rate):")
    for (reg, d), (n, f) in sorted(side.items()):
        print(f"    {reg:12} {d:4}  n={n:<6} fill={100.0*f/max(n,1):4.0f}%")
    print("\nReading it: if strong(R+) fill% is materially below range(R-), the limit structurally misses")
    print("the high-conviction trend-fades (they run away) — which is why R+ alerts fire but don't trade.")


if __name__ == '__main__':
    main()
