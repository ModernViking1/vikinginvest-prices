"""What trailing config works at 1:1 (intraday) and for FX majors / crypto (2:1)?

The swing 1R trail (arm +1R, trail 1R) helps indices/comm/minors but not crypto/
majors, and it's meaningless at 1:1 (arm +1R == the target). This sweeps
(arm_r, trail_r) trailing configs across both reward:risk levels and every class,
so we pick intraday (1:1) params by data and see whether ANY trail rescues majors
/ crypto at 2:1.

Same RR-flexible breakout population as the earlier studies, h1, market fills+cost.
arm_r = profit (in R) at which the trail arms; trail_r = distance (in R) behind the
best price. First locked level = arm_r - trail_r (so equal => break-even).

Run: python intraday_trail_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from detect_triggers import PAIR_CLASS
from be_stop_research import signals, LOOK, HOLD


def walk_baseline(bars, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def walk_trail(bars, i0, entry, stop, d, rr, arm_r, trail_r):
    R = abs(entry - stop)
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    es = stop; armed = False; peak = entry
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es: return (es - entry) / R
            if b['h'] >= tgt: return rr
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + arm_r * R: armed = True
            if armed: es = max(es, peak - trail_r * R)
        else:
            if b['h'] >= es: return (entry - es) / R
            if b['l'] <= tgt: return rr
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - arm_r * R: armed = True
            if armed: es = min(es, peak + trail_r * R)
    return None


# per RR: baseline + a grid of (label, arm_r, trail_r)
CONFIGS = {
    1.0: [('baseline', None, None),
          ('arm.50/tr.50', 0.50, 0.50),   # BE at 50% to target
          ('arm.50/tr.25', 0.50, 0.25),   # lock +0.25R at 50%
          ('arm.75/tr.25', 0.75, 0.25),   # lock +0.50R at 75%
          ('arm.75/tr.50', 0.75, 0.50),   # lock +0.25R at 75%
          ('arm.30/tr.30', 0.30, 0.30)],  # early BE
    2.0: [('baseline', None, None),
          ('arm1.0/tr1.0', 1.00, 1.00),   # the shipped swing config
          ('arm.50/tr.50', 0.50, 0.50),   # tighter: BE at 25% to target
          ('arm1.0/tr.50', 1.00, 0.50),   # lock +0.5R at 50%
          ('arm1.5/tr.50', 1.50, 0.50)],  # lock +1R at 75%
}
FOCUS = ['crypto', 'major', 'index', 'comm', 'minor']


def main():
    d = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    'historical-ohlc.json')))['pairs']
    for rr in (1.0, 2.0):
        store = {lbl: defaultdict(list) for (lbl, _, _) in CONFIGS[rr]}
        for pk in [x for x in PAIR_CLASS if x in d]:
            cls = PAIR_CLASS.get(pk)
            bars = _bars_norm(d[pk].get('h1', []))
            if len(bars) < LOOK + 300:
                continue
            for (ei, entry, stop, dr) in signals(bars):
                if ei >= len(bars):
                    continue
                R = abs(entry - stop)
                for (lbl, arm, tr) in CONFIGS[rr]:
                    o = walk_baseline(bars, ei, entry, stop, dr, rr) if arm is None \
                        else walk_trail(bars, ei, entry, stop, dr, rr, arm, tr)
                    if o is not None:
                        store[lbl][cls].append((bars[ei]['_ts'], o - cost(1 if o > 0 else -1, entry, R)))
        print("=" * 96)
        print(f"Trailing sweep @ RR {rr:.1f}  (arm_r / trail_r; first locked = arm-trail) — h1, cost")
        print("=" * 96)
        for cls in FOCUS:
            if not store['baseline'][cls]:
                continue
            print(f"\n  ===== {cls} =====")
            for (lbl, _, _) in CONFIGS[rr]:
                rows = sorted(store[lbl][cls]); n, wr, e = agg([r for _, r in rows])
                print(f"      {lbl:<14} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R")
        print()


if __name__ == '__main__':
    main()
