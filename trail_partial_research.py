"""Protect-against-loss (break-even) vs lock-in-profit (trailing / partial-close).

Same RR2 breakout population as be_stop_research.py (h1, all pairs, per class),
comparing five exit-management schemes so we choose with data, not feel:

  baseline    target-or-stop, RR2, no management.
  BE@60       move stop to entry once price travels 60% to target (scratch = 0R).
  trail 1.0R  once +1R in profit, trail the stop 1.0R behind the best price (locks
              in a rising floor; exit at the trailed stop).
  trail 1.5R  same, 1.5R trail (looser — gives the trade more room).
  partial     bank HALF at +1R, move the runner to break-even, let it ride to +2R.
              Outcomes: -1R (stopped early) / +0.5R (runner scratches) / +1.5R (runner wins).

The point of trailing/partial is that a trade that goes deep in profit then reverses
still BANKS something, instead of giving it all back. Conservative intrabar model
(adverse checked first; arming bar doesn't also resolve). Market fills + dealing cost.

Run: python trail_partial_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from detect_triggers import PAIR_CLASS
from be_stop_research import signals, LOOK, HOLD, RR


def _levels(entry, stop, d):
    R = abs(entry - stop)
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    return R, tgt


def walk_baseline(bars, i0, entry, stop, d):
    R, tgt = _levels(entry, stop, d)
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return RR
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return RR
    return None


def walk_be(bars, i0, entry, stop, d, frac):
    R, tgt = _levels(entry, stop, d)
    trig = entry + frac * (tgt - entry); armed = False
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        es = entry if armed else stop
        if d == 'bull':
            if b['l'] <= es: return 0.0 if armed else -1.0
            if b['h'] >= tgt: return RR
            if not armed and b['h'] >= trig: armed = True
        else:
            if b['h'] >= es: return 0.0 if armed else -1.0
            if b['l'] <= tgt: return RR
            if not armed and b['l'] <= trig: armed = True
    return None


def walk_trail(bars, i0, entry, stop, d, trail_r, start_r=1.0):
    """Trail `trail_r`*R behind the best price, once the trade is +start_r in profit."""
    R, tgt = _levels(entry, stop, d)
    es = stop; armed = False; peak = entry
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es: return (es - entry) / R
            if b['h'] >= tgt: return RR
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + start_r * R: armed = True
            if armed: es = max(es, peak - trail_r * R)
        else:
            if b['h'] >= es: return (entry - es) / R
            if b['l'] <= tgt: return RR
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - start_r * R: armed = True
            if armed: es = min(es, peak + trail_r * R)
    return None


def walk_partial(bars, i0, entry, stop, d):
    """Bank half at +1R, move runner to break-even, ride to +2R."""
    R, tgt = _levels(entry, stop, d)
    half = entry + 1.0 * R if d == 'bull' else entry - 1.0 * R
    banked = 0.0; runner = False; es = stop
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es: return banked + (0.5 * ((es - entry) / R) if runner else -1.0)
            if runner and b['h'] >= tgt: return banked + 0.5 * RR
            if not runner and b['h'] >= half:
                banked = 0.5 * 1.0; runner = True; es = entry
        else:
            if b['h'] >= es: return banked + (0.5 * ((entry - es) / R) if runner else -1.0)
            if runner and b['l'] <= tgt: return banked + 0.5 * RR
            if not runner and b['l'] <= half:
                banked = 0.5 * 1.0; runner = True; es = entry
    return None


SCHEMES = {
    'baseline': lambda b, i, e, s, d: walk_baseline(b, i, e, s, d),
    'BE@60': lambda b, i, e, s, d: walk_be(b, i, e, s, d, 0.60),
    'trail 1.0R': lambda b, i, e, s, d: walk_trail(b, i, e, s, d, 1.0),
    'trail 1.5R': lambda b, i, e, s, d: walk_trail(b, i, e, s, d, 1.5),
    'partial+BE': lambda b, i, e, s, d: walk_partial(b, i, e, s, d),
}


def main():
    dpairs = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          'historical-ohlc.json')))['pairs']
    store = {name: defaultdict(list) for name in SCHEMES}
    for pk in [x for x in PAIR_CLASS if x in dpairs]:
        cls = PAIR_CLASS.get(pk)
        bars = _bars_norm(dpairs[pk].get('h1', []))
        if len(bars) < LOOK + 300:
            continue
        for (ei, entry, stop, dr) in signals(bars):
            if ei >= len(bars):
                continue
            R = abs(entry - stop)
            for name, fn in SCHEMES.items():
                o = fn(bars, ei, entry, stop, dr)
                if o is not None:
                    store[name][cls].append((bars[ei]['_ts'], o - cost(1 if o > 0 else -1, entry, R)))

    def line(name, rows):
        rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq)
        print(f"      {name:<12} n={n:>4} win/scratch/loss WR={wr:>5.1f}% exp={e:>+6.3f}R")

    print("=" * 92)
    print("Exit management: break-even vs trailing vs partial-close — RR2 breakout, h1, cost, per class")
    print("=" * 92)
    for cls in ['index', 'comm', 'crypto', 'major', 'minor']:
        if not store['baseline'][cls]:
            continue
        print(f"\n===== {cls} =====")
        for name in SCHEMES:
            line(name, store[name][cls])
    print("\n===== ALL =====")
    for name in SCHEMES:
        line(name, [r for c in store[name] for r in store[name][c]])


if __name__ == '__main__':
    main()
