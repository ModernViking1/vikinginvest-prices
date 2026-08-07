"""Does moving the stop to break-even at X% of the way to target help or hurt?

Motivated by a DE40 trade that sat ~+4.5K in profit then rode all the way back to a
full stop-out. The proposed rule: once price has travelled X% of the entry->target
distance, move the stop to break-even (entry). This protects the give-back, but can
also stop you out flat on trades that would have reached target after a pullback —
so it must be MEASURED, not assumed.

Test bed: the validated RR2 breakout (gbreak logic) across every pair on h1, so we get
a large, clean, direction-agnostic trade population. For each trade we compare:
  baseline   target-or-stop (RR2), no management
  BE@X       same, but when price reaches X% to target the stop jumps to entry (0R)
Conservative intrabar model: adverse checked first; the BE-trigger bar only ARMS BE
(doesn't also resolve that bar). Realistic market fills + dealing cost. Reported per
asset class (indices = DE40's class) and overall.

Run: python be_stop_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
LOOK = 48
ATRLB = 10
COOLDOWN = 4
HOLD = 80
RR = 2.0
BE_FRACS = [0.50, 0.65, 0.75, 0.85]


def donch(bars, i, look):
    if i < look:
        return None
    seg = bars[i - look:i]
    return min(x['l'] for x in seg), max(x['h'] for x in seg)


def signals(bars):
    out = []; n = len(bars); last = -1
    for i in range(LOOK + 2, n - 1):
        if i <= last:
            continue
        band = donch(bars, i, LOOK)
        if not band:
            continue
        lo, hi = band; a = atr(bars, 14, i); ap = atr(bars, 14, i - ATRLB)
        if a is None or ap is None or a <= 0 or a <= ap:
            continue
        b = bars[i]
        if b['c'] > hi:
            entry, stop = b['c'], hi - a
            if stop < entry:
                out.append((i + 1, entry, stop, 'bull')); last = i + COOLDOWN
        elif b['c'] < lo:
            entry, stop = b['c'], lo + a
            if stop > entry:
                out.append((i + 1, entry, stop, 'bear')); last = i + COOLDOWN
    return out


def walk_baseline(bars, i0, entry, stop, d):
    R = abs(entry - stop)
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
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
    """Move stop to entry (break-even) once price reaches `frac` of the way to target."""
    R = abs(entry - stop)
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    trig = entry + frac * (tgt - entry)          # the X%-to-target price
    be_armed = False
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            eff_stop = entry if be_armed else stop
            if b['l'] <= eff_stop:
                return 0.0 if be_armed else -1.0
            if b['h'] >= tgt:
                return RR
            if not be_armed and b['h'] >= trig:
                be_armed = True                  # arm only this bar
        else:
            eff_stop = entry if be_armed else stop
            if b['h'] >= eff_stop:
                return 0.0 if be_armed else -1.0
            if b['l'] <= tgt:
                return RR
            if not be_armed and b['l'] <= trig:
                be_armed = True
    return None


def main():
    d = json.load(open(HIST))['pairs']
    base = defaultdict(list)
    be = {f: defaultdict(list) for f in BE_FRACS}
    for pk in [x for x in PAIR_CLASS if x in d]:
        cls = PAIR_CLASS.get(pk)
        bars = _bars_norm(d[pk].get('h1', []))
        if len(bars) < LOOK + 300:
            continue
        for (ei, entry, stop, dr) in signals(bars):
            if ei >= len(bars):
                continue
            R = abs(entry - stop)
            o = walk_baseline(bars, ei, entry, stop, dr)
            if o is not None:
                base[cls].append((bars[ei]['_ts'], o - cost(o, entry, R)))
            for f in BE_FRACS:
                ob = walk_be(bars, ei, entry, stop, dr, f)
                if ob is not None:
                    be[f][cls].append((bars[ei]['_ts'], ob - cost(ob, entry, R)))

    def line(label, rows):
        rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq)
        print(f"      {label:<22} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R")

    print("=" * 92)
    print("Break-even-at-X%-to-target vs baseline — RR2 breakout, h1, all pairs, market fills+cost")
    print("=" * 92)
    for cls in ['index', 'comm', 'major', 'minor', 'crypto']:
        if not base[cls]:
            continue
        print(f"\n===== {cls} =====")
        line('baseline (no BE)', base[cls])
        for f in BE_FRACS:
            line(f'BE @ {int(f*100)}% to target', be[f][cls])
    print("\n===== ALL =====")
    line('baseline (no BE)', [r for c in base for r in base[c]])
    for f in BE_FRACS:
        line(f'BE @ {int(f*100)}% to target', [r for c in be[f] for r in be[f][c]])


if __name__ == '__main__':
    main()
