"""Liquidity-sweep + RETEST-entry setup (user screenshot, amntrading1).

Bullish: price sweeps a recent low (takes liquidity below) then RECLAIMS it (closes
back above). Wait for price to RETEST the swept level and hold, then BUY. Stop below
the sweep low; target RR. Bearish mirror (sweep a recent high, reclaim, retest, sell).

Distinct from engulf_manip: entry is on the RETEST of the swept level, not on the
sweep candle itself. Realistic next-bar fills, structural stop, RR sweep, OOS, per
class, 4h/h1/daily. Generic price-action, clean-room.

Run: python liquidity_sweep_retest_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
LB = 5              # bars defining the recent low/high (liquidity level)
SWEEP_TOL = 0.10    # retest must come within this*ATR of the swept level
RETEST_WIN = 12     # bars to wait for the retest after the reclaim
BUF = 0.15
COOLDOWN = 4
RRS = [1.5, 2.0, 3.0]
HOLD = {'h1': 48, '4h': 60, 'daily': 25}


def scan(bars, tf, store, cls, store_cls):
    n = len(bars); last = -1
    for i in range(LB + 1, n - 1):
        if i <= last:
            continue
        prior = bars[i-LB:i]
        lvl_lo = min(b['l'] for b in prior); lvl_hi = max(b['h'] for b in prior)
        # bullish sweep+reclaim of a recent low
        if bars[i]['l'] < lvl_lo and bars[i]['c'] > lvl_lo:
            d = 'bull'; lvl = lvl_lo; sweep_ext = bars[i]['l']
        elif bars[i]['h'] > lvl_hi and bars[i]['c'] < lvl_hi:
            d = 'bear'; lvl = lvl_hi; sweep_ext = bars[i]['h']
        else:
            continue
        a0 = atr(bars, 14, i) or 0.0
        ei = None
        for j in range(i + 1, min(i + 1 + RETEST_WIN, n - 1)):
            if d == 'bull':
                if bars[j]['c'] < sweep_ext:      # lost the sweep low -> failed
                    break
                if bars[j]['l'] <= lvl + SWEEP_TOL*a0 and bars[j]['c'] > lvl:
                    ei = j + 1; break
            else:
                if bars[j]['c'] > sweep_ext:
                    break
                if bars[j]['h'] >= lvl - SWEEP_TOL*a0 and bars[j]['c'] < lvl:
                    ei = j + 1; break
        if ei is None or ei <= last or ei >= n:
            continue
        entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
        stop = (sweep_ext - BUF*a) if d == 'bull' else (sweep_ext + BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        for rr in RRS:
            o = walk(bars, ei, entry, stop, d, rr, HOLD[tf])
            if o is not None:
                net = o - cost(o, entry, R)
                store[(tf, rr)].append((ts, net))
                store_cls[cls][(tf, rr)].append((ts, net))
        last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, store, cls, store_cls)

    print(f"Liquidity-sweep + retest entry — {npairs} pairs, realistic fills, OOS\n")
    for tf in ('4h', 'h1', 'daily'):
        for rr in RRS:
            line(f"{tf} RR{rr}", store[(tf, rr)])
        print()
    print("=== 4H per class (RR2) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][('4h', 2.0)])


if __name__ == '__main__':
    main()
