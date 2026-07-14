"""Stress-test the one trendline cell that passed the first screen:
4H timeframe, retest confirmation candle must be a no-wick candle.

Three tests, the same bar the real edges had to clear:
  1. 6-fold rolling walk-forward (chronological) — needs most folds > 0.
  2. Parameter sensitivity — perturb PIVOT_L / RETEST_WIN / NOWICK thresholds /
     COOLDOWN and see if the edge survives or is knife-edge on one setting.
  3. Per-class of the 4H nowick_entry variant.
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
RR = 2.0
HOLD_4H = 60


def pivots(bars, L):
    highs, lows = [], []
    n = len(bars)
    for i in range(L, n - L):
        if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, L+1)):
            highs.append(i)
        if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, L+1)):
            lows.append(i)
    return highs, lows


def nowick(bar, d, body_min, rej_max):
    rng = bar['h'] - bar['l']
    if rng <= 0:
        return False
    body = abs(bar['c'] - bar['o'])
    if d == 'bull':
        rej = min(bar['o'], bar['c']) - bar['l']; directional = bar['c'] > bar['o']
    else:
        rej = bar['h'] - max(bar['o'], bar['c']); directional = bar['c'] < bar['o']
    return directional and (body / rng >= body_min) and (rej / rng <= rej_max)


def collect(pivot_l, retest_win, cooldown, body_min, rej_max, atr_buf=0.25):
    """Return list of (ts, net_r, cls) for the 4H nowick_entry variant."""
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    out = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        bars = agg4h(h1)
        if len(bars) < 120:
            continue
        ph, pl = pivots(bars, pivot_l); n = len(bars); last_fired = -1
        for b in range(2, n - 1):
            if b <= last_fired:
                continue
            for dd in ('bull', 'bear'):
                piv = ph if dd == 'bull' else pl
                hi = bisect.bisect_right(piv, b - pivot_l - 1) - 1
                if hi < 1:
                    continue
                p2 = piv[hi]; p1 = piv[hi - 1]
                v1 = bars[p1]['h'] if dd == 'bull' else bars[p1]['l']
                v2 = bars[p2]['h'] if dd == 'bull' else bars[p2]['l']
                if dd == 'bull' and not (v2 < v1):
                    continue
                if dd == 'bear' and not (v2 > v1):
                    continue
                slope = (v2 - v1) / (p2 - p1)
                def line(x):
                    return v1 + slope * (x - p1)
                if dd == 'bull':
                    broke = bars[b-1]['c'] <= line(b-1) and bars[b]['c'] > line(b) and bars[b]['c'] > bars[b]['o']
                else:
                    broke = bars[b-1]['c'] >= line(b-1) and bars[b]['c'] < line(b) and bars[b]['c'] < bars[b]['o']
                if not broke:
                    continue
                for r in range(b + 1, min(b + 1 + retest_win, n - 1)):
                    lr = line(r)
                    touched = (bars[r]['l'] <= lr and bars[r]['c'] > lr) if dd == 'bull' else (bars[r]['h'] >= lr and bars[r]['c'] < lr)
                    if not touched:
                        continue
                    ei = r + 1; entry = bars[ei]['o']; a = atr(bars, 14, r) or 0.0
                    if dd == 'bull':
                        stop = min(bars[r]['l'], lr) - atr_buf * a
                        if stop >= entry:
                            break
                    else:
                        stop = max(bars[r]['h'], lr) + atr_buf * a
                        if stop <= entry:
                            break
                    if nowick(bars[r], dd, body_min, rej_max):
                        o = walk(bars, ei, entry, stop, dd, RR, HOLD_4H)
                        if o is not None:
                            out.append((bars[ei]['_ts'], o - cost(o, entry, abs(entry-stop)), cls))
                    last_fired = ei + cooldown
                    break
                if b <= last_fired:
                    break
    return out


def main():
    base = collect(3, 12, 8, 0.65, 0.20)
    base.sort()
    seq = [r for _, r, _ in base]
    n, w, e = agg(seq)
    print(f"BASE 4H nowick_entry: n={n} WR={w:.1f}% exp={e:+.3f}R\n")

    print("1) 6-fold rolling walk-forward (chronological):")
    k = 6; sz = len(base) // k; passed = 0
    for f in range(k):
        lo = f * sz; hi = (f + 1) * sz if f < k - 1 else len(base)
        fold = [r for _, r, _ in base[lo:hi]]
        fn, fw, fe = agg(fold)
        ok = fe > 0; passed += ok
        print(f"   fold {f+1}: n={fn:>3} WR={fw:>5.1f}% exp={fe:>+7.3f}R  {'ok' if ok else 'NEG'}")
    print(f"   -> {passed}/{k} folds positive\n")

    print("2) Parameter sensitivity (each row re-runs the whole backtest):")
    grid = [
        ('pivot_l=2', dict(pivot_l=2, retest_win=12, cooldown=8, body_min=0.65, rej_max=0.20)),
        ('pivot_l=4', dict(pivot_l=4, retest_win=12, cooldown=8, body_min=0.65, rej_max=0.20)),
        ('retest=8',  dict(pivot_l=3, retest_win=8,  cooldown=8, body_min=0.65, rej_max=0.20)),
        ('retest=16', dict(pivot_l=3, retest_win=16, cooldown=8, body_min=0.65, rej_max=0.20)),
        ('cooldown=4',dict(pivot_l=3, retest_win=12, cooldown=4, body_min=0.65, rej_max=0.20)),
        ('cooldown=12',dict(pivot_l=3,retest_win=12, cooldown=12,body_min=0.65, rej_max=0.20)),
        ('body>=0.55',dict(pivot_l=3, retest_win=12, cooldown=8, body_min=0.55, rej_max=0.25)),
        ('body>=0.75',dict(pivot_l=3, retest_win=12, cooldown=8, body_min=0.75, rej_max=0.15)),
    ]
    for label, kw in grid:
        rows = collect(**kw); s = [r for _, r, _ in rows]
        nn, ww, ee = agg(s)
        print(f"   {label:<12} n={nn:>4} WR={ww:>5.1f}% exp={ee:>+7.3f}R")
    print()

    print("3) Per-class (base config):")
    byc = defaultdict(list)
    for ts, r, c in base:
        byc[c].append(r)
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        nn, ww, ee = agg(byc[c])
        print(f"   {c:<8} n={nn:>4} WR={ww:>5.1f}% exp={ee:>+7.3f}R")


if __name__ == '__main__':
    main()
