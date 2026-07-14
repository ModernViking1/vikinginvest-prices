"""Trendline break-and-retest strategy (user-proposed, IMG_6632 / XAUUSD 5m).

Faithful mechanical version of the setup:
  1. A trendline is drawn across the last two CONFIRMED descending pivot highs
     (bullish setup) or ascending pivot lows (bearish setup).
  2. BREAK: a candle CLOSES beyond the line in the trade direction and is itself
     directional (green close>open for a bull break of a downtrend line).
  3. RETEST: price pulls back to the line; a candle touches it and closes back
     on the trade side. Enter at the NEXT bar open (realistic market fill).
  4. Stop just beyond the retest extreme (+ATR buffer); target at RR 2:1.

Variants tested:
  plain        — retest confirmation candle, no shape requirement
  nowick_entry — retest candle must be a strong no-wick (marubozu-ish) candle
  nowick_break — the BREAK candle must be a strong no-wick candle

Everything else matches the house framework: next-bar-open entry, structural
stop, RR2, fixed realistic cost, chronological OOS split, per-class breakdown.
No lookahead — pivots are only used once confirmed (L bars to their right).

Run: python trendline_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')

RR = 2.0
PIVOT_L = 3            # fractal half-width: pivot confirmed L bars to its right
RETEST_WIN = 12       # bars after the break to wait for a retest
COOLDOWN = 8          # bars to skip after a fired trade (avoid clustering on one line)
ATR_BUF = 0.25        # stop buffer as a fraction of ATR(14)
NOWICK_BODY = 0.65    # body/range >= this  AND
NOWICK_REJ = 0.20     # rejection-side wick/range <= this  => "no-wick" candle
HOLD = {'m15': 48, 'h1': 48, '4h': 60}


def pivots(bars, L):
    """Return (highs, lows) index lists. A pivot high at i is the strict max of
    its L neighbours each side; confirmed only at bar i+L."""
    highs, lows = [], []
    n = len(bars)
    for i in range(L, n - L):
        hi = bars[i]['h']; lo = bars[i]['l']
        if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, L+1)):
            highs.append(i)
        if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, L+1)):
            lows.append(i)
    return highs, lows


def nowick(bar, d):
    rng = bar['h'] - bar['l']
    if rng <= 0:
        return False
    body = abs(bar['c'] - bar['o'])
    if d == 'bull':
        rej = min(bar['o'], bar['c']) - bar['l']       # lower wick
        directional = bar['c'] > bar['o']
    else:
        rej = bar['h'] - max(bar['o'], bar['c'])       # upper wick
        directional = bar['c'] < bar['o']
    return directional and (body / rng >= NOWICK_BODY) and (rej / rng <= NOWICK_REJ)


def scan(bars, tf, store, cls, store_cls):
    ph, pl = pivots(bars, PIVOT_L)
    n = len(bars)
    hold = HOLD[tf]
    last_fired = -1

    for b in range(2, n - 1):
        if b <= last_fired:
            continue

        for d in ('bull', 'bear'):
            piv = ph if d == 'bull' else pl
            # confirmed pivots strictly before the break bar (p + L < b)
            hi_idx = bisect.bisect_right(piv, b - PIVOT_L - 1) - 1
            if hi_idx < 1:
                continue
            p2 = piv[hi_idx]; p1 = piv[hi_idx - 1]
            v1 = bars[p1]['h'] if d == 'bull' else bars[p1]['l']
            v2 = bars[p2]['h'] if d == 'bull' else bars[p2]['l']
            # descending highs for a downtrend line (bull), ascending lows (bear)
            if d == 'bull' and not (v2 < v1):
                continue
            if d == 'bear' and not (v2 > v1):
                continue
            slope = (v2 - v1) / (p2 - p1)
            def line(x):
                return v1 + slope * (x - p1)

            # fresh directional break CLOSE beyond the line
            if d == 'bull':
                broke = bars[b-1]['c'] <= line(b-1) and bars[b]['c'] > line(b) and bars[b]['c'] > bars[b]['o']
            else:
                broke = bars[b-1]['c'] >= line(b-1) and bars[b]['c'] < line(b) and bars[b]['c'] < bars[b]['o']
            if not broke:
                continue

            break_nowick = nowick(bars[b], d)

            # look for a retest of the line
            for r in range(b + 1, min(b + 1 + RETEST_WIN, n - 1)):
                lr = line(r)
                if d == 'bull':
                    touched = bars[r]['l'] <= lr and bars[r]['c'] > lr
                else:
                    touched = bars[r]['h'] >= lr and bars[r]['c'] < lr
                if not touched:
                    continue
                entry_i = r + 1
                entry = bars[entry_i]['o']
                a = atr(bars, 14, r) or 0.0
                if d == 'bull':
                    stop = min(bars[r]['l'], lr) - ATR_BUF * a
                    if stop >= entry:
                        break
                else:
                    stop = max(bars[r]['h'], lr) + ATR_BUF * a
                    if stop <= entry:
                        break
                R = abs(entry - stop); ts = bars[entry_i]['_ts']
                o = walk(bars, entry_i, entry, stop, d, RR, hold)
                if o is not None:
                    net = o - cost(o, entry, R)
                    store['plain'].append((ts, net))
                    store_cls[('plain', cls)].append((ts, net))
                    if nowick(bars[r], d):
                        store['nowick_entry'].append((ts, net))
                        store_cls[('nowick_entry', cls)].append((ts, net))
                    if break_nowick:
                        store['nowick_break'].append((ts, net))
                        store_cls[('nowick_break', cls)].append((ts, net))
                last_fired = entry_i + COOLDOWN
                break
            if b <= last_fired:
                break


def report(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    verdict = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>5} WR={w:>5.1f}% exp={e:>+7.3f}R  OOS[{eh:>+6.3f}/{es:>+6.3f}]  {verdict}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    tfs = ['m15', 'h1', '4h']
    per_tf = {tf: defaultdict(list) for tf in tfs}
    per_tf_cls = {tf: defaultdict(list) for tf in tfs}
    npairs = defaultdict(int)

    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        series = {'m15': m15, 'h1': h1, '4h': agg4h(h1)}
        for tf in tfs:
            bars = series[tf]
            if len(bars) < 120:
                continue
            npairs[tf] += 1
            scan(bars, tf, per_tf[tf], cls, per_tf_cls[tf])

    print("Trendline break-and-retest — RR 2:1, next-bar-open fills, realistic cost, OOS split\n")
    for tf in tfs:
        print(f"=== {tf.upper()}  ({npairs[tf]} pairs) ===")
        for variant in ('plain', 'nowick_entry', 'nowick_break'):
            report(variant, per_tf[tf][variant])
        print()

    # per-class breakdown of the plain variant on each timeframe
    print("Per-class breakdown (plain variant):")
    classes = ['comm', 'crypto', 'index', 'major', 'minor']
    for tf in tfs:
        print(f"  --- {tf.upper()} ---")
        for c in classes:
            report(c, per_tf_cls[tf][('plain', c)])
        print()


if __name__ == '__main__':
    main()
