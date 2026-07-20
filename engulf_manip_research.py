"""4H 'manipulation' engulfing-candle reversal (user screenshots, tomcampcoaching).

Bullish: an engulfing candle whose LOW takes out recent lows (the manipulation /
liquidity sweep) then CLOSES bullish, engulfing the prior candle -> BUY.
Bearish mirror: engulf sweeps recent highs then closes bearish -> SELL.
Stop = beyond the engulfing candle's swept extreme (its low for longs); target RR.

Entry next-bar open (realistic), structural stop, RR sweep, OOS, per class.
Primary tf = 4H (as described); h1/daily shown for context. Generic candlestick
price-action, clean-room.

Run: python engulf_manip_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg, is_engulf

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
LB = 3            # bars whose low/high the candle must sweep ("previous low")
BUF = 0.10        # stop buffer as fraction of ATR
COOLDOWN = 3
RRS = [1.0, 1.5, 2.0]
HOLD = {'h1': 48, '4h': 60, 'daily': 25}


def scan(bars, tf, store, cls, store_cls):
    n = len(bars); last = -1
    for i in range(LB + 2, n - 1):
        if i <= last:
            continue
        prior = bars[i-LB:i]
        d = None
        # bullish: bullish engulf + low sweeps recent lows
        if is_engulf(bars, i, 'bull') and bars[i]['l'] < min(b['l'] for b in prior):
            d = 'bull'
        elif is_engulf(bars, i, 'bear') and bars[i]['h'] > max(b['h'] for b in prior):
            d = 'bear'
        if d is None:
            continue
        ei = i + 1; entry = bars[ei]['o']; a = atr(bars, 14, i) or 0.0
        stop = (bars[i]['l'] - BUF*a) if d == 'bull' else (bars[i]['h'] + BUF*a)
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
    be = 100/(1+float(label.split('RR')[-1])) if 'RR' in label else 0
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<12} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


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

    print(f"4H manipulation engulf reversal — {npairs} pairs, realistic fills, OOS\n")
    for tf in ('4h', 'h1', 'daily'):
        print(f"=== {tf.upper()} (breakeven WR: RR1=50%, RR1.5=40%, RR2=33.3%) ===")
        for rr in RRS:
            line(f"{tf} RR{rr}", store[(tf, rr)])
        print()
    print("=== 4H per class (RR2) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c} RR2.0", store_cls[c][('4h', 2.0)])


if __name__ == '__main__':
    main()
