"""Replicate Paul Bratby's xBrat 'Confluence Not Coincidence' grader and test its
core thesis: does more confluence = better expectancy?

His xBrat Algo grades a trade on 12 points of control (6* = 12/12) built from:
EMA configuration, EMA Cloud, Stochastics, MACD, and multi-timeframe bias. We
replicate the same MARKER SET (5 directional markers) and grade each trade 1..5.

Trigger = MACD cross (his Roller Coaster is a Stochastic/MACD cross). At the
trigger bar we count how many of the other markers agree, giving a grade. Entry
next-bar-open, structural stop, RR2, realistic cost, OOS split. We report
expectancy BY GRADE — the whole point is to see whether the high-confluence
buckets (his 5*/6*) actually outperform, or underperform like our own 4/4 does.

Markers (each bull/bear/neutral):
  1 MACD cross          (the trigger)
  2 EMA stack           ema8>ema21>ema50 (trend configuration)
  3 EMA cloud           close vs the ema21/ema50 band  (~ our cl_dir)
  4 Stochastic          %K vs %D with room             (the NEW marker)
  5 HTF bias            ema50 slope (trend-of-trend, multi-TF proxy)

[Volume/VWAP and the literal 6-timeframe heatmap are approximated — no volume in
the feed, and we use a single-series slope for HTF bias.]

Run: python bratby_confluence_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import ema, agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
SWING = 6
ATR_BUF = 0.25
COOLDOWN = 5
HOLD = {'m15': 48, 'h1': 48, '4h': 60}


def macd_lines(closes):
    m = [None]*len(closes)
    e12, e26 = ema(closes, 12), ema(closes, 26)
    for i in range(len(closes)):
        if e12[i] is not None and e26[i] is not None:
            m[i] = e12[i] - e26[i]
    vals = [x if x is not None else 0.0 for x in m]
    sig = ema(vals, 9)
    return m, sig


def stochastic(bars, n=14, d=3):
    K = [None]*len(bars)
    for i in range(len(bars)):
        if i < n-1:
            continue
        win = bars[i-n+1:i+1]
        hh = max(b['h'] for b in win); ll = min(b['l'] for b in win)
        K[i] = 50.0 if hh == ll else 100*(bars[i]['c']-ll)/(hh-ll)
    Kv = [x if x is not None else 50.0 for x in K]
    D = ema(Kv, d)
    return K, D


def markers(bars, closes, e8, e21, e50, macd, sig, K, D, i, dirn):
    """Return how many of markers 2..5 agree with dirn (0..4)."""
    agree = 0
    # 2 EMA stack
    if None not in (e8[i], e21[i], e50[i]):
        st = 'bull' if e8[i] > e21[i] > e50[i] else ('bear' if e8[i] < e21[i] < e50[i] else None)
        agree += st == dirn
    # 3 EMA cloud (ema21/ema50 band)
    if None not in (e21[i], e50[i]):
        hi, lo = max(e21[i], e50[i]), min(e21[i], e50[i])
        cl = 'bull' if closes[i] > hi else ('bear' if closes[i] < lo else None)
        agree += cl == dirn
    # 4 Stochastic (K vs D with room)
    if K[i] is not None and D[i] is not None:
        sto = 'bull' if (K[i] > D[i] and K[i] < 80) else ('bear' if (K[i] < D[i] and K[i] > 20) else None)
        agree += sto == dirn
    # 5 HTF bias (ema50 slope over 3 bars)
    if e50[i] is not None and e50[i-3] is not None:
        hb = 'bull' if e50[i] > e50[i-3] else ('bear' if e50[i] < e50[i-3] else None)
        agree += hb == dirn
    return agree


def scan(bars, tf, store, cls, store_cls):
    closes = [b['c'] for b in bars]
    e8, e21, e50 = ema(closes, 8), ema(closes, 21), ema(closes, 50)
    macd, sig = macd_lines(closes)
    K, D = stochastic(bars)
    n = len(bars); last = -1
    for i in range(51, n-1):
        if i <= last:
            continue
        if None in (macd[i], macd[i-1], sig[i], sig[i-1]):
            continue
        up = macd[i-1] <= sig[i-1] and macd[i] > sig[i]
        dn = macd[i-1] >= sig[i-1] and macd[i] < sig[i]
        if not (up or dn):
            continue
        dirn = 'bull' if up else 'bear'
        grade = 1 + markers(bars, closes, e8, e21, e50, macd, sig, K, D, i, dirn)
        ei = i+1; entry = bars[ei]['o']; a = atr(bars, 14, i) or 0.0
        if dirn == 'bull':
            stop = min(b['l'] for b in bars[max(0, i-SWING):i+1]) - ATR_BUF*a
        else:
            stop = max(b['h'] for b in bars[max(0, i-SWING):i+1]) + ATR_BUF*a
        if (dirn == 'bull' and stop >= entry) or (dirn == 'bear' and stop <= entry):
            continue
        R = abs(entry-stop); ts = bars[ei]['_ts']
        o = walk(bars, ei, entry, stop, dirn, RR, HOLD[tf])
        if o is None:
            continue
        r = o - cost(o, entry, R)
        store[(tf, grade)].append((ts, r))
        store[(tf, 'ALL')].append((ts, r))
        store_cls[cls][(tf, grade)].append((ts, r))
        last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>5} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'m15': m15, 'h1': h1, '4h': agg4h(h1)}.items():
            if len(bars) < 120:
                continue
            scan(bars, tf, store, cls, store_cls)

    print(f"Bratby xBrat confluence grader — {npairs} pairs · MACD-cross trigger · RR2 · OOS split\n")
    print("Expectancy BY CONFLUENCE GRADE (1 = trigger only … 5 = all markers aligned):")
    for tf in ('m15', 'h1', '4h'):
        print(f"=== {tf.upper()} ===")
        for g in (1, 2, 3, 4, 5):
            line(f"grade {g}", store[(tf, g)])
        line("ALL grades", store[(tf, 'ALL')])
        # high vs low confluence
        hi = store[(tf, 4)] + store[(tf, 5)]; lo = store[(tf, 1)] + store[(tf, 2)]
        line("HIGH (4-5)", hi); line("LOW (1-2)", lo)
        print()


if __name__ == '__main__':
    main()
