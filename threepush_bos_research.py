"""3-push + break-of-structure + retest reversal (user-drawn, IMG_6725).

Bearish setup (as described — "move up 3 times taking previous highs, then break
structure and sell the retrace"):
  1. THREE pushes up: swing highs h1<h2<h3 on rising higher-lows l0<l1<l2
     (a clean 3-drive uptrend). The 'structure' = the last higher low l2.
  2. BREAK OF STRUCTURE: price closes below l2 (uptrend broken). Invalid if a new
     high above h3 prints first (trend resumed).
  3. RETEST: price rallies back UP to the broken level l2 (old support -> resistance)
     and closes back below it. SELL the retest (next-bar open, market fill).
  4. Stop just above the retest / structure; target RR2.

Bullish mirror (3 pushes down -> break up -> retest -> buy) tested too. Realistic
next-bar fills, structural stop, RR sweep, OOS split, per class (commodities
highlighted), h1/4h/daily. Generic price-action, clean-room.

Run: python threepush_bos_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PRD = 3
BOS_WIN = 30
RETEST_WIN = 20
BUF = 0.25
COOLDOWN = 5
RRS = [1.5, 2.0, 3.0]
HOLD = {'h1': 48, '4h': 60, 'daily': 25}


def zigzag(bars, prd):
    n = len(bars); piv = []
    for i in range(prd, n - prd):
        if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, prd+1)):
            piv.append((i, bars[i]['h'], 'H'))
        if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, prd+1)):
            piv.append((i, bars[i]['l'], 'L'))
    piv.sort()
    out = []
    for p in piv:
        if out and p[2] == out[-1][2]:
            if (p[2] == 'H' and p[1] > out[-1][1]) or (p[2] == 'L' and p[1] < out[-1][1]):
                out[-1] = p
        else:
            out.append(p)
    return out


def _emit(store, store_cls, cls, tf, bars, ei, entry, stop, d):
    if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']; hold = HOLD[tf]
    for rr in RRS:
        o = walk(bars, ei, entry, stop, d, rr, hold)
        if o is not None:
            net = o - cost(o, entry, R)
            store[(tf, rr)].append((ts, net))
            store_cls[cls][(tf, rr)].append((ts, net))


def scan(bars, tf, store, cls, store_cls):
    zz = zigzag(bars, PRD); n = len(bars); last = -1
    for k in range(len(zz) - 5):
        w = zz[k:k+6]; kinds = tuple(x[2] for x in w)
        # ── bearish: 3 pushes UP -> break down -> retest -> SELL ──
        if kinds == ('L', 'H', 'L', 'H', 'L', 'H'):
            l0, h1, l1, h2, l2, h3 = (x[1] for x in w)
            if not (h1 < h2 < h3 and l0 < l1 < l2):
                continue
            struct = l2; i_h3 = w[5][0]
            start = i_h3 + PRD + 1; bos = None
            for j in range(start, min(start + BOS_WIN, n - 1)):
                if bars[j]['h'] > h3:
                    break
                if bars[j]['c'] < struct:
                    bos = j; break
            if bos is None:
                continue
            ei = None
            for j in range(bos + 1, min(bos + 1 + RETEST_WIN, n - 1)):
                if bars[j]['c'] > h3:
                    break
                if bars[j]['h'] >= struct and bars[j]['c'] < struct:
                    ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
            stop = max(bars[ei-1]['h'], struct) + BUF * a
            _emit(store, store_cls, cls, tf, bars, ei, entry, stop, 'bear'); last = ei + COOLDOWN
        # ── bullish mirror: 3 pushes DOWN -> break up -> retest -> BUY ──
        elif kinds == ('H', 'L', 'H', 'L', 'H', 'L'):
            h0, l1, h1b, l2b, h2b, l3 = (x[1] for x in w)
            if not (l1 > l2b > l3 and h0 > h1b > h2b):
                continue
            struct = h2b; i_l3 = w[5][0]
            start = i_l3 + PRD + 1; bos = None
            for j in range(start, min(start + BOS_WIN, n - 1)):
                if bars[j]['l'] < l3:
                    break
                if bars[j]['c'] > struct:
                    bos = j; break
            if bos is None:
                continue
            ei = None
            for j in range(bos + 1, min(bos + 1 + RETEST_WIN, n - 1)):
                if bars[j]['c'] < l3:
                    break
                if bars[j]['l'] <= struct and bars[j]['c'] > struct:
                    ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
            stop = min(bars[ei-1]['l'], struct) - BUF * a
            _emit(store, store_cls, cls, tf, bars, ei, entry, stop, 'bull'); last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


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

    print(f"3-push + BOS + retest reversal — {npairs} pairs, realistic fills, OOS\n")
    print("=== ALL PAIRS ===")
    for tf in ('h1', '4h', 'daily'):
        for rr in RRS:
            line(f"{tf} RR{rr}", store[(tf, rr)])
        print()
    print("=== COMMODITIES (primary) — RR2 ===")
    for tf in ('h1', '4h', 'daily'):
        line(f"comm {tf}", store_cls['comm'][(tf, 2.0)])
    print("\n=== other classes — RR2 (4h) ===")
    for c in ['crypto', 'index', 'major', 'minor']:
        line(f"{c} 4h", store_cls[c][('4h', 2.0)])


if __name__ == '__main__':
    main()
