"""Denislav Dantev-style Fibonacci golden-zone REVERSAL/continuation method.

Public description (the exact rules are paywalled): retrace into the Fibonacci
golden zone of a clean swing leg, enter on reversal confirmation, defined stop
and Fib-extension targets. Claimed ~64% win rate and ~2.30 PROFIT FACTOR over
6,000 student trades.

Faithful mechanical version:
  - Clean swing legs from fractal pivots (prd=5): swing-low -> swing-high = up-leg.
  - ENTRY: after the leg, price retraces into the 50-61.8% golden zone and a bar
    closes back in-trend (bullish reversal for an up-leg). Enter next-bar open.
    Invalidate if price trades beyond the 78.6% retrace before entry.
  - STOP: just beyond the 78.6% level (tight invalidation, + ATR buffer).
  - TARGETS (Fib extensions of the leg, tested separately):
        100%  = the prior swing high/low
        127.2%, 161.8% = measured extensions of the leg
  - Next-bar fills, realistic cost, OOS split, all pairs, daily / 4h / h1.
  - Reports WR, mean-R expectancy, AND PROFIT FACTOR (gross win R / gross loss R)
    so the 2.30 claim can be checked head-on.

Run: python dantev_fib_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PRD = 5
Z_HI, Z_LO = 0.5, 0.618          # golden zone
STOP_LVL = 0.786
ATR_BUF = 0.25
BREAK_WIN = 40
COOLDOWN = 4
EXTS = {'100%': 1.0, '127%': 1.272, '162%': 1.618}
FIXED_RR = {'RR2': 2.0}          # his published avg win ~= +2.14R -> a ~2R target
HOLD = {'daily': 30, '4h': 40, 'h1': 60}


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


def walk_to(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= target:
                return (target - entry) / R
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= target:
                return (entry - target) / R
    return None


def scan(bars, tf, store, cls, store_cls):
    piv = zigzag(bars, PRD); n = len(bars); hold = HOLD[tf]; last = -1
    for k in range(1, len(piv)):
        p0, p1 = piv[k-1], piv[k]
        # up-leg L->H (bull continuation) or down-leg H->L (bear)
        if p0[2] == 'L' and p1[2] == 'H':
            d = 'bull'; L, H = p0[1], p1[1]
        elif p0[2] == 'H' and p1[2] == 'L':
            d = 'bear'; H, L = p0[1], p1[1]
        else:
            continue
        rng = H - L
        if rng <= 0:
            continue
        if d == 'bull':
            zhi = H - Z_HI*rng; zlo = H - Z_LO*rng; void = H - STOP_LVL*rng
        else:
            zlo = L + Z_HI*rng; zhi = L + Z_LO*rng; void = L + STOP_LVL*rng
        start = p1[0] + PRD + 1; ei = None
        for j in range(start, min(start + BREAK_WIN, n - 1)):
            b = bars[j]
            if d == 'bull':
                if b['l'] < void:
                    break
                if b['l'] <= zhi and b['c'] > b['o'] and b['c'] > zlo:
                    ei = j + 1; break
            else:
                if b['h'] > void:
                    break
                if b['h'] >= zlo and b['c'] < b['o'] and b['c'] < zhi:
                    ei = j + 1; break
        if ei is None or ei <= last or ei >= n:
            continue
        entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
        stop = (void - ATR_BUF*a) if d == 'bull' else (void + ATR_BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        targets = {name: ((L + ext*rng) if d == 'bull' else (H - ext*rng)) for name, ext in EXTS.items()}
        for name, rr in FIXED_RR.items():
            targets[name] = (entry + rr*R) if d == 'bull' else (entry - rr*R)
        for name, target in targets.items():
            if (d == 'bull' and target <= entry) or (d == 'bear' and target >= entry):
                continue
            o = walk_to(bars, ei, entry, stop, target, d, hold)
            if o is None:
                continue
            net = o - cost(o, entry, R)
            store[(tf, name)].append((ts, net))
            store_cls[cls][(tf, name)].append((ts, net))
        last = ei + COOLDOWN


def stats(rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n = len(seq)
    if not n:
        return 0, 0, 0, 0, 0, 0
    w = sum(1 for x in seq if x > 0); wr = 100*w/n; exp = sum(seq)/n
    gw = sum(x for x in seq if x > 0); gl = -sum(x for x in seq if x < 0)
    pf = (gw/gl) if gl > 0 else float('inf')
    mid = n//2
    eh = (sum(seq[:mid])/mid) if mid else 0
    es = (sum(seq[mid:])/(n-mid)) if n-mid else 0
    return n, wr, exp, pf, eh, es


def line(label, rows):
    n, wr, exp, pf, eh, es = stats(rows)
    v = 'PASS' if (exp > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>4} WR={wr:>5.1f}% exp={exp:>+7.3f}R  PF={pf:>4.2f}  OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'daily': daily, '4h': agg4h(h1), 'h1': h1}.items():
            if len(bars) < 120:
                continue
            scan(bars, tf, store, cls, store_cls)

    tgt_names = list(EXTS) + list(FIXED_RR)
    print(f"Dantev-style Fib golden-zone reversal — {npairs} pairs, realistic cost, OOS. Published: 52.5% WR / PF 2.39 / +0.70R\n")
    print("=== ALL PAIRS ===")
    for tf in ('daily', '4h', 'h1'):
        for name in tgt_names:
            line(f"{tf} {name}", store[(tf, name)])
        print()

    print("=== BY ASSET CLASS (does minor FX stand out?) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        print(f"--- {c} ---")
        for tf in ('daily', '4h', 'h1'):
            for name in tgt_names:
                line(f"{tf} {name}", store_cls[c][(tf, name)])
        print()


if __name__ == '__main__':
    main()
