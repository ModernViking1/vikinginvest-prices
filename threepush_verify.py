"""Stress-test the one 3-push+BOS+retest cell that passed: COMMODITIES, 4H, RR2.
6-fold walk-forward + parameter sensitivity — the bar the real edges cleared.
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
HOLD4 = 60


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


def collect(prd=3, bos_win=30, retest_win=20, buf=0.25, cooldown=5):
    d = json.load(open(HIST)); pairs = d['pairs']; rows = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        if PAIR_CLASS.get(pk) != 'comm':
            continue
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        bars = agg4h(h1)
        if len(bars) < 150:
            continue
        zz = zigzag(bars, prd); n = len(bars); last = -1
        for k in range(len(zz) - 5):
            w = zz[k:k+6]; kinds = tuple(x[2] for x in w)
            if kinds == ('L', 'H', 'L', 'H', 'L', 'H'):
                l0, h1v, l1, h2, l2, h3 = (x[1] for x in w)
                if not (h1v < h2 < h3 and l0 < l1 < l2):
                    continue
                struct = l2; start = w[5][0] + prd + 1; bos = None
                for j in range(start, min(start + bos_win, n - 1)):
                    if bars[j]['h'] > h3: break
                    if bars[j]['c'] < struct: bos = j; break
                if bos is None: continue
                ei = None
                for j in range(bos + 1, min(bos + 1 + retest_win, n - 1)):
                    if bars[j]['c'] > h3: break
                    if bars[j]['h'] >= struct and bars[j]['c'] < struct: ei = j + 1; break
                if ei is None or ei <= last or ei >= n: continue
                entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
                stop = max(bars[ei-1]['h'], struct) + buf * a; d = 'bear'
            elif kinds == ('H', 'L', 'H', 'L', 'H', 'L'):
                h0, l1, h1b, l2b, h2b, l3 = (x[1] for x in w)
                if not (l1 > l2b > l3 and h0 > h1b > h2b):
                    continue
                struct = h2b; start = w[5][0] + prd + 1; bos = None
                for j in range(start, min(start + bos_win, n - 1)):
                    if bars[j]['l'] < l3: break
                    if bars[j]['c'] > struct: bos = j; break
                if bos is None: continue
                ei = None
                for j in range(bos + 1, min(bos + 1 + retest_win, n - 1)):
                    if bars[j]['c'] < l3: break
                    if bars[j]['l'] <= struct and bars[j]['c'] > struct: ei = j + 1; break
                if ei is None or ei <= last or ei >= n: continue
                entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
                stop = min(bars[ei-1]['l'], struct) - buf * a; d = 'bull'
            else:
                continue
            if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
                continue
            o = walk(bars, ei, entry, stop, d, RR, HOLD4)
            if o is not None:
                rows.append((bars[ei]['_ts'], o - cost(o, entry, abs(entry-stop))))
            last = ei + cooldown
    rows.sort()
    return rows


def main():
    base = collect()
    seq = [r for _, r in base]; n, w, e = agg(seq)
    print(f"BASE comm 4h RR2: n={n} WR={w:.1f}% exp={e:+.3f}R\n")

    print("1) 6-fold walk-forward:")
    k = 6; sz = len(base)//k; p = 0
    for f in range(k):
        lo = f*sz; hi = (f+1)*sz if f < k-1 else len(base)
        fn, fw, fe = agg([r for _, r in base[lo:hi]]); ok = fe > 0; p += ok
        print(f"   fold {f+1}: n={fn:>2} WR={fw:>5.1f}% exp={fe:>+7.3f}R {'ok' if ok else 'NEG'}")
    print(f"   -> {p}/{k} folds positive\n")

    print("2) parameter sensitivity:")
    for lbl, kw in [('prd=4', dict(prd=4)), ('bos_win=20', dict(bos_win=20)),
                    ('bos_win=40', dict(bos_win=40)), ('retest=15', dict(retest_win=15)),
                    ('retest=25', dict(retest_win=25)), ('buf=0.5', dict(buf=0.5))]:
        rows = collect(**kw); s = [r for _, r in rows]; nn, ww, ee = agg(s)
        print(f"   {lbl:<12} n={nn:>3} WR={ww:>5.1f}% exp={ee:>+7.3f}R")


if __name__ == '__main__':
    main()
