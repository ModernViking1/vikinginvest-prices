"""Stress-test the ICT BOS+FVG cells that passed the first screen (crypto m15/4h,
comm h1). 6-fold walk-forward + parameter sensitivity — the bar the real edges
cleared. If a cell only works at one parameter setting or fails walk-forward, it's
noise, not an edge.
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
ATR_BUF = 0.25
COOLDOWN = 5
HOLD = {'m15': 60, 'h1': 48, '4h': 60}


def struct(bars, prd):
    n = len(bars)
    pl = sorted((i + prd, bars[i]['l']) for i in range(prd, n - prd)
                if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, prd+1)))
    ph = sorted((i + prd, bars[i]['h']) for i in range(prd, n - prd)
                if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, prd+1)))
    last_pl = [None]*n; last_ph = [None]*n; a = 0; b = 0
    for i in range(n):
        while a < len(pl) and pl[a][0] <= i:
            last_pl[i] = pl[a][1]; a += 1
        if i and last_pl[i] is None:
            last_pl[i] = last_pl[i-1]
        while b < len(ph) and ph[b][0] <= i:
            last_ph[i] = ph[b][1]; b += 1
        if i and last_ph[i] is None:
            last_ph[i] = last_ph[i-1]
    bdir = [None]*n; bbar = [-10**9]*n; cd, cb = None, -10**9
    for i in range(n):
        if last_pl[i] is not None and bars[i]['c'] < last_pl[i]:
            cd, cb = 'bear', i
        if last_ph[i] is not None and bars[i]['c'] > last_ph[i]:
            cd, cb = 'bull', i
        bdir[i], bbar[i] = cd, cb
    return bdir, bbar


def collect(cls_filter, tf, prd=3, bos_rec=12, retr=20):
    d = json.load(open(HIST)); pairs = d['pairs']; rows = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        if PAIR_CLASS.get(pk) != cls_filter:
            continue
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        bars = {'m15': m15, 'h1': h1, '4h': agg4h(h1)}[tf]
        if len(bars) < 150:
            continue
        n = len(bars); bdir, bbar = struct(bars, prd); last = -1
        for i in range(prd + 2, n - 1):
            if i <= last:
                continue
            d0 = bdir[i]
            if d0 is None or (i - bbar[i]) > bos_rec or i - 2 < bbar[i]:
                continue
            if d0 == 'bear':
                if not (bars[i-2]['l'] > bars[i]['h']):
                    continue
                z_bot, z_top = bars[i]['h'], bars[i-2]['l']
            else:
                if not (bars[i-2]['h'] < bars[i]['l']):
                    continue
                z_bot, z_top = bars[i-2]['h'], bars[i]['l']
            ei = None
            for r in range(i + 1, min(i + 1 + retr, n - 1)):
                bb = bars[r]
                if d0 == 'bear':
                    if bb['c'] > z_top:
                        break
                    if bb['h'] >= z_bot:
                        ei = r + 1; break
                else:
                    if bb['c'] < z_bot:
                        break
                    if bb['l'] <= z_top:
                        ei = r + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
            if d0 == 'bear':
                stop = z_top + ATR_BUF * a
                if stop <= entry:
                    continue
            else:
                stop = z_bot - ATR_BUF * a
                if stop >= entry:
                    continue
            o = walk(bars, ei, entry, stop, d0, RR, HOLD[tf])
            if o is not None:
                rows.append((bars[ei]['_ts'], o - cost(o, entry, abs(entry-stop))))
            last = ei + COOLDOWN
    rows.sort()
    return rows


def rep(label, rows):
    seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    print(f"  {label:<24} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}]")


def folds(rows, k=6):
    sz = len(rows)//k; p = 0; out = []
    for f in range(k):
        lo = f*sz; hi = (f+1)*sz if f < k-1 else len(rows)
        fn, fw, fe = agg([r for _, r in rows[lo:hi]]); ok = fe > 0; p += ok
        out.append(f"f{f+1}:{fe:+.2f}{'ok' if ok else 'NEG'}")
    return p, out


def main():
    print("1) Base cells (PRD3/BOS12/RETR20, RR2):")
    for c, tf in [('crypto', '4h'), ('crypto', 'm15'), ('comm', 'h1'), ('major', '4h')]:
        rep(f"{c} {tf}", collect(c, tf))

    print("\n2) Walk-forward:")
    for c, tf in [('crypto', '4h'), ('crypto', 'm15'), ('comm', 'h1')]:
        p, o = folds(collect(c, tf))
        print(f"  {c} {tf}: {p}/6  [{' '.join(o)}]")

    print("\n3) Parameter sensitivity — crypto 4h:")
    for lbl, kw in [('prd=2', dict(prd=2)), ('prd=4', dict(prd=4)),
                    ('bos_rec=8', dict(bos_rec=8)), ('bos_rec=16', dict(bos_rec=16)),
                    ('retr=15', dict(retr=15)), ('retr=30', dict(retr=30))]:
        rep(f"crypto 4h {lbl}", collect('crypto', '4h', **kw))

    print("\n4) Parameter sensitivity — crypto m15:")
    for lbl, kw in [('prd=2', dict(prd=2)), ('prd=4', dict(prd=4)),
                    ('bos_rec=8', dict(bos_rec=8)), ('retr=30', dict(retr=30))]:
        rep(f"crypto m15 {lbl}", collect('crypto', 'm15', **kw))


if __name__ == '__main__':
    main()
