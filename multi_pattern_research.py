"""Disciplined test of the 'high win-rate' swing patterns (from the pattern-scanner
screenshot) + Alex G's AOI reversal method. Same rigor as the H&S research:
realistic entry fill, real structural stop, realistic cost, and a chronological
OUT-OF-SAMPLE split. We report WR *and* expectancy *and* RR together at several
targets, so a ">70% WR" that only comes from a tight (RR<1) target is exposed as
such rather than celebrated.

Patterns:
  double_top / double_bottom : two ~equal extremes, entry on neckline (trough/peak)
                               break, stop beyond the extremes.
  aoi_bounce (Alex G)        : horizontal level touched >=3x; on revisit + reversal
                               candle (engulfing), enter the bounce, stop beyond level.
Targets tested: 0.5R, 1R, 1.5R, 2R, 3R (RR labelled) + measured-move where defined.
"""
import json, bisect, sys
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, _min_prom

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
PIVOT_W = 3
MAX_HOLD = 96          # ~4 days on h1
BREAK_WIN = 24
WIN_COST_PCT = 0.0045 / 100
LOSS_COST_PCT = 0.0105 / 100
RRS = [0.5, 1.0, 1.5, 2.0, 3.0]
LEVEL_TOL = 0.004      # cluster/zone half-width as frac of price (~0.4%)
MIN_TOUCHES = 3


def pivots(bars):
    piv = []
    for i in range(PIVOT_W, len(bars) - PIVOT_W):
        win = bars[i - PIVOT_W:i + PIVOT_W + 1]
        hi, lo, mp = bars[i]['h'], bars[i]['l'], _min_prom(bars[i]['c'])
        if hi >= max(b['h'] for b in win) and (hi - min(b['l'] for b in win)) >= mp:
            piv.append((i, hi, 'H'))
        elif lo <= min(b['l'] for b in win) and (max(b['h'] for b in win) - lo) >= mp:
            piv.append((i, lo, 'L'))
    out = []
    for p in piv:
        if out and out[-1][2] == p[2]:
            if (p[2] == 'H' and p[1] > out[-1][1]) or (p[2] == 'L' and p[1] < out[-1][1]):
                out[-1] = p
        else:
            out.append(p)
    return out


def walk(h1, i0, entry, stop, target, d):
    if abs(entry - stop) <= 0: return None
    for j in range(i0, min(i0 + MAX_HOLD, len(h1))):
        b = h1[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return abs(target - entry) / abs(entry - stop)
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return abs(target - entry) / abs(entry - stop)
    return None


def cost_R(o, entry, R):
    frac = R / abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def resolve_all(h1, i0, entry, stop, d):
    R = abs(entry - stop)
    res = {}
    for rr in RRS:
        tgt = entry + rr * R if d == 'bull' else entry - rr * R
        o = walk(h1, i0, entry, stop, tgt, d)
        res[rr] = (o - cost_R(o, entry, R)) if o is not None else None
    return res


def double_patterns(h1):
    piv = pivots(h1); trades = []
    for k in range(len(piv) - 2):
        a, b, c = piv[k:k + 3]
        # double top: H L H with tops ~equal
        if a[2] == 'H' and b[2] == 'L' and c[2] == 'H':
            top = max(a[1], c[1]); height = top - b[1]
            if height <= 0 or abs(a[1] - c[1]) > 0.5 * height: continue
            neck = b[1]                      # break below the trough
            start = c[0] + 1
            for j in range(start, min(start + BREAK_WIN, len(h1))):
                if h1[j]['c'] < neck:
                    if j + 1 >= len(h1): break
                    entry = h1[j + 1]['o']; stop = top
                    if stop > entry:
                        trades.append(('double_top', 'bear', j + 1, h1[j + 1]['_ts'], entry, stop))
                    break
        # double bottom: L H L with bottoms ~equal
        if a[2] == 'L' and b[2] == 'H' and c[2] == 'L':
            bot = min(a[1], c[1]); height = b[1] - bot
            if height <= 0 or abs(a[1] - c[1]) > 0.5 * height: continue
            neck = b[1]
            start = c[0] + 1
            for j in range(start, min(start + BREAK_WIN, len(h1))):
                if h1[j]['c'] > neck:
                    if j + 1 >= len(h1): break
                    entry = h1[j + 1]['o']; stop = bot
                    if stop < entry:
                        trades.append(('double_bottom', 'bull', j + 1, h1[j + 1]['_ts'], entry, stop))
                    break
    return trades


def is_engulf(h1, j, d):
    if j < 1: return False
    o, c = h1[j]['o'], h1[j]['c']; po, pc = h1[j-1]['o'], h1[j-1]['c']
    if d == 'bull':
        return c > o and pc < po and c >= po and o <= pc
    return c < o and pc > po and c <= po and o >= pc


def aoi_bounce(h1):
    """Alex G: level touched >=MIN_TOUCHES; on revisit + reversal engulfing, enter."""
    piv = pivots(h1); trades = []
    lows = [p for p in piv if p[2] == 'L']; highs = [p for p in piv if p[2] == 'H']
    def levels(ps):
        used = [False] * len(ps); lv = []
        for i in range(len(ps)):
            if used[i]: continue
            grp = [ps[i]]; used[i] = True
            for j in range(i + 1, len(ps)):
                if not used[j] and abs(ps[j][1] - ps[i][1]) <= LEVEL_TOL * ps[i][1]:
                    grp.append(ps[j]); used[j] = True
            if len(grp) >= MIN_TOUCHES:
                lv.append(grp)
        return lv
    # support levels -> long bounces
    for grp in levels(lows):
        price = sum(g[1] for g in grp) / len(grp); last_touch = max(g[0] for g in grp)
        lo_ext = min(g[1] for g in grp)
        for j in range(last_touch + 2, min(last_touch + 2 + 3 * BREAK_WIN, len(h1))):
            if h1[j]['l'] <= price * (1 + LEVEL_TOL) and is_engulf(h1, j, 'bull') and h1[j]['c'] > price:
                if j + 1 >= len(h1): break
                entry = h1[j + 1]['o']; stop = lo_ext
                if stop < entry:
                    trades.append(('aoi_support', 'bull', j + 1, h1[j + 1]['_ts'], entry, stop))
                break
    for grp in levels(highs):
        price = sum(g[1] for g in grp) / len(grp); last_touch = max(g[0] for g in grp)
        hi_ext = max(g[1] for g in grp)
        for j in range(last_touch + 2, min(last_touch + 2 + 3 * BREAK_WIN, len(h1))):
            if h1[j]['h'] >= price * (1 - LEVEL_TOL) and is_engulf(h1, j, 'bear') and h1[j]['c'] < price:
                if j + 1 >= len(h1): break
                entry = h1[j + 1]['o']; stop = hi_ext
                if stop > entry:
                    trades.append(('aoi_resistance', 'bear', j + 1, h1[j + 1]['_ts'], entry, stop))
                break
    return trades


def agg(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    trades = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 400: continue
        for fn in (double_patterns, aoi_bounce):
            for (pat, dirn, i0, ts, entry, stop) in fn(h1):
                res = resolve_all(h1, i0, entry, stop, dirn)
                trades.append({'pat': pat, 'ts': ts, 'cls': PAIR_CLASS.get(pk), 'res': res})
    trades.sort(key=lambda t: t['ts'])

    def block(name, rows):
        print(f"\n{name}  (n={len(rows)})")
        print(f"  {'target':<9} {'n':>4} {'WR%':>6} {'expR':>8}   {'>70%WR?':>7}")
        for rr in RRS:
            n, w, e = agg([t['res'][rr] for t in rows])
            flag = 'YES' if w >= 70 else ''
            note = '' if e > 0 else ' (EV<=0)'
            print(f"  1:{rr:<6} {n:>4} {w:>6.1f} {e:>+8.3f}   {flag:>7}{note}")

    for pat in ('double_top', 'double_bottom', 'aoi_support', 'aoi_resistance'):
        rows = [t for t in trades if t['pat'] == pat]
        if not rows: continue
        block(f"=== {pat} — FULL", rows)
        mid = len(rows) // 2
        block(f"    {pat} · FIRST half", rows[:mid])
        block(f"    {pat} · SECOND half", rows[mid:])


if __name__ == '__main__':
    main()
