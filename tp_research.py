"""Disciplined test of the TRENDLINE patterns from the AI-firm table: ascending /
descending / symmetric triangles, rising / falling wedges, channels up/down,
flags & pennants. Fit an upper trendline to swing highs and a lower to swing
lows in a rolling window, classify by slopes, trade the BREAKOUT (close beyond a
boundary -> enter next bar), stop at the opposite boundary. Realistic cost,
WR+EV+RR, chronological OOS split. Same rigor bar as everything else.
"""
import json
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, _min_prom

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
PIVOT_W = 3
WIN = 48               # rolling window (~2 days on h1)
MAX_HOLD = 96
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RRS = [1.0, 2.0]
FLAT = 0.02 / 100      # |rel slope per bar| below this = "flat"
IMPULSE_WIN = 24       # look-back for flag/pennant impulse


def pivots(bars, lo, hi):
    piv = []
    for i in range(max(PIVOT_W, lo), min(hi, len(bars) - PIVOT_W)):
        win = bars[i - PIVOT_W:i + PIVOT_W + 1]
        h, l, mp = bars[i]['h'], bars[i]['l'], _min_prom(bars[i]['c'])
        if h >= max(b['h'] for b in win) and (h - min(b['l'] for b in win)) >= mp:
            piv.append((i, h, 'H'))
        elif l <= min(b['l'] for b in win) and (max(b['h'] for b in win) - l) >= mp:
            piv.append((i, l, 'L'))
    return piv


def fit(pts):
    n = len(pts)
    if n < 2: return None
    sx = sum(p[0] for p in pts); sy = sum(p[1] for p in pts)
    sxx = sum(p[0]**2 for p in pts); sxy = sum(p[0]*p[1] for p in pts)
    den = n*sxx - sx*sx
    if den == 0: return None
    m = (n*sxy - sx*sy) / den; b = (sy - m*sx) / n
    return m, b


def classify(sh, sl, price):
    rh, rl = sh/price, sl/price
    conv = abs(sh) > abs(sl) if (rh > 0 and rl > 0) else (abs(sh) > abs(sl))
    if abs(rh) < FLAT and rl > FLAT: return 'asc_triangle'
    if abs(rl) < FLAT and rh < -FLAT: return 'desc_triangle'
    if rh < -FLAT and rl > FLAT: return 'sym_triangle'
    if rh > FLAT and rl > FLAT:
        return 'rising_wedge' if abs(rh - rl) > FLAT else 'channel_up'
    if rh < -FLAT and rl < -FLAT:
        return 'falling_wedge' if abs(rh - rl) > FLAT else 'channel_down'
    return None


def walk(h1, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0: return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + MAX_HOLD, len(h1))):
        b = h1[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def cost(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def scan(h1):
    trades = []; i = WIN; last = -1
    while i < len(h1) - 1:
        if i <= last:
            i += 1; continue
        piv = pivots(h1, i - WIN, i)
        H = [(p[0], p[1]) for p in piv if p[2] == 'H']
        L = [(p[0], p[1]) for p in piv if p[2] == 'L']
        if len(H) < 2 or len(L) < 2:
            i += 1; continue
        fh, fl = fit(H[-3:]), fit(L[-3:])
        if not fh or not fl:
            i += 1; continue
        upper = fh[0]*i + fh[1]; lower = fl[0]*i + fl[1]
        price = h1[i]['c']
        if upper <= lower:
            i += 1; continue
        pat = classify(fh[0], fl[0], price)
        if pat is None:
            i += 1; continue
        c = h1[i]['c']; d = None
        if c > upper and (c - upper) >= _min_prom(c): d = 'bull'
        elif c < lower and (lower - c) >= _min_prom(c): d = 'bear'
        if d is None:
            i += 1; continue
        if i + 1 >= len(h1): break
        entry = h1[i+1]['o']
        stop = lower if d == 'bull' else upper
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            i += 1; continue
        trades.append({'pat': pat, 'i0': i+1, 'ts': h1[i+1]['_ts'], 'entry': entry, 'stop': stop, 'dir': d})
        last = i + WIN // 2   # avoid overlapping re-detections
        i += 1
    return trades


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    trades = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 400: continue
        for tr in scan(h1):
            R = abs(tr['entry'] - tr['stop'])
            tr['res'] = {}
            for rr in RRS:
                o = walk(h1, tr['i0'], tr['entry'], tr['stop'], tr['dir'], rr)
                tr['res'][rr] = (o - cost(o, tr['entry'], R)) if o is not None else None
            trades.append(tr)
    trades.sort(key=lambda t: t['ts'])
    byp = defaultdict(list)
    for t in trades: byp[t['pat']].append(t)
    print(f"trendline breakout trades: {len(trades)}\n")
    print(f"{'pattern':<15} {'n':>5} | {'1:1 WR/exp':>16} | {'1:2 WR/exp':>16} | {'OOS 1:2 (h1/h2 exp)':>22}")
    for pat in sorted(byp):
        rows = byp[pat]
        n1, w1, e1 = agg([t['res'][1.0] for t in rows])
        n2, w2, e2 = agg([t['res'][2.0] for t in rows])
        mid = len(rows)//2
        _, _, eh = agg([t['res'][2.0] for t in rows[:mid]])
        _, _, es = agg([t['res'][2.0] for t in rows[mid:]])
        oos = 'PASS' if (eh > 0 and es > 0) else 'fail'
        print(f"{pat:<15} {n1:>5} | {w1:>5.1f}% {e1:>+8.3f} | {w2:>5.1f}% {e2:>+8.3f} | {eh:>+7.3f}/{es:>+7.3f} {oos}")


if __name__ == '__main__':
    main()
