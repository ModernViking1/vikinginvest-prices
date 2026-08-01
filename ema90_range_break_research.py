"""90-EMA range-break strategy (TikTok 'reversal vs not' video).

Price trends along the 90 EMA; when it consolidates AROUND the EMA it forms a range (the
recent high/low 'marked off the 90 ma' = the red box). Trade the BREAK of that box:
close above the box high -> long; close below the box low -> short. Stop beyond the
opposite side of the box, target from 2:1.

Encoding: 90 EMA on the trading TF; box = highest-high / lowest-low over the last BOX_LOOK
bars; require the EMA to sit INSIDE the box (price consolidating around it) and the box to
be a tight consolidation (height <= BOX_MAXATR*ATR). Entry on the first close beyond the
box; stop = opposite side + buffer; RR 2/3 (and 1.5). MARKET fills, dealing cost,
chronological OOS split, per class, m15 / h1 / 4h.

Run: python ema90_range_break_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
EMA_P = 90
BOX_LOOK = 12          # recent-range lookback (bars) for the box
BOX_MAXATR = 3.0       # box height <= this * ATR (a real consolidation, not a trend leg)
BUF = 0.10             # stop buffer beyond the box (ATR)
COOLDOWN = 5
HOLD = 80
RRS = [1.5, 2.0, 3.0]


def ema(vals, p):
    k = 2.0 / (p + 1); out = [None] * len(vals); e = None
    for i, v in enumerate(vals):
        e = v if e is None else v * k + e * (1 - k); out[i] = e
    return out


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def walk(bars, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(bars, store, cls):
    e = ema([b['c'] for b in bars], EMA_P); n = len(bars); last = -1
    for i in range(EMA_P + BOX_LOOK, n - 1):
        if i <= last or e[i] is None:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        seg = bars[i - BOX_LOOK:i]
        box_hi = max(x['h'] for x in seg); box_lo = min(x['l'] for x in seg)
        if not (box_lo <= e[i] <= box_hi):          # EMA must sit inside the range
            continue
        if (box_hi - box_lo) > BOX_MAXATR * a:       # must be a tight consolidation
            continue
        c = bars[i]['c']; d = None
        if c > box_hi:
            d = 'bull'; entry = c; stop = box_lo - BUF * a
        elif c < box_lo:
            d = 'bear'; entry = c; stop = box_hi + BUF * a
        if not d:
            continue
        ts = bars[i]['_ts']
        for rr in RRS:
            o = walk(bars, i + 1, entry, stop, d, rr)
            if o is not None:
                store[cls][rr].append((ts, o - cost(o, entry, abs(entry - stop))))
        last = i + COOLDOWN


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<8} RR{rr} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        if tf == '4h':
            h1 = _bars_norm(pairs[pk].get('h1', []))
            if len(h1) < 400: continue
            bars = make_4h(h1)
        else:
            bars = _bars_norm(pairs[pk].get(tf, []))
        if len(bars) < 300: continue
        npr += 1
        scan(bars, store, cls)
    print(f"\n===== 90-EMA range break · {tf} — {npr} pairs =====")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        for rr in RRS:
            line(c, store[c][rr], rr)
    for rr in RRS:
        line('ALL', [r for c in store for r in store[c][rr]], rr)


def main():
    print("=" * 88)
    print("90-EMA range-break (consolidation at the 90 EMA, break either way) — all pairs/TFs")
    print("=" * 88)
    for tf in ('m15', 'h1', '4h'):
        run(tf)


if __name__ == '__main__':
    main()
