"""'One isolated leg' — 50% retracement fade (7AM Research screenshots).

Idea: after an impulse leg (down in the example), mark the 50% retracement. When price
retraces back INTO the 50% level and rejects it, take the small counter-leg — SHORT in
a down-impulse (the bounce fades off 50%), LONG in an up-impulse (mirror). Only the one
small 'isolated leg' is traded; you exit quickly because 'afterwards price can go
further up and down'. Marketed as not needing a high win rate.

That framing (tiny target, quick exit) is exactly where a high hit-rate monetises to
nothing — the low-RR mirage we keep finding — so we sweep the target size and check
whether ANY of them clears a real, OOS-robust edge.

Encoding per timeframe: fractal-pivot impulse legs (size >= IMP*ATR); 50% level; the
first retrace bar that tags 50% and rejects it (closes back through) is the entry
(market, bar close); stop just beyond the rejection extreme; fixed-RR targets swept.
Realistic fills, dealing cost, chronological OOS split (both halves +), per class.

Run: python fib50_leg_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
FRAC = 3                 # pivot fractal half-width
IMP = 1.2                # min impulse-leg size, in ATR
WINDOW = 40              # bars after the leg to find the 50% retrace
TOL = 0.15               # 50% zone half-width, as a fraction of the leg
BUF = 0.25               # stop buffer beyond the rejection extreme, in ATR
RRS = [0.25, 0.5, 1.0, 1.5, 2.0]   # incl. 0.25R (tiny counter-move): WR ~65% vs 80% breakeven — still loses


def pivots(bars, k):
    hi, lo = [], []
    for i in range(k, len(bars) - k):
        seg = bars[i - k:i + k + 1]
        if bars[i]['h'] == max(x['h'] for x in seg): hi.append(i)
        if bars[i]['l'] == min(x['l'] for x in seg): lo.append(i)
    return hi, lo


def walk(bars, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    hold = int(20 + 30 * rr)
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(bars, store_cls, cls, store_pair, pk):
    hi, lo = pivots(bars, FRAC)
    piv = sorted([(i, 'h') for i in hi] + [(i, 'l') for i in lo])
    n = len(bars)
    for a in range(len(piv) - 1):
        i1, t1 = piv[a]; i2, t2 = piv[a + 1]
        a14 = atr(bars, 14, i2) or 0.0
        if a14 <= 0:
            continue
        # DOWN-impulse: pivot high -> pivot low; short the 50% bounce
        if t1 == 'h' and t2 == 'l':
            H = bars[i1]['h']; L = bars[i2]['l']; leg = H - L
            if leg < IMP * a14:
                continue
            fifty = L + 0.5 * leg; band = TOL * leg
            for j in range(i2 + 1, min(i2 + 1 + WINDOW, n - 1)):
                b = bars[j]
                if b['c'] >= fifty + band:      # broke cleanly above 50% -> no fade setup
                    break
                if b['h'] >= fifty and b['c'] < fifty:   # tagged 50% and rejected
                    entry = b['c']; stop = max(b['h'], fifty) + BUF * a14
                    if stop > entry:
                        _emit(bars, j + 1, entry, stop, 'bear', store_cls, cls, store_pair, pk)
                    break
        # UP-impulse: pivot low -> pivot high; long the 50% pullback
        elif t1 == 'l' and t2 == 'h':
            L = bars[i1]['l']; H = bars[i2]['h']; leg = H - L
            if leg < IMP * a14:
                continue
            fifty = H - 0.5 * leg; band = TOL * leg
            for j in range(i2 + 1, min(i2 + 1 + WINDOW, n - 1)):
                b = bars[j]
                if b['c'] <= fifty - band:
                    break
                if b['l'] <= fifty and b['c'] > fifty:
                    entry = b['c']; stop = min(b['l'], fifty) - BUF * a14
                    if stop < entry:
                        _emit(bars, j + 1, entry, stop, 'bull', store_cls, cls, store_pair, pk)
                    break


def _emit(bars, ei, entry, stop, d, store_cls, cls, store_pair, pk):
    if ei >= len(bars):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']
    for rr in RRS:
        o = walk(bars, ei, entry, stop, d, rr)
        if o is not None:
            net = o - cost(o, entry, R)
            store_cls[cls][rr].append((ts, net))
            store_pair[pk][rr].append((ts, net))


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100 / (1 + rr)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<10} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store_cls = defaultdict(lambda: defaultdict(list)); store_pair = defaultdict(lambda: defaultdict(list))
    npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        raw = pairs[pk].get(tf, [])
        if tf == '4h':
            h1 = _bars_norm(pairs[pk].get('h1', []))
            bars = [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
                     'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]
        else:
            bars = _bars_norm(raw)
        if len(bars) < 300:
            continue
        npairs += 1
        scan(bars, store_cls, cls, store_pair, pk)
    print(f"\n===== timeframe {tf} — {npairs} pairs =====")
    print("Per class:")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        print(f"  {c}:")
        for rr in RRS:
            line(f"RR{rr}", store_cls[c][rr], rr)
    # pooled
    print("  ALL pooled:")
    for rr in RRS:
        pooled = [r for c in store_cls for r in store_cls[c][rr]]
        line(f"RR{rr}", pooled, rr)


def main():
    print("=" * 90)
    print("7AM 'one isolated leg' — 50% retracement fade (short down-impulse / long up-impulse)")
    print("=" * 90)
    for tf in ('h1', '4h'):
        run(tf)


if __name__ == '__main__':
    main()
