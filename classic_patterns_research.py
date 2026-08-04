"""Classic chart patterns vs the book's claim (Ch1 Author's Note, p.30).

The claim under test: head & shoulders (+ inverse), ascending/descending triangles, and
double tops/bottoms "have historically shown success rates of somewhere between 80 and
83%... at least 75 out of every hundred high-quality setups you trade will hit their
one-to-one target."

Mechanical, causal encoding of each pattern from swing pivots; entry at the confirmation
break (MARKET, break-bar close); structural stop; test primarily at RR 1:1 (the literal
claim) and RR 2:1 for context. Realistic dealing cost, chronological OOS split (both
halves positive + n>=40 = PASS), per class. 4h and daily (the classic swing-pattern
timeframes); h1 for sample size.

Honest caveat: 'high-quality' pattern selection is discretionary in the book; a mechanical
detector is the fairest *systematic* proxy and a lower bound on hand-picked quality. But
if the 75%-at-1:1 claim needs discretionary cherry-picking to hold, that is exactly the
thing worth knowing before print.

Run: python classic_patterns_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost, agg4h

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
W = 3                 # pivot half-window (7-bar swing pivot)
PEAK_TOL = 0.8        # two peaks/troughs 'equal' if within PEAK_TOL * ATR
MIN_H = 1.5           # pattern height must be >= MIN_H * ATR (a real, tradeable structure)
MAX_SPAN = 60         # bars between the two defining pivots (coherent pattern)
MIN_SPAN = 4
BUF = 0.10            # structural stop buffer (ATR)
CONF = 20             # bars after the pattern completes to see the confirmation break
HOLD = 120            # bars to reach the target
RRS = [1.0, 2.0]


def pivots(bars):
    hi, lo = [], []
    for i in range(W, len(bars) - W):
        seg = bars[i - W:i + W + 1]
        if bars[i]['h'] == max(b['h'] for b in seg): hi.append(i)
        if bars[i]['l'] == min(b['l'] for b in seg): lo.append(i)
    return hi, lo


def scan(bars):
    """Return signal dicts: {pat, dir, ti (trigger bar), entry, stop}."""
    n = len(bars); out = []; H, L = pivots(bars)
    Hs = set(H); Ls = set(L)

    def a_at(i): return atr(bars, 14, i) or 0.0

    # ---- double top / bottom: two equal peaks (troughs) around a valley (peak) ----
    for k in range(1, len(H)):
        p1, p2 = H[k - 1], H[k]
        if not (MIN_SPAN <= p2 - p1 <= MAX_SPAN): continue
        a = a_at(p2)
        if a <= 0: continue
        if abs(bars[p1]['h'] - bars[p2]['h']) > PEAK_TOL * a: continue
        mids = [j for j in L if p1 < j < p2]
        if not mids: continue
        neck = min(bars[j]['l'] for j in mids)
        top = max(bars[p1]['h'], bars[p2]['h'])
        if top - neck < MIN_H * a: continue
        for t in range(p2 + 1, min(p2 + 1 + CONF, n)):
            if bars[t]['c'] < neck:
                out.append({'pat': 'double_top', 'dir': 'bear', 'ti': t,
                            'entry': bars[t]['c'], 'stop': top + BUF * a}); break
    for k in range(1, len(L)):
        p1, p2 = L[k - 1], L[k]
        if not (MIN_SPAN <= p2 - p1 <= MAX_SPAN): continue
        a = a_at(p2)
        if a <= 0: continue
        if abs(bars[p1]['l'] - bars[p2]['l']) > PEAK_TOL * a: continue
        mids = [j for j in H if p1 < j < p2]
        if not mids: continue
        neck = max(bars[j]['h'] for j in mids)
        bot = min(bars[p1]['l'], bars[p2]['l'])
        if neck - bot < MIN_H * a: continue
        for t in range(p2 + 1, min(p2 + 1 + CONF, n)):
            if bars[t]['c'] > neck:
                out.append({'pat': 'double_bot', 'dir': 'bull', 'ti': t,
                            'entry': bars[t]['c'], 'stop': bot - BUF * a}); break

    # ---- head & shoulders / inverse: 3 peaks, middle highest, shoulders ~equal ----
    for k in range(2, len(H)):
        ls, hd, rs = H[k - 2], H[k - 1], H[k]
        if not (MIN_SPAN <= rs - ls <= 2 * MAX_SPAN): continue
        a = a_at(rs)
        if a <= 0: continue
        if not (bars[hd]['h'] > bars[ls]['h'] and bars[hd]['h'] > bars[rs]['h']): continue
        if abs(bars[ls]['h'] - bars[rs]['h']) > PEAK_TOL * a: continue
        necks = [j for j in L if ls < j < rs]
        if len(necks) < 1: continue
        neck = min(bars[j]['l'] for j in necks)
        if bars[hd]['h'] - neck < MIN_H * a: continue
        for t in range(rs + 1, min(rs + 1 + CONF, n)):
            if bars[t]['c'] < neck:
                out.append({'pat': 'hns', 'dir': 'bear', 'ti': t,
                            'entry': bars[t]['c'], 'stop': bars[rs]['h'] + BUF * a}); break
    for k in range(2, len(L)):
        ls, hd, rs = L[k - 2], L[k - 1], L[k]
        if not (MIN_SPAN <= rs - ls <= 2 * MAX_SPAN): continue
        a = a_at(rs)
        if a <= 0: continue
        if not (bars[hd]['l'] < bars[ls]['l'] and bars[hd]['l'] < bars[rs]['l']): continue
        if abs(bars[ls]['l'] - bars[rs]['l']) > PEAK_TOL * a: continue
        necks = [j for j in H if ls < j < rs]
        if len(necks) < 1: continue
        neck = max(bars[j]['h'] for j in necks)
        if neck - bars[hd]['l'] < MIN_H * a: continue
        for t in range(rs + 1, min(rs + 1 + CONF, n)):
            if bars[t]['c'] > neck:
                out.append({'pat': 'inv_hns', 'dir': 'bull', 'ti': t,
                            'entry': bars[t]['c'], 'stop': bars[rs]['l'] - BUF * a}); break

    # ---- ascending / descending triangles ----
    for k in range(1, len(H)):
        h1, h2 = H[k - 1], H[k]
        if not (MIN_SPAN <= h2 - h1 <= MAX_SPAN): continue
        a = a_at(h2)
        if a <= 0: continue
        if abs(bars[h1]['h'] - bars[h2]['h']) > PEAK_TOL * a: continue      # flat resistance
        lows = [j for j in L if h1 < j < h2]
        if len(lows) < 2: continue
        if not (bars[lows[-1]]['l'] > bars[lows[0]]['l'] + 0.3 * a): continue  # rising lows
        res = max(bars[h1]['h'], bars[h2]['h'])
        if res - bars[lows[0]]['l'] < MIN_H * a: continue
        for t in range(h2 + 1, min(h2 + 1 + CONF, n)):
            if bars[t]['c'] > res:
                out.append({'pat': 'asc_tri', 'dir': 'bull', 'ti': t,
                            'entry': bars[t]['c'], 'stop': bars[lows[-1]]['l'] - BUF * a}); break
    for k in range(1, len(L)):
        l1, l2 = L[k - 1], L[k]
        if not (MIN_SPAN <= l2 - l1 <= MAX_SPAN): continue
        a = a_at(l2)
        if a <= 0: continue
        if abs(bars[l1]['l'] - bars[l2]['l']) > PEAK_TOL * a: continue      # flat support
        highs = [j for j in H if l1 < j < l2]
        if len(highs) < 2: continue
        if not (bars[highs[-1]]['h'] < bars[highs[0]]['h'] - 0.3 * a): continue  # falling highs
        sup = min(bars[l1]['l'], bars[l2]['l'])
        if bars[highs[0]]['h'] - sup < MIN_H * a: continue
        for t in range(l2 + 1, min(l2 + 1 + CONF, n)):
            if bars[t]['c'] < sup:
                out.append({'pat': 'desc_tri', 'dir': 'bear', 'ti': t,
                            'entry': bars[t]['c'], 'stop': bars[highs[-1]]['h'] + BUF * a}); break
    return out


def walk(bars, ti, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0: return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(ti + 1, min(ti + 1 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def line(label, rows, rr, breakeven):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    flag = '  <-- claim: >=75% WR' if (rr == 1.0 and label == 'ALL') else ''
    print(f"    {label:<10} RR{rr:g} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}{flag}")


def run(tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    # store[pat][rr] and store['ALL'][rr]
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 500: continue
        bars = agg4h(h1) if tf == '4h' else (_bars_norm(pairs[pk].get('daily', [])) if tf == 'daily' else h1)
        if len(bars) < 200: continue
        npr += 1
        for s in scan(bars):
            for rr in RRS:
                o = walk(bars, s['ti'], s['entry'], s['stop'], s['dir'], rr)
                if o is not None:
                    net = o - cost(o, s['entry'], abs(s['entry'] - s['stop']))
                    store[s['pat']][rr].append((bars[s['ti']]['_ts'], net))
                    store['ALL'][rr].append((bars[s['ti']]['_ts'], net))
    print(f"\n===== classic patterns · {tf} — {npr} pairs =====")
    for pat in ['double_top', 'double_bot', 'hns', 'inv_hns', 'asc_tri', 'desc_tri', 'ALL']:
        for rr in RRS:
            if store[pat][rr]:
                line(pat, store[pat][rr], rr, 1.0 / (1.0 + rr))


def main():
    print("=" * 92)
    print("Classic chart patterns vs the book's 80-83% / '75% hit 1:1' claim — realistic fills")
    print("=" * 92)
    print("Breakeven WR after cost: ~50% at RR1, ~33% at RR2. The claim is >=75% at 1:1.")
    for tf in ('daily', '4h', 'h1'):
        run(tf)


if __name__ == '__main__':
    main()
