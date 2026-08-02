"""'Test Money Move' (ak.iskander) — imbalance / fair-value-gap retrace continuation.

Four steps: (1) acceptance of price (the origin/impulse), (2) imbalance = a fair-value gap
(3-candle inefficiency left by a strong impulse candle), (3) test = price retraces back into
the gap, (4) money move = ride the continuation in the impulse direction.

Encoding: a bullish FVG is bars[i-1].high < bars[i+1].low with a strong up impulse at bar i
(body >= IMP*ATR = 'acceptance/imbalance'); the gap zone is [bars[i-1].high, bars[i+1].low].
When price later retraces INTO the gap and holds (test), enter in the impulse direction
(MARKET, retrace-bar close), stop just beyond the far edge of the gap (full fill = invalid),
target 2:1 / 3:1 (the money move). Bearish mirror. This is the FVG-continuation family — the
same idea as the wired obfvg (OB+FVG) — tested here universe-wide across timeframes.

Realistic fills, dealing cost, per class, m15 / h1 / 4h, chronological OOS split.

Run: python money_move_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
IMP = 1.0            # impulse (imbalance-creating) candle body >= IMP*ATR
RETR = 24            # bars to wait for the test (retrace into the gap)
BUF = 0.10           # stop buffer beyond the gap far edge (ATR)
COOLDOWN = 3
HOLD = 80
RRS = [2.0, 3.0]


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
    n = len(bars); last = -1
    for i in range(15, n - 2):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        body = bars[i]['c'] - bars[i]['o']
        # bullish FVG: gap between bar[i-1].high and bar[i+1].low, strong up impulse at i
        if body >= IMP * a and bars[i + 1]['l'] > bars[i - 1]['h']:
            g_bot = bars[i - 1]['h']; g_top = bars[i + 1]['l']            # the imbalance zone
            for r in range(i + 2, min(i + 2 + RETR, n - 1)):
                b = bars[r]
                if b['l'] <= g_top and b['c'] > g_bot:                    # tested the gap and held
                    entry = b['c']; stop = g_bot - BUF * a
                    if stop < entry:
                        _emit(bars, r + 1, entry, stop, 'bull', store, cls); last = r + COOLDOWN
                    break
                if b['c'] < g_bot:                                        # gap fully filled -> invalid
                    break
        # bearish FVG: gap between bar[i-1].low and bar[i+1].high, strong down impulse at i
        elif -body >= IMP * a and bars[i + 1]['h'] < bars[i - 1]['l']:
            g_top = bars[i - 1]['l']; g_bot = bars[i + 1]['h']
            for r in range(i + 2, min(i + 2 + RETR, n - 1)):
                b = bars[r]
                if b['h'] >= g_bot and b['c'] < g_top:
                    entry = b['c']; stop = g_top + BUF * a
                    if stop > entry:
                        _emit(bars, r + 1, entry, stop, 'bear', store, cls); last = r + COOLDOWN
                    break
                if b['c'] > g_top:
                    break


def _emit(bars, ei, entry, stop, d, store, cls):
    if ei >= len(bars):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']
    for rr in RRS:
        o = walk(bars, ei, entry, stop, d, rr)
        if o is not None:
            store[cls][rr].append((ts, o - cost(o, entry, R)))


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
    print(f"\n===== money-move (FVG imbalance retrace) · {tf} — {npr} pairs =====")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        for rr in RRS:
            line(c, store[c][rr], rr)
    for rr in RRS:
        line('ALL', [r for c in store for r in store[c][rr]], rr)


def main():
    print("=" * 88)
    print("'Test Money Move' — imbalance (FVG) retrace continuation — all pairs / timeframes")
    print("=" * 88)
    for tf in ('m15', 'h1', '4h'):
        run(tf)


if __name__ == '__main__':
    main()
