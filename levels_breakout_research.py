"""Simple level strategies — horizontal S/R break-and-retest, and Fib-level continuation.

Two 'trade to the level' mechanics from the screenshots:
  A. BREAK-AND-RETEST (S/R role reversal): break a horizontal swing level (resistance),
     retest it as support, continue in the break direction. Mirror for support -> short.
  B. FIB CONTINUATION: after an impulse leg, enter in the impulse direction when price
     retraces into the 38.2-61.8 zone (classic Fibonacci retracement entry).

Both are CONTINUATION entries at a level (the direction that has held up all session),
tested universe-wide to see whether the simple, unscoped versions clear the bar or only
survive in the already-wired scoped cells (gbreak/gfib/fib_gz/tl_nowick).

Realistic fills (market, next-bar), dealing cost, fixed-RR grid, chronological OOS split
(both halves +), per class, h1 + 4h.

Run: python levels_breakout_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
FRAC = 3
RRS = [1.0, 1.5, 2.0, 3.0]
BR_BUF = 0.10          # breakout buffer beyond the level (ATR)
BR_WINDOW = 30         # bars to find the retest after the break
BR_TOL = 0.30          # retest proximity to the level (ATR)
STOP_BUF = 0.30        # stop buffer beyond the level / swing (ATR)
IMP = 1.2              # min impulse-leg size (ATR) for the Fib variant
FIB_WINDOW = 40


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
    hold = int(30 + 30 * rr)
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def _emit(bars, ei, entry, stop, d, sc, cls, sp, pk):
    if ei >= len(bars):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']
    for rr in RRS:
        o = walk(bars, ei, entry, stop, d, rr)
        if o is not None:
            net = o - cost(o, entry, R)
            sc[cls][rr].append((ts, net)); sp[pk][rr].append((ts, net))


def scan_breakretest(bars, sc, cls, sp, pk):
    hi, lo = pivots(bars, FRAC); n = len(bars)
    # resistance breaks (swing highs) -> long continuation on the retest
    for p in hi:
        lvl = bars[p]['h']; a = atr(bars, 14, p) or 0.0
        if a <= 0:
            continue
        brk = None
        for j in range(p + FRAC + 1, min(p + FRAC + 1 + BR_WINDOW, n - 1)):
            if bars[j]['c'] > lvl + BR_BUF * a:
                brk = j; break
        if brk is None:
            continue
        for j in range(brk + 1, min(brk + 1 + BR_WINDOW, n - 1)):
            b = bars[j]
            if b['c'] < lvl - BR_BUF * a:      # fell back through -> failed break
                break
            if b['l'] <= lvl + BR_TOL * a and b['c'] > lvl:   # retest held
                entry = b['c']; stop = min(b['l'], lvl) - STOP_BUF * a
                if stop < entry:
                    _emit(bars, j + 1, entry, stop, 'bull', sc, cls, sp, pk)
                break
    # support breaks (swing lows) -> short continuation on the retest
    for p in lo:
        lvl = bars[p]['l']; a = atr(bars, 14, p) or 0.0
        if a <= 0:
            continue
        brk = None
        for j in range(p + FRAC + 1, min(p + FRAC + 1 + BR_WINDOW, n - 1)):
            if bars[j]['c'] < lvl - BR_BUF * a:
                brk = j; break
        if brk is None:
            continue
        for j in range(brk + 1, min(brk + 1 + BR_WINDOW, n - 1)):
            b = bars[j]
            if b['c'] > lvl + BR_BUF * a:
                break
            if b['h'] >= lvl - BR_TOL * a and b['c'] < lvl:
                entry = b['c']; stop = max(b['h'], lvl) + STOP_BUF * a
                if stop > entry:
                    _emit(bars, j + 1, entry, stop, 'bear', sc, cls, sp, pk)
                break


def scan_fib(bars, sc, cls, sp, pk):
    hi, lo = pivots(bars, FRAC)
    piv = sorted([(i, 'h') for i in hi] + [(i, 'l') for i in lo]); n = len(bars)
    for a_ in range(len(piv) - 1):
        i1, t1 = piv[a_]; i2, t2 = piv[a_ + 1]
        at = atr(bars, 14, i2) or 0.0
        if at <= 0:
            continue
        # up-impulse (low->high): long the retrace into 38.2-61.8
        if t1 == 'l' and t2 == 'h':
            L = bars[i1]['l']; H = bars[i2]['h']; leg = H - L
            if leg < IMP * at:
                continue
            zt = H - 0.382 * leg; zf = H - 0.618 * leg
            for j in range(i2 + 1, min(i2 + 1 + FIB_WINDOW, n - 1)):
                b = bars[j]
                if b['l'] < L:      # broke the origin -> failed
                    break
                if zf <= b['l'] <= zt and b['c'] > b['o']:   # in zone, bullish reaction
                    entry = b['c']; stop = L - STOP_BUF * at
                    if stop < entry:
                        _emit(bars, j + 1, entry, stop, 'bull', sc, cls, sp, pk)
                    break
        # down-impulse (high->low): short the retrace into 38.2-61.8
        elif t1 == 'h' and t2 == 'l':
            H = bars[i1]['h']; L = bars[i2]['l']; leg = H - L
            if leg < IMP * at:
                continue
            zt = L + 0.382 * leg; zf = L + 0.618 * leg
            for j in range(i2 + 1, min(i2 + 1 + FIB_WINDOW, n - 1)):
                b = bars[j]
                if b['h'] > H:
                    break
                if zt <= b['h'] <= zf and b['c'] < b['o']:
                    entry = b['c']; stop = H + STOP_BUF * at
                    if stop > entry:
                        _emit(bars, j + 1, entry, stop, 'bear', sc, cls, sp, pk)
                    break


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<9} RR{rr} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(which, tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    sc = defaultdict(lambda: defaultdict(list)); sp = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        if tf == '4h':
            h1 = _bars_norm(pairs[pk].get('h1', []))
            if len(h1) < 300: continue
            bars = make_4h(h1)
        else:
            bars = _bars_norm(pairs[pk].get(tf, []))
        if len(bars) < 300: continue
        npr += 1
        (scan_breakretest if which == 'br' else scan_fib)(bars, sc, cls, sp, pk)
    title = 'BREAK-AND-RETEST (horizontal S/R)' if which == 'br' else 'FIB CONTINUATION (38.2-61.8)'
    print(f"\n===== {title} · {tf} — {npr} pairs =====")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        for rr in (1.5, 2.0):
            line(c, sc[c][rr], rr)
    for rr in RRS:
        line('ALL', [r for c in sc for r in sc[c][rr]], rr)


def main():
    print("=" * 90)
    print("SIMPLE LEVEL STRATEGIES — break-and-retest + Fib continuation (trade to the level)")
    print("=" * 90)
    for which in ('br', 'fib'):
        for tf in ('h1', '4h'):
            run(which, tf)


if __name__ == '__main__':
    main()
