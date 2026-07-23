"""'John Wick' box strategy (user screenshots + description).

Mark the PREVIOUS 1H bar's high/low as a box, projected onto the lower timeframe.
When price reaches the BOTTOM (or TOP) of the box, wait for a breakout on the
lower TF followed by a RETRACEMENT, then enter the reversal. Stop OUTSIDE the box.

Encoding: box = previous completed H1 bar [low, high] (no lookahead — prior bar is
closed). Trigger/entry on m15 (our finest data; the source uses 5m). LONG at the
box bottom: m15 tags the lower zone, a bar closes above the recent micro swing high
(breakout), a later bar pulls back (retracement), enter next bar; stop below the
box; SHORT mirror at the top. Targets: opposite box side (structural) and fixed
1:1 / 2:1. Pivots confirmed k bars out, fixed cost, both-OOS-halves gate, per class.

Run: python john_wick_box_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PIV_K = 2
TAG_BAND = 0.20      # "at the bottom/top" = within this fraction of box height of the edge
BRK_WIN = 8          # m15 bars after the tag to get the breakout
RETR_WIN = 6         # bars after breakout to get the retracement
BUF = 0.10
COOLDOWN = 6
HOLD = 48
RRS = [1.0, 2.0]     # plus structural (opposite box edge)


def pivots(bars, k):
    n = len(bars); ph = [None]*n; pl = [None]*n
    for i in range(k, n-k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)): ph[i] = h
        if all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)): pl[i] = l
    return ph, pl


def walk(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    rr = abs(target - entry)/R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return (-1.0, rr)
            if b['h'] >= target: return (rr, rr)
        else:
            if b['h'] >= stop: return (-1.0, rr)
            if b['l'] <= target: return (rr, rr)
    return None


def scan(m15, h1, store, cls, store_cls):
    ph, pl = pivots(m15, PIV_K); n = len(m15)
    t1 = [b['_ts'] for b in h1]
    lastPH = [None]*n; lastPL = [None]*n; ch = cl = None
    for i in range(n):
        if i-PIV_K >= 0 and ph[i-PIV_K] is not None: ch = ph[i-PIV_K]
        if i-PIV_K >= 0 and pl[i-PIV_K] is not None: cl = pl[i-PIV_K]
        lastPH[i] = ch; lastPL[i] = cl
    last = -1; i = PIV_K + 2
    while i < n - 1:
        if i <= last:
            i += 1; continue
        k = bisect.bisect_right(t1, m15[i]['_ts']) - 1        # current (in-progress) H1 bar
        if k < 1:
            i += 1; continue
        BL = h1[k-1]['l']; BH = h1[k-1]['h']; H = BH - BL      # previous H1 bar = the box
        if H <= 0:
            i += 1; continue
        a = atr(m15, 14, i) or 0.0
        if a <= 0:
            i += 1; continue
        made = False
        # LONG at bottom of box
        if m15[i]['l'] <= BL + TAG_BAND*H and m15[i]['l'] >= BL - H:
            for j in range(i+1, min(i+1+BRK_WIN, n-1)):
                if lastPH[j] is not None and m15[j]['c'] > lastPH[j]:      # breakout up
                    brk = m15[j]['c']
                    for r in range(j+1, min(j+1+RETR_WIN, n-1)):
                        if m15[r]['l'] < brk:                              # retracement
                            ei = r+1; entry = m15[ei]['o']; stop = BL - BUF*a
                            if stop < entry:
                                _emit(m15, ei, entry, stop, BH, 'bull', store, cls, store_cls)
                            last = ei + COOLDOWN; i = last + 1; made = True
                            break
                    break
        # SHORT at top of box
        if not made and m15[i]['h'] >= BH - TAG_BAND*H and m15[i]['h'] <= BH + H:
            for j in range(i+1, min(i+1+BRK_WIN, n-1)):
                if lastPL[j] is not None and m15[j]['c'] < lastPL[j]:
                    brk = m15[j]['c']
                    for r in range(j+1, min(j+1+RETR_WIN, n-1)):
                        if m15[r]['h'] > brk:
                            ei = r+1; entry = m15[ei]['o']; stop = BH + BUF*a
                            if stop > entry:
                                _emit(m15, ei, entry, stop, BL, 'bear', store, cls, store_cls)
                            last = ei + COOLDOWN; i = last + 1; made = True
                            break
                    break
        if not made:
            i += 1


def _emit(bars, ei, entry, stop, box_far, d, store, cls, store_cls):
    R = abs(entry - stop); ts = bars[ei]['_ts']
    # structural target = opposite box edge
    res = walk(bars, ei, entry, stop, box_far, d, HOLD)
    if res is not None:
        o, _ = res; net = o - cost(o, entry, R)
        store[('struct',)].append((ts, net)); store_cls[cls][('struct',)].append((ts, net))
    for rr in RRS:
        tgt = entry + rr*R if d == 'bull' else entry - rr*R
        res = walk(bars, ei, entry, stop, tgt, d, HOLD)
        if res is not None:
            o, _ = res; net = o - cost(o, entry, R)
            store[(rr,)].append((ts, net)); store_cls[cls][(rr,)].append((ts, net))


def line(label, rows, be=None):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    betxt = f" (be {be:.0f}%)" if be else ""
    print(f"  {label:<16} n={n:>4} WR={w:>5.1f}%{betxt} exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        m15 = _bars_norm(pairs[pk].get('m15', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(m15) < 1000 or len(h1) < 200:
            continue
        npairs += 1
        scan(m15, h1, store, cls, store_cls)

    print(f"John Wick box (prev-H1 box -> m15 breakout+retrace at extremes) — {npairs} pairs\n")
    line("struct (opp edge)", store[('struct',)])
    line("RR1.0", store[(1.0,)], 50)
    line("RR2.0", store[(2.0,)], 33)
    print("\nper class (RR1.0):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c][(1.0,)], 50)
    print("\nper class (struct / opposite box edge):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c][('struct',)])


if __name__ == '__main__':
    main()
