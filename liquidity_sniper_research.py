"""'Liquidity Sniper' — 4H-open equilibrium + 15m liquidity-sweep + MSS (screenshots).

The OPEN of the current 4-hour candle is the reference level, projected onto 15m.
LONG (buy in discount, below the 4H open): on 15m, price sweeps a recent swing LOW
  below the 4H open (takes liquidity, "$"), then a MARKET STRUCTURE SHIFT up — a
  15m close back above the most recent swing high — triggers the entry. Stop below
  the swept low; target 2:1.
SHORT (mirror, above the 4H open): sweep a swing HIGH above the open, MSS down
  (close below the recent swing low), sell, stop above the swept high, target 2:1.

4H open known at candle open (no lookahead); pivots confirmed k bars out; entry on
the MSS bar close; fixed cost; chronological OOS split (both halves +); per class.
15m data (~64 days). Generic ICT-style price-action, clean-room.

Run: python liquidity_sniper_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PIV_K = 2            # 15m fractal pivot lookback
SWEEP_WIN = 20       # bars after a sweep to get the MSS
BUF = 0.10
COOLDOWN = 6
RR = 2.0
HOLD = 64            # 15m bars to resolve (~16h)


def pivots(bars, k):
    n = len(bars); ph = [None]*n; pl = [None]*n
    for i in range(k, n-k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)):
            ph[i] = h
        if all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)):
            pl[i] = l
    return ph, pl


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop); tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(m15, h1, store, cls, store_cls, store_pair, pk):
    b4 = agg4h(h1)
    op4 = [b['o'] for b in b4]; t4 = [b['_ts'] for b in b4]
    if len(b4) < 5:
        return
    ph, pl = pivots(m15, PIV_K)
    n = len(m15); last = -1
    # last confirmed pivot high/low as of each bar (confirmed PIV_K bars after the pivot)
    lastPH = [None]*n; lastPL = [None]*n; ch = None; cl = None
    for i in range(n):
        if i - PIV_K >= 0 and ph[i-PIV_K] is not None:
            ch = ph[i-PIV_K]
        if i - PIV_K >= 0 and pl[i-PIV_K] is not None:
            cl = pl[i-PIV_K]
        lastPH[i] = ch; lastPL[i] = cl

    def open4(ts):
        k = bisect.bisect_right(t4, ts) - 1
        return op4[k] if k >= 0 else None

    i = PIV_K + 2
    while i < n - 1:
        if i <= last:
            i += 1; continue
        O = open4(m15[i]['_ts'])
        if O is None or lastPL[i] is None or lastPH[i] is None:
            i += 1; continue
        a = atr(m15, 14, i) or 0.0
        if a <= 0:
            i += 1; continue
        made = False
        # LONG: sweep a swing low below the 4H open, then MSS up
        if m15[i]['l'] < lastPL[i] and m15[i]['l'] < O:
            sweep_low = m15[i]['l']; ref_ph = lastPH[i]
            for j in range(i+1, min(i+1+SWEEP_WIN, n)):
                sweep_low = min(sweep_low, m15[j]['l'])
                if m15[j]['c'] > ref_ph:                 # market structure shift up
                    entry = m15[j]['c']; stop = sweep_low - BUF*a
                    if stop < entry:
                        o = walk(m15, j+1, entry, stop, 'bull', RR, HOLD)
                        if o is not None:
                            R = entry-stop; net = o - cost(o, entry, R)
                            store[()].append((m15[j]['_ts'], net)); store_cls[cls][()].append((m15[j]['_ts'], net)); store_pair[pk].append((m15[j]['_ts'], net))
                        last = j + COOLDOWN; i = last + 1; made = True
                    break
        # SHORT: sweep a swing high above the 4H open, then MSS down
        if not made and m15[i]['h'] > lastPH[i] and m15[i]['h'] > O:
            sweep_hi = m15[i]['h']; ref_pl = lastPL[i]
            for j in range(i+1, min(i+1+SWEEP_WIN, n)):
                sweep_hi = max(sweep_hi, m15[j]['h'])
                if m15[j]['c'] < ref_pl:
                    entry = m15[j]['c']; stop = sweep_hi + BUF*a
                    if stop > entry:
                        o = walk(m15, j+1, entry, stop, 'bear', RR, HOLD)
                        if o is not None:
                            R = stop-entry; net = o - cost(o, entry, R)
                            store[()].append((m15[j]['_ts'], net)); store_cls[cls][()].append((m15[j]['_ts'], net)); store_pair[pk].append((m15[j]['_ts'], net))
                        last = j + COOLDOWN; i = last + 1; made = True
                    break
        if not made:
            i += 1


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100/(1+RR)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); store_pair = defaultdict(list); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        m15 = _bars_norm(pairs[pk].get('m15', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(m15) < 1000 or len(h1) < 200:
            continue
        npairs += 1
        scan(m15, h1, store, cls, store_cls, store_pair, pk)

    print(f"Liquidity Sniper — 4H-open + 15m sweep + MSS, RR2 — {npairs} pairs, ~64d m15\n")
    line("ALL", store[()])
    print("\nper class:")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c][()])


if __name__ == '__main__':
    main()
