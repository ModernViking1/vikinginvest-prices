"""W-bottom / M-top neckline-break reversal (user screenshots).

M-top (short): run-up, two roughly-equal PEAKS, middle trough = neckline. Place a
  SELL-STOP at the neckline; it triggers on the break DOWN through it. Stop = a few
  pips above the highest peak. Target = 1:1 (neckline - (stop-neckline)).
W-bottom (long, mirror): run-down, two roughly-equal TROUGHS, middle peak = neckline.
  BUY-STOP at the neckline (break UP). Stop below the lowest trough. Target = 1:1.

Invalidation (Step #5): cancel the pending order if price breaks the far pattern
extreme BEFORE triggering, or if >MAX_WAIT bars pass without a trigger (sideways).
Optional "look-left" filter: the 1:1 target must have been visited in the recent
past (price has a history of reaching it).

Stop-order entry (momentum/breakout fill — NOT a favourable limit, so no adverse-
selection inflation). Fractal pivots (confirmed PRD bars out, no lookahead), fixed
cost, chronological OOS split (both halves +), per class, 4h/h1/daily. Reports
1:1 (the book's target) plus 1.5/2.0 for context, with and without look-left.

Run: python wm_reversal_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PRD = 3              # fractal pivot lookback/forward
PEAK_TOL = 0.40      # two peaks/troughs "roughly equal": diff <= PEAK_TOL * pattern height
MAX_WAIT = 15        # bars after pattern to trigger before it's invalidated (Step #5, Scenario #2)
BUF = 0.10           # stop buffer beyond the pattern extreme, in ATR (~ a few pips)
LOOKLEFT = 60        # bars to look back for the "price has been to the target" check
COOLDOWN = 4
RRS = [1.0, 1.5, 2.0]
HOLD = {'h1': 96, '4h': 80, 'daily': 40}


def pivots(bars, k):
    n = len(bars); out = []
    for i in range(k, n - k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)):
            out.append((i, h, 'H'))
        elif all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)):
            out.append((i, l, 'L'))
    return out


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(bars, tf, store, store_ll, cls, store_cls):
    n = len(bars); piv = pivots(bars, PRD); last = -1
    for p in range(3, len(piv)):
        a4 = piv[p-3]; b3 = piv[p-2]; c2 = piv[p-1]; d1 = piv[p]
        # M top: L,H,L,H  (run-up, peak1, trough=neckline, peak2)
        # W bottom: H,L,H,L (run-down, trough1, peak=neckline, trough2)
        if b3[2] == 'H' and c2[2] == 'L' and d1[2] == 'H' and a4[2] == 'L':
            d = 'bear'; peak1 = b3[1]; peak2 = d1[1]; neck = c2[1]; run0 = a4[1]
            height = max(peak1, peak2) - neck
            if height <= 0 or abs(peak1 - peak2) > PEAK_TOL*height:
                continue
            if run0 >= neck:                       # need a genuine run-up into the M
                continue
            extreme = max(peak1, peak2)
        elif b3[2] == 'L' and c2[2] == 'H' and d1[2] == 'L' and a4[2] == 'H':
            d = 'bull'; tr1 = b3[1]; tr2 = d1[1]; neck = c2[1]; run0 = a4[1]
            height = neck - min(tr1, tr2)
            if height <= 0 or abs(tr1 - tr2) > PEAK_TOL*height:
                continue
            if run0 <= neck:
                continue
            extreme = min(tr1, tr2)
        else:
            continue
        confirm = d1[0] + PRD                       # 2nd peak/trough confirmable here (no lookahead)
        if confirm <= last or confirm >= n - 1:
            continue
        a = atr(bars, PRD*3, d1[0]) or atr(bars, 14, min(d1[0], n-1)) or 0.0
        stop = (extreme + BUF*a) if d == 'bear' else (extreme - BUF*a)
        # entry watch: first break through the neckline, subject to invalidation
        ei = None
        for k in range(confirm, min(confirm + MAX_WAIT, n)):
            bk = bars[k]
            if d == 'bear':
                if bk['h'] > extreme:               # broke the top of the M -> cancel
                    break
                if bk['l'] <= neck:                 # sell-stop triggers
                    ei = k; break
            else:
                if bk['l'] < extreme:               # broke the bottom of the W -> cancel
                    break
                if bk['h'] >= neck:                 # buy-stop triggers
                    ei = k; break
        if ei is None:
            continue
        entry = neck
        if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        # look-left: has price visited the 1:1 target before? (window before peak1)
        tgt1 = entry - R if d == 'bear' else entry + R
        lo0 = max(0, b3[0] - LOOKLEFT)
        seg = bars[lo0:b3[0]]
        been = any(s['l'] <= tgt1 <= s['h'] for s in seg) if seg else False
        for rr in RRS:
            o = walk(bars, ei, entry, stop, d, rr, HOLD[tf])
            if o is not None:
                net = o - cost(o, entry, R)
                store[(tf, rr)].append((ts, net))
                store_cls[cls][(tf, rr)].append((ts, net))
                if been:
                    store_ll[(tf, rr)].append((ts, net))
        last = ei + COOLDOWN


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100/(1+rr)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_ll = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, store, store_ll, cls, store_cls)

    print(f"W/M neckline-break reversal — {npairs} pairs, stop-order fills, OOS\n")
    for tf in ('4h', 'h1', 'daily'):
        for rr in RRS:
            line(f"{tf} RR{rr}", store[(tf, rr)], rr)
        print()
    print("=== 1:1 with look-left filter (target visited before) ===")
    for tf in ('4h', 'h1', 'daily'):
        line(f"{tf} RR1.0 LL", store_ll[(tf, 1.0)], 1.0)
    print("\n=== per class (4H, RR1.0 — the book's target) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][('4h', 1.0)], 1.0)


if __name__ == '__main__':
    main()
