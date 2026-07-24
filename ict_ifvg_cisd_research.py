"""ICT 'FVG inversion + CISD' confluence model (user screenshots, 'millionaire trader').

Model: a liquidity SWEEP (take out a prior swing high/low), then an FVG INVERSION
(iFVG) — a fair value gap that price CLOSES THROUGH to the opposite side (not just
wicks), signalling the order-flow shift (the CISD / change-in-state-of-delivery).
Enter on the close-through in the reversal direction; stop beyond the swept extreme;
target the opposite liquidity (DOL) or a fixed RR.

Bearish: sweep a swing HIGH; a bullish FVG (gap up) then gets a bar CLOSING below its
lower edge -> short. Bullish mirror: sweep a swing LOW; a bearish FVG gets a close
above its upper edge -> long.

Entry is a close-through = a realistic MARKET/momentum fill (no favourable limit), so
one fill mode. Pivots confirmed k out, no lookahead, fixed cost, both-OOS-halves gate,
per class. Tested on 15m / h1 / 4h / daily (all timeframes, per the request).

Run: python ict_ifvg_cisd_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PIV_K = 2
INV_WIN = 20         # bars after the FVG to get the inversion close-through
BUF = 0.10
COOLDOWN = 5
RRS = [1.5, 2.0, 3.0]
HOLD = {'15m': 80, 'h1': 72, '4h': 72, 'daily': 40}


def pivots(bars, k):
    n = len(bars); ph = [None]*n; pl = [None]*n
    for i in range(k, n-k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)): ph[i] = h
        if all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)): pl[i] = l
    return ph, pl


def walk(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0: return None
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


def scan(bars, tf, S, cls, SC):
    n = len(bars); ph, pl = pivots(bars, PIV_K)
    lastPH = [None]*n; lastPL = [None]*n; a=b=None
    for i in range(n):
        if i-PIV_K>=0 and ph[i-PIV_K] is not None: a=ph[i-PIV_K]
        if i-PIV_K>=0 and pl[i-PIV_K] is not None: b=pl[i-PIV_K]
        lastPH[i]=a; lastPL[i]=b
    last=-1
    for k in range(PIV_K+3, n-2):
        if k <= last: continue
        at = atr(bars, 14, k) or 0.0
        if at<=0: continue
        # bullish FVG at [k-2,k-1,k]: gap up; formed on an up-move that swept a swing high
        if bars[k-2]['h'] < bars[k]['l'] and lastPH[k] is not None and max(bars[k-2]['h'],bars[k-1]['h'],bars[k]['h']) > lastPH[k]:
            fvg_lo = bars[k-2]['h']; sweep_hi = max(bars[k-1]['h'], bars[k]['h'])
            for j in range(k+1, min(k+1+INV_WIN, n-1)):
                sweep_hi = max(sweep_hi, bars[j]['h'])
                if bars[j]['c'] < fvg_lo:                        # inversion: closed through -> bearish
                    entry = bars[j]['c']; stop = sweep_hi + BUF*at
                    tgt = lastPL[j] if lastPL[j] is not None and lastPL[j] < entry else None
                    _emit(bars, j+1, entry, stop, tgt, 'bear', tf, S, cls, SC)
                    last = j + COOLDOWN; break
        # bearish FVG at [k-2,k-1,k]: gap down; formed on a down-move that swept a swing low
        elif bars[k-2]['l'] > bars[k]['h'] and lastPL[k] is not None and min(bars[k-2]['l'],bars[k-1]['l'],bars[k]['l']) < lastPL[k]:
            fvg_hi = bars[k-2]['l']; sweep_lo = min(bars[k-1]['l'], bars[k]['l'])
            for j in range(k+1, min(k+1+INV_WIN, n-1)):
                sweep_lo = min(sweep_lo, bars[j]['l'])
                if bars[j]['c'] > fvg_hi:                        # inversion -> bullish
                    entry = bars[j]['c']; stop = sweep_lo - BUF*at
                    tgt = lastPH[j] if lastPH[j] is not None and lastPH[j] > entry else None
                    _emit(bars, j+1, entry, stop, tgt, 'bull', tf, S, cls, SC)
                    last = j + COOLDOWN; break


def _emit(bars, ei, entry, stop, tgt, d, tf, S, cls, SC):
    if ei >= len(bars): return
    if (d=='bear' and stop<=entry) or (d=='bull' and stop>=entry): return
    R = abs(entry-stop); ts = bars[ei-1]['_ts']
    # structural DOL target (opposite liquidity), if available
    if tgt is not None:
        o = walk(bars, ei, entry, stop, tgt, d, HOLD[tf])
        if o is not None:
            S[(tf,'dol')].append((ts, o[0]-cost(o[0],entry,R))); SC[cls][(tf,'dol')].append((ts, o[0]-cost(o[0],entry,R)))
    for rr in RRS:
        ft = entry + rr*R if d=='bull' else entry - rr*R
        o = walk(bars, ei, entry, stop, ft, d, HOLD[tf])
        if o is not None:
            S[(tf,rr)].append((ts, o[0]-cost(o[0],entry,R)))
            SC[cls][(tf,rr)].append((ts, o[0]-cost(o[0],entry,R)))


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    S = defaultdict(list); SC = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        m15 = _bars_norm(pairs[pk].get('m15', [])); h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400: continue
        npairs += 1
        tfs = {'h1': h1, '4h': agg4h(h1), 'daily': daily}
        if len(m15) >= 1000: tfs['15m'] = m15
        for tf, bars in tfs.items():
            if len(bars) < 150: continue
            scan(bars, tf, S, cls=PAIR_CLASS.get(pk), SC=SC)

    print(f"ICT FVG-inversion + CISD (sweep -> iFVG close-through) — {npairs} pairs\n")
    for tf in ('15m', 'h1', '4h', 'daily'):
        print(f"=== {tf} ===")
        line("DOL (opp liq)", S[(tf,'dol')])
        for rr in RRS:
            line(f"RR{rr}", S[(tf,rr)])
    print("\n=== per class (h1, RR2) ===")
    for c in ['comm','crypto','index','major','minor']:
        line(c, SC[c][('h1',2.0)])
    print("=== per class (15m, RR2) ===")
    for c in ['comm','crypto','index','major','minor']:
        line(c, SC[c][('15m',2.0)])


if __name__ == '__main__':
    main()
