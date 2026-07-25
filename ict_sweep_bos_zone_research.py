"""ICT sequential model (user pseudocode): sweep + reclaim -> BOS -> retrace into a
bullish/bearish zone (FVG/OB/Breaker) -> enter, SL beyond sweep, TP = next unswept
liquidity. Conditions must fire IN SEQUENCE.

Encoded (bullish): (1) a bar sweeps a recent swing LOW then closes back above it
(reclaim); (2) a later bar closes above the recent swing HIGH (bullish BOS); (3) the
BOS impulse leaves a bullish FVG (3-bar gap up) = the demand zone; (4) price retraces
DOWN into that FVG -> enter long. SL = sweep low - buffer. TP = nearest unswept swing
high above entry. Bearish mirror.

Two entries (the retrace is a limit -> fill-illusion check):
  A) LIMIT at the zone edge (optimistic).
  B) MARKET at the close of the bar that tags the zone (realistic).
Pivots confirmed k out (no lookahead), fixed cost, both-OOS gate, per class, tested
on 15m/h1/4h/daily. TP = structural (next liquidity); a fixed-RR2 comparison alongside.

Run: python ict_sweep_bos_zone_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PIV_K = 2
SWEEP_WIN = 6        # bars to reclaim after the sweep
BOS_WIN = 10         # bars to get the BOS after reclaim
RETR_WIN = 15        # bars to retrace into the zone after BOS
BUF = 0.10
COOLDOWN = 5
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
    ph_idx = [i for i in range(n) if ph[i] is not None]
    pl_idx = [i for i in range(n) if pl[i] is not None]
    last=-1
    for i in range(PIV_K+2, n-3):
        if i <= last: continue
        at = atr(bars, 14, i) or 0.0
        if at<=0: continue
        # ---- BULLISH sequence ----
        if lastPL[i] is not None and bars[i]['l'] < lastPL[i]:      # (1a) swept a swing low
            swept = lastPL[i]; sweep_lo = bars[i]['l']; reclaim = None
            for j in range(i, min(i+SWEEP_WIN, n)):
                sweep_lo = min(sweep_lo, bars[j]['l'])
                if bars[j]['c'] > swept: reclaim = j; break      # (1b) closed back above
            if reclaim is not None:
                bos = None
                for j in range(reclaim+1, min(reclaim+1+BOS_WIN, n)):
                    if lastPH[j] is not None and bars[j]['c'] > lastPH[j]: bos = j; break   # (2) BOS up
                if bos is not None:
                    # (3) bullish FVG in the BOS impulse: gap up around bos
                    fvg_top = None
                    for g in range(max(bos-2, 2), min(bos+2, n)):
                        if bars[g-2]['h'] < bars[g]['l']:
                            fvg_top = bars[g-2]['h']              # zone upper edge (demand)
                    if fvg_top is not None and fvg_top > sweep_lo:
                        for r in range(bos+1, min(bos+1+RETR_WIN, n-1)):   # (4) retrace into zone
                            if bars[r]['l'] <= fvg_top:
                                tgt = next((ph[q] for q in ph_idx if q < r and ph[q] > fvg_top and ph[q] > bars[r]['c']), None)
                                stop = sweep_lo - BUF*at
                                _emit(bars, r, fvg_top, stop, tgt, 'bull', tf, S, cls, SC)
                                last = r + COOLDOWN; break
        # ---- BEARISH sequence ----
        elif lastPH[i] is not None and bars[i]['h'] > lastPH[i]:
            swept = lastPH[i]; sweep_hi = bars[i]['h']; reclaim = None
            for j in range(i, min(i+SWEEP_WIN, n)):
                sweep_hi = max(sweep_hi, bars[j]['h'])
                if bars[j]['c'] < swept: reclaim = j; break
            if reclaim is not None:
                bos = None
                for j in range(reclaim+1, min(reclaim+1+BOS_WIN, n)):
                    if lastPL[j] is not None and bars[j]['c'] < lastPL[j]: bos = j; break
                if bos is not None:
                    fvg_bot = None
                    for g in range(max(bos-2, 2), min(bos+2, n)):
                        if bars[g-2]['l'] > bars[g]['h']:
                            fvg_bot = bars[g-2]['l']
                    if fvg_bot is not None and fvg_bot < sweep_hi:
                        for r in range(bos+1, min(bos+1+RETR_WIN, n-1)):
                            if bars[r]['h'] >= fvg_bot:
                                tgt = next((pl[q] for q in pl_idx if q < r and pl[q] < fvg_bot and pl[q] < bars[r]['c']), None)
                                stop = sweep_hi + BUF*at
                                _emit(bars, r, fvg_bot, stop, tgt, 'bear', tf, S, cls, SC)
                                last = r + COOLDOWN; break


def _emit(bars, r, zone_edge, stop, tgt, d, tf, S, cls, SC):
    ts = bars[r]['_ts']
    for mode, entry in (('limit', zone_edge), ('mkt', bars[r]['c'])):
        if (d=='bear' and stop<=entry) or (d=='bull' and stop>=entry): continue
        R = abs(entry-stop)
        ei = r if mode == 'limit' else r+1
        if ei >= len(bars): continue
        # structural TP (next liquidity)
        if tgt is not None and ((d=='bull' and tgt>entry) or (d=='bear' and tgt<entry)):
            o = walk(bars, ei, entry, stop, tgt, d, HOLD[tf])
            if o is not None:
                S[(tf,mode,'dol')].append((ts, o[0]-cost(o[0],entry,R))); SC[cls][(tf,mode,'dol')].append((ts, o[0]-cost(o[0],entry,R)))
        ft = entry+2*R if d=='bull' else entry-2*R
        o = walk(bars, ei, entry, stop, ft, d, HOLD[tf])
        if o is not None:
            S[(tf,mode,'rr2')].append((ts, o[0]-cost(o[0],entry,R))); SC[cls][(tf,mode,'rr2')].append((ts, o[0]-cost(o[0],entry,R)))


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<20} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


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
            scan(bars, tf, S, PAIR_CLASS.get(pk), SC)

    print(f"ICT sweep+reclaim -> BOS -> retrace-to-zone (FVG) -> next-liq TP — {npairs} pairs\n")
    for tf in ('15m', 'h1', '4h', 'daily'):
        print(f"=== {tf} ===")
        line("limit / DOL", S[(tf,'limit','dol')]); line("limit / RR2", S[(tf,'limit','rr2')])
        line("market / DOL", S[(tf,'mkt','dol')]); line("market / RR2", S[(tf,'mkt','rr2')])
    print("\n=== per class (h1, MARKET RR2) ===")
    for c in ['comm','crypto','index','major','minor']:
        line(c, SC[c][('h1','mkt','rr2')])


if __name__ == '__main__':
    main()
