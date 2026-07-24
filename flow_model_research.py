"""ICT 'Flow Model' (user screenshots): HTF no-wick candle -> 1H liquidity ->
5m FVG + sweep + DOL -> mechanical entry on the aggressive pullback.

Rules encoded:
  - 4H no-wick candle = context/bias. A bullish candle with ~no UPPER wick (closes
    at its high) signals reversal DOWN; a bearish candle with ~no LOWER wick signals
    reversal UP. (screenshot: "no wick ... price WILL reverse").
  - 1H liquidity = the recent 1H swing high (buy-side) / low (sell-side) price is
    drawn to sweep.
  - On m15 (finest data; source uses 5m): price SWEEPS that 1H liquidity, shifts
    structure (MSS) the other way, leaving a FAIR VALUE GAP; price pulls back into
    the FVG -> entry. Stop beyond the sweep extreme (higher high / lower low). Target
    = opposite 1H liquidity (DOL) and fixed 2:1.

Two entries tested to separate edge from fill illusion:
  A) FVG-limit  = fill at the FVG proximal edge (optimistic, favourable limit).
  B) market     = fill at the MSS/pullback bar's close (realistic momentum fill).
No lookahead (4H/1H levels from closed bars, pivots confirmed k out), fixed cost,
both-OOS-halves gate, per class.

Run: python flow_model_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
NOWICK_MAX = 0.12    # "no wick": the reversal-side wick <= this fraction of the 4H range
NOWICK_BODY = 0.55   # and a decisive body
NOWICK_LOOK = 3      # 4H candles back that the no-wick context stays valid
PIV_K = 2
SWEEP_WIN = 24       # m15 bars after the sweep to complete MSS+FVG+entry
BUF = 0.10
COOLDOWN = 8
HOLD = 80
NY_ONLY = True       # focused NY-open window (playbook: 9:30-10:20am EST). The edge
                     # only exists here — all-hours the model is negative.


def pivots(bars, k):
    n = len(bars); ph = [None]*n; pl = [None]*n
    for i in range(k, n-k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)): ph[i] = h
        if all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)): pl[i] = l
    return ph, pl


def nowick_dir(b):
    rng = b['h'] - b['l']
    if rng <= 0:
        return None
    body = abs(b['c'] - b['o'])
    if body < NOWICK_BODY*rng:
        return None
    up_wick = b['h'] - max(b['o'], b['c']); dn_wick = min(b['o'], b['c']) - b['l']
    if b['c'] > b['o'] and up_wick <= NOWICK_MAX*rng:      # bullish, no upper wick -> reverse DOWN
        return 'bear'
    if b['c'] < b['o'] and dn_wick <= NOWICK_MAX*rng:      # bearish, no lower wick -> reverse UP
        return 'bull'
    return None


def walk(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    rr = abs(target - entry)/R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return rr
    return None


def scan(m15, h1, S, cls, SC):
    b4 = agg4h(h1); t4 = [b['_ts'] for b in b4]
    ph1, pl1 = pivots(h1, PIV_K); t1 = [b['_ts'] for b in h1]
    lph1 = [None]*len(h1); lpl1 = [None]*len(h1); a=b=None
    for k in range(len(h1)):
        if k-PIV_K>=0 and ph1[k-PIV_K] is not None: a=ph1[k-PIV_K]
        if k-PIV_K>=0 and pl1[k-PIV_K] is not None: b=pl1[k-PIV_K]
        lph1[k]=a; lpl1[k]=b
    phm, plm = pivots(m15, PIV_K); n=len(m15)
    lphm=[None]*n; lplm=[None]*n; c=d=None
    for i in range(n):
        if i-PIV_K>=0 and phm[i-PIV_K] is not None: c=phm[i-PIV_K]
        if i-PIV_K>=0 and plm[i-PIV_K] is not None: d=plm[i-PIV_K]
        lphm[i]=c; lplm[i]=d
    last=-1; i=PIV_K+4
    while i < n-1:
        if i <= last:
            i += 1; continue
        # NY-open window filter (9:30-10:20 EST/EDT ~= 13:30-15:20 UTC across DST)
        if NY_ONLY and ((m15[i]['_ts']//3600) % 24) not in (13, 14, 15):
            i += 1; continue
        # current 4H bias from the most recent CLOSED no-wick 4H candle
        k4 = bisect.bisect_right(t4, m15[i]['_ts']-4*3600) - 1
        bias=None
        for kk in range(k4, max(-1,k4-NOWICK_LOOK), -1):
            if kk<0: break
            bias = nowick_dir(b4[kk])
            if bias: break
        if bias is None:
            i += 1; continue
        k1 = bisect.bisect_right(t1, m15[i]['_ts']) - 1
        if k1<1 or lph1[k1] is None or lpl1[k1] is None:
            i += 1; continue
        a = atr(m15,14,i) or 0.0
        if a<=0:
            i += 1; continue
        made=False
        if bias=='bear' and m15[i]['h'] > lph1[k1]:            # swept 1H buy-side liquidity
            sweep_hi=m15[i]['h']; tgt=lpl1[k1]                 # DOL = 1H sell-side liquidity
            fvg_lo=None; mss=False
            for j in range(i+1, min(i+1+SWEEP_WIN, n-1)):
                sweep_hi=max(sweep_hi, m15[j]['h'])
                if not mss and lplm[j] is not None and m15[j]['c']<lplm[j]: mss=True
                if mss and j>=i+2 and m15[j-2]['l'] > m15[j]['h']:          # bearish FVG (gap down)
                    fvg_lo=m15[j]['h']; fvg_hi=m15[j-2]['l']
                if mss and fvg_lo is not None:
                    # A) FVG-limit: price pulls back UP into the gap
                    for r in range(j+1, min(j+1+SWEEP_WIN, n-1)):
                        if m15[r]['h'] >= fvg_lo:
                            stop=sweep_hi+BUF*a
                            _emit(m15, r, fvg_lo, stop, tgt, 'bear', S, cls, SC, 'limit')
                            _emit(m15, r, m15[r]['c'], stop, tgt, 'bear', S, cls, SC, 'mkt')
                            last=r+COOLDOWN; i=last+1; made=True; break
                    break
        elif bias=='bull' and m15[i]['l'] < lpl1[k1]:
            sweep_lo=m15[i]['l']; tgt=lph1[k1]
            fvg_hi=None; mss=False
            for j in range(i+1, min(i+1+SWEEP_WIN, n-1)):
                sweep_lo=min(sweep_lo, m15[j]['l'])
                if not mss and lphm[j] is not None and m15[j]['c']>lphm[j]: mss=True
                if mss and j>=i+2 and m15[j-2]['h'] < m15[j]['l']:          # bullish FVG (gap up)
                    fvg_hi=m15[j]['l']
                if mss and fvg_hi is not None:
                    for r in range(j+1, min(j+1+SWEEP_WIN, n-1)):
                        if m15[r]['l'] <= fvg_hi:
                            stop=sweep_lo-BUF*a
                            _emit(m15, r, fvg_hi, stop, tgt, 'bull', S, cls, SC, 'limit')
                            _emit(m15, r, m15[r]['c'], stop, tgt, 'bull', S, cls, SC, 'mkt')
                            last=r+COOLDOWN; i=last+1; made=True; break
                    break
        if not made:
            i += 1


def _emit(bars, ei, entry, stop, tgt, d, S, cls, SC, mode):
    if ei>=len(bars): return
    if (d=='bear' and (stop<=entry or tgt>=entry)) or (d=='bull' and (stop>=entry or tgt<=entry)): return
    R=abs(entry-stop); ts=bars[ei]['_ts']
    o=walk(bars, ei, entry, stop, tgt, d, HOLD)                 # structural (DOL) target
    if o is not None:
        S[(mode,'struct')].append((ts,o-cost(o,entry,R))); SC[cls][(mode,'struct')].append((ts,o-cost(o,entry,R)))
    ft = entry+2*R if d=='bull' else entry-2*R
    o2=walk(bars, ei, entry, stop, ft, d, HOLD)
    if o2 is not None:
        S[(mode,'rr2')].append((ts,o2-cost(o2,entry,R)))


def line(label, rows):
    rows=sorted(rows); seq=[r for _,r in rows]; n,w,e=agg(seq); mid=len(rows)//2
    _,_,eh=agg([r for _,r in rows[:mid]]); _,_,es=agg([r for _,r in rows[mid:]])
    v='PASS' if (e>0 and eh>0 and es>0 and n>=40) else ('thin' if n<40 else 'fail')
    print(f"  {label:<22} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d=json.load(open(HIST)); pairs=d.get('pairs',{}); S=defaultdict(list); SC=defaultdict(lambda:defaultdict(list)); npairs=0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        m15=_bars_norm(pairs[pk].get('m15',[])); h1=_bars_norm(pairs[pk].get('h1',[]))
        if len(m15)<1000 or len(h1)<200: continue
        npairs+=1; scan(m15,h1,S,PAIR_CLASS[pk],SC)
    print(f"ICT Flow Model — 4H no-wick + 1H liq + m15 sweep/MSS/FVG — {npairs} pairs\n")
    print("A) FVG-LIMIT entry (optimistic favourable fill):")
    line("  limit / DOL target", S[('limit','struct')]); line("  limit / RR2", S[('limit','rr2')])
    print("B) MARKET entry (realistic momentum fill):")
    line("  market / DOL target", S[('mkt','struct')]); line("  market / RR2", S[('mkt','rr2')])
    print("\nper class (MARKET / DOL target):")
    for c in ['comm','crypto','index','major','minor']:
        line(c, SC[c][('mkt','struct')])


if __name__ == '__main__':
    main()
