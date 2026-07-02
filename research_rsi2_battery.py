#!/usr/bin/env python3
"""RSI(2) mean-reversion — full test battery before any deploy decision.

  (1) BIGGER SAMPLE  — run on h1 (~5k bars/index) not just daily (~250).
  (2) RISK MODEL     — hard ATR stop => outcomes in R (comparable to our
                       1:1 engine), bounding the mean-reversion tail.
  (3) DIVERSIFICATION— does it fire when our momentum engine is QUIET? Check
                       MACD posture at each entry (a dip => bearish MACD =>
                       our momentum-long would NOT fire => genuine add-on).
  (4) ROBUSTNESS     — first-half vs second-half (walk-forward), a parameter
                       sweep, and a DAX-specific diagnosis (the one failure).

Long-only (buy oversold dip in an uptrend). Read-only measurement.
"""
import ast, json, statistics

def indices(hist):
    for n in ast.parse(open('detect_triggers.py').read()).body:
        if isinstance(n, ast.Assign) and any(getattr(t,'id',None)=='PAIR_CLASS' for t in n.targets):
            pc = ast.literal_eval(n.value)
            return [p for p,c in pc.items() if c=='index' and p in hist['pairs']]
    return []

def sma(vals,n,i):
    if i+1<n: return None
    return sum(vals[i-n+1:i+1])/n

def rsi(vals,period,i):
    if i<period: return None
    g=l=0.0
    for j in range(i-period+1,i+1):
        d=vals[j]-vals[j-1]
        if d>0: g+=d
        else: l-=d
    ag=g/period; al=l/period
    if al==0: return 100.0
    return 100-100/(1+ag/al)

def atr(bars,period,i):
    if i<period: return None
    s=0.0
    for j in range(i-period+1,i+1):
        pc=bars[j-1]['c']
        tr=max(bars[j]['h']-bars[j]['l'], abs(bars[j]['h']-pc), abs(bars[j]['l']-pc))
        s+=tr
    return s/period

def macd_series(closes,f=12,s=26,sig=9):
    def ema(vals,p):
        out=[None]*len(vals)
        if len(vals)<p: return out
        k=2/(p+1); e=sum(vals[:p])/p; out[p-1]=e
        for i in range(p,len(vals)): out[i]=(vals[i]-out[i-1])*k+out[i-1]
        return out
    fe=ema(closes,f); se=ema(closes,s)
    m=[ (fe[i]-se[i]) if (fe[i] is not None and se[i] is not None) else None for i in range(len(closes)) ]
    first=next((i for i,v in enumerate(m) if v is not None),-1)
    sg=[None]*len(closes)
    if first>=0:
        sub=m[first:]; se2=ema([x if x is not None else 0 for x in sub],sig)
        for j,v in enumerate(se2): sg[first+j]=v
    return m,sg

def run_rsi2(bars, trend_n=50, rsi_buy=10, stop_atr=1.5, hold_cap=48, want_macd=False):
    closes=[b['c'] for b in bars]
    macd=sgl=None
    if want_macd: macd,sgl=macd_series(closes)
    trades=[]  # each: {'R':outcome_in_R, 'idx':i, 'bear_posture':bool}
    i=max(trend_n,27)
    while i < len(closes)-1:
        c=closes[i]; r2=rsi(closes,2,i); tr=sma(closes,trend_n,i); a=atr(bars,14,i)
        if None in (r2,tr,a) or a<=0: i+=1; continue
        if r2<rsi_buy and c>tr:
            entry=c; stop=entry-stop_atr*a; R=entry-stop
            out=None
            for j in range(i+1,min(i+1+hold_cap,len(closes))):
                if bars[j]['l']<=stop: out=-1.0; break         # stop hit => -1R
                s5=sma(closes,5,j)
                if s5 is not None and closes[j]>s5:
                    out=(closes[j]-entry)/R; break             # recovery exit
            if out is None:
                out=(closes[min(i+hold_cap,len(closes)-1)]-entry)/R
            bear=False
            if want_macd and macd[i] is not None and sgl[i] is not None:
                bear = macd[i] < sgl[i]   # bearish MACD posture at entry
            trades.append({'R':out,'idx':i,'bear':bear})
            i = j if out!=-1.0 else i+1
            i=max(i, trades[-1]['idx']+1)
        else:
            i+=1
    return trades

def stats(trades):
    if not trades: return None
    Rs=[t['R'] for t in trades]
    wins=[r for r in Rs if r>0]; losses=[r for r in Rs if r<=0]
    gp=sum(wins); gl=-sum(losses)
    return {'n':len(Rs),'wr':100*len(wins)/len(Rs),'ev':statistics.mean(Rs),
            'sumR':sum(Rs),'worst':min(Rs),'pf':(gp/gl if gl>0 else float('inf'))}

def line(label, s):
    if not s: print(f"  {label:10} (no trades)"); return
    pf=f"{s['pf']:.2f}" if s['pf']!=float('inf') else "inf"
    print(f"  {label:10} n={s['n']:>4} WR={s['wr']:5.1f}% EV={s['ev']:+.3f}R sumR={s['sumR']:+7.1f} worst={s['worst']:+.1f}R PF={pf}")

def main():
    hist=json.load(open('historical-ohlc.json'))
    idx=indices(hist)

    print("="*78); print("(1)+(2) RSI(2) on H1 with ATR stop → R-terms  [rsi<10, >SMA50, stop 1.5*ATR]"); print("="*78)
    all_h1=[]
    for k in idx:
        t=run_rsi2(hist['pairs'][k].get('h1',[]), want_macd=True)
        all_h1 += t; line(k, stats(t))
    line('ALL', stats(all_h1))

    print("\n"+"="*78); print("Daily corroboration (concept-faithful, small n) [same rules, hold_cap=10]"); print("="*78)
    all_d=[]
    for k in idx:
        t=run_rsi2(hist['pairs'][k].get('daily',[]), hold_cap=10)
        all_d+=t; line(k, stats(t))
    line('ALL', stats(all_d))

    print("\n"+"="*78); print("(3) DIVERSIFICATION — MACD posture at each H1 entry"); print("="*78)
    bear=sum(1 for t in all_h1 if t['bear']); tot=len(all_h1)
    print(f"  entries in BEARISH MACD posture (our momentum-long would NOT fire): "
          f"{bear}/{tot} = {100*bear/tot:.0f}%")
    print("  → high % = RSI(2) fires when the momentum engine is quiet = genuine diversification.")

    print("\n"+"="*78); print("(4a) WALK-FORWARD — first half vs second half of H1 sample (aggregate)"); print("="*78)
    fh=[t for t in all_h1 if t['idx']%2==0]  # crude interleave-free split below
    # proper split: per-index by time index midpoint
    first=[]; second=[]
    for k in idx:
        bars=hist['pairs'][k].get('h1',[]); mid=len(bars)//2
        t=run_rsi2(bars)
        for tr in t:
            (first if tr['idx']<mid else second).append(tr)
    line('1st half', stats(first)); line('2nd half', stats(second))

    print("\n"+"="*78); print("(4b) PARAMETER SWEEP (H1 aggregate) — is the edge a knife-edge?"); print("="*78)
    for rb in (5,10,15):
        for tn in (50,100):
            agg=[]
            for k in idx: agg+=run_rsi2(hist['pairs'][k].get('h1',[]), trend_n=tn, rsi_buy=rb)
            s=stats(agg)
            print(f"  rsi<{rb:<2} SMA{tn:<3}: n={s['n']:>4} WR={s['wr']:5.1f}% EV={s['ev']:+.3f}R sumR={s['sumR']:+7.1f} PF={s['pf']:.2f}")

    print("\n"+"="*78); print("(4c) DAX DIAGNOSIS — why de40 fails"); print("="*78)
    dt=run_rsi2(hist['pairs']['de40'].get('h1',[]))
    s=stats(dt); line('de40 h1', s)
    Rs=sorted(t['R'] for t in dt)
    print(f"  worst 5 trades (R): {[round(x,1) for x in Rs[:5]]}")
    print(f"  stop-outs (-1R): {sum(1 for t in dt if t['R']==-1.0)}/{len(dt)}")
    print(f"  median R: {statistics.median([t['R'] for t in dt]):+.2f}")

if __name__=='__main__':
    main()
