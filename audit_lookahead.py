"""Look-ahead audit: backtest WR with the confluence gate indexed on the
CONTAINING higher-TF bar (look-ahead, as the current backtest does) vs the
last CLOSED higher-TF bar (honest, as live can actually see).

If the honest run collapses toward the ~41% live WR, the confluence look-ahead
IS the live-vs-backtest gap.
"""
import json, sys, bisect
from detect_triggers import RSI_GATE_BY_CLASS, PAIR_CLASS
from backtest_rsi_per_class import (
    _bars_norm, _min_prom, precompute_break_dirs, precompute_cl_dir,
    precompute_rsi, _find_struct_high, _find_struct_low,
)
from backtest_school_run_full import classify_setup

HIST='/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB,TL_LB,EW_LB,BOS_LB,WALK=5,8,8,24,48


def base_outcome(m15,i,entry,stop,target,direction):
    R=abs(entry-stop)
    if R<=0: return None
    for j in range(i+1,min(i+1+WALK,len(m15))):
        b=m15[j]
        if direction=='bull':
            if b['l']<=stop: return -1.0
            if b['h']>=target: return 1.0
        else:
            if b['h']>=stop: return -1.0
            if b['l']<=target: return 1.0
    return None


def run(pd,pk,htf_back):
    """htf_back=0 -> containing bar (look-ahead). htf_back=1 -> last CLOSED bar."""
    h1=_bars_norm(pd.get('h1',[])); m15=_bars_norm(pd.get('m15',[])); daily=_bars_norm(pd.get('daily',[]))
    if len(h1)<220 or len(m15)<100 or len(daily)<35: return []
    h1_ts=[b['_ts'] for b in h1]; d_ts=[b['_ts'] for b in daily]
    ew_arr=precompute_break_dirs(daily,EW_LB); tl_arr=precompute_break_dirs(h1,TL_LB)
    nw_arr=precompute_break_dirs(m15,NW_LB); cl_arr=precompute_cl_dir(h1)
    rsi_arr=precompute_rsi([b['c'] for b in h1],14)
    gate=RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pk),{'hi':80,'lo':20}); cls=PAIR_CLASS.get(pk)
    out=[]; last=-1
    for i in range(40,len(m15)-1):
        if i<=last: continue
        ts=m15[i]['_ts']
        h0=bisect.bisect_right(h1_ts,ts)-1; d0=bisect.bisect_right(d_ts,ts)-1
        h=h0-htf_back; dd=d0-htf_back          # honest = step back to last CLOSED bar
        if h<TL_LB or dd<EW_LB: continue
        ew=ew_arr[dd]; tl=tl_arr[h]; nw=nw_arr[i]; cl=cl_arr[h]
        if None in (ew,tl,nw,cl): continue
        conf,direction=classify_setup(ew,tl,nw,cl)
        if conf<2 or direction is None: continue
        lb=m15[max(0,i-8):i]
        if len(lb)<5: continue
        shi=max(b['h'] for b in lb); slo=min(b['l'] for b in lb); mp=_min_prom(m15[i]['c'])
        if direction=='bull':
            if not (m15[i]['c']>shi and (m15[i]['c']-shi)>=mp): continue
        else:
            if not (m15[i]['c']<slo and (slo-m15[i]['c'])>=mp): continue
        bos=m15[max(0,i-BOS_LB):i]; prom=_min_prom(m15[i]['c'])
        if direction=='bear':
            entry=m15[i]['h']; stop=_find_struct_high(bos,prom)
            if stop<=entry: continue
            target=entry-(stop-entry)
        else:
            entry=m15[i]['l']; stop=_find_struct_low(bos,prom)
            if stop>=entry: continue
            target=entry+(entry-stop)
        atr=m15[max(0,i-20):i]
        if len(atr)>=14:
            a20=sum(max(b['h']-b['l'],1e-9) for b in atr)/len(atr)
            fxf=(0.12 if abs(entry)>50 else 0.0012) if cls in ('major','minor') else 0
            if (stop-entry if direction=='bear' else entry-stop)<max(0.5*a20,fxf): continue
        r=rsi_arr[h] if h<len(rsi_arr) else None
        if r is not None:
            if direction=='bull' and r>=gate['hi']: continue
            if direction=='bear' and r<=gate['lo']: continue
        o=base_outcome(m15,i,entry,stop,target,direction)
        if o is None: last=i+WALK; continue
        out.append(o); last=i+1
    return out


def main():
    d=json.load(open(HIST)); pairs=d.get('pairs',{})
    uni=[p for p in PAIR_CLASS if p in pairs]
    for mode,back in (("LOOK-AHEAD (containing bar — current backtest)",0),
                      ("HONEST (last CLOSED bar — what live sees)",1)):
        allo=[]
        for p in uni: allo+=run(pairs[p],p,back)
        n=len(allo); w=sum(1 for x in allo if x>0)
        wr=100*w/max(1,n); exp=sum(allo)/max(1,n)
        print(f"{mode:<48} n={n:5}  WR={wr:5.1f}%  exp={exp:+.3f}R")
    print("\nLive reference: 243 trades, 41% WR, -0.28R/trade")


if __name__=='__main__': main()
