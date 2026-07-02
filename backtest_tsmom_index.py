#!/usr/bin/env python3
"""TSMOM long-bias filter on the index engine — head-to-head.

Time-series momentum (Moskowitz/Ooi/Pedersen): the sign of an instrument's
own trailing return predicts its next move. Indices carry a strong long
drift, so signals that FIGHT the medium-term trend may be the soft spot.

Test: replay index macd-primary under the deployed index rule-set (conf>=1
+ H5 + RSI + pip-floor), record each kept signal's direction + outcome +
timestamp, then score a TSMOM directional filter (block signals whose dir
opposes the sign of the trailing L-day return). Same adversarial check as
the EW-veto / H4: a good filter DROPS a sub-50% cohort, not winners.

Sweeps lookback L (indices only have ~1yr daily, so classic 252d is too
long — test 21/63/126d). Also reports a pure long-only variant. Read-only.
"""
import ast, json, bisect, datetime
import detect_triggers as dt

LOOKBACK_STRUCT=8; EXPIRY_BARS=64; RESOLVE_CAP=96
ROWS_CACHE="/tmp/claude-0/-home-user-vikinginvest-prices/35e9b401-fd05-54bf-b99f-87764f479b75/scratchpad/tsmom_rows.json"

def macd_dir_at(ml,sl,i):
    m0,m1,s0,s1=ml[i-1],ml[i],sl[i-1],sl[i]
    if None in (m0,m1,s0,s1): return None
    if m0<=s0 and m1>s1: return 'bull'
    if m0>=s0 and m1<s1: return 'bear'
    return None

def simulate(m15,i,d,entry,stop,target):
    n=len(m15); filled=False
    for j in range(i+1,min(i+1+EXPIRY_BARS,n)):
        b=m15[j]
        if not filled and ((d=='bull' and b['l']<=entry) or (d=='bear' and b['h']>=entry)): filled=True
        if filled:
            for jj in range(j,min(j+RESOLVE_CAP,n)):
                bb=m15[jj]
                if d=='bull':
                    if bb['l']<=stop: return 'loss'
                    if bb['h']>=target: return 'win'
                else:
                    if bb['h']>=stop: return 'loss'
                    if bb['l']<=target: return 'win'
            return 'expired'
    return 'nofill'

def indices(hist):
    for n in ast.parse(open('detect_triggers.py').read()).body:
        if isinstance(n,ast.Assign) and any(getattr(t,'id',None)=='PAIR_CLASS' for t in n.targets):
            pc=ast.literal_eval(n.value)
            return [p for p,c in pc.items() if c=='index' and p in hist['pairs']]
    return []

def collect_rows(hist, idx):
    rows=[]
    for pair in idx:
        p=hist['pairs'][pair]; m15=p.get('m15',[]); h1a=p.get('h1',[]); da=p.get('daily',[])
        closes=[b['c'] for b in m15]
        if len(closes)<60: continue
        ml,sl=dt.macd_series(closes,12,26,9)
        last=-1
        for i in range(35,len(closes)):
            if i<=last: continue
            md=macd_dir_at(ml,sl,i)
            if not md: continue
            T=m15[i]['t']; sM=m15[:i+1]
            h1_asof=[b for b in h1a if b['t']<=T]; d_asof=[b for b in da if b['t']<=T]
            if len(d_asof)<30 or len(h1_asof)<12: continue
            h1b=dt.build_h1_series(pair,sM,{pair:{'h1':h1_asof}})
            nw=dt.calc_independent_dir(sM,5); tl=dt.calc_independent_dir(h1b,8)
            cl=dt.calc_4h_cloud_dir(h1b); ew=dt.calc_independent_dir(d_asof,8)
            try:
                a=dt.auto_detect_ew(d_asof); ewp=a.get('ew') if a.get('ok') else None
                if ewp and ewp.get('dir') in ('bull','bear') and ewp.get('confidence',0)>=dt.AUTO_EW_MIN_CONFIDENCE and ewp.get('pattern') in dt.AUTO_EW_VALID_PATTERNS:
                    ew=ewp['dir']
            except Exception: pass
            if dt._htf_blocks(md,cl,enabled=dt.MACDP_HTF_FILTER): continue
            h1c=[b['c'] for b in h1b if b.get('c') is not None]
            rsi=dt.calc_rsi(h1c,14) if len(h1c)>=15 else None
            if rsi is None: continue
            if md=='bull' and rsi>=50: continue
            if md=='bear' and rsi<=50: continue
            conf=sum(1 for x in (ew,tl,nw,cl) if x==md)
            if conf<1: continue                       # index gate: skip 0/4 only
            cb=m15[i]; slc=m15[max(0,i-LOOKBACK_STRUCT):i]
            if md=='bull':
                entry=cb['l']; st=min((b['l'] for b in slc),default=None)
                if st is None or st>=entry: continue
                r=entry-st; target=entry+r; stop=st
            else:
                entry=cb['h']; st=max((b['h'] for b in slc),default=None)
                if st is None or st<=entry: continue
                r=st-entry; target=entry-r; stop=st
            if r<=0 or dt._stop_too_tight(r,entry,'index'): continue
            oc=simulate(m15,i,md,entry,stop,target)
            if oc not in ('win','loss'): continue
            last=i
            rows.append({'pair':pair,'dir':md,'win':oc=='win','T':T})
    return rows

def tsmom_sign(daily_ts, daily_close, T, L):
    di = bisect.bisect_right(daily_ts, T) - 1     # latest daily bar <= T
    if di < L: return None
    r = daily_close[di]/daily_close[di-L] - 1.0
    if r > 0: return 'bull'
    if r < 0: return 'bear'
    return None

def score(rows, keep):
    kept=[r for r in rows if keep(r)]; rem=[r for r in rows if not keep(r)]
    def st(rs):
        w=sum(1 for r in rs if r['win']); n=len(rs)
        return n,w,n-w,(100*w/n if n else None),(w-(n-w))
    return st(kept), st(rem)

def line(label, kept, rem, base_net):
    n,w,l,wr,net=kept; rn,rw,rl,rwr,rnet=rem
    wrs=f"{wr:5.1f}%" if wr is not None else "  -  "
    rwrs=f"{rwr:4.1f}%" if rwr is not None else " -  "
    print(f"  {label:22} keep n={n:>3} WR={wrs} netR={net:>+4} (Δ{net-base_net:>+4})   drop n={rn:>3} ({rw}W/{rl}L WR={rwrs})")

def main():
    hist=json.load(open('historical-ohlc.json')); idx=indices(hist)
    import os
    if os.path.exists(ROWS_CACHE):
        rows=json.load(open(ROWS_CACHE)); print(f"[loaded {len(rows)} cached rows]")
    else:
        rows=collect_rows(hist, idx)
        try: json.dump(rows, open(ROWS_CACHE,'w'))
        except Exception: pass
    # per-pair daily arrays for TSMOM lookup
    dd={}
    for k in idx:
        d=hist['pairs'][k].get('daily',[])
        dd[k]=([b['t'] for b in d],[b['c'] for b in d])

    base=score(rows, lambda r: True)[0]; base_net=base[4]
    print(f"\nBaseline (deployed index rule-set, all macd-primary): n={base[0]} WR={base[3]:.1f}% netR={base[4]:+d}")
    longs=sum(1 for r in rows if r['dir']=='bull'); shorts=len(rows)-longs
    print(f"  signal mix: {longs} long / {shorts} short\n")

    print("TSMOM directional filter — block signals opposing the trailing L-day return:")
    for L in (21,63,126):
        def keep(r, L=L):
            ts,cl=dd[r['pair']]; s=tsmom_sign(ts,cl,r['T'],L)
            return s is None or s==r['dir']   # neutral allowed; else must align
        kept,rem=score(rows, keep)
        line(f"TSMOM {L}d-aligned", kept, rem, base_net)

    print("\nPure long-only (indices trend up — drop all shorts):")
    kept,rem=score(rows, lambda r: r['dir']=='bull')
    line("long-only", kept, rem, base_net)

    print("\nShorts-only WR (the suspected soft spot):")
    sh=[r for r in rows if r['dir']=='bear']
    if sh:
        w=sum(1 for r in sh if r['win'])
        print(f"  shorts: n={len(sh)} WR={100*w/len(sh):.1f}% netR={2*w-len(sh):+d}")

if __name__=='__main__':
    main()
