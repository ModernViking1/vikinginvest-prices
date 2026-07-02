#!/usr/bin/env python3
"""NFP / event trade-free-zone analysis.

Replays macd-primary (deployed rule-set) on the USD-sensitive universe,
records each signal's entry time + outcome, then tags by proximity to the
NFP release and compares win-rate / net-R for event-window trades vs clear.

True NFP time is 12:30 UTC (8:30 ET, summer) — NOT the 16:30 UTC in
events.json (which is +4h off). NFP dates in the m15 window (1st Friday,
holiday-shifted): 2026-05-01, 2026-06-05, 2026-07-02 (Jul-3 was the July-4
holiday, so NFP moved to Thu Jul 2). Read-only.
"""
import ast, json, datetime
import detect_triggers as dt

LOOKBACK_STRUCT=8; EXPIRY_BARS=64; RESOLVE_CAP=96
NFP=[datetime.datetime(2026,5,1,12,30,tzinfo=datetime.timezone.utc),
     datetime.datetime(2026,6,5,12,30,tzinfo=datetime.timezone.utc),
     datetime.datetime(2026,7,2,12,30,tzinfo=datetime.timezone.utc)]
PAIRS=['eurusd','gbpusd','usdjpy','usdchf','usdcad','audusd','nzdusd',
       'cadjpy','nzdjpy','euraud','usdsgd','eursgd','gbpcad',
       'xauusd','xagusd','wtiusd','spx500','nas100','dj30']
ROWS="/tmp/claude-0/-home-user-vikinginvest-prices/35e9b401-fd05-54bf-b99f-87764f479b75/scratchpad/nfp_rows.json"

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

def collect(hist):
    rows=[]
    for pair in PAIRS:
        if pair not in hist['pairs']: continue
        pc=dt.PAIR_CLASS.get(pair)
        p=hist['pairs'][pair]; m15=p.get('m15',[]); h1a=p.get('h1',[]); da=p.get('daily',[])
        closes=[b['c'] for b in m15]
        if len(closes)<60: continue
        ml,sl=dt.macd_series(closes,12,26,9); last=-1
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
            gate=(conf>=1 and conf<=3) if pc in ('major','comm') else (conf>=1)
            if not gate: continue
            cb=m15[i]; slc=m15[max(0,i-LOOKBACK_STRUCT):i]
            if md=='bull':
                entry=cb['l']; st=min((b['l'] for b in slc),default=None)
                if st is None or st>=entry: continue
                r=entry-st; target=entry+r; stop=st
            else:
                entry=cb['h']; st=max((b['h'] for b in slc),default=None)
                if st is None or st<=entry: continue
                r=st-entry; target=entry-r; stop=st
            if r<=0 or dt._stop_too_tight(r,entry,pc): continue
            oc=simulate(m15,i,md,entry,stop,target)
            if oc not in ('win','loss'): continue
            last=i
            rows.append({'pair':pair,'cls':pc,'win':oc=='win','T':T})
    return rows

def dt_of(T):
    return datetime.datetime.fromisoformat(T.replace('Z','').split('.')[0]).replace(tzinfo=datetime.timezone.utc)

def nearest_nfp_delta_h(T):
    t=dt_of(T)
    return min((t-n).total_seconds()/3600 for n in NFP), t  # signed hours to nearest NFP (min by abs)

def wr(rows):
    if not rows: return None
    w=sum(1 for r in rows if r['win']); n=len(rows)
    return (100*w/n, n, 2*w-n)

def main():
    import os
    hist=json.load(open('historical-ohlc.json'))
    if os.path.exists(ROWS):
        rows=json.load(open(ROWS)); print(f"[loaded {len(rows)} cached rows]")
    else:
        rows=collect(hist)
        try: json.dump(rows, open(ROWS,'w'))
        except Exception: pass
    for r in rows:
        t=dt_of(r['T'])
        r['delta']=min(((t-n).total_seconds()/3600 for n in NFP), key=abs)
    base=wr(rows)
    print(f"\nUniverse: {len(PAIRS)} USD-sensitive pairs · macd-primary signals: {base[1]}")
    print(f"Baseline WR={base[0]:.1f}% netR={base[2]:+d}  (3 NFP events in window)\n")

    def bucket(pred,label):
        sel=[r for r in rows if pred(r['delta'])]
        s=wr(sel)
        if s: print(f"  {label:34} n={s[1]:>4} WR={s[0]:5.1f}% netR={s[2]:>+4}")
        else: print(f"  {label:34} (none)")

    print("Entry timing relative to NFP (12:30 UTC):")
    bucket(lambda d: abs(d)<=1, "within +/-1h of NFP")
    bucket(lambda d: abs(d)<=2, "within +/-2h of NFP")
    bucket(lambda d: abs(d)<=3, "within +/-3h of NFP")
    bucket(lambda d: -3<=d<0,  "0-3h BEFORE NFP (held into it)")
    bucket(lambda d: -2<=d<0,  "0-2h BEFORE NFP")
    bucket(lambda d: 0<=d<=2,  "0-2h AFTER NFP")
    bucket(lambda d: abs(d)>3, "clear of NFP (>3h away)")

if __name__=='__main__':
    main()
