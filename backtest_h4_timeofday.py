#!/usr/bin/env python3
"""H4 — time-of-day filter, head-to-head vs the deployed baseline.

Replays macd-primary on FX (major+minor) under the DEPLOYED rule-set
(conf gate + H5 + pip-floor + EW-veto), records each kept signal's UTC
hour + outcome, then scores candidate time-of-day skip policies. Same
discipline as the EW-veto/H7 head-to-head: a filter is only worth it if
the cohort it REMOVES is sub-50% WR (i.e. it drops losers, not winners).

Windows are session-based (a-priori), not fitted to individual hours, to
avoid overfitting the same data we measure on. Read-only.

    python3 backtest_h4_timeofday.py
"""
import json, datetime
import detect_triggers as dt

LOOKBACK_STRUCT = 8
EXPIRY_BARS, RESOLVE_CAP = 64, 96
ROWS_CACHE = "/tmp/claude-0/-home-user-vikinginvest-prices/35e9b401-fd05-54bf-b99f-87764f479b75/scratchpad/h4_rows.json"

def macd_dir_at(ml, sl, i):
    m0,m1,s0,s1 = ml[i-1],ml[i],sl[i-1],sl[i]
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

def parse_hour(iso):
    s = iso.replace('Z','').split('.')[0]
    return datetime.datetime.fromisoformat(s).hour

def collect_rows(hist):
    fx=[p for p,c in dt.PAIR_CLASS.items() if c in ('major','minor') and p in hist['pairs']]
    rows=[]
    for pair in fx:
        p=hist['pairs'][pair]; m15=p.get('m15',[]); h1a=p.get('h1',[]); da=p.get('daily',[])
        closes=[b['c'] for b in m15]
        if len(closes)<60: continue
        ml,sl=dt.macd_series(closes,12,26,9); pc=dt.PAIR_CLASS.get(pair)
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
            gate = (conf>=1 and conf<=3) if pc=='major' else (conf>=1)
            if not gate: continue
            # deployed EW-veto
            if ew in ('bull','bear') and ew!=md: continue
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
            rows.append({'hour':parse_hour(T),'win':oc=='win'})
    return rows

def score(rows, skip):
    kept=[r for r in rows if r['hour'] not in skip]
    rem =[r for r in rows if r['hour'] in skip]
    def stats(rs):
        w=sum(1 for r in rs if r['win']); l=len(rs)-w; n=len(rs)
        return n,w,l,(100*w/n if n else None),(w-l)
    return stats(kept), stats(rem)

def line(label, kept, rem, base_net):
    n,w,l,wr,net = kept
    rn,rw,rl,rwr,rnet = rem
    wrs=f"{wr:5.1f}%" if wr is not None else "   -  "
    rwrs=f"{rwr:4.1f}%" if rwr is not None else "  -  "
    print(f"  {label:26} keep n={n:>3} WR={wrs} netR={net:>+4} (Δ{net-base_net:>+4})   "
          f"drop n={rn:>2} ({rw}W/{rl}L, WR={rwrs})")

def main():
    import os
    if os.path.exists(ROWS_CACHE):
        rows=json.load(open(ROWS_CACHE)); print(f"[loaded {len(rows)} cached rows]")
    else:
        hist=json.load(open('historical-ohlc.json'))
        rows=collect_rows(hist)
        try: json.dump(rows, open(ROWS_CACHE,'w'))
        except Exception: pass
    base=score(rows, set())[0]
    base_net=base[4]
    print(f"\nBaseline (deployed rule-set, all hours): n={base[0]} WR={base[3]:.1f}% netR={base[4]:+d}\n")
    print("Candidate time-of-day skips (session-based, a-priori):")
    print("  policy                     kept set                          dropped cohort (want <50% WR)")
    policies = [
        ("skip London-open 07-09", set(range(7,10))),
        ("skip London-open 07-10", set(range(7,11))),
        ("skip rollover 21",       {21}),
        ("skip late-NY 18-21",     set(range(18,22))),
        ("skip Asian 00-05",       set(range(0,6))),
        ("skip Asian 01-05",       set(range(1,6))),
        ("open+rollover 07-09,21", set(range(7,10))|{21}),
        ("open+rollover+Asian",    set(range(7,10))|{21}|set(range(1,6))),
        ("concentrate 12-17 only", set(range(0,24))-set(range(12,18))),
    ]
    for name, skip in policies:
        kept, rem = score(rows, skip)
        line(name, kept, rem, base_net)

if __name__=='__main__':
    main()
