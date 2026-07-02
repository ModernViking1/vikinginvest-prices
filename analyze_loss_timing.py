#!/usr/bin/env python3
"""When do losses happen? Time-of-day / day-of-week analysis.

Two lenses:
  (1) HISTORICAL replay of macd-primary on FX (major+minor) under the
      DEPLOYED rule-set (minor conf>=1 / major conf 1-3, H5 filter,
      pip-floor, EW-veto). Large sample. Also tracks which losses the
      EW-veto now REMOVES, bucketed by time, so we can see whether the
      latest releases cut losses at particular hours/days.
  (2) LIVE executions.json closed trades, bucketed the same way.

Buckets: UTC hour (with FX-session label) and UTC weekday. Read-only.
"""
import ast, json, datetime
import detect_triggers as dt

LOOKBACK_STRUCT = 8
EXPIRY_BARS, RESOLVE_CAP = 64, 96
WEEKDAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']

def session(h):
    if 22 <= h or h < 7:  return 'Asian'
    if 7 <= h < 12:       return 'London'
    if 12 <= h < 16:      return 'Lon/NY overlap'
    return 'New York'

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

def parse_ts(iso):
    # historical m15 't' like 2026-06-30T09:15:00.000000000Z
    s = iso.replace('Z','').split('.')[0]
    return datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc)

def run_hist(hist):
    fx=[p for p,c in dt.PAIR_CLASS.items() if c in ('major','minor') and p in hist['pairs']]
    kept={}   # (bucketkind,key) -> [w,l]
    removed={} # losses/wins the EW-veto removed, by time
    def add(d,key,outcome):
        b=d.setdefault(key,[0,0]); b[0 if outcome=='win' else 1]+=1
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
            dtm=parse_ts(T); hr=dtm.hour; wd=dtm.weekday()
            vetoed = ew in ('bull','bear') and ew!=md   # EW-veto removes these
            if vetoed:
                add(removed,('wd',wd),oc); add(removed,('hr',hr),oc)
            else:
                add(kept,('wd',wd),oc); add(kept,('hr',hr),oc)
    return kept, removed

def run_live():
    ex=json.load(open('executions.json')).get('executions',[])
    closed=[r for r in ex if r.get('event')=='closed' and r.get('realized_r') is not None]
    kept={}
    def add(key,rr):
        b=kept.setdefault(key,[0,0,0.0]); b[0 if rr>0 else 1]+=1; b[2]+=rr
    for r in closed:
        ts=r.get('ts')
        if not ts: continue
        dtm=datetime.datetime.utcfromtimestamp(ts/1000)
        add(('wd',dtm.weekday()), r['realized_r']); add(('hr',dtm.hour), r['realized_r'])
    return kept, len(closed)

def wr(b):
    d=b[0]+b[1]; return (100*b[0]/d) if d else None

def show_hist(kept, removed, kind, labels):
    print(f"  {'bucket':16}{'W':>4}{'L':>4}{'WR%':>7}{'netR':>7}   veto-removed(W/L)")
    keys = sorted(set([k[1] for k in kept if k[0]==kind] + [k[1] for k in removed if k[0]==kind]))
    for key in keys:
        b=kept.get((kind,key),[0,0]); rem=removed.get((kind,key),[0,0])
        w=wr(b); netR=b[0]-b[1]
        lbl = labels(key)
        print(f"  {lbl:16}{b[0]:>4}{b[1]:>4}{(f'{w:6.1f}' if w is not None else '     -'):>7}{netR:>+7}   {rem[0]}/{rem[1]}")

def show_live(kept, kind, labels):
    print(f"  {'bucket':16}{'W':>4}{'L':>4}{'WR%':>7}{'netR':>8}")
    keys=sorted(k[1] for k in kept if k[0]==kind)
    for key in keys:
        b=kept[(kind,key)]; w=wr(b)
        print(f"  {labels(key):16}{b[0]:>4}{b[1]:>4}{(f'{w:6.1f}' if w is not None else '     -'):>7}{b[2]:>+8.1f}")

def main():
    hist=json.load(open('historical-ohlc.json'))
    print("="*66)
    print("HISTORICAL (FX, deployed rule-set: conf gate + H5 + pip-floor + EW-veto)")
    print("="*66)
    kept, removed = run_hist(hist)
    print("\nBY WEEKDAY (UTC):")
    show_hist(kept, removed, 'wd', lambda k: WEEKDAYS[k])
    print("\nBY HOUR (UTC · session):")
    show_hist(kept, removed, 'hr', lambda k: f"{k:02d}:00 {session(k)}")

    print("\n"+"="*66)
    print("LIVE (executions.json closed trades, all classes)")
    print("="*66)
    lk, ncl = run_live()
    print(f"closed trades: {ncl}")
    print("\nBY WEEKDAY (UTC):")
    show_live(lk, 'wd', lambda k: WEEKDAYS[k])
    print("\nBY HOUR (UTC · session):")
    show_live(lk, 'hr', lambda k: f"{k:02d}:00 {session(k)}")

if __name__=='__main__':
    main()
