#!/usr/bin/env python3
"""Opening Range Breakout (ORB) on the indices — completeness test.

Zarattini/Aziz-style ORB, adapted to our m15 OHLC (no volume). For each
index and each session day: the opening-range (OR) bar is the first 15m bar
at the cash-session open; the first subsequent break of the OR high (long)
or low (short) is the entry, stop at the opposite OR extreme (R = OR range).
Test 1:1 target (comparable to our engine), a 2R target, and hold-to-session-
close. Both directions, no trend filter (raw ORB). Read-only.

Session opens (UTC, summer): DAX/FTSE 07:00, US 13:30 (NY 9:30 EDT),
Nikkei 00:00.
"""
import json, statistics
from collections import defaultdict

OPEN = {'de40':'07:00','ftse100':'07:00','dj30':'13:30','nas100':'13:30',
        'spx500':'13:30','jp225':'00:00'}
SESSION_BARS = 26   # ~6.5h to resolve / hold-to-close cap

def day(t): return t[:10]
def hm(t):  return t[11:16]

def run_orb(bars, open_hm, mode='1R'):
    # group bars by day, preserving order
    days=defaultdict(list)
    for b in bars: days[day(b['t'])].append(b)
    trades=[]
    for d,dbars in days.items():
        dbars.sort(key=lambda b:b['t'])
        oi=next((k for k,b in enumerate(dbars) if hm(b['t'])==open_hm), None)
        if oi is None or oi+1>=len(dbars): continue
        orb=dbars[oi]; hi=orb['h']; lo=orb['l']; R=hi-lo
        if R<=0: continue
        sess=dbars[oi+1:oi+1+SESSION_BARS]
        # first breakout
        entry=stop=tgt=direction=None; bi=None
        for k,b in enumerate(sess):
            if b['h']>=hi: direction='bull'; entry=hi; stop=lo; bi=k; break
            if b['l']<=lo: direction='bear'; entry=lo; stop=hi; bi=k; break
        if direction is None: continue
        if mode=='1R': tgt = entry+R if direction=='bull' else entry-R
        elif mode=='2R': tgt = entry+2*R if direction=='bull' else entry-2*R
        # simulate from breakout bar to session end
        outcome=None; exitpx=None
        for b in sess[bi:]:
            if direction=='bull':
                if b['l']<=stop: outcome='loss'; exitpx=stop; break
                if mode!='close' and b['h']>=tgt: outcome='win'; exitpx=tgt; break
            else:
                if b['h']>=stop: outcome='loss'; exitpx=stop; break
                if mode!='close' and b['l']<=tgt: outcome='win'; exitpx=tgt; break
        if outcome is None:  # ran to session close
            exitpx=sess[-1]['c']
            rr=(exitpx-entry)/R if direction=='bull' else (entry-exitpx)/R
            outcome='win' if rr>0 else 'loss'; r_real=rr
        else:
            r_real = ( (2 if mode=='2R' else 1) if outcome=='win' else -1 )
        trades.append({'dir':direction,'outcome':outcome,'R':r_real})
    return trades

def stats(trades):
    if not trades: return None
    Rs=[t['R'] for t in trades]; w=sum(1 for t in trades if t['outcome']=='win')
    gp=sum(r for r in Rs if r>0); gl=-sum(r for r in Rs if r<0)
    return {'n':len(trades),'wr':100*w/len(trades),'ev':statistics.mean(Rs),
            'sumR':sum(Rs),'pf':(gp/gl if gl>0 else float('inf'))}

def line(label,s):
    if not s: print(f"  {label:10} (no trades)"); return
    pf=f"{s['pf']:.2f}" if s['pf']!=float('inf') else 'inf'
    print(f"  {label:10} n={s['n']:>4} WR={s['wr']:5.1f}% EV={s['ev']:+.3f}R sumR={s['sumR']:+7.1f} PF={pf}")

def main():
    hist=json.load(open('historical-ohlc.json'))
    for mode in ('1R','2R','close'):
        title={'1R':'1:1 target','2R':'2R target','close':'hold to session close'}[mode]
        print("="*70); print(f"ORB — {title}"); print("="*70)
        allt=[]
        for k,ohm in OPEN.items():
            t=run_orb(hist['pairs'][k].get('m15',[]), ohm, mode)
            allt+=t; line(k, stats(t))
        line('ALL', stats(allt))
        print()

if __name__=='__main__':
    main()
