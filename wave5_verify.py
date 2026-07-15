"""Stress-test wave 5, two ways:
  A) 6-fold walk-forward on the 4H breakout entry (the one marginal cell).
  B) Bratby's ACTUAL method: wave-4 pullback-zone entry (buy the wave-4 dip into
     the 38.2-61.8% fib retrace of wave 3, on a bounce), stop below the pullback
     low, target W5 = W1. Tighter stop than the breakout -> better RR. Tested on
     h1/4h/daily with fixed RR + native target and an OOS split.
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg
from elliott_research import zigzag, PRD, BREAK_WIN, ATR_BUF, HOLD

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RRS = [1.6, 2.0]
ZONE_LO, ZONE_HI = 0.382, 0.618      # fib retracement band of wave 3 (Bratby zones)
DEEP = 0.786                          # deeper than this -> setup void


def wave5_breakout(bars, tf):
    """Return list of (ts, realized_r at RR2) for the breakout entry — for folds."""
    piv = zigzag(bars, PRD); n = len(bars); last = -1; out = []
    for k in range(len(piv) - 4):
        pv = piv[k:k+5]
        for d, kinds in (('bull', ('L','H','L','H','L')), ('bear', ('H','L','H','L','H'))):
            if tuple(p[2] for p in pv) != kinds: continue
            p0,p1,p2,p3,p4 = (p[1] for p in pv)
            w1 = abs(p1-p0); w3 = abs(p3-p2)
            ok = (p2>p0 and p3>p1 and p4>p1 and p4<p3 and w3>=w1) if d=='bull' else (p2<p0 and p3<p1 and p4<p1 and p4>p3 and w3>=w1)
            if not ok or w1<=0: continue
            start = pv[4][0]+PRD+1; ei=None; inv=False
            for j in range(start, min(start+BREAK_WIN, n-1)):
                b=bars[j]
                if d=='bull':
                    if b['l']<p4: inv=True; break
                    if b['h']>p3: ei=j+1; break
                else:
                    if b['h']>p4: inv=True; break
                    if b['l']<p3: ei=j+1; break
            if inv or ei is None or ei<=last or ei>=n: continue
            entry=bars[ei]['o']; a=atr(bars,14,ei-1) or 0.0
            stop=(p4-ATR_BUF*a) if d=='bull' else (p4+ATR_BUF*a)
            o=walk(bars,ei,entry,stop,d,2.0,HOLD[tf])
            if o is not None: out.append((bars[ei]['_ts'], o-cost(o,entry,abs(entry-stop))))
            last=ei+6
    return out


def wave5_pullback(bars, tf, store, cls, store_cls):
    """Bratby pullback entry: after 0-1-2-3 (partial impulse), buy the wave-4 dip
    into the 38.2-61.8% retrace of wave 3 on a bounce; stop below the dip low."""
    piv = zigzag(bars, PRD); n = len(bars); last = -1
    for k in range(len(piv) - 3):
        p0v,p1v,p2v,p3v = piv[k:k+4]
        for d, kinds in (('bull', ('L','H','L','H')), ('bear', ('H','L','H','L'))):
            if (p0v[2],p1v[2],p2v[2],p3v[2]) != kinds: continue
            p0,p1,p2,p3 = p0v[1],p1v[1],p2v[1],p3v[1]
            w1=abs(p1-p0); w3=abs(p3-p2)
            ok = (p2>p0 and p3>p1 and w3>=w1) if d=='bull' else (p2<p0 and p3<p1 and w3>=w1)
            if not ok or w3<=0: continue
            if d=='bull':
                z_hi = p3 - ZONE_LO*w3; z_lo = p3 - ZONE_HI*w3; void = p3 - DEEP*w3
            else:
                z_lo = p3 + ZONE_LO*w3; z_hi = p3 + ZONE_HI*w3; void = p3 + DEEP*w3
            start = p3v[0]+PRD+1; ei=None; dip=None
            for j in range(start, min(start+BREAK_WIN, n-1)):
                b=bars[j]
                if d=='bull':
                    dip = b['l'] if dip is None else min(dip, b['l'])
                    if b['l'] < void or b['l'] < p1: break         # too deep / overlaps w1 -> void
                    if b['l'] <= z_hi and b['c'] > b['o'] and b['c'] > z_lo:   # in zone + bounce
                        ei=j+1; break
                else:
                    dip = b['h'] if dip is None else max(dip, b['h'])
                    if b['h'] > void or b['h'] > p1: break
                    if b['h'] >= z_lo and b['c'] < b['o'] and b['c'] < z_hi:
                        ei=j+1; break
            if ei is None or ei<=last or ei>=n: continue
            entry=bars[ei]['o']; a=atr(bars,14,ei-1) or 0.0
            stop=(dip-ATR_BUF*a) if d=='bull' else (dip+ATR_BUF*a)
            if (d=='bull' and stop>=entry) or (d=='bear' and stop<=entry): continue
            R=abs(entry-stop); ts=bars[ei]['_ts']
            native = entry + w1 if d=='bull' else entry - w1
            for rr in RRS:
                o=walk(bars,ei,entry,stop,d,rr,HOLD[tf])
                if o is not None:
                    r=o-cost(o,entry,R); store[(tf,f'RR{rr}')].append((ts,r)); store_cls[cls][(tf,f'RR{rr}')].append((ts,r))
            nrr=abs(native-entry)/R if R else 0
            if nrr>0:
                o=walk(bars,ei,entry,stop,d,nrr,HOLD[tf])
                if o is not None:
                    r=o-cost(o,entry,R); store[(tf,'native')].append((ts,r)); store_cls[cls][(tf,'native')].append((ts,r))
            last=ei+6


def line(label, rows):
    rows=sorted(rows); seq=[r for _,r in rows]; n,w,e=agg(seq); mid=len(rows)//2
    _,_,eh=agg([r for _,r in rows[:mid]]); _,_,es=agg([r for _,r in rows[mid:]])
    v='PASS' if (e>0 and eh>0 and es>0 and n>=40) else ('thin' if n<40 else 'fail')
    print(f"  {label:<18} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d=json.load(open(HIST)); pairs=d.get('pairs',{})
    bo=[]; store=defaultdict(list); store_cls=defaultdict(lambda: defaultdict(list))
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls=PAIR_CLASS.get(pk)
        h1=_bars_norm(pairs[pk].get('h1',[])); daily=_bars_norm(pairs[pk].get('daily',[]))
        if len(h1)<400 or len(daily)<80: continue
        series={'h1':h1,'4h':agg4h(h1),'daily':daily}
        bo += wave5_breakout(series['4h'],'4h')
        for tf,bars in series.items():
            if len(bars)<120: continue
            wave5_pullback(bars,tf,store,cls,store_cls)

    print("A) Wave5 4H BREAKOUT — 6-fold walk-forward (RR2):")
    bo.sort(); k=6; sz=len(bo)//k; passed=0
    for f in range(k):
        lo=f*sz; hi=(f+1)*sz if f<k-1 else len(bo); fold=[r for _,r in bo[lo:hi]]
        fn,fw,fe=agg(fold); ok=fe>0; passed+=ok
        print(f"   fold {f+1}: n={fn:>3} WR={fw:>5.1f}% exp={fe:>+7.3f}R {'ok' if ok else 'NEG'}")
    print(f"   -> {passed}/{k} folds positive\n")

    print("B) Wave5 PULLBACK entry (Bratby zones) — h1/4h/daily:")
    for tf in ('h1','4h','daily'):
        for tag in ('RR1.6','RR2.0','native'):
            line(f"{tf} {tag}", store[(tf,tag)])
    print("\n   per-class (4h RR2.0):")
    for c in ['comm','crypto','index','major','minor']:
        line(c, store_cls[c][('4h','RR2.0')])


if __name__ == '__main__':
    main()
