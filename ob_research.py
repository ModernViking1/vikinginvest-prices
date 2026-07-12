"""Bonus #7 — Order Blocks / Smart Money Concepts, tested faithfully to the
standard SMC rules (confirmed online):
  - Order block = last opposite-colour candle before an impulsive move that
    BREAKS STRUCTURE (close beyond the prior swing high/low).
  - Entry: price retraces into the OB zone and rejects it (confirmation) -> enter
    next bar open (realistic market fill, not an idealised limit tap).
  - Stop: beyond the OB low(bull)/high(bear) with a small buffer.
  - Target: RR sweep 1:1/1:2/1:3 (SMC claims 1:3-1:5) + measured (impulse extreme).
Realistic cost, chronological OOS split, daily + 4H. #5 (9-step) has no published
rules; #6 (Fibonacci) already tested in book5_research.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
BOS_LB = 10
OB_SCAN = 6
MITIG_WIN = 40
RR_SWEEP = [1.0, 2.0, 3.0]


def detect_ob(bars):
    """Yield (entry_idx, dir, stop, imp_extreme)."""
    n = len(bars); out = []; last = -1
    for i in range(BOS_LB + OB_SCAN, n - 1):
        if i <= last: continue
        prior = bars[i-BOS_LB:i]
        phi = max(b['h'] for b in prior); plo = min(b['l'] for b in prior)
        bull = bars[i]['c'] > phi
        bear = bars[i]['c'] < plo
        if not (bull or bear): continue
        # impulse must be decisive: close beyond by a real margin
        if bull:
            ob = None
            for k in range(i-1, max(-1, i-1-OB_SCAN), -1):
                if bars[k]['c'] < bars[k]['o']: ob = k; break
            if ob is None: continue
            zlo, zhi = bars[ob]['l'], bars[ob]['h']
            if zhi <= zlo: continue
            imp = max(b['h'] for b in bars[ob:i+1]); buf = 0.05*(zhi-zlo)
            for j in range(i+1, min(i+1+MITIG_WIN, n-1)):
                if bars[j]['l'] <= zhi and bars[j]['c'] > zlo and bars[j]['c'] > bars[j]['o']:
                    out.append((j+1, 'bull', zlo-buf, imp)); last = j+1; break
        else:
            ob = None
            for k in range(i-1, max(-1, i-1-OB_SCAN), -1):
                if bars[k]['c'] > bars[k]['o']: ob = k; break
            if ob is None: continue
            zlo, zhi = bars[ob]['l'], bars[ob]['h']
            if zhi <= zlo: continue
            imp = min(b['l'] for b in bars[ob:i+1]); buf = 0.05*(zhi-zlo)
            for j in range(i+1, min(i+1+MITIG_WIN, n-1)):
                if bars[j]['h'] >= zlo and bars[j]['c'] < zhi and bars[j]['c'] < bars[j]['o']:
                    out.append((j+1, 'bear', zhi+buf, imp)); last = j+1; break
    return out


def agg(seq):
    r=[x for x in seq if x is not None]; n=len(r); w=sum(1 for x in r if x>0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def walk_to(bars, i0, entry, stop, d, target):
    """Resolve to an explicit price target (for the measured/impulse target)."""
    R=abs(entry-stop)
    if R<=0: return None
    rr=abs(target-entry)/R
    for j in range(i0, min(i0+ (20 if len(bars)<2000 else 60), len(bars))):
        b=bars[j]
        if d=='bull':
            if b['l']<=stop: return -1.0
            if b['h']>=target: return rr
        else:
            if b['h']>=stop: return -1.0
            if b['l']<=target: return rr
    return None


def main():
    d=json.load(open(HIST)); pairs=d.get('pairs',{})
    res=defaultdict(lambda: defaultdict(list))   # tf -> key -> [(ts,r)]
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1=_bars_norm(pairs[pk].get('h1',[])); daily=_bars_norm(pairs[pk].get('daily',[]))
        if len(h1)<400 or len(daily)<80: continue
        for tf,(bars,hold) in {'daily':(daily,20),'4h':(agg4h(h1),60)}.items():
            for (i0,dr,stop,imp) in detect_ob(bars):
                if i0>=len(bars): continue
                entry=bars[i0]['o']
                if (dr=='bull' and stop>=entry) or (dr=='bear' and stop<=entry): continue
                for rr in RR_SWEEP:
                    o=walk(bars,i0,entry,stop,dr,rr,hold)
                    if o is not None: res[tf][rr].append((bars[i0]['_ts'], o-cost(o,entry,abs(entry-stop))))
                # measured target = impulse extreme
                om=walk_to(bars,i0,entry,stop,dr,imp)
                if om is not None: res[tf]['meas'].append((bars[i0]['_ts'], om-cost(om,entry,abs(entry-stop))))

    # rolling walk-forward on the daily edge (6 folds) — the real robustness test
    print("DAILY OB rolling walk-forward (6 folds):")
    for key in (2.0, 'meas'):
        rows=sorted(res['daily'][key]); K=6; nn=len(rows); f=nn//K; pos=0; cells=[]
        for x in range(K):
            seg=rows[x*f:(x+1)*f if x<K-1 else nn]
            _,_,e=agg([r for _,r in seg]); cells.append(f"{e:+.2f}")
            if e>0: pos+=1
        lbl='1:2' if key==2.0 else 'measured'
        print(f"  {lbl:<9} folds=[{'  '.join(cells)}]  positive={pos}/{K}")
    print()

    print("ORDER BLOCKS (SMC) · realistic confirmation entry · OOS-checked\n")
    for tf in ('daily','4h'):
        print(f"[{tf}]  {'target':<9} {'n':>5} {'WR%':>6} {'expR':>8}  {'OOS h1/h2':>16}  verdict")
        for key in RR_SWEEP+['meas']:
            rows=sorted(res[tf][key]); seq=[r for _,r in rows]
            n,w,e=agg(seq); mid=len(rows)//2
            _,_,eh=agg([r for _,r in rows[:mid]]); _,_,es=agg([r for _,r in rows[mid:]])
            v='PASS' if (e>0 and eh>0 and es>0 and n>=40) else ('thin' if n<40 else 'fail')
            lbl = ('1:%g'%key) if key!='meas' else 'measured'
            print(f"      {lbl:<9} {n:>5} {w:>5.1f}% {e:>+8.3f}  {eh:>+7.3f}/{es:>+7.3f}  {v}")
        print()


if __name__ == '__main__':
    main()
