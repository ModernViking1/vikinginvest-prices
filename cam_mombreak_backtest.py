"""cam_rev 'momentum-of-break' guard — skip fading a level whose rejection candle has an EXPANDED
range (> K*ATR), the run-you-over case. Faithful: H.detect_cam_rev + H.score_sess (bracket-honest,
resolved-only). Reports KEPT vs SKIPPED across K, with OOS halves. Commits nothing.

  python cam_mombreak_backtest.py --hist historical-ohlc.json   # ~3 months
  python cam_mombreak_backtest.py --hist deep-ohlc.json         # 3 years (CI, restores deep cache)
"""
import argparse, bisect, json
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'index', 'comm'}
ATR_N = 14; APPROACH_N = 4

def atr_series(bars, n):
    tr = [0.0]*len(bars)
    for i in range(1, len(bars)):
        h, l, pc = bars[i]['h'], bars[i]['l'], bars[i-1]['c']
        tr[i] = max(h-l, abs(h-pc), abs(l-pc))
    out = [None]*len(bars)
    if len(bars) <= n: return out
    s = sum(tr[1:n+1])/n; out[n] = s
    for i in range(n+1, len(bars)): s = (s*(n-1)+tr[i])/n; out[i] = s
    return out

def agg(rs):
    n=len(rs)
    if not n: return None
    w=sum(1 for r in rs if r>0)
    return dict(n=n, wr=100.0*w/n, exp=sum(rs)/n, tot=sum(rs))

def oos(recs):
    recs=sorted(recs,key=lambda x:x[0]); m=len(recs)//2
    a=agg([r for _,r in recs[:m]]); b=agg([r for _,r in recs[m:]])
    return (a['exp'] if a else 0),(b['exp'] if b else 0)

def line(label,a,base=None):
    if not a: print(f"  {label:34} (n<1)"); return
    d=f"  Δexp {a['exp']-base['exp']:+.3f}" if base else ""
    print(f"  {label:34} n={a['n']:<5} WR={a['wr']:4.0f}%  exp={a['exp']:+.3f}R  tot={a['tot']:+6.0f}{d}")

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--hist',default='historical-ohlc.json'); args=ap.parse_args()
    pairs=json.load(open(args.hist)).get('pairs',{}); recs=[]
    for pk,layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or not isinstance(layers,dict): continue
        m15=_bars_norm(layers.get('m15') or []); daily=_bars_norm(layers.get('daily') or [])
        if len(m15)<500 or len(daily)<30: continue
        ts=[b['_ts'] for b in m15]; atr=atr_series(m15,ATR_N)
        for s in H.detect_cam_rev(pk,m15,daily):
            st,o=H.score_sess(m15,s['entry_ts'],s['entry'],s['stop'],s['target'],s['dir'],H.CAM_HOLD)
            if st!='resolved': continue
            i=bisect.bisect_left(ts,s['entry_ts'])-1
            if i<APPROACH_N or atr[i] is None or atr[i]<=0: continue
            a=atr[i]; rng_exp=(m15[i]['h']-m15[i]['l'])/a
            mv=m15[i]['c']-m15[i-APPROACH_N]['c']; against=(mv if s['dir']=='bear' else -mv)/a
            recs.append((s['entry_ts'],o,rng_exp,against))
    R=[x[1] for x in recs]; base=agg(R); o1,o2=oos([(x[0],x[1]) for x in recs])
    print(f"=== cam_rev · momentum-of-break guard · {len(recs)} resolved fills · {args.hist} ===\n")
    print(">> BASELINE (no guard):"); line('all',base); print(f"    OOS halves: {o1:+.3f} / {o2:+.3f}\n")
    def test(name,key,ths):
        print(f">> {name}")
        for K in ths:
            kept=[(x[0],x[1]) for x in recs if key(x)<=K]; skip=[(x[0],x[1]) for x in recs if key(x)>K]
            ak=agg([r for _,r in kept]); as_=agg([r for _,r in skip]); ret=100.0*len(kept)/len(recs); k1,k2=oos(kept)
            print(f"  K={K}: retain {ret:.0f}%"); line('   KEPT (fade)',ak,base); print(f"        OOS: {k1:+.3f}/{k2:+.3f}"); line('   SKIPPED (momentum break)',as_,base)
        print()
    test("GUARD 1 — trigger-candle range expansion (skip if range > K*ATR):",lambda x:x[2],[1.5,2.0,2.5,3.0])
    test(f"GUARD 2 — {APPROACH_N}-bar momentum into level (skip if move-against > K*ATR):",lambda x:x[3],[1.5,2.0,3.0])

if __name__=='__main__': main()
