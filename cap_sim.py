"""Theoretical $10k -> 1yr for (a) the RSI-cross S5 trigger and (b) all-4-indicators
combined (macd+rsi+wyckoff+golden, deduped union), run through the ACTUAL simulated
trade sequence with fixed-fractional risk. This is an ILLUSTRATION on ~1yr of
SIMULATED backtest data (zero live trades) — not a projection.
"""
import json, bisect
from detect_triggers import PAIR_CLASS, macd_series
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import ema, atr, adx, agg4h, weekly, walk, cost, is_engulf, HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RR = 2.0
INDS = ('macd', 'rsi', 'wyckoff', 'golden')


def sma(v, n, i): return None if i+1 < n else sum(v[i-n+1:i+1])/n


def fires(b4, i, d, trig, pre):
    m, s, r, c = pre
    if trig == 'macd':
        if None in (m[i-1], m[i], s[i-1], s[i]): return False
        return (d=='bull' and m[i-1]<=s[i-1] and m[i]>s[i]) or (d=='bear' and m[i-1]>=s[i-1] and m[i]<s[i])
    if trig == 'rsi':
        if r[i-1] is None or r[i] is None: return False
        return (d=='bull' and r[i-1]<=50<r[i]) or (d=='bear' and r[i-1]>=50>r[i])
    if trig == 'wyckoff':
        LB=20
        if i<LB: return False
        sup=min(b['l'] for b in b4[i-LB:i]); res=max(b['h'] for b in b4[i-LB:i])
        return (d=='bull' and b4[i]['l']<sup and b4[i]['c']>sup) or (d=='bear' and b4[i]['h']>res and b4[i]['c']<res)
    if trig == 'golden':
        a0,a1=sma(c,10,i-1),sma(c,10,i); b0,b1=sma(c,50,i-1),sma(c,50,i)
        if None in (a0,a1,b0,b1): return False
        return (d=='bull' and a0<=b0 and a1>b1) or (d=='bear' and a0>=b0 and a1<b1)
    return False


def collect(pairs):
    per = {t: [] for t in INDS}       # trigger -> [(ts, pair, r)]
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1=_bars_norm(pairs[pk].get('h1',[])); daily=_bars_norm(pairs[pk].get('daily',[]))
        if len(h1)<400 or len(daily)<80: continue
        b4=agg4h(h1); wk=weekly(daily)
        if len(wk)<12 or len(b4)<250: continue
        wc=[b['c'] for b in wk]; we20=ema(wc,10); dc=[b['c'] for b in daily]; de50=ema(dc,50)
        d_ts=[b['_ts'] for b in daily]; w_ts=[b['_ts'] for b in wk]
        c4=[b['c'] for b in b4]; m4,s4=macd_series(c4,12,26,9); r4=precompute_rsi(c4,14); pre=(m4,s4,r4,c4)
        last={t:-1 for t in INDS}
        for i in range(2,len(b4)-1):
            ts=b4[i]['_ts']; di=bisect.bisect_right(d_ts,ts)-1; wi=bisect.bisect_right(w_ts,ts)-1
            if di<51 or wi<11 or we20[wi] is None or de50[di] is None: continue
            up=wc[wi]>we20[wi] and we20[wi]>we20[wi-1]; dn=wc[wi]<we20[wi] and we20[wi]<we20[wi-1]
            if not (up or dn): continue
            a=atr(daily,14,di)
            if a is None or abs(daily[di]['c']-de50[di])>0.5*a: continue
            av=adx(b4,14,i)
            if av is None or av<22: continue
            d='bull' if up else 'bear'
            for t in INDS:
                if i<=last[t] or not fires(b4,i,d,t,pre): continue
                stop=min(b4[i]['l'],b4[i-1]['l']) if d=='bull' else max(b4[i]['h'],b4[i-1]['h'])
                entry=b4[i+1]['o']
                if (d=='bull' and stop>=entry) or (d=='bear' and stop<=entry): continue
                o=walk(b4,i+1,entry,stop,d,RR,HOLD['4h'])
                if o is not None:
                    per[t].append((ts, pk, o-cost(o,entry,abs(entry-stop))))
                last[t]=i+1
    return per


def sim(seq, risk, start=10000.0):
    eq=peak=start; mdd=0.0
    for r in seq:
        eq*=(1+risk*r); peak=max(peak,eq); mdd=min(mdd,(eq-peak)/peak)
    return eq, mdd


def report(name, trades):
    trades=sorted(trades)
    seq=[r for _,_,r in trades]; n=len(seq)
    if not n: print(f"{name}: no trades"); return
    ts=[t for t,_,_ in trades]; years=(max(ts)-min(ts))/(365.25*86400)
    totalR=sum(seq); wr=100*sum(1 for x in seq if x>0)/n; exp=totalR/n
    annR=totalR/years; annN=n/years
    print(f"\n{name}")
    print(f"  trades={n} over {years:.2f}yr ({annN:.0f}/yr, {annN/52:.1f}/wk)  WR={wr:.1f}%  exp={exp:+.3f}R  totalR={totalR:+.1f} (annualized {annR:+.0f}R)")
    print(f"  {'risk/trade':>10} | {'simple 1yr':>12} | {'compounded 1yr':>15} | {'CAGR':>7} | {'maxDD':>7}")
    for risk in (0.005, 0.01, 0.02):
        eq, mdd = sim(seq, risk)                       # over the actual period
        mult=eq/10000.0; cagr=mult**(1/years)-1
        simple_1yr=10000*(1+risk*annR)
        comp_1yr=10000*(1+cagr)
        print(f"  {risk*100:>8.1f}% | ${simple_1yr:>10,.0f} | ${comp_1yr:>13,.0f} | {cagr*100:>5.1f}% | {mdd*100:>5.1f}%")


def main():
    d=json.load(open(HIST)); pairs=d.get('pairs',{})
    per=collect(pairs)
    # (a) RSI only
    report("(a) RSI-CROSS only  [S5, RR2]", per['rsi'])
    # (b) all 4 indicators combined (dedup by pair+ts)
    seen=set(); union=[]
    for t in INDS:
        for (ts,pk,r) in per[t]:
            k=(pk,ts)
            if k in seen: continue
            seen.add(k); union.append((ts,pk,r))
    report("(b) ALL 4 INDICATORS combined  [S5, RR2, deduped union]", union)
    print("\nNOTE: SIMULATED ~1yr backtest, ZERO live trades. Fixed-fractional assumes")
    print("one position's risk at a time; real concurrency/correlation/slippage differ.")
    print("RSI won as best-of-5 triggers -> real magnitude likely lower. Illustration only.")


if __name__ == '__main__':
    main()
