#!/usr/bin/env python3
"""Index add-on strategy research — measure candidate edges on OUR data.

Our existing index engine is intraday MOMENTUM (macd-cross + confluence +
EW + FIB). The US indices have softened toward ~67-69% WR. This tests
research-backed, DATA-FEASIBLE (OHLC-only, no volume) candidates that would
DIVERSIFY that engine, on the 6 indices in historical-ohlc.json:

  1. OVERNIGHT DRIFT  — nearly all index gains accrue close->open (NY Fed
     "Overnight Drift"; Lou/Polk/Skouras). Uncorrelated with intraday
     momentum. Measure: overnight vs intraday return decomposition.
  2. RSI(2) MEAN-REVERSION (Connors) — buy deep-oversold in an uptrend,
     exit on recovery. Opposite signal logic to our momentum engine.

Read-only. This is measurement, not deployment.
"""
import ast, json, statistics

def indices():
    for n in ast.parse(open('detect_triggers.py').read()).body:
        if isinstance(n, ast.Assign) and any(getattr(t,'id',None)=='PAIR_CLASS' for t in n.targets):
            pc = ast.literal_eval(n.value)
            return [p for p,c in pc.items() if c=='index']
    return []

def sma(vals, n, i):
    if i+1 < n: return None
    return sum(vals[i-n+1:i+1]) / n

def rsi(vals, period, i):
    if i < period: return None
    gains=losses=0.0
    for j in range(i-period+1, i+1):
        d = vals[j]-vals[j-1]
        if d>0: gains+=d
        else: losses-=d
    ag=gains/period; al=losses/period
    if al==0: return 100.0
    rs=ag/al
    return 100 - 100/(1+rs)

def overnight_drift(daily):
    on=[]; day=[]
    for i in range(1,len(daily)):
        pc=daily[i-1].get('c'); o=daily[i].get('o'); c=daily[i].get('c')
        if None in (pc,o,c) or pc<=0 or o<=0: continue
        on.append(o/pc-1.0)      # close(t-1) -> open(t)  [overnight]
        day.append(c/o-1.0)      # open(t) -> close(t)     [intraday]
    def ann(rets):
        if not rets: return None
        m=statistics.mean(rets)
        return m*252*100  # annualised %, ~252 trading days
    def wr(rets):
        return 100*sum(1 for r in rets if r>0)/len(rets) if rets else None
    return {'n':len(on),'on_ann':ann(on),'day_ann':ann(day),
            'on_wr':wr(on),'day_wr':wr(day),
            'on_mean_bps':statistics.mean(on)*1e4 if on else None,
            'day_mean_bps':statistics.mean(day)*1e4 if day else None}

def rsi2_meanrev(daily, trend_n=50, rsi_buy=10, hold_cap=10):
    closes=[b.get('c') for b in daily]
    trades=[]
    i=trend_n
    while i < len(closes)-1:
        c=closes[i]
        r2=rsi(closes,2,i); tr=sma(closes,trend_n,i); s5=sma(closes,5,i)
        if r2 is None or tr is None or c is None: i+=1; continue
        # Entry: deep oversold within an uptrend (close above trend SMA)
        if r2 < rsi_buy and c > tr:
            entry=c; ex=None
            for j in range(i+1, min(i+1+hold_cap, len(closes))):
                s5j=sma(closes,5,j)
                if s5j is not None and closes[j] > s5j:  # Connors exit
                    ex=closes[j]; i=j; break
            if ex is None:
                ex=closes[min(i+hold_cap,len(closes)-1)]; i=i+hold_cap
            trades.append(ex/entry-1.0)
        else:
            i+=1
    if not trades: return {'n':0}
    wins=[t for t in trades if t>0]
    return {'n':len(trades),'wr':100*len(wins)/len(trades),
            'avg_bps':statistics.mean(trades)*1e4,
            'total_pct':sum(trades)*100,
            'best_bps':max(trades)*1e4,'worst_bps':min(trades)*1e4}

def main():
    hist=json.load(open('historical-ohlc.json'))
    idx=[p for p in indices() if p in hist['pairs']]   # dxy has no OHLC feed
    print("Indices:", idx, "\n")

    print("="*74)
    print("1) OVERNIGHT DRIFT — close->open vs open->close (annualised %, long-only)")
    print("="*74)
    print(f"  {'index':8}{'days':>5}{'ON ann%':>9}{'DAY ann%':>10}{'ON WR%':>8}{'DAY WR%':>9}{'ON bps/d':>10}")
    agg_on=[]; agg_day=[]
    for k in idx:
        d=hist['pairs'][k].get('daily',[])
        r=overnight_drift(d)
        if r['n']:
            print(f"  {k:8}{r['n']:>5}{r['on_ann']:>9.1f}{r['day_ann']:>10.1f}{r['on_wr']:>8.1f}{r['day_wr']:>9.1f}{r['on_mean_bps']:>10.1f}")
    print("\n  Read: if ON ann% >> DAY ann% and ON WR>50%, the overnight anomaly is")
    print("  present in our data — a genuinely uncorrelated add-on (needs overnight hold).")

    print("\n"+"="*74)
    print("2) RSI(2) MEAN-REVERSION — buy RSI2<10 in uptrend(>SMA50), exit >SMA5")
    print("="*74)
    print(f"  {'index':8}{'trades':>7}{'WR%':>7}{'avg bps':>9}{'total%':>9}{'worst bps':>11}")
    for k in idx:
        d=hist['pairs'][k].get('daily',[])
        r=rsi2_meanrev(d)
        if r['n']:
            print(f"  {k:8}{r['n']:>7}{r['wr']:>7.1f}{r['avg_bps']:>9.1f}{r['total_pct']:>9.2f}{r['worst_bps']:>11.1f}")
        else:
            print(f"  {k:8}{'0':>7}  (no signals — thin daily window)")
    print("\n  Read: high WR + positive avg = MR edge on dips. n is small (only ~250")
    print("  daily bars/index); treat as directional, corroborate on more history.")

if __name__=='__main__':
    main()
