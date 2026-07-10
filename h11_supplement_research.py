"""Does the H11 faytterro spring/UTAD event (H1 RSI(14) crossing up through 30 =
spring->BUY, down through 70 = UTAD->SELL, within the last 5 H1 bars) discriminate
the three SWING edges the way it does the intraday macdp trades (aligned 88% vs
no-event 37%)? If aligned swing trades are materially better AND survive OOS, the
H11 event is a useful supplement (prioritise / size-up aligned entries, or a
timing gate).

For each edge (hs / s5_rsi / s5_engulf) we bucket every historical trade by
whether the H11 event aligned with the trade direction at entry, and compare
WR / expectancy at 1:2 with a chronological OOS split.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import detect_hs, detect_s5
from five_strategies_research import agg4h, walk, cost, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RR = 2.0


def wilder_rsi(closes, per=14):
    n = len(closes); rsi = [None]*n
    if n < per+1: return rsi
    gg=ll=0.0
    for i in range(1, per+1):
        ch=closes[i]-closes[i-1]
        if ch>0: gg+=ch
        else: ll+=-ch
    gg/=per; ll/=per
    rsi[per]=100-100/(1+(gg/ll if ll>0 else 999))
    for i in range(per+1,n):
        ch=closes[i]-closes[i-1]
        gg=(gg*(per-1)+max(ch,0.0))/per
        ll=(ll*(per-1)+max(-ch,0.0))/per
        rsi[i]=100-100/(1+(gg/ll if ll>0 else 999))
    return rsi


def h11_aligned(rsi, idx, d):
    """True if a spring(bull)/UTAD(bear) crossing occurred in bars [idx-4..idx]."""
    if idx is None or idx < 15: return False
    spring=utad=False
    for si in range(max(15, idx-4), idx+1):
        rc, rp = rsi[si], rsi[si-1]
        if rc is None or rp is None: continue
        if rc>30 and rp<30: spring=True
        if rc<70 and rp>70: utad=True
    return spring if d=='bull' else utad


def agg(seq):
    r=[x for x in seq if x is not None]; n=len(r); w=sum(1 for x in r if x>0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def line(label, rows):
    rows=sorted(rows); seq=[r for _,r in rows]
    n,w,e=agg(seq); mid=len(rows)//2
    _,_,eh=agg([r for _,r in rows[:mid]]); _,_,es=agg([r for _,r in rows[mid:]])
    v='PASS' if (e>0 and eh>0 and es>0 and n>=40) else ('thin' if n<40 else 'fail')
    print(f"  {label:<28} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}  OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d=json.load(open(HIST)); pairs=d.get('pairs',{})
    # edge -> aligned/not -> [(ts,r)]
    store=defaultdict(lambda: {'aligned':[], 'not':[]})
    frac=defaultdict(lambda:[0,0])
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1=_bars_norm(pairs[pk].get('h1',[])); daily=_bars_norm(pairs[pk].get('daily',[]))
        draw=pairs[pk].get('daily',[])
        if len(h1)<400 or len(daily)<80: continue
        b4=agg4h(h1); h1_ts=[b['_ts'] for b in h1]
        rsi=wilder_rsi([b['c'] for b in h1],14)
        edges={
            'hs': (detect_hs(pk,h1,daily,draw), h1, HS_HOLD),
            's5_rsi': (detect_s5(pk,h1,daily,'rsi'), b4, HOLD['4h']),
            's5_engulf': (detect_s5(pk,h1,daily,'engulf'), b4, HOLD['4h']),
        }
        for edge,(sigs,bars,hold) in edges.items():
            bts=[b['_ts'] for b in bars]
            for s in sigs:
                # resolve outcome on the edge's own timeframe
                i0=bisect.bisect_left(bts, s['entry_ts'])
                if i0>=len(bars): continue
                o=walk(bars, i0, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None: continue
                r=o-cost(o, s['entry'], abs(s['entry']-s['stop']))
                # H11 alignment at entry (H1 RSI event)
                hidx=bisect.bisect_right(h1_ts, s['entry_ts'])-1
                al=h11_aligned(rsi, hidx, s['dir'])
                store[edge]['aligned' if al else 'not'].append((s['entry_ts'], r))
                frac[edge][0]+= 1 if al else 0; frac[edge][1]+=1

    print("H11 spring/UTAD event alignment on the SWING edges (RR2, H1 RSI(14) cross 30/70 in last 5 bars)\n")
    for edge in ('hs','s5_rsi','s5_engulf'):
        al,tot=frac[edge]
        print(f"{edge}  (H11 aligns on {100*al/max(1,tot):.0f}% of entries):")
        line("H11 ALIGNED", store[edge]['aligned'])
        line("not aligned", store[edge]['not'])
        print()


if __name__ == '__main__':
    main()
