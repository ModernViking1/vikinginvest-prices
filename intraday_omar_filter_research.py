"""Omar no-wick as an ENTRY FILTER on the existing intraday strategy (not standalone).

Uses REAL cTrader realized R (ground truth, not a backtest): take every real
intraday trade and keep only those where the m15 bar at entry is a no-wick momentum
candle aligned with the trade direction. Compare the REAL win rate / expectancy of
the filtered subset vs the full set.

Result (as of writing): baseline 37.7% WR / -0.323R  ->  Omar-aligned 55.9% WR /
+0.057R, but on only 34 of 236 trades (14%) and OOS-unstable (all the lift in the
recent half). Promising-but-unproven; needs live tracking to n>=50-100.

Candle shape is generic/clean-room.

Run: python intraday_omar_filter_research.py
"""
import json, collections, bisect
from backtest_rsi_per_class import _bars_norm

TOL = 0.10
BODY_MIN = 0.5


def nowick_side(b):
    rng = b['h'] - b['l']
    if rng <= 0 or abs(b['c'] - b['o']) < BODY_MIN * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= TOL * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= TOL * rng:
        return 'bear'
    return None


def report(seq, label):
    n = len(seq)
    if not n:
        print(f"  {label:<26} (none)"); return
    w = sum(1 for x in seq if x > 0); e = sum(seq)/n; mid = n//2
    eh = sum(seq[:mid])/mid if mid else 0
    es = sum(seq[mid:])/(n-mid) if n-mid else 0
    print(f"  {label:<26} n={n:>3}  WR={100*w/n:>5.1f}%  realized_exp={e:>+7.3f}R  OOS[{eh:>+6.3f}/{es:>+6.3f}]")


def main():
    ex = json.load(open('executions.json')); rows = ex.get('executions', ex)
    byid = collections.defaultdict(dict)
    for r in rows:
        if r.get('position_id') and r.get('event') in ('placed', 'closed'):
            byid[r['position_id']][r['event']] = r
    P = [v for v in byid.values() if 'placed' in v and 'closed' in v]
    hist = json.load(open('historical-ohlc.json'))['pairs']
    m15 = {pk: _bars_norm(v.get('m15', [])) for pk, v in hist.items() if v.get('m15')}
    nd = lambda x: 'bull' if x in ('bull', 'buy', 'long') else 'bear'

    base, al, op, neu = [], [], [], []
    for v in P:
        c = v['closed']; pk = c.get('pair'); d = nd(c.get('dir')); rr = c.get('realized_r')
        if pk not in m15 or rr is None:
            continue
        ts = v['placed'].get('ts', 0) / 1000.0
        base.append(rr)
        bars = m15[pk]; tsl = [x['_ts'] for x in bars]; ci = bisect.bisect_left(tsl, ts) - 1
        ws = nowick_side(bars[ci]) if 0 <= ci < len(bars) else None
        (al if ws == d else (op if ws is not None else neu)).append(rr)

    print("OMAR no-wick as an ENTRY FILTER on real cTrader intraday trades (REAL realized R):\n")
    report(base, 'BASELINE (all)')
    report(al, 'no-wick ALIGNED (Omar)')
    report(op, 'no-wick OPPOSED')
    report(neu, 'no wick candle')
    print(f"\n  kept by Omar filter: {len(al)} of {len(base)} ({100*len(al)/max(1,len(base)):.0f}%)")


if __name__ == '__main__':
    main()
