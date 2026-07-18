"""Does an xGhozt-style wickless candle at entry improve the REAL cTrader intraday
signals? Filter test at 1:1.

Takes the 243 real intraday signals (real entry_filled + stop + dir from
executions.json), replays each on m15 at RR=1.0, and buckets by the wickless
candle on the last closed m15 bar before entry (no lookahead):

  ALIGNED  : wickless candle in the TRADE direction
             (bull trade + no-lower-wick bullish candle / bear + no-upper-wick)
  OPPOSED  : wickless candle in the OPPOSITE direction (xGhozt 'missing wick fills')

Reports WR / expectancy vs the baseline (all signals). Wickless = a decisive candle
with ~no wick on one side (generic candle shape, clean-room). ~0.10R cost applied.

Run: python intraday_xghozt_filter_research.py
"""
import json, collections, bisect
from backtest_rsi_per_class import _bars_norm

RR = 1.0
HOLD = 40
COST = 0.10
TOL = 0.10
BODY_MIN = 0.5


def wickless_side(b):
    rng = b['h'] - b['l']
    if rng <= 0 or abs(b['c'] - b['o']) < BODY_MIN * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= TOL * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= TOL * rng:
        return 'bear'
    return None


def walk(bars, ts, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tsl = [x['_ts'] for x in bars]; i0 = bisect.bisect_left(tsl, ts)
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        x = bars[j]
        if d == 'bull':
            if x['l'] <= stop: return -1.0
            if x['h'] >= tgt: return rr
        else:
            if x['h'] >= stop: return -1.0
            if x['l'] <= tgt: return rr
    return None


def stat(seq):
    n = len(seq)
    if not n:
        return (0, 0, 0, 0, 0)
    w = sum(1 for x in seq if x > 0); e = sum(seq)/n
    mid = n//2
    eh = sum(seq[:mid])/mid if mid else 0
    es = sum(seq[mid:])/(n-mid) if n-mid else 0
    return (n, 100*w/n, e, eh, es)


def line(label, seq):
    n, wr, e, eh, es = stat(seq)
    print(f"  {label:<18} n={n:>4} WR={wr:>5.1f}% exp={e:>+7.3f}R  net(-cost)={e-COST:>+7.3f}R  OOS[{eh:>+6.3f}/{es:>+6.3f}]")


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
        c = v['closed']; pk = c.get('pair'); d = nd(c.get('dir'))
        entry = c.get('entry_filled'); stop = c.get('stop')
        if pk not in m15 or not entry or not stop or entry == stop:
            continue
        ts = v['placed'].get('ts', 0) / 1000.0
        o = walk(m15[pk], ts, entry, stop, d, RR)
        if o is None:
            continue
        r = o - COST if o > 0 else o   # cost folded consistently below via net; keep raw for buckets
        base.append(o)
        bars = m15[pk]; tsl = [x['_ts'] for x in bars]
        ci = bisect.bisect_left(tsl, ts) - 1
        ws = wickless_side(bars[ci]) if 0 <= ci < len(bars) else None
        if ws == d: al.append(o)
        elif ws is not None: op.append(o)
        else: neu.append(o)

    print("xGhozt wickless-candle FILTER on real cTrader intraday signals — 1:1, m15 replay\n")
    line("BASELINE (all)", base)
    line("wickless ALIGNED", al)
    line("wickless OPPOSED", op)
    line("no wickless", neu)
    bn = stat(base)[0]
    print(f"\n(baseline covers {bn} signals; aligned={len(al)}, opposed={len(op)}, none={len(neu)})")


if __name__ == '__main__':
    main()
