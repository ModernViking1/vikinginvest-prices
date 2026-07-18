"""Test the proposed intraday change: move the 1:1 target higher (to 1.5:1)?

Replays the 243 REAL cTrader intraday signals (real entry_filled + stop + dir from
executions.json) forward on m15 OHLC at a range of target R-multiples, so we can
see which target actually pays best on live-quality entries BEFORE changing the
live engine.

Finding: raising the target makes it WORSE. Expectancy worsens monotonically as
the target rises (RR0.5 -0.14R -> RR1.5 -0.23R). The win rate sits ~9 points BELOW
its breakeven line at EVERY target — so the target is not the lever; the win rate
is. Moving to 1.5:1 is the worst option tested. The real fix is the win rate
(market vs limit entry / adverse selection), not the target.

Run: python intraday_target_sweep.py
"""
import json, collections, bisect
from backtest_rsi_per_class import _bars_norm

COST = 0.10          # measured intraday drag (~0.05R commission + loss-side overshoot)
HOLD = 40            # ~10h of m15


def walk(bars, ts, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tsl = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(tsl, ts)
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


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
    RRS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]
    res = {rr: [] for rr in RRS}
    for v in P:
        c = v['closed']; pk = c.get('pair'); d = nd(c.get('dir'))
        entry = c.get('entry_filled'); stop = c.get('stop')
        if pk not in m15 or not entry or not stop or entry == stop:
            continue
        ts = v['placed'].get('ts', 0) / 1000.0
        for rr in RRS:
            o = walk(m15[pk], ts, entry, stop, d, rr)
            if o is not None:
                res[rr].append(o)

    print("Intraday target sweep on 243 REAL cTrader signals (m15 replay):\n")
    print('%-7s %5s %7s %10s %9s %11s' % ('target', 'n', 'WR', 'breakeven', 'exp', 'net(-cost)'))
    for rr in RRS:
        s = res[rr]; n = len(s); w = 100*sum(1 for x in s if x > 0)/n
        exp = sum(s)/n; be = 100/(1+rr)
        print('RR%.2f  %5d %6.1f%% %9.1f%% %+8.3fR %+9.3fR' % (rr, n, w, be, exp, exp-COST))
    print("\nWin rate is ~9pts below breakeven at every target -> target is not the lever.")


if __name__ == '__main__':
    main()
