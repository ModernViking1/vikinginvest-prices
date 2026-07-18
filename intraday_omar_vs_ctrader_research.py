"""Omar no-wick 15m 1:1 (standalone) vs the LIVE real cTrader intraday results.

Omar's '90% no-wick strategy': a decisive m15 candle with no wick on the open side
is momentum — trade WITH it, 1:1 R:R, stop at the candle's wickless origin.

We run that model mechanically (next-bar-open fill, structural stop = candle
extreme, RR=1.0, realistic cost) and put it side-by-side with what our own intraday
system actually did on the real cTrader demo (243 paired trades). Three cuts of the
Omar model: all pairs full-history, the 32 traded pairs full-history, and the same
pairs over the recent ~30-day window that overlaps the live cTrader sample.

Candle shape is generic/clean-room. Not Omar's protected material.

Run: python intraday_omar_vs_ctrader_research.py
"""
import json, collections, bisect
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

RR = 1.0
HOLD = 40
COST = 0.10
TOL = 0.10
BODY_MIN = 0.5
BUF = 0.10          # stop buffer as fraction of ATR-less range proxy (candle range)
COOLDOWN = 3


def nowick_side(b):
    rng = b['h'] - b['l']
    if rng <= 0 or abs(b['c'] - b['o']) < BODY_MIN * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= TOL * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= TOL * rng:
        return 'bear'
    return None


def walk(bars, i0, entry, stop, d):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + RR*R if d == 'bull' else entry - RR*R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        x = bars[j]
        if d == 'bull':
            if x['l'] <= stop: return -1.0
            if x['h'] >= tgt: return RR
        else:
            if x['h'] >= stop: return -1.0
            if x['l'] <= tgt: return RR
    return None


def run_omar(m15map, pairs=None, since_ts=None):
    out = []
    for pk, bars in m15map.items():
        if pairs is not None and pk not in pairs:
            continue
        n = len(bars); last = -1
        for i in range(5, n - 1):
            if i <= last:
                continue
            side = nowick_side(bars[i])
            if side is None:
                continue
            ei = i + 1
            if since_ts and bars[ei]['_ts'] < since_ts:
                continue
            entry = bars[ei]['o']; rng = bars[i]['h'] - bars[i]['l']
            stop = (bars[i]['l'] - BUF*rng) if side == 'bull' else (bars[i]['h'] + BUF*rng)
            if (side == 'bull' and stop >= entry) or (side == 'bear' and stop <= entry):
                continue
            o = walk(bars, ei, entry, stop, side)
            if o is not None:
                out.append(o)
            last = ei + COOLDOWN
    return out


def rep(label, seq, note=''):
    n = len(seq)
    if not n:
        print(f"  {label:<34} (no trades)"); return
    w = sum(1 for x in seq if x > 0); e = sum(seq)/n
    print(f"  {label:<34} n={n:>5}  WR={100*w/n:>5.1f}%  exp={e:>+7.3f}R  net(-cost)={e-COST:>+7.3f}R  {note}")


def main():
    hist = json.load(open('historical-ohlc.json'))['pairs']
    m15 = {pk: _bars_norm(v.get('m15', [])) for pk, v in hist.items()
           if v.get('m15') and len(_bars_norm(v.get('m15', []))) > 200}

    # real cTrader intraday
    ex = json.load(open('executions.json')); rows = ex.get('executions', ex)
    byid = collections.defaultdict(dict)
    for r in rows:
        if r.get('position_id') and r.get('event') in ('placed', 'closed'):
            byid[r['position_id']][r['event']] = r
    P = [v for v in byid.values() if 'placed' in v and 'closed' in v]
    traded = set(v['placed'].get('pair') for v in P if v['placed'].get('pair'))
    real_realized = [v['closed'].get('realized_r', 0) for v in P]
    # earliest live signal ts (for the period-matched Omar cut)
    live_ts = [v['placed'].get('ts', 0)/1000.0 for v in P if v['placed'].get('ts')]
    since = min(live_ts) if live_ts else None

    print("OMAR no-wick 15m 1:1 (backtest, realistic fills) vs LIVE real cTrader intraday\n")
    print("Omar no-wick model:")
    rep("all pairs, full history", run_omar(m15))
    rep("traded pairs, full history", run_omar(m15, pairs=traded))
    rep("traded pairs, live window", run_omar(m15, pairs=traded, since_ts=since))
    print("\nReal cTrader intraday (our 4/4 system, actual demo fills):")
    n = len(real_realized); w = sum(1 for x in real_realized if x > 0)
    print(f"  {'realized (ground truth)':<34} n={n:>5}  WR={100*w/n:>5.1f}%  exp={sum(real_realized)/n:>+7.3f}R  (real spread/slippage/commission baked in)")
    print(f"\n  Omar's advertised claim: ~90% win rate. Reality of a 1:1 no-wick model: coin-flip.")


if __name__ == '__main__':
    main()
