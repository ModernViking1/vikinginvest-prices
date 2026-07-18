"""Does a no-wick candle at entry work as a supportive filter on the swing edges?

For every historical signal of each swing strategy, look at the last closed bar
before entry (ei-1, no lookahead) on the strategy's own timeframe and bucket the
RR2 outcome by the no-wick candle there:

  ALIGNED  (Omar continuation): a no-wick candle in the TRADE direction
           (bull trade + bullish no-lower-wick candle / bear + bearish no-upper-wick)
  OPPOSED  (xGhozt fade thesis): a no-wick candle in the OPPOSITE direction
           (the missing wick would fill in the trade's favour)

A filter helps only if a bucket beats the strategy baseline on expectancy AND holds
on both OOS halves AND keeps enough trades. Candle logic is generic/clean-room
(no lower wick <=10% of range + body>=50%). Show-only.

Run: python nowick_candle_supplement_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, agg, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD
from unified_shadow_harness import (detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb,
                                     detect_s5_rsi_wide, detect_rsimr, detect_fibgz, detect_fredtl)

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
TOL = 0.10
BODY_MIN = 0.5


def nowick_side(b):
    rng = b['h'] - b['l']
    if rng <= 0:
        return None
    if abs(b['c'] - b['o']) < BODY_MIN * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= TOL * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= TOL * rng:
        return 'bear'
    return None


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<16} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list))
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80:
            continue
        b4 = agg4h(h1)
        series = {'h1': h1, '4h': b4, 'daily': daily}
        ts = {tf: [b['_ts'] for b in bars] for tf, bars in series.items()}
        holds = {'h1': HS_HOLD, '4h': HOLD['4h'], 'daily': 20}
        sigs = {
            'hs': (detect_hs(pk, h1, daily, draw), 'h1'),
            's5_rsi': (detect_s5(pk, h1, daily, 'rsi'), '4h'),
            's5_engulf': (detect_s5(pk, h1, daily, 'engulf'), '4h'),
            'ob': (detect_ob(pk, h1, daily), 'daily'),
            'tl_nowick': (detect_tl(pk, h1, daily), '4h'),
            'w5_pullback': (detect_w5pb(pk, h1, daily), '4h'),
            's5_rsi_wide': (detect_s5_rsi_wide(pk, h1, daily), '4h'),
            'rsimr': (detect_rsimr(pk, h1, daily), '4h'),
            'fib_gz': (detect_fibgz(pk, h1, daily), 'h1'),
            'fred_tl': (detect_fredtl(pk, h1, daily), '4h'),
        }
        for strat, (found, tf) in sigs.items():
            bars = series[tf]; hold = holds[tf]
            for s in found:
                ei = bisect.bisect_left(ts[tf], s['entry_ts'])
                if ei >= len(bars) or ei < 1:
                    continue
                o = walk(bars, ei, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None:
                    continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop'])); rec = (s['entry_ts'], r)
                store[strat]['baseline'].append(rec)
                nw = nowick_side(bars[ei-1])
                if nw == s['dir']:
                    store[strat]['aligned'].append(rec)
                elif nw is not None:
                    store[strat]['opposed'].append(rec)

    order = ['s5_rsi', 's5_rsi_wide', 'hs', 's5_engulf', 'ob', 'tl_nowick', 'fib_gz', 'w5_pullback', 'rsimr', 'fred_tl']
    print("No-wick candle at entry as a supportive filter on the swing edges (RR2):\n")
    for strat in order:
        base = store[strat]['baseline']
        if not base:
            continue
        _, _, bexp = agg([r for _, r in base])
        print(f"  {strat}  (baseline n={len(base)} exp={bexp:+.3f}R):")
        line("no-wick ALIGNED", store[strat]['aligned'])
        line("no-wick OPPOSED", store[strat]['opposed'])
        print()


if __name__ == '__main__':
    main()
