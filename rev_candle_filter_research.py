"""Does a reversal-candle entry filter improve the validated swing edges?

Entry model (user screenshot): a bullish/bearish ENGULFING, 3-BAR REVERSAL, or
PIN BAR at the confirmation candle. Tested as a read-only FILTER on each successful
swing strategy: split every signal into 'reversal-candle confirmed at the pre-entry
bar' vs ALL, and compare win rate + expectancy (the filter also cuts trade count, so
expectancy is the real test, not WR alone).

Strategies covered: the validated edges — s5_rsi, s5_rsi_wide, hs, ob, tl_nowick.
Scored at the harness's RR2 (score()), same fills/cost as production. No lookahead
(confirmation candle is the bar BEFORE entry).

Run: python rev_candle_filter_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h
from unified_shadow_harness import (
    detect_s5, detect_hs, detect_ob, detect_tl, detect_s5_rsi_wide, score, cost, RR,
)

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
HS_HOLD = 60


def is_engulf_dir(bars, i, d):
    if i < 1: return False
    o, c = bars[i]['o'], bars[i]['c']; po, pc = bars[i-1]['o'], bars[i-1]['c']
    lo1, hi1 = min(po, pc), max(po, pc)
    if d == 'bull': return c > o and o <= lo1 and c >= hi1
    return c < o and o >= hi1 and c <= lo1


def is_pin_dir(b, d):
    rng = b['h'] - b['l']
    if rng <= 0: return False
    if d == 'bull': return (min(b['o'], b['c']) - b['l']) >= 0.5*rng
    return (b['h'] - max(b['o'], b['c'])) >= 0.5*rng


def is_3bar_dir(bars, i, d):
    if i < 2: return False
    if d == 'bull':
        return bars[i-2]['c'] < bars[i-2]['o'] and bars[i]['c'] > bars[i]['o'] and bars[i]['c'] > bars[i-2]['h']
    return bars[i-2]['c'] > bars[i-2]['o'] and bars[i]['c'] < bars[i]['o'] and bars[i]['c'] < bars[i-2]['l']


def rev_candle(bars, i, d):
    if i < 0 or i >= len(bars): return False
    return is_engulf_dir(bars, i, d) or is_pin_dir(bars[i], d) or is_3bar_dir(bars, i, d)


def agg(rows):
    r = [x for _, x in rows]; n = len(r)
    if not n: return (0, 0.0, 0.0)
    w = sum(1 for x in r if x > 0)
    return (n, 100*w/n, sum(r)/n)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    ALL = defaultdict(list); CONF = defaultdict(list)
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1)
        tfbars = {'h1': h1, '4h': b4, 'daily': daily}
        tfts = {k: [b['_ts'] for b in v] for k, v in tfbars.items()}
        draw = pairs[pk].get('daily', [])
        found = {}
        found['s5_rsi'] = detect_s5(pk, h1, daily, 'rsi')
        found['s5_rsi_wide'] = detect_s5_rsi_wide(pk, h1, daily)
        found['hs'] = detect_hs(pk, h1, daily, draw)
        found['ob'] = detect_ob(pk, h1, daily)
        found['tl_nowick'] = detect_tl(pk, h1, daily)
        for strat, sigs in found.items():
            for s in sigs:
                tf = s['tf']; bars = tfbars[tf]; ts = tfts[tf]
                hold = HS_HOLD if tf == 'h1' else (60 if tf == '4h' else 20)
                st, o = score(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'], hold)
                if st != 'resolved': continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop']))
                ci = bisect.bisect_left(ts, s['entry_ts'])           # entry bar index
                confirmed = rev_candle(bars, ci-1, s['dir'])          # candle BEFORE entry
                ALL[strat].append((s['entry_ts'], r))
                if confirmed:
                    CONF[strat].append((s['entry_ts'], r))

    print("Reversal-candle entry filter on the validated swing edges (RR2, resolved)\n")
    print(f"  {'strategy':<13} {'ALL n / WR / exp':<26}   {'CONFIRMED n / WR / exp':<26}  lift")
    for strat in ('s5_rsi', 's5_rsi_wide', 'hs', 'ob', 'tl_nowick'):
        na, wa, ea = agg(ALL[strat]); nc, wc, ec = agg(CONF[strat])
        keep = 100*nc/na if na else 0
        lift = ec - ea
        print(f"  {strat:<13} n={na:>4} WR={wa:>4.1f}% exp={ea:>+6.3f}   "
              f"n={nc:>4} WR={wc:>4.1f}% exp={ec:>+6.3f}   "
              f"Δexp={lift:>+6.3f} (kept {keep:>4.1f}%)")


if __name__ == '__main__':
    main()
