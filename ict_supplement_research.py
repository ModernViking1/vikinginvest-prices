"""Does ICT structure (BOS + FVG) work as a SUPPORTIVE confluence filter on the 10
forward-test strategies?

For every historical signal of each swing strategy we evaluate, at the signal bar
(no lookahead) on the strategy's own timeframe:
  BOS   : most-recent break-of-structure direction agrees with the trade
  FVG   : a fresh aligned 3-candle fair-value-gap formed in the last 6 bars
  BOTH  : both true
and bucket the RR2 outcome. A filter helps only if the aligned bucket beats the
strategy's baseline on expectancy AND holds on both OOS halves AND keeps enough
trades. Show-only. (rsimr scored at RR2 here for a consistent yardstick.)

Run: python ict_supplement_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, agg, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD
from ict_bos_fvg_research import structure_state
from unified_shadow_harness import (detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb,
                                     detect_s5_rsi_wide, detect_rsimr, detect_fibgz, detect_fredtl)

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
FVG_LB = 6


def fvg_recent(bars, i, d):
    """Aligned 3-candle FVG formed in bars (i-FVG_LB .. i)."""
    for j in range(max(2, i - FVG_LB + 1), i + 1):
        if d == 'bull' and bars[j-2]['h'] < bars[j]['l']:
            return True
        if d == 'bear' and bars[j-2]['l'] > bars[j]['h']:
            return True
    return False


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<14} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


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
        struct = {tf: structure_state(bars) for tf, bars in series.items()}
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
            bars = series[tf]; hold = holds[tf]; bdir, _bbar = struct[tf]
            for s in found:
                ei = bisect.bisect_left(ts[tf], s['entry_ts'])
                if ei >= len(bars) or ei < 3:
                    continue
                o = walk(bars, ei, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None:
                    continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop'])); rec = (s['entry_ts'], r)
                store[strat]['baseline'].append(rec)
                bos = bdir[ei-1] == s['dir']
                fvg = fvg_recent(bars, ei-1, s['dir'])
                if bos:
                    store[strat]['bos'].append(rec)
                if fvg:
                    store[strat]['fvg'].append(rec)
                if bos and fvg:
                    store[strat]['both'].append(rec)

    order = ['s5_rsi', 's5_rsi_wide', 'hs', 's5_engulf', 'ob', 'tl_nowick', 'fib_gz', 'w5_pullback', 'rsimr', 'fred_tl']
    print("ICT (BOS/FVG) as a supportive confluence filter on the forward-test strategies (RR2):\n")
    for strat in order:
        base = store[strat]['baseline']
        if not base:
            continue
        _, _, bexp = agg([r for _, r in base])
        print(f"  {strat}  (baseline n={len(base)} exp={bexp:+.3f}R):")
        line("BOS aligned", store[strat]['bos'])
        line("FVG present", store[strat]['fvg'])
        line("BOTH", store[strat]['both'])
        print()


if __name__ == '__main__':
    main()
