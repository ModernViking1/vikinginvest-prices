"""Does a Stochastic filter (Bratby's one marker we don't use) add an edge to any
existing swing strategy?

For every historical signal of each swing edge we evaluate the Stochastic on the
strategy's own timeframe at the SIGNAL bar (entry_i-1, no lookahead) and bucket
the RR2 outcome by whether momentum agrees with the trade. Two filter flavours:
  mom      : %K vs %D agrees with the trade direction
  mom+room : agrees AND not already extended (bull %K<80 / bear %K>20)

A filter is only interesting if the aligned bucket beats the baseline on
expectancy AND holds up on both OOS halves AND keeps enough trades. Show-only —
nothing is wired in.

Run: python stoch_supplement_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, agg, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD
from unified_shadow_harness import detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb
from bratby_confluence_research import stochastic

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0


def stoch_dir(K, D, i, mode):
    if i < 0 or i >= len(K) or K[i] is None or D[i] is None:
        return None
    if mode == 'mom':
        return 'bull' if K[i] > D[i] else ('bear' if K[i] < D[i] else None)
    # mom+room
    if K[i] > D[i] and K[i] < 80:
        return 'bull'
    if K[i] < D[i] and K[i] > 20:
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
    # store[strat][bucket] where bucket in baseline/mom_al/mom_op/room_al/room_op
    store = defaultdict(lambda: defaultdict(list))
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80:
            continue
        b4 = agg4h(h1)
        series = {'h1': h1, '4h': b4, 'daily': daily}
        ts = {tf: [b['_ts'] for b in bars] for tf, bars in series.items()}
        stoch = {tf: stochastic(bars) for tf, bars in series.items()}
        holds = {'h1': HS_HOLD, '4h': HOLD['4h'], 'daily': 20}

        sigs = {
            'hs': (detect_hs(pk, h1, daily, draw), 'h1'),
            's5_rsi': (detect_s5(pk, h1, daily, 'rsi'), '4h'),
            's5_engulf': (detect_s5(pk, h1, daily, 'engulf'), '4h'),
            'ob': (detect_ob(pk, h1, daily), 'daily'),
            'tl_nowick': (detect_tl(pk, h1, daily), '4h'),
            'w5_pullback': (detect_w5pb(pk, h1, daily), '4h'),
        }
        for strat, (found, tf) in sigs.items():
            bars = series[tf]; K, D = stoch[tf]; hold = holds[tf]
            for s in found:
                ei = bisect.bisect_left(ts[tf], s['entry_ts'])
                if ei >= len(bars) or ei < 1:
                    continue
                o = walk(bars, ei, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None:
                    continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop']))
                rec = (s['entry_ts'], r)
                store[strat]['baseline'].append(rec)
                md = stoch_dir(K, D, ei-1, 'mom')
                store[strat]['mom_al' if md == s['dir'] else ('mom_op' if md else 'mom_neu')].append(rec)
                rd = stoch_dir(K, D, ei-1, 'room')
                store[strat]['room_al' if rd == s['dir'] else ('room_op' if rd else 'room_neu')].append(rec)

    print("Stochastic as a supplement filter on the swing edges (RR2, no lookahead):\n")
    for strat in ('hs', 's5_rsi', 's5_engulf', 'ob', 'tl_nowick', 'w5_pullback'):
        base = store[strat]['baseline']
        _, bwr, bexp = agg([r for _, r in base])
        print(f"  {strat}  (baseline n={len(base)} exp={bexp:+.3f}R):")
        line("stoch mom ALIGN", store[strat]['mom_al'])
        line("stoch mom OPPOSE", store[strat]['mom_op'])
        line("stoch mom+room ALIGN", store[strat]['room_al'])
        line("stoch mom+room OPP", store[strat]['room_op'])
        print()


if __name__ == '__main__':
    main()
