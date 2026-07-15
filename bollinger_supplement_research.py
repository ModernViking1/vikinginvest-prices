"""Does a Bollinger Band filter add an edge to any existing swing strategy?

Same method as the Stochastic supplement test. For each historical signal we
evaluate BB(20,2) on the strategy's own timeframe at the SIGNAL bar (entry_i-1,
no lookahead) and bucket the RR2 outcome three ways:

  TREND      : %B on the trade side of the mid-band (bull: close > mid / %B>0.5)
  REVERSION  : %B on the far side (bull: close < mid / %B<0.5 — stretched down)
  SQUEEZE    : bandwidth < 0.85 x its trailing-100 mean (low-vol, pre-expansion)
  WIDE       : bandwidth above the trailing mean (already-expanded vol)

A filter is worth adding only if a bucket beats baseline on expectancy AND holds
on both OOS halves AND keeps enough trades. Show-only — nothing is wired in.

Run: python bollinger_supplement_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, agg, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD
from unified_shadow_harness import detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
BB_N, BB_K = 20, 2.0


def bollinger(bars):
    c = [b['c'] for b in bars]; n = len(c)
    pctB = [None]*n; bw = [None]*n
    for i in range(BB_N-1, n):
        win = c[i-BB_N+1:i+1]; m = sum(win)/BB_N
        var = sum((x-m)**2 for x in win)/BB_N; sd = var**0.5
        up, lo = m + BB_K*sd, m - BB_K*sd
        if up > lo:
            pctB[i] = (c[i]-lo)/(up-lo)
        bw[i] = (up-lo)/m if m else None
    # trailing-100 mean of bandwidth -> squeeze flag (no lookahead)
    sq = [None]*n; acc = []
    for i in range(n):
        if bw[i] is None:
            continue
        prev = [x for x in bw[max(0, i-100):i] if x is not None]
        if prev:
            sq[i] = bw[i] < 0.85*(sum(prev)/len(prev))
    return pctB, bw, sq


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<18} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


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
        bb = {tf: bollinger(bars) for tf, bars in series.items()}
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
            bars = series[tf]; pctB, bw, sq = bb[tf]; hold = holds[tf]
            for s in found:
                ei = bisect.bisect_left(ts[tf], s['entry_ts'])
                if ei >= len(bars) or ei < 1:
                    continue
                o = walk(bars, ei, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None:
                    continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop']))
                rec = (s['entry_ts'], r); store[strat]['baseline'].append(rec)
                p = pctB[ei-1]
                if p is not None:
                    side = 'bull' if p > 0.5 else 'bear'
                    store[strat]['trend_al' if side == s['dir'] else 'trend_op'].append(rec)
                    store[strat]['rev_al' if side != s['dir'] else 'rev_op'].append(rec)
                if sq[ei-1] is True:
                    store[strat]['squeeze'].append(rec)
                elif sq[ei-1] is False:
                    store[strat]['wide'].append(rec)

    print("Bollinger Band as a supplement filter on the swing edges (RR2, no lookahead):\n")
    for strat in ('hs', 's5_rsi', 's5_engulf', 'ob', 'tl_nowick', 'w5_pullback'):
        base = store[strat]['baseline']; _, _, bexp = agg([r for _, r in base])
        print(f"  {strat}  (baseline n={len(base)} exp={bexp:+.3f}R):")
        line("TREND align", store[strat]['trend_al'])
        line("REVERSION align", store[strat]['rev_al'])
        line("SQUEEZE (low-vol)", store[strat]['squeeze'])
        line("WIDE (high-vol)", store[strat]['wide'])
        print()


if __name__ == '__main__':
    main()
