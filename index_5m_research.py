"""Backtest the desk's INDEX reversal strategies (sweepfvg = liquidity_sweep_fvg variant_B,
crt = crt_signals) on true 5m vs their current m15, NET OF COSTS, per index pocket — to see
whether a 5-minute variant beats the m15 version (a separate question from the fill/cadence
issue). If a 5m variant clears the net gate, it's a forward-test (5m-index observer) candidate.

5m from m5-sample-ohlc.json (scope=all fetch includes indices); m15 from historical-ohlc.json.
crt needs daily pivots — derived from each feed's own prior-day aggregation. Fixed RR2, hold
~2 trading days, chronological OOS split, cost-adjusted. Run: python index_5m_research.py
"""
import json
import os
from datetime import datetime, timezone
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from detect_triggers import PAIR_CLASS
from five_strategies_research import cost
from liquidity_sweep_fvg_research import variant_B as sweepfvg_sig
from crt_research import crt_signals

_HERE = os.path.dirname(os.path.abspath(__file__))
IDX = [p for p, c in PAIR_CLASS.items() if c == 'index']
RR = 2.0


def _daily(bars):
    """Aggregate intraday bars to daily OHLC (for crt's swing pivots)."""
    days = {}
    for b in bars:
        d = datetime.fromtimestamp(b['_ts'], timezone.utc).strftime('%Y-%m-%d')
        r = days.get(d)
        if r is None:
            days[d] = {'o': b['o'], 'h': b['h'], 'l': b['l'], 'c': b['c'], '_ts': b['_ts'], 't': d}
        else:
            r['h'] = max(r['h'], b['h']); r['l'] = min(r['l'], b['l']); r['c'] = b['c']
    return [days[d] for d in sorted(days)]


def score_net(bars, ei, entry, stop, d, hold):
    R = abs(entry - stop)
    if R <= 0 or ei >= len(bars):
        return None
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    o = None
    for j in range(ei + 1, min(ei + 1 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                o = -1.0; break
            if b['h'] >= tgt:
                o = RR; break
        else:
            if b['h'] >= stop:
                o = -1.0; break
            if b['l'] <= tgt:
                o = RR; break
    if o is None:
        return None
    return o - cost(o, entry, R)


def stat(seq):
    seq = sorted(seq); rs = [r for _, r in seq]; n = len(rs)
    if not n:
        return (0, 0, 0, 0, 0)
    wr = 100 * sum(1 for x in rs if x > 0) / n
    exp = sum(rs) / n; m = n // 2
    return (n, wr, exp, sum(rs[:m]) / m if m else 0, sum(rs[m:]) / (n - m) if n - m else 0)


def run_tf(feeds, hold, label):
    print("\n" + "=" * 80)
    print("INDEX reversal strategies — %s, NET OF COSTS, fixed RR2" % label)
    print("=" * 80)
    for name, gen in (('sweepfvg', 'sweepfvg'), ('crt', 'crt')):
        agg = []
        per = {}
        for pk, bars in feeds.items():
            if len(bars) < 400:
                continue
            if name == 'sweepfvg':
                sigs = [(s[0], s[1], s[2], s[3]) for s in sweepfvg_sig(bars)]
            else:
                sigs = [(s[0], s[1], s[2], s[3]) for s in crt_signals(bars, _daily(bars))]
            rs = []
            for (ei, e, s, d) in sigs:
                r = score_net(bars, ei, e, s, d, hold)
                if r is not None:
                    rs.append((bars[ei]['_ts'], r)); agg.append((bars[ei]['_ts'], r))
            per[pk] = rs
        n, wr, exp, eh, es = stat(agg)
        tag = "  <== NET GATE" if (n >= 40 and eh > 0 and es > 0 and exp > 0) else ""
        print("\n%-9s ALL n=%4d WR=%2.0f%% net=%+0.3fR [%+0.3f/%+0.3f]%s" % (name, n, wr, exp, eh, es, tag))
        for pk in IDX:
            if per.get(pk):
                n, wr, exp, eh, es = stat(per[pk])
                print("   %-8s n=%3d WR=%2.0f%% net=%+0.3fR [%+0.3f/%+0.3f]" % (pk, n, wr, exp, eh, es))


def main():
    m5p = os.path.join(_HERE, 'm5-sample-ohlc.json')
    if os.path.exists(m5p):
        d = json.load(open(m5p)).get('pairs', {})
        feeds = {pk: _bars_norm(d[pk].get('m5', [])) for pk in IDX if pk in d and len(d[pk].get('m5', [])) >= 400}
        if feeds:
            run_tf(feeds, hold=576, label="TRUE 5-MINUTE (~2-day hold)")
    hp = os.path.join(_HERE, 'historical-ohlc.json')
    d = json.load(open(hp)).get('pairs', {})
    feeds = {pk: _bars_norm(d[pk].get('m15', [])) for pk in IDX if pk in d and len(d[pk].get('m15', [])) >= 400}
    run_tf(feeds, hold=192, label="m15 baseline (~2-day hold)")


if __name__ == '__main__':
    main()
