"""Selective 5-minute re-test of the INTRADAY / MICROSTRUCTURE-logic strategies only —
the ones whose edge is intrinsically about intraday timing, where finer granularity is
the point (opening-range breakouts, session reversals, VWAP reversion). Deliberately NOT
a wholesale re-run of every strategy (that's a multiple-comparisons trap); this is a
short, pre-specified list.

Reuses each strategy's existing signal generator, run on true 5m bars from
m5-sample-ohlc.json, scored at FIXED RR2 (what the cBot would execute), per asset class,
with a chronological OOS split. Flags any (strategy × class) that clears the gate
(n>=40, both OOS halves +, exp>0) — those, and only those, would be observer candidates.

Run: python intraday_5m_research.py   (needs m5-sample-ohlc.json — fetch via m5-sample-fetch)
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from detect_triggers import PAIR_CLASS
from five_strategies_research import cost
from session_2h_reversal_research import find_signals as _sess, GEO as _GEO
from astongill_orb_po3_research import orb_signals as _orb, po3_signals as _po3
from vwap_research import session_vwap as _svwap, sig_mr as _vmr, sig_tp as _vtp

_HERE = os.path.dirname(os.path.abspath(__file__))
M5 = os.path.join(_HERE, 'm5-sample-ohlc.json')
RR = 2.0
HOLD = 288          # 5m bars (~1 trading day)
US, LN = 13, 7      # session open hours (UTC)

# name -> callable(bars) -> iterable of (idx, entry, stop, dir, ...)
STRATS = {
    'sess2h_US':  lambda b: _sess(b, US, _GEO['m5']),
    'sess2h_LN':  lambda b: _sess(b, LN, _GEO['m5']),
    'orb_US':     lambda b: _orb(b, US),
    'orb_LN':     lambda b: _orb(b, LN),
    'po3_US':     lambda b: _po3(b, US),
    'po3_LN':     lambda b: _po3(b, LN),
    'vwap_mr':    lambda b: _vmr(b, _svwap(b)),
    'vwap_tp':    lambda b: _vtp(b, _svwap(b)),
}


def _norm_dir(d):
    return 'bull' if d in ('bull', 'long') else 'bear'


def score(bars, ei, entry, stop, d):
    """Fixed-RR2, NET of the desk's realistic per-side cost model. For a fast intraday
    fade the cost drag is decisive, so gross scoring is misleading — this returns the
    cost-adjusted R (o - cost) so the gate reflects what would actually be tradeable."""
    R = abs(entry - stop)
    if R <= 0 or ei >= len(bars):
        return None
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    o = None
    for j in range(ei + 1, min(ei + 1 + HOLD, len(bars))):
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
    exp = sum(rs) / n
    m = n // 2
    eh = sum(rs[:m]) / m if m else 0
    es = sum(rs[m:]) / (n - m) if n - m else 0
    return (n, wr, exp, eh, es)


def main():
    if not os.path.exists(M5):
        print("no m5-sample-ohlc.json — run m5-sample-fetch (scope=all) first")
        return
    pairs = json.load(open(M5)).get('pairs', {})
    feeds = {pk: _bars_norm(v.get('m5', [])) for pk, v in pairs.items() if len(v.get('m5', [])) >= 400}

    # results[strat][class] = [(ts, r), ...]
    results = defaultdict(lambda: defaultdict(list))
    for pk, bars in feeds.items():
        cls = PAIR_CLASS.get(pk, '?')
        for name, gen in STRATS.items():
            try:
                sigs = list(gen(bars))
            except Exception as e:
                print(f"  ({name} failed on {pk}: {e})")
                continue
            for s in sigs:
                ei, entry, stop, d = s[0], s[1], s[2], _norm_dir(s[3])
                r = score(bars, ei, entry, stop, d)
                if r is not None:
                    results[name][cls].append((bars[ei]['_ts'], r))

    print("=" * 90)
    print("INTRADAY/MICROSTRUCTURE STRATEGIES @ TRUE 5m — fixed RR2 NET OF COSTS, per class, OOS split")
    print("=" * 90)
    hits = []
    for name in STRATS:
        rowcls = results[name]
        allv = [x for s in rowcls.values() for x in s]
        n0, wr0, exp0, eh0, es0 = stat(allv)
        print(f"\n{name}   (ALL: n={n0} WR={wr0:.0f}% exp={exp0:+.3f}R [{eh0:+.3f}/{es0:+.3f}])")
        for cls in ['index', 'comm', 'crypto', 'major', 'minor']:
            if rowcls.get(cls):
                n, wr, exp, eh, es = stat(rowcls[cls])
                gate = n >= 40 and eh > 0 and es > 0 and exp > 0
                tag = "  <== GATE PASS" if gate else ""
                if gate:
                    hits.append((name, cls, n, exp, eh, es))
                print("   %-8s n=%4d WR=%2.0f%% exp=%+0.3fR [%+0.3f/%+0.3f]%s" % (cls, n, wr, exp, eh, es, tag))

    print("\n" + "=" * 90)
    if hits:
        print("GATE-PASSING cells (n>=40, both OOS halves +, exp>0) — observer candidates:")
        for name, cls, n, exp, eh, es in hits:
            print("  %-10s %-7s n=%d exp=%+0.3fR [%+0.3f/%+0.3f]" % (name, cls, n, exp, eh, es))
    else:
        print("No (strategy x class) cell clears the gate at 5m — nothing to bring in as an observer.")


if __name__ == '__main__':
    main()
