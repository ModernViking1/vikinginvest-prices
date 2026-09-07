"""Box Theory (the.rumers.trading) — disciplined backtest.

Rules: mark the PREVIOUS day's high/low as a box. On the intraday timeframe, wait for
price to reach a box edge, then for a reversal — a bar breaking the previous opposite-
colour candle (break the last RED candle's high at the box LOW -> long; break the last
GREEN candle's low at the box HIGH -> short). Stop just beyond the box edge; native
target = the FAR side of the box (variable RR). Boxes are derived from the intraday feed
itself (prior-day min low / max high). One trade per day, first trigger.

Scores TWO ways: the native far-side target (as described) AND a fixed RR2 (what the cBot
would actually execute). Per class, chronological OOS split, unresolved-within-hold
excluded. No lookahead: entry at the broken level, resolution from the next bar.

Reads m5 from m5-sample-ohlc.json (true 5-minute, the strategy's stated timeframe) and/or
m15 from historical-ohlc.json (all classes). Run: python box_theory_research.py
"""
import json
import os
from datetime import datetime, timezone
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from detect_triggers import PAIR_CLASS

_HERE = os.path.dirname(os.path.abspath(__file__))
BUF_FRAC = 0.10          # stop buffer beyond the box edge, as a fraction of box height
HOLD = 192               # max bars to resolve
RR_FIX = 2.0


def _date(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d')


def _boxes(bars):
    days = {}
    for b in bars:
        d = _date(b['_ts'])
        r = days.setdefault(d, [b['l'], b['h']])
        r[0] = min(r[0], b['l']); r[1] = max(r[1], b['h'])
    ds = sorted(days)
    return {ds[i]: (days[ds[i - 1]][0], days[ds[i - 1]][1]) for i in range(1, len(ds))}


def signals(bars):
    box = _boxes(bars)
    out = []
    cur = None; armed_l = armed_s = done = False
    last_red_h = last_grn_l = None
    for i, b in enumerate(bars):
        d = _date(b['_ts'])
        if d != cur:
            cur = d; armed_l = armed_s = done = False; last_red_h = last_grn_l = None
        bx = box.get(d)
        if bx and not done:
            lo, hi = bx; h = hi - lo
            if h > 0:
                if b['l'] <= lo:
                    armed_l = True
                if b['h'] >= hi:
                    armed_s = True
                buf = BUF_FRAC * h
                if armed_l and last_red_h is not None and b['h'] > last_red_h and (last_red_h > lo - buf):
                    out.append((i, last_red_h, lo - buf, hi, 'bull')); done = True
                elif armed_s and last_grn_l is not None and b['l'] < last_grn_l and (last_grn_l < hi + buf):
                    out.append((i, last_grn_l, hi + buf, lo, 'bear')); done = True
        if b['c'] < b['o']:
            last_red_h = b['h']
        elif b['c'] > b['o']:
            last_grn_l = b['l']
    return out


def resolve(bars, ei, entry, stop, target, d, fixed_rr=None):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = target if fixed_rr is None else (entry + fixed_rr * R if d == 'bull' else entry - fixed_rr * R)
    for j in range(ei + 1, min(ei + 1 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= tgt:
                return (tgt - entry) / R
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= tgt:
                return (entry - tgt) / R
    return None


def stat(seq):
    seq = sorted(seq); rs = [r for _, r in seq]; n = len(rs)
    if not n:
        return (0, 0, 0, 0, 0, 0)
    wr = 100 * sum(1 for x in rs if x > 0) / n
    exp = sum(rs) / n
    m = n // 2
    eh = sum(rs[:m]) / m if m else 0
    es = sum(rs[m:]) / (n - m) if n - m else 0
    wins = [x for x in rs if x > 0]
    return (n, wr, exp, eh, es, (sum(wins) / len(wins) if wins else 0))


def run(pairs_bars, label):
    print("=" * 84); print(label); print("=" * 84)
    native = defaultdict(list); fixed = defaultdict(list)
    for cls, bl in pairs_bars.items():
        for bars in bl:
            for (ei, e, s, t, d) in signals(bars):
                r = resolve(bars, ei, e, s, t, d)
                if r is not None:
                    native[cls].append((bars[ei]['_ts'], r))
                rf = resolve(bars, ei, e, s, t, d, fixed_rr=RR_FIX)
                if rf is not None:
                    fixed[cls].append((bars[ei]['_ts'], rf))
    for title, store, extra in (("Native target = far side of box (variable RR):", native, True),
                                ("Fixed RR2 target (what the cBot would execute):", fixed, False)):
        print(title)
        for cls in ['index', 'comm', 'crypto', 'major', 'minor']:
            if store.get(cls):
                n, wr, exp, eh, es, rr = stat(store[cls])
                tag = " <==+halves" if (n >= 40 and eh > 0 and es > 0 and exp > 0) else ""
                rrs = f" avgWinRR={rr:.1f}" if extra else ""
                print("  %-8s n=%4d WR=%2.0f%% exp=%+0.3fR [%+0.3f/%+0.3f]%s%s" % (cls, n, wr, exp, eh, es, rrs, tag))
        allv = [x for s in store.values() for x in s]
        n, wr, exp, eh, es, rr = stat(allv)
        print("  %-8s n=%4d WR=%2.0f%% exp=%+0.3fR [%+0.3f/%+0.3f]" % ('ALL', n, wr, exp, eh, es))


def main():
    m5p = os.path.join(_HERE, 'm5-sample-ohlc.json')
    if os.path.exists(m5p):
        d = json.load(open(m5p)).get('pairs', {})
        pb = defaultdict(list)
        for pk, v in d.items():
            bars = _bars_norm(v.get('m5', []))
            if len(bars) >= 400:
                pb[PAIR_CLASS.get(pk, '?')].append(bars)
        if pb:
            run(pb, "BOX THEORY — TRUE 5-MINUTE (m5-sample-ohlc.json), representative instruments")
            print()
    hp = os.path.join(_HERE, 'historical-ohlc.json')
    d = json.load(open(hp)).get('pairs', {})
    pb = defaultdict(list)
    for pk, cls in PAIR_CLASS.items():
        if pk in d:
            m15 = _bars_norm(d[pk].get('m15', []))
            if len(m15) >= 400:
                pb[cls].append(m15)
    run(pb, "BOX THEORY — m15, all asset classes (coarser TF, for comparison)")


if __name__ == '__main__':
    main()
