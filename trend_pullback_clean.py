"""Clean-target variant of the trend-pullback system: SAME entry (daily EMA200 trend
+ EMA20 pullback + RSI>50 + ADX>=22 + rejection-candle stop-break), but exit purely
at a fixed RR target or stop — no partials, no breakeven, no trend-failure exit. Tests
whether the raw entry carries an edge the conservative management was capping.

Run: python trend_pullback_clean.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import agg4h, ema, atr, adx, agg, cost
from trend_pullback_research import (
    TREND_BUF, ENTRY_BUF, STOP_BUF, RSI_MIN, ADX_MIN, SETUP_LB, TRIG_WIN, HOLD,
)

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RRS = [1.5, 2.0, 3.0]


def walk_clean(bars, ei, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(ei, min(ei+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(ebars, daily, tf, store, cls, store_cls):
    ec = [b['c'] for b in ebars]; ema20 = ema(ec, 20); rsi = precompute_rsi(ec, 14)
    dc = [b['c'] for b in daily]; ema200d = ema(dc, 200); dts = [b['_ts'] for b in daily]
    n = len(ebars); last = -1
    for i in range(SETUP_LB+2, n-1):
        if i <= last or ema20[i] is None or rsi[i] is None:
            continue
        a = atr(ebars, 14, i) or 0.0
        if a <= 0:
            continue
        di = bisect.bisect_right(dts, ebars[i]['_ts'] - 86400) - 1
        if di < 0 or ema200d[di] is None:
            continue
        ad_ = atr(daily, 14, di) or 0.0
        trend_up = dc[di] > ema200d[di] + TREND_BUF*ad_
        trend_dn = dc[di] < ema200d[di] - TREND_BUF*ad_
        ax = adx(ebars, 14, i)
        if ax is None or ax < ADX_MIN:
            continue
        b = ebars[i]; sl = min(x['l'] for x in ebars[i-SETUP_LB+1:i+1]); sh = max(x['h'] for x in ebars[i-SETUP_LB+1:i+1])
        d = None
        if (trend_up and rsi[i] > RSI_MIN and b['l'] <= ema20[i] + ENTRY_BUF*a
                and b['c'] > b['o'] and b['c'] > ebars[i-1]['h'] and b['c'] > ema20[i]):
            d = 'bull'; trig = b['h']; stop = min(b['l'], sl) - STOP_BUF*a
        elif (trend_dn and rsi[i] < RSI_MIN and b['h'] >= ema20[i] - ENTRY_BUF*a
                and b['c'] < b['o'] and b['c'] < ebars[i-1]['l'] and b['c'] < ema20[i]):
            d = 'bear'; trig = b['l']; stop = max(b['h'], sh) + STOP_BUF*a
        if d is None:
            continue
        ei = None; entry = None
        for j in range(i+1, min(i+1+TRIG_WIN, n)):
            if d == 'bull' and ebars[j]['h'] >= trig:
                ei = j; entry = max(trig, ebars[j]['o']); break
            if d == 'bear' and ebars[j]['l'] <= trig:
                ei = j; entry = min(trig, ebars[j]['o']); break
            if (d == 'bull' and ebars[j]['c'] < ema20[j]) or (d == 'bear' and ebars[j]['c'] > ema20[j]):
                break
        if ei is None or (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        R = abs(entry-stop); ts = ebars[ei]['_ts']
        for rr in RRS:
            o = walk_clean(ebars, ei, entry, stop, d, rr, HOLD[tf])
            if o is not None:
                store[(tf, rr)].append((ts, o - cost(o, entry, R)))
                store_cls[cls][(tf, rr)].append((ts, o - cost(o, entry, R)))
        last = ei + 2


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100/(1+rr)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 220:
            continue
        npairs += 1
        scan(agg4h(h1), daily, '4h', store, cls, store_cls)
        scan(daily, daily, '1d', store, cls, store_cls)

    print(f"Trend-pullback CLEAN target (no partials/trend-exit) — {npairs} pairs\n")
    for tf in ('4h', '1d'):
        for rr in RRS:
            line(f"{tf} RR{rr}", store[(tf, rr)], rr)
        print()
    print("=== per class (4H, RR2) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c][('4h', 2.0)], 2.0)
    print("=== per class (1D, RR2) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c][('1d', 2.0)], 2.0)


if __name__ == '__main__':
    main()
