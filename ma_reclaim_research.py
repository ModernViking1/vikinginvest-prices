"""50-period EMA reclaim — the author's own long-taught signal (tariff-shock note, p.82).

"Price reclaimed the red 50-period moving average on the weekly timeframe and held above it
— the same signal I have used and taught for years."

Weekly is NOT testable here (daily history ~36 weeks; a 50-week EMA can't even seed), so this
tests the identical signal on the DAILY timeframe (a tradeable-frequency proxy for the same
regime idea) and on 4h for sample size. Two honest framings:

  (A) Reclaim as an ENTRY: prior close below the 50-EMA, current close back above (bull; mirror
      for bear); enter next bar at MARKET; stop below the pre-reclaim swing low. Two exits —
      fixed 2:1, and a REGIME exit (ride until price closes back through the 50-EMA the other
      way), which is closer to how a trend signal is actually used.

  (B) Reclaim side as a FILTER on our continuation edge: does aligning obfvg entries with the
      daily-50-EMA side improve expectancy vs trading against it? (This is how the book uses
      it — a regime read, not a trigger.)

Realistic cost, chronological OOS (both halves + n>=40 = PASS), per class.

Run: python ma_reclaim_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import _obfvg_signals
from five_strategies_research import ema, atr, agg, cost, agg4h

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
MA = 50
SWING = 10        # swing-low/high lookback for the stop
BUF = 0.25        # stop buffer (ATR)
HOLD = 60         # bars to reach a fixed target / regime timeout
COOLDOWN = 5
RR = 2.0


def reclaims(bars):
    """Yield (i, dir): a fresh close back across the 50-EMA."""
    e = ema([b['c'] for b in bars], MA); out = []; last = -1
    for i in range(MA + 1, len(bars) - 1):
        if i <= last or e[i] is None or e[i - 1] is None:
            continue
        if bars[i - 1]['c'] < e[i - 1] and bars[i]['c'] > e[i]:
            out.append((i, 'bull')); last = i + COOLDOWN
        elif bars[i - 1]['c'] > e[i - 1] and bars[i]['c'] < e[i]:
            out.append((i, 'bear')); last = i + COOLDOWN
    return out, e


def score_fixed(bars, i, d, e):
    a = atr(bars, 14, i) or 0.0
    if a <= 0 or i + 1 >= len(bars):
        return None
    entry = bars[i + 1]['o']
    if d == 'bull':
        stop = min(b['l'] for b in bars[max(0, i - SWING):i + 1]) - BUF * a
        if stop >= entry: return None
    else:
        stop = max(b['h'] for b in bars[max(0, i - SWING):i + 1]) + BUF * a
        if stop <= entry: return None
    R = abs(entry - stop); tgt = entry + RR * R if d == 'bull' else entry - RR * R
    for j in range(i + 1, min(i + 1 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return (-1.0, entry, R)
            if b['h'] >= tgt: return (RR, entry, R)
        else:
            if b['h'] >= stop: return (-1.0, entry, R)
            if b['l'] <= tgt: return (RR, entry, R)
    return None


def score_regime(bars, i, d, e):
    """Ride until price closes back through the 50-EMA the other way (or timeout). R measured
    vs the same swing stop; result reported as the R-multiple achieved at the regime exit."""
    a = atr(bars, 14, i) or 0.0
    if a <= 0 or i + 1 >= len(bars):
        return None
    entry = bars[i + 1]['o']
    if d == 'bull':
        stop = min(b['l'] for b in bars[max(0, i - SWING):i + 1]) - BUF * a
        if stop >= entry: return None
    else:
        stop = max(b['h'] for b in bars[max(0, i - SWING):i + 1]) + BUF * a
        if stop <= entry: return None
    R = abs(entry - stop)
    for j in range(i + 1, min(i + 1 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return (-1.0, entry, R)
            if e[j] is not None and b['c'] < e[j]:
                return ((b['c'] - entry) / R, entry, R)
        else:
            if b['h'] >= stop: return (-1.0, entry, R)
            if e[j] is not None and b['c'] > e[j]:
                return ((entry - b['c']) / R, entry, R)
    last = bars[min(i + HOLD, len(bars) - 1)]['c']
    return (((last - entry) if d == 'bull' else (entry - last)) / R, entry, R)


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<10} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run_entry(tf, exit_mode):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        bars = agg4h(h1) if tf == '4h' else _bars_norm(pairs[pk].get('daily', []))
        if len(bars) < MA + 60:
            continue
        npr += 1
        recs, e = reclaims(bars)
        for i, dr in recs:
            res = (score_fixed if exit_mode == 'fixed' else score_regime)(bars, i, dr, e)
            if res is not None:
                o, entry, R = res
                store[cls].append((bars[i]['_ts'], o - cost(o, entry, R)))
    print(f"\n===== 50-EMA reclaim entry · {tf} · exit={exit_mode} — {npr} pairs =====")
    for c in ['index', 'major', 'minor', 'comm', 'crypto']:
        if store[c]:
            line(c, store[c])
    line('ALL', [r for c in store for r in store[c]])


def run_filter():
    """Does the daily-50-EMA side improve obfvg continuation entries?"""
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    aligned = defaultdict(list); counter = defaultdict(list)
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 500 or len(daily) < MA + 20:
            continue
        de = ema([b['c'] for b in daily], MA); dts = [b['_ts'] for b in daily]
        for s in _obfvg_signals(pk, h1, 'x'):
            di = bisect.bisect_right(dts, s['entry_ts']) - 1
            if di < MA or de[di] is None:
                continue
            side_up = daily[di]['c'] > de[di]
            al = (side_up and s['dir'] == 'bull') or ((not side_up) and s['dir'] == 'bear')
            # score at fixed RR2 via a quick bracket on h1
            hts = [b['_ts'] for b in h1]; i0 = bisect.bisect_left(hts, s['entry_ts'])
            R = abs(s['entry'] - s['stop'])
            if R <= 0 or i0 >= len(h1): continue
            tgt = s['entry'] + RR * R if s['dir'] == 'bull' else s['entry'] - RR * R
            o = None
            for j in range(i0, min(i0 + 120, len(h1))):
                b = h1[j]
                if s['dir'] == 'bull':
                    if b['l'] <= s['stop']: o = -1.0; break
                    if b['h'] >= tgt: o = RR; break
                else:
                    if b['h'] >= s['stop']: o = -1.0; break
                    if b['l'] <= tgt: o = RR; break
            if o is None: continue
            net = o - cost(o, s['entry'], R)
            (aligned if al else counter)[cls].append((s['entry_ts'], net))
    print("\n===== daily-50-EMA side as a FILTER on obfvg continuation (RR2) =====")
    for label, store in (('ALIGNED (with 50-EMA)', aligned), ('COUNTER (against)', counter)):
        print(f"  {label}")
        for c in ['index', 'major', 'minor', 'comm', 'crypto']:
            if store[c]:
                line(c, store[c])
        line('ALL', [r for c in store for r in store[c]])


def main():
    print("=" * 92)
    print("50-EMA reclaim (author's own signal, p.82) — daily proxy for the weekly claim")
    print("=" * 92)
    run_entry('daily', 'fixed')
    run_entry('daily', 'regime')
    run_entry('4h', 'fixed')
    run_filter()


if __name__ == '__main__':
    main()
