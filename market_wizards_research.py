"""Market Wizards setups + the volume-breakout 'breakthrough' — tested with discipline.

  turtle_soup   Raschke — fade a false breakout of the 20-bar extreme (new low then
                close back above the violated level -> long; mirror for shorts).
  holy_grail    Raschke — ADX>25 (strong trend) + pullback that tags the 20-EMA and
                closes back through it, in the trend direction.
  two_b         Sperandeo — a marginal new high/low that fails and closes back inside
                the prior swing -> reverse.
  volbreak      "Breakthrough": price breaks the 20-bar resistance by a margin WITH
                volume >= 1.5x its 20-bar average -> enter, stop below the breakout bar.
  volbreak+200  same, with the Paul Tudor Jones 200-period trend-regime filter (only
                long above the 200-MA, short below).

All on h1, RR2, MARKET fills at the signal-bar close, dealing cost, bracket-honest
(unresolved-in-hold excluded), chronological OOS. Volume setups: crypto = real
Coinbase volume (honest), others = OANDA tick volume. n>=40 + both OOS halves + = PASS.

Run: python market_wizards_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost, ema, adx
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
LOOK = 20
RECLAIM = 8
COOL = 3
HOLD = 80
RR = 2.0
VOLMULT = 1.5
CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd', 'suiusd', 'taousd', 'nearusd'}


def turtle_soup(bars):
    out = []; n = len(bars); last = -1
    for i in range(LOOK + 5, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        ll = min(x['l'] for x in bars[i - LOOK:i]); hh = max(x['h'] for x in bars[i - LOOK:i])
        if bars[i]['l'] < ll:                                   # false breakdown
            for j in range(i + 1, min(i + 1 + RECLAIM, n - 1)):
                if bars[j]['c'] > ll:
                    e, s = bars[j]['c'], bars[i]['l'] - 0.1 * a
                    if s < e: out.append((j + 1, e, s, 'bull')); last = j + COOL
                    break
                if bars[j]['c'] < bars[i]['l']:
                    break
        elif bars[i]['h'] > hh:                                 # false breakout
            for j in range(i + 1, min(i + 1 + RECLAIM, n - 1)):
                if bars[j]['c'] < hh:
                    e, s = bars[j]['c'], bars[i]['h'] + 0.1 * a
                    if s > e: out.append((j + 1, e, s, 'bear')); last = j + COOL
                    break
                if bars[j]['c'] > bars[i]['h']:
                    break
    return out


def holy_grail(bars):
    out = []; n = len(bars); last = -1
    c = [x['c'] for x in bars]; e20 = ema(c, 20); e50 = ema(c, 50)
    for i in range(55, n - 1):
        if i <= last or e20[i] is None or e50[i] is None:
            continue
        a = atr(bars, 14, i) or 0.0
        ad = adx(bars, 14, i)
        if a <= 0 or ad is None or ad < 25:
            continue
        b = bars[i]
        if e20[i] > e50[i] and b['l'] <= e20[i] and b['c'] > e20[i]:
            s = min(x['l'] for x in bars[max(0, i - 3):i + 1]) - 0.1 * a
            if s < b['c']: out.append((i + 1, b['c'], s, 'bull')); last = i + COOL
        elif e20[i] < e50[i] and b['h'] >= e20[i] and b['c'] < e20[i]:
            s = max(x['h'] for x in bars[max(0, i - 3):i + 1]) + 0.1 * a
            if s > b['c']: out.append((i + 1, b['c'], s, 'bear')); last = i + COOL
    return out


def two_b(bars, PIV=10):
    out = []; n = len(bars); last = -1
    for i in range(PIV + 5, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        ph = max(x['h'] for x in bars[i - PIV:i - 1]); pl = min(x['l'] for x in bars[i - PIV:i - 1])
        b = bars[i]
        if b['h'] > ph and b['c'] < ph:
            if b['h'] + 0.1 * a > b['c']: out.append((i + 1, b['c'], b['h'] + 0.1 * a, 'bear')); last = i + COOL
        elif b['l'] < pl and b['c'] > pl:
            if b['l'] - 0.1 * a < b['c']: out.append((i + 1, b['c'], b['l'] - 0.1 * a, 'bull')); last = i + COOL
    return out


def volbreak(bars, regime=False):
    out = []; n = len(bars); last = -1
    c = [x['c'] for x in bars]; e200 = ema(c, 200) if regime else None
    start = 200 if regime else LOOK
    for i in range(start + 1, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        avgv = sum((x.get('v', 0) or 0) for x in bars[i - 20:i]) / 20
        if avgv <= 0 or (bars[i].get('v', 0) or 0) < VOLMULT * avgv:   # 1.5x volume gate
            continue
        res = max(x['h'] for x in bars[i - LOOK:i]); sup = min(x['l'] for x in bars[i - LOOK:i])
        b = bars[i]
        if b['c'] > res + 0.1 * a:                                     # break resistance by a margin
            if regime and e200[i] is not None and b['c'] < e200[i]:
                continue
            e, s = b['c'], b['l'] - 0.1 * a                            # stop below the breakout bar
            if s < e: out.append((i + 1, e, s, 'bull')); last = i + COOL
        elif b['c'] < sup - 0.1 * a:
            if regime and e200[i] is not None and b['c'] > e200[i]:
                continue
            e, s = b['c'], b['h'] + 0.1 * a
            if s > e: out.append((i + 1, e, s, 'bear')); last = i + COOL
    return out


def walk(bars, ei, entry, stop, d):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    for j in range(ei, min(ei + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return RR
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return RR
    return None


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq); m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]]); _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<12} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


DETECTORS = [('turtle_soup', turtle_soup), ('holy_grail', holy_grail), ('two_b', two_b),
             ('volbreak', volbreak), ('volbreak+200', lambda b: volbreak(b, regime=True))]


def main():
    d = json.load(open(HIST))['pairs']
    for name, fn in DETECTORS:
        allc = defaultdict(list)
        for pk in [x for x in PAIR_CLASS if x in d]:
            cls = PAIR_CLASS.get(pk)
            bars = _bars_norm(d[pk].get('h1', []))
            if len(bars) < 400:
                continue
            for (ei, entry, stop, dr) in fn(bars):
                if ei >= len(bars):
                    continue
                o = walk(bars, ei, entry, stop, dr)
                if o is not None:
                    allc[cls].append((bars[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
        print(f"\n===== {name} (h1, RR2, cost, OOS) =====")
        for cls in ['crypto', 'index', 'comm', 'major', 'minor']:
            if allc[cls]:
                line(cls, allc[cls])
        line('ALL', [r for c in allc for r in allc[c]])


if __name__ == '__main__':
    main()
