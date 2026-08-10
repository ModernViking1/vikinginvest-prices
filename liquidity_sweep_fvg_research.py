"""nefarioustrades 'fake highs/lows' — liquidity-sweep reversal with FVG logic.

The setup (bearish shown; mirror for bullish):
  1. Price sweeps a prior swing HIGH (takes buy-side liquidity) but closes back inside
     = a fake high.
  2. After the sweep, look at the displacement:
       A  NO bullish FVG forms (the up-move fails to displace) -> the sweep is fake,
          price REVERSES -> short at the failure.
       B  a BEARISH FVG forms (down-displacement), then price RETRACES up into the
          fib zone (50-61.8%) of that leg -> short the retracement, price drops further.

All price-action (FVG = a 3-candle gap; no volume/order-flow needed). Market fills,
dealing cost, bracket-honest, chronological OOS (both halves + and n>=40 = PASS). m15 +
h1, per class.

Run: python liquidity_sweep_fvg_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
SWING = 20          # swing-extreme lookback for the liquidity level
WIN = 12            # bars after the sweep to find the FVG / reversal
BUF = 0.10
FIB_LO, FIB_HI = 0.50, 0.618
HOLD = 160
RRS = [1.5, 2.0, 3.0]
CLASSES = ['crypto', 'comm', 'index', 'major', 'minor']


def bull_fvg(bars, k):
    """Bullish 3-candle FVG at k: gap between k-1 high and k+1 low."""
    return k >= 1 and k + 1 < len(bars) and bars[k + 1]['l'] > bars[k - 1]['h']


def bear_fvg(bars, k):
    """Bearish 3-candle FVG at k: gap between k-1 low and k+1 high."""
    return k >= 1 and k + 1 < len(bars) and bars[k + 1]['h'] < bars[k - 1]['l']


def sweeps(bars):
    """Yield (i, direction, level) where bar i sweeps a swing extreme and closes back inside.
    direction = 'bear' (swept a high -> look to short) or 'bull' (swept a low -> long)."""
    for i in range(SWING + 1, len(bars) - WIN - 2):
        sh = max(x['h'] for x in bars[i - SWING:i]); sl = min(x['l'] for x in bars[i - SWING:i])
        b = bars[i]
        if b['h'] > sh and b['c'] < sh:
            yield (i, 'bear', sh)
        elif b['l'] < sl and b['c'] > sl:
            yield (i, 'bull', sl)


def variant_A(bars):
    """Sweep + NO continuation FVG -> fade (reverse). Enter on the failure bar."""
    out = []; last = -1
    for (i, d, lvl) in sweeps(bars):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        if d == 'bear':
            # no bullish FVG in the sweep area AND next bar confirms down (close < sweep low)
            if bull_fvg(bars, i):
                continue
            j = i + 1
            if bars[j]['c'] < bars[i]['l']:
                entry = bars[j]['c']; stop = bars[i]['h'] + BUF * a
                if stop > entry:
                    out.append((j + 1, entry, stop, 'bear')); last = j + 1
        else:
            if bear_fvg(bars, i):
                continue
            j = i + 1
            if bars[j]['c'] > bars[i]['h']:
                entry = bars[j]['c']; stop = bars[i]['l'] - BUF * a
                if stop < entry:
                    out.append((j + 1, entry, stop, 'bull')); last = j + 1
    return out


def variant_B(bars):
    """Sweep + opposite FVG (displacement) + fib retracement into it -> continuation entry."""
    out = []; last = -1
    for (i, d, lvl) in sweeps(bars):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        if d == 'bear':
            ext = bars[i]['h']                                   # sweep high = leg top
            fvg_lo = None
            for k in range(i + 1, min(i + 1 + WIN, len(bars) - 1)):
                if bear_fvg(bars, k):
                    fvg_lo = bars[k + 1]['h']                    # top of the bearish gap
                    leg_low = min(x['l'] for x in bars[i:k + 2])
                    zlo = leg_low + FIB_LO * (ext - leg_low)
                    zhi = leg_low + FIB_HI * (ext - leg_low)
                    for r in range(k + 2, min(k + 2 + WIN, len(bars) - 1)):
                        b = bars[r]
                        if b['h'] >= zlo and b['c'] < zhi:       # retraced into fib zone, closed back
                            entry = b['c']; stop = ext + BUF * a
                            if stop > entry:
                                out.append((r + 1, entry, stop, 'bear')); last = r + 1
                            break
                        if b['h'] > ext:                          # invalidated (new high)
                            break
                    break
        else:
            ext = bars[i]['l']
            for k in range(i + 1, min(i + 1 + WIN, len(bars) - 1)):
                if bull_fvg(bars, k):
                    leg_high = max(x['h'] for x in bars[i:k + 2])
                    zhi = leg_high - FIB_LO * (leg_high - ext)
                    zlo = leg_high - FIB_HI * (leg_high - ext)
                    for r in range(k + 2, min(k + 2 + WIN, len(bars) - 1)):
                        b = bars[r]
                        if b['l'] <= zhi and b['c'] > zlo:
                            entry = b['c']; stop = ext - BUF * a
                            if stop < entry:
                                out.append((r + 1, entry, stop, 'bull')); last = r + 1
                            break
                        if b['l'] < ext:
                            break
                    break
    return out


def walk(bars, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq); m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]]); _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<10} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(fn, tf, title):
    d = json.load(open(HIST))['pairs']
    byc = {c: defaultdict(list) for c in CLASSES}
    for pk in [x for x in PAIR_CLASS if x in d]:
        bars = _bars_norm(d[pk].get(tf, []))
        if len(bars) < 400:
            continue
        for (ei, entry, stop, dr) in fn(bars):
            if ei >= len(bars):
                continue
            R = abs(entry - stop); ts = bars[ei]['_ts']
            for rr in RRS:
                o = walk(bars, ei, entry, stop, dr, rr)
                if o is not None:
                    byc[PAIR_CLASS[pk]][rr].append((ts, o - cost(o, entry, R)))
    print(f"\n===== {title} · {tf} =====")
    for c in CLASSES:
        if not byc[c][RRS[0]]:
            continue
        print(f"   {c}:")
        for rr in RRS:
            line(f'RR {rr}', byc[c][rr])


def main():
    print("=" * 92)
    print("Liquidity-sweep reversal + FVG (price-action only) · market fills · cost · OOS")
    print("=" * 92)
    for tf in ('m15', 'h1'):
        run(variant_A, tf, "A · sweep + no continuation FVG (fade)")
        run(variant_B, tf, "B · sweep + opposite FVG + fib retrace (continuation)")


if __name__ == '__main__':
    main()
