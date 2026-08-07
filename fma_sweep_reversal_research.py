"""Millionaire Trading Academy '$100 -> $1M' FMA setup — learned from the screenshots.

Gold 15-minute, ICT/SMC tooling (liquidity swings, order blocks), a 50-EMA and a
momentum oscillator. The trade shown: price sweeps a prior swing HIGH (grabs the
liquidity) into a supply zone but closes back inside, THEN — 'don't jump without
confirmation' — a market-structure shift confirms (a close back through the 50-EMA)
and you enter the reversal, stop beyond the sweep, target the opposite side of the
range. Mirror for a swept swing LOW.

Mechanised on m15: sweep of the 20-bar swing extreme that closes back inside, then a
50-EMA reclaim within a few bars = confirmation → entry; stop beyond the sweep; target
the opposite swing (range) plus a fixed-RR sweep. Market fills, dealing cost, bracket-
honest (unresolved-in-hold excluded), chronological OOS (both halves + and n>=40 =
PASS). Tested on GOLD (native m15 ~3mo AND 12-month m5-resampled m15) and every pair,
reported per class.

Run: python fma_sweep_reversal_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import ema, atr, agg, cost
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
GOLD_M5 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gold-m5-ohlc.json')

EMA_LEN = 50
SWING = 20          # swing-extreme lookback (bars)
CONFIRM = 8         # bars to wait for the 50-EMA reclaim confirmation
BUF = 0.10          # stop buffer in ATR
COOL = 4
HOLD = 192          # m15 bracket horizon (~2 days)
RRS = [1.5, 2.0, 3.0]
CLASSES = ['crypto', 'comm', 'index', 'major', 'minor']


def fma_signals(bars):
    """Yield (entry_idx, entry, stop, dir, opp_swing) sweep+EMA-reclaim reversals."""
    n = len(bars)
    e = ema([b['c'] for b in bars], EMA_LEN)
    out = []; last = -1
    for i in range(SWING + EMA_LEN, n - 1):
        if i <= last or e[i] is None:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        swing_hi = max(x['h'] for x in bars[i - SWING:i])
        swing_lo = min(x['l'] for x in bars[i - SWING:i])
        b = bars[i]
        if b['h'] > swing_hi and b['c'] < swing_hi:              # swept the high, closed back inside
            sweep = b['h']
            for j in range(i + 1, min(i + 1 + CONFIRM, n - 1)):
                if bars[j]['h'] > sweep:                          # swept again higher → invalidate
                    break
                if e[j] is not None and bars[j]['c'] < e[j]:      # 50-EMA reclaim DOWN = confirmation
                    entry = bars[j]['c']; stop = sweep + BUF * a
                    if stop > entry:
                        out.append((j + 1, entry, stop, 'bear', swing_lo)); last = j + COOL
                    break
        elif b['l'] < swing_lo and b['c'] > swing_lo:            # swept the low, closed back inside
            sweep = b['l']
            for j in range(i + 1, min(i + 1 + CONFIRM, n - 1)):
                if bars[j]['l'] < sweep:
                    break
                if e[j] is not None and bars[j]['c'] > e[j]:      # 50-EMA reclaim UP = confirmation
                    entry = bars[j]['c']; stop = sweep - BUF * a
                    if stop < entry:
                        out.append((j + 1, entry, stop, 'bull', swing_hi)); last = j + COOL
                    break
    return out


def walk(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return (target - entry) / R
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return (entry - target) / R
    return None


def resample(bars, factor):
    out = []
    for i in range(0, len(bars) - factor + 1, factor):
        g = bars[i:i + factor]
        out.append({'_ts': g[0]['_ts'], 'o': g[0]['o'], 'h': max(x['h'] for x in g),
                    'l': min(x['l'] for x in g), 'c': g[-1]['c'],
                    'v': sum((x.get('v', 0) or 0) for x in g)})
    return out


def score_all(bars, store_rr, store_rng):
    """Score every fma signal in `bars` into per-RR and range-target stores."""
    for (ei, entry, stop, d, opp) in fma_signals(bars):
        if ei >= len(bars):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        # range target = the opposite swing
        o = walk(bars, ei, entry, stop, opp, d, HOLD)
        if o is not None and ((d == 'bull' and opp > entry) or (d == 'bear' and opp < entry)):
            store_rng.append((ts, o - cost(1 if o > 0 else -1, entry, R)))
        for rr in RRS:
            tgt = entry + rr * R if d == 'bull' else entry - rr * R
            o = walk(bars, ei, entry, stop, tgt, d, HOLD)
            if o is not None:
                store_rr[rr].append((ts, o - cost(o, entry, R)))


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq); m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]]); _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<14} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def report(title, rr_store, rng_store):
    print(f"\n===== {title} =====")
    line('range-target', rng_store)
    for rr in RRS:
        line(f'RR {rr}', rr_store[rr])


def main():
    d = json.load(open(HIST))['pairs']
    print("=" * 96)
    print("FMA sweep + 50-EMA-reclaim reversal · m15 · market fills · cost · OOS (both halves + = PASS)")
    print("=" * 96)

    # ── Per class across all pairs (native m15, ~3 months each) ──
    by_class_rr = {c: defaultdict(list) for c in CLASSES}
    by_class_rng = {c: [] for c in CLASSES}
    for pk in [x for x in PAIR_CLASS if x in d]:
        bars = _bars_norm(d[pk].get('m15', []))
        if len(bars) < 400:
            continue
        score_all(bars, by_class_rr[PAIR_CLASS[pk]], by_class_rng[PAIR_CLASS[pk]])
    for c in CLASSES:
        report(f"{c.upper()} (all pairs, native m15)", by_class_rr[c], by_class_rng[c])

    # ── GOLD focus — native m15 (3mo) AND the 12-month m5-resampled m15 ──
    grr = defaultdict(list); grng = []
    score_all(_bars_norm(d['xauusd']['m15']), grr, grng)
    report("GOLD xauusd — native m15 (~3mo)", grr, grng)
    if os.path.exists(GOLD_M5):
        m5 = _bars_norm(json.load(open(GOLD_M5))['pairs']['xauusd']['m5'])
        grr2 = defaultdict(list); grng2 = []
        score_all(resample(m5, 3), grr2, grng2)
        report("GOLD xauusd — 12-month m5->m15 (full year)", grr2, grng2)


if __name__ == '__main__':
    main()
