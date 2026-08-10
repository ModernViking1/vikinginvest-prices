"""Whale Pivot Model (POC / Delta) — the ACHIEVABLE approximation, tested with discipline.

The footprint model in the screenshots (Absorption, Aggression, Delta Flip) needs bid-vs-
ask volume PER PRICE LEVEL (order flow / tick data). Our bars are OHLC + one total volume,
so absorption and aggression cannot be computed at all. What IS approximable:

  POC        rolling volume-profile Point of Control (highest-volume price node over a
             lookback). Author's premise: when price moves AWAY from the POC, expect a move.
  Delta      PROXY only: signed volume = volume x sign(close-open). Cumulative-delta flip =
             the running signed-volume sum crossing zero. NOT true footprint delta.

Tests:
  A  POC-breakout standalone — close clears POC by k*ATR -> enter that way, stop at POC side.
  B  POC-breakout + delta-proxy alignment (does the delta confirmation help?).
  C  Delta-proxy alignment as a supporting FILTER on the FMA sweep-reversal (the edge that
     already works) — does footprint-style confirmation lift it?

Market fills, dealing cost, bracket-honest, chronological OOS (both halves + and n>=40 =
PASS). m15 + h1, per class. Crypto = real volume (honest); others = tick-volume proxy.

Run: python volume_footprint_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
LOOK = 50          # volume-profile lookback (bars)
BINS = 24          # price bins for the profile
BREAK_K = 0.5      # ATR distance beyond the POC to call a breakout
DELTA_LB = 20      # cumulative-delta-proxy window
COOL = 3
HOLD = 160
RRS = [1.5, 2.0]
CLASSES = ['crypto', 'comm', 'index', 'major', 'minor']


def poc(bars, i, look=LOOK, bins=BINS):
    """Point of Control over bars[i-look:i] — bin the range, spread each bar's volume across
    its H-L, return the mid-price of the highest-volume bin."""
    seg = bars[i - look:i]
    lo = min(b['l'] for b in seg); hi = max(b['h'] for b in seg)
    if hi <= lo:
        return None
    w = (hi - lo) / bins
    prof = [0.0] * bins
    for b in seg:
        v = b.get('v', 0) or 0.0
        blo = int((b['l'] - lo) / w); bhi = int((b['h'] - lo) / w)
        blo = max(0, min(bins - 1, blo)); bhi = max(0, min(bins - 1, bhi))
        span = bhi - blo + 1
        for j in range(blo, bhi + 1):
            prof[j] += v / span
    k = max(range(bins), key=lambda j: prof[j])
    return lo + (k + 0.5) * w


def poc_signals(bars, use_delta=False):
    """POC-breakout: close clears the POC by BREAK_K*ATR -> momentum entry, stop at the POC."""
    out = []; n = len(bars); last = -1
    for i in range(LOOK + 1, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        pc = poc(bars, i)
        if pc is None:
            continue
        c = bars[i]['c']
        if use_delta:
            d = sum((bars[j].get('v', 0) or 0) * (1 if bars[j]['c'] >= bars[j]['o'] else -1)
                    for j in range(i - DELTA_LB, i))
        else:
            d = None
        if c > pc + BREAK_K * a:                       # broke ABOVE the POC = up move
            if use_delta and d is not None and d <= 0:
                continue                               # delta-proxy must confirm (net buying)
            entry = c; stop = pc - 0.1 * a
            if stop < entry:
                out.append((i + 1, entry, stop, 'bull')); last = i + COOL
        elif c < pc - BREAK_K * a:                     # broke BELOW = down move
            if use_delta and d is not None and d >= 0:
                continue
            entry = c; stop = pc + 0.1 * a
            if stop > entry:
                out.append((i + 1, entry, stop, 'bear')); last = i + COOL
    return out


def walk(bars, i0, entry, stop, d, rr, hold=HOLD):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + hold, len(bars))):
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
    print(f"      {label:<14} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(fn, tf, title, use_delta=False):
    d = json.load(open(HIST))['pairs']
    byc = {c: defaultdict(list) for c in CLASSES}
    for pk in [x for x in PAIR_CLASS if x in d]:
        bars = _bars_norm(d[pk].get(tf, []))
        if len(bars) < 400:
            continue
        cls = PAIR_CLASS[pk]
        for (ei, entry, stop, dr) in fn(bars, use_delta) if use_delta else fn(bars):
            if ei >= len(bars):
                continue
            R = abs(entry - stop); ts = bars[ei]['_ts']
            for rr in RRS:
                o = walk(bars, ei, entry, stop, dr, rr)
                if o is not None:
                    byc[cls][rr].append((ts, o - cost(o, entry, R)))
    print(f"\n===== {title} · {tf} =====")
    for c in CLASSES:
        if not byc[c][RRS[0]]:
            continue
        print(f"   {c}:")
        for rr in RRS:
            line(f'RR {rr}', byc[c][rr])


def main():
    print("=" * 92)
    print("Whale Pivot Model (POC / delta-PROXY) · absorption+aggression NOT computable (no order flow)")
    print("=" * 92)
    for tf in ('m15', 'h1'):
        run(lambda b: poc_signals(b, False), tf, "A · POC-breakout standalone")
        run(lambda b, ud: poc_signals(b, ud), tf, "B · POC-breakout + delta-proxy confirm", use_delta=True)


if __name__ == '__main__':
    main()
