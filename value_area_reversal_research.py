"""Value-Area failed-breakout REVERSAL, declining-conviction filtered (fabervaale setup).

Creator setup (4 frames): build a volume profile -> mark the Value Area (~70% band,
VAH/VAL). Price breaks OUT of the value area on DECLINING VOLUME; when it CLOSES BACK
inside the value area, take the reversal, stop beyond the breakout extreme, target 2:1.

DATA CAVEAT (important): our feed has NO VOLUME (OHLC only). So two ingredients are
substituted with volume-free proxies and this is NOT a literal test of the volume version:
  1. Value Area via TPO (Time-Price-Opportunity) — Market Profile's original method:
     count TIME-at-price instead of volume-at-price, then take the 70% band. A legitimate,
     standard VA proxy computed causally from a rolling window of prior bars.
  2. "Declining volume" -> declining-CONVICTION proxy: the breakout bar's range is BELOW
     its recent ATR (a weak, low-conviction push likely to fail). This is a proxy for the
     volume filter, not the volume itself.

Everything else is faithful: reclaim-into-VA reversal entry (MARKET), stop beyond the
failed-breakout extreme, fixed 2:1 target. Two variants — 'all' (no conviction filter) vs
'weak' (declining-range filter) — to isolate whether the filter adds anything. Realistic
cost, per class, chronological OOS (both halves + n>=40 = PASS). h1 bars.

Run: python value_area_reversal_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
L = 96               # profile lookback (h1 bars, ~4 trading days)
BINS = 40
VA_PCT = 0.70
STEP = 2             # recompute VA every STEP bars (slow-moving structure) — speed
DECL = 0.90          # 'weak' breakout: breakout-bar range <= DECL * ATR (declining conviction)
RECLAIM_WIN = 24     # bars after breakout to allow the close-back-inside reclaim
BUF = 0.10           # stop buffer beyond the failed-breakout extreme (ATR)
HOLD = 120           # bars to reach the 2:1 target
RR = 2.0
COOLDOWN = 6


def value_area(win):
    lo = min(b['l'] for b in win); hi = max(b['h'] for b in win)
    if hi <= lo:
        return None
    w = (hi - lo) / BINS
    counts = [0] * BINS
    for b in win:                                   # TPO: time-at-price
        b0 = int((b['l'] - lo) / w); b1 = int((b['h'] - lo) / w)
        b0 = 0 if b0 < 0 else (BINS - 1 if b0 > BINS - 1 else b0)
        b1 = 0 if b1 < 0 else (BINS - 1 if b1 > BINS - 1 else b1)
        for k in range(b0, b1 + 1):
            counts[k] += 1
    total = sum(counts)
    poc = max(range(BINS), key=lambda k: counts[k])
    acc = counts[poc]; lo_i = hi_i = poc; tgt = VA_PCT * total
    while acc < tgt and (lo_i > 0 or hi_i < BINS - 1):
        left = counts[lo_i - 1] if lo_i > 0 else -1
        right = counts[hi_i + 1] if hi_i < BINS - 1 else -1
        if right >= left:
            hi_i += 1; acc += counts[hi_i]
        else:
            lo_i -= 1; acc += counts[lo_i]
    return lo + lo_i * w, lo + (hi_i + 1) * w        # VAL, VAH


def scan(bars, variant):
    n = len(bars); out = []; last = -1; va = None
    for i in range(L, n - 1):
        if i <= last:
            continue
        if (i % STEP) == 0 or va is None:
            va = value_area(bars[i - L:i])
        if va is None:
            continue
        val, vah = va
        c = bars[i]['c']
        a = atr(bars, 14, i) or 0.0
        if a <= 0 or not (val < c < vah):            # only arm when price is INSIDE the VA
            continue
        # look for a breakout beyond the VA on the following bars
        for j in range(i + 1, min(i + 1 + RECLAIM_WIN, n - 1)):
            bj = bars[j]
            up = bj['c'] > vah; dn = bj['c'] < val
            if not (up or dn):
                continue
            rng = bj['h'] - bj['l']
            weak = rng <= DECL * a
            if variant == 'weak' and not weak:
                last = j; break                       # a conviction breakout — don't fade it
            side = 'up' if up else 'dn'
            ext = bj['h'] if up else bj['l']
            for k in range(j + 1, min(j + 1 + RECLAIM_WIN, n - 1)):
                bk = bars[k]
                ext = max(ext, bk['h']) if side == 'up' else min(ext, bk['l'])
                reclaim = (bk['c'] < vah) if side == 'up' else (bk['c'] > val)
                if reclaim:
                    d = 'bear' if side == 'up' else 'bull'
                    entry = bk['c']
                    stop = ext + BUF * a if d == 'bear' else ext - BUF * a
                    if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
                        break
                    out.append((bars, k + 1, entry, stop, d)); last = k + COOLDOWN
                    break
            break
    return out


def walk(bars, i0, entry, stop, d):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return RR
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return RR
    return None


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<8} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(variant):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        bars = _bars_norm(pairs[pk].get('h1', []))
        if len(bars) < L + 300:
            continue
        npr += 1
        for (bb, ei, entry, stop, dr) in scan(bars, variant):
            if ei >= len(bb):
                continue
            o = walk(bb, ei, entry, stop, dr)
            if o is not None:
                store[cls].append((bb[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
    print(f"\n===== value-area failed-breakout reversal · h1 · [{variant}] — {npr} pairs =====")
    for c in ['index', 'major', 'minor', 'comm', 'crypto']:
        if store[c]:
            line(c, store[c])
    line('ALL', [r for c in store for r in store[c]])


def main():
    print("=" * 92)
    print("Value-Area (TPO) failed-breakout REVERSAL, declining-conviction filtered — market fills")
    print("=" * 92)
    run('all')       # no conviction filter — fade every VA-breakout reclaim
    run('weak')      # declining-conviction filter — only fade weak (small-range) breakouts


if __name__ == '__main__':
    main()
