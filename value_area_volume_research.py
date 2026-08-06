"""fabervaale value-area reversal — the REAL volume version (now that we have volume).

The original value_area_reversal_research.py could only proxy this social-media setup:
time-at-price (TPO) for the profile, and 'breakout-bar range < ATR' for the declining-
volume filter — because the feed had no volume. Now it does, so this is the literal test:

  - Volume profile = VOLUME-at-price (each bar's volume spread across its H-L range),
    Value Area = the 70% band around the POC.
  - Setup: price inside the VA breaks beyond VAH/VAL on DECLINING volume (breakout-bar
    volume < DECL_VOL x recent average), then closes back inside (failed breakout) ->
    fade back toward the VA, stop beyond the failed extreme, RR2.

Crypto = REAL Coinbase volume (the honest test); FX/indices/comm = OANDA tick volume
(the standard proxy). Market fills, dealing cost, chronological OOS, bracket-honest
(unresolved-in-hold excluded). n>=40 + both OOS halves + = PASS.

Run: python value_area_volume_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
L = 96
BINS = 40
VA_PCT = 0.70
STEP = 2
DECL_VOL = 0.90       # 'declining volume' breakout: bar volume <= DECL_VOL * recent avg volume
VOL_LB = 20
RECLAIM_WIN = 24
BUF = 0.10
HOLD = 120
RR = 2.0
COOLDOWN = 6


def value_area_vol(win):
    """Value Area from a VOLUME-at-price profile (each bar's volume spread across its range)."""
    lo = min(b['l'] for b in win); hi = max(b['h'] for b in win)
    if hi <= lo:
        return None
    w = (hi - lo) / BINS
    counts = [0.0] * BINS
    for b in win:
        v = b.get('v', 0) or 0.0
        if v <= 0:
            continue
        b0 = int((b['l'] - lo) / w); b1 = int((b['h'] - lo) / w)
        b0 = 0 if b0 < 0 else (BINS - 1 if b0 > BINS - 1 else b0)
        b1 = 0 if b1 < 0 else (BINS - 1 if b1 > BINS - 1 else b1)
        share = v / (b1 - b0 + 1)
        for k in range(b0, b1 + 1):
            counts[k] += share
    total = sum(counts)
    if total <= 0:
        return None
    poc = max(range(BINS), key=lambda k: counts[k])
    acc = counts[poc]; lo_i = hi_i = poc; tgt = VA_PCT * total
    while acc < tgt and (lo_i > 0 or hi_i < BINS - 1):
        left = counts[lo_i - 1] if lo_i > 0 else -1
        right = counts[hi_i + 1] if hi_i < BINS - 1 else -1
        if right >= left:
            hi_i += 1; acc += counts[hi_i]
        else:
            lo_i -= 1; acc += counts[lo_i]
    return lo + lo_i * w, lo + (hi_i + 1) * w


def _avgvol(bars, i):
    if i < VOL_LB:
        return None
    s = sum((bars[x].get('v', 0) or 0) for x in range(i - VOL_LB, i))
    return s / VOL_LB if s > 0 else None


def scan(bars):
    n = len(bars); out = []; last = -1; va = None
    for i in range(L, n - 1):
        if i <= last:
            continue
        if (i % STEP) == 0 or va is None:
            va = value_area_vol(bars[i - L:i])
        if va is None:
            continue
        val, vah = va
        c = bars[i]['c']; a = atr(bars, 14, i) or 0.0
        if a <= 0 or not (val < c < vah):
            continue
        for j in range(i + 1, min(i + 1 + RECLAIM_WIN, n - 1)):
            bj = bars[j]
            up = bj['c'] > vah; dn = bj['c'] < val
            if not (up or dn):
                continue
            avgv = _avgvol(bars, j)
            weak = avgv is not None and (bj.get('v', 0) or 0) <= DECL_VOL * avgv   # REAL declining volume
            if not weak:
                last = j; break                            # conviction breakout on volume — don't fade
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
                    out.append((k + 1, entry, stop, d)); last = k + COOLDOWN
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


def main():
    d = json.load(open(HIST))['pairs']
    store = defaultdict(list); npr = 0
    for pk in [x for x in PAIR_CLASS if x in d]:
        cls = PAIR_CLASS.get(pk)
        bars = _bars_norm(d[pk].get('h1', []))
        if len(bars) < L + 300:
            continue
        npr += 1
        for (ei, entry, stop, dr) in scan(bars):
            if ei >= len(bars):
                continue
            o = walk(bars, ei, entry, stop, dr)
            if o is not None:
                store[cls].append((bars[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
    print("=" * 92)
    print(f"fabervaale VALUE-AREA reversal — REAL volume profile + declining-volume filter · h1 · {npr} pairs")
    print("  crypto = real volume (honest test); others = OANDA tick volume (proxy)")
    print("=" * 92)
    for c in ['crypto', 'index', 'major', 'minor', 'comm']:
        if store[c]:
            line(c, store[c])
    line('ALL', [r for c in store for r in store[c]])


if __name__ == '__main__':
    main()
