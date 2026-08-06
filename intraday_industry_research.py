"""Top documented 15-minute day-trading strategies vs our intraday book.

Our four live/observed 15m methods are macdp (MACD cross + confluence), wick
(wick reversal), fib (Fibonacci half-entry) and divg (MACD divergence). This
tests the three most-cited industry 15m strategies that are MECHANICALLY
DIFFERENT from those four, on our own m15 OHLC, with the same discipline as
everything else (market fills, dealing cost, chronological OOS, BOTH halves
positive + n>=40 = PASS):

  1. ORB   Opening-Range Breakout — the canonical intraday strategy (Crabel;
           the 2023 Zarattini-Aziz stock study revived it). Range = first hour
           of each UTC day; trade the first break of that range for the rest
           of the day, stop the far side, target = 1x the range (~1:1-ish).
           DIFFERENT from ours: none of our four is a session-range breakout.

  2. EMA920  9/20-EMA pullback-in-trend — trade a pullback to the 20-EMA while
             9>20 (up) / 9<20 (down), enter on the reclaim, 1-ATR stop, RR1.5.
             DIFFERENT from macdp: continuation off a moving-average pullback,
             not a MACD zero-cross.

  3. BBMR  Bollinger-Band mean-reversion — a close outside the 2σ band is faded
           back toward the 20-SMA middle band, 1-ATR stop beyond entry.
           DIFFERENT from ours: explicit volatility-band fade, variable R.

NOT TESTED — VWAP. VWAP reversion/trend is arguably the single most popular
prop-desk 15m strategy, but VWAP needs traded VOLUME and our feed is OHLC-only
(no volume on any timeframe), so it cannot be computed faithfully. Flagged, not
faked. (Same reason our engine has never carried a volume-based method.)

Run: python intraday_industry_research.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost, ema

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
TF = 'm15'
BARS_PER_DAY = 96          # 24h × 4 (FX/crypto); indices trade fewer but the day anchor still holds
OR_BARS = 4                # opening range = first hour (4 × 15m)
HOLD = 40                  # ~10h max hold on 15m
COOLDOWN = 2


def _day_index(ts):
    # _bars_norm emits _ts in SECONDS, so a UTC day is 86400 s.
    return int(ts // 86400) if ts else 0


# ---------- 1. Opening-Range Breakout ----------
def sig_orb(bars):
    out = []
    by_day = defaultdict(list)
    for i, b in enumerate(bars):
        by_day[_day_index(b['_ts'])].append(i)
    for day, idxs in by_day.items():
        if len(idxs) < OR_BARS + 4:
            continue
        rng = idxs[:OR_BARS]
        hi = max(bars[j]['h'] for j in rng)
        lo = min(bars[j]['l'] for j in rng)
        if hi <= lo:
            continue
        h = hi - lo
        fired = False
        for j in idxs[OR_BARS:]:
            b = bars[j]
            if not fired and b['c'] > hi:
                out.append((j + 1, b['c'], lo, 'bull', h)); fired = True; break
            if not fired and b['c'] < lo:
                out.append((j + 1, b['c'], hi, 'bear', h)); fired = True; break
    return out


# ---------- 2. 9/20-EMA pullback-in-trend ----------
def sig_ema920(bars):
    out = []
    n = len(bars)
    last = -1
    c = [x['c'] for x in bars]
    e9, e20 = ema(c, 9), ema(c, 20)
    for i in range(25, n - 1):
        if i <= last or e9[i] is None or e20[i] is None:
            continue
        a = atr(bars, 14, i)
        if not a or a <= 0:
            continue
        b = bars[i]
        if e9[i] > e20[i] and b['l'] <= e20[i] and b['c'] > e9[i]:       # uptrend pullback reclaimed
            out.append((i + 1, b['c'], e20[i] - a, 'bull', None)); last = i + COOLDOWN
        elif e9[i] < e20[i] and b['h'] >= e20[i] and b['c'] < e9[i]:     # downtrend pullback reclaimed
            out.append((i + 1, b['c'], e20[i] + a, 'bear', None)); last = i + COOLDOWN
    return out


# ---------- 3. Bollinger-Band mean-reversion ----------
def _sma(vals, n, i):
    if i < n - 1:
        return None
    return sum(vals[i - n + 1:i + 1]) / n


def _std(vals, n, i, m):
    if i < n - 1:
        return None
    seg = vals[i - n + 1:i + 1]
    return (sum((x - m) ** 2 for x in seg) / n) ** 0.5


def sig_bbmr(bars):
    out = []
    n = len(bars)
    last = -1
    c = [x['c'] for x in bars]
    for i in range(25, n - 1):
        if i <= last:
            continue
        m = _sma(c, 20, i)
        sd = _std(c, 20, i, m) if m is not None else None
        a = atr(bars, 14, i)
        if m is None or sd is None or sd <= 0 or not a or a <= 0:
            continue
        upper, lower = m + 2 * sd, m - 2 * sd
        b = bars[i]
        if b['c'] < lower:                       # fade the down-extreme, target the mean
            out.append((i + 1, b['c'], b['c'] - a, 'bull', m)); last = i + COOLDOWN
        elif b['c'] > upper:
            out.append((i + 1, b['c'], b['c'] + a, 'bear', m)); last = i + COOLDOWN
    return out


def walk_to(bars, i0, entry, stop, d, target, hold):
    """First-touch stop/target; returns realised R against the stop distance."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= target:
                return (target - entry) / R
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= target:
                return (entry - target) / R
    # time-stop at last close, marked to market
    last_c = bars[min(i0 + hold, len(bars) - 1)]['c']
    return ((last_c - entry) if d == 'bull' else (entry - last_c)) / R


DETECTORS = {
    'ORB (opening-range breakout)': (sig_orb, 'range'),
    'EMA9/20 pullback': (sig_ema920, 1.5),
    'Bollinger mean-reversion': (sig_bbmr, 'mid'),
}


def run_pair(fn, mode, bars):
    rows = []
    for sg in fn(bars):
        ei, entry, stop, d, extra = sg
        if ei >= len(bars):
            continue
        R = abs(entry - stop)
        if R <= 0:
            continue
        if mode == 'range':                              # target = 1× opening range
            target = entry + extra if d == 'bull' else entry - extra
        elif mode == 'mid':                              # target = BB middle band
            target = extra
        else:                                            # fixed RR multiple
            target = entry + mode * R if d == 'bull' else entry - mode * R
        o = walk_to(bars, ei, entry, stop, d, target, HOLD)
        if o is not None:
            rows.append((bars[ei]['_ts'], o - cost(o, entry, R)))
    return rows


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<10} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST))['pairs']
    # crypto = our live intraday class; split so we can see if an edge is live-class or not
    CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd'}
    print('=' * 96)
    print('Top-3 industry 15m strategies (different from ours) — m15, market fills+cost, OOS')
    print('=' * 96)
    for name, (fn, mode) in DETECTORS.items():
        print(f"\n===== {name} =====")
        per_pair = {}
        for pk in d:
            bars = _bars_norm(d.get(pk, {}).get(TF, []))
            if len(bars) < 400:
                continue
            r = run_pair(fn, mode, bars)
            if r:
                per_pair[pk] = r
        allrows = [r for pk in per_pair for r in per_pair[pk]]
        crypto_rows = [r for pk in per_pair if pk in CRYPTO for r in per_pair[pk]]
        other_rows = [r for pk in per_pair if pk not in CRYPTO for r in per_pair[pk]]
        line('ALL', allrows)
        if crypto_rows:
            line('crypto', crypto_rows)
        if other_rows:
            line('non-crypto', other_rows)


if __name__ == '__main__':
    main()
