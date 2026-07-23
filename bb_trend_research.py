"""Multi-timeframe Bollinger-Band trend-continuation (user screenshots, 'Deni' 7 steps).

Step #1 BB(20,2); middle = 20-SMA.
Step #2 (4H context): a strong move in one direction — MA angled in the move
  direction + expanding bands + momentum (>=2 of the last 3 bars long-solid).
Step #3 (1H pullback): price retraces to touch (within a small tolerance) the 1H
  20-SMA (middle band), a slow/corrective move, bands contracting.
Step #4 (entry setup): a reversal candle in the trend direction — engulfing or pin
  bar (3-bar reversal approximated) — enter on its close (Step #6).
Step #5 stop: beyond the recent swing (pullback low for longs / high for shorts).
Step #7 management: close half at the 1:1 target, trail the rest below each higher
  low (long) / above each lower high (short) until stopped.

Tested: fixed 1:1 / 1.5 / 2.0 (full position) for the core edge, AND the book's
half-at-1:1 + trail. 4H context uses only CLOSED 4H bars (no lookahead), next-bar
resolution from the close fill, fixed cost, chronological OOS split (both halves +),
per class. Generic price-action, clean-room.

Run: python bb_trend_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost, is_engulf

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
MA_N = 20
TOL = 0.25           # pullback: bar low/high within TOL*ATR of the middle band
PB_WIN = 6           # pullback / swing window on 1H
BUF = 0.10
COOLDOWN = 4
RRS = [1.0, 1.5, 2.0]
HOLD = 96
TRAIL_PRD = 2        # pivot lookback for the trailing stop


def sma(vals, n):
    out = [None]*len(vals); s = 0.0
    for i, v in enumerate(vals):
        s += v
        if i >= n:
            s -= vals[i-n]
        if i >= n-1:
            out[i] = s/n
    return out


def bbw(closes, n=20, k=2):
    ma = sma(closes, n); out = [None]*len(closes)
    for i in range(n-1, len(closes)):
        win = closes[i-n+1:i+1]; m = ma[i]
        sd = (sum((x-m)**2 for x in win)/n) ** 0.5
        out[i] = (2*k*sd)/m if m else None
    return out


def pinbar(b, d):
    rng = b['h'] - b['l']
    if rng <= 0:
        return False
    if d == 'bull':
        return (min(b['o'], b['c']) - b['l']) >= 0.5*rng
    return (b['h'] - max(b['o'], b['c'])) >= 0.5*rng


def long_solid(b, d):
    rng = b['h'] - b['l']
    if rng <= 0:
        return False
    body = b['c'] - b['o']
    return (body >= 0.6*rng) if d == 'bull' else (-body >= 0.6*rng)


def scan(h1, tf_unused, store, storeM, cls, store_cls):
    b4 = agg4h(h1)
    c1 = [b['c'] for b in h1]; ma1 = sma(c1, MA_N); bw1 = bbw(c1)
    c4 = [b['c'] for b in b4]; ma4 = sma(c4, MA_N); bw4 = bbw(c4)
    t4 = [b['_ts'] for b in b4]
    import bisect
    n = len(h1); last = -1
    for i in range(MA_N + PB_WIN, n - 1):
        if i <= last or ma1[i] is None or bw1[i] is None or bw1[i-PB_WIN] is None:
            continue
        # locate last CLOSED 4H bar
        k = bisect.bisect_right(t4, h1[i]['_ts'] - 4*3600) - 1
        if k < 6 or ma4[k] is None or ma4[k-2] is None or bw4[k] is None or bw4[k-3] is None:
            continue
        for d in ('bull', 'bear'):
            # Step #2: 4H strong move — angled MA + expanding bands + momentum
            if d == 'bull':
                if not (ma4[k] > ma4[k-2] and bw4[k] > bw4[k-3]):
                    continue
            else:
                if not (ma4[k] < ma4[k-2] and bw4[k] > bw4[k-3]):
                    continue
            if sum(1 for b in b4[k-2:k+1] if long_solid(b, d)) < 2:
                continue
            # Step #3: 1H pullback to the middle band + contracting bands
            if bw1[i] >= bw1[i-PB_WIN]:
                continue
            a = atr(h1, 14, i) or 0.0
            if a <= 0:
                continue
            touched = any(h1[j]['l'] <= ma1[j] + TOL*a and h1[j]['h'] >= ma1[j] - TOL*a
                          for j in range(i-PB_WIN, i+1) if ma1[j] is not None)
            if not touched:
                continue
            if d == 'bull' and not (h1[i]['c'] > ma1[i]):
                continue
            if d == 'bear' and not (h1[i]['c'] < ma1[i]):
                continue
            # Step #4: reversal candle in trend direction
            rev = is_engulf(h1, i, d) or pinbar(h1[i], d)
            if not rev or (d == 'bull' and h1[i]['c'] <= h1[i]['o']) or (d == 'bear' and h1[i]['c'] >= h1[i]['o']):
                continue
            entry = h1[i]['c']
            if d == 'bull':
                stop = min(h1[j]['l'] for j in range(i-PB_WIN, i+1)) - BUF*a
            else:
                stop = max(h1[j]['h'] for j in range(i-PB_WIN, i+1)) + BUF*a
            R = abs(entry - stop)
            if R <= 0 or (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
                continue
            ts = h1[i]['_ts']
            # fixed-RR outcomes
            for rr in RRS:
                o = walk(h1, i+1, entry, stop, d, rr, HOLD)
                if o is not None:
                    store[(rr,)].append((ts, o - cost(o, entry, R)))
                    store_cls[cls][(rr,)].append((ts, o - cost(o, entry, R)))
            # book management: half at 1:1, trail the rest
            om = manage(h1, i+1, entry, stop, d, R)
            if om is not None:
                storeM.append((ts, om - cost(om, entry, R)))
            last = i + COOLDOWN
            break


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop); tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def manage(bars, i0, entry, stop, d, R):
    """Half off at 1:1, trail the remaining half below each higher-low (long) /
    above each lower-high (short). Blended realized R."""
    t1 = entry + R if d == 'bull' else entry - R
    half_done = False; cur_stop = stop; k = TRAIL_PRD
    for j in range(i0, min(i0+HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= cur_stop:
                r2 = (cur_stop - entry)/R
                return -1.0 if not half_done else 0.5*1.0 + 0.5*r2
            if not half_done and b['h'] >= t1:
                half_done = True
            if half_done and j-k >= i0 and all(bars[j-k]['l'] <= bars[j-k-m]['l'] and bars[j-k]['l'] <= bars[j-k+m]['l'] for m in range(1, k+1)):
                cur_stop = max(cur_stop, bars[j-k]['l'])
        else:
            if b['h'] >= cur_stop:
                r2 = (entry - cur_stop)/R
                return -1.0 if not half_done else 0.5*1.0 + 0.5*r2
            if not half_done and b['l'] <= t1:
                half_done = True
            if half_done and j-k >= i0 and all(bars[j-k]['h'] >= bars[j-k-m]['h'] and bars[j-k]['h'] >= bars[j-k+m]['h'] for m in range(1, k+1)):
                cur_stop = min(cur_stop, bars[j-k]['h'])
    b = bars[min(i0+HOLD, len(bars))-1]
    r2 = (b['c']-entry)/R if d == 'bull' else (entry-b['c'])/R
    return 0.5*1.0 + 0.5*r2 if half_done else r2


def line(label, rows, rr=None):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); storeM = []; store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 600 or len(daily) < 80:
            continue
        npairs += 1
        scan(h1, None, store, storeM, cls, store_cls)

    print(f"BB trend-continuation (4H context -> 1H pullback -> reversal candle) — {npairs} pairs\n")
    print("FIXED targets (full position):")
    for rr in RRS:
        line(f"RR{rr}", store[(rr,)], rr)
    print("\nBOOK management (half at 1:1, trail rest):")
    line("half+trail", storeM)
    print("\nPer class (RR1.0):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][(1.0,)], 1.0)
    print("\nPer class (RR2.0):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][(2.0,)], 2.0)


if __name__ == '__main__':
    main()
