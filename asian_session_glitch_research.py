"""'Asian Session Gold Glitch' — session-timed liquidity-sweep reversal (user
screenshots, algo.jan).

Model: mark the HIGH and LOW of the last hour of the US session. When the Asian
session opens, price runs to one of those levels (sweeps the prior US high/low =
liquidity grab), breaks structure and REVERSES. Enter the reversal, target a
minimum 2:1 RR.

Encoding (hourly UTC data): each day, capture ref_high/ref_low from the US-close
hour bar (default 20:00 UTC — the last full hour before the 21:00 FX rollover
gap). During the following Asian window (default 23:00-06:00 UTC) find the FIRST
bar that sweeps ref_high (high>ref_high) or ref_low (low<ref_low), then wait for
the RECLAIM (a close back inside the level = the reversal). Enter next-bar open,
stop beyond the swept extreme (+ATR buffer), fixed RR target. Realistic fills,
fixed cost, chronological OOS split (both halves +), per class, XAUUSD called out.
Generic session price-action, clean-room.

Run: python asian_session_glitch_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
US_HOUR = 20                       # UTC hour whose bar defines the US last-hour high/low
ASIA_HOURS = {23, 0, 1, 2, 3, 4, 5}   # sweep+reversal window (Asian session)
ASIA_MAX = 12                      # bars after the ref bar to keep the episode alive
BUF = 0.10                         # stop buffer beyond the swept extreme, in ATR
HOLD = 18                          # hourly bars to reach target (Asian+London)
RRS = [1.5, 2.0, 3.0]


def hour_utc(b):
    return (b['_ts'] // 3600) % 24


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(bars, us_hour, asia_hours, store, cls, store_cls, store_pair, pk):
    n = len(bars)
    for r in range(14, n - 2):
        if hour_utc(bars[r]) != us_hour:
            continue
        ref_hi = bars[r]['h']; ref_lo = bars[r]['l']
        swept = None; sweep_ext = None
        for j in range(r + 1, min(r + 1 + ASIA_MAX, n - 1)):
            hr = hour_utc(bars[j])
            if hr == us_hour:          # reached next day's US hour — episode over
                break
            if hr not in asia_hours:
                continue
            b = bars[j]
            if swept is None:
                if b['h'] > ref_hi:
                    swept = 'high'; sweep_ext = b['h']
                elif b['l'] < ref_lo:
                    swept = 'low'; sweep_ext = b['l']
                # allow same-bar reclaim (wick sweep that closes back inside)
                if swept == 'high' and b['c'] < ref_hi:
                    _emit(bars, j, sweep_ext, 'bear', store, cls, store_cls, store_pair, pk); break
                if swept == 'low' and b['c'] > ref_lo:
                    _emit(bars, j, sweep_ext, 'bull', store, cls, store_cls, store_pair, pk); break
            else:
                sweep_ext = max(sweep_ext, b['h']) if swept == 'high' else min(sweep_ext, b['l'])
                if swept == 'high' and b['c'] < ref_hi:
                    _emit(bars, j, sweep_ext, 'bear', store, cls, store_cls, store_pair, pk); break
                if swept == 'low' and b['c'] > ref_lo:
                    _emit(bars, j, sweep_ext, 'bull', store, cls, store_cls, store_pair, pk); break


def _emit(bars, k, sweep_ext, d, store, cls, store_cls, store_pair, pk):
    ei = k + 1
    if ei >= len(bars):
        return
    entry = bars[ei]['o']; a = atr(bars, 14, k) or 0.0
    stop = (sweep_ext + BUF*a) if d == 'bear' else (sweep_ext - BUF*a)
    if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']
    for rr in RRS:
        o = walk(bars, ei, entry, stop, d, rr, HOLD)
        if o is not None:
            net = o - cost(o, entry, R)
            store[rr].append((ts, net))
            store_cls[cls][rr].append((ts, net))
            store_pair[pk][rr].append((ts, net))


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100/(1+rr)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(us_hour, asia_hours, tag):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list))
    store_pair = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 800:
            continue
        npairs += 1
        scan(h1, us_hour, asia_hours, store, cls, store_cls, store_pair, pk)
    print(f"\n===== {tag}: US_HOUR={us_hour} ASIA={sorted(asia_hours)} — {npairs} pairs =====")
    print("AGGREGATE (all pairs):")
    for rr in RRS:
        line(f"RR{rr}", store[rr], rr)
    print("XAUUSD (gold):")
    for rr in RRS:
        line(f"gold RR{rr}", store_pair['xauusd'][rr], rr)
    print("Per class (RR2.0):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][2.0], 2.0)


def main():
    run(20, {23, 0, 1, 2, 3, 4, 5}, 'primary')
    # timing sensitivity
    run(19, {23, 0, 1, 2, 3, 4, 5}, 'US_HOUR=19')
    run(20, {0, 1, 2, 3, 4, 5, 6}, 'Asia 00-06')
    run(21, {0, 1, 2, 3, 4, 5, 6}, 'US_HOUR=21')


if __name__ == '__main__':
    main()
