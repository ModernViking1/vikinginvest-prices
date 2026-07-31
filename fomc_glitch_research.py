"""'The FOMC Glitch' — 2:00-2:29pm ET range, sweep, reclaim, reverse (index futures).

Rules (screenshots): during the 2:00-2:29 ET window (FOMC release time) do nothing, just
mark the range high (B$L, buy-side liquidity) and low (S$L, sell-side liquidity). After the
window, wait for price to BREAK the range and sweep one side's liquidity, then RETRACE back
inside (reclaim) -> enter the reversal (sweep the low + reclaim -> long; sweep the high +
reclaim -> short), target the opposite liquidity at RR 2-3:1, stop beyond the sweep extreme.

This is a session-timed sweep-reversal (same family as asianglitch) pinned to the 2pm ET
window. FOMC itself is only ~8 days/yr (untestable), so we test the DAILY 2pm-ET version —
the tradeable interpretation the influencer actually uses — on the US indices (and majors,
since the 2pm data drop moves FX too). m15, DST-aware ET window, RR2 & RR3, OOS split.

Run: python fomc_glitch_research.py
"""
import json, os
from datetime import datetime, timedelta
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
BUF = 0.20            # stop buffer beyond the sweep extreme (ATR)
SWEEP_WINDOW = 16     # m15 bars after the range (~4h) to find the sweep+reclaim
HOLD = 32             # m15 bars to reach the RR target (~8h)
RRS = [2.0, 3.0]


def _nth_sunday(year, month, n):
    d = datetime(year, month, 1)
    offset = (6 - d.weekday()) % 7            # Monday=0..Sunday=6
    return d + timedelta(days=offset + 7 * (n - 1))


def is_us_dst(dt):
    """US Eastern DST: 2nd Sunday March 07:00 UTC -> 1st Sunday November 06:00 UTC."""
    start = _nth_sunday(dt.year, 3, 2).replace(hour=7)
    end = _nth_sunday(dt.year, 11, 1).replace(hour=6)
    return start <= dt < end


def et_2pm_utc_hour(dt):
    return 18 if is_us_dst(dt) else 19        # 2pm ET = 18:00 UTC (EDT) / 19:00 UTC (EST)


def walk(m15, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def _emit(m15, ei, entry, stop, d, store, cls, pk):
    if ei >= len(m15):
        return
    R = abs(entry - stop); ts = m15[ei]['_ts']
    for rr in RRS:
        o = walk(m15, ei, entry, stop, d, rr)
        if o is not None:
            net = o - cost(o, entry, R)
            store[cls][rr].append((ts, net)); store[('pair', pk)][rr].append((ts, net))


def scan(m15, store, cls, pk):
    days = defaultdict(list)
    for i, b in enumerate(m15):
        days[int(b['_ts'] // 86400)].append(i)
    for day, idxs in days.items():
        dt0 = datetime.utcfromtimestamp(day * 86400 + 12 * 3600)
        h2pm = et_2pm_utc_hour(dt0)
        rng = [i for i in idxs
               if int((m15[i]['_ts'] // 3600) % 24) == h2pm and int(m15[i]['_ts'] % 3600) // 60 in (0, 15)]
        if len(rng) < 2:
            continue
        hi = max(m15[i]['h'] for i in rng); lo = min(m15[i]['l'] for i in rng)
        start = max(rng) + 1
        for j in range(start, min(start + SWEEP_WINDOW, len(m15) - 1)):
            b = m15[j]; a = atr(m15, 14, j) or 0.0
            if b['l'] < lo and b['c'] > lo:            # swept sell-side liq, reclaimed -> LONG
                entry = b['c']; stop = b['l'] - BUF * a
                if stop < entry:
                    _emit(m15, j + 1, entry, stop, 'bull', store, cls, pk)
                break
            if b['h'] > hi and b['c'] < hi:            # swept buy-side liq, reclaimed -> SHORT
                entry = b['c']; stop = b['h'] + BUF * a
                if stop > entry:
                    _emit(m15, j + 1, entry, stop, 'bear', store, cls, pk)
                break


def scan_cont(m15, store, cls, pk):
    """CONTINUATION mirror — the reversal loses, so also test trading WITH the 2pm break
    (close beyond the range -> go with it, stop at the other side of the range)."""
    days = defaultdict(list)
    for i, b in enumerate(m15):
        days[int(b['_ts'] // 86400)].append(i)
    for day, idxs in days.items():
        h2 = et_2pm_utc_hour(datetime.utcfromtimestamp(day * 86400 + 43200))
        rng = [i for i in idxs if int((m15[i]['_ts'] // 3600) % 24) == h2 and int(m15[i]['_ts'] % 3600) // 60 in (0, 15)]
        if len(rng) < 2:
            continue
        hi = max(m15[i]['h'] for i in rng); lo = min(m15[i]['l'] for i in rng); start = max(rng) + 1
        for j in range(start, min(start + SWEEP_WINDOW, len(m15) - 1)):
            b = m15[j]; a = atr(m15, 14, j) or 0.0
            d = 'bull' if b['c'] > hi else ('bear' if b['c'] < lo else None)
            if not d:
                continue
            entry = b['c']; stop = (lo - BUF * a) if d == 'bull' else (hi + BUF * a)
            if (d == 'bull' and stop < entry) or (d == 'bear' and stop > entry):
                _emit(m15, j + 1, entry, stop, d, store, cls, pk)
            break


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<10} RR{rr} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        if cls not in ('index', 'major', 'comm', 'crypto'):
            continue
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(m15) < 1000:
            continue
        npr += 1
        scan(m15, store, cls, pk)
    print("=" * 88)
    print(f"FOMC glitch — 2pm-ET range sweep+reclaim reversal (m15, indices/majors) — {npr} pairs")
    print("=" * 88)
    for c in ['index', 'major', 'comm', 'crypto']:
        print(f"  {c}:")
        for rr in RRS:
            line(c, store[c][rr], rr)
    print("  US indices per pair (RR2):")
    for pk in ('nas100', 'spx500', 'dj30'):
        line(pk, store[('pair', pk)][2.0], 2.0)
    print("  ALL pooled (reversal):")
    for rr in RRS:
        line('ALL', [r for c in ('index', 'major', 'comm', 'crypto') for r in store[c][rr]], rr)

    # continuation mirror
    cont = defaultdict(lambda: defaultdict(list))
    for pk in [x for x in pairs if x in PAIR_CLASS and PAIR_CLASS[x] in ('index', 'major', 'comm', 'crypto')]:
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(m15) < 1000:
            continue
        scan_cont(m15, cont, PAIR_CLASS[pk], pk)
    print("  CONTINUATION mirror (trade WITH the 2pm break):")
    for rr in RRS:
        line('ALL', [r for c in ('index', 'major', 'comm', 'crypto') for r in cont[c][rr]], rr)


if __name__ == '__main__':
    main()
