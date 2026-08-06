"""#3 — does a higher reward:risk target rescue the engine methods?

The engine trades ~1:1, where breakeven is 50% WR — a brutal bar after cost. This
re-walks the flagship macdp core (exact live logic) to a range of RR targets to see
whether a higher target (breakeven 40% at 1.5:1, 33% at 2:1) turns it profitable,
on crypto and on the faytterro-aligned cohort.

macdp core = 15m MACD cross + H1 RSI centerline + structural stop, MARKET fill.
Bracket-honest (timeout excluded), dealing cost, chronological OOS.

Run: python intraday_rr_research.py
"""
import json
import os
import bisect

from detect_triggers import PAIR_CLASS, macd_series, h11_event_aligned
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import cost, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
STRUCT = 8
HOLD = 96
H1WIN = 60
RRS = [1.0, 1.5, 2.0, 3.0]
CRYPTO = [pk for pk, c in PAIR_CLASS.items() if c == 'crypto']


def scan(pk):
    """Yield (ts, {rr: outcome_R}, aligned) for each macdp signal (outcome per RR)."""
    d = json.load(open(HIST))['pairs'].get(pk, {})
    m15 = _bars_norm(d.get('m15', [])); h1 = _bars_norm(d.get('h1', []))
    if len(m15) < 200 or len(h1) < 80:
        return []
    mc = [b['c'] for b in m15]
    macd_line, sig_line = macd_series(mc, 12, 26, 9)
    h1c = [b['c'] for b in h1]; h1rsi = precompute_rsi(h1c, 14); h1ts = [b['_ts'] for b in h1]
    out = []; last = -1
    for i in range(35, len(m15) - 1):
        if i <= last:
            continue
        m0, m1, s0, s1 = macd_line[i - 1], macd_line[i], sig_line[i - 1], sig_line[i]
        if None in (m0, m1, s0, s1):
            continue
        if m0 <= s0 and m1 > s1:
            d_ = 'bull'
        elif m0 >= s0 and m1 < s1:
            d_ = 'bear'
        else:
            continue
        hi = bisect.bisect_right(h1ts, m15[i]['_ts']) - 1
        if hi < 20 or h1rsi[hi] is None:
            continue
        rv = h1rsi[hi]
        if (d_ == 'bull' and rv >= 50) or (d_ == 'bear' and rv <= 50):
            continue
        seg = m15[max(0, i - STRUCT):i + 1]; entry = m15[i]['c']
        if d_ == 'bull':
            stop = min(x['l'] for x in seg)
            if stop >= entry:
                continue
            R = entry - stop
        else:
            stop = max(x['h'] for x in seg)
            if stop <= entry:
                continue
            R = stop - entry
        aligned = h11_event_aligned(h1c[max(0, hi - H1WIN):hi + 1], d_)
        # walk once, record first-touch outcome for EACH rr target (stop shared)
        outcomes = {}
        for rr in RRS:
            tgt = entry + rr * R if d_ == 'bull' else entry - rr * R
            res = None
            for j in range(i + 1, min(i + 1 + HOLD, len(m15))):
                b = m15[j]
                if d_ == 'bull':
                    if b['l'] <= stop:
                        res = -1.0; break
                    if b['h'] >= tgt:
                        res = rr; break
                else:
                    if b['h'] >= stop:
                        res = -1.0; break
                    if b['l'] <= tgt:
                        res = rr; break
            if res is not None:
                outcomes[rr] = res - cost(res, entry, R)
        if outcomes:
            last = i + 2
            out.append((m15[i]['_ts'], outcomes, aligned))
    return out


def line(label, rows, rr):
    rows = [(ts, o[rr]) for ts, o, _ in rows if rr in o]
    rows.sort()
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<22} RR{rr:<3} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    allrows = []
    for pk in CRYPTO:
        allrows += scan(pk)
    aligned = [r for r in allrows if r[2]]
    print('=' * 100)
    print('macdp core at higher RR targets — crypto, bracket-honest, OOS (does a bigger target beat 1:1?)')
    print('=' * 100)
    print('  -- all crypto macdp --')
    for rr in RRS:
        line('crypto', allrows, rr)
    print('  -- faytterro-aligned only --')
    for rr in RRS:
        line('faytterro-aligned', aligned, rr)


if __name__ == '__main__':
    main()
