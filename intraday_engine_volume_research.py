"""Does a relative-volume gate improve the flagship intraday engine method (macdp)?

macdp is the dominant live intraday method (currently ~44% WR / negative at 1:1).
The faytterro (spring/UTAD) filter already lifts its aligned cohort; this asks the
independent question: does gating macdp entries to HIGH RELATIVE-VOLUME bars help,
and does it STACK with faytterro? Tested where volume is real (crypto).

macdp core is the exact live logic (imported macd_series + h11_event_aligned from
detect_triggers): 15m MACD(12,26,9) cross, H1 RSI centerline filter, structural
stop over the last 8 m15 bars, MARKET fill at the cross close, fixed 1:1 target.
Bracket-honest: unresolved-within-hold trades are EXCLUDED (not marked to market).

Cohorts reported on crypto: base / relvol>=X sweep / faytterro-aligned /
relvol>=1.5 AND faytterro. n>=40 + both OOS halves + = PASS.

Run: python intraday_engine_volume_research.py
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
VOL_LB = 20
CRYPTO = [pk for pk, c in PAIR_CLASS.items() if c == 'crypto']


def scan(pk):
    """Yield (ts, r_after_cost, relvol, aligned) for each resolved macdp signal."""
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
            R = entry - stop; tgt = entry + R
        else:
            stop = max(x['h'] for x in seg)
            if stop <= entry:
                continue
            R = stop - entry; tgt = entry - R
        # relative volume on the cross bar
        relvol = None
        if i >= VOL_LB:
            avg = sum((x.get('v', 0) or 0) for x in m15[i - VOL_LB:i]) / VOL_LB
            if avg > 0:
                relvol = (m15[i].get('v', 0) or 0) / avg
        aligned = h11_event_aligned(h1c[max(0, hi - H1WIN):hi + 1], d_)
        o = None
        for j in range(i + 1, min(i + 1 + HOLD, len(m15))):
            b = m15[j]
            if d_ == 'bull':
                if b['l'] <= stop:
                    o = -1.0; break
                if b['h'] >= tgt:
                    o = 1.0; break
            else:
                if b['h'] >= stop:
                    o = -1.0; break
                if b['l'] <= tgt:
                    o = 1.0; break
        if o is None:
            continue
        last = i + 2
        out.append((m15[i]['_ts'], o - cost(o, entry, R), relvol, bool(aligned)))
    return out


def line(label, rows):
    rows = sorted(rows, key=lambda x: x[0])
    seq = [r for _, r, _, _ in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r, _, _ in rows[:mid]])
    _, _, es = agg([r for _, r, _, _ in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<26} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    allrows = []
    for pk in CRYPTO:
        allrows += scan(pk)
    print('=' * 100)
    print('macdp (flagship live intraday method) + relative-volume gate — crypto, 1:1, bracket-honest, OOS')
    print('=' * 100)
    gated = lambda rel: [r for r in allrows if r[2] is not None and r[2] >= rel]
    line('base (no gate)', allrows)
    for rel in (1.2, 1.5, 2.0):
        line(f'relvol>={rel}', gated(rel))
    line('faytterro-aligned', [r for r in allrows if r[3]])
    line('relvol>=1.5 AND faytterro', [r for r in allrows if r[3] and r[2] is not None and r[2] >= 1.5])


if __name__ == '__main__':
    main()
