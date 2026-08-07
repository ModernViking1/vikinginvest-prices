"""astongilltrading '99% win rate' ensemble — decoded + tested across all pairs.

The '99%' is a prop-firm PASS probability (Monte Carlo), not a per-trade win rate;
the actual backtest shown is 60.3% WR / PF 1.835. The ensemble is AGT / PO3 / ORB A /
ORB B. AGT can't be reliably decoded from the screenshots, so this tests the two that
can:

  ORB (A=US open, B=London open) — opening-range breakout: the first hour after the
      session open sets a range; a close beyond it enters in that direction, stop at
      the opposite side of the range, fixed-RR target.
  PO3 (Power of Three)           — the opening range is 'accumulation'; a sweep beyond
      it that closes back inside is 'manipulation'; enter the 'distribution' move the
      other way, stop beyond the sweep, target the opposite side / fixed RR.

All on m15, market fills, dealing cost, bracket-honest (unresolved-in-hold excluded),
chronological OOS (both halves + and n>=40 = PASS). Reported per class.

Run: python astongill_orb_po3_research.py
"""
import json
import os
import datetime as dt
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from detect_triggers import PAIR_CLASS

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RNG_LEN = 4         # opening-range length in m15 bars (1 hour)
BREAK_WIN = 16      # bars after the range to look for a breakout / sweep (~4h)
BUF = 0.10          # buffer in ATR
HOLD = 192          # m15 bracket horizon (~2 days)
RRS = [1.5, 2.0, 3.0]
SESS = {'US': 13, 'LN': 7}        # session open hour (UTC): US 13:00, London 07:00
CLASSES = ['crypto', 'comm', 'index', 'major', 'minor']


def _by_day_hour(bars, hour):
    d = defaultdict(list)
    for i, b in enumerate(bars):
        if dt.datetime.utcfromtimestamp(b['_ts']).hour == hour:
            d[dt.datetime.utcfromtimestamp(b['_ts']).date()].append(i)
    return d


def orb_signals(bars, open_hour):
    """Opening-range breakout: close beyond the first-hour range -> enter that way."""
    out = []
    for day, idxs in sorted(_by_day_hour(bars, open_hour).items()):
        if not idxs:
            continue
        i0 = idxs[0]
        rng = bars[i0:i0 + RNG_LEN]
        if len(rng) < RNG_LEN:
            continue
        rhi = max(x['h'] for x in rng); rlo = min(x['l'] for x in rng)
        e_idx = i0 + RNG_LEN
        a = atr(bars, 14, e_idx) if e_idx < len(bars) else None
        if not a or a <= 0 or rhi <= rlo:
            continue
        for j in range(e_idx, min(e_idx + BREAK_WIN, len(bars) - 1)):
            b = bars[j]
            if b['c'] > rhi:                                   # break up = long
                entry = b['c']; stop = rlo - BUF * a
                if stop < entry:
                    out.append((j + 1, entry, stop, 'bull')); break
            if b['c'] < rlo:                                   # break down = short
                entry = b['c']; stop = rhi + BUF * a
                if stop > entry:
                    out.append((j + 1, entry, stop, 'bear')); break
    return out


def po3_signals(bars, open_hour):
    """Power-of-Three: sweep of the opening range that closes back inside -> fade it."""
    out = []
    for day, idxs in sorted(_by_day_hour(bars, open_hour).items()):
        if not idxs:
            continue
        i0 = idxs[0]
        rng = bars[i0:i0 + RNG_LEN]
        if len(rng) < RNG_LEN:
            continue
        rhi = max(x['h'] for x in rng); rlo = min(x['l'] for x in rng)
        e_idx = i0 + RNG_LEN
        a = atr(bars, 14, e_idx) if e_idx < len(bars) else None
        if not a or a <= 0 or rhi <= rlo:
            continue
        for j in range(e_idx, min(e_idx + BREAK_WIN, len(bars) - 1)):
            b = bars[j]
            if b['h'] > rhi and b['c'] < rhi:                  # swept high, closed back = short
                entry = b['c']; stop = b['h'] + BUF * a
                if stop > entry:
                    out.append((j + 1, entry, stop, 'bear')); break
            if b['l'] < rlo and b['c'] > rlo:                  # swept low, closed back = long
                entry = b['c']; stop = b['l'] - BUF * a
                if stop < entry:
                    out.append((j + 1, entry, stop, 'bull')); break
    return out


def walk(bars, i0, entry, stop, d, rr, hold):
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
    print(f"      {label:<10} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST))['pairs']
    variants = [('ORB A · US open', orb_signals, SESS['US']),
                ('ORB B · London open', orb_signals, SESS['LN']),
                ('PO3 · US open', po3_signals, SESS['US']),
                ('PO3 · London open', po3_signals, SESS['LN'])]
    print("=" * 92)
    print("astongill ensemble (ORB A/B + PO3) · m15 · market fills · cost · OOS (both halves + = PASS)")
    print("=" * 92)
    for name, fn, hour in variants:
        by_cls = {c: defaultdict(list) for c in CLASSES}
        for pk in [x for x in PAIR_CLASS if x in d]:
            bars = _bars_norm(d[pk].get('m15', []))
            if len(bars) < 400:
                continue
            cls = PAIR_CLASS[pk]
            for (ei, entry, stop, dr) in fn(bars, hour):
                if ei >= len(bars):
                    continue
                R = abs(entry - stop); ts = bars[ei]['_ts']
                for rr in RRS:
                    o = walk(bars, ei, entry, stop, dr, rr, HOLD)
                    if o is not None:
                        by_cls[cls][rr].append((ts, o - cost(o, entry, R)))
        print(f"\n===== {name} =====")
        for c in CLASSES:
            if not by_cls[c][RRS[0]]:
                continue
            print(f"   {c}:")
            for rr in RRS:
                line(f'RR {rr}', by_cls[c][rr])


if __name__ == '__main__':
    main()
