"""Detailed per-class breakdown of the Market Wizards / breakthrough setups.

Answers 'why do they only fit crypto?' by running every setup on EVERY class —
FX majors & minors, indices, commodities, crypto, and US equities (from the pilot
data) — with the trailing runner exit that made the continuation setups work.

Trailing exit: arm at +1R, trail 1R behind the best price, mark-to-market at the
hold horizon (a runner is still an open position at timeout). Market fills, dealing
cost, chronological OOS (both halves + and n>=40 = PASS). h1 for all; a separate
m15 pass for Holy Grail (intraday candidate).

Run: python market_wizards_breakdown.py
"""
import json
import os
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from detect_triggers import PAIR_CLASS
from market_wizards_research import turtle_soup, holy_grail, two_b, volbreak

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
EQUITY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'equity-ohlc.json')
EQ_SYMBOLS = ['aapl', 'nvda', 'tsla', 'msft', 'amzn']


def walk_trail_open(bars, ei, entry, stop, d, arm=1.0, trail=1.0, hold=200):
    R = abs(entry - stop)
    if R <= 0:
        return None
    es = stop; armed = False; peak = entry
    for j in range(ei, min(ei + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es: return (es - entry) / R
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + arm * R: armed = True
            if armed: es = max(es, peak - trail * R)
        else:
            if b['h'] >= es: return (entry - es) / R
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - arm * R: armed = True
            if armed: es = min(es, peak + trail * R)
    lc = bars[min(ei + hold, len(bars) - 1)]['c']
    return ((lc - entry) if d == 'bull' else (entry - lc)) / R


def stat(rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq); m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]]); _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    return n, wr, e, eh, es, v


def collect(fn, tf='h1'):
    """Return {class: [rows]} for a detector across the whole universe + equities."""
    out = defaultdict(list)
    d = json.load(open(HIST))['pairs']
    for pk in [x for x in PAIR_CLASS if x in d]:
        cls = PAIR_CLASS.get(pk)
        bars = _bars_norm(d[pk].get(tf, []))
        if len(bars) < 400:
            continue
        for (ei, entry, stop, dr) in fn(bars):
            if ei >= len(bars):
                continue
            o = walk_trail_open(bars, ei, entry, stop, dr)
            if o is not None:
                out[cls].append((bars[ei]['_ts'], o - cost(1 if o > 0 else -1, entry, abs(entry - stop))))
    if os.path.exists(EQUITY):
        eq = json.load(open(EQUITY)).get('pairs', {})
        for pk in EQ_SYMBOLS:
            bars = _bars_norm(eq.get(pk, {}).get(tf, []))
            if len(bars) < 400:
                continue
            for (ei, entry, stop, dr) in fn(bars):
                if ei >= len(bars):
                    continue
                o = walk_trail_open(bars, ei, entry, stop, dr)
                if o is not None:
                    out['equity'].append((bars[ei]['_ts'], o - cost(1 if o > 0 else -1, entry, abs(entry - stop))))
    return out


CLASSES = ['crypto', 'equity', 'index', 'comm', 'major', 'minor']


def main():
    print("=" * 100)
    print("Market Wizards / breakthrough — TRAILING runner exit, per class (incl. US equities), h1, OOS")
    print("=" * 100)
    for name, fn in [('turtle_soup', turtle_soup), ('holy_grail', holy_grail),
                     ('two_b', two_b), ('volbreak (breakthrough)', volbreak)]:
        print(f"\n===== {name} =====")
        byc = collect(fn)
        for cls in CLASSES:
            if not byc[cls]:
                continue
            n, wr, e, eh, es, v = stat(byc[cls])
            print(f"      {cls:<8} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")

    # Holy Grail on m15 (intraday candidate) — crypto only (the live intraday class)
    print("\n===== holy_grail · m15 (intraday) =====")
    byc = collect(holy_grail, tf='m15')
    for cls in CLASSES:
        if not byc[cls]:
            continue
        n, wr, e, eh, es, v = stat(byc[cls])
        print(f"      {cls:<8} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


if __name__ == '__main__':
    main()
