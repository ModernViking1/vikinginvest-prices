"""Opening-range FALSE-BREAKOUT reversal (fade to the opposite extreme).

Creator setup (E-mini S&P screenshot): map the opening-range high/low over a fixed
morning window (08:12-09:12 US Eastern by instruction); once price BREAKS OUT of the
range either direction, WAIT for a reversal signal (price recloses back inside the
range), then trade the reversal back to the OPPOSITE extreme of the range — break up
-> reverse short -> target the range LOW; break down -> reverse long -> target the range
HIGH. Stop beyond the failed-breakout extreme.

Tested near the US open AND other regional opens (London for EU indices / EUR-GBP FX;
Tokyo for JP225 / JPY), and across other pairs (FX / commodities / crypto) on the US
window. Realistic MARKET fills (reclaim-bar close, no favourable limit), fixed dealing
cost, one setup per session-day, chronological OOS split (both halves positive + n>=40
= PASS), per class.

Data: m15, UTC timestamps, 2026-05 .. 2026-08 (all EDT/BST/JST, no DST shift in-window).
  US Eastern = UTC-4 · London = UTC+1 · Tokyo = UTC+9.

Run: python opening_range_reversal_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from five_strategies_research import cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
BUF = 0.05           # stop buffer beyond the failed-breakout extreme, as a fraction of range
TRADE_MIN = 480      # minutes after the window to allow the setup to complete (~8h, same session)
MIN_OR_BARS = 3      # need at least 3 m15 bars in the opening-range window
US_INDEX = {'dj30', 'nas100', 'spx500'}
EU_INDEX = {'de40', 'ftse100'}


def _bars(raw):
    """m15 bars -> [(sortkey, tod_min, date, o,h,l,c)], UTC, chronological."""
    out = []
    for b in raw:
        t = b['t']
        tod = int(t[11:13]) * 60 + int(t[14:16])
        out.append((t, tod, t[:10], b['o'], b['h'], b['l'], b['c']))
    return out


def _score_day(day, w_start, w_end, variant):
    """One session-day's bars (list of tuples). Returns (rr, entry, R) or None."""
    orb = [b for b in day if w_start <= b[1] < w_end]
    if len(orb) < MIN_OR_BARS:
        return None
    or_hi = max(b[4] for b in orb); or_lo = min(b[5] for b in orb)
    rng = or_hi - or_lo
    if rng <= 0:
        return None
    post = [b for b in day if w_end <= b[1] < w_end + TRADE_MIN]
    if len(post) < 4:
        return None
    # find first breakout
    side = None; bi = None
    for i, b in enumerate(post):
        if b[4] > or_hi:
            side = 'up'; bi = i; break
        if b[5] < or_lo:
            side = 'down'; bi = i; break
    if side is None:
        return None
    # track failed-breakout extreme, then wait for reclaim back inside the range
    if side == 'up':
        ext = or_hi
        for j in range(bi, len(post)):
            ext = max(ext, post[j][4])
            candle_ok = (post[j][6] < post[j][3]) if variant == 'candle' else True   # bearish reclaim bar
            if post[j][6] < or_hi and candle_ok:                                     # reclaim inside
                entry = post[j][6]; stop = ext + BUF * rng; tgt = or_lo
                R = stop - entry
                if R <= 0 or entry <= tgt:
                    return None
                rr = (entry - tgt) / R
                for k in range(j + 1, len(post)):
                    if post[k][4] >= stop: return (-1.0, entry, R)
                    if post[k][5] <= tgt:  return (rr, entry, R)
                return None
    else:
        ext = or_lo
        for j in range(bi, len(post)):
            ext = min(ext, post[j][5])
            candle_ok = (post[j][6] > post[j][3]) if variant == 'candle' else True   # bullish reclaim bar
            if post[j][6] > or_lo and candle_ok:
                entry = post[j][6]; stop = ext - BUF * rng; tgt = or_hi
                R = entry - stop
                if R <= 0 or entry >= tgt:
                    return None
                rr = (tgt - entry) / R
                for k in range(j + 1, len(post)):
                    if post[k][5] <= stop: return (-1.0, entry, R)
                    if post[k][4] >= tgt:  return (rr, entry, R)
                return None
    return None


def _agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = _agg(seq); mid = len(rows) // 2
    _, _, eh = _agg([r for _, r in rows[:mid]]); _, _, es = _agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<8} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(title, w_start, w_end, universe, variant='reclaim'):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs and x in universe]:
        cls = PAIR_CLASS.get(pk)
        raw = pairs[pk].get('m15', [])
        if len(raw) < 500:
            continue
        bars = _bars(raw)
        days = defaultdict(list)
        for b in bars:
            days[b[2]].append(b)
        npr += 1
        for date in sorted(days):
            res = _score_day(days[date], w_start, w_end, variant)
            if res is not None:
                rr, entry, R = res
                store[cls].append((date, rr - cost(rr, entry, R)))
    print(f"\n===== {title}  [{variant}] — {npr} pairs =====")
    for c in ['index', 'major', 'minor', 'comm', 'crypto']:
        if store[c]:
            line(c, store[c])
    line('ALL', [r for c in store for r in store[c]])


def main():
    ALL = set(PAIR_CLASS)
    print("=" * 92)
    print("Opening-range false-breakout REVERSAL (fade to opposite extreme) — realistic fills")
    print("=" * 92)
    # US literal window 08:12-09:12 ET = 12:12-13:12 UTC, across all classes, both variants
    run("US 08:12-09:12 ET (literal) · all pairs", 732, 792, ALL, 'reclaim')
    run("US 08:12-09:12 ET (literal) · all pairs", 732, 792, ALL, 'candle')
    # US cash open 09:30-10:30 ET = 13:30-14:30 UTC (robustness)
    run("US 09:30-10:30 ET (cash open)   · all pairs", 810, 870, ALL, 'reclaim')
    # London open 08:00-09:00 London = 07:00-08:00 UTC — EU indices + EUR/GBP FX
    lon = EU_INDEX | {p for p in PAIR_CLASS if any(x in p for x in ('eur', 'gbp')) and PAIR_CLASS[p] in ('major', 'minor')}
    run("London 08:00-09:00 (07:00 UTC)  · EU idx + EUR/GBP", 420, 480, lon, 'reclaim')
    # Tokyo open 09:00-10:00 JST = 00:00-01:00 UTC — JP225 + JPY FX
    tyo = {'jp225'} | {p for p in PAIR_CLASS if 'jpy' in p and PAIR_CLASS[p] in ('major', 'minor')}
    run("Tokyo 09:00-10:00 JST (00:00 UTC)· JP225 + JPY", 0, 60, tyo, 'reclaim')


if __name__ == '__main__':
    main()
