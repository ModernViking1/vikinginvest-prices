"""Tom Hougaard 'add to a winner' — opening-range breakout + one pyramid add.

Rules (from the screenshots): define the session opening range (first 15m); on a
CONFIRMED close beyond it, enter a probe (stop = opposite side of the OR, R = that
distance). ADD one unit only if unrealized >= +1R AND structure confirms continuation
(higher-low then micro-high break for longs; mirror for shorts); on the add, move the
first unit to breakeven and trail both under structure. Max one add. Exit on the
trailing stop or an end-of-session flat. Indices: DAX / DJI / FTSE.

The question this strategy exists to answer is: does ADDING beat the plain probe?
So we run both with identical entries and compare:
  PROBE-ONLY  : one unit, initial OR stop, hold to stop / session end.
  PROBE+ADD   : identical until the add trigger, then +1 unit, breakeven the first,
                trail both under structure. (Trades that never trigger the add are
                identical in both — the delta is purely the add mechanism.)
Total R is in units of the initial risk R. Realistic: entry/exit at bar close/level,
fixed dealing cost per unit. Chronological OOS split (both halves +).

Sessions use a per-index UTC open (DST shifts it ~1h half the year — the RELATIVE
probe-vs-add comparison is robust to that; alt open hours are swept for the absolute).

Run: python hougaard_pyramid_research.py
"""
import json, os
from collections import defaultdict
from backtest_rsi_per_class import _bars_norm

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
OR_BARS = 1                 # 15-minute opening range = one m15 bar
SESSION_BARS = 32           # ~8h session window after the OR
FRAC = 1                    # fractal half-width for micro swing detection
# primary session opens (UTC, hh*3600+mm*60) and sensitivity alternates
OPENS = {
    'de40':    [7 * 3600, 6 * 3600, 8 * 3600],          # DAX cash ~08:00/09:00 CET
    'ftse100': [8 * 3600, 7 * 3600],                    # FTSE 08:00 London
    'dj30':    [14 * 3600 + 1800, 13 * 3600 + 1800],    # DJI 09:30 ET
}


def cost_R(o, entry, R):
    frac = R / abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def sessions(m15, open_sec):
    """Yield the bar-index window [or_start, end) for each session."""
    n = len(m15)
    for i in range(n):
        if int(m15[i]['_ts'] % 86400) == open_sec:
            yield i, min(i + SESSION_BARS, n)


def swings(bars, lo_or_hi):
    """Confirmed fractal swing indices (confirmed FRAC bars later)."""
    out = []
    for i in range(FRAC, len(bars) - FRAC):
        seg = bars[i - FRAC:i + FRAC + 1]
        if lo_or_hi == 'lo' and bars[i]['l'] == min(x['l'] for x in seg):
            out.append(i)
        if lo_or_hi == 'hi' and bars[i]['h'] == max(x['h'] for x in seg):
            out.append(i)
    return out


def simulate(sess, side, entry_j, entry1, stop1):
    """Return (R_probe_only, R_probe_add) in units of initial R for one trade."""
    R = abs(entry1 - stop1)
    n = len(sess)
    # ---- PROBE-ONLY: hold unit1 with the OR stop to stop-hit / session end ----
    ro = None
    for j in range(entry_j + 1, n):
        b = sess[j]
        if side == 'long' and b['l'] <= stop1:
            ro = (stop1 - entry1) / R; break
        if side == 'short' and b['h'] >= stop1:
            ro = (entry1 - stop1) / R; break
    if ro is None:
        ex = sess[n - 1]['c']
        ro = (ex - entry1) / R if side == 'long' else (entry1 - ex) / R

    # ---- PROBE+ADD ----
    lows = swings(sess, 'lo'); highs = swings(sess, 'hi')
    added = False; entry2 = None; stop = stop1; ra = None
    for j in range(entry_j + 1, n):
        b = sess[j]
        if not added:
            # pre-add stop = OR stop
            if side == 'long' and b['l'] <= stop1:
                ra = (stop1 - entry1) / R; break
            if side == 'short' and b['h'] >= stop1:
                ra = (entry1 - stop1) / R; break
            # add trigger: +1R reached AND structure confirms continuation
            sl = [k for k in lows if entry_j < k < j]      # swing lows after breakout, confirmed before j
            sh = [k for k in highs if entry_j < k < j]
            if side == 'long':
                hit_1R = b['h'] >= entry1 + R
                higher_low = len(sl) >= 2 and sess[sl[-1]]['l'] > sess[sl[-2]]['l']
                micro_break = len(sh) >= 1 and b['c'] > sess[sh[-1]]['h']
                if hit_1R and higher_low and micro_break:
                    entry2 = b['c']; stop = entry1; added = True   # breakeven the first unit
                    trail_anchor = sess[sl[-1]]['l']
            else:
                hit_1R = b['l'] <= entry1 - R
                lower_high = len(sh) >= 2 and sess[sh[-1]]['h'] < sess[sh[-2]]['h']
                micro_break = len(sl) >= 1 and b['c'] < sess[sl[-1]]['l']
                if hit_1R and lower_high and micro_break:
                    entry2 = b['c']; stop = entry1; added = True
                    trail_anchor = sess[sh[-1]]['h']
        else:
            # trailing under structure
            if side == 'long':
                nl = [k for k in lows if entry_j < k < j]
                if nl:
                    trail_anchor = max(trail_anchor, sess[nl[-1]]['l'])
                stop = max(stop, trail_anchor)
                if b['l'] <= stop:
                    ex = stop
                    ra = ((ex - entry1) + (ex - entry2)) / R; break
            else:
                nh = [k for k in highs if entry_j < k < j]
                if nh:
                    trail_anchor = min(trail_anchor, sess[nh[-1]]['h'])
                stop = min(stop, trail_anchor)
                if b['h'] >= stop:
                    ex = stop
                    ra = ((entry1 - ex) + (entry2 - ex)) / R; break
    if ra is None:
        ex = sess[n - 1]['c']
        if not added:
            ra = (ex - entry1) / R if side == 'long' else (entry1 - ex) / R
        else:
            ra = (((ex - entry1) + (ex - entry2)) / R) if side == 'long' else (((entry1 - ex) + (entry2 - ex)) / R)
    # dealing cost: probe-only = 1 unit, probe+add = 2 units when added
    ro -= cost_R(ro, entry1, R)
    ra -= cost_R(ra, entry1, R) * (2 if added else 1)
    return ro, ra, added


def run_index(m15, open_sec):
    only, addv, nadd = [], [], 0
    for i0, end in sessions(m15, open_sec):
        sess = m15[i0:end]
        if len(sess) < OR_BARS + 4:
            continue
        orb = sess[:OR_BARS]
        OR_hi = max(b['h'] for b in orb); OR_lo = min(b['l'] for b in orb)
        # first confirmed close beyond the OR
        side = None
        for j in range(OR_BARS, len(sess)):
            c = sess[j]['c']
            if c > OR_hi:
                side = 'long'; entry_j = j; break
            if c < OR_lo:
                side = 'short'; entry_j = j; break
        if side is None:
            continue
        entry1 = sess[entry_j]['c']
        stop1 = OR_lo if side == 'long' else OR_hi
        if abs(entry1 - stop1) <= 0:
            continue
        ro, ra, added = simulate(sess, side, entry_j, entry1, stop1)
        ts = sess[entry_j]['_ts']
        only.append((ts, ro)); addv.append((ts, ra)); nadd += int(added)
    return only, addv, nadd


def stats(rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n = len(seq)
    if not n:
        return (0, 0, 0, 0, 0)
    wr = 100 * sum(1 for r in seq if r > 0) / n; e = sum(seq) / n
    mid = n // 2
    eh = sum(r for _, r in rows[:mid]) / mid if mid else 0
    es = sum(r for _, r in rows[mid:]) / (n - mid) if n - mid else 0
    return (n, wr, e, eh, es)


def line(label, rows):
    n, wr, e, eh, es = stats(rows)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    return f"{label:<20} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}"


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    print("=" * 92)
    print("TOM HOUGAARD — opening-range breakout + one pyramid ADD (does adding beat the probe?)")
    print("=" * 92)
    pooled_only, pooled_add = [], []
    for pk in ('de40', 'dj30', 'ftse100'):
        if pk not in pairs:
            continue
        m15 = _bars_norm(pairs[pk].get('m15', []))
        print(f"\n### {pk.upper()} (m15) ###")
        for oi, open_sec in enumerate(OPENS[pk]):
            only, addv, nadd = run_index(m15, open_sec)
            tag = "primary" if oi == 0 else f"alt open {open_sec//3600:02d}:{(open_sec%3600)//60:02d}"
            hhmm = f"{open_sec//3600:02d}:{(open_sec%3600)//60:02d}"
            print(f"  open {hhmm} ({tag}) — {len(only)} trades, add fired on {nadd} ({100*nadd/max(1,len(only)):.0f}%)")
            print(f"    {line('PROBE-ONLY', only)}")
            print(f"    {line('PROBE+ADD', addv)}")
            if oi == 0:
                pooled_only += only; pooled_add += addv
    print("\n### POOLED (DAX+DJI+FTSE, primary opens) ###")
    print(f"  {line('PROBE-ONLY', pooled_only)}")
    print(f"  {line('PROBE+ADD', pooled_add)}")
    no = stats(pooled_only); na = stats(pooled_add)
    print(f"  ADD DELTA: exp {na[2]-no[2]:+.3f}R  (probe {no[2]:+.3f} -> add {na[2]:+.3f})")


if __name__ == '__main__':
    main()
