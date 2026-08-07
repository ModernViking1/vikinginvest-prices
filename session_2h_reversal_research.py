"""itstomtrades 'second opening hour' session-reversal — tested on GOLD with discipline.

The setup (from the four clips):
  1. Wait for the SECOND opening hour of a session — focus Asia and London (US tested too).
  2. In that hour, look for an expansion move on rising volume pushing ONE direction
     (the session 'displacement').
  3. Drop to a lower timeframe and wait for a structure-shift REVERSAL against that move.
  4. Stop just beyond the reversal swing ('mark the stop above'); target the RANGE of the
     original breakout move. Also test higher reward:risk.

Data reality: gold has m15 (~3 months) + h1 (12 months), NO 5-minute feed. The 'lower
timeframe reversal (5min)' is therefore approximated on m15 — the finest we have — so this
is a PRELIMINARY read over the ~3-month m15 window, not a full-year verdict.

Everything on m15: displacement is self-calibrated per session hour (expansion vs the
trailing average of the SAME hour, rising volume, decisively directional); the reversal is
a close through the recent m15 swing against the move; market fills, dealing cost,
bracket-honest (unresolved-in-hold excluded), chronological OOS (both halves + and n>=40 =
PASS). Sessions keyed by the UTC hour of their 2nd hour (parameterisable).

Run: python session_2h_reversal_research.py
"""
import json
import os
import datetime as dt
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PAIR = 'xauusd'

# 2nd opening hour of each session, as a UTC hour (the session opens the hour before).
SESSIONS = {'asia': 1, 'london': 8, 'us': 14}

DISP_K = 1.10       # 2nd-hour range must exceed 1.10x the trailing avg of the SAME hour
VOL_K = 1.05        # ... on >= 1.05x that hour's trailing avg volume
DIRECTIONAL = 0.50  # net move / hour range — decisively one-directional
LOOKBACK = 10       # trailing same-hour occurrences for the averages
BUF = 0.10          # stop buffer in ATR
RRS = [1.0, 1.5, 2.0, 3.0]
# per-timeframe reversal/hold geometry (bars). h1 = full-year sample (coarse reversal);
# m15 = the ~3-month '5min proxy' fidelity check.
GEO = {'h1': dict(SWING=1, REV_WIN=6, HOLD=48, PERHR=1),
       'm15': dict(SWING=2, REV_WIN=24, HOLD=96, PERHR=4)}


def walk_bracket(bars, i0, entry, stop, target, td, hold):
    """Realized R for a bracketed trade; None if unresolved within hold (excluded)."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if td == 'short':
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return (entry - target) / R
        else:
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return (target - entry) / R
    return None


def hour_of(ts):
    return dt.datetime.utcfromtimestamp(ts).hour


def day_of(ts):
    return dt.datetime.utcfromtimestamp(ts).date()


def find_signals(bars, sess_hour, geo):
    """Yield (entry_idx, entry, stop, trade_dir, hour_range) for a session's 2nd hour."""
    by_day = defaultdict(list)
    for i, b in enumerate(bars):
        if hour_of(b['_ts']) == sess_hour:
            by_day[day_of(b['_ts'])].append(i)
    hist_range = []; hist_vol = []          # trailing same-hour stats
    for d in sorted(by_day):
        idxs = by_day[d]
        seg = [bars[i] for i in idxs]
        h_open = seg[0]['o']; h_close = seg[-1]['c']
        h_hi = max(x['h'] for x in seg); h_lo = min(x['l'] for x in seg)
        h_rng = h_hi - h_lo; h_vol = sum((x.get('v', 0) or 0) for x in seg)
        net = h_close - h_open
        if len(hist_range) >= LOOKBACK and h_rng > 0:
            avg_rng = sum(hist_range[-LOOKBACK:]) / LOOKBACK
            avg_vol = sum(hist_vol[-LOOKBACK:]) / LOOKBACK
            expansion = h_rng >= DISP_K * avg_rng
            volup = avg_vol > 0 and h_vol >= VOL_K * avg_vol
            directional = abs(net) / h_rng >= DIRECTIONAL
            if expansion and volup and directional:
                td = 'short' if net > 0 else 'long'      # trade AGAINST the displacement
                peak = h_hi if td == 'short' else h_lo
                sig = _reversal(bars, idxs[-1], td, peak, h_rng, geo)
                if sig:
                    yield sig
        hist_range.append(h_rng); hist_vol.append(h_vol)


def _reversal(bars, e_idx, td, peak, h_rng, geo):
    """First structure-break against the displacement within REV_WIN bars."""
    sw = geo['SWING']
    for j in range(e_idx + 1, min(e_idx + 1 + geo['REV_WIN'], len(bars))):
        b = bars[j]
        a = atr(bars, 14, j) or 0.0
        if a <= 0:
            continue
        if td == 'short':
            peak = max(peak, b['h'])
            recent_low = min(x['l'] for x in bars[max(0, j - sw):j])
            if b['c'] < recent_low:                       # break down = reversal
                entry = b['c']; stop = peak + BUF * a
                return (j + 1, entry, stop, 'short', h_rng) if stop > entry else None
        else:
            peak = min(peak, b['l'])
            recent_high = max(x['h'] for x in bars[max(0, j - sw):j])
            if b['c'] > recent_high:                       # break up = reversal
                entry = b['c']; stop = peak - BUF * a
                return (j + 1, entry, stop, 'long', h_rng) if stop < entry else None
    return None


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq); m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]]); _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<14} n={n:>3} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run_tf(bars, tf, note):
    geo = GEO[tf]
    print("\n" + "=" * 92)
    print(f"GOLD · {tf} ({note}) · {len(bars)} bars · HOLD {geo['HOLD']} bars")
    print("=" * 92)
    for sess, hr in SESSIONS.items():
        sigs = list(find_signals(bars, hr, geo))
        print(f"\n===== {sess.upper()} (2nd hour = {hr:02d}:00 UTC) — {len(sigs)} setups =====")
        if not sigs:
            print("      (no setups)"); continue
        rng_rows = []
        for (ei, entry, stop, td, h_rng) in sigs:
            if ei >= len(bars):
                continue
            target = entry - h_rng if td == 'short' else entry + h_rng
            o = walk_bracket(bars, ei, entry, stop, target, td, geo['HOLD'])
            if o is not None:
                rng_rows.append((bars[ei]['_ts'], o - cost(1 if o > 0 else -1, entry, abs(entry - stop))))
        line('range-target', rng_rows)
        for rr in RRS:
            rows = []
            for (ei, entry, stop, td, h_rng) in sigs:
                if ei >= len(bars):
                    continue
                R = abs(entry - stop)
                target = entry - rr * R if td == 'short' else entry + rr * R
                o = walk_bracket(bars, ei, entry, stop, target, td, geo['HOLD'])
                if o is not None:
                    rows.append((bars[ei]['_ts'], o - cost(o, entry, R)))
            line(f'RR {rr:.1f}', rows)


def main():
    g = json.load(open(HIST))['pairs'][PAIR]
    print("itstomtrades 2nd-hour session reversal · GOLD (XAUUSD)")
    print("displacement = expansion + rising volume + directional | reversal = structure break against it")
    run_tf(_bars_norm(g['h1']), 'h1', 'full year — larger sample, coarse reversal')
    run_tf(_bars_norm(g['m15']), 'm15', '~3 months — 5min proxy, fidelity check')


if __name__ == '__main__':
    main()
