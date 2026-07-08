"""Path-accurate exit-overlay backtest — Base vs Breakeven vs Partial @ +0.5R.

Answers: does moving the stop to breakeven at +0.5R (H2), or banking half at
+0.5R (H3), improve the full-set result once you account for the REAL cost to
winners (retracing to BE) — not just the loss-only replay?

Method: reuses backtest_school_run_full's setup detection (confluence >= 2
creators, per-class RSI gate, hybrid min-R floor) to build the same trade set,
then models the cBot's actual structure — enter at the structural entry, stop
at the structural stop, 1:1 target — and walks the m15 path BAR BY BAR from
entry, computing three outcomes per DECIDED trade:

  base    : original stop / target
  BE      : once +0.5R is touched, stop moves to entry (0R). Path-accurate:
            a winner that retraces to entry after +0.5R is exited at 0.
  partial : bank half at +0.5R, runner rides to the base outcome
            (0.25 + 0.5*base_R when +0.5R was touched).

Within a bar, stop is checked before target (conservative). Reports
expectancy, total R, win rate, max drawdown (chronological), and the
empirical winner-retrace rate that BE actually incurs.

Run:  python backtest_exit_overlays.py
"""
import json
import sys
import time

from detect_triggers import RSI_GATE_BY_CLASS, PAIR_CLASS
from backtest_rsi_per_class import (
    _bars_norm, _min_prom,
    precompute_break_dirs, precompute_cl_dir, precompute_rsi,
    _find_struct_high, _find_struct_low,
)
from backtest_school_run_full import classify_setup

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB, TL_LB, EW_LB, BOS_LB = 5, 8, 8, 24
WALK_BARS = 48          # ~12h to resolve a 1:1 intraday trade
PARTIAL_TRIGGER = 0.5   # +0.5R
BE_TRIGGER = 0.5        # +0.5R


def walk_overlays(m15, i, entry, stop, target, direction):
    """From creator bar i (entry assumed filled at `entry`), walk the m15 path
    and return per-trade R for base / BE / partial + mfe, or None if the base
    trade doesn't resolve (expired) within WALK_BARS."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    sign = 1 if direction == 'bull' else -1

    def fav(p):
        return sign * (p - entry) / R   # favorable R-multiple

    mfe = 0.0
    armed = False                      # +0.5R touched -> arms BE stop @ entry & partial
    base_out = None
    be_out = None
    for j in range(i + 1, min(i + 1 + WALK_BARS, len(m15))):
        b = m15[j]
        hi, lo = b['h'], b['l']
        bar_fav = max(fav(hi), fav(lo))
        if base_out is None:
            mfe = max(mfe, bar_fav)
        if bar_fav >= 0.5:
            armed = True

        # ---- base ----
        if base_out is None:
            if direction == 'bull':
                hit_stop = lo <= stop
                hit_tgt = hi >= target
            else:
                hit_stop = hi >= stop
                hit_tgt = lo <= target
            if hit_stop:
                base_out = -1.0        # stop-first (conservative) when both in-bar
            elif hit_tgt:
                base_out = +1.0

        # ---- breakeven ----
        if be_out is None:
            eff_stop = entry if armed else stop
            if direction == 'bull':
                hit_bestop = lo <= eff_stop
                hit_tgt = hi >= target
            else:
                hit_bestop = hi >= eff_stop
                hit_tgt = lo <= target
            if hit_bestop:
                be_out = 0.0 if armed else -1.0
            elif hit_tgt:
                be_out = +1.0

        if base_out is not None and be_out is not None:
            break

    if base_out is None:
        return None                    # expired — excluded from all variants
    if be_out is None:
        be_out = 0.0 if armed else base_out
    partial_out = (0.25 + 0.5 * base_out) if mfe >= PARTIAL_TRIGGER else base_out
    return {'ts': m15[i]['_ts'], 'base': base_out, 'be': be_out,
            'partial': partial_out, 'mfe': mfe,
            'won': base_out > 0, 'be_killed_winner': base_out > 0 and be_out <= 0}


def find_trades(pair_data, pair_key):
    """Same setup detection as backtest_school_run_full.find_all_setups, but
    the forward walk computes the exit-overlay outcomes on the 1:1 structural
    trade instead of the fib tier outcome."""
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)

    def find_idx(ts_arr, ts):
        lo, hi = 0, len(ts_arr) - 1; ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if ts_arr[mid] <= ts: ans = mid; lo = mid + 1
            else: hi = mid - 1
        return ans

    gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pair_key), {'hi': 80, 'lo': 20})
    cls = PAIR_CLASS.get(pair_key)
    out = []
    last_resolved = -1
    n = len(m15)
    for i in range(40, n - 1):
        if i <= last_resolved: continue
        cur_ts = m15[i]['_ts']
        h1_idx = find_idx(h1_ts, cur_ts); d_idx = find_idx(daily_ts, cur_ts)
        if h1_idx < TL_LB or d_idx < EW_LB: continue
        ew, tl, nw, cl = ew_arr[d_idx], tl_arr[h1_idx], nw_arr[i], cl_arr[h1_idx]
        if ew is None or tl is None or nw is None or cl is None: continue
        confluence, direction = classify_setup(ew, tl, nw, cl)
        if confluence < 2 or direction is None: continue

        lb = m15[max(0, i - 8):i]
        if len(lb) < 5: continue
        sw_hi = max(b['h'] for b in lb); sw_lo = min(b['l'] for b in lb)
        mp = _min_prom(m15[i]['c'])
        if direction == 'bull':
            if not (m15[i]['c'] > sw_hi and (m15[i]['c'] - sw_hi) >= mp): continue
        else:
            if not (m15[i]['c'] < sw_lo and (sw_lo - m15[i]['c']) >= mp): continue

        bos = m15[max(0, i - BOS_LB):i]; prom = _min_prom(m15[i]['c'])
        if direction == 'bear':
            entry = m15[i]['h']; stop = _find_struct_high(bos, prom)
            if stop <= entry: continue
            R = stop - entry; target = entry - R
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos, prom)
            if stop >= entry: continue
            R = entry - stop; target = entry + R

        atr = m15[max(0, i - 20):i]
        if len(atr) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
            fx_floor = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if R < max(0.5 * atr20, fx_floor): continue

        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if direction == 'bull' and rsi_at >= gate['hi']: continue
            if direction == 'bear' and rsi_at <= gate['lo']: continue

        res = walk_overlays(m15, i, entry, stop, target, direction)
        if res is None:  # expired
            last_resolved = i + WALK_BARS
            continue
        res['confluence'] = confluence
        out.append(res)
        last_resolved = i + 1
    return out


def agg(trades, key):
    rs = [t[key] for t in trades]
    n = len(rs)
    if n == 0:
        return None
    exp = sum(rs) / n
    tot = sum(rs)
    wr = 100 * sum(1 for x in rs if x > 0) / n
    cum = peak = mdd = 0.0
    for t in sorted(trades, key=lambda x: x['ts']):
        cum += t[key]; peak = max(peak, cum); mdd = min(mdd, cum - peak)
    return exp, tot, wr, mdd


def report(trades, label):
    n = len(trades)
    print(f"\n{'='*72}\n{label}  ·  decided trades n={n}\n{'='*72}")
    print(f"{'variant':<14} {'expR':>8} {'totR':>9} {'WR%':>6} {'maxDD_R':>9}")
    print('-' * 52)
    for k, nm in (('base', 'BASE'), ('partial', 'PARTIAL +0.5R'), ('be', 'BREAKEVEN')):
        a = agg(trades, k)
        if a:
            print(f"{nm:<14} {a[0]:>+8.3f} {a[1]:>+9.1f} {a[2]:>6.1f} {a[3]:>9.1f}")
    winners = [t for t in trades if t['won']]
    killed = sum(1 for t in winners if t['be_killed_winner'])
    lt = [t for t in trades if not t['won']]
    lt_touch = sum(1 for t in lt if t['mfe'] >= 0.5)
    print(f"\n  winners: {len(winners)}  ·  BE retraced-and-killed: {killed} "
          f"({100*killed/max(1,len(winners)):.0f}% of winners)")
    print(f"  losers: {len(lt)}  ·  touched +0.5R first: {lt_touch} "
          f"({100*lt_touch/max(1,len(lt)):.0f}% of losers)")


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})
    universe = [p for p in PAIR_CLASS if p in pairs]
    print(f"Path-accurate exit-overlay backtest over {len(universe)} pairs...", file=sys.stderr)
    t0 = time.time()
    allt = []
    for p in universe:
        tr = find_trades(pairs[p], p)
        allt.extend(tr)
        print(f"  {p}: {len(tr)} decided trades", file=sys.stderr)
    print(f"done in {time.time()-t0:.1f}s · total {len(allt)} decided trades", file=sys.stderr)

    report(allt, "FULL UNIVERSE (all confluence >= 2)")
    report([t for t in allt if t['confluence'] >= 3], "HIGHER-CONVICTION (confluence >= 3, production-like)")
    report([t for t in allt if t['confluence'] >= 4], "4/4 ONLY")


if __name__ == '__main__':
    main()
