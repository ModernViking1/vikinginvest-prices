"""Backtest sweep for Option A — post-break monotonic-reversal guard on
calc_independent_dir. Tests whether downgrading the structural break
direction to neutral when the LAST 3 closes are monotonically against
the break would improve aggregate WR.

The guard is the same shape as the 2026-06-10f one I added to
calc_4h_cloud_dir (the EUR/AUD fix), applied here to the structural
direction calc instead.

Test configurations (cumulative — each row adds a level):

  baseline       guard nowhere (current production)
  guard_15m      guard at NW only (15m, lookback=5)
  guard_15m_1h   guard at NW + TL (15m + 1H, lookback=5 + 8)
  guard_all      guard at NW + TL + EW (daily lookback=8 too)

For each, measure:
  - Aggregate WR (decided trades only)
  - Total decided setup count (gauge filter aggressiveness)
  - Per-asset-class WR breakdown

Decision rule: deploy the configuration with the best aggregate WR
that DOESN'T cut decided count by more than ~25%. A guard that drops
the count too far is over-firing on routine noise.
"""
import json
import sys
import time

from detect_triggers import (
    RSI_GATE_BY_CLASS, PAIR_CLASS,
)
from backtest_rsi_per_class import (
    DROPPED,
    _bars_norm, _min_prom,
    _find_struct_high, _find_struct_low,
    precompute_cl_dir, precompute_rsi,
)

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

NW_LB = 5
TL_LB = 8
EW_LB = 8
BOS_LB = 24
EXPIRY = 8


def _min_prom_local(px):
    ap = abs(px)
    if ap > 1000:
        return ap * 0.001
    if ap > 5:
        return ap * 0.0008
    return 0.0005


def precompute_break_dirs_with_guard(bars, lookback, enable_guard=False):
    """Variant of precompute_break_dirs that optionally applies the
    post-break monotonic-reversal guard at every bar idx.

    Guard: if last_break_dir was bear AND the last 3 close deltas at
    bar i are monotonically positive → flip to None (let slope check
    or neutral take over). Mirror for bull. Bumps the guard before
    the 2× lookback staleness decay so we catch the early reversal
    case the staleness check misses.
    """
    n = len(bars)
    out = [None] * n
    last_dir = None
    last_idx = -1
    for i in range(n):
        if i < lookback:
            continue
        slc = bars[max(0, i - lookback):i]
        if len(slc) < 5:
            out[i] = last_dir
            continue
        sw_hi = max(b['h'] for b in slc)
        sw_lo = min(b['l'] for b in slc)
        c = bars[i]['c']
        mp = _min_prom_local(c)
        if c > sw_hi and (c - sw_hi) >= mp:
            last_dir = 'bull'
            last_idx = i
        elif c < sw_lo and (sw_lo - c) >= mp:
            last_dir = 'bear'
            last_idx = i

        # Resolve this bar's direction
        cur_dir = last_dir

        # Apply post-break momentum-reversal guard
        if enable_guard and last_dir is not None and i >= 3:
            c0 = bars[i - 3]['c']; c1 = bars[i - 2]['c']
            c2 = bars[i - 1]['c']; c3 = bars[i]['c']
            up_seq = (c1 > c0 and c2 > c1 and c3 > c2)
            dn_seq = (c1 < c0 and c2 < c1 and c3 < c2)
            if last_dir == 'bear' and up_seq:
                cur_dir = None  # flip to neutral signal
            elif last_dir == 'bull' and dn_seq:
                cur_dir = None

        # Staleness decay 2× lookback (existing rule, applied AFTER guard)
        if cur_dir is not None and last_idx >= 0:
            bars_since = i - last_idx
            if bars_since >= 2 * lookback:
                cur_dir = None

        if cur_dir is None:
            # Slope fallback (mirrors calc_independent_dir)
            if i >= lookback:
                first_c = bars[i - lookback]['c']
                change = (c - first_c) / first_c if first_c else 0
                if change > 0.0015:
                    out[i] = 'bull'
                elif change < -0.0015:
                    out[i] = 'bear'
                else:
                    out[i] = 'neutral'
            else:
                out[i] = 'neutral'
        else:
            out[i] = cur_dir
    return out


def find_setups(pair_data, pair_key, guard_at_nw=False, guard_at_tl=False, guard_at_ew=False):
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None
    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]

    ew_arr = precompute_break_dirs_with_guard(daily, EW_LB, enable_guard=guard_at_ew)
    tl_arr = precompute_break_dirs_with_guard(h1, TL_LB, enable_guard=guard_at_tl)
    nw_arr = precompute_break_dirs_with_guard(m15, NW_LB, enable_guard=guard_at_nw)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)

    def find_h1_idx(ts):
        lo, hi = 0, len(h1_ts) - 1
        ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if h1_ts[mid] <= ts:
                ans = mid; lo = mid + 1
            else:
                hi = mid - 1
        return ans

    def find_d_idx(ts):
        lo, hi = 0, len(daily_ts) - 1
        ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if daily_ts[mid] <= ts:
                ans = mid; lo = mid + 1
            else:
                hi = mid - 1
        return ans

    gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pair_key), {'hi': 80, 'lo': 20})
    setups = []
    last_resolved = -1
    n_m15 = len(m15)

    for i in range(40, n_m15 - 1):
        if i <= last_resolved:
            continue
        cur_ts = m15[i]['_ts']
        h1_idx = find_h1_idx(cur_ts)
        d_idx = find_d_idx(cur_ts)
        if h1_idx < TL_LB or d_idx < EW_LB:
            continue

        ew = ew_arr[d_idx]
        tl = tl_arr[h1_idx]
        nw = nw_arr[i]
        cl = cl_arr[h1_idx]
        if not ew or not tl or not nw or not cl:
            continue
        if ew != tl or tl != nw or nw != cl:
            continue
        if ew not in ('bull', 'bear'):
            continue

        lb_slc = m15[max(0, i - 8):i]
        if len(lb_slc) < 5:
            continue
        sw_hi = max(b['h'] for b in lb_slc)
        sw_lo = min(b['l'] for b in lb_slc)
        mp = _min_prom(m15[i]['c'])
        if ew == 'bull':
            if not (m15[i]['c'] > sw_hi and (m15[i]['c'] - sw_hi) >= mp):
                continue
        else:
            if not (m15[i]['c'] < sw_lo and (sw_lo - m15[i]['c']) >= mp):
                continue

        bos_slc = m15[max(0, i - BOS_LB):i]
        prom = _min_prom(m15[i]['c'])
        if ew == 'bear':
            entry = m15[i]['h']
            stop = _find_struct_high(bos_slc, prom)
            if stop <= entry:
                continue
            R = stop - entry
            target = entry - R
        else:
            entry = m15[i]['l']
            stop = _find_struct_low(bos_slc, prom)
            if stop >= entry:
                continue
            R = entry - stop
            target = entry + R

        atr_slc = m15[max(0, i - 20):i]
        if len(atr_slc) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr_slc) / len(atr_slc)
            if R < 0.5 * atr20:
                continue

        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if ew == 'bull' and rsi_at >= gate['hi']:
                continue
            if ew == 'bear' and rsi_at <= gate['lo']:
                continue

        # Forward walk — wick path only (the deployed methodology mix
        # for indices is in WICK_METHODOLOGY_OVERRIDES + FIB_ENTRY_PAIRS;
        # we test the wick path here since that's where the user's
        # ETHUSD case lives).
        lift_done = False
        outcome = None
        last_j = i
        for j in range(i + 1, min(i + 1 + EXPIRY + 32, n_m15)):
            b = m15[j]
            if not lift_done:
                if ew == 'bull' and b['h'] >= m15[i]['h']:
                    lift_done = True
                elif ew == 'bear' and b['l'] <= m15[i]['l']:
                    lift_done = True
            if lift_done:
                reach = (ew == 'bull' and b['l'] <= entry) or (ew == 'bear' and b['h'] >= entry)
                if reach:
                    for jj in range(j, min(j + 32, n_m15)):
                        bb = m15[jj]
                        if ew == 'bull':
                            if bb['l'] <= stop:
                                outcome = 'loss'; last_j = jj; break
                            if bb['h'] >= target:
                                outcome = 'win'; last_j = jj; break
                        else:
                            if bb['h'] >= stop:
                                outcome = 'loss'; last_j = jj; break
                            if bb['l'] <= target:
                                outcome = 'win'; last_j = jj; break
                    break
            if j - i > EXPIRY:
                outcome = 'expired'; last_j = j; break
        last_resolved = last_j
        setups.append({'class': PAIR_CLASS.get(pair_key), 'outcome': outcome})
    return setups


def tally(setups):
    w = sum(1 for s in setups if s['outcome'] == 'win')
    l = sum(1 for s in setups if s['outcome'] == 'loss')
    e = sum(1 for s in setups if s['outcome'] == 'expired')
    decided = w + l
    return {'n': len(setups), 'w': w, 'l': l, 'e': e,
            'decided': decided, 'wr': (w / decided * 100) if decided else None}


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    configs = [
        ('baseline', False, False, False),
        ('guard_nw_only', True, False, False),
        ('guard_nw_tl',   True, True, False),
        ('guard_all',     True, True, True),
    ]

    results = {}
    for name, gnw, gtl, gew in configs:
        print(f'Running config: {name}...', file=sys.stderr)
        t0 = time.time()
        all_setups = []
        for k in sorted(pairs.keys()):
            if k in DROPPED or k not in PAIR_CLASS:
                continue
            setups = find_setups(pairs[k], k,
                                 guard_at_nw=gnw, guard_at_tl=gtl, guard_at_ew=gew)
            if setups:
                all_setups.extend(setups)
        results[name] = all_setups
        agg = tally(all_setups)
        wr = f'{agg["wr"]:.1f}%' if agg['wr'] is not None else '—'
        print(f'  {name}: n={agg["n"]} decided={agg["decided"]} '
              f'WR={wr} ({time.time() - t0:.1f}s)', file=sys.stderr)

    # ─── Aggregate table ───
    print()
    print('=' * 90)
    print('OPTION A SWEEP — post-break monotonic-reversal guard on calc_independent_dir')
    print('=' * 90)
    print('Guard rule: if last_break_dir was bear AND last 3 closes are monotonically up → neutral.')
    print('            Mirror for bull. Applied BEFORE the 2× lookback staleness decay.')
    print()
    print(f'{"config":<16}  {"n_setups":>9}  {"n_decided":>9}  {"W":>4}  {"L":>4}  {"E":>4}  {"WR":>7}  {"vs baseline":>11}  {"Δsetups":>9}')
    print('-' * 100)
    base = tally(results['baseline'])
    for name, _, _, _ in configs:
        a = tally(results[name])
        wr_s = f'{a["wr"]:.1f}%' if a['wr'] is not None else '—'
        if a['wr'] is not None and base['wr'] is not None:
            d_wr = f'{a["wr"] - base["wr"]:+.2f}pp'
        else:
            d_wr = '—'
        d_n = a['n'] - base['n']
        d_pct = (d_n / base['n'] * 100) if base['n'] else 0
        d_n_s = f'{d_n:+d} ({d_pct:+.1f}%)'
        print(f'{name:<16}  {a["n"]:>9}  {a["decided"]:>9}  {a["w"]:>4}  {a["l"]:>4}  {a["e"]:>4}  {wr_s:>7}  {d_wr:>11}  {d_n_s:>9}')

    # ─── Per-class for each config ───
    print()
    print('=' * 90)
    print('PER-CLASS WR — same configs, broken out')
    print('=' * 90)
    classes = ('major', 'minor', 'comm', 'index', 'crypto')
    print(f'{"class":<8}  ' + '  '.join([f'{c:>15}' for c, _, _, _ in configs]))
    print('-' * 90)
    for cls in classes:
        row = f'{cls:<8}  '
        for name, _, _, _ in configs:
            sub = [s for s in results[name] if s['class'] == cls]
            t = tally(sub)
            if t['wr'] is not None:
                row += f'  {t["wr"]:>5.1f}% (n={t["decided"]:>3}) '
            else:
                row += f'  {"— (n=0)":>14} '
        print(row)


if __name__ == '__main__':
    main()
