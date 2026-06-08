"""Auto-EW confidence threshold sweep — estimate the WR impact of
raising AUTO_EW_MIN_CONFIDENCE from the current 0.70 to {0.75, 0.80,
0.85}, with a 0.65 floor for context.

Concern from the user: a 0.80 hard gate might block too many trades.
This sweep answers: at each candidate threshold, how many trades do we
keep, what's the per-class WR, and what's the aggregate delta vs the
current 0.70?

Method (extends backtest_rsi_per_class.py):
  - For each pair, precompute auto-EW result {dir, confidence} at every
    daily bar (one auto_detect_ew call per bar, cached for the whole
    sweep). This is the heavy work — ~30s per pair on the active 40-pair
    universe.
  - Precompute structural EW direction (same as the RSI sweep).
  - For each candidate threshold T, walk the m15 series with:
      effective_ew(d) = auto_ew[d].dir if auto_ew[d].conf >= T
                                          and pattern in VALID
                       else structural_ew[d]
  - Check 4/4 alignment with effective_ew, find creator bars, apply the
    A1 per-class RSI gate (live config), walk forward to outcome.
  - Aggregate by class + total.

Caveat: same simplification as the RSI sweep (no fib half-size, no
counter-bar / opposing-CHoCH invalidation walks). Threshold-independent
components cancel in the per-threshold deltas, so the numbers are
decision-useful for choosing whether to deploy.
"""
import json
import sys
import time

from detect_triggers import (
    auto_detect_ew, AUTO_EW_VALID_PATTERNS,
    RSI_GATE_BY_CLASS,  # the deployed per-class A1 gate
)
from backtest_rsi_per_class import (
    PAIR_CLASS, DROPPED,
    _bars_norm, _min_prom,
    precompute_break_dirs, precompute_cl_dir, precompute_rsi,
    _find_struct_high, _find_struct_low,
)

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

# Threshold sweep — production currently 0.70.
THRESHOLDS = [0.65, 0.70, 0.75, 0.80, 0.85]

NW_LOOKBACK = 5
TL_LOOKBACK = 8
EW_LOOKBACK = 8
BOS_LOOKBACK = 24
EXPIRY_BARS = 8


def precompute_auto_ew(daily):
    """For each daily idx ≥ 30, run auto_detect_ew on daily[:i+1] and
    store {dir, conf} for valid patterns. None entries elsewhere.

    Expensive — one call per daily bar. Cached so the sweep across N
    thresholds reuses the same precomputation.
    """
    n = len(daily)
    out = [None] * n
    for i in range(30, n):
        try:
            result = auto_detect_ew(daily[:i + 1])
            if not result.get('ok'):
                continue
            ew = result.get('ew', {})
            pat = ew.get('pattern')
            d = ew.get('dir')
            conf = ew.get('confidence', 0)
            if d in ('bull', 'bear') and pat in AUTO_EW_VALID_PATTERNS:
                out[i] = {'dir': d, 'conf': conf}
        except Exception:
            pass
    return out


def find_setups_at_threshold(pair_data, threshold, pair_key):
    """Walk m15 + h1 + daily and return list of resolved setups using
    the auto-EW threshold to gate the macro direction. Each setup
    captures dir + outcome so apply_rsi can replay at any per-class
    RSI gate.
    """
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))

    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None, 0

    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]

    # Precomputed direction + RSI arrays (structural EW serves as fallback
    # when auto-EW conf < threshold)
    ew_struct = precompute_break_dirs(daily, EW_LOOKBACK)
    tl_arr = precompute_break_dirs(h1, TL_LOOKBACK)
    nw_arr = precompute_break_dirs(m15, NW_LOOKBACK)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    auto_ew = pair_data.get('_cached_auto_ew')  # set by caller (sweep shares)

    n_m15 = len(m15)

    def find_h1_idx(ts):
        lo, hi = 0, len(h1_ts) - 1
        ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if h1_ts[mid] <= ts:
                ans = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return ans

    def find_d_idx(ts):
        lo, hi = 0, len(daily_ts) - 1
        ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if daily_ts[mid] <= ts:
                ans = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return ans

    def get_effective_ew(d_idx):
        ae = auto_ew[d_idx] if auto_ew and d_idx < len(auto_ew) else None
        if ae and ae['conf'] >= threshold:
            return ae['dir']
        return ew_struct[d_idx] if d_idx < len(ew_struct) else None

    # Per-class RSI gate (the deployed config)
    rsi_gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pair_key), {'hi': 80, 'lo': 20})

    setups = []
    auto_ew_setups = 0  # diagnostic: how many setups used auto-EW dir
    last_resolved = -1
    for i in range(40, n_m15 - 1):
        if i <= last_resolved:
            continue
        cur_ts = m15[i]['_ts']
        h1_idx = find_h1_idx(cur_ts)
        d_idx = find_d_idx(cur_ts)
        if h1_idx < TL_LOOKBACK or d_idx < EW_LOOKBACK:
            continue

        ew = get_effective_ew(d_idx)
        tl = tl_arr[h1_idx]
        nw = nw_arr[i]
        cl = cl_arr[h1_idx]
        if not ew or not tl or not nw or not cl:
            continue
        if ew != tl or tl != nw or nw != cl:
            continue
        if ew not in ('bull', 'bear'):
            continue

        # Diagnostic: did auto-EW carry this setup?
        ae = auto_ew[d_idx] if auto_ew and d_idx < len(auto_ew) else None
        used_auto = bool(ae and ae['conf'] >= threshold and ae['dir'] == ew)

        # Creator check
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

        # Setup
        bos_slc = m15[max(0, i - BOS_LOOKBACK):i]
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

        # Min-R floor
        atr_slc = m15[max(0, i - 20):i]
        if len(atr_slc) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr_slc) / len(atr_slc)
            if R < 0.5 * atr20:
                continue

        # A1 per-class RSI gate
        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if ew == 'bull' and rsi_at >= rsi_gate['hi']:
                continue
            if ew == 'bear' and rsi_at <= rsi_gate['lo']:
                continue

        if used_auto:
            auto_ew_setups += 1

        # Forward walk
        lift_done = False
        outcome = None
        last_j = i
        for j in range(i + 1, min(i + 1 + EXPIRY_BARS + 32, n_m15)):
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
                                outcome = 'loss'
                                last_j = jj
                                break
                            if bb['h'] >= target:
                                outcome = 'win'
                                last_j = jj
                                break
                        else:
                            if bb['h'] >= stop:
                                outcome = 'loss'
                                last_j = jj
                                break
                            if bb['l'] <= target:
                                outcome = 'win'
                                last_j = jj
                                break
                    break
            if j - i > EXPIRY_BARS:
                outcome = 'expired'
                last_j = j
                break
        last_resolved = last_j
        setups.append({'outcome': outcome, 'used_auto_ew': used_auto})

    return setups, auto_ew_setups


def aggregate(setups):
    wins = losses = expired = 0
    used_auto = 0
    for s in setups:
        if s['outcome'] == 'win':
            wins += 1
        elif s['outcome'] == 'loss':
            losses += 1
        elif s['outcome'] == 'expired':
            expired += 1
        if s['used_auto_ew']:
            used_auto += 1
    decided = wins + losses
    return {
        'n': wins + losses + expired,
        'wins': wins, 'losses': losses, 'expired': expired,
        'used_auto_ew': used_auto,
        'wr': (wins / decided * 100) if decided else None,
    }


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    # Pass 1: precompute auto-EW per pair (shared across thresholds)
    print('Precomputing auto-EW per pair (this is the heavy part — '
          'one auto_detect_ew call per daily bar)...', file=sys.stderr)
    pair_data_with_cache = {}
    t0 = time.time()
    for k in sorted(pairs.keys()):
        if k in DROPPED or k not in PAIR_CLASS:
            continue
        daily = _bars_norm(pairs[k].get('daily', []))
        if len(daily) < 35:
            continue
        ae = precompute_auto_ew(daily)
        # Inject the cache into the pair dict for find_setups_at_threshold
        pd = dict(pairs[k])
        pd['_cached_auto_ew'] = ae
        pair_data_with_cache[k] = pd
        n_with_pattern = sum(1 for x in ae if x is not None)
        print(f'  {k:<10} daily_bars={len(daily)}  with_pattern={n_with_pattern}',
              file=sys.stderr)
    print(f'Precompute took {time.time() - t0:.1f}s', file=sys.stderr)

    # Pass 2: sweep thresholds
    results = {}  # results[threshold][pair] = aggregated
    for T in THRESHOLDS:
        print(f'\n=== AUTO_EW_MIN_CONFIDENCE = {T:.2f} ===', file=sys.stderr)
        results[T] = {}
        for k, pd in pair_data_with_cache.items():
            setups, _ = find_setups_at_threshold(pd, T, k)
            if setups is None:
                continue
            results[T][k] = aggregate(setups)

    # Aggregate per class
    print()
    print('=' * 90)
    print('AUTO-EW CONFIDENCE THRESHOLD SWEEP (per asset class)')
    print('=' * 90)
    print('Reads RSI gate from per-class deployed config (RULES_VERSION 2026-06-10a).')
    print()
    header = f'{"class":<8} {"n_pairs":>7}  '
    for T in THRESHOLDS:
        marker = ' (now)' if abs(T - 0.70) < 1e-9 else ''
        header += f'{T:.2f}{marker:<6}  '
    print(header)
    print('-' * 90)
    by_class = {}
    for cls in ('major', 'minor', 'comm', 'index', 'crypto'):
        n_pairs = sum(1 for k in pair_data_with_cache if PAIR_CLASS.get(k) == cls)
        if n_pairs == 0:
            continue
        by_class[cls] = {}
        row = f'{cls:<8} {n_pairs:>7}  '
        for T in THRESHOLDS:
            agg = {'wins': 0, 'losses': 0, 'expired': 0, 'used_auto_ew': 0, 'n': 0}
            for k, r in results[T].items():
                if PAIR_CLASS.get(k) != cls:
                    continue
                for fld in ('wins', 'losses', 'expired', 'used_auto_ew', 'n'):
                    agg[fld] += r[fld]
            decided = agg['wins'] + agg['losses']
            wr = (agg['wins'] / decided * 100) if decided else None
            by_class[cls][T] = {'wr': wr, 'decided': decided, **agg}
            row += f'{wr:>5.1f}% ({decided:>3})  ' if wr is not None else f'{"—":>12}  '
        print(row)
    # Decided trade counts on a separate line for clarity
    print()
    print('Aggregate (all classes):')
    print(f'{"":<8} {"":<7}  ' + '  '.join([f'{"WR (n_dec)":>12}' for _ in THRESHOLDS]))
    agg_all = {}
    for T in THRESHOLDS:
        agg = {'wins': 0, 'losses': 0, 'used_auto_ew': 0, 'n': 0}
        for k, r in results[T].items():
            agg['wins'] += r['wins']
            agg['losses'] += r['losses']
            agg['used_auto_ew'] += r['used_auto_ew']
            agg['n'] += r['n']
        decided = agg['wins'] + agg['losses']
        wr = (agg['wins'] / decided * 100) if decided else None
        agg_all[T] = {'wr': wr, 'decided': decided, **agg}
    row = f'{"TOTAL":<8} {"":<7}  '
    for T in THRESHOLDS:
        r = agg_all[T]
        row += f'{r["wr"]:>5.1f}% ({r["decided"]:>3})  ' if r['wr'] is not None else f'{"—":>12}  '
    print(row)

    print()
    print('AUTO-EW USAGE (how many setups had auto-EW carry the EW direction)')
    print('Lower = more setups relying on structural fallback')
    print(f'{"":<8} {"":<7}  ' + '  '.join([f'{"used_auto":>12}' for _ in THRESHOLDS]))
    row = f'{"TOTAL":<8} {"":<7}  '
    for T in THRESHOLDS:
        r = agg_all[T]
        pct = r['used_auto_ew'] / r['n'] * 100 if r['n'] else 0
        row += f'{r["used_auto_ew"]:>4} ({pct:>4.1f}%)  '
    print(row)

    print()
    print('=' * 90)
    print('DELTA vs current production (AUTO_EW_MIN_CONFIDENCE = 0.70)')
    print('=' * 90)
    print(f'{"class":<8} {"  baseline (0.70)":<22} {"  0.75":<14} {"  0.80":<14} {"  0.85":<14}')
    print('-' * 80)
    for cls in by_class:
        baseline = by_class[cls].get(0.70)
        if not baseline or baseline['wr'] is None:
            continue
        cells = []
        cells.append(f'{baseline["wr"]:5.1f}% (n={baseline["decided"]})')
        for T in (0.75, 0.80, 0.85):
            r = by_class[cls].get(T)
            if not r or r['wr'] is None:
                cells.append('—')
                continue
            d_wr = r['wr'] - baseline['wr']
            d_n = r['decided'] - baseline['decided']
            cells.append(f'{d_wr:+5.1f}pp (Δn={d_n:+d})')
        print(f'{cls:<8} {cells[0]:<22} {cells[1]:<14} {cells[2]:<14} {cells[3]:<14}')

    # Aggregate delta
    b = agg_all[0.70]
    print()
    print('Aggregate impact (all classes combined):')
    for T in (0.75, 0.80, 0.85):
        r = agg_all[T]
        if r['wr'] is None or b['wr'] is None:
            continue
        d_wr = r['wr'] - b['wr']
        d_n = r['decided'] - b['decided']
        pct_n = (d_n / b['decided'] * 100) if b['decided'] else 0
        print(f'  {T:.2f}: WR {b["wr"]:.2f}% → {r["wr"]:.2f}% ({d_wr:+.2f}pp) · '
              f'decided trades {b["decided"]} → {r["decided"]} ({pct_n:+.1f}%)')


if __name__ == '__main__':
    main()
