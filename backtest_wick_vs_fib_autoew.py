"""Auto-EW-integrated wick-vs-fib backtest for indices.

The 2026-06-10i wick switch failed because backtest_rsi_per_class.py
uses STRUCTURAL EW direction only. Production prefers auto-EW (Lux-
Algo-inspired pivot-hierarchy detector) when it returns a high-
confidence pattern (conf >= AUTO_EW_MIN_CONFIDENCE = 0.70). Auto-EW
triggers MORE setups on indices than structural alone, with their own
WR distribution my simplified backtest missed.

This script repeats the wick-vs-fib comparison on indices, with
auto-EW direction properly integrated:

  ew = auto_ew_dir   if auto_ew.confidence >= 0.70 and pattern in valid
                     and auto_ew_dir in (bull, bear)
       structural    otherwise

This matches scan_pairs in detect_triggers.py and what calcRecentBacktest
uses in the dashboard's auto-ew mode. Per-pair WR is reported for both
the WICK trigger and the FIB 38% trigger, with the larger setup
population that auto-EW unlocks.

Decision rule for re-deploying a wick switch:
  - Sample size >= 30 decided per pair
  - Wick WR > Fib WR by >= 5pp (margin of safety for sample variance)
  - No class-level regression at the aggregate
"""
import json
import sys
import time

from detect_triggers import (
    auto_detect_ew, AUTO_EW_VALID_PATTERNS,
    RSI_GATE_BY_CLASS, PAIR_CLASS,
)
from backtest_rsi_per_class import (
    DROPPED,
    _bars_norm, _min_prom,
    precompute_break_dirs, precompute_cl_dir, precompute_rsi,
    _find_struct_high, _find_struct_low,
)

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

AUTO_EW_MIN_CONFIDENCE = 0.70

NW_LB = 5
TL_LB = 8
EW_LB = 8
BOS_LB = 24
EXPIRY = 8


def precompute_auto_ew_dirs(daily):
    """At every daily idx, run auto_detect_ew on daily[:i+1] and return
    {'dir': bull/bear, 'conf': float, 'pattern': str} when valid, else
    None. Expensive — ~1 call per daily bar per pair."""
    n = len(daily)
    out = [None] * n
    for i in range(30, n):
        try:
            r = auto_detect_ew(daily[:i + 1])
            if not r.get('ok'):
                continue
            ew = r.get('ew', {})
            pat = ew.get('pattern')
            d = ew.get('dir')
            conf = ew.get('confidence', 0)
            if (d in ('bull', 'bear')
                    and pat in AUTO_EW_VALID_PATTERNS):
                out[i] = {'dir': d, 'conf': conf, 'pattern': pat}
        except Exception:
            pass
    return out


def find_setups_dual(pair_data, pair_key, auto_ew_dirs):
    """Walk the 4/4 engine with auto-EW direction priority, tracking
    BOTH wick and fib outcomes for every setup so we can compare
    methodologies on the same population."""
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None
    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]

    ew_struct = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
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

        # Auto-EW direction priority (matches scan_pairs in detect_triggers.py)
        auto = auto_ew_dirs[d_idx] if d_idx < len(auto_ew_dirs) else None
        if auto and auto['conf'] >= AUTO_EW_MIN_CONFIDENCE:
            ew = auto['dir']
            ew_source = 'auto'
        else:
            ew = ew_struct[d_idx]
            ew_source = 'struct'

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

        # Fib 38% entry off the creator bar
        creator_h, creator_l = m15[i]['h'], m15[i]['l']
        creator_range = max(creator_h - creator_l, 1e-9)
        if ew == 'bear':
            fib_entry = creator_l + creator_range * 0.382
            fib_R = stop - fib_entry
            fib_target = fib_entry - fib_R if fib_R > 0 else None
        else:
            fib_entry = creator_h - creator_range * 0.382
            fib_R = fib_entry - stop
            fib_target = fib_entry + fib_R if fib_R > 0 else None

        # Forward walk — track BOTH outcomes
        lift_done = False
        wick_t = False; wick_o = None
        fib_t = False; fib_o = None
        last_j = i
        for j in range(i + 1, min(i + 1 + EXPIRY + 32, n_m15)):
            b = m15[j]
            if not lift_done:
                if ew == 'bull' and b['h'] >= m15[i]['h']:
                    lift_done = True
                elif ew == 'bear' and b['l'] <= m15[i]['l']:
                    lift_done = True
            if lift_done:
                if not fib_t and fib_target is not None:
                    if (ew == 'bear' and b['h'] >= fib_entry) or (ew == 'bull' and b['l'] <= fib_entry):
                        fib_t = True
                        for jj in range(j, min(j + 32, n_m15)):
                            bb = m15[jj]
                            if ew == 'bull':
                                if bb['l'] <= stop:
                                    fib_o = 'loss'; break
                                if bb['h'] >= fib_target:
                                    fib_o = 'win'; break
                            else:
                                if bb['h'] >= stop:
                                    fib_o = 'loss'; break
                                if bb['l'] <= fib_target:
                                    fib_o = 'win'; break
                if not wick_t:
                    reach = (ew == 'bull' and b['l'] <= entry) or (ew == 'bear' and b['h'] >= entry)
                    if reach:
                        wick_t = True
                        for jj in range(j, min(j + 32, n_m15)):
                            bb = m15[jj]
                            if ew == 'bull':
                                if bb['l'] <= stop:
                                    wick_o = 'loss'; last_j = jj; break
                                if bb['h'] >= target:
                                    wick_o = 'win'; last_j = jj; break
                            else:
                                if bb['h'] >= stop:
                                    wick_o = 'loss'; last_j = jj; break
                                if bb['l'] <= target:
                                    wick_o = 'win'; last_j = jj; break
                        break
            if j - i > EXPIRY:
                if not wick_t:
                    wick_o = 'expired'; last_j = j; break
        last_resolved = last_j
        setups.append({
            'ew_source': ew_source,
            'wick_triggered': wick_t, 'wick': wick_o,
            'fib_triggered': fib_t, 'fib': fib_o,
        })
    return setups


def tally(setups, path):
    """Tally W/L/expired for a methodology path ('wick' or 'fib')."""
    w = sum(1 for s in setups if s[path] == 'win')
    l = sum(1 for s in setups if s[path] == 'loss')
    e = sum(1 for s in setups if s[path] == 'expired')
    decided = w + l
    return {'n': len(setups), 'w': w, 'l': l, 'e': e,
            'decided': decided, 'wr': (w / decided * 100) if decided else None}


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    # Run on indices only — the cohort under question
    targets = ['de40', 'dj30', 'nas100', 'spx500', 'ftse100', 'jp225']
    print('Precomputing auto-EW per index (heavy — one auto_detect_ew per daily bar)...',
          file=sys.stderr)
    t0 = time.time()
    results = {}
    for k in targets:
        if k in DROPPED or k not in PAIR_CLASS:
            continue
        p = pairs.get(k)
        if not p:
            continue
        daily = _bars_norm(p.get('daily', []))
        if len(daily) < 35:
            continue
        ae = precompute_auto_ew_dirs(daily)
        setups = find_setups_dual(p, k, ae)
        if not setups:
            continue
        results[k] = setups
        wick = tally(setups, 'wick')
        fib = tally(setups, 'fib')
        ww_s = f'{wick["wr"]:.1f}%' if wick['wr'] is not None else '—'
        fw_s = f'{fib["wr"]:.1f}%' if fib['wr'] is not None else '—'
        print(f'  {k:<8} n_setups={len(setups):>3}  wick decided={wick["decided"]:>3} WR={ww_s:>6}  '
              f'fib decided={fib["decided"]:>3} WR={fw_s:>6}', file=sys.stderr)
    print(f'Precompute + walk took {time.time() - t0:.1f}s', file=sys.stderr)

    print()
    print('=' * 110)
    print('AUTO-EW-INTEGRATED INDEX METHODOLOGY COMPARISON')
    print('=' * 110)
    print(f'EW priority: auto-EW (conf >= {AUTO_EW_MIN_CONFIDENCE}) > structural — matches scan_pairs in detect_triggers.py')
    print()
    print(f'{"pair":<8}  {"n":>4}  {"%auto":>6}  '
          f'{"wick_trig":>9} {"wick_W/L":>10} {"wick_WR":>8}  '
          f'{"fib_trig":>9} {"fib_W/L":>10} {"fib_WR":>8}  {"delta":>8}')
    print('-' * 110)

    agg_wick = {'w': 0, 'l': 0, 'd': 0}
    agg_fib = {'w': 0, 'l': 0, 'd': 0}
    for k in targets:
        if k not in results:
            continue
        setups = results[k]
        n = len(setups)
        auto_pct = sum(1 for s in setups if s['ew_source'] == 'auto') / n * 100 if n else 0
        wick = tally(setups, 'wick')
        fib = tally(setups, 'fib')
        ww_s = f'{wick["wr"]:.1f}%' if wick['wr'] is not None else '—'
        fw_s = f'{fib["wr"]:.1f}%' if fib['wr'] is not None else '—'
        delta_s = '—'
        if wick['wr'] is not None and fib['wr'] is not None:
            delta_s = f'{wick["wr"] - fib["wr"]:+.1f}pp'
        wick_t = sum(1 for s in setups if s['wick_triggered'])
        fib_t = sum(1 for s in setups if s['fib_triggered'])
        print(f'{k:<8}  {n:>4}  {auto_pct:>5.0f}%  '
              f'{wick_t:>9}  {wick["w"]:>4}W/{wick["l"]:>3}L {ww_s:>8}  '
              f'{fib_t:>9}  {fib["w"]:>4}W/{fib["l"]:>3}L {fw_s:>8}  {delta_s:>8}')
        agg_wick['w'] += wick['w']; agg_wick['l'] += wick['l']; agg_wick['d'] += wick['decided']
        agg_fib['w'] += fib['w']; agg_fib['l'] += fib['l']; agg_fib['d'] += fib['decided']

    print('-' * 110)
    a_wick_wr = agg_wick['w'] / (agg_wick['w'] + agg_wick['l']) * 100 if (agg_wick['w'] + agg_wick['l']) else 0
    a_fib_wr = agg_fib['w'] / (agg_fib['w'] + agg_fib['l']) * 100 if (agg_fib['w'] + agg_fib['l']) else 0
    print(f'{"INDICES":<8}  {"":<4}  {"":<6}  '
          f'{"":<9}  {agg_wick["w"]:>4}W/{agg_wick["l"]:>3}L {a_wick_wr:>7.1f}%  '
          f'{"":<9}  {agg_fib["w"]:>4}W/{agg_fib["l"]:>3}L {a_fib_wr:>7.1f}%  '
          f'{a_wick_wr - a_fib_wr:+7.1f}pp')

    print()
    print('Decision rule: re-deploy wick on a pair only if n_decided >= 30 AND delta >= +5pp.')
    print()
    candidates = []
    for k in targets:
        if k not in results:
            continue
        wick = tally(results[k], 'wick')
        fib = tally(results[k], 'fib')
        if wick['decided'] < 30:
            continue
        if wick['wr'] is None or fib['wr'] is None:
            continue
        delta = wick['wr'] - fib['wr']
        if delta >= 5:
            candidates.append((k, wick, fib, delta))
    if candidates:
        print('CANDIDATES FOR WICK METHODOLOGY:')
        for k, w, f, d in candidates:
            print(f'  {k}: wick {w["wr"]:.1f}% (n={w["decided"]}) vs fib {f["wr"]:.1f}% (n={f["decided"]}) — delta {d:+.1f}pp')
    else:
        print('NO candidates passed the decision rule. Fib stays as production for all indices.')


if __name__ == '__main__':
    main()
