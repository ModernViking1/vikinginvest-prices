"""School Run + 4/4 integration backtest for DE40 and DJ30.

Four variants tested on the same 12-month historical-ohlc.json sample
with the deployed config (per-class RSI 70/30 for indices, FIB 38%
half-size entry methodology per the 10j revert):

  V0 — Baseline                  current 4/4 + RSI 70/30 + fib half-size
  V1 — SR-aligned filter         only 4/4 setups where direction matches
                                 the reference candle's break direction
                                 AND setup fires within 2h of ref close
  V2 — Anti-SR filter             only fire when ref range was broken
                                 then re-entered (failed breakout) AND
                                 setup direction = opposite of broken side
  V3 — SR booster tagging        all 4/4 setups still fire; sub-tag
                                 the SR-aligned cohort as "5/5" to see
                                 if that subset shows WR uplift

Reference candle: the FIRST 15-min bar of the cash session.

  DE40: bar opening 07:00 UTC (CEST summer) or 08:00 UTC (CET winter)
        = DAX 09:00 CET Frankfurt open
  DJ30: bar opening 13:30 UTC (EDT summer) or 14:30 UTC (EST winter)
        = NYSE 09:30 ET open

DST handled by accepting both UTC offsets per pair.

Decision rule (locked from the 10j post-mortem):
  Deploy a variant only if WR uplift over V0 baseline >= +5pp AND
  n_decided >= 30 on that variant's cohort.
"""
import json
import sys
import time

from detect_triggers import (
    RSI_GATE_BY_CLASS, PAIR_CLASS,
)
from backtest_rsi_per_class import (
    _bars_norm, _min_prom,
    precompute_break_dirs, precompute_cl_dir, precompute_rsi,
    _find_struct_high, _find_struct_low,
)

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

NW_LB = 5
TL_LB = 8
EW_LB = 8
BOS_LB = 24
EXPIRY = 8

# Per-pair reference candle definitions. Accept both DST offsets so a
# 12-month window spanning the spring/autumn transitions is handled
# without explicit DST math.
PAIR_REF_CANDLE = {
    'de40': {
        'open_times': ('07:00', '08:00'),  # 09:00 CET / CEST
        'session_label': 'DAX 09:00 CET',
    },
    'dj30': {
        'open_times': ('13:30', '14:30'),  # 09:30 ET / EDT
        'session_label': 'DJ30 09:30 ET',
    },
}

WINDOW_BARS = 8  # 8 × 15m = 2 hours after ref candle close (default).
                 # main() sweeps {8, 16, 24} = {2h, 4h, 6h} for comparison
                 # so we can see whether widening preserves the SR signal.


def find_reference_candles(m15, pair):
    """Return list of {idx, ref_high, ref_low, date, open_time} for every
    bar that matches a reference-candle time for this pair."""
    valid = PAIR_REF_CANDLE[pair]['open_times']
    refs = []
    for i, b in enumerate(m15):
        ts = b.get('t', '')
        if len(ts) < 16:
            continue
        if ts[11:16] in valid:
            refs.append({
                'idx': i,
                'ref_high': b['h'],
                'ref_low': b['l'],
                'date': ts[:10],
                'open_time': ts[11:16],
            })
    return refs


def get_active_ref(setup_idx, ref_bars, window_bars=WINDOW_BARS):
    """Most-recent ref candle whose 2-hour window contains setup_idx.
    Returns the ref dict or None if outside any window."""
    # Reversed walk — most recent ref is most likely match. Linear is
    # fine because n_refs ≤ ~365 for a 12-month sample.
    for ref in reversed(ref_bars):
        # In-window: setup must be AFTER the ref (>=) and within
        # window_bars of it (<= ref_idx + window_bars).
        if ref['idx'] < setup_idx <= ref['idx'] + window_bars:
            return ref
        # Refs are sorted ascending by idx; if we've gone past, stop.
        if ref['idx'] + window_bars < setup_idx:
            break
    return None


def compute_sr_state(m15, ref, current_idx):
    """SR state at current_idx for the given ref candle.
    Walks bars (ref.idx+1 .. current_idx) and applies the state machine."""
    state = 'pending'
    ref_hi, ref_lo = ref['ref_high'], ref['ref_low']
    for j in range(ref['idx'] + 1, current_idx + 1):
        if j >= len(m15):
            break
        c = m15[j].get('c')
        if c is None:
            continue
        if state == 'pending':
            if c > ref_hi:
                state = 'bull_broken'
            elif c < ref_lo:
                state = 'bear_broken'
        elif state == 'bull_broken':
            # Failed break: closed back inside the range
            if c <= ref_hi:
                state = 'bull_failed'
        elif state == 'bear_broken':
            if c >= ref_lo:
                state = 'bear_failed'
        # 'bull_failed' and 'bear_failed' are terminal for our purposes
    return state


def find_setups_with_sr(pair_data, pair_key, window_bars=WINDOW_BARS):
    """Walk 4/4 engine + tag each setup with SR context. Tracks BOTH
    fib outcomes (production methodology for indices) and wick outcomes
    (for reference)."""
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None
    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]

    ew_arr = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    ref_bars = find_reference_candles(m15, pair_key)

    def find_h1_idx(ts):
        lo, hi = 0, len(h1_ts) - 1; ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if h1_ts[mid] <= ts: ans = mid; lo = mid + 1
            else: hi = mid - 1
        return ans

    def find_d_idx(ts):
        lo, hi = 0, len(daily_ts) - 1; ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if daily_ts[mid] <= ts: ans = mid; lo = mid + 1
            else: hi = mid - 1
        return ans

    gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pair_key), {'hi': 80, 'lo': 20})
    setups = []
    last_resolved = -1
    n_m15 = len(m15)

    for i in range(40, n_m15 - 1):
        if i <= last_resolved: continue
        cur_ts = m15[i]['_ts']
        h1_idx = find_h1_idx(cur_ts); d_idx = find_d_idx(cur_ts)
        if h1_idx < TL_LB or d_idx < EW_LB: continue
        ew = ew_arr[d_idx]; tl = tl_arr[h1_idx]; nw = nw_arr[i]; cl = cl_arr[h1_idx]
        if not ew or not tl or not nw or not cl: continue
        if ew != tl or tl != nw or nw != cl: continue
        if ew not in ('bull', 'bear'): continue
        lb_slc = m15[max(0, i - 8):i]
        if len(lb_slc) < 5: continue
        sw_hi = max(b['h'] for b in lb_slc); sw_lo = min(b['l'] for b in lb_slc)
        mp = _min_prom(m15[i]['c'])
        if ew == 'bull':
            if not (m15[i]['c'] > sw_hi and (m15[i]['c'] - sw_hi) >= mp): continue
        else:
            if not (m15[i]['c'] < sw_lo and (sw_lo - m15[i]['c']) >= mp): continue
        bos_slc = m15[max(0, i - BOS_LB):i]; prom = _min_prom(m15[i]['c'])
        if ew == 'bear':
            entry = m15[i]['h']; stop = _find_struct_high(bos_slc, prom)
            if stop <= entry: continue
            R = stop - entry; target = entry - R
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos_slc, prom)
            if stop >= entry: continue
            R = entry - stop; target = entry + R
        atr_slc = m15[max(0, i - 20):i]
        if len(atr_slc) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr_slc) / len(atr_slc)
            if R < 0.5 * atr20: continue
        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if ew == 'bull' and rsi_at >= gate['hi']: continue
            if ew == 'bear' and rsi_at <= gate['lo']: continue

        # Fib 38% entry off creator
        ch, cl_bar = m15[i]['h'], m15[i]['l']
        crange = max(ch - cl_bar, 1e-9)
        if ew == 'bear':
            fib_entry = cl_bar + crange * 0.382
            fib_R = stop - fib_entry
            fib_target = fib_entry - fib_R if fib_R > 0 else None
        else:
            fib_entry = ch - crange * 0.382
            fib_R = fib_entry - stop
            fib_target = fib_entry + fib_R if fib_R > 0 else None

        # Walk forward — track fib path (production for indices)
        lift_done = False; fib_t = False; fib_o = None; last_j = i
        for j in range(i + 1, min(i + 1 + EXPIRY + 32, n_m15)):
            b = m15[j]
            if not lift_done:
                if ew == 'bull' and b['h'] >= m15[i]['h']: lift_done = True
                elif ew == 'bear' and b['l'] <= m15[i]['l']: lift_done = True
            if lift_done and not fib_t and fib_target is not None:
                reach = (ew == 'bear' and b['h'] >= fib_entry) or (ew == 'bull' and b['l'] <= fib_entry)
                if reach:
                    fib_t = True
                    for jj in range(j, min(j + 32, n_m15)):
                        bb = m15[jj]
                        if ew == 'bull':
                            if bb['l'] <= stop: fib_o = 'loss'; last_j = jj; break
                            if bb['h'] >= fib_target: fib_o = 'win'; last_j = jj; break
                        else:
                            if bb['h'] >= stop: fib_o = 'loss'; last_j = jj; break
                            if bb['l'] <= fib_target: fib_o = 'win'; last_j = jj; break
                    break
            if j - i > EXPIRY:
                if not fib_t: fib_o = 'expired'; last_j = j; break
        last_resolved = last_j

        # SR context for this setup
        active_ref = get_active_ref(i, ref_bars, window_bars=window_bars)
        sr_state = None
        sr_aligned = False
        sr_anti_aligned = False
        if active_ref is not None:
            sr_state = compute_sr_state(m15, active_ref, i)
            # Aligned: setup direction matches the SR break direction
            if (sr_state == 'bull_broken' and ew == 'bull') or \
               (sr_state == 'bear_broken' and ew == 'bear'):
                sr_aligned = True
            # Anti-aligned: setup direction = opposite of failed break
            if (sr_state == 'bull_failed' and ew == 'bear') or \
               (sr_state == 'bear_failed' and ew == 'bull'):
                sr_anti_aligned = True

        setups.append({
            'idx': i,
            'time': m15[i].get('t', ''),
            'dir': ew,
            'in_window': active_ref is not None,
            'sr_state': sr_state,
            'sr_aligned': sr_aligned,
            'sr_anti_aligned': sr_anti_aligned,
            'fib_outcome': fib_o,
        })

    return setups, ref_bars


def tally(setups):
    w = sum(1 for s in setups if s['fib_outcome'] == 'win')
    l = sum(1 for s in setups if s['fib_outcome'] == 'loss')
    e = sum(1 for s in setups if s['fib_outcome'] == 'expired')
    decided = w + l
    return {'n': len(setups), 'w': w, 'l': l, 'e': e,
            'decided': decided, 'wr': (w / decided * 100) if decided else None}


def run_variants(setups, label, v0_baseline_wr=None):
    """Tally V0/V1/V2/V3a/V3b for a given setup list. Returns dict."""
    v0 = tally(setups)
    v1 = tally([s for s in setups if s['in_window'] and s['sr_aligned']])
    v2 = tally([s for s in setups if s['in_window'] and s['sr_anti_aligned']])
    v3a = tally([s for s in setups if not (s['in_window'] and s['sr_aligned'])])
    v3b = tally([s for s in setups if s['in_window'] and s['sr_aligned']])
    return {'V0': v0, 'V1': v1, 'V2': v2, 'V3a': v3a, 'V3b': v3b, 'label': label}


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    # Path 2 window sweep — re-run the backtest at 3 window widths to see
    # whether widening preserves the SR signal AND gets us closer to the
    # n_decided >= 30 deploy threshold.
    WINDOWS = [(8, '2h'), (16, '4h'), (24, '6h')]

    print(f'Running School Run sweep on {", ".join(f"{w[1]}" for w in WINDOWS)} windows...',
          file=sys.stderr)
    t0 = time.time()

    # All-windows-by-pair: {pair: {window_bars: setups}}
    all_runs = {}
    for pair in ['de40', 'dj30']:
        p = pairs.get(pair)
        if not p:
            print(f'  {pair}: no data', file=sys.stderr); continue
        all_runs[pair] = {}
        for w_bars, w_lbl in WINDOWS:
            setups, ref_bars = find_setups_with_sr(p, pair, window_bars=w_bars)
            if setups is None: continue
            all_runs[pair][w_bars] = {'setups': setups, 'ref_bars': ref_bars, 'label': w_lbl}
            v1 = tally([s for s in setups if s['in_window'] and s['sr_aligned']])
            print(f'  {pair} {w_lbl}: SR-aligned cohort n_set={v1["n"]} n_dec={v1["decided"]} '
                  f'W={v1["w"]} L={v1["l"]}', file=sys.stderr)
    print(f'Sweep completed in {time.time() - t0:.1f}s', file=sys.stderr)

    # ─── Sanity check (ref candles — invariant across windows, show once) ───
    print()
    print('=' * 100)
    print('REFERENCE-CANDLE SANITY CHECK (first 4 detected per pair)')
    print('=' * 100)
    for pair, runs in all_runs.items():
        baseline_run = runs.get(WINDOWS[0][0], {})
        ref_bars = baseline_run.get('ref_bars', [])
        label = PAIR_REF_CANDLE[pair]['session_label']
        print(f'\n{pair.upper()} — {label}')
        for ref in ref_bars[:4]:
            print(f'  {ref["date"]} {ref["open_time"]}Z  ref_high={ref["ref_high"]:.2f}  '
                  f'ref_low={ref["ref_low"]:.2f}')
        print(f'  ... total ref candles in sample: {len(ref_bars)}')

    # ─── Per-pair × per-window table ───
    print()
    print('=' * 105)
    print('WINDOW-WIDTH SWEEP — does widening preserve the V1 SR-aligned edge AND lift n above 30?')
    print('=' * 105)
    print(f'{"pair":<5} {"window":<7} {"variant":<22} {"n_set":>5} {"n_dec":>5} {"W":>3} {"L":>3} {"E":>3}  {"WR":>7}  {"vs V0":>9}')
    print('-' * 105)

    deploy_per_pair = []
    for pair, runs in all_runs.items():
        for w_bars, w_lbl in WINDOWS:
            run = runs.get(w_bars)
            if not run: continue
            r = run_variants(run['setups'], w_lbl)
            v0_wr = r['V0']['wr']
            for vk, vlabel in [
                ('V0', 'V0 baseline'),
                ('V1', 'V1 SR-aligned filter'),
                ('V2', 'V2 Anti-SR filter'),
                ('V3a', 'V3a 4/4 (non-SR)'),
                ('V3b', 'V3b 5/5 (SR-aligned)'),
            ]:
                t = r[vk]
                wr_s = f'{t["wr"]:.1f}%' if t['wr'] is not None else '—'
                delta_s = '—'
                if t['wr'] is not None and v0_wr is not None and vk != 'V0':
                    delta_s = f'{t["wr"] - v0_wr:+.2f}pp'
                print(f'{pair:<5} {w_lbl:<7} {vlabel:<22} {t["n"]:>5} {t["decided"]:>5} '
                      f'{t["w"]:>3} {t["l"]:>3} {t["e"]:>3}  {wr_s:>7}  {delta_s:>9}')
                if vk in ('V1', 'V3b') and t['wr'] is not None and v0_wr is not None:
                    delta = t['wr'] - v0_wr
                    if delta >= 5 and t['decided'] >= 30:
                        deploy_per_pair.append((pair, w_lbl, vk, t, delta))
            print()

    # ─── Aggregate across both pairs, per window ───
    print('=' * 105)
    print('AGGREGATE (DE40 + DJ30 combined) — per window')
    print('=' * 105)
    print(f'{"window":<7} {"variant":<22} {"n_set":>5} {"n_dec":>5} {"W":>3} {"L":>3}  {"WR":>7}  {"vs V0":>9}')
    print('-' * 105)

    deploy_agg = []
    for w_bars, w_lbl in WINDOWS:
        merged_setups = []
        for pair in all_runs:
            run = all_runs[pair].get(w_bars)
            if not run: continue
            merged_setups.extend(run['setups'])
        if not merged_setups: continue
        r = run_variants(merged_setups, w_lbl)
        v0_wr = r['V0']['wr']
        for vk, vlabel in [
            ('V0', 'V0 baseline'),
            ('V1', 'V1 SR-aligned filter'),
            ('V2', 'V2 Anti-SR filter'),
            ('V3a', 'V3a 4/4 (non-SR)'),
            ('V3b', 'V3b 5/5 (SR-aligned)'),
        ]:
            t = r[vk]
            wr_s = f'{t["wr"]:.1f}%' if t['wr'] is not None else '—'
            delta_s = '—'
            if t['wr'] is not None and v0_wr is not None and vk != 'V0':
                delta_s = f'{t["wr"] - v0_wr:+.2f}pp'
            print(f'{w_lbl:<7} {vlabel:<22} {t["n"]:>5} {t["decided"]:>5} '
                  f'{t["w"]:>3} {t["l"]:>3}  {wr_s:>7}  {delta_s:>9}')
            if vk in ('V1', 'V3b') and t['wr'] is not None and v0_wr is not None:
                delta = t['wr'] - v0_wr
                if delta >= 5 and t['decided'] >= 30:
                    deploy_agg.append((w_lbl, vk, t, delta))
        print()

    # ─── Decision summary ───
    print('=' * 105)
    print('DEPLOY DECISION — rule: WR uplift >= +5pp AND n_decided >= 30')
    print('=' * 105)
    if deploy_per_pair:
        print('CANDIDATES PASSING ON A SINGLE-PAIR BASIS:')
        for pair, w_lbl, vk, t, delta in deploy_per_pair:
            print(f'  {pair} · {w_lbl} window · {vk}: WR={t["wr"]:.1f}% '
                  f'(n_dec={t["decided"]}) — {delta:+.1f}pp vs V0')
    else:
        print('No single-pair candidates pass.')
    print()
    if deploy_agg:
        print('CANDIDATES PASSING ON AGGREGATE (DE40 + DJ30 combined):')
        for w_lbl, vk, t, delta in deploy_agg:
            print(f'  {w_lbl} window · {vk}: WR={t["wr"]:.1f}% '
                  f'(n_dec={t["decided"]}) — {delta:+.1f}pp vs V0')
        print()
        print('Recommendation: deploy the widest window that passes (more data = more robust).')
    else:
        print('No aggregate candidates pass either.')
        print('Decision: stay with Path 1 (non-gating UI badge) and accumulate forward data.')


if __name__ == '__main__':
    main()
