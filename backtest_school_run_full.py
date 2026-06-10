"""Full School Run + Anti School Run backtest at all tier levels.

Extends backtest_school_run.py to relax the 4/4 confluence gate and
explore the full tier grid. Each setup is classified by:

  confluence ∈ {4, 3, 2}    based on 4-layer (EW/TL/NW/CL) agreement
  sr_state ∈ {pending, bull_broken, bear_broken, bull_failed, bear_failed}
  variant ∈ {SR_ALIGNED, ASR_ALIGNED, NONE}

Tier label combines:
  4/4 + SR_ALIGNED   → 5/5
  3/4 + SR_ALIGNED   → 4/5
  2/4 + SR_ALIGNED   → 3/5
  4/4 + ASR_ALIGNED  → ASR-5/5  (fade)
  3/4 + ASR_ALIGNED  → ASR-4/5
  2/4 + ASR_ALIGNED  → ASR-3/5

Methodology stays at fib 38% half-size for indices (production rule
restored in 10j after the wick switch reverted). RSI gate per-class
(70/30 for index class). Decision rule for deploying a tier as a
live filter: n_decided >= 30 AND WR uplift >= +5pp vs baseline
(2/4-no-SR cohort).
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
from backtest_school_run import (
    PAIR_REF_CANDLE, find_reference_candles, get_active_ref, compute_sr_state,
)

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

NW_LB = 5
TL_LB = 8
EW_LB = 8
BOS_LB = 24
EXPIRY = 8
WINDOW_BARS = 8  # 2h post-ref-candle window


def classify_setup(ew, tl, nw, cl):
    """Return (confluence_score, direction) — same logic as sc(k) in
    the dashboard. tot = max of bull_count / bear_count across the 4
    layers; direction = whichever count is higher (None on ties)."""
    layers = [ew, tl, nw, cl]
    bull = sum(1 for v in layers if v == 'bull')
    bear = sum(1 for v in layers if v == 'bear')
    tot = max(bull, bear)
    if bull > bear:
        return tot, 'bull'
    if bear > bull:
        return tot, 'bear'
    return tot, None  # tied — not tradeable


def find_all_setups(pair_data, pair_key):
    """Walk every m15 bar and find creators at confluence levels >= 2.
    Returns list of setup dicts tagged with confluence + SR context."""
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
        if ew is None or tl is None or nw is None or cl is None: continue

        confluence, direction = classify_setup(ew, tl, nw, cl)
        if confluence < 2 or direction is None: continue

        # Creator check (broke prior 8-bar swing in `direction`)
        lb_slc = m15[max(0, i - 8):i]
        if len(lb_slc) < 5: continue
        sw_hi = max(b['h'] for b in lb_slc); sw_lo = min(b['l'] for b in lb_slc)
        mp = _min_prom(m15[i]['c'])
        if direction == 'bull':
            if not (m15[i]['c'] > sw_hi and (m15[i]['c'] - sw_hi) >= mp): continue
        else:
            if not (m15[i]['c'] < sw_lo and (sw_lo - m15[i]['c']) >= mp): continue

        # Setup levels
        bos_slc = m15[max(0, i - BOS_LB):i]; prom = _min_prom(m15[i]['c'])
        if direction == 'bear':
            entry = m15[i]['h']; stop = _find_struct_high(bos_slc, prom)
            if stop <= entry: continue
            R = stop - entry; target = entry - R
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos_slc, prom)
            if stop >= entry: continue
            R = entry - stop; target = entry + R

        # Min-R floor (matches 2026-06-10n hybrid: ATR + FX abs)
        atr_slc = m15[max(0, i - 20):i]
        if len(atr_slc) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr_slc) / len(atr_slc)
            atr_floor = 0.5 * atr20
            cls = PAIR_CLASS.get(pair_key)
            fx_floor = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if R < max(atr_floor, fx_floor): continue

        # RSI gate
        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if direction == 'bull' and rsi_at >= gate['hi']: continue
            if direction == 'bear' and rsi_at <= gate['lo']: continue

        # Fib 38% entry (indices use fib half-size as production methodology)
        ch, cl_bar = m15[i]['h'], m15[i]['l']
        crange = max(ch - cl_bar, 1e-9)
        if direction == 'bear':
            fib_entry = cl_bar + crange * 0.382
            fib_R = stop - fib_entry
            fib_target = fib_entry - fib_R if fib_R > 0 else None
        else:
            fib_entry = ch - crange * 0.382
            fib_R = fib_entry - stop
            fib_target = fib_entry + fib_R if fib_R > 0 else None

        # Forward walk for outcome (fib path — production for indices)
        lift_done = False; fib_t = False; fib_o = None; last_j = i
        for j in range(i + 1, min(i + 1 + EXPIRY + 32, n_m15)):
            b = m15[j]
            if not lift_done:
                if direction == 'bull' and b['h'] >= m15[i]['h']: lift_done = True
                elif direction == 'bear' and b['l'] <= m15[i]['l']: lift_done = True
            if lift_done and not fib_t and fib_target is not None:
                reach = ((direction == 'bear' and b['h'] >= fib_entry)
                         or (direction == 'bull' and b['l'] <= fib_entry))
                if reach:
                    fib_t = True
                    for jj in range(j, min(j + 32, n_m15)):
                        bb = m15[jj]
                        if direction == 'bull':
                            if bb['l'] <= stop: fib_o = 'loss'; last_j = jj; break
                            if bb['h'] >= fib_target: fib_o = 'win'; last_j = jj; break
                        else:
                            if bb['h'] >= stop: fib_o = 'loss'; last_j = jj; break
                            if bb['l'] <= fib_target: fib_o = 'win'; last_j = jj; break
                    break
            if j - i > EXPIRY:
                if not fib_t: fib_o = 'expired'; last_j = j; break
        last_resolved = last_j

        # SR context
        active_ref = get_active_ref(i, ref_bars, window_bars=WINDOW_BARS)
        sr_state = None
        sr_aligned = False
        asr_aligned = False
        if active_ref is not None:
            sr_state = compute_sr_state(m15, active_ref, i)
            # SR-aligned: setup dir matches the break dir
            if (sr_state == 'bull_broken' and direction == 'bull') or \
               (sr_state == 'bear_broken' and direction == 'bear'):
                sr_aligned = True
            # ASR-aligned: setup dir = OPPOSITE of the FAILED break
            if (sr_state == 'bull_failed' and direction == 'bear') or \
               (sr_state == 'bear_failed' and direction == 'bull'):
                asr_aligned = True

        setups.append({
            'confluence': confluence,
            'direction': direction,
            'sr_state': sr_state,
            'sr_aligned': sr_aligned,
            'asr_aligned': asr_aligned,
            'in_window': active_ref is not None,
            'outcome': fib_o,
        })
    return setups


def tier_label(confluence, variant):
    """Map (confluence, variant) → tier string."""
    if variant == 'SR':
        return {4: '5/5', 3: '4/5', 2: '3/5'}.get(confluence)
    if variant == 'ASR':
        return {4: 'ASR-5/5', 3: 'ASR-4/5', 2: 'ASR-3/5'}.get(confluence)
    return None


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

    print('Walking full School Run / Anti-School-Run tier grid for DE40 + DJ30...',
          file=sys.stderr)
    t0 = time.time()
    all_setups = {}
    for pair in ['de40', 'dj30']:
        p = pairs.get(pair)
        if not p:
            print(f'  {pair}: no data', file=sys.stderr); continue
        setups = find_all_setups(p, pair)
        if not setups:
            print(f'  {pair}: insufficient data', file=sys.stderr); continue
        all_setups[pair] = setups
        # Quick per-pair tier counts
        print(f'  {pair}: {len(setups)} total setups across all confluence levels',
              file=sys.stderr)
    print(f'Walk completed in {time.time() - t0:.1f}s', file=sys.stderr)

    # ─── Per-pair tier table ───
    print()
    print('=' * 105)
    print('SCHOOL RUN + ANTI SCHOOL RUN — full tier grid (per pair)')
    print('=' * 105)
    print('Methodology: fib 38% half-size (indices). RSI 70/30. Hybrid min-R floor.')
    print('Decision rule for deploying as live gate: n_decided >= 30 AND WR uplift >= +5pp vs baseline.')
    print()
    print(f'{"pair":<5}  {"tier":<10}  {"n_set":>5} {"n_dec":>5} {"W":>3} {"L":>3} {"E":>3}  {"WR":>7}  {"vs base":>9}')
    print('-' * 95)

    deploy_candidates = []
    for pair in ('de40', 'dj30'):
        if pair not in all_setups: continue
        setups = all_setups[pair]
        # Baseline: ALL setups at this pair regardless of SR alignment
        baseline_setups = setups
        base = tally(baseline_setups)
        base_wr = base['wr']
        print(f'{pair:<5}  {"baseline":<10}  {base["n"]:>5} {base["decided"]:>5} '
              f'{base["w"]:>3} {base["l"]:>3} {base["e"]:>3}  '
              f'{(f"{base_wr:.1f}%" if base_wr is not None else "—"):>7}  {"—":>9}')

        # SR tiers
        for conf in (4, 3, 2):
            tier_setups = [s for s in setups if s['confluence'] == conf and s['sr_aligned']]
            t = tally(tier_setups)
            label = tier_label(conf, 'SR')
            delta = (t['wr'] - base_wr) if (t['wr'] is not None and base_wr is not None) else None
            d_s = f'{delta:+.1f}pp' if delta is not None else '—'
            wr_s = f'{t["wr"]:.1f}%' if t['wr'] is not None else '—'
            print(f'{pair:<5}  {label:<10}  {t["n"]:>5} {t["decided"]:>5} '
                  f'{t["w"]:>3} {t["l"]:>3} {t["e"]:>3}  {wr_s:>7}  {d_s:>9}')
            if delta is not None and delta >= 5 and t['decided'] >= 30:
                deploy_candidates.append((pair, label, t, delta))

        # ASR tiers
        for conf in (4, 3, 2):
            tier_setups = [s for s in setups if s['confluence'] == conf and s['asr_aligned']]
            t = tally(tier_setups)
            label = tier_label(conf, 'ASR')
            delta = (t['wr'] - base_wr) if (t['wr'] is not None and base_wr is not None) else None
            d_s = f'{delta:+.1f}pp' if delta is not None else '—'
            wr_s = f'{t["wr"]:.1f}%' if t['wr'] is not None else '—'
            print(f'{pair:<5}  {label:<10}  {t["n"]:>5} {t["decided"]:>5} '
                  f'{t["w"]:>3} {t["l"]:>3} {t["e"]:>3}  {wr_s:>7}  {d_s:>9}')
            if delta is not None and delta >= 5 and t['decided'] >= 30:
                deploy_candidates.append((pair, label, t, delta))
        print()

    # ─── Aggregate across both pairs ───
    print('=' * 105)
    print('AGGREGATE (DE40 + DJ30 combined)')
    print('=' * 105)
    print(f'{"tier":<10}  {"n_set":>5} {"n_dec":>5} {"W":>3} {"L":>3}  {"WR":>7}  {"vs base":>9}')
    print('-' * 75)

    merged = []
    for pair in all_setups:
        merged.extend(all_setups[pair])
    agg_base = tally(merged)
    agg_base_wr = agg_base['wr']
    base_s = f'{agg_base_wr:.1f}%' if agg_base_wr is not None else '—'
    print(f'{"baseline":<10}  {agg_base["n"]:>5} {agg_base["decided"]:>5} '
          f'{agg_base["w"]:>3} {agg_base["l"]:>3}  {base_s:>7}  {"—":>9}')
    for variant, name in (('SR', 'SR'), ('ASR', 'ASR')):
        for conf in (4, 3, 2):
            if variant == 'SR':
                tier_setups = [s for s in merged if s['confluence'] == conf and s['sr_aligned']]
            else:
                tier_setups = [s for s in merged if s['confluence'] == conf and s['asr_aligned']]
            t = tally(tier_setups)
            label = tier_label(conf, variant)
            delta = (t['wr'] - agg_base_wr) if (t['wr'] is not None and agg_base_wr is not None) else None
            d_s = f'{delta:+.1f}pp' if delta is not None else '—'
            wr_s = f'{t["wr"]:.1f}%' if t['wr'] is not None else '—'
            print(f'{label:<10}  {t["n"]:>5} {t["decided"]:>5} '
                  f'{t["w"]:>3} {t["l"]:>3}  {wr_s:>7}  {d_s:>9}')

    # ─── Decision ───
    print()
    print('=' * 105)
    print('DEPLOY DECISION — rule: n_decided >= 30 AND WR uplift >= +5pp vs same-pair baseline')
    print('=' * 105)
    if deploy_candidates:
        print('CANDIDATES PASSING:')
        for pair, label, t, delta in deploy_candidates:
            print(f'  {pair} · {label}: WR={t["wr"]:.1f}% (n_dec={t["decided"]}) — {delta:+.1f}pp')
    else:
        print('No tier passes the decision rule on a single-pair basis.')


if __name__ == '__main__':
    main()
