"""Per-pattern auto-EW WR breakdown — find which Elliott-wave pattern
shapes are systematically money-losing so we can drop them from
AUTO_EW_VALID_PATTERNS instead of blanket-tightening the confidence
threshold (B1 — measured at -0.14pp aggregate, abandoned).

Method:
  - For each pair, precompute auto-EW result {dir, conf, pattern} at every
    daily bar (one auto_detect_ew call per bar, cached).
  - Walk the m15 → h1 → daily 4/4 confluence engine with the deployed
    config (AUTO_EW_MIN_CONFIDENCE=0.70, per-class A1 RSI gate).
  - Whenever a setup fires, record which pattern (if any) auto-EW
    supplied. If structural EW carried the bar, the pattern is None.
  - Tally W/L/expired per pattern across the whole pair universe.
  - Report per-pattern WR alongside the structural-only WR as control.

The pattern list mirrors AUTO_EW_VALID_PATTERNS in detect_triggers.py:
  - 5-wave-impulse-complete
  - 5-wave-diagonal-complete
  - 5-wave-impulse-truncated
  - WXY-double-zigzag-complete
  - ABC-correction-complete
  - in-progress-impulse-w2

Decision rule: any pattern with WR ≥ 8pp below the structural control
AND sample size ≥ 30 decided is a drop candidate.
"""
import json
import sys
import time

from detect_triggers import (
    auto_detect_ew, AUTO_EW_VALID_PATTERNS,
    RSI_GATE_BY_CLASS,
)
from backtest_rsi_per_class import (
    PAIR_CLASS, DROPPED,
    _bars_norm, _min_prom,
    precompute_break_dirs, precompute_cl_dir, precompute_rsi,
    _find_struct_high, _find_struct_low,
)

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

# Deployed config — anything that varies these is a separate experiment.
AUTO_EW_MIN_CONFIDENCE = 0.70

NW_LOOKBACK = 5
TL_LOOKBACK = 8
EW_LOOKBACK = 8
BOS_LOOKBACK = 24
EXPIRY_BARS = 8


def precompute_auto_ew(daily):
    """For each daily idx ≥ 30, run auto_detect_ew on daily[:i+1] and
    store {dir, conf, pattern} for valid patterns. None elsewhere.
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
                out[i] = {'dir': d, 'conf': conf, 'pattern': pat}
        except Exception:
            pass
    return out


def find_setups_with_pattern(pair_data, pair_key, auto_ew):
    """Walk m15/h1/daily with deployed config; tag each setup with the
    auto-EW pattern that supplied the EW direction (None if structural
    carried the bar)."""
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))

    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None

    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]

    ew_struct = precompute_break_dirs(daily, EW_LOOKBACK)
    tl_arr = precompute_break_dirs(h1, TL_LOOKBACK)
    nw_arr = precompute_break_dirs(m15, NW_LOOKBACK)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)

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

    rsi_gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pair_key), {'hi': 80, 'lo': 20})

    setups = []
    last_resolved = -1
    for i in range(40, n_m15 - 1):
        if i <= last_resolved:
            continue
        cur_ts = m15[i]['_ts']
        h1_idx = find_h1_idx(cur_ts)
        d_idx = find_d_idx(cur_ts)
        if h1_idx < TL_LOOKBACK or d_idx < EW_LOOKBACK:
            continue

        ae = auto_ew[d_idx] if d_idx < len(auto_ew) else None
        if ae and ae['conf'] >= AUTO_EW_MIN_CONFIDENCE:
            ew = ae['dir']
            pattern = ae['pattern']  # auto-EW carried the direction
        else:
            ew = ew_struct[d_idx] if d_idx < len(ew_struct) else None
            pattern = None  # structural fallback

        tl = tl_arr[h1_idx]
        nw = nw_arr[i]
        cl = cl_arr[h1_idx]
        if not ew or not tl or not nw or not cl:
            continue
        if ew != tl or tl != nw or nw != cl:
            continue
        if ew not in ('bull', 'bear'):
            continue

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
        setups.append({
            'outcome': outcome,
            'pattern': pattern,  # None = structural; else the auto-EW pattern name
            'class': PAIR_CLASS.get(pair_key, 'unknown'),
        })
    return setups


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    print('Precomputing auto-EW per pair (one auto_detect_ew per daily bar)...',
          file=sys.stderr)
    t0 = time.time()
    all_setups = []
    for k in sorted(pairs.keys()):
        if k in DROPPED or k not in PAIR_CLASS:
            continue
        daily = _bars_norm(pairs[k].get('daily', []))
        if len(daily) < 35:
            continue
        ae = precompute_auto_ew(daily)
        setups = find_setups_with_pattern(pairs[k], k, ae)
        if setups is None:
            continue
        # Diagnostic line: how many of this pair's setups used auto-EW
        n_with_pattern = sum(1 for s in setups if s['pattern'] is not None)
        print(f'  {k:<10} setups={len(setups):>3}  auto_ew={n_with_pattern:>3}', file=sys.stderr)
        all_setups.extend(setups)
    print(f'Walk + precompute took {time.time() - t0:.1f}s', file=sys.stderr)

    # Aggregate by pattern source
    print()
    print('=' * 90)
    print('AUTO-EW PATTERN WR BREAKDOWN')
    print('=' * 90)
    print('Deployed config: AUTO_EW_MIN_CONFIDENCE=0.70, per-class A1 RSI gate live.')
    print('"None (structural)" = bars where auto-EW conf < 0.70 and structural EW carried the dir.')
    print()

    def tally(setups):
        w = l = e = 0
        for s in setups:
            if s['outcome'] == 'win':
                w += 1
            elif s['outcome'] == 'loss':
                l += 1
            elif s['outcome'] == 'expired':
                e += 1
        decided = w + l
        return {
            'n': len(setups), 'wins': w, 'losses': l, 'expired': e,
            'decided': decided,
            'wr': (w / decided * 100) if decided else None,
        }

    # Bucket by pattern
    buckets = {}
    for s in all_setups:
        key = s['pattern'] if s['pattern'] is not None else '_structural'
        buckets.setdefault(key, []).append(s)

    # Print pattern breakdown — patterns sorted by sample size descending,
    # with structural pinned at the bottom as the control.
    ordered_keys = sorted(
        [k for k in buckets if k != '_structural'],
        key=lambda k: -len(buckets[k]),
    )
    ordered_keys.append('_structural')

    print(f'{"pattern":<32} {"n_setups":>8} {"n_decided":>9} {"W":>4} {"L":>4} {"WR":>7}')
    print('-' * 75)
    structural_wr = None
    structural_decided = None
    for key in ordered_keys:
        s = tally(buckets[key])
        wr_s = f'{s["wr"]:.1f}%' if s['wr'] is not None else '—'
        label = 'None (structural EW)' if key == '_structural' else key
        print(f'{label:<32} {s["n"]:>8} {s["decided"]:>9} {s["wins"]:>4} {s["losses"]:>4} {wr_s:>7}')
        if key == '_structural':
            structural_wr = s['wr']
            structural_decided = s['decided']

    # Drop-candidate recommendation
    print()
    print('=' * 90)
    print('DROP-CANDIDATE RECOMMENDATIONS')
    print('=' * 90)
    print(f'Rule: drop any pattern with WR ≥ 8pp below the structural control')
    print(f'      AND ≥ 30 decided trades (small samples don\'t earn a decision).')
    print(f'Structural control WR: '
          f'{structural_wr:.1f}% (n_decided={structural_decided})' if structural_wr is not None else 'Structural control: insufficient data')
    print()
    drop_candidates = []
    keep_strong = []
    insufficient = []
    for key in ordered_keys:
        if key == '_structural':
            continue
        s = tally(buckets[key])
        if s['decided'] < 30:
            insufficient.append((key, s))
            continue
        delta = (s['wr'] - structural_wr) if (s['wr'] is not None and structural_wr is not None) else 0
        if delta <= -8:
            drop_candidates.append((key, s, delta))
        elif delta >= 5:
            keep_strong.append((key, s, delta))

    if drop_candidates:
        print('DROP candidates (WR ≥ 8pp worse than structural):')
        for key, s, delta in drop_candidates:
            print(f'  - {key}: {s["wr"]:.1f}% ({s["decided"]} decided, {delta:+.1f}pp vs structural)')
    else:
        print('No drop candidates — no pattern is ≥8pp worse than structural at sufficient sample.')

    if keep_strong:
        print()
        print('STRONG patterns (WR ≥ 5pp better than structural):')
        for key, s, delta in keep_strong:
            print(f'  + {key}: {s["wr"]:.1f}% ({s["decided"]} decided, {delta:+.1f}pp vs structural)')

    if insufficient:
        print()
        print('Insufficient sample (<30 decided, no recommendation):')
        for key, s in insufficient:
            wr_s = f'{s["wr"]:.1f}%' if s['wr'] is not None else '—'
            print(f'  ? {key}: {wr_s} ({s["decided"]} decided)')

    # Per-class breakdown of the dominant pattern(s)
    print()
    print('=' * 90)
    print('PER-CLASS PATTERN USAGE')
    print('=' * 90)
    print('Where each pattern shows up most — useful for spotting class-specific drops.')
    print()
    classes = ('major', 'minor', 'comm', 'index', 'crypto')
    pattern_keys = ordered_keys  # same order
    print(f'{"pattern":<32}  ' + '  '.join(f'{c:>10}' for c in classes))
    print('-' * 90)
    for key in pattern_keys:
        label = 'None (structural)' if key == '_structural' else key
        row = f'{label:<32}  '
        for cls in classes:
            sub = [s for s in buckets[key] if s['class'] == cls]
            t = tally(sub)
            if t['decided'] == 0:
                row += f'{"—":>10}  '
            else:
                row += f'{t["wr"]:5.1f}%({t["decided"]:>2})  '
        print(row)


if __name__ == '__main__':
    main()
