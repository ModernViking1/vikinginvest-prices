"""Per-asset-class RSI threshold sweep — find the best 1H RSI hard-gate
upper/lower bounds for each asset class (FX major, FX minor, commodity,
index, crypto) instead of a one-size-fits-all 80/20.

Why per-class:
  - eurusd / eurnzd / usoil / btcusd / ethusd showed +1 to +5pp WR
    when the gate was loosened from 80/20 to 90/10 (catches trend
    continuation past the textbook overbought line).
  - ondousd / xagusd / nearusd / ltcusd showed -1.5 to -3.7pp at 90/10
    (RSI 80-90 marks real exhaustion on these and the tight gate is
    catching it).
  - That's a clear class-level pattern. The 80/20 gate is the wrong
    default for some classes and the right default for others.

Method:
  - Walk historical-ohlc.json with the same simplified 4/4 engine used
    in the dropped 80/20-vs-90/10 comparison (no fib variant, no
    counter-bar / opposing-CHoCH invalidation — threshold-independent
    components cancel in the per-class deltas).
  - For each pair, for each threshold pair {(70,30), (75,25), (80,20),
    (85,15), (90,10), None=no-gate}, record n_signals, wins, losses,
    expired, blocked.
  - Aggregate per asset class. Surface the threshold that maximises WR
    AND has at least 50 decided trades.

The pair → class mapping mirrors the dashboard's MKTS[k].t field
(major / minor / comm / index / crypto). Dropped pairs (audchf, eursek,
usdnok) are excluded.
"""
import json
import sys

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

# Mirrors the dashboard's MKTS[k].t classifications. Source of truth is
# the v69 HTML; re-extract with the one-liner in commit message if a
# pair is added or its class changes.
PAIR_CLASS = {
    'eurusd': 'major', 'gbpusd': 'major', 'usdjpy': 'major',
    'usdcad': 'major', 'usdchf': 'major', 'nzdusd': 'major',
    'audusd': 'major',
    'cadjpy': 'minor', 'eurnzd': 'minor',
    # gbpaud removed 2026-06-10h — chronic ~50% WR.
    'euraud': 'minor', 'usdsgd': 'minor', 'audnzd': 'minor',
    'eurgbp': 'minor', 'gbpcad': 'minor',
    # audcad removed 2026-06-10i — low WR.
    'nzdjpy': 'minor', 'gbpnzd': 'minor',
    # nzdcad removed 2026-06-10 — low win-rate drag.
    'eurnok': 'minor', 'nzdchf': 'minor',
    # gbpchf / usdcnh removed 2026-06-10 — low win-rate drag.
    'usdzar': 'minor', 'eursgd': 'minor',
    'xauusd': 'comm', 'xagusd': 'comm', 'usoil': 'comm',
    'wtiusd': 'comm', 'natgas': 'comm', 'xptusd': 'comm',
    'de40': 'index', 'ftse100': 'index', 'dj30': 'index',
    'nas100': 'index', 'spx500': 'index', 'jp225': 'index',
    # fra40 removed 2026-06-10 — low win-rate drag.
    'btcusd': 'crypto', 'ethusd': 'crypto', 'solusd': 'crypto',
    'xrpusd': 'crypto', 'suiusd': 'crypto',
    # ltcusd removed 2026-06-10 — low win-rate drag.
    'taousd': 'crypto', 'nearusd': 'crypto',
    # hypeusd removed 2026-06-10.
    'ondousd': 'crypto',
}
DROPPED = {'audchf', 'eursek', 'usdnok',
           # 2026-06-10 round
           'ltcusd', 'gbpchf', 'usdcnh', 'fra40',
           # 2026-06-10 follow-up
           'nzdcad',
           # 2026-06-10g
           'hypeusd',
           # 2026-06-10h
           'gbpaud',
           # 2026-06-10i
           'audcad'}

THRESHOLDS = [
    (None, None),    # no gate (baseline)
    (70, 30),
    (75, 25),
    (80, 20),        # current production
    (85, 15),
    (90, 10),
]

NW_LOOKBACK = 5
TL_LOOKBACK = 8
EW_LOOKBACK = 8
BOS_LOOKBACK = 24
EXPIRY_BARS = 8


def _ts(b):
    from datetime import datetime
    t = b.get('t')
    if not t:
        return 0
    try:
        return datetime.fromisoformat(t.replace('Z', '+00:00')).timestamp()
    except Exception:
        return 0


def _bars_norm(arr):
    out = []
    for b in arr:
        c = b.get('c', b.get('p'))
        if c is None:
            continue
        out.append({
            'o': b.get('o', c), 'c': c,
            'h': b.get('h', c), 'l': b.get('l', c),
            'v': b.get('v', 0) or 0,          # traded/tick volume (0 on legacy bars fetched pre-volume)
            't': b.get('t', ''), '_ts': _ts(b),
        })
    return out


def _min_prom(px):
    ap = abs(px)
    if ap > 1000:
        return ap * 0.001
    if ap > 5:
        return ap * 0.0008
    return 0.0005


def precompute_break_dirs(bars, lookback):
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
        mp = _min_prom(c)
        if c > sw_hi and (c - sw_hi) >= mp:
            last_dir = 'bull'
            last_idx = i
        elif c < sw_lo and (sw_lo - c) >= mp:
            last_dir = 'bear'
            last_idx = i
        bars_since = i - last_idx if last_idx >= 0 else 999
        if last_dir is not None and bars_since >= 2 * lookback:
            first_c = bars[i - lookback]['c']
            change = (c - first_c) / first_c if first_c else 0
            if change > 0.0015:
                out[i] = 'bull'
            elif change < -0.0015:
                out[i] = 'bear'
            else:
                out[i] = 'neutral'
        else:
            out[i] = last_dir
    return out


def precompute_cl_dir(h1, fast=21, slow=55):
    n = len(h1)
    out = [None] * n
    if n < slow * 4:
        return out
    k_f = 2 / (fast + 1)
    k_s = 2 / (slow + 1)
    cur_dir = None
    last_key = None
    cur_close = None
    seed_f = []
    seed_s = []
    ema_f = None
    ema_s = None

    def finalise(close):
        nonlocal ema_f, ema_s, cur_dir
        if close is None:
            return
        if len(seed_f) < fast:
            seed_f.append(close)
            if len(seed_f) == fast:
                ema_f = sum(seed_f) / fast
        else:
            ema_f = close * k_f + ema_f * (1 - k_f)
        if len(seed_s) < slow:
            seed_s.append(close)
            if len(seed_s) == slow:
                ema_s = sum(seed_s) / slow
        else:
            ema_s = close * k_s + ema_s * (1 - k_s)
        if ema_f is not None and ema_s is not None:
            if ema_f > ema_s:
                cur_dir = 'neutral' if (close < ema_f and close < ema_s) else 'bull'
            elif ema_f < ema_s:
                cur_dir = 'neutral' if (close > ema_f and close > ema_s) else 'bear'
            else:
                cur_dir = 'neutral'

    for j in range(n):
        bb = h1[j]
        t = bb.get('t', '')
        if len(t) < 13:
            out[j] = cur_dir
            continue
        try:
            hr = int(t[11:13])
        except ValueError:
            out[j] = cur_dir
            continue
        bh = (hr // 4) * 4
        key = t[:11] + (f'0{bh}' if bh < 10 else str(bh))
        if last_key is None:
            last_key = key
            cur_close = bb['c']
        elif key != last_key:
            finalise(cur_close)
            last_key = key
            cur_close = bb['c']
        else:
            cur_close = bb['c']
        out[j] = cur_dir
    return out


def precompute_rsi(closes, period=14):
    n = len(closes)
    out = [None] * n
    if n < period + 1:
        return out
    gain = loss = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gain += d
        else:
            loss -= d
    avg_g = gain / period
    avg_l = loss / period
    out[period] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0
        l = -d if d < 0 else 0
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
        out[i] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    return out


def _find_struct_high(slc, mp):
    n = len(slc)
    for j in range(n - 2, 1, -1):
        this_h = slc[j]['h']
        prev1 = slc[j - 1]['h']
        prev2 = slc[j - 2]['h'] if j >= 2 else prev1
        right_h = slc[j + 1]['h']
        left_h = max(prev1, prev2)
        if this_h > left_h and this_h > right_h and (this_h - max(left_h, right_h)) >= mp:
            return this_h
    return max(b['h'] for b in slc)


def _find_struct_low(slc, mp):
    n = len(slc)
    for j in range(n - 2, 1, -1):
        this_l = slc[j]['l']
        prev1 = slc[j - 1]['l']
        prev2 = slc[j - 2]['l'] if j >= 2 else prev1
        right_l = slc[j + 1]['l']
        left_l = min(prev1, prev2)
        if this_l < left_l and this_l < right_l and (min(left_l, right_l) - this_l) >= mp:
            return this_l
    return min(b['l'] for b in slc)


def find_setups(pair_data):
    """Pass 1: walk the m15 series once and capture every {creator, rsi,
    setup, outcome} tuple. Pass 2 (apply_gate) replays this list at any
    threshold in O(n_setups). Lets us sweep 6 thresholds in 1× the
    walk cost instead of 6×.
    """
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))

    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None

    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]

    ew_arr = precompute_break_dirs(daily, EW_LOOKBACK)
    tl_arr = precompute_break_dirs(h1, TL_LOOKBACK)
    nw_arr = precompute_break_dirs(m15, NW_LOOKBACK)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)

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

    setups = []
    last_resolved = -1
    n_m15 = len(m15)

    for i in range(40, n_m15 - 1):
        if i <= last_resolved:
            continue
        cur_ts = m15[i]['_ts']
        h1_idx = find_h1_idx(cur_ts)
        d_idx = find_d_idx(cur_ts)
        if h1_idx < TL_LOOKBACK or d_idx < EW_LOOKBACK:
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

        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None

        # Forward walk to outcome
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
        setups.append({'dir': ew, 'rsi': rsi_at, 'outcome': outcome})
    return setups


def apply_gate(setups, rsi_hi, rsi_lo):
    """Count outcomes for a given RSI threshold. None,None = no gate."""
    wins = losses = expired = blocked = 0
    for s in setups:
        if rsi_hi is not None and s['rsi'] is not None:
            if s['dir'] == 'bull' and s['rsi'] >= rsi_hi:
                blocked += 1
                continue
            if s['dir'] == 'bear' and s['rsi'] <= rsi_lo:
                blocked += 1
                continue
        if s['outcome'] == 'win':
            wins += 1
        elif s['outcome'] == 'loss':
            losses += 1
        elif s['outcome'] == 'expired':
            expired += 1
    decided = wins + losses
    wr = (wins / decided * 100) if decided else None
    return {
        'n': wins + losses + expired,
        'wins': wins, 'losses': losses, 'expired': expired,
        'blocked': blocked, 'wr': wr,
    }


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    # Pass 1: walk every pair once, capture setup tuples.
    print('Walking pairs...', file=sys.stderr)
    pair_setups = {}
    for k in sorted(pairs.keys()):
        if k in DROPPED:
            continue
        if k not in PAIR_CLASS:
            continue
        setups = find_setups(pairs[k])
        if setups is None:
            continue
        pair_setups[k] = setups
        print(f'  {k:<10} {len(setups)} setups', file=sys.stderr)

    # Pass 2: aggregate by class for each threshold.
    by_class = {}  # by_class[cls][threshold] = {n, wins, losses, expired, blocked}
    for cls in ('major', 'minor', 'comm', 'index', 'crypto'):
        by_class[cls] = {}
        for hi, lo in THRESHOLDS:
            agg = {'n': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'blocked': 0}
            for k, setups in pair_setups.items():
                if PAIR_CLASS[k] != cls:
                    continue
                r = apply_gate(setups, hi, lo)
                agg['n'] += r['n']
                agg['wins'] += r['wins']
                agg['losses'] += r['losses']
                agg['expired'] += r['expired']
                agg['blocked'] += r['blocked']
            decided = agg['wins'] + agg['losses']
            agg['wr'] = (agg['wins'] / decided * 100) if decided else None
            by_class[cls][(hi, lo)] = agg

    # Print the per-class sweep
    print()
    print('=' * 86)
    print('PER-CLASS RSI THRESHOLD SWEEP')
    print('=' * 86)
    header = f'{"class":<8} {"n_pairs":>7}  '
    for hi, lo in THRESHOLDS:
        label = 'no-gate' if hi is None else f'{hi}/{lo}'
        header += f'{label:>10}'
    print(header)
    print('-' * 86)
    for cls in ('major', 'minor', 'comm', 'index', 'crypto'):
        n_pairs = sum(1 for k in pair_setups if PAIR_CLASS[k] == cls)
        row = f'{cls:<8} {n_pairs:>7}  '
        for hi, lo in THRESHOLDS:
            r = by_class[cls][(hi, lo)]
            wr = r['wr']
            row += f'{wr:>9.1f}%' if wr is not None else f'{"—":>10}'
        print(row)
    # Decided trades row (for context — small n is unreliable)
    print()
    print(f'{"":<8} {"":<7}  ' + '  '.join(['(decided n)' for _ in THRESHOLDS]))
    for cls in ('major', 'minor', 'comm', 'index', 'crypto'):
        row = f'{cls:<8} {"":>7}  '
        for hi, lo in THRESHOLDS:
            r = by_class[cls][(hi, lo)]
            n = r['wins'] + r['losses']
            row += f'{n:>10}'
        print(row)

    print()
    print('=' * 86)
    print('RECOMMENDED PER-CLASS RSI THRESHOLDS')
    print('=' * 86)
    print('Picking the threshold with the highest WR per class, with ≥50 decided')
    print('trades to avoid small-sample noise. Tie → looser threshold (keep more trades).')
    print()
    print(f'{"class":<8} {"best":<10} {"WR":>6} {"decided":>8} {"vs 80/20":>10} {"vs no-gate":>11}')
    print('-' * 65)
    recommendations = {}
    for cls in ('major', 'minor', 'comm', 'index', 'crypto'):
        # Find threshold with max WR, requiring ≥50 decided
        best = None
        best_wr = -1
        for hi, lo in THRESHOLDS:
            r = by_class[cls][(hi, lo)]
            decided = r['wins'] + r['losses']
            if decided < 50 or r['wr'] is None:
                continue
            # Tie-break: prefer looser (later in THRESHOLDS list)
            if r['wr'] > best_wr or (abs(r['wr'] - best_wr) < 1e-9):
                best_wr = r['wr']
                best = (hi, lo)
        baseline_8020 = by_class[cls][(80, 20)]['wr']
        baseline_none = by_class[cls][(None, None)]['wr']
        recommendations[cls] = best
        if best:
            best_label = 'no-gate' if best[0] is None else f'{best[0]}/{best[1]}'
            decided = by_class[cls][best]['wins'] + by_class[cls][best]['losses']
            v_8020 = f'{best_wr - baseline_8020:+.1f}pp' if baseline_8020 is not None else '—'
            v_none = f'{best_wr - baseline_none:+.1f}pp' if baseline_none is not None else '—'
            print(f'{cls:<8} {best_label:<10} {best_wr:>5.1f}% {decided:>8} {v_8020:>10} {v_none:>11}')
        else:
            print(f'{cls:<8} (insufficient sample)')

    # Mock the live aggregate impact: simulate "apply per-class recommended" across all pairs
    agg = {'wins': 0, 'losses': 0, 'expired': 0, 'blocked': 0, 'n': 0}
    for k, setups in pair_setups.items():
        cls = PAIR_CLASS[k]
        hi, lo = recommendations.get(cls, (80, 20)) or (80, 20)
        r = apply_gate(setups, hi, lo)
        agg['wins'] += r['wins']
        agg['losses'] += r['losses']
        agg['expired'] += r['expired']
        agg['blocked'] += r['blocked']
        agg['n'] += r['n']
    # Compare against current 80/20 across the board
    agg_8020 = {'wins': 0, 'losses': 0, 'expired': 0, 'blocked': 0, 'n': 0}
    for k, setups in pair_setups.items():
        r = apply_gate(setups, 80, 20)
        agg_8020['wins'] += r['wins']
        agg_8020['losses'] += r['losses']
        agg_8020['expired'] += r['expired']
        agg_8020['blocked'] += r['blocked']
        agg_8020['n'] += r['n']
    wr_per_class = agg['wins'] / (agg['wins'] + agg['losses']) * 100 if (agg['wins'] + agg['losses']) else 0
    wr_8020 = agg_8020['wins'] / (agg_8020['wins'] + agg_8020['losses']) * 100 if (agg_8020['wins'] + agg_8020['losses']) else 0
    print()
    print('=' * 86)
    print('AGGREGATE COMPARISON — PER-CLASS RECOMMENDED vs CURRENT 80/20')
    print('=' * 86)
    print(f'  current 80/20 (all classes): {wr_8020:.2f}% over {agg_8020["wins"] + agg_8020["losses"]} decided')
    print(f'  per-class recommended      : {wr_per_class:.2f}% over {agg["wins"] + agg["losses"]} decided')
    print(f'  delta                      : {wr_per_class - wr_8020:+.2f}pp')


if __name__ == '__main__':
    main()
