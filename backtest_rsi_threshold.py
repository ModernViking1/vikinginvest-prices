"""Counterfactual backtest — compare RSI hard-gate thresholds (80/20 vs 90/10).

Walks historical-ohlc.json per pair, simulates the 4/4 confluence engine
(EW daily, TL 1H, NW 15m, CL 4H cloud), evaluates every candidate creator
bar, applies the RSI gate at the chosen threshold, and reports per-pair +
aggregate win rate.

Not byte-identical to the dashboard's recent-backtest engine — we omit
the fib-half-size variant and counter-bar / opposing-CHoCH invalidation
walks (roughly threshold-independent, so they cancel out in the
COMPARISON). What we model precisely:

  * 4/4 alignment at the candidate bar
  * Creator detection (close beyond 8-bar swing in aligned dir)
  * RSI gate at the parameterised threshold
  * Forward walk: target hit, stop hit, expiry

Output: a table of {pair, n_signals, wr, n_blocked_by_gate} for each
threshold. The DELTA between the two columns is what we use to decide
whether to deploy 90/10.

PERFORMANCE: direction arrays (EW, TL, NW, CL) and the h1 RSI series
are pre-computed ONCE per pair. The m15 loop then does O(1) index
lookups instead of re-walking history each bar.
"""
import json
import sys
from datetime import datetime

HIST_PATH = '/home/user/vikinginvest-prices/historical-ohlc.json'

NW_LOOKBACK = 5
TL_LOOKBACK = 8
EW_LOOKBACK = 8
BOS_LOOKBACK = 24
EXPIRY_BARS = 8


def _ts(b):
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
        h = b.get('h', c)
        l = b.get('l', c)
        o = b.get('o', c)
        out.append({'o': o, 'c': c, 'h': h, 'l': l, 't': b.get('t', ''), '_ts': _ts(b)})
    return out


def _min_prom(px):
    ap = abs(px)
    if ap > 1000:
        return ap * 0.001
    if ap > 5:
        return ap * 0.0008
    return 0.0005


# ── Pre-computed direction arrays ──────────────────────────────────

def precompute_break_dirs(bars, lookback):
    """For each bar idx i, return the most-recent-break direction
    considering bars[:i+1]. Uses staleness decay (2× lookback) and
    slope fallback — mirrors calc_independent_dir in detect_triggers.

    O(n × lookback) total. Returns array of len(bars).
    """
    n = len(bars)
    out = [None] * n
    last_break_dir = None
    last_break_idx = -1
    for i in range(n):
        if i < lookback:
            out[i] = None
            continue
        slc = bars[max(0, i - lookback):i]
        if len(slc) < 5:
            out[i] = last_break_dir
            continue
        sw_hi = max(b['h'] for b in slc)
        sw_lo = min(b['l'] for b in slc)
        c = bars[i]['c']
        px_abs = abs(c)
        mp = px_abs * 0.001 if px_abs > 1000 else (px_abs * 0.0008 if px_abs > 5 else 0.0005)
        if c > sw_hi and (c - sw_hi) >= mp:
            last_break_dir = 'bull'
            last_break_idx = i
        elif c < sw_lo and (sw_lo - c) >= mp:
            last_break_dir = 'bear'
            last_break_idx = i
        # Staleness decay 2× lookback
        bars_since = i - last_break_idx if last_break_idx >= 0 else 999
        if last_break_dir is not None and bars_since >= 2 * lookback:
            # Use slope fallback
            first_c = bars[i - lookback]['c'] if i >= lookback else None
            last_c = c
            change = (last_c - first_c) / first_c if first_c else 0
            if change > 0.0015:
                out[i] = 'bull'
            elif change < -0.0015:
                out[i] = 'bear'
            else:
                out[i] = 'neutral'
        else:
            out[i] = last_break_dir
    return out


def precompute_cl_dir(h1, fast=21, slow=55):
    """4H EMA cloud direction at every h1 idx. Aggregates h1 to 4H by
    UTC hour bucket, finalises bucket close on bucket-change, maintains
    EMA21/55, applies price-through-cloud guard.
    """
    n = len(h1)
    out = [None] * n
    if n < slow * 4:
        return out
    k_f = 2 / (fast + 1)
    k_s = 2 / (slow + 1)
    bucket_closes = []  # finalised 4H closes
    cur_dir = None
    last_key = None
    cur_close = None
    seed21 = []
    seed55 = []
    ema21 = None
    ema55 = None

    def finalise(close):
        nonlocal ema21, ema55, cur_dir
        if close is None:
            return
        if len(seed21) < fast:
            seed21.append(close)
            if len(seed21) == fast:
                ema21 = sum(seed21) / fast
        else:
            ema21 = close * k_f + ema21 * (1 - k_f)
        if len(seed55) < slow:
            seed55.append(close)
            if len(seed55) == slow:
                ema55 = sum(seed55) / slow
        else:
            ema55 = close * k_s + ema55 * (1 - k_s)
        if ema21 is not None and ema55 is not None:
            if ema21 > ema55:
                # price-through-cloud guard
                if close < ema21 and close < ema55:
                    cur_dir = 'neutral'
                else:
                    cur_dir = 'bull'
            elif ema21 < ema55:
                if close > ema21 and close > ema55:
                    cur_dir = 'neutral'
                else:
                    cur_dir = 'bear'
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
    """Wilder RSI series at every idx. Returns nulls during warm-up."""
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


def _find_struct_high(slc, min_prom):
    n = len(slc)
    for j in range(n - 2, 1, -1):
        if j < 1 or j >= n - 1:
            continue
        this_h = slc[j]['h']
        prev1 = slc[j - 1]['h']
        prev2 = slc[j - 2]['h'] if j >= 2 else prev1
        right_h = slc[j + 1]['h']
        left_h = max(prev1, prev2)
        if this_h > left_h and this_h > right_h and (this_h - max(left_h, right_h)) >= min_prom:
            return this_h
    return max(b['h'] for b in slc)


def _find_struct_low(slc, min_prom):
    n = len(slc)
    for j in range(n - 2, 1, -1):
        if j < 1 or j >= n - 1:
            continue
        this_l = slc[j]['l']
        prev1 = slc[j - 1]['l']
        prev2 = slc[j - 2]['l'] if j >= 2 else prev1
        right_l = slc[j + 1]['l']
        left_l = min(prev1, prev2)
        if this_l < left_l and this_l < right_l and (min(left_l, right_l) - this_l) >= min_prom:
            return this_l
    return min(b['l'] for b in slc)


def backtest_pair(pair_data, rsi_hi=80, rsi_lo=20):
    h1 = _bars_norm(pair_data.get('h1', []))
    m15 = _bars_norm(pair_data.get('m15', []))
    daily = _bars_norm(pair_data.get('daily', []))

    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return None

    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]
    m15_ts = [b['_ts'] for b in m15]

    # Pre-compute direction arrays
    ew_arr = precompute_break_dirs(daily, EW_LOOKBACK)
    tl_arr = precompute_break_dirs(h1, TL_LOOKBACK)
    nw_arr = precompute_break_dirs(m15, NW_LOOKBACK)
    cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)

    # Index helpers
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

    wins = losses = expired = blocked = 0
    n_signals = 0
    last_resolved = -1

    n_m15 = len(m15)
    for i in range(40, n_m15 - 1):
        if i <= last_resolved:
            continue
        cur_ts = m15_ts[i]
        h1_idx = find_h1_idx(cur_ts)
        d_idx = find_d_idx(cur_ts)
        if h1_idx < TL_LOOKBACK or d_idx < EW_LOOKBACK:
            continue

        ew = ew_arr[d_idx]
        tl = tl_arr[h1_idx]
        nw = nw_arr[i]
        cl = cl_arr[h1_idx] if h1_idx < len(cl_arr) else None

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

        # Min-R floor
        atr_slc = m15[max(0, i - 20):i]
        if len(atr_slc) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr_slc) / len(atr_slc)
            if R < 0.5 * atr20:
                continue

        # A1 RSI gate
        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if ew == 'bull' and rsi_at >= rsi_hi:
                blocked += 1
                continue
            if ew == 'bear' and rsi_at <= rsi_lo:
                blocked += 1
                continue

        n_signals += 1

        # Forward walk with lift-then-retest semantics
        lift_done = False
        resolved = None
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
                                resolved = 'loss'
                                last_j = jj
                                break
                            if bb['h'] >= target:
                                resolved = 'win'
                                last_j = jj
                                break
                        else:
                            if bb['h'] >= stop:
                                resolved = 'loss'
                                last_j = jj
                                break
                            if bb['l'] <= target:
                                resolved = 'win'
                                last_j = jj
                                break
                    break
            if j - i > EXPIRY_BARS:
                resolved = 'expired'
                last_j = j
                break
        last_resolved = last_j
        if resolved == 'win':
            wins += 1
        elif resolved == 'loss':
            losses += 1
        elif resolved == 'expired':
            expired += 1

    decided = wins + losses
    wr = (wins / decided * 100) if decided else None
    return {
        'n_signals': n_signals,
        'wins': wins,
        'losses': losses,
        'expired': expired,
        'blocked': blocked,
        'wr': wr,
    }


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})

    results = {}
    for hi, lo in [(80, 20), (90, 10)]:
        print(f'\n=== RSI gate {hi}/{lo} ===', file=sys.stderr)
        results[(hi, lo)] = {}
        for k in sorted(pairs.keys()):
            r = backtest_pair(pairs[k], hi, lo)
            if r and r['n_signals'] > 0:
                results[(hi, lo)][k] = r
                wr_s = f'{r["wr"]:.1f}%' if r['wr'] is not None else '—'
                print(f'  {k:<8} n={r["n_signals"]:>3} wr={wr_s:>6} blocked={r["blocked"]}',
                      file=sys.stderr)

    print()
    print(f'{"pair":<10}  {"n_80/20":>7} {"wr_80/20":>9} {"blk":>5}   {"n_90/10":>7} {"wr_90/10":>9} {"blk":>5}   {"delta_wr":>9}')
    print('-' * 95)
    all_pairs = sorted(set(results[(80, 20)].keys()) | set(results[(90, 10)].keys()))
    agg_80 = {'wins': 0, 'losses': 0, 'n': 0, 'blocked': 0}
    agg_90 = {'wins': 0, 'losses': 0, 'n': 0, 'blocked': 0}
    for k in all_pairs:
        a = results[(80, 20)].get(k, {'n_signals': 0, 'wr': None, 'blocked': 0, 'wins': 0, 'losses': 0})
        b = results[(90, 10)].get(k, {'n_signals': 0, 'wr': None, 'blocked': 0, 'wins': 0, 'losses': 0})
        a_wr = f'{a["wr"]:.1f}%' if a.get('wr') is not None else '—'
        b_wr = f'{b["wr"]:.1f}%' if b.get('wr') is not None else '—'
        if a.get('wr') is not None and b.get('wr') is not None:
            delta = b['wr'] - a['wr']
            delta_s = f'{delta:+.1f}pp'
        else:
            delta_s = '—'
        print(f'{k:<10}  {a["n_signals"]:>7} {a_wr:>9} {a["blocked"]:>5}   '
              f'{b["n_signals"]:>7} {b_wr:>9} {b["blocked"]:>5}   {delta_s:>9}')
        agg_80['wins'] += a['wins']
        agg_80['losses'] += a['losses']
        agg_80['n'] += a['n_signals']
        agg_80['blocked'] += a['blocked']
        agg_90['wins'] += b['wins']
        agg_90['losses'] += b['losses']
        agg_90['n'] += b['n_signals']
        agg_90['blocked'] += b['blocked']
    print('-' * 95)
    a_wr_t = agg_80['wins'] / (agg_80['wins'] + agg_80['losses']) * 100 if (agg_80['wins'] + agg_80['losses']) else 0
    b_wr_t = agg_90['wins'] / (agg_90['wins'] + agg_90['losses']) * 100 if (agg_90['wins'] + agg_90['losses']) else 0
    print(f'{"AGG":<10}  {agg_80["n"]:>7} {a_wr_t:>8.1f}% {agg_80["blocked"]:>5}   '
          f'{agg_90["n"]:>7} {b_wr_t:>8.1f}% {agg_90["blocked"]:>5}   '
          f'{b_wr_t - a_wr_t:+9.2f}pp')
    print()
    print(f'80/20: {agg_80["wins"]}W / {agg_80["losses"]}L (+{agg_80["n"] - agg_80["wins"] - agg_80["losses"]} expired) · {agg_80["blocked"]} pre-blocked by gate')
    print(f'90/10: {agg_90["wins"]}W / {agg_90["losses"]}L (+{agg_90["n"] - agg_90["wins"] - agg_90["losses"]} expired) · {agg_90["blocked"]} pre-blocked by gate')

    # Delta isolation: how many trades does 90/10 LET THROUGH that 80/20 blocked?
    delta_blocked = agg_80['blocked'] - agg_90['blocked']
    print(f'\nMarginal trades unlocked by loosening 80/20 → 90/10: {delta_blocked}')


if __name__ == '__main__':
    main()
