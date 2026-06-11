"""Backtest the "low-opposing-wick" counter-bar rule.

User flagged XAG/USD 2026-06-11 cancellation: two consecutive
"counter" bars (bull body in a bear setup) with significant upper
wicks (indicating rejection at the highs) triggered the momentum-
reversal cancellation, even though the second bar's close was well
below its high — not actual confirmed bullish pressure.

Current rule (Viking_Invest_Trading_v69.html ~L12856):
  isCounter(b) = (b.c > b.o)  AND  (b.c - b.o) >= minBody
  where minBody = 0.25 × minProminence
  Plus two consecutive + rising/falling closes between them.

Prior NOWICK attempts (body/range ratio, see comment ~L12834):
  0.55 → aggregate WR 58.4%  (-11.3pp vs no filter)
  0.70 → aggregate WR 63.2%  ( -6.5pp vs no filter)
Both degraded performance. Reverted to current.

NEW rule tested here — "opposing-wick budget":
  bear setup: bull body  AND  upper_wick <= body × WICK_RATIO
  bull setup: bear body  AND  lower_wick <= body × WICK_RATIO
Intuition: a "real" counter-bar closes near its extreme in the
counter direction. If the opposing wick is bigger than the body
the bar is mostly rejection, not confirmation.

For the XAG case at idx 95:
  body 0.17, upper_wick 0.15 → upper_wick/body = 0.88
  Fails at WICK_RATIO = 0.5 → not a counter-bar → no cancel.

Tested at three WICK_RATIO values: 0.5 / 1.0 / 1.5. Reports per-pair
and per-class WR vs current production rule. Decision: deploy if
overall delta ≥ 0 AND comm/index both ≥ 0; otherwise keep current.
"""
import json
import sys
import time

from detect_triggers import (
    auto_detect_ew, AUTO_EW_VALID_PATTERNS,
    RSI_GATE_BY_CLASS, PAIR_CLASS,
)
from backtest_rsi_per_class import (
    DROPPED, _bars_norm, _min_prom,
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

FIB_PAIRS = {'xauusd','xagusd','usoil','wtiusd','natgas','xptusd',
             'de40','nas100','dj30','ftse100','spx500','jp225'}


def production_method(k):
    return 'fib' if k in FIB_PAIRS else 'wick'


def precompute_auto_ew_dirs(daily):
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
            if d in ('bull', 'bear') and pat in AUTO_EW_VALID_PATTERNS:
                out[i] = {'dir': d, 'conf': conf, 'pattern': pat}
        except Exception:
            pass
    return out


def is_counter_old(b, setup_dir, min_body):
    """Current production rule — any body ≥ min_body in the opposing direction."""
    c, o = b.get('c'), b.get('o')
    if c is None or o is None:
        return False
    if setup_dir == 'bear':
        return (c > o) and ((c - o) >= min_body)
    return (c < o) and ((o - c) >= min_body)


def is_counter_new(b, setup_dir, min_body, wick_ratio):
    """Proposed: counter-bar must close near its opposing extreme.
       For bear setup → bull body + upper_wick ≤ body × wick_ratio.
       For bull setup → bear body + lower_wick ≤ body × wick_ratio."""
    o, h, l, c = b.get('o'), b.get('h'), b.get('l'), b.get('c')
    if None in (o, h, l, c):
        return False
    if setup_dir == 'bear':
        body = c - o
        if body < min_body:
            return False
        upper_wick = h - max(o, c)
        return upper_wick <= body * wick_ratio
    body = o - c
    if body < min_body:
        return False
    lower_wick = min(o, c) - l
    return lower_wick <= body * wick_ratio


def opposing_choch(bars, creator_idx, current_idx, setup_dir, min_prom):
    """Lightweight stand-in for detect_opposing_choch — gives the
       walk a structure-flip exit (matches existing simulator)."""
    if current_idx - creator_idx < 3:
        return False
    if setup_dir == 'bear':
        post_low = min((bars[j]['l'] for j in range(creator_idx + 1, current_idx + 1)
                        if bars[j].get('l') is not None), default=None)
        if post_low is None:
            return False
        peak = float('-inf')
        for j in range(creator_idx + 1, current_idx):
            h = bars[j].get('h')
            if h is not None and h > peak:
                peak = h
        if not (peak > -float('inf')):
            return False
        last_c = bars[current_idx].get('c')
        return last_c is not None and last_c > peak and (last_c - peak) >= min_prom
    else:
        post_hi = max((bars[j]['h'] for j in range(creator_idx + 1, current_idx + 1)
                       if bars[j].get('h') is not None), default=None)
        if post_hi is None:
            return False
        trough = float('inf')
        for j in range(creator_idx + 1, current_idx):
            l = bars[j].get('l')
            if l is not None and l < trough:
                trough = l
        if not (trough < float('inf')):
            return False
        last_c = bars[current_idx].get('c')
        return last_c is not None and last_c < trough and (trough - last_c) >= min_prom


def walk_setup(m15, i, n_m15, entry, stop, target, ew, counter_fn, min_prom):
    """Forward walk a single setup with a pluggable counter-bar test."""
    lift_done = False
    triggered = False
    outcome = 'expired'
    min_body = min_prom * 0.25

    for j in range(i + 1, min(i + 1 + EXPIRY + 32, n_m15)):
        b = m15[j]

        # 2-bar momentum-reversal check (the rule under test)
        if j >= 2:
            prev = m15[j - 1]; curr = b
            if counter_fn(prev, ew, min_body) and counter_fn(curr, ew, min_body):
                if ew == 'bear' and curr['c'] > prev['c']:
                    outcome = 'invalidated'; break
                if ew == 'bull' and curr['c'] < prev['c']:
                    outcome = 'invalidated'; break

        # Opposing CHoCH check (structure flip)
        if opposing_choch(m15, i, j, ew, min_prom):
            outcome = 'invalidated'; break

        # Lift gate
        if not lift_done:
            if ew == 'bull' and b['h'] >= m15[i]['h']:
                lift_done = True
            elif ew == 'bear' and b['l'] <= m15[i]['l']:
                lift_done = True

        if not lift_done:
            continue

        # Trigger check
        if not triggered:
            reach = (ew == 'bull' and b['l'] <= entry) or \
                    (ew == 'bear' and b['h'] >= entry)
            if reach:
                triggered = True
                for jj in range(j, min(j + 32, n_m15)):
                    bb = m15[jj]
                    if ew == 'bull':
                        if bb['l'] <= stop:
                            outcome = 'loss'; break
                        if bb['h'] >= target:
                            outcome = 'win'; break
                    else:
                        if bb['h'] >= stop:
                            outcome = 'loss'; break
                        if bb['l'] <= target:
                            outcome = 'win'; break
                break
    return outcome


def find_setups(pair_data, pair_key, auto_ew_dirs, counter_fn):
    m15 = _bars_norm(pair_data.get('m15', []))
    h1 = _bars_norm(pair_data.get('h1', []))
    daily = _bars_norm(pair_data.get('daily', []))
    if len(m15) < 100 or len(h1) < TL_LB + 5 or len(daily) < EW_LB + 5:
        return []
    import datetime as _dt
    for b in m15:
        if '_ts' not in b:
            b['_ts'] = _dt.datetime.fromisoformat(b['t'].replace('Z', '+00:00')).timestamp()
    for b in h1:
        if '_ts' not in b:
            b['_ts'] = _dt.datetime.fromisoformat(b['t'].replace('Z', '+00:00')).timestamp()
    h1_ts = [b['_ts'] for b in h1]
    daily_ts = [b['_ts'] for b in daily]
    ew_struct = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
    cl_arr = precompute_cl_dir(h1)
    h1_closes = [b['c'] for b in h1]
    rsi_arr = precompute_rsi(h1_closes)

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
        auto = auto_ew_dirs[d_idx] if d_idx < len(auto_ew_dirs) else None
        if auto and auto['conf'] >= AUTO_EW_MIN_CONFIDENCE:
            ew = auto['dir']
        else:
            ew = ew_struct[d_idx]
        tl = tl_arr[h1_idx]; nw = nw_arr[i]; cl = cl_arr[h1_idx]
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
            if stop <= entry: continue
            R = stop - entry; target = entry - R
        else:
            entry = m15[i]['l']
            stop = _find_struct_low(bos_slc, prom)
            if stop >= entry: continue
            R = entry - stop; target = entry + R

        atr_slc = m15[max(0, i - 20):i]
        if len(atr_slc) >= 14:
            atr20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr_slc) / len(atr_slc)
            if R < 0.5 * atr20:
                continue

        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if ew == 'bull' and rsi_at >= gate['hi']: continue
            if ew == 'bear' and rsi_at <= gate['lo']: continue

        outcome = walk_setup(m15, i, n_m15, entry, stop, target, ew, counter_fn, prom)
        setups.append({'pair': pair_key, 'ew': ew, 'outcome': outcome})
        last_resolved = i + EXPIRY
    return setups


def tally(setups):
    w = sum(1 for s in setups if s['outcome'] == 'win')
    l = sum(1 for s in setups if s['outcome'] == 'loss')
    inv = sum(1 for s in setups if s['outcome'] == 'invalidated')
    exp = sum(1 for s in setups if s['outcome'] == 'expired')
    decided = w + l
    return {'n': len(setups), 'w': w, 'l': l, 'inv': inv, 'exp': exp,
            'decided': decided,
            'wr': (w / decided * 100) if decided else None}


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})
    targets = sorted([k for k in pairs.keys()
                      if k not in DROPPED and k in PAIR_CLASS and k != 'dxy'])

    # Test variants:
    variants = [
        ('OLD (any-body)', lambda b, dir, mb: is_counter_old(b, dir, mb)),
        ('NEW wick≤body×0.5',  lambda b, dir, mb: is_counter_new(b, dir, mb, 0.5)),
        ('NEW wick≤body×1.0',  lambda b, dir, mb: is_counter_new(b, dir, mb, 1.0)),
        ('NEW wick≤body×1.5',  lambda b, dir, mb: is_counter_new(b, dir, mb, 1.5)),
    ]

    # Per-pair results
    print(f'Backtesting counter-bar rule variants over {len(targets)} pairs...',
          file=sys.stderr)
    t0 = time.time()

    # Auto-EW cache is expensive; compute once per pair and reuse.
    ae_cache = {}
    for k in targets:
        p = pairs.get(k)
        if not p: continue
        daily = _bars_norm(p.get('daily', []))
        if len(daily) < 35: continue
        ae_cache[k] = precompute_auto_ew_dirs(daily)

    all_results = {}  # {variant_name: {pair: tally}}
    for vname, fn in variants:
        all_results[vname] = {}
        for k in sorted(ae_cache):
            p = pairs[k]
            setups = find_setups(p, k, ae_cache[k], fn)
            if not setups: continue
            all_results[vname][k] = tally(setups)
        print(f'  {vname} done', file=sys.stderr)
    print(f'\nTotal: {time.time() - t0:.1f}s\n', file=sys.stderr)

    # Per-pair table
    print('=' * 130)
    print('COUNTER-BAR RULE COMPARISON')
    print('=' * 130)
    hdr = f'{"pair":<10}{"class":<7}{"meth":<5}'
    for vname, _ in variants:
        hdr += f'{vname:>22}'
    print(hdr)
    print('-' * 130)

    for k in sorted(ae_cache):
        cls = PAIR_CLASS.get(k, '?')
        meth = production_method(k)
        row = f'{k:<10}{cls:<7}{meth:<5}'
        for vname, _ in variants:
            t = all_results[vname].get(k)
            if not t:
                row += f'{"—":>22}'
            else:
                wr_s = f'{t["wr"]:5.1f}%' if t["wr"] is not None else '  —  '
                row += f'  {t["w"]:>2}W/{t["l"]:>2}L {wr_s} (inv={t["inv"]})'.rjust(22)
        print(row)

    # Class aggregates
    print()
    print('=' * 130)
    print('AGGREGATE BY CLASS')
    print('=' * 130)
    classes = sorted(set(PAIR_CLASS.get(k) for k in ae_cache if PAIR_CLASS.get(k)))
    for cls in classes:
        print(f'\n--- {cls} ---')
        for vname, _ in variants:
            w = sum(all_results[vname].get(k, {}).get('w', 0) for k in ae_cache
                    if PAIR_CLASS.get(k) == cls)
            l = sum(all_results[vname].get(k, {}).get('l', 0) for k in ae_cache
                    if PAIR_CLASS.get(k) == cls)
            inv = sum(all_results[vname].get(k, {}).get('inv', 0) for k in ae_cache
                      if PAIR_CLASS.get(k) == cls)
            d_ = w + l
            wr = (w / d_ * 100) if d_ else None
            wr_s = f'{wr:5.1f}%' if wr is not None else '   —  '
            print(f'  {vname:<25}  {w:>3}W/{l:>3}L  WR={wr_s}  invalidated={inv}')

    # Overall
    print()
    print('=' * 130)
    print('OVERALL')
    print('=' * 130)
    for vname, _ in variants:
        w = sum(t.get('w', 0) for t in all_results[vname].values())
        l = sum(t.get('l', 0) for t in all_results[vname].values())
        inv = sum(t.get('inv', 0) for t in all_results[vname].values())
        d_ = w + l
        wr = (w / d_ * 100) if d_ else None
        wr_s = f'{wr:5.2f}%' if wr is not None else '   —  '
        print(f'  {vname:<25}  {w:>4}W/{l:>4}L  WR={wr_s}  invalidated={inv}')


if __name__ == '__main__':
    main()
