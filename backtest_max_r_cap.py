"""Max-R cap impact backtest — compare current rules vs cap@2.5xATR(20).

Asked for after the XAG/USD 226-pip case (commit 20d82a71). The live
signal path in Viking_Invest_Trading_v69.html and the matching
detect_triggers.py path now cap the wick stop distance at 2.5xATR(20)
on top of the existing min-R floor. The 12-month deep backtest walker
(calcRecentBacktest) is unchanged so the displayed card WR is
unaffected for now — but if the cap degrades live performance the
user wants to know before applying it to the BT too.

Method: walk the SAME setup-finding logic used by backtest_wick_vs_fib_autoew.py
across the full universe, and for every qualifying 4/4 setup compute
FOUR outcomes side by side:
  wick_old / wick_new — full-size wick entry with current vs capped stop
  fib_old  / fib_new  — half-size fib 38% entry with current vs capped stop
Aggregate per pair, per class, and overall.

Decision rule the user asked for: don't lower the deep BT >=70% aggregate.
We report:
  - per-pair WR for the production methodology (wick on FX/crypto,
    fib on comm/index)
  - delta (new - old) at the per-pair and class-aggregate level
  - sample sizes so noise is visible
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
MAX_R_ATR_MULT = 2.5   # the cap under test

# Match production: live cap only fires on commodity + index pairs after
# the comparison run showed FX/crypto either neutral or negative.
CAP_CLASSES = {'comm', 'index'}

NW_LB = 5
TL_LB = 8
EW_LB = 8
BOS_LB = 24
EXPIRY = 8

# Pairs producing the FIB entry in production (matches FIB_ENTRY_PAIRS
# in detect_triggers.py / fibPairs in RULES_FINGERPRINT)
FIB_CLASS_PAIRS = {'xauusd','xagusd','usoil','wtiusd','natgas','xptusd',
                   'de40','nas100','dj30','ftse100','spx500','jp225'}


def production_method(pair_key):
    """Return 'wick' or 'fib' to match _btMethodFor in the dashboard."""
    return 'fib' if pair_key in FIB_CLASS_PAIRS else 'wick'


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
            if (d in ('bull', 'bear')
                    and pat in AUTO_EW_VALID_PATTERNS):
                out[i] = {'dir': d, 'conf': conf, 'pattern': pat}
        except Exception:
            pass
    return out


def _atr_of_slice(slc):
    if not slc:
        return 0
    total = 0
    n = 0
    for b in slc:
        h = b.get('h'); l = b.get('l')
        if h is None or l is None:
            continue
        total += max(h - l, 0)
        n += 1
    return total / n if n else 0


def _walk_outcome(m15, i, n_m15, entry, stop, target, ew, fib_entry, fib_target):
    """Forward walk a single setup. Returns dict with wick & fib outcomes."""
    lift_done = False
    wick_t = False; wick_o = 'expired'
    fib_t = False; fib_o = 'expired'

    for j in range(i + 1, min(i + 1 + EXPIRY + 32, n_m15)):
        b = m15[j]
        if not lift_done:
            if ew == 'bull' and b['h'] >= m15[i]['h']:
                lift_done = True
            elif ew == 'bear' and b['l'] <= m15[i]['l']:
                lift_done = True

        if lift_done:
            if not fib_t and fib_target is not None:
                if ((ew == 'bear' and b['h'] >= fib_entry) or
                        (ew == 'bull' and b['l'] <= fib_entry)):
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
                reach = ((ew == 'bull' and b['l'] <= entry) or
                         (ew == 'bear' and b['h'] >= entry))
                if reach:
                    wick_t = True
                    for jj in range(j, min(j + 32, n_m15)):
                        bb = m15[jj]
                        if ew == 'bull':
                            if bb['l'] <= stop:
                                wick_o = 'loss'; break
                            if bb['h'] >= target:
                                wick_o = 'win'; break
                        else:
                            if bb['h'] >= stop:
                                wick_o = 'loss'; break
                            if bb['l'] <= target:
                                wick_o = 'win'; break
                    break
        if j - i > EXPIRY and not wick_t and not fib_t:
            break
    return {'wick': wick_o, 'fib': fib_o}


def find_setups_dual(pair_data, pair_key, auto_ew_dirs):
    """Walk 4/4 setups, recording outcomes for BOTH old and capped rules
    (wick + fib). Borrows the validated setup-detection from
    backtest_wick_vs_fib_autoew.py."""
    m15 = _bars_norm(pair_data.get('m15', []))
    h1 = _bars_norm(pair_data.get('h1', []))
    daily = _bars_norm(pair_data.get('daily', []))
    if len(m15) < 100 or len(h1) < TL_LB + 5 or len(daily) < EW_LB + 5:
        return []
    for b in m15:
        if '_ts' not in b:
            import datetime as _dt
            b['_ts'] = _dt.datetime.fromisoformat(
                b['t'].replace('Z', '+00:00')).timestamp()
    for b in h1:
        if '_ts' not in b:
            import datetime as _dt
            b['_ts'] = _dt.datetime.fromisoformat(
                b['t'].replace('Z', '+00:00')).timestamp()
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
            stop_old = _find_struct_high(bos_slc, prom)
            if stop_old <= entry:
                continue
            R_old = stop_old - entry
        else:
            entry = m15[i]['l']
            stop_old = _find_struct_low(bos_slc, prom)
            if stop_old >= entry:
                continue
            R_old = entry - stop_old

        atr_slc = m15[max(0, i - 20):i]
        atr20 = _atr_of_slice(atr_slc) if len(atr_slc) >= 14 else 0
        if atr20 > 0 and R_old < 0.5 * atr20:
            continue

        rsi_at = rsi_arr[h1_idx] if h1_idx < len(rsi_arr) else None
        if rsi_at is not None:
            if ew == 'bull' and rsi_at >= gate['hi']:
                continue
            if ew == 'bear' and rsi_at <= gate['lo']:
                continue

        # Build OLD rule levels (no cap)
        if ew == 'bear':
            target_old = entry - R_old
        else:
            target_old = entry + R_old

        # Build NEW rule levels (cap stop distance at 2.5 * ATR(20))
        R_new = R_old
        stop_new = stop_old
        max_R = MAX_R_ATR_MULT * atr20 if atr20 > 0 else 0
        was_capped = False
        pair_cls = PAIR_CLASS.get(pair_key)
        # Production only caps comm + index — match that filter so the
        # comparison shows the actual behaviour of the deployed rule.
        if pair_cls in CAP_CLASSES and max_R > 0 and R_old > max_R:
            R_new = max_R
            was_capped = True
            if ew == 'bear':
                stop_new = entry + R_new
            else:
                stop_new = entry - R_new
        if ew == 'bear':
            target_new = entry - R_new
        else:
            target_new = entry + R_new

        # Fib entries (same for both rules — derived from creator candle).
        # Their R uses the rule-specific stop, so fib_R / fib_target differ.
        creator_h = m15[i]['h']; creator_l = m15[i]['l']
        creator_range = max(creator_h - creator_l, 1e-9)
        if ew == 'bear':
            fib_entry = creator_l + creator_range * 0.382
        else:
            fib_entry = creator_h - creator_range * 0.382

        def _fib_levels(stop):
            if ew == 'bear':
                fib_R = stop - fib_entry
                ft = fib_entry - fib_R if fib_R > 0 else None
            else:
                fib_R = fib_entry - stop
                ft = fib_entry + fib_R if fib_R > 0 else None
            return ft

        fib_target_old = _fib_levels(stop_old)
        fib_target_new = _fib_levels(stop_new)

        # Walk forward twice — once per rule set
        out_old = _walk_outcome(m15, i, n_m15, entry, stop_old, target_old,
                                ew, fib_entry, fib_target_old)
        out_new = _walk_outcome(m15, i, n_m15, entry, stop_new, target_new,
                                ew, fib_entry, fib_target_new)

        setups.append({
            'pair': pair_key,
            'ew': ew,
            'R_old_pips': R_old,
            'R_new_pips': R_new,
            'capped': was_capped,
            'wick_old': out_old['wick'],
            'fib_old': out_old['fib'],
            'wick_new': out_new['wick'],
            'fib_new': out_new['fib'],
        })
        last_resolved = i + EXPIRY
    return setups


def tally(setups, path):
    w = sum(1 for s in setups if s[path] == 'win')
    l = sum(1 for s in setups if s[path] == 'loss')
    e = sum(1 for s in setups if s[path] == 'expired')
    decided = w + l
    return {'n': len(setups), 'w': w, 'l': l, 'e': e,
            'decided': decided,
            'wr': (w / decided * 100) if decided else None}


def fmt_wr(t):
    if t['wr'] is None:
        return ' —    '
    return f"{t['wr']:5.1f}%"


def main():
    with open(HIST_PATH) as f:
        d = json.load(f)
    pairs = d.get('pairs', {})
    targets = sorted([k for k in pairs.keys()
                      if k not in DROPPED and k in PAIR_CLASS and k != 'dxy'])

    print(f'Backtesting max-R cap (MAX_R_ATR_MULT={MAX_R_ATR_MULT}) over '
          f'{len(targets)} pairs...', file=sys.stderr)
    t0 = time.time()
    results = {}
    for k in targets:
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
        meth = production_method(k)
        old_key = f'{meth}_old'
        new_key = f'{meth}_new'
        old_t = tally(setups, old_key)
        new_t = tally(setups, new_key)
        capped = sum(1 for s in setups if s['capped'])
        d_str = (f'+{new_t["wr"] - old_t["wr"]:+.1f}pp'
                 if old_t['wr'] is not None and new_t['wr'] is not None else '—')
        print(f'  {k:<8} setups={len(setups):>3} capped={capped:>3} '
              f'  {meth}_old WR={fmt_wr(old_t)} (n={old_t["decided"]:>3})  '
              f'{meth}_new WR={fmt_wr(new_t)} (n={new_t["decided"]:>3})  '
              f'delta={d_str}',
              file=sys.stderr)
    print(f'\nTotal walk took {time.time() - t0:.1f}s\n', file=sys.stderr)

    # Aggregate per class
    print('=' * 116)
    print(f'MAX-R CAP IMPACT — production methodology only (wick on FX/crypto, fib on comm/index)')
    print(f'  cap rule: R_new = min(R_old, {MAX_R_ATR_MULT} x ATR20)   '
          f'min-R floor unchanged (0.5xATR + 12pip FX)')
    print('=' * 116)

    print()
    print(f'{"pair":<10}{"class":<8}{"meth":<6}{"setups":>7}'
          f'{"capped":>8}{"old_W/L":>10}{"old_WR":>9}'
          f'{"new_W/L":>10}{"new_WR":>9}{"delta_WR":>10}'
          f'{"old_R_med":>11}{"new_R_med":>11}')
    print('-' * 116)

    import statistics
    class_agg = {}
    for k in sorted(results.keys()):
        setups = results[k]
        cls = PAIR_CLASS.get(k, '?')
        meth = production_method(k)
        old_t = tally(setups, f'{meth}_old')
        new_t = tally(setups, f'{meth}_new')
        capped = sum(1 for s in setups if s['capped'])
        delta = (new_t['wr'] - old_t['wr']
                 if old_t['wr'] is not None and new_t['wr'] is not None
                 else None)
        med_old = statistics.median([s['R_old_pips'] for s in setups])
        med_new = statistics.median([s['R_new_pips'] for s in setups])
        old_wr_s = fmt_wr(old_t)
        new_wr_s = fmt_wr(new_t)
        delta_s = f'{delta:+.1f}pp' if delta is not None else '—'
        print(f'{k:<10}{cls:<8}{meth:<6}{len(setups):>7}{capped:>8}'
              f'{old_t["w"]:>3}/{old_t["l"]:<6}{old_wr_s:>9}'
              f'{new_t["w"]:>3}/{new_t["l"]:<6}{new_wr_s:>9}'
              f'{delta_s:>10}{med_old:>11.5f}{med_new:>11.5f}')
        agg = class_agg.setdefault(cls, {'old_w':0,'old_l':0,'new_w':0,'new_l':0,'setups':0,'capped':0})
        agg['old_w'] += old_t['w']; agg['old_l'] += old_t['l']
        agg['new_w'] += new_t['w']; agg['new_l'] += new_t['l']
        agg['setups'] += len(setups); agg['capped'] += capped

    print()
    print('=' * 116)
    print('AGGREGATE BY CLASS')
    print('=' * 116)
    total_old_w = total_old_l = total_new_w = total_new_l = 0
    for cls, agg in sorted(class_agg.items()):
        old_d = agg['old_w'] + agg['old_l']
        new_d = agg['new_w'] + agg['new_l']
        old_wr = agg['old_w'] / old_d * 100 if old_d else None
        new_wr = agg['new_w'] / new_d * 100 if new_d else None
        delta = (new_wr - old_wr) if (old_wr is not None and new_wr is not None) else None
        delta_s = f'{delta:+.1f}pp' if delta is not None else '—'
        old_s = f'{old_wr:.1f}%' if old_wr is not None else '—'
        new_s = f'{new_wr:.1f}%' if new_wr is not None else '—'
        cap_pct = (agg['capped'] / agg['setups'] * 100) if agg['setups'] else 0
        print(f'  {cls:<8} setups={agg["setups"]:>5}  capped={agg["capped"]:>4} ({cap_pct:4.1f}%)  '
              f'old {agg["old_w"]:>3}W/{agg["old_l"]:>3}L WR={old_s:>7}  '
              f'new {agg["new_w"]:>3}W/{agg["new_l"]:>3}L WR={new_s:>7}  '
              f'delta={delta_s}')
        total_old_w += agg['old_w']; total_old_l += agg['old_l']
        total_new_w += agg['new_w']; total_new_l += agg['new_l']

    print('-' * 116)
    old_total = total_old_w + total_old_l
    new_total = total_new_w + total_new_l
    old_agg_wr = total_old_w / old_total * 100 if old_total else None
    new_agg_wr = total_new_w / new_total * 100 if new_total else None
    delta_agg = new_agg_wr - old_agg_wr if (old_agg_wr is not None and new_agg_wr is not None) else None
    print(f'  OVERALL  old {total_old_w}W/{total_old_l}L  WR={old_agg_wr:.1f}% '
          f'  new {total_new_w}W/{total_new_l}L  WR={new_agg_wr:.1f}% '
          f'  delta={delta_agg:+.1f}pp')


if __name__ == '__main__':
    main()
