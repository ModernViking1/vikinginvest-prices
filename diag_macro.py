"""Does tightening the MACRO read help? At 4/4 the structural EW is already
aligned, so the only macro lever left is READ QUALITY: require a high-confidence
auto-EW *pattern* (>=AUTO_EW_MIN_CONFIDENCE, valid pattern) in the trade
direction, not just a structural daily break.

For each 4/4 comm+crypto setup we compute auto_detect_ew on the honest daily
slice (completed bars only) and bucket the realistic-fill outcome by whether a
high-conf auto-EW pattern AGREES with the trade. Reported per time-half so any
lift must survive out-of-sample.
"""
import json, bisect
from detect_triggers import (
    RSI_GATE_BY_CLASS, PAIR_CLASS, auto_detect_ew,
    AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import (
    _bars_norm, _min_prom, precompute_break_dirs, precompute_cl_dir,
    precompute_rsi, _find_struct_high, _find_struct_low,
)
from backtest_school_run_full import classify_setup

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB, TL_LB, EW_LB, BOS_LB, WALK, EXPIRY, MIN_STOP_REL = 5, 8, 8, 24, 48, 3, 0.0015
LIVE = ('comm', 'crypto')


def walk(m15, s, e, st, tg, d):
    if abs(e - st) <= 0: return None
    for j in range(s, min(s + WALK, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= st: return -1.0
            if b['h'] >= tg: return 1.0
        else:
            if b['h'] >= st: return -1.0
            if b['l'] <= tg: return 1.0
    return None


def collect(pd, pk, out):
    cls = PAIR_CLASS.get(pk)
    if cls not in LIVE: return
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', [])); daily = _bars_norm(pd.get('daily', []))
    draw = pd.get('daily', [])   # raw daily dicts for auto_detect_ew
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    gate = RSI_GATE_BY_CLASS.get(cls, {'hi': 80, 'lo': 20})
    ew_cache = {}
    def auto_ew_at(dd):
        if dd not in ew_cache:
            try:
                r = auto_detect_ew(draw[:dd + 1])
                ewp = r.get('ew') if r.get('ok') else None
                if ewp and ewp.get('dir') in ('bull', 'bear') and ewp.get('confidence', 0) >= AUTO_EW_MIN_CONFIDENCE and ewp.get('pattern') in AUTO_EW_VALID_PATTERNS:
                    ew_cache[dd] = ewp['dir']
                else:
                    ew_cache[dd] = None
            except Exception:
                ew_cache[dd] = None
        return ew_cache[dd]
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last: continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < TL_LB or dd < EW_LB: continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        if None in (ew, tl, nw, cl): continue
        conf, d = classify_setup(ew, tl, nw, cl)
        if conf != 4 or d is None: continue
        lb = m15[max(0, i - 8):i]
        if len(lb) < 5: continue
        shi = max(b['h'] for b in lb); slo = min(b['l'] for b in lb); mp = _min_prom(m15[i]['c'])
        if d == 'bull':
            if not (m15[i]['c'] > shi and (m15[i]['c'] - shi) >= mp): continue
        else:
            if not (m15[i]['c'] < slo and (slo - m15[i]['c']) >= mp): continue
        bos = m15[max(0, i - BOS_LB):i]; prom = _min_prom(m15[i]['c'])
        if d == 'bear':
            entry = m15[i]['h']; stop = _find_struct_high(bos, prom)
            if stop <= entry: continue
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos, prom)
            if stop >= entry: continue
        atr = m15[max(0, i - 20):i]
        if len(atr) < 14: continue
        a20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
        if (stop - entry if d == 'bear' else entry - stop) < 0.5 * a20: continue
        rv = rsi_arr[h] if h < len(rsi_arr) else None
        if rv is not None:
            if d == 'bull' and rv >= gate['hi']: continue
            if d == 'bear' and rv <= gate['lo']: continue
        S = abs(entry - stop); stop_frac = S / abs(entry) if entry else 0
        if stop_frac < MIN_STOP_REL:
            last = i + WALK; continue
        target = entry + S if d == 'bull' else entry - S
        fj = None
        for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
            b = m15[j]
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                fj = j; break
        if fj is None: continue
        o = walk(m15, fj + 1, entry, stop, target, d)
        if o is None:
            last = i + WALK; continue
        aew = auto_ew_at(dd)
        agree = (aew == d)                 # high-conf auto-EW pattern agrees with trade
        out.append({'ts': ts, 'o': o, 'aew_agree': agree, 'aew_present': aew is not None})
        last = i + 1


def wr(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def show(label, rows):
    n, w, e = wr([r['o'] for r in rows])
    print(f"  {label:<34} n={n:>4}  WR={w:>5.1f}%  exp={e:>+.3f}R")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    rows = []
    for p in [x for x in PAIR_CLASS if x in pairs]: collect(pairs[p], p, rows)
    rows.sort(key=lambda r: r['ts'])
    mid = len(rows) // 2
    frac = sum(1 for r in rows if r['aew_agree']) / max(1, len(rows))
    print(f"total 4/4 comm+crypto: {len(rows)}   ·   high-conf auto-EW agrees on {100*frac:.0f}%\n")
    for name, half in [('FIRST half', rows[:mid]), ('SECOND half', rows[mid:])]:
        print(f"{name}:")
        show("baseline (all 4/4)", half)
        show("auto-EW pattern AGREES", [r for r in half if r['aew_agree']])
        show("auto-EW absent / disagrees", [r for r in half if not r['aew_agree']])
        print()


if __name__ == '__main__':
    main()
