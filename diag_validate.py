"""Out-of-sample discipline: do the candidate levers (avoid RSI centerline,
prefer high-ATR) survive a time split, or are they in-sample noise? This is the
check that was MISSING when the 74% over-fits were deployed.

Split the 4/4 comm+crypto setups chronologically into first half / second half.
A lever is credible only if it lifts WR in BOTH halves. Report baseline + each
lever per half with sample sizes.
"""
import json, bisect
from detect_triggers import RSI_GATE_BY_CLASS, PAIR_CLASS
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
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    gate = RSI_GATE_BY_CLASS.get(cls, {'hi': 80, 'lo': 20})
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
        fxf = 0
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
        out.append({'ts': ts, 'o': o, 'rsi': rv, 'atr_rel': a20 / abs(entry) if entry else None})
        last = i + 1


def wr(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def show(label, rows):
    n, w, e = wr([r['o'] for r in rows])
    print(f"  {label:<30} n={n:>4}  WR={w:>5.1f}%  exp={e:>+.3f}R")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    rows = []
    for p in [x for x in PAIR_CLASS if x in pairs]: collect(pairs[p], p, rows)
    rows.sort(key=lambda r: r['ts'])
    mid = len(rows) // 2
    halves = [('FIRST half (older)', rows[:mid]), ('SECOND half (newer)', rows[mid:])]
    # ATR-high threshold defined on FIRST half only (no peeking at second half)
    fvals = sorted(r['atr_rel'] for r in rows[:mid] if r['atr_rel'])
    atr_hi = fvals[len(fvals)//2] if fvals else 0    # median of first half = "high" cut
    print(f"ATR-high cut (median of FIRST half): {atr_hi*100:.3f}% of price\n")
    for name, half in halves:
        print(f"{name}:")
        show("baseline (all 4/4)", half)
        show("avoid RSI [45,55)", [r for r in half if not (r['rsi'] is not None and 45 <= r['rsi'] < 55)])
        show("ATR >= high-cut", [r for r in half if r['atr_rel'] and r['atr_rel'] >= atr_hi])
        show("avoid-centerline AND high-ATR", [r for r in half if r['atr_rel'] and r['atr_rel'] >= atr_hi and not (r['rsi'] is not None and 45 <= r['rsi'] < 55)])
        print()


if __name__ == '__main__':
    main()
