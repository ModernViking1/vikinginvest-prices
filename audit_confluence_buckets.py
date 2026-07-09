"""Per-confluence WR of the STRUCTURAL setup (the 82% headline family), honest
closed-bar indexing. Answers: does a 4/4 gate actually hold up, and how often
does each bucket fire? This decides whether '#1 gate to 4/4' is the right move.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import RSI_GATE_BY_CLASS, PAIR_CLASS
from backtest_rsi_per_class import (
    _bars_norm, _min_prom, precompute_break_dirs, precompute_cl_dir,
    precompute_rsi, _find_struct_high, _find_struct_low,
)
from backtest_school_run_full import classify_setup

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB, TL_LB, EW_LB, BOS_LB, WALK = 5, 8, 8, 24, 48


def base_outcome(m15, i, entry, stop, target, direction):
    if abs(entry - stop) <= 0:
        return None
    for j in range(i + 1, min(i + 1 + WALK, len(m15))):
        b = m15[j]
        if direction == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return 1.0
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return 1.0
    return None


def run(pd, pk, buckets):
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', [])); daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pk), {'hi': 80, 'lo': 20}); cls = PAIR_CLASS.get(pk)
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last: continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 1 - 1     # honest: last CLOSED h1
        dd = bisect.bisect_right(d_ts, ts) - 1 - 1     # honest: last CLOSED daily
        if h < TL_LB or dd < EW_LB: continue
        ew = ew_arr[dd]; tl = tl_arr[h]; nw = nw_arr[i]; cl = cl_arr[h]
        if None in (ew, tl, nw, cl): continue
        conf, direction = classify_setup(ew, tl, nw, cl)
        if conf < 2 or direction is None: continue
        lb = m15[max(0, i - 8):i]
        if len(lb) < 5: continue
        shi = max(b['h'] for b in lb); slo = min(b['l'] for b in lb); mp = _min_prom(m15[i]['c'])
        if direction == 'bull':
            if not (m15[i]['c'] > shi and (m15[i]['c'] - shi) >= mp): continue
        else:
            if not (m15[i]['c'] < slo and (slo - m15[i]['c']) >= mp): continue
        bos = m15[max(0, i - BOS_LB):i]; prom = _min_prom(m15[i]['c'])
        if direction == 'bear':
            entry = m15[i]['h']; stop = _find_struct_high(bos, prom)
            if stop <= entry: continue
            target = entry - (stop - entry)
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos, prom)
            if stop >= entry: continue
            target = entry + (entry - stop)
        atr = m15[max(0, i - 20):i]
        if len(atr) >= 14:
            a20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
            fxf = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if (stop - entry if direction == 'bear' else entry - stop) < max(0.5 * a20, fxf): continue
        r = rsi_arr[h] if h < len(rsi_arr) else None
        if r is not None:
            if direction == 'bull' and r >= gate['hi']: continue
            if direction == 'bear' and r <= gate['lo']: continue
        o = base_outcome(m15, i, entry, stop, target, direction)
        if o is None: last = i + WALK; continue
        buckets[conf].append(o); buckets['cls:%s:%d' % (cls, conf)].append(o)
        last = i + 1


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    uni = [p for p in PAIR_CLASS if p in pairs]
    buckets = defaultdict(list)
    for p in uni: run(pairs[p], p, buckets)
    print("STRUCTURAL setup — honest closed-bar indexing — WR by confluence:")
    print(f"{'conf':>5} {'n':>6} {'WR%':>7} {'expR':>8}")
    tot = []
    for c in (2, 3, 4):
        o = buckets[c]; tot += o
        if o:
            print(f"{c:>5} {len(o):>6} {100*sum(1 for x in o if x>0)/len(o):>6.1f}% {sum(o)/len(o):>+8.3f}")
    if tot:
        print(f"{'2-4':>5} {len(tot):>6} {100*sum(1 for x in tot if x>0)/len(tot):>6.1f}% {sum(tot)/len(tot):>+8.3f}")
    print("\nPer-class 4/4-only (the strictest gate):")
    for key in sorted(k for k in buckets if isinstance(k, str) and k.endswith(':4')):
        o = buckets[key]
        if len(o) >= 10:
            print(f"  {key:<18} n={len(o):>4} WR={100*sum(1 for x in o if x>0)/len(o):.1f}% exp={sum(o)/len(o):+.3f}")


if __name__ == '__main__':
    main()
