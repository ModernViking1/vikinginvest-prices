"""Same entry-fill reality check, applied to the STRUCTURAL 4/4 setup (the 82%
headline family the user's '#1 gate to 4/4' targets). If this ALSO collapses to
~50% under a realistic fill, then gating to 4/4 cannot close the live gap — the
edge is the fill assumption, not the confluence.
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
EXPIRY = 3


def walk(m15, start, entry, stop, target, d):
    if abs(entry - stop) <= 0: return None
    for j in range(start, min(start + WALK, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return 1.0
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return 1.0
    return None


def run(pd, pk, acc, only_conf4):
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
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < TL_LB or dd < EW_LB: continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        if None in (ew, tl, nw, cl): continue
        conf, d = classify_setup(ew, tl, nw, cl)
        if conf < 2 or d is None: continue
        if only_conf4 and conf != 4: continue
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
            target = entry - (stop - entry)
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos, prom)
            if stop >= entry: continue
            target = entry + (entry - stop)
        atr = m15[max(0, i - 20):i]
        if len(atr) >= 14:
            a20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
            fxf = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if (stop - entry if d == 'bear' else entry - stop) < max(0.5 * a20, fxf): continue
        rv = rsi_arr[h] if h < len(rsi_arr) else None
        if rv is not None:
            if d == 'bull' and rv >= gate['hi']: continue
            if d == 'bear' and rv <= gate['lo']: continue

        acc['ideal'].append(walk(m15, i + 1, entry, stop, target, d))
        if i + 1 < len(m15):
            mo = m15[i + 1]['o']; mrr = (mo - stop) if d == 'bull' else (stop - mo)
            if mrr > 0:
                mt = mo + mrr if d == 'bull' else mo - mrr
                acc['market'].append(walk(m15, i + 2, mo, stop, mt, d))
        fill_j = None
        for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
            b = m15[j]
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                fill_j = j; break
        if fill_j is not None:
            acc['limit'].append(walk(m15, fill_j + 1, entry, stop, target, d))
        else:
            acc['limit_nofill'] += 1
        last = i + 1


def summ(name, arr):
    r = [x for x in arr if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    print(f"  {name:<8} decided={n:5}  WR={100*w/max(1,n):5.1f}%  exp={sum(r)/max(1,n):+.3f}R")


def main():
    import sys
    only4 = '--conf4' in sys.argv
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    acc = defaultdict(list); acc['limit_nofill'] = 0
    for p in [x for x in PAIR_CLASS if x in pairs]: run(pairs[p], p, acc, only4)
    print(f"STRUCTURAL setup {'(conf==4 only)' if only4 else '(conf>=2)'} — entry-model comparison:")
    summ('IDEAL', acc['ideal'])
    summ('MARKET', acc['market'])
    summ('LIMIT', acc['limit'])
    print(f"  LIMIT never filled within {EXPIRY} bars: {acc['limit_nofill']}")


if __name__ == '__main__':
    main()
