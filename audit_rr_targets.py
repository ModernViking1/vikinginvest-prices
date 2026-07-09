"""Can a wider reward:risk target make the 4/4 cohort profitable under a REALISTIC
(limit) fill? At ~50-53% raw hit rate a 1:1 target can't clear costs. This sweeps
target multiples on the structural 4/4 setup with the cBot's actual limit-fill
model, and nets an approximate cost per trade so we can see break-even.

Outcome model per trade: limit fills only if the struct level is touched within
EXPIRY bars; then walk the m15 path to stop (-1R) or the RR target (+RR R),
stop-checked before target within a bar (conservative).
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
NW_LB, TL_LB, EW_LB, BOS_LB, WALK = 5, 8, 8, 24, 96
EXPIRY = 3
RR_TARGETS = [1.0, 1.5, 2.0, 2.5, 3.0]
# approx round-trip cost as a fraction of R. Live executions showed commission
# + spread + swap materially eroding small-R intraday trades; ~0.10R is a
# conservative blended estimate across classes (indices/crypto wider, FX tighter).
COST_R = 0.10


def walk_rr(m15, start, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0: return None
    target = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(start, min(start + WALK, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return rr
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
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos, prom)
            if stop >= entry: continue
        atr = m15[max(0, i - 20):i]
        if len(atr) >= 14:
            a20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
            fxf = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if (stop - entry if d == 'bear' else entry - stop) < max(0.5 * a20, fxf): continue
        rv = rsi_arr[h] if h < len(rsi_arr) else None
        if rv is not None:
            if d == 'bull' and rv >= gate['hi']: continue
            if d == 'bear' and rv <= gate['lo']: continue
        # realistic limit fill
        fill_j = None
        for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
            b = m15[j]
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                fill_j = j; break
        if fill_j is None:
            continue
        for rr in RR_TARGETS:
            o = walk_rr(m15, fill_j + 1, entry, stop, d, rr)
            if o is not None:
                acc[rr].append(o)
        last = i + 1


def main():
    import sys
    only4 = '--conf4' in sys.argv
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    acc = defaultdict(list)
    for p in [x for x in PAIR_CLASS if x in pairs]: run(pairs[p], p, acc, only4)
    print(f"STRUCTURAL {'conf==4' if only4 else 'conf>=2'} — LIMIT fill — expectancy by RR target")
    print(f"(approx round-trip cost {COST_R:.2f}R/trade)")
    print(f"{'RR':>5} {'n':>6} {'WR%':>7} {'grossR':>9} {'netR':>9}")
    for rr in RR_TARGETS:
        o = acc[rr]; n = len(o)
        if not n: continue
        wr = 100 * sum(1 for x in o if x > 0) / n
        gross = sum(o) / n
        print(f"{rr:>5.1f} {n:>6} {wr:>6.1f}% {gross:>+9.3f} {gross - COST_R:>+9.3f}")


if __name__ == '__main__':
    main()
