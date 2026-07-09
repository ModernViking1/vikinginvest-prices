"""Combined change validation: STRUCTURAL 4/4 gate + realistic LIMIT fill +
minimum-R (cost-per-R) floor, with a cost model calibrated from live executions.

Cost is ~fixed in PRICE (spread+commission+slippage), so a larger absolute stop
dilutes it. Live-calibrated (median, swap-outliers removed):
    winner cost ~0.0045% of price   loser cost ~0.0105% of price
We also report a CONSERVATIVE case at 2x those constants, so the floor
recommendation is robust to cost uncertainty and cross-class spread variation.

Sweep: minimum stop distance as % of entry price. For each floor, report trade
count, WR, gross expectancy, and NET expectancy (central + conservative cost).
1:1 target throughout (the RR sweep showed wider targets don't help).
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
WIN_COST_PCT = 0.0045 / 100      # fraction of price
LOSS_COST_PCT = 0.0105 / 100
FLOORS = [0.0, 0.0010, 0.0015, 0.0020, 0.0030, 0.0040, 0.0050]  # min stop as frac of price


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


def run(pd, pk, trades, by_class):
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
        if conf != 4 or d is None: continue          # 4/4 GATE
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
        S = abs(entry - stop)
        stop_frac = S / abs(entry) if entry else 0
        target = entry + S if d == 'bull' else entry - S
        # realistic limit fill
        fill_j = None
        for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
            b = m15[j]
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                fill_j = j; break
        if fill_j is None:
            continue
        o = walk(m15, fill_j + 1, entry, stop, target, d)
        if o is None:
            last = i + WALK; continue
        trades.append((stop_frac, o))
        by_class[cls].append((stop_frac, o))
        last = i + 1


def cost_R(o, stop_frac, mult):
    if stop_frac <= 0: return 0.0
    c_price = (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) * mult
    return c_price / stop_frac


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    trades = []; by_class = defaultdict(list)
    for p in [x for x in PAIR_CLASS if x in pairs]: run(pairs[p], p, trades, by_class)
    print("STRUCTURAL 4/4 gate + LIMIT fill + min-stop floor — net expectancy")
    print(f"{'floor%':>8} {'n':>6} {'WR%':>6} {'grossR':>8} {'net(central)':>13} {'net(2x cost)':>13}")
    for f in FLOORS:
        sub = [(sf, o) for (sf, o) in trades if sf >= f]
        n = len(sub)
        if not n: continue
        gross = sum(o for _, o in sub) / n
        net1 = sum(o - cost_R(o, sf, 1.0) for sf, o in sub) / n
        net2 = sum(o - cost_R(o, sf, 2.0) for sf, o in sub) / n
        wr = 100 * sum(1 for _, o in sub if o > 0) / n
        print(f"{f*100:>7.2f}% {n:>6} {wr:>5.1f}% {gross:>+8.3f} {net1:>+13.3f} {net2:>+13.3f}")
    for FL in (0.0, 0.0020):
        print(f"\nPer-class @ floor {FL*100:.2f}% (net central / net 2x):")
        for cls in sorted(by_class):
            sub = [(sf, o) for (sf, o) in by_class[cls] if sf >= FL]
            n = len(sub)
            if not n: continue
            wr = 100 * sum(1 for _, o in sub if o > 0) / n
            net1 = sum(o - cost_R(o, sf, 1.0) for sf, o in sub) / n
            net2 = sum(o - cost_R(o, sf, 2.0) for sf, o in sub) / n
            print(f"  {cls:<7} n={n:>4} WR={wr:>5.1f}% net={net1:>+.3f} / {net2:>+.3f}")


if __name__ == '__main__':
    main()
