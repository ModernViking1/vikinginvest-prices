"""Honest path-accurate backtest of the MACD-PRIMARY cross (the live 'macdp'
signal that is 63% of live volume at 43.5% WR), bucketed by confluence.

Decides implementation of '#1 gate to 4/4':
  - if macdp@4/4 has a real edge  -> tighten macdp gate to confluence==4
  - if macdp@4/4 is still weak     -> disable macdp expansion; let the already
                                      4/4-gated structural signal carry live.

Replicates detect_macd_primary's trigger (12/26/9 cross, RSI centerline, HTF
cloud filter) over the series, computes EW/TL/NW/CL from the last CLOSED
higher-TF bars (honest), and forward-walks the 1:1 structural trade.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS, macd_series, MACDP_HTF_FILTER, _htf_blocks, _stop_too_tight
from backtest_rsi_per_class import (
    _bars_norm, precompute_break_dirs, precompute_cl_dir, precompute_rsi,
)

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB, TL_LB, EW_LB, WALK, STRUCT_LB = 5, 8, 8, 48, 8


def outcome(m15, i, entry, stop, target, d):
    if abs(entry - stop) <= 0: return None
    for j in range(i + 1, min(i + 1 + WALK, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return 1.0
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return 1.0
    return None


def run(pd, pk, buckets):
    cls = PAIR_CLASS.get(pk)
    if cls not in ('index', 'minor', 'major', 'comm', 'crypto'): return
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', [])); daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    h1_rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    closes = [b['c'] for b in m15]
    macd_line, sig_line = macd_series(closes, 12, 26, 9)
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last: continue
        m0, m1, s0, s1 = macd_line[i-1], macd_line[i], sig_line[i-1], sig_line[i]
        if None in (m0, m1, s0, s1): continue
        if m0 <= s0 and m1 > s1: d = 'bull'
        elif m0 >= s0 and m1 < s1: d = 'bear'
        else: continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2      # honest last CLOSED h1
        dd = bisect.bisect_right(d_ts, ts) - 2      # honest last CLOSED daily
        if h < TL_LB or dd < EW_LB: continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        if _htf_blocks(d, cl, enabled=MACDP_HTF_FILTER): continue
        r = h1_rsi_arr[h] if h < len(h1_rsi_arr) else None
        if r is None: continue
        if d == 'bull' and r >= 50: continue
        if d == 'bear' and r <= 50: continue
        conf = sum(1 for layer in (ew, tl, nw, cl) if layer == d)
        # structural entry/stop/target
        ss = m15[max(0, i - STRUCT_LB):i]
        if d == 'bull':
            entry = m15[i]['l']; stop = min((b['l'] for b in ss), default=None)
            if stop is None or stop >= entry: continue
            rr = entry - stop
        else:
            entry = m15[i]['h']; stop = max((b['h'] for b in ss), default=None)
            if stop is None or stop <= entry: continue
            rr = stop - entry
        if rr <= 0 or _stop_too_tight(rr, entry, cls): continue
        target = entry + rr if d == 'bull' else entry - rr
        o = outcome(m15, i, entry, stop, target, d)
        if o is None: last = i + WALK; continue
        buckets[conf].append(o); buckets['cls:%s:%d' % (cls, conf)].append(o)
        last = i + 1


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    buckets = defaultdict(list)
    for p in [x for x in PAIR_CLASS if x in pairs]: run(pairs[p], p, buckets)
    print("MACD-PRIMARY cross — honest closed-bar indexing — WR by confluence:")
    print(f"{'conf':>5} {'n':>6} {'WR%':>7} {'expR':>8}")
    for c in (0, 1, 2, 3, 4):
        o = buckets[c]
        if o:
            print(f"{c:>5} {len(o):>6} {100*sum(1 for x in o if x>0)/len(o):>6.1f}% {sum(o)/len(o):>+8.3f}")
    live = buckets[1] + buckets[2] + buckets[3] + buckets[4]
    g4 = buckets[4]
    print(f"\ncurrent gate (conf>=1): n={len(live)} WR={100*sum(1 for x in live if x>0)/max(1,len(live)):.1f}% exp={sum(live)/max(1,len(live)):+.3f}")
    print(f"proposed gate (conf==4): n={len(g4)} WR={100*sum(1 for x in g4 if x>0)/max(1,len(g4)):.1f}% exp={sum(g4)/max(1,len(g4)):+.3f}")


if __name__ == '__main__':
    main()
