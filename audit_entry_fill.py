"""Is the live-vs-backtest gap an ENTRY-FILL illusion?

The structural backtests assume you are filled at the cross/creator bar's exact
low (bull) / high (bear) and then ride to a 1:1 target. But that price is the
BEST price of the signal bar — live, the cBot places a LIMIT there with a 45-min
(3 x m15) expiry. Two things the idealized model ignores:

  1. Fill selection: the limit only fills if price trades back to it. Trades
     where price ran away (no pullback) are counted as wins in the idealized
     model but NEVER HAPPEN live. The ones that DO pull back are
     disproportionately the ones continuing to the stop.
  2. Expiry: if the level isn't touched within the expiry window, no trade.

This backtests the MACD-PRIMARY cross under three entry models on the same
signals:
  IDEAL    : filled at struct level, walk from i+1 (what the current backtest does)
  MARKET   : filled at next bar's open (a plain market order)
  LIMIT    : filled only if the struct level is traded within EXPIRY bars,
             from the fill bar; else no trade (the cBot's actual behavior)
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS, macd_series, MACDP_HTF_FILTER, _htf_blocks, _stop_too_tight
from backtest_rsi_per_class import _bars_norm, precompute_break_dirs, precompute_cl_dir, precompute_rsi

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB, TL_LB, EW_LB, WALK, STRUCT_LB = 5, 8, 8, 48, 8
EXPIRY = 3   # 45 min / 15 min = 3 m15 bars (cBot LimitExpiryMin=45)


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


def run(pd, pk, acc):
    cls = PAIR_CLASS.get(pk)
    if cls not in ('index', 'minor', 'major', 'comm', 'crypto'): return
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', [])); daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    h1_rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    macd_line, sig_line = macd_series([b['c'] for b in m15], 12, 26, 9)
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last: continue
        m0, m1, s0, s1 = macd_line[i-1], macd_line[i], sig_line[i-1], sig_line[i]
        if None in (m0, m1, s0, s1): continue
        if m0 <= s0 and m1 > s1: d = 'bull'
        elif m0 >= s0 and m1 < s1: d = 'bear'
        else: continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < TL_LB or dd < EW_LB: continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        if _htf_blocks(d, cl, enabled=MACDP_HTF_FILTER): continue
        r = h1_rsi_arr[h] if h < len(h1_rsi_arr) else None
        if r is None: continue
        if d == 'bull' and r >= 50: continue
        if d == 'bear' and r <= 50: continue
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

        # IDEAL — filled at struct level, walk from i+1
        acc['ideal'].append(walk(m15, i + 1, entry, stop, target, d))
        # MARKET — filled at next bar's open; keep same structural stop, 1:1 from fill
        if i + 1 < len(m15):
            mo = m15[i + 1]['o']
            mrr = (mo - stop) if d == 'bull' else (stop - mo)
            if mrr > 0:
                mt = mo + mrr if d == 'bull' else mo - mrr
                acc['market'].append(walk(m15, i + 2, mo, stop, mt, d))
        # LIMIT — fill only if struct level traded within EXPIRY bars, from fill bar
        fill_j = None
        for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
            b = m15[j]
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                fill_j = j; break
        if fill_j is not None:
            acc['limit'].append(walk(m15, fill_j + 1, entry, stop, target, d))
        else:
            acc['limit_nofill'] += 1


def summ(name, arr):
    r = [x for x in arr if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    print(f"  {name:<8} decided={n:5}  WR={100*w/max(1,n):5.1f}%  exp={sum(r)/max(1,n):+.3f}R")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    acc = defaultdict(list); acc['limit_nofill'] = 0
    for p in [x for x in PAIR_CLASS if x in pairs]: run(pairs[p], p, acc)
    print("MACD-PRIMARY cross — entry-model comparison (all confluence, honest HTF):")
    summ('IDEAL', acc['ideal'])
    summ('MARKET', acc['market'])
    summ('LIMIT', acc['limit'])
    print(f"  LIMIT never filled within {EXPIRY} bars (no trade): {acc['limit_nofill']}")
    print("\nLive reference (macdp): 43.5% WR")


if __name__ == '__main__':
    main()
