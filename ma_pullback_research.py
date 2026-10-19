"""9EMA / 20SMA pullback-continuation with a 200SMA trend filter — the strategy from the
screenshots. Mechanical version, across available timeframes and all asset classes, 3yr.

Setup (bull; bear mirrors), per the charts:
  - TREND: close > SMA200 and SMA20 > SMA200 (price in an uptrend above the 200SMA).
  - MOMENTUM: EMA9 > SMA20 (fast above slow).
  - PULLBACK: within the last PB bars price dipped to the fast MA (a low <= max(EMA9,SMA20)).
  - TRIGGER: the signal bar closes back above EMA9 and makes a higher high (resumption).
  - ENTRY: next bar open. STOP: pullback low - ATR buffer. TARGETS: fixed 1R / 2R / 3R.

Timeframes from screenshot 1 are intraday (1/2/5/15min, session-specific) — we only have m15
as the finest granularity, so TFs tested = m15, h1, 4h(agg), daily (where history exists).
Data windows: historical-ohlc daily=3yr, h1=1yr, m15=~3mo; equity broker m15/h1=3yr.

    python ma_pullback_research.py --hist historical-ohlc.json --equity <all_eq.json>
"""
import json, argparse
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, cost
import unified_shadow_harness as H
from unified_shadow_harness import _ema, score_trail_open, TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST
from equity_broker_backtest import market

PB = 5
BUF = 0.25
FAST_EMA = 9          # buy the retrace to 20SMA / 9EMA (per the chart indicators)
RRS = [1.0, 2.0, 3.0]
HOLD = {'m1': 60, 'm5': 48, 'm15': 40, 'h1': 60, '4h': 40, 'daily': 30}


def _sma(closes, p):
    n = len(closes); out = [None]*n; s = 0.0
    for i in range(n):
        s += closes[i]
        if i >= p: s -= closes[i-p]
        if i >= p-1: out[i] = s/p
    return out


def scan(bars):
    n = len(bars)
    if n < 230:
        return []
    closes = [b['c'] for b in bars]
    s20 = _sma(closes, 20); s200 = _sma(closes, 200); e9 = _ema(closes, FAST_EMA)
    out = []; last = -1
    for i in range(201, n-1):
        if None in (s20[i], s200[i], e9[i]):
            continue
        c = bars[i]['c']
        bull = c > s200[i] and s20[i] > s200[i] and e9[i] > s20[i]
        bear = c < s200[i] and s20[i] < s200[i] and e9[i] < s20[i]
        if not (bull or bear) or i <= last:
            continue
        lo, hi = i-PB, i
        if bull:
            pulled = any(bars[j]['l'] <= max(e9[j], s20[j]) for j in range(max(0, lo), hi+1))
            trig = c > e9[i] and bars[i]['h'] > bars[i-1]['h']
            if pulled and trig:
                entry = bars[i+1]['o']; a = atr(bars, 14, i) or 0.0
                stop = min(bars[j]['l'] for j in range(max(0, lo), hi+1)) - BUF*a
                if stop < entry:
                    out.append(('bull', i+1, entry, stop, bars[i+1]['_ts'])); last = i+2
        else:
            pulled = any(bars[j]['h'] >= min(e9[j], s20[j]) for j in range(max(0, lo), hi+1))
            trig = c < e9[i] and bars[i]['l'] < bars[i-1]['l']
            if pulled and trig:
                entry = bars[i+1]['o']; a = atr(bars, 14, i) or 0.0
                stop = max(bars[j]['h'] for j in range(max(0, lo), hi+1)) + BUF*a
                if stop > entry:
                    out.append(('bear', i+1, entry, stop, bars[i+1]['_ts'])); last = i+2
    return out


def walk(bars, i0, entry, stop, target, d, hold):
    R = abs(entry-stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return (target-entry)/R
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return (entry-target)/R
    return None


def run(bars, tf, cls, store):
    hold = HOLD[tf]
    for d, ei, entry, stop, ts in scan(bars):
        R = abs(entry-stop)
        for rr in RRS:
            tgt = entry + rr*R if d == 'bull' else entry - rr*R
            o = walk(bars, ei, entry, stop, tgt, d, hold)
            if o is None:
                continue
            store[(cls, tf, rr)].append((ts, o - cost(o, entry, R)))
        # TRAILING-runner exit — "ride the trend toward the 200SMA" (let winners run), the
        # equity book's native exit. This is the exit the strategy actually intends (not a fixed RR).
        st, o = score_trail_open(bars, ts, entry, stop, d, TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
        if st == 'resolved':
            store[(cls, tf, 'trail')].append((ts, o - cost(o, entry, R)))


def agg(rows):
    rows = sorted(rows); rs = [r for _, r in rows]; n = len(rs)
    if not n:
        return None
    m = n//2; w = sum(1 for r in rs if r > 0)
    return n, 100*w/n, sum(rs)/n, sum(rs), (sum(rs[:m])/m if m else 0), (sum(rs[m:])/(n-m) if n-m else 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='historical-ohlc.json')
    ap.add_argument('--equity', default='')
    ap.add_argument('--lowtf', default='', help='one-off equity-lowtf-ohlc.json (m5+m1) from VikingHistoryExportLowTF')
    args = ap.parse_args()
    store = defaultdict(list)

    if args.lowtf:          # one-off finer-TF test (separate file; not the live pipeline)
        from unified_shadow_harness import score_trail_open, TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST
        lt = json.load(open(args.lowtf)).get('pairs', {})
        lstore = defaultdict(list)
        for key, layers in lt.items():
            for tf in ('m5', 'm1'):
                bars = _bars_norm(layers.get(tf) or [])
                if len(bars) < 300:
                    continue
                for d, ei, entry, stop, ts in scan(bars):
                    R = abs(entry-stop)
                    st, o = score_trail_open(bars, ts, entry, stop, d, TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
                    if st == 'resolved':
                        lstore[(tf, 'trail')].append((ts, o - cost(o, entry, R)))
                    o2 = walk(bars, ei, entry, stop, entry + 2*R if d == 'bull' else entry - 2*R, d, HOLD[tf])
                    if o2 is not None:
                        lstore[(tf, 2.0)].append((ts, o2 - cost(o2, entry, R)))
        print(f"ma_pull on equity LOW TIMEFRAMES (one-off, {len(lt)} symbols) — trailing + 1:2\n")
        for tf in ('m5', 'm1'):
            for ex in ('trail', 2.0):
                a = agg(lstore[(tf, ex)])
                lbl = 'trail' if ex == 'trail' else '1:2'
                print(f"  {tf:<4} {lbl:<6} " + (f"n={a[0]:<6} WR={a[1]:.0f}% exp={a[2]:+.3f}R totR={a[3]:+.0f} OOS[{a[4]:+.2f}/{a[5]:+.2f}]" if a else "no data"))
        return

    pairs = json.load(open(args.hist)).get('pairs', {}) if args.hist.lower() not in ('', 'none') else {}
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        if not cls:
            continue
        h1 = _bars_norm(layers.get('h1') or [])
        daily = _bars_norm(layers.get('daily') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(daily) >= 230: run(daily, 'daily', cls, store)
        if len(h1) >= 230: run(h1, 'h1', cls, store);
        if len(h1) >= 900: run(agg4h(h1), '4h', cls, store)
        if len(m15) >= 230: run(m15, 'm15', cls, store)

    if args.equity:
        eq = json.load(open(args.equity)).get('pairs', {})
        for key, layers in eq.items():
            h1 = _bars_norm(layers.get('h1') or [])
            m15 = _bars_norm(layers.get('m15') or [])
            if len(h1) < 400:
                continue
            run(h1, 'h1', 'equity', store)
            run(agg4h(h1), '4h', 'equity', store)
            if len(m15) >= 400:
                run(m15, 'm15', 'equity', store)

    classes = ['major', 'minor', 'comm', 'index', 'crypto', 'equity']
    tfs = ['m15', 'h1', '4h', 'daily']
    print(f"{FAST_EMA}EMA/20SMA pullback + 200SMA trend filter (with-trend only) — WR% / expR by class x tf\n")
    # TRAILING exit first — the intended 'ride toward the 200SMA' exit
    print("===== TRAILING-runner exit (ride the trend) =====")
    print(f"  {'class':<8}" + "".join(f"{tf:>18}" for tf in tfs))
    for cls in classes:
        cells = []
        for tf in tfs:
            a = agg(store[(cls, tf, 'trail')])
            cells.append(f"{a[1]:.0f}%/{a[2]:+.2f}/n{a[0]}" if a else "—")
        print(f"  {cls:<8}" + "".join(f"{c:>18}" for c in cells))
    print()
    for rr in RRS:
        print(f"===== TARGET 1:{int(rr)} =====")
        print(f"  {'class':<8}" + "".join(f"{tf:>16}" for tf in tfs))
        for cls in classes:
            cells = []
            for tf in tfs:
                a = agg(store[(cls, tf, rr)])
                cells.append(f"{a[1]:.0f}%/{a[2]:+.2f}/n{a[0]}" if a else "—")
            print(f"  {cls:<8}" + "".join(f"{c:>16}" for c in cells))
        print()

    print("Best cells (exp>+0.05R, n>=40, both OOS halves +):")
    for (cls, tf, rr), rows in sorted(store.items(), key=lambda kv: str(kv[0])):
        a = agg(rows)
        if a and a[0] >= 40 and a[2] > 0.05 and a[4] > 0 and a[5] > 0:
            lbl = 'trail' if rr == 'trail' else f"1:{int(rr)}"
            print(f"  {cls:<7} {tf:<5} {lbl:<6} n={a[0]:<5} WR={a[1]:.0f}% exp={a[2]:+.3f}R totR={a[3]:+.0f} OOS[{a[4]:+.2f}/{a[5]:+.2f}]")


if __name__ == '__main__':
    main()
