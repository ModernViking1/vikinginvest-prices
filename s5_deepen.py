"""Deepen Strategy 5 (Multi-Timeframe Confluence) — the second setup to clear the
OOS bar. Same rigor as the H&S deepening: rolling walk-forward (6 folds),
per-class breakdown, ADX-filter robustness, and trade cadence. Reuses the
detector primitives from five_strategies_research.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import (
    ema, atr, adx, agg4h, weekly, walk, cost, is_engulf, HOLD,
)

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RRS = [2.0, 3.0]


def collect_s5(daily, b4, wk, cls, adx_min):
    if len(wk) < 12 or len(b4) < 250:
        return []
    wc = [b['c'] for b in wk]; we20 = ema(wc, 10)
    dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
    d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
    out = []; last = -1
    for i in range(2, len(b4) - 1):
        if i <= last: continue
        ts = b4[i]['_ts']
        di = bisect.bisect_right(d_ts, ts) - 1; wi = bisect.bisect_right(w_ts, ts) - 1
        if di < 51 or wi < 11 or we20[wi] is None or de50[di] is None: continue
        wk_up = wc[wi] > we20[wi] and we20[wi] > we20[wi-1]
        wk_dn = wc[wi] < we20[wi] and we20[wi] < we20[wi-1]
        a = atr(daily, 14, di)
        near50 = a is not None and abs(daily[di]['c'] - de50[di]) <= 0.5 * a
        if adx_min > 0:
            av = adx(b4, 14, i)
            if av is None or av < adx_min: continue
        d = None; stop = None
        if wk_up and near50 and is_engulf(b4, i, 'bull'):
            d = 'bull'; stop = min(b4[i]['l'], b4[i-1]['l'])
        elif wk_dn and near50 and is_engulf(b4, i, 'bear'):
            d = 'bear'; stop = max(b4[i]['h'], b4[i-1]['h'])
        if d is None: continue
        entry = b4[i+1]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        R = abs(entry - stop)
        row = {'ts': ts, 'cls': cls}
        for rr in RRS:
            o = walk(b4, i+1, entry, stop, d, rr, HOLD['4h'])
            row[rr] = (o - cost(o, entry, R)) if o is not None else None
        out.append(row); last = i + 1
    return out


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    def build(adx_min):
        trades = []
        span = [1e18, 0]
        for pk in [x for x in PAIR_CLASS if x in pairs]:
            h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
            if len(h1) < 400 or len(daily) < 80: continue
            rows = collect_s5(daily, agg4h(h1), weekly(daily), PAIR_CLASS.get(pk), adx_min)
            trades += rows
            for r in rows:
                span[0] = min(span[0], r['ts']); span[1] = max(span[1], r['ts'])
        trades.sort(key=lambda t: t['ts'])
        return trades, span

    trades, span = build(22)
    weeks = (span[1] - span[0]) / (7*86400) if trades else 1
    print(f"Strategy 5 (ADX>22): {len(trades)} trades over {weeks:.0f} weeks = {len(trades)/weeks:.1f}/week\n")

    print("ROLLING WALK-FORWARD (6 folds):")
    import datetime
    def dt(ms): return datetime.datetime.fromtimestamp(ms, datetime.timezone.utc).strftime('%m-%d')
    K = 6; n = len(trades); fold = n // K; pos = defaultdict(int)
    print(f"  {'fold':>4} {'period':>13} | {'RR2 exp/WR':>14} | {'RR3 exp/WR':>14}")
    for f in range(K):
        seg = trades[f*fold:(f+1)*fold if f < K-1 else n]
        if not seg: continue
        cells = []
        for rr in RRS:
            k, w, e = agg([t[rr] for t in seg])
            cells.append(f"{e:>+6.3f}/{w:>4.1f}%")
            if e > 0: pos[rr] += 1
        print(f"  {f+1:>4} {dt(seg[0]['ts'])+'..'+dt(seg[-1]['ts']):>13} | {cells[0]:>14} | {cells[1]:>14}")
    print(f"  folds positive: RR2={pos[2.0]}/{K}  RR3={pos[3.0]}/{K}")

    print("\nPER-CLASS (RR2 / RR3):")
    byc = defaultdict(list)
    for t in trades: byc[t['cls']].append(t)
    for c in sorted(byc):
        k2, w2, e2 = agg([t[2.0] for t in byc[c]]); _, w3, e3 = agg([t[3.0] for t in byc[c]])
        print(f"  {c:<8} n={k2:>4}  RR2 {e2:>+6.3f}/{w2:>4.1f}%   RR3 {e3:>+6.3f}/{w3:>4.1f}%")

    print("\nADX-FILTER ROBUSTNESS (RR2 exp):")
    for am in (0, 20, 22, 25, 30):
        tr, _ = build(am)
        _, w, e = agg([t[2.0] for t in tr])
        print(f"  ADX>{am:<3} n={len(tr):>4}  WR={w:>4.1f}%  exp={e:>+.3f}")


if __name__ == '__main__':
    main()
