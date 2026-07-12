"""Does the 15m 'no-wick' structural direction (m15 CHoCH break dir, the NW leg of
the intraday 4/4 stack) SUPPLEMENT the swing edges as an entry-timing confirmation?

For each swing edge (hs / s5_rsi) we bucket every historical trade by whether the
m15 structural break direction at entry AGREES with the trade direction, and
compare WR / expectancy at RR2 with a chronological OOS split. (Standalone is
already established: the m15 structural 4/4 setup is the entry-fill illusion —
82% ideal -> ~50% realistic -> 34-41% live.)
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, precompute_break_dirs
from unified_shadow_harness import detect_hs, detect_s5
from five_strategies_research import agg4h, walk, cost, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RR = 2.0
NW_LB = 5


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<26} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}  OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: {'aligned': [], 'not': [], 'neutral': []})
    frac = defaultdict(lambda: [0, 0])
    n_no_m15 = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', [])); draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80: continue
        if len(m15) < 100:
            n_no_m15 += 1; continue
        b4 = agg4h(h1); m15_ts = [b['_ts'] for b in m15]
        nw = precompute_break_dirs(m15, NW_LB)
        edges = {'hs': (detect_hs(pk, h1, daily, draw), h1, HS_HOLD),
                 's5_rsi': (detect_s5(pk, h1, daily, 'rsi'), b4, HOLD['4h'])}
        for edge, (sigs, bars, hold) in edges.items():
            bts = [b['_ts'] for b in bars]
            for s in sigs:
                i0 = bisect.bisect_left(bts, s['entry_ts'])
                if i0 >= len(bars): continue
                o = walk(bars, i0, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None: continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop']))
                mi = bisect.bisect_right(m15_ts, s['entry_ts']) - 1
                mdir = nw[mi] if 0 <= mi < len(nw) else None
                bucket = 'neutral' if mdir is None else ('aligned' if mdir == s['dir'] else 'not')
                store[edge][bucket].append((s['entry_ts'], r))
                frac[edge][0] += 1 if bucket == 'aligned' else 0; frac[edge][1] += 1

    if n_no_m15:
        print(f"(note: {n_no_m15} pairs had no usable m15 history — excluded)\n")
    print("15m no-wick (m15 structural break dir) as a supplement to the swing edges (RR2):\n")
    for edge in ('hs', 's5_rsi'):
        al, tot = frac[edge]
        print(f"{edge}  (m15 aligns on {100*al/max(1,tot):.0f}% of entries):")
        line("m15 ALIGNED", store[edge]['aligned'])
        line("m15 opposes", store[edge]['not'])
        line("m15 neutral/none", store[edge]['neutral'])
        base = store[edge]['aligned'] + store[edge]['not'] + store[edge]['neutral']
        line("BASELINE (all)", base)
        print()


if __name__ == '__main__':
    main()
