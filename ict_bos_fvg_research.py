"""ICT 'BOS + FVG entry' model (thetraderrichie: STRUCTURE -> FVG -> ENTER).

  1. BOS (break of structure): close breaks the most recent confirmed swing low
     (bearish) / swing high (bullish) -> directional bias.
  2. FVG (fair value gap): a 3-candle imbalance in the impulse leg after the BOS.
     Bearish FVG: bars[i-2].low > bars[i].high (gap = [bars[i].high, bars[i-2].low]).
     Bullish FVG: bars[i-2].high < bars[i].low.
  3. ENTER: price retraces back INTO the FVG; enter in the BOS direction. Stop
     just beyond the gap; target at RR (swept 1/1.5/2/3).

Realistic mechanics: the FVG entry is taken at the NEXT BAR OPEN after price first
trades into the gap (a market fill — NOT a limit at the FVG level, which would
re-create the entry-fill illusion that sank the intraday system). Invalidate if
price closes through the far side of the gap before retracing. No lookahead —
pivots confirmed +prd, FVG known at its 3rd bar, entry strictly after the retrace.

All pairs, m15/h1/4h/daily, realistic cost, chronological OOS split, per class.

Run: python ict_bos_fvg_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
PRD = 3                  # fractal half-width for structure swings
BOS_RECENCY = 12         # FVG must form within this many bars after the BOS
RETRACE_WIN = 20         # bars to wait for price to retrace into the FVG
ATR_BUF = 0.25
COOLDOWN = 5
RRS = [1.0, 1.5, 2.0, 3.0]
HOLD = {'m15': 60, 'h1': 48, '4h': 60, 'daily': 25}


def structure_state(bars):
    """Return (bos_dir[i], bos_bar[i]) — the most recent break-of-structure as of
    each bar, using swing pivots confirmed prd bars to their right (no lookahead)."""
    n = len(bars)
    pl = [(i + PRD, bars[i]['l']) for i in range(PRD, n - PRD)
          if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, PRD+1))]
    ph = [(i + PRD, bars[i]['h']) for i in range(PRD, n - PRD)
          if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, PRD+1))]
    pl.sort(); ph.sort()
    last_pl = [None]*n; last_ph = [None]*n
    a = 0
    for i in range(n):
        while a < len(pl) and pl[a][0] <= i:
            last_pl[i] = pl[a][1]; a += 1
        if i and last_pl[i] is None:
            last_pl[i] = last_pl[i-1]
    b = 0
    for i in range(n):
        while b < len(ph) and ph[b][0] <= i:
            last_ph[i] = ph[b][1]; b += 1
        if i and last_ph[i] is None:
            last_ph[i] = last_ph[i-1]
    bdir = [None]*n; bbar = [-10**9]*n
    cur_d, cur_b = None, -10**9
    for i in range(n):
        if last_pl[i] is not None and bars[i]['c'] < last_pl[i]:
            cur_d, cur_b = 'bear', i
        if last_ph[i] is not None and bars[i]['c'] > last_ph[i]:
            cur_d, cur_b = 'bull', i
        bdir[i], bbar[i] = cur_d, cur_b
    return bdir, bbar


def scan(bars, tf, store, cls, store_cls):
    n = len(bars); hold = HOLD[tf]
    bdir, bbar = structure_state(bars)
    last = -1
    for i in range(PRD + 2, n - 1):
        if i <= last:
            continue
        d = bdir[i]
        if d is None or (i - bbar[i]) > BOS_RECENCY or i - 2 < bbar[i]:
            continue
        # FVG aligned with the BOS direction, formed in the post-BOS impulse
        if d == 'bear':
            if not (bars[i-2]['l'] > bars[i]['h']):
                continue
            z_bot, z_top = bars[i]['h'], bars[i-2]['l']
        else:
            if not (bars[i-2]['h'] < bars[i]['l']):
                continue
            z_bot, z_top = bars[i-2]['h'], bars[i]['l']
        # wait for retrace INTO the gap
        ei = None
        for r in range(i + 1, min(i + 1 + RETRACE_WIN, n - 1)):
            b = bars[r]
            if d == 'bear':
                if b['c'] > z_top:          # closed through the top -> gap violated
                    break
                if b['h'] >= z_bot:         # traded up into the gap
                    ei = r + 1; break
            else:
                if b['c'] < z_bot:
                    break
                if b['l'] <= z_top:
                    ei = r + 1; break
        if ei is None or ei <= last or ei >= n:
            continue
        entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
        if d == 'bear':
            stop = z_top + ATR_BUF * a
            if stop <= entry:
                continue
        else:
            stop = z_bot - ATR_BUF * a
            if stop >= entry:
                continue
        wd = 'bear' if d == 'bear' else 'bull'
        R = abs(entry - stop); ts = bars[ei]['_ts']
        for rr in RRS:
            o = walk(bars, ei, entry, stop, wd, rr, hold)
            if o is not None:
                net = o - cost(o, entry, R)
                store[(tf, rr)].append((ts, net))
                store_cls[cls][(tf, rr)].append((ts, net))
        last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>5} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'m15': m15, 'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, store, cls, store_cls)

    print(f"ICT BOS+FVG entry — {npairs} pairs, realistic next-bar fills, RR sweep, OOS\n")
    for tf in ('m15', 'h1', '4h', 'daily'):
        print(f"=== {tf.upper()} ===")
        for rr in RRS:
            line(f"RR{rr}", store[(tf, rr)])
        print()
    print("Per-class (RR2):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        print(f"  --- {c} ---")
        for tf in ('m15', 'h1', '4h', 'daily'):
            line(tf, store_cls[c][(tf, 2.0)])


if __name__ == '__main__':
    main()
