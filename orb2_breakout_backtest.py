"""2-candle ORB breakout backtest (from the TSLA/15m TikTok).

Rule:
  box over the prior 2 candles: box_high = max(H[-1],H[-2]), box_low = min(L[-1],L[-2]).
  a candle that CLOSES above box_high -> BUY at its close; CLOSES below box_low -> SELL at its close.
  stop = opposite level (long: box_low, short: box_high).
  target = "10 points" -> tested as an R-multiple of the box height R=|entry-stop| (the video's own
           R/R was ~1.0-1.07, so RR~1 is faithful; swept 1.0/1.5/2.0 for comparability across pairs).

One position at a time (no new entry while in a trade). Bracket-honest: stop-first on same-bar
ambiguity (conservative), unresolved-within-hold excluded. Gross of costs — see the box-size
diagnostic: a 2-candle m15 box is tiny, so spread/slippage is a large fraction of R (the usual
killer for tight breakout systems). Commits nothing.

  python orb2_breakout_backtest.py --hist deep-ohlc.json     # 3yr (CI)
"""
import argparse
import json
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from detect_triggers import PAIR_CLASS

CLASSES = {'major', 'minor', 'index', 'comm', 'crypto'}
BLACKLIST = {'xptusd'}
HOLD = 48            # m15 bars (~12h) to resolve the 1:1-ish bracket; unresolved excluded
RRS = [1.0, 1.5, 2.0]


def _run_pair(bars, rr):
    """One-at-a-time 2-candle ORB over a pair. Yields (entry_ts, R) for resolved trades, plus a
    running sum of box sizes (in price) and count for the box-size diagnostic."""
    out = []; box_sum = 0.0; box_n = 0
    n = len(bars); i = 2
    while i < n - 1:
        bh = max(bars[i-1]['h'], bars[i-2]['h'])
        bl = min(bars[i-1]['l'], bars[i-2]['l'])
        c = bars[i]['c']
        d = None
        if c > bh:
            d = 'bull'; entry = c; stop = bl
        elif c < bl:
            d = 'bear'; entry = c; stop = bh
        if d is None:
            i += 1; continue
        R = abs(entry - stop)
        if R <= 0:
            i += 1; continue
        box_sum += R; box_n += 1
        target = entry + rr * R if d == 'bull' else entry - rr * R
        # score from the NEXT bar
        end = min(i + 1 + HOLD, n); res = None; exit_j = end - 1
        for j in range(i + 1, end):
            b = bars[j]
            if d == 'bull':
                if b['l'] <= stop: res = -1.0; exit_j = j; break
                if b['h'] >= target: res = rr; exit_j = j; break
            else:
                if b['h'] >= stop: res = -1.0; exit_j = j; break
                if b['l'] <= target: res = rr; exit_j = j; break
        if res is not None:
            out.append((bars[i]['_ts'], res))
            i = exit_j + 1            # one position at a time: resume after the exit
        else:
            i = end                   # unresolved: skip past the hold window
    return out, box_sum, box_n


def _agg(rows):
    rows = sorted(rows, key=lambda r: r[0]); rs = [r[1] for r in rows]
    n = len(rs)
    if not n: return None
    m = n // 2
    return dict(n=n, wr=100.0*sum(1 for r in rs if r > 0)/n, exp=sum(rs)/n, tot=sum(rs),
                oos1=(sum(rs[:m])/m if m else 0), oos2=(sum(rs[m:])/(n-m) if n-m else 0))


def _line(label, v):
    if not v: print(f"  {label:9} (n<1)"); return
    print(f"  {label:9} n={v['n']:<6} WR={v['wr']:4.0f}%  exp={v['exp']:+.3f}R  tot={v['tot']:+7.0f}  "
          f"OOS {v['oos1']:+.3f}/{v['oos2']:+.3f}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    print(f"=== 2-candle ORB breakout · {args.hist} ===")
    print("(close-breakout of the prior-2-candle box; stop=opposite level; target=RR x box. "
          "one-at-a-time, bracket-honest, GROSS of costs)\n")
    # box-size diagnostic (once, rr-independent)
    bsum = bn = 0
    for pk, layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or pk in BLACKLIST or not isinstance(layers, dict): continue
        m15 = _bars_norm(layers.get('m15') or [])
        if len(m15) < 500: continue
        _, s, c = _run_pair(m15, 1.0); bsum += s; bn += c
    for rr in RRS:
        byc = defaultdict(list)
        for pk, layers in pairs.items():
            cls = PAIR_CLASS.get(pk)
            if cls not in CLASSES or pk in BLACKLIST or not isinstance(layers, dict): continue
            m15 = _bars_norm(layers.get('m15') or [])
            if len(m15) < 500: continue
            rows, _, _ = _run_pair(m15, rr)
            byc[cls] += rows; byc['ALL'] += rows
        print(f">> TARGET = {rr:g}R (stop = box):")
        for c in ['ALL', 'major', 'minor', 'index', 'comm', 'crypto']:
            _line(c, _agg(byc.get(c, [])))
        print()
    print(f"box-size diagnostic: mean 2-candle box R = {bsum/max(bn,1):.6f} price units over {bn} breakouts")
    print("Reading it: break-even at RR=1 is 50% WR, at RR=2 is 33%. A positive GROSS exp still has to")
    print("clear the spread, which on a tiny 2-candle m15 box is a large fraction of R.")


if __name__ == '__main__':
    main()
