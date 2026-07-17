"""Fred Trading (Fredtradingdk) trendline-RETRACEMENT / bounce strategy.

From the posted charts: an established trendline (ascending support for longs,
descending resistance for shorts) is drawn across two pivots; price retraces back
to the line, BOUNCES off it (line holds — not a break), and you enter in the trend
direction. Stop beyond the line (red box), target at min 2:1 (green box).

Distinct from our tl_nowick (which required a trendline BREAK then retest) — here
the line is intact and respected.

Faithful mechanical version:
  - Clean alternating pivots (fractal, prd=4). Two ascending pivot lows define a
    support line (longs); two descending pivot highs define resistance (shorts).
  - 3rd-touch BOUNCE: after the 2nd pivot, price returns to the line (low reaches
    it within a small ATR band) and CLOSES back in-trend off it (bullish close
    above support). Invalidate if a bar closes decisively through the line first.
  - Entry next-bar open, stop beyond the line/bounce extreme + ATR buffer, RR2.
  - Next-bar fills, realistic cost, OOS split, per class, m15/h1/4h/daily.

Run: python fredtrading_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0
PRD = 4
TOUCH_WIN = 50           # bars after the 2nd pivot to wait for the retrace-bounce
NEAR = 0.25              # low must reach within NEAR*ATR of the line
BREAK_TOL = 0.5          # a close this many ATR beyond the line = line broken (void)
ATR_BUF = 0.25
COOLDOWN = 6
HOLD = {'m15': 60, 'h1': 48, '4h': 60, 'daily': 25}


def zigzag(bars, prd):
    n = len(bars); piv = []
    for i in range(prd, n - prd):
        if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, prd+1)):
            piv.append((i, bars[i]['h'], 'H'))
        if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, prd+1)):
            piv.append((i, bars[i]['l'], 'L'))
    piv.sort()
    out = []
    for p in piv:
        if out and p[2] == out[-1][2]:
            if (p[2] == 'H' and p[1] > out[-1][1]) or (p[2] == 'L' and p[1] < out[-1][1]):
                out[-1] = p
        else:
            out.append(p)
    return out


def scan(bars, tf, store, cls, store_cls):
    zz = zigzag(bars, PRD); n = len(bars); hold = HOLD[tf]; last = -1
    lows = [(i, p) for (i, p, k) in zz if k == 'L']
    highs = [(i, p) for (i, p, k) in zz if k == 'H']
    for d, seq in (('bull', lows), ('bear', highs)):
        for m in range(1, len(seq)):
            (i1, v1), (i2, v2) = seq[m-1], seq[m]
            if i2 <= i1:
                continue
            slope = (v2 - v1) / (i2 - i1)
            # ascending support for longs, descending resistance for shorts
            if d == 'bull' and slope <= 0:
                continue
            if d == 'bear' and slope >= 0:
                continue

            def line(x, _v1=v1, _i1=i1, _s=slope):
                return _v1 + _s * (x - _i1)

            start = i2 + PRD + 1; ei = None
            for j in range(start, min(start + TOUCH_WIN, n - 1)):
                b = bars[j]; lv = line(j); a = atr(bars, 14, j) or 0.0
                if a <= 0:
                    continue
                if d == 'bull':
                    if b['c'] < lv - BREAK_TOL * a:            # closed through support -> void
                        break
                    if b['l'] <= lv + NEAR * a and b['c'] > lv and b['c'] > b['o']:
                        ei = j + 1; break
                else:
                    if b['c'] > lv + BREAK_TOL * a:
                        break
                    if b['h'] >= lv - NEAR * a and b['c'] < lv and b['c'] < b['o']:
                        ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
            lv = line(ei)
            if d == 'bull':
                stop = min(bars[ei-1]['l'], lv) - ATR_BUF * a
                if stop >= entry:
                    continue
            else:
                stop = max(bars[ei-1]['h'], lv) + ATR_BUF * a
                if stop <= entry:
                    continue
            R = abs(entry - stop); ts = bars[ei]['_ts']
            o = walk(bars, ei, entry, stop, d, RR, hold)
            if o is None:
                continue
            net = o - cost(o, entry, R)
            store[tf].append((ts, net)); store_cls[cls][tf].append((ts, net))
            last = ei + COOLDOWN


def line_report(label, rows):
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

    print(f"Fred Trading trendline-bounce (RR2) — {npairs} pairs, realistic cost, OOS\n")
    for tf in ('m15', 'h1', '4h', 'daily'):
        line_report(tf, store[tf])
    print("\nPer-class:")
    for tf in ('m15', 'h1', '4h', 'daily'):
        print(f"  --- {tf} ---")
        for c in ['comm', 'crypto', 'index', 'major', 'minor']:
            line_report(c, store_cls[c][tf])


if __name__ == '__main__':
    main()
