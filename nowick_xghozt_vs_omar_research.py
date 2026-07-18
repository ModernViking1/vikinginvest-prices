"""No-wick candle entry: Omar (continuation) vs xGhozt-style (fade the missing wick).

Both are 'no-wick candle' models but with OPPOSITE directional logic (from their
public descriptions — no protected code reproduced):

  Detection (shared): a decisive candle with ~no wick on one side —
    'no lower wick' bull : close>open, lower_wick <= TOL*range  (opened at the low)
    'no upper wick' bear : close<open, upper_wick <= TOL*range  (opened at the high)
    body >= 0.5*range (a real, directional candle)

  OMAR arm  (momentum continuation): no-lower-wick -> LONG ; no-upper-wick -> SHORT
  XGHOZT arm (missing wick fills / fade): no-lower-wick -> SHORT ; no-upper-wick -> LONG

Same entry (next-bar open), same ATR stop, same RR targets — only the DIRECTION
differs, so the win-rate gap is purely which interpretation of a no-wick candle
is right. Also shown: our internal threshold no-wick (body>=0.65, rej<=0.20) run
Omar-style, to see whether exact-vs-threshold detection matters. Realistic cost,
OOS split, m15/h1/4h, all pairs.

Run: python nowick_xghozt_vs_omar_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
TOL = 0.10          # entry-side wick <= 10% of range = "no wick"
BODY_MIN = 0.5      # decisive candle
SL_ATR = 1.0        # ATR-based stop (same for both arms -> direction-only comparison)
COOLDOWN = 3
RRS = [1.0, 1.5, 2.0]
HOLD = {'m15': 48, 'h1': 48, '4h': 60}
# internal threshold no-wick (matches nowick() used elsewhere)
NW_BODY, NW_REJ = 0.65, 0.20


def nowick_side(b):
    """Return 'bull' (no lower wick, bullish) / 'bear' (no upper wick, bearish) / None."""
    rng = b['h'] - b['l']
    if rng <= 0:
        return None
    body = abs(b['c'] - b['o'])
    if body < BODY_MIN * rng:
        return None
    lw = min(b['o'], b['c']) - b['l']
    uw = b['h'] - max(b['o'], b['c'])
    if b['c'] > b['o'] and lw <= TOL * rng:
        return 'bull'      # no lower wick, bullish
    if b['c'] < b['o'] and uw <= TOL * rng:
        return 'bear'      # no upper wick, bearish
    return None


def thresh_nowick_side(b):
    rng = b['h'] - b['l']
    if rng <= 0:
        return None
    body = abs(b['c'] - b['o'])
    if body < NW_BODY * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= NW_REJ * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= NW_REJ * rng:
        return 'bear'
    return None


def scan(bars, tf, store):
    n = len(bars); hold = HOLD[tf]; last = -1
    for i in range(20, n - 1):
        if i <= last:
            continue
        side = nowick_side(bars[i]); tside = thresh_nowick_side(bars[i])
        if side is None and tside is None:
            continue
        ei = i + 1; entry = bars[ei]['o']; a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        R = SL_ATR * a
        def score(direction, key):
            if direction == 'bull':
                stop = entry - R
            else:
                stop = entry + R
            for rr in RRS:
                o = walk(bars, ei, entry, stop, direction, rr, hold)
                if o is not None:
                    store[(key, rr)].append((bars[ei]['_ts'], o - cost(o, entry, R)))
        if side is not None:
            omar_dir = side                                   # continuation
            xg_dir = 'bear' if side == 'bull' else 'bull'     # fade the missing wick
            score(omar_dir, 'omar'); score(xg_dir, 'xghozt')
            last = ei + COOLDOWN
        if tside is not None:
            score(tside, 'thresh')                            # our internal def, continuation


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<22} n={n:>5} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    per = {tf: defaultdict(list) for tf in ('m15', 'h1', '4h')}
    npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'m15': m15, 'h1': h1, '4h': agg4h(h1)}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, per[tf])

    print(f"No-wick candle: OMAR (continuation) vs xGHOZT (fade missing wick) — {npairs} pairs, cost, OOS\n")
    for tf in ('m15', 'h1', '4h'):
        print(f"=== {tf.upper()} ===")
        for rr in RRS:
            print(f"  RR {rr}:")
            line('OMAR continuation', per[tf][('omar', rr)])
            line('xGHOZT fade', per[tf][('xghozt', rr)])
            line('internal-thresh cont.', per[tf][('thresh', rr)])
        print()


if __name__ == '__main__':
    main()
