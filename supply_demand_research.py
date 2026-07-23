"""Supply & Demand zone strategy (user screenshots — 'last opposite candle before
the impulse' base).

Zone formation:
  SUPPLY (short): a small base candle (consolidation) is followed by a NOTICEABLE
    DOWN impulse that breaks below the base. The zone = the high/low of the LAST
    BULLISH candle before the drop. On a later RALLY back UP into the zone, SELL;
    stop above the zone (distal edge), target a fixed RR.
  DEMAND (long, mirror): small base + noticeable UP impulse breaking above it; the
    zone = the LAST BEARISH candle before the rally. On a later DIP back DOWN into
    the zone, BUY; stop below the zone.

Entry follows the book's Step #3 (reversal candle): price must return to the zone
AND print a rejection PIN BAR (wick >= 50% of the candle range — lower wick for
longs, upper wick for shorts) that tested the zone; entry is a STOP order on the
BREAK of that pin bar (above it for longs, below for shorts). This is a
buy-strength / sell-weakness fill, NOT a favourable limit at the edge, so it
avoids the adverse-selection illusion. Stop beyond the pin's rejection wick + ATR
buffer. Fixed RR target. No lookahead (zone known at impulse close, retest+pin+
break strictly after), fixed cost, chronological OOS split (both halves +), per
class, 4h/h1/daily. Generic price-action, clean-room.

Run: python supply_demand_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
IMP = 1.0            # impulse candle body must be >= IMP * ATR (the "noticeable move")
CONSOL = 0.8         # base candle range must be <= CONSOL * ATR (small / sideways)
BASE_WIN = 4         # look back this many bars for the last opposite-colour base candle
BREAK_LOOK = 3       # impulse must break the extreme of the prior BREAK_LOOK bars
MAX_AGE = 60         # zone valid for a retest for this many bars
WICK_MIN = 0.50      # pin bar: rejection wick must be >= this fraction of the range
BREAK_WIN = 3        # bars to break the pin bar after it forms
BUF = 0.10           # stop buffer beyond the pin's rejection wick, in ATR
COOLDOWN = 3
RRS = [1.5, 2.0, 3.0]
HOLD = {'h1': 96, '4h': 90, 'daily': 40}


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(bars, tf, store, cls, store_cls, store_pair, pk):
    n = len(bars); last = -1
    for j in range(BASE_WIN + BREAK_LOOK, n - 1):
        if j <= last:
            continue
        a = atr(bars, 14, j) or 0.0
        if a <= 0:
            continue
        body = bars[j]['c'] - bars[j]['o']
        prior_hi = max(bars[k]['h'] for k in range(j - BREAK_LOOK, j))
        prior_lo = min(bars[k]['l'] for k in range(j - BREAK_LOOK, j))
        d = zone_lo = zone_hi = None
        # UP impulse -> DEMAND zone = last bearish candle before it
        if body >= IMP*a and bars[j]['c'] > prior_hi:
            for b in range(j - 1, j - 1 - BASE_WIN, -1):
                if bars[b]['c'] < bars[b]['o']:
                    if (bars[b]['h'] - bars[b]['l']) <= CONSOL*a:
                        d = 'bull'; zone_lo = bars[b]['l']; zone_hi = bars[b]['h']
                    break
        # DOWN impulse -> SUPPLY zone = last bullish candle before it
        elif -body >= IMP*a and bars[j]['c'] < prior_lo:
            for b in range(j - 1, j - 1 - BASE_WIN, -1):
                if bars[b]['c'] > bars[b]['o']:
                    if (bars[b]['h'] - bars[b]['l']) <= CONSOL*a:
                        d = 'bear'; zone_lo = bars[b]['l']; zone_hi = bars[b]['h']
                    break
        if d is None:
            continue
        prox = zone_hi if d == 'bull' else zone_lo
        # find the first REJECTION PIN BAR that tests the zone (strictly after impulse)
        ei = entry = stop = None
        for k in range(j + 1, min(j + 1 + MAX_AGE, n - 1)):
            b = bars[k]
            rng = b['h'] - b['l']
            if rng <= 0:
                continue
            if d == 'bull':
                if b['c'] < zone_lo:                       # zone broken on a close
                    break
                lower_wick = min(b['o'], b['c']) - b['l']
                pin = (lower_wick >= WICK_MIN*rng) and (b['l'] <= prox)   # long wick below, tested zone
            else:
                if b['c'] > zone_hi:
                    break
                upper_wick = b['h'] - max(b['o'], b['c'])
                pin = (upper_wick >= WICK_MIN*rng) and (b['h'] >= prox)   # long wick above, tested zone
            if not pin:
                continue
            # STOP entry on the break of the pin bar (above for longs / below for shorts)
            for m in range(k + 1, min(k + 1 + BREAK_WIN, n)):
                if d == 'bull' and bars[m]['h'] >= b['h']:
                    ei = m; entry = b['h']; stop = b['l'] - BUF*a; break
                if d == 'bear' and bars[m]['l'] <= b['l']:
                    ei = m; entry = b['l']; stop = b['h'] + BUF*a; break
            if ei is not None:
                break
        if ei is None:
            continue
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        for rr in RRS:
            o = walk(bars, ei, entry, stop, d, rr, HOLD[tf])
            if o is not None:
                net = o - cost(o, entry, R)
                store[(tf, rr)].append((ts, net))
                store_cls[cls][(tf, rr)].append((ts, net))
                store_pair[pk][(tf, rr)].append((ts, net))
        last = ei + COOLDOWN


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100/(1+rr)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list))
    store_pair = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, store, cls, store_cls, store_pair, pk)

    print(f"Supply & Demand zone retest — {npairs} pairs, limit-at-zone fills, OOS\n")
    for tf in ('4h', 'h1', 'daily'):
        for rr in RRS:
            line(f"{tf} RR{rr}", store[(tf, rr)], rr)
        print()
    print("=== per class (4H, RR2.0) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][('4h', 2.0)], 2.0)
    print("\n=== per class (daily, RR2.0) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"{c}", store_cls[c][('daily', 2.0)], 2.0)


if __name__ == '__main__':
    main()
