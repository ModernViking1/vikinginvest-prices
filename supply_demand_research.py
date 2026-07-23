"""Supply & Demand zone strategy — FAITHFUL to the book's 5 steps (user screenshots).

Zone (base = last opposite-colour candle before a noticeable impulse out of a small
consolidation): SUPPLY = last bullish candle before a down-impulse; DEMAND = last
bearish candle before an up-impulse.

Step #3 reversal candle: on the retest, require a rejection PIN BAR (wick >= 50% of
range; lower wick for longs, upper wick for shorts) that tested the zone.
Step #5 entry: enter at the reversal candle's CLOSE (market), as soon as it closes.
Step #4 stop: a few pips beyond the reversal candle (its low for longs / high for
shorts) + ATR buffer.
Step #4 take profit: the NEAREST OPPOSITE zone — long targets just below the nearest
supply area, short targets just above the nearest demand area (structural, variable
RR).
Rule #1: skip any trade whose structural target gives < 2:1 reward:risk.
Rule #2 (fresh zones): each zone is used for at most one trade.
Rule #3 (avoid red-news days): NOT modelled — no news feed; noted as an omitted filter.

Realistic: zone/target zones known only from bars formed BEFORE entry (no lookahead),
next-bar resolution from the close fill, fixed cost, chronological OOS split (both
halves +), per class, 4h/h1/daily. A fixed-2:1 variant (same entry/stop) is shown
alongside so the structural-target and fixed-target versions are separable.

Run: python supply_demand_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
IMP = 1.0            # impulse body must be >= IMP * ATR (the "noticeable move")
CONSOL = 0.8         # base candle range must be <= CONSOL * ATR (small / sideways)
BASE_WIN = 4         # look back this many bars for the last opposite-colour base candle
BREAK_LOOK = 3       # impulse must break the extreme of the prior BREAK_LOOK bars
MAX_AGE = 60         # zone valid for a retest for this many bars
WICK_MIN = 0.50      # pin bar: rejection wick must be >= this fraction of the range
BUF = 0.10           # stop buffer beyond the reversal candle, in ATR
MIN_RR = 2.0         # Rule #1: reward:risk must be >= this or the trade is skipped
COOLDOWN = 3
HOLD = {'h1': 120, '4h': 90, 'daily': 45}


def detect_zones(bars):
    n = len(bars); zones = []
    for j in range(BASE_WIN + BREAK_LOOK, n - 1):
        a = atr(bars, 14, j) or 0.0
        if a <= 0:
            continue
        body = bars[j]['c'] - bars[j]['o']
        prior_hi = max(bars[k]['h'] for k in range(j - BREAK_LOOK, j))
        prior_lo = min(bars[k]['l'] for k in range(j - BREAK_LOOK, j))
        if body >= IMP*a and bars[j]['c'] > prior_hi:                 # up impulse -> demand
            for b in range(j - 1, j - 1 - BASE_WIN, -1):
                if bars[b]['c'] < bars[b]['o']:
                    if (bars[b]['h'] - bars[b]['l']) <= CONSOL*a:
                        zones.append({'i': j, 'lo': bars[b]['l'], 'hi': bars[b]['h'], 'type': 'demand'})
                    break
        elif -body >= IMP*a and bars[j]['c'] < prior_lo:             # down impulse -> supply
            for b in range(j - 1, j - 1 - BASE_WIN, -1):
                if bars[b]['c'] > bars[b]['o']:
                    if (bars[b]['h'] - bars[b]['l']) <= CONSOL*a:
                        zones.append({'i': j, 'lo': bars[b]['l'], 'hi': bars[b]['h'], 'type': 'supply'})
                    break
    return zones


def walk_to(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    rr = abs(target - entry) / R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return rr
    return None


def scan(bars, tf, struct, fixed, cls, struct_cls):
    n = len(bars)
    zones = detect_zones(bars)
    supply = [z for z in zones if z['type'] == 'supply']
    demand = [z for z in zones if z['type'] == 'demand']
    last = -1
    for z in zones:
        d = 'bull' if z['type'] == 'demand' else 'bear'
        prox = z['hi'] if d == 'bull' else z['lo']
        j = z['i']; a = atr(bars, 14, j) or 0.0
        entry = stop = None; pin_k = None
        for k in range(j + 1, min(j + 1 + MAX_AGE, n - 1)):
            b = bars[k]; rng = b['h'] - b['l']
            if rng <= 0:
                continue
            if d == 'bull':
                if b['c'] < z['lo']:
                    break
                if (min(b['o'], b['c']) - b['l']) >= WICK_MIN*rng and b['l'] <= prox:
                    entry = b['c']; stop = b['l'] - BUF*a; pin_k = k; break
            else:
                if b['c'] > z['hi']:
                    break
                if (b['h'] - max(b['o'], b['c'])) >= WICK_MIN*rng and b['h'] >= prox:
                    entry = b['c']; stop = b['h'] + BUF*a; pin_k = k; break
        if pin_k is None:
            continue
        ei = pin_k + 1
        if ei >= n or ei <= last:
            continue
        R = abs(entry - stop)
        if R <= 0 or (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        # Step #4 take profit: nearest OPPOSITE zone formed before the entry
        if d == 'bull':
            cands = [s['lo'] for s in supply if s['i'] < pin_k and s['lo'] > entry]
            if not cands:
                continue
            tgt = min(cands)
        else:
            cands = [s['hi'] for s in demand if s['i'] < pin_k and s['hi'] < entry]
            if not cands:
                continue
            tgt = max(cands)
        rr_avail = (tgt - entry)/R if d == 'bull' else (entry - tgt)/R
        if rr_avail < MIN_RR:            # Rule #1
            continue
        ts = bars[ei]['_ts']
        o = walk_to(bars, ei, entry, stop, tgt, d, HOLD[tf])
        if o is not None:
            struct[tf].append((ts, o - cost(o, entry, R)))
            struct_cls[cls][tf].append((ts, o - cost(o, entry, R)))
        # fixed 2:1 comparison (same entry/stop)
        ft = entry + MIN_RR*R if d == 'bull' else entry - MIN_RR*R
        of = walk_to(bars, ei, entry, stop, ft, d, HOLD[tf])
        if of is not None:
            fixed[tf].append((ts, of - cost(of, entry, R)))
        last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    struct = defaultdict(list); fixed = defaultdict(list); struct_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, struct, fixed, cls, struct_cls)

    print(f"Supply & Demand — book-faithful (pin-close entry, structural target, Rule #1 >=2:1) — {npairs} pairs\n")
    print("STRUCTURAL target (nearest opposite zone, >=2:1):")
    for tf in ('4h', 'h1', 'daily'):
        line(f"{tf} struct", struct[tf])
    print("\nFIXED 2:1 target (same entry/stop, for comparison):")
    for tf in ('4h', 'h1', 'daily'):
        line(f"{tf} fixed2:1", fixed[tf])
    print("\nSTRUCTURAL per class (all TFs pooled):")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        pooled = struct_cls[c]['4h'] + struct_cls[c]['h1'] + struct_cls[c]['daily']
        line(f"{c}", pooled)


if __name__ == '__main__':
    main()
