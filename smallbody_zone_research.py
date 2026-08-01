"""'Small-body candle' supply/demand (ak.iskander) — tight base + explosive departure.

Rules: on HTF (4h / daily) find a FRESH supply/demand zone = a SMALL-body candle
immediately followed by an EXPLOSIVE candle (tight consolidation -> violent move away).
The small candle's range is the zone. Wait for price to retrace into the zone on the LTF
and reject it (small candles reject the line), enter at the zone, stop beyond the zone
extreme, target 2:1. Demand = small candle then explosive UP (buy the retrace); supply =
small candle then explosive DOWN (sell the retrace).

The distinctive filter vs a generic order block is the SMALL-body base + EXPLOSIVE
departure — a tighter, more selective zone. Tested here to see if that selectivity buys an
edge the generic OB family (ob/obfvg/supply_demand) didn't have.

HTF zones (4h from h1, and daily); execution on h1 with MARKET fills (retrace bar close,
require a close that holds the zone = rejection), stop beyond the zone, RR2 & RR3. All
pairs, per class, chronological OOS split.

Run: python smallbody_zone_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
SMALL = 0.6          # base body <= SMALL * ATR (small-body candle)
EXPL = 1.3           # departure body >= EXPL * ATR (explosive candle)
BUF = 0.20           # stop buffer beyond the zone (ATR)
RETR_H1 = 400        # h1 bars to wait for the retrace
HOLD = 160           # h1 bars to reach the target
RRS = [2.0, 3.0]


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def zones(htf, bar_secs):
    """(ready_ts, dir, zlo, zhi) for each small-base -> explosive-departure zone."""
    out = []
    for i in range(14, len(htf) - 1):
        a = atr(htf, 14, i) or 0.0
        if a <= 0:
            continue
        base = htf[i]; nxt = htf[i + 1]
        base_body = abs(base['c'] - base['o']); dep_body = nxt['c'] - nxt['o']
        if base_body > SMALL * a:
            continue
        ready = nxt['_ts'] + bar_secs        # start scanning after the explosive candle closes
        zlo, zhi = base['l'], base['h']
        if dep_body >= EXPL * a:             # explosive UP -> demand zone (buy)
            out.append((ready, 'bull', zlo, zhi))
        elif -dep_body >= EXPL * a:          # explosive DOWN -> supply zone (sell)
            out.append((ready, 'bear', zlo, zhi))
    return out


def score(h1, ready_ts, d, zlo, zhi, rr, retr=None, hold=None):
    retr = retr or RETR_H1; hold = hold or HOLD
    ts = [b['_ts'] for b in h1]; s = bisect.bisect_left(ts, ready_ts)
    for r in range(s, min(s + retr, len(h1) - 1)):
        b = h1[r]
        if d == 'bull':
            touched = b['l'] <= zhi and b['c'] > zlo          # dipped into zone, held above its low
        else:
            touched = b['h'] >= zlo and b['c'] < zhi
        if not touched:
            continue
        a = atr(h1, 14, r) or 0.0
        entry = b['c']; stop = (zlo - BUF * a) if d == 'bull' else (zhi + BUF * a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            return None
        R = abs(entry - stop); tgt = entry + rr * R if d == 'bull' else entry - rr * R
        for j in range(r + 1, min(r + 1 + hold, len(h1))):
            bb = h1[j]
            if d == 'bull':
                if bb['l'] <= stop: return (-1.0, entry, R)
                if bb['h'] >= tgt: return (rr, entry, R)
            else:
                if bb['h'] >= stop: return (-1.0, entry, R)
                if bb['l'] <= tgt: return (rr, entry, R)
        return None
    return None


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<8} RR{rr} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(zone_tf, exec_tf=None):
    """zone_tf = timeframe the zone is detected on; exec_tf = timeframe the retrace/entry is
    resolved on (default: same TF for m15/h1, h1 for 4h/daily). Retrace/hold windows scale
    with the exec TF so every combination covers the same wall-clock (~16d retrace / ~6.7d)."""
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    ZSECS = {'m15': 900, 'h1': 3600, '4h': 4 * 3600, 'daily': 86400}
    # exec windows in bars = ~16.7d retrace / ~6.7d hold at that TF
    EWIN = {'m15': (1600, 640), 'h1': (400, 160)}
    ex = exec_tf or ('h1' if zone_tf in ('4h', 'daily') else zone_tf)
    retr, hold = EWIN[ex]
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 500:
            continue
        zbars = ({'m15': _bars_norm(pairs[pk].get('m15', [])), 'h1': h1, '4h': make_4h(h1),
                  'daily': _bars_norm(pairs[pk].get('daily', []))}[zone_tf])
        ebars = h1 if ex == 'h1' else _bars_norm(pairs[pk].get(ex, []))
        if len(zbars) < 60 or len(ebars) < 500:
            continue
        npr += 1
        for ready, dr, zlo, zhi in zones(zbars, ZSECS[zone_tf]):
            for rr in RRS:
                res = score(ebars, ready, dr, zlo, zhi, rr, retr, hold)
                if res is not None:
                    o, entry, R = res
                    store[cls][rr].append((ready, o - cost(o, entry, R)))
    print(f"\n===== {zone_tf}-zone -> {ex} entry (small-body supply/demand) — {npr} pairs =====")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        for rr in RRS:
            line(c, store[c][rr], rr)
    for rr in RRS:
        line('ALL', [r for c in store for r in store[c][rr]], rr)


def main():
    print("=" * 88)
    print("Small-body-base + explosive-departure supply/demand zone (retrace entry, RR2/3)")
    print("=" * 88)
    run('m15'); run('h1')                    # LTF detect + trade
    run('4h'); run('daily')                  # HTF zone, h1 entry
    run('4h', exec_tf='m15')                 # HTF zone, 15m entry (finer confirmation)


if __name__ == '__main__':
    main()
