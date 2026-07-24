"""Engulfing-candle + imbalance retracement (user screenshots, tomcampcoaching).

A LARGE engulfing candle takes out the previous candle (bullish closes above the
prior high / bearish closes below the prior low). The candle's 50% level is the
"balance"; its body leaves an imbalance (FVG). On a RETRACEMENT into that zone,
enter IN THE ENGULFING DIRECTION (bullish->buy / bearish->sell), stop at the
pattern box (engulfing extreme), target 2:1.

Two entries to separate edge from fill illusion:
  A) LIMIT at the 50% balance (optimistic favourable fill).
  B) MARKET on the next bar after price tags the 50% (realistic).
Tested on 4H / 1H / 15m across pairs. No lookahead (zone from the closed engulfing
candle, retrace strictly after), fixed cost, both-OOS-halves gate, per class.

Run: python engulf_imbalance_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
ENG_ATR = 1.0        # "large" engulfing: range >= this * ATR
RETR_WIN = 12        # bars to retrace into the 50% balance
BUF = 0.10
COOLDOWN = 4
RR = 2.0
HOLD = {'15m': 64, 'h1': 60, '4h': 60}


def big_engulf(bars, i):
    """Return 'bull'/'bear' if bars[i] is a large candle engulfing bars[i-1] and
    closing beyond the prior candle's extreme; else None."""
    a = atr(bars, 14, i) or 0.0
    if a <= 0:
        return None
    rng = bars[i]['h'] - bars[i]['l']
    if rng < ENG_ATR*a:
        return None
    o, c = bars[i]['o'], bars[i]['c']
    po, pc = bars[i-1]['o'], bars[i-1]['c']
    lo1, hi1 = min(po, pc), max(po, pc)
    if c > o and o <= lo1 and c >= hi1 and c >= bars[i-1]['h']:
        return 'bull'
    if c < o and o >= hi1 and c <= lo1 and c <= bars[i-1]['l']:
        return 'bear'
    return None


def walk(bars, i0, entry, stop, d, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + RR*R if d == 'bull' else entry - RR*R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return RR
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return RR
    return None


def scan(bars, tf, S, cls, SC):
    n = len(bars); last = -1
    for i in range(15, n-2):
        if i <= last:
            continue
        d = big_engulf(bars, i)
        if d is None:
            continue
        mid = (bars[i]['h'] + bars[i]['l']) / 2.0
        stop = (bars[i]['l'] - BUF*atr(bars, 14, i)) if d == 'bull' else (bars[i]['h'] + BUF*atr(bars, 14, i))
        # find the first retrace into the 50% balance
        t = None
        for j in range(i+1, min(i+1+RETR_WIN, n-1)):
            if d == 'bull' and bars[j]['l'] <= mid:
                t = j; break
            if d == 'bear' and bars[j]['h'] >= mid:
                t = j; break
        if t is None:
            continue
        # A) limit fill at the 50% balance (walk from the tag bar)
        if (d == 'bull' and stop < mid) or (d == 'bear' and stop > mid):
            o = walk(bars, t, mid, stop, d, HOLD[tf])
            if o is not None:
                R = abs(mid-stop); S[(tf, 'limit')].append((bars[t]['_ts'], o - cost(o, mid, R)))
                SC[cls][(tf, 'limit')].append((bars[t]['_ts'], o - cost(o, mid, R)))
        # B) market fill on the next bar
        ei = t+1
        if ei < n:
            entry = bars[ei]['o']
            if (d == 'bull' and stop < entry) or (d == 'bear' and stop > entry):
                o = walk(bars, ei, entry, stop, d, HOLD[tf])
                if o is not None:
                    R = abs(entry-stop); S[(tf, 'mkt')].append((bars[ei]['_ts'], o - cost(o, entry, R)))
                    SC[cls][(tf, 'mkt')].append((bars[ei]['_ts'], o - cost(o, entry, R)))
        last = t + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<18} n={n:>4} WR={w:>5.1f}% (be 33%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    S = defaultdict(list); SC = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        m15 = _bars_norm(pairs[pk].get('m15', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 400:
            continue
        npairs += 1
        tfs = {'h1': h1, '4h': agg4h(h1)}
        if len(m15) >= 1000:
            tfs['15m'] = m15
        for tf, bars in tfs.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, S, cls, SC)

    print(f"Engulfing + imbalance retrace (2:1) — {npairs} pairs\n")
    for tf in ('4h', 'h1', '15m'):
        print(f"=== {tf} ===")
        line("limit (optimistic)", S[(tf, 'limit')])
        line("market (realistic)", S[(tf, 'mkt')])
    print("\n=== per class (4H, market) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, SC[c][('4h', 'mkt')])
    print("=== per class (15m, market) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, SC[c][('15m', 'mkt')])


if __name__ == '__main__':
    main()
