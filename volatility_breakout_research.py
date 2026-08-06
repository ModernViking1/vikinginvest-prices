"""World Cup Championship of Futures Trading — winning-lineage archetype test.

HONESTY NOTE. Brent Carlile's *exact* competition rules are not publicly
documented in a form I can verify, so this does NOT claim to reproduce his
proprietary system. What IS well documented is the winning LINEAGE of that
competition (Robbins Trading World Cup): the volatility / opening-range
breakout with a trend bias and aggressive position sizing — the family Larry
Williams used for his famous run and that most futures-cup champions trade
some variant of. This tests that archetype on our futures-like instruments
(indices, metals, energy, FX majors) so we have a real, cost-honest read while
we source Carlile's actual method.

Setup (Williams-style volatility breakout, daily):
  buyStop  = open + K * range(prev day)     → enter long if the bar trades up to it
  sellStop = open - K * range(prev day)     → enter short if it trades down to it
  stop = 1 ATR from entry, target = RR x risk, max hold a few days.
  Optional 50-EMA trend bias (longs only above, shorts only below).

Discipline as everywhere else: fills at the breakout level (a stop order — the
one place a resting order is realistic), fixed dealing cost, chronological OOS
(BOTH halves positive + n>=40 = PASS).

Run: python volatility_breakout_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost, ema

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
KS = [0.4, 0.6, 0.8]
RR = 2.0
HOLD = 5
COOLDOWN = 1
# Futures-like universe: indices, metals, energy, FX majors (the World Cup trades futures).
UNIVERSE = ['de40', 'dj30', 'fra40', 'ftse100', 'jp225', 'nas100', 'spx500',
            'xauusd', 'xagusd', 'xptusd', 'usoil', 'wtiusd', 'natgas',
            'eurusd', 'gbpusd', 'usdjpy', 'audusd', 'usdcad']


def signals(bars, k, trend):
    out = []
    n = len(bars)
    last = -1
    c = [x['c'] for x in bars]
    e50 = ema(c, 50)
    for i in range(51, n - 1):
        if i <= last:
            continue
        rng = bars[i - 1]['h'] - bars[i - 1]['l']
        a = atr(bars, 14, i)
        if rng <= 0 or not a or a <= 0:
            continue
        o = bars[i]['o']
        buy_stop = o + k * rng
        sell_stop = o - k * rng
        bias = None
        if trend and e50[i] is not None:
            bias = 'bull' if bars[i]['c'] > e50[i] else 'bear'   # coarse regime read
        # Long breakout
        if bars[i]['h'] >= buy_stop and (bias in (None, 'bull')):
            entry = buy_stop
            out.append((i + 1, entry, entry - a, 'bull')); last = i + COOLDOWN
            continue
        # Short breakout
        if bars[i]['l'] <= sell_stop and (bias in (None, 'bear')):
            entry = sell_stop
            out.append((i + 1, entry, entry + a, 'bear')); last = i + COOLDOWN
    return out


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= tgt:
                return rr
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= tgt:
                return rr
    return None


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<16} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(d, k, trend):
    allrows = []
    for pk in UNIVERSE:
        bars = _bars_norm(d.get(pk, {}).get('daily', []))
        if len(bars) < 120:
            continue
        for (ei, entry, stop, dr) in signals(bars, k, trend):
            if ei >= len(bars):
                continue
            o = walk(bars, ei, entry, stop, dr, RR, HOLD)
            if o is not None:
                allrows.append((bars[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
    return allrows


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 96)
    print('World Cup futures lineage — volatility breakout archetype (NOT Carlile\'s verified rules)')
    print('daily · fills at the breakout level · cost · OOS · RR2')
    print('=' * 96)
    for trend in (False, True):
        print(f"\n===== trend-bias filter: {'ON (50-EMA)' if trend else 'OFF'} =====")
        for k in KS:
            line(f'K={k}', run(d, k, trend))


if __name__ == '__main__':
    main()
