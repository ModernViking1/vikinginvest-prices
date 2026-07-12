"""Test the tractable strategies from 'Six Figures From Scratch' under the same
discipline as everything else (realistic next-bar-open fill, structural stop,
RR2, realistic cost, chronological OOS split), on daily + 4H:

  bollinger : mean-reversion — bar pierces the 20,2 band and closes back inside
  rsi_x     : oversold/overbought — RSI(14) crosses back up through 30 / down 70
  fib       : retracement — pullback into the 50-61.8% zone of the last swing leg,
              closing back in the trend direction

#3 M/W double top-bottom already tested (multi_pattern_research: ~50%, fails OOS).
#4 9-step reversal has no published rules -> not encodable. Reported book WRs are
73-80% (marketing); this measures what survives realistic execution.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, HOLD
from h11_supplement_research import wilder_rsi

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RR = 2.0
RR_SWEEP = [0.5, 1.0, 1.5, 2.0]
SWING_LB = 10


def bb(closes, i, n=20, k=2.0):
    if i + 1 < n: return None
    w = closes[i-n+1:i+1]; m = sum(w)/n
    sd = (sum((x-m)**2 for x in w)/n) ** 0.5
    return m - k*sd, m, m + k*sd


def struct_stop(bars, i, d):
    seg = bars[max(0, i-SWING_LB):i+1]
    return min(b['l'] for b in seg) if d == 'bull' else max(b['h'] for b in seg)


def bollinger(bars):
    c = [b['c'] for b in bars]; out = []; last = -1
    for i in range(21, len(bars)-1):
        if i <= last: continue
        band = bb(c, i)
        if not band: continue
        lo, mid, up = band; bar = bars[i]
        if bar['l'] < lo and bar['c'] > lo and bar['c'] > bar['o']:      # dip below, close back in, bullish
            out.append((i, 'bull', bar['l'])); last = i
        elif bar['h'] > up and bar['c'] < up and bar['c'] < bar['o']:
            out.append((i, 'bear', bar['h'])); last = i
    return out


def rsi_x(bars):
    c = [b['c'] for b in bars]; r = wilder_rsi(c, 14); out = []; last = -1
    for i in range(16, len(bars)-1):
        if i <= last: continue
        if r[i-1] is None or r[i] is None: continue
        if r[i-1] < 30 <= r[i]:
            out.append((i, 'bull', struct_stop(bars, i, 'bull'))); last = i
        elif r[i-1] > 70 >= r[i]:
            out.append((i, 'bear', struct_stop(bars, i, 'bear'))); last = i
    return out


def fib(bars):
    """Pullback into 50-61.8% of the last ~20-bar swing leg, closing back in-trend."""
    out = []; last = -1; LB = 20
    for i in range(LB+2, len(bars)-1):
        if i <= last: continue
        seg = bars[i-LB:i]
        hi = max(b['h'] for b in seg); lo = min(b['l'] for b in seg)
        rng = hi - lo
        if rng <= 0: continue
        hi_idx = max(range(len(seg)), key=lambda k: seg[k]['h'])
        lo_idx = min(range(len(seg)), key=lambda k: seg[k]['l'])
        bar = bars[i]
        # uptrend leg (low then high): buy the pullback into 50-61.8% from the top
        if lo_idx < hi_idx:
            z_hi = hi - 0.5*rng; z_lo = hi - 0.618*rng
            if bar['l'] <= z_hi and bar['l'] >= z_lo - 0.1*rng and bar['c'] > bar['o'] and bar['c'] > z_lo:
                out.append((i, 'bull', lo)); last = i
        else:  # downtrend leg (high then low): sell the pullback into 50-61.8% from the bottom
            z_lo = lo + 0.5*rng; z_hi = lo + 0.618*rng
            if bar['h'] >= z_lo and bar['h'] <= z_hi + 0.1*rng and bar['c'] < bar['o'] and bar['c'] < z_hi:
                out.append((i, 'bear', hi)); last = i
    return out


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    res = defaultdict(list)   # (strat, tf) -> [(ts, r)]
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        tfs = {'daily': (daily, 20), '4h': (agg4h(h1), 60)}
        for tf, (bars, hold) in tfs.items():
            for name, fn in (('bollinger', bollinger), ('rsi_x', rsi_x), ('fib', fib)):
                for (i, dr, stop) in fn(bars):
                    if i+1 >= len(bars): continue
                    entry = bars[i+1]['o']
                    if (dr == 'bull' and stop >= entry) or (dr == 'bear' and stop <= entry): continue
                    o = walk(bars, i+1, entry, stop, dr, RR, hold)
                    if o is None: continue
                    res[(name, tf)].append((bars[i+1]['_ts'], o - cost(o, entry, abs(entry-stop))))

    # RR-sweep view (fair to mean-reversion: high WR lives at tight targets)
    print("BOOK-5 · daily · target sweep (WR / expectancy net of cost, OOS-checked):")
    print(f"{'strategy':<12} | " + " | ".join(f"RR{rr:<4}".ljust(16) for rr in RR_SWEEP))
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    sweep = defaultdict(lambda: defaultdict(list))
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        for name, fn in (('bollinger', bollinger), ('rsi_x', rsi_x), ('fib', fib)):
            for (i, dr, stop) in fn(daily):
                if i+1 >= len(daily): continue
                entry = daily[i+1]['o']
                if (dr == 'bull' and stop >= entry) or (dr == 'bear' and stop <= entry): continue
                for rr in RR_SWEEP:
                    o = walk(daily, i+1, entry, stop, dr, rr, 20)
                    if o is not None:
                        sweep[name][rr].append((daily[i+1]['_ts'], o - cost(o, entry, abs(entry-stop))))
    for name in ('bollinger', 'rsi_x', 'fib'):
        cells = []
        for rr in RR_SWEEP:
            rows = sorted(sweep[name][rr]); seq = [r for _, r in rows]
            n, w, e = agg(seq); mid = len(rows)//2
            _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
            ok = (e > 0 and eh > 0 and es > 0)
            cells.append(f"{w:>4.0f}%/{e:>+.3f}{'✓' if ok else '✗'}")
        print(f"{name:<12} | " + " | ".join(c.ljust(16) for c in cells))
    print("  ✓ = positive expectancy AND both OOS halves positive · ✗ = fails")


if __name__ == '__main__':
    main()
