"""Elliott Wave 3rd- and 5th-wave entries — disciplined backtest.

WAVE 3 — faithful to LonesomeTheBlue's TradingView '3rd Wave' indicator:
  ZigZag pivots form 0 -> 1 (wave 1) -> 2 (wave 2 pullback). Wave 2 must retrace
  38.2%-78.6% of wave 1. Entry on the BREAKOUT beyond the wave-1 extreme (high>W1
  high for longs) — i.e. wave 3 igniting. Stop below the wave-2 low (invalidation).
  Native measured-move target = wave-2 base + 1.618 x wave1 (the indicator's T2).
  [Volume-support filter OMITTED — no volume in the feed.]

WAVE 5 — Bratby 'Trade the Fifth':
  Pivots 0-1-2-3-4 form a valid impulse (w2 doesn't fully retrace w1; w3 makes a
  new extreme; w4 doesn't overlap w1). Entry on the breakout beyond the wave-3
  extreme (wave 5 igniting). Stop below the wave-4 low. Native target: W5 = W1.

Both scored the house way: next-bar-open fills, realistic cost, fixed RR targets
(1.6 = Bratby's stated min, and 2.0 = our standard) AND the native measured move,
chronological OOS split, per class, on h1/4h/daily. No lookahead — a pivot is only
used once confirmed (prd bars to its right), and breakouts are sought only after.

Run: python elliott_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, atr, agg

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')

PRD = 5                       # zigzag fractal half-width
RET_MIN, RET_MAX = 0.382, 0.786
BREAK_WIN = 40                # bars after the pullback to wait for the wave breakout
COOLDOWN = 6
ATR_BUF = 0.25
NATIVE_W3 = 1.618             # measured-move target multiple for wave 3 (indicator T2)
NATIVE_W5 = 1.0               # W5 = W1
RRS = [1.6, 2.0]
HOLD = {'h1': 48, '4h': 60, 'daily': 20}


def zigzag(bars, prd):
    """Alternating confirmed-pivot sequence: list of (idx, price, 'H'|'L')."""
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


def _emit(store, key, ts, r):
    if r is not None:
        store[key].append((ts, r))


def _score_all(store, strat, tf, bars, entry_i, entry, stop, d, native_tgt):
    """Score fixed-RR (1.6, 2.0) and the native measured-move target."""
    if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
        return
    R = abs(entry - stop); ts = bars[entry_i]['_ts']; hold = HOLD[tf]
    for rr in RRS:
        o = walk(bars, entry_i, entry, stop, d, rr, hold)
        _emit(store, (strat, tf, f'RR{rr}'), ts, None if o is None else o - cost(o, entry, R))
    # native target: walk to native_tgt (as an RR multiple) else stop
    nrr = abs(native_tgt - entry) / R if R else 0
    if nrr > 0:
        o = walk(bars, entry_i, entry, stop, d, nrr, hold)
        _emit(store, (strat, tf, 'native'), ts, None if o is None else o - cost(o, entry, R))


def detect_wave3(bars, tf, store, cls, store_cls):
    piv = zigzag(bars, PRD); n = len(bars); last = -1
    for k in range(len(piv) - 2):
        p0, p1, p2 = piv[k], piv[k+1], piv[k+2]
        for d, kinds in (('bull', ('L', 'H', 'L')), ('bear', ('H', 'L', 'H'))):
            if (p0[2], p1[2], p2[2]) != kinds:
                continue
            if d == 'bull' and not (p1[1] > p0[1] and p0[1] < p2[1] < p1[1]):
                continue
            if d == 'bear' and not (p1[1] < p0[1] and p0[1] > p2[1] > p1[1]):
                continue
            w1 = abs(p1[1] - p0[1]); w2 = abs(p2[1] - p1[1])
            if w1 <= 0 or not (RET_MIN <= w2 / w1 <= RET_MAX):
                continue
            start = max(p2[0] + PRD + 1, p2[0] + 1)
            entry_i = None; invalid = False
            for j in range(start, min(start + BREAK_WIN, n - 1)):
                b = bars[j]
                if d == 'bull':
                    if b['l'] < p0[1]:
                        invalid = True; break          # wave 2 fully retraced -> invalid
                    if b['h'] > p1[1]:
                        entry_i = j + 1; break
                else:
                    if b['h'] > p0[1]:
                        invalid = True; break
                    if b['l'] < p1[1]:
                        entry_i = j + 1; break
            if invalid or entry_i is None or entry_i <= last or entry_i >= n:
                continue
            entry = bars[entry_i]['o']; a = atr(bars, 14, entry_i - 1) or 0.0
            stop = (p2[1] - ATR_BUF * a) if d == 'bull' else (p2[1] + ATR_BUF * a)
            base = p2[1]
            native = base + w1 * NATIVE_W3 if d == 'bull' else base - w1 * NATIVE_W3
            _score_all(store, 'wave3', tf, bars, entry_i, entry, stop, d, native)
            _score_all(store_cls[cls], 'wave3', tf, bars, entry_i, entry, stop, d, native)
            last = entry_i + COOLDOWN


def detect_wave5(bars, tf, store, cls, store_cls):
    piv = zigzag(bars, PRD); n = len(bars); last = -1
    for k in range(len(piv) - 4):
        pv = piv[k:k+5]
        for d, kinds in (('bull', ('L', 'H', 'L', 'H', 'L')), ('bear', ('H', 'L', 'H', 'L', 'H'))):
            if tuple(p[2] for p in pv) != kinds:
                continue
            p0, p1, p2, p3, p4 = (p[1] for p in pv)
            w1 = abs(p1 - p0); w3 = abs(p3 - p2)
            if d == 'bull':
                ok = p2 > p0 and p3 > p1 and p4 > p1 and p4 < p3 and w3 >= w1
            else:
                ok = p2 < p0 and p3 < p1 and p4 < p1 and p4 > p3 and w3 >= w1
            if not ok or w1 <= 0:
                continue
            start = pv[4][0] + PRD + 1
            entry_i = None; invalid = False
            for j in range(start, min(start + BREAK_WIN, n - 1)):
                b = bars[j]
                if d == 'bull':
                    if b['l'] < p4:
                        invalid = True; break          # broke wave-4 low -> count invalid
                    if b['h'] > p3:
                        entry_i = j + 1; break
                else:
                    if b['h'] > p4:
                        invalid = True; break
                    if b['l'] < p3:
                        entry_i = j + 1; break
            if invalid or entry_i is None or entry_i <= last or entry_i >= n:
                continue
            entry = bars[entry_i]['o']; a = atr(bars, 14, entry_i - 1) or 0.0
            stop = (p4 - ATR_BUF * a) if d == 'bull' else (p4 + ATR_BUF * a)
            native = entry + w1 * NATIVE_W5 if d == 'bull' else entry - w1 * NATIVE_W5
            _score_all(store, 'wave5', tf, bars, entry_i, entry, stop, d, native)
            _score_all(store_cls[cls], 'wave5', tf, bars, entry_i, entry, stop, d, native)
            last = entry_i + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<22} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R  OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list))
    npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        series = {'h1': h1, '4h': agg4h(h1), 'daily': daily}
        for tf, bars in series.items():
            if len(bars) < 120:
                continue
            detect_wave3(bars, tf, store, cls, store_cls)
            detect_wave5(bars, tf, store, cls, store_cls)

    print(f"Elliott Wave 3rd & 5th — {npairs} pairs · next-bar fills · realistic cost · OOS split\n")
    for strat in ('wave3', 'wave5'):
        print(f"=== {strat.upper()} ===")
        for tf in ('h1', '4h', 'daily'):
            for tag in ('RR1.6', 'RR2.0', 'native'):
                line(f"{tf} {tag}", store[(strat, tf, tag)])
        print()

    # per-class breakdown for each strategy's best-looking config (daily native + 4h RR2)
    print("Per-class (daily · native measured-move target):")
    for strat in ('wave3', 'wave5'):
        print(f"  --- {strat} ---")
        for c in ['comm', 'crypto', 'index', 'major', 'minor']:
            line(c, store_cls[c][(strat, 'daily', 'native')])
        print()


if __name__ == '__main__':
    main()
