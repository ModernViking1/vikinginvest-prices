"""'FX STATE PRIVATE' (Julien L.) intraday sweep-reversal with 3 scaled TPs.

From the signals screenshots: an M5 structure indicator marks swing highs/lows; trade
plans post an Entry, a single SL, and THREE scaled take-profits ("open using 3 separate
positions"). The recurring entry logic in the commentary is a liquidity sweep at a
range/session extreme that reverses ("the Asian low's been swept, price ready to bounce")
— buy a swept swing-low that reclaims, sell a swept swing-high that reclaims. SLs seen
were ~17-20 pips with TPs at roughly 0.75R / 1.5R / 2.25R.

Encoding: fractal swing pivots; a later bar whose wick sweeps a recent pivot extreme and
CLOSES back inside (reclaim) is the entry (market, bar close); SL just beyond the sweep
extreme. We report the three TP levels standalone AND the blended 3-position exit (equal
size, shared SL) — both plain and with SL->breakeven after TP1. Realistic fills, dealing
cost, OOS split, per class. NOTE: our data has no m5, so m15 / h1 / 4h are tested.

Run: python fxstate_sweep_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
FRAC = 3
LOOKFWD = 30          # bars after a pivot to find the sweep
BUF = 0.20            # SL buffer beyond the sweep extreme (ATR)
COOLDOWN = 3
TPS = [0.75, 1.5, 2.25]     # the three scaled take-profits
HOLD = 80


def pivots(bars, k):
    hi, lo = [], []
    for i in range(k, len(bars) - k):
        seg = bars[i - k:i + k + 1]
        if bars[i]['h'] == max(x['h'] for x in seg): hi.append(i)
        if bars[i]['l'] == min(x['l'] for x in seg): lo.append(i)
    return hi, lo


def _fixed(bars, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def _scaled(bars, i0, entry, stop, d, be_after_tp1):
    """Blended R of 3 equal positions sharing SL, TPs at TPS. Optional SL->BE after TP1."""
    R = abs(entry - stop)
    tgts = [entry + t * R if d == 'bull' else entry - t * R for t in TPS]
    hit = [False, False, False]; sl = stop
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        # targets first (intrabar order unknown; give TP the benefit as the channel would bank it)
        for k in range(3):
            if not hit[k]:
                if d == 'bull' and b['h'] >= tgts[k]: hit[k] = True
                if d == 'bear' and b['l'] <= tgts[k]: hit[k] = True
        if be_after_tp1 and hit[0]:
            sl = entry
        stopped = (b['l'] <= sl) if d == 'bull' else (b['h'] >= sl)
        if stopped:
            outs = []
            for k in range(3):
                outs.append(TPS[k] if hit[k] else (0.0 if (be_after_tp1 and hit[0]) else -1.0))
            return sum(outs) / 3
        if all(hit):
            return sum(TPS) / 3
    # ran out of data: banked TPs count, rest exit ~flat
    outs = [TPS[k] if hit[k] else 0.0 for k in range(3)]
    return sum(outs) / 3


def scan(bars, store, cls, pk):
    hi, lo = pivots(bars, FRAC); n = len(bars); last = -1
    piv = sorted([(i, 'l') for i in lo] + [(i, 'h') for i in hi])
    for pidx, side in piv:
        lvl = bars[pidx]['l'] if side == 'l' else bars[pidx]['h']
        for j in range(pidx + FRAC + 1, min(pidx + FRAC + 1 + LOOKFWD, n - 1)):
            if j <= last:
                continue
            b = bars[j]
            if side == 'l' and b['l'] < lvl and b['c'] > lvl:        # swept low, reclaimed -> BUY
                a = atr(bars, 14, j) or 0.0
                entry = b['c']; stop = b['l'] - BUF * a
                if stop < entry:
                    _emit(bars, j + 1, entry, stop, 'bull', store, cls, pk); last = j + COOLDOWN
                break
            if side == 'h' and b['h'] > lvl and b['c'] < lvl:        # swept high, reclaimed -> SELL
                a = atr(bars, 14, j) or 0.0
                entry = b['c']; stop = b['h'] + BUF * a
                if stop > entry:
                    _emit(bars, j + 1, entry, stop, 'bear', store, cls, pk); last = j + COOLDOWN
                break


def _emit(bars, ei, entry, stop, d, store, cls, pk):
    if ei >= len(bars):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']
    for rr in TPS + [1.0, 2.0]:
        o = _fixed(bars, ei, entry, stop, d, rr)
        if o is not None:
            store[cls][('rr', rr)].append((ts, o - cost(o, entry, R)))
    for tag, be in (('blend', False), ('blendBE', True)):
        o = _scaled(bars, ei, entry, stop, d, be)
        store[cls][(tag, 0)].append((ts, o - cost(1 if o > 0 else -1, entry, R)))


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<10} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        if tf == '4h':
            h1 = _bars_norm(pairs[pk].get('h1', []))
            if len(h1) < 300: continue
            bars = make_4h(h1)
        else:
            bars = _bars_norm(pairs[pk].get(tf, []))
        if len(bars) < 300: continue
        npr += 1
        scan(bars, store, cls, pk)
    print(f"\n===== SWEEP-REVERSAL · {tf} — {npr} pairs =====")
    keys = [('rr', 0.75), ('rr', 1.5), ('rr', 2.25), ('blend', 0), ('blendBE', 0)]
    labels = {('rr', 0.75): 'TP1 .75R', ('rr', 1.5): 'TP2 1.5R', ('rr', 2.25): 'TP3 2.25R',
              ('blend', 0): '3TP blend', ('blendBE', 0): '3TP +BE'}
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        print(f"  {c}:")
        for k in keys:
            line(labels[k], store[c][k])
    print("  ALL pooled:")
    for k in keys:
        line(labels[k], [r for c in store for r in store[c][k]])


def main():
    print("=" * 90)
    print("FX STATE sweep-reversal (buy swept lows / sell swept highs) + 3 scaled TPs")
    print("=" * 90)
    for tf in ('m15', 'h1', '4h'):
        run(tf)


if __name__ == '__main__':
    main()
