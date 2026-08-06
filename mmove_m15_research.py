"""Port the H1 'money-move' (FVG retrace-continuation) detector down to m15.

mmove is a validated structure edge on H1 (crypto) / H4 (indices, commodities):
a >=1-ATR impulse leaves a 3-candle fair-value gap, price retraces into the gap
and holds, then continues in the impulse direction — stop beyond the gap, RR2.
This runs the SAME detector (imported from unified_shadow_harness, not
re-implemented) on m15 bars across the universe, to see whether the imbalance
edge survives at the 15-minute timeframe.

Discipline: MARKET fills at the confirmation-bar close, fixed dealing cost,
chronological OOS split (BOTH halves positive + n>=40 = PASS). RR2, matching
the live mmove. Hold is swept (the native 80-bar H1 hold = 80h; on m15 the same
bar count is only 20h, so we also test longer holds to give the 2:1 target room).

Run: python mmove_m15_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from unified_shadow_harness import _mmove_signals

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
TF = 'm15'
RR = 2.0
HOLDS = [80, 192, 384]     # 20h / 48h / 96h on m15
CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd'}


def walk_rr(bars, ei, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(ei, min(ei + hold, len(bars))):
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


def run_pair(bars, hold):
    idx = {b['_ts']: i for i, b in enumerate(bars)}
    rows = []
    for sig in _mmove_signals(bars, 'x', 'mmove_m15', 'm15'):
        ei = idx.get(sig['entry_ts'])
        if ei is None:
            continue
        o = walk_rr(bars, ei, sig['entry'], sig['stop'], sig['dir'], RR, hold)
        if o is not None:
            rows.append((sig['entry_ts'], o - cost(o, sig['entry'], abs(sig['entry'] - sig['stop']))))
    return rows


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<12} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 96)
    print('mmove (FVG retrace-continuation) ported to m15 — RR2, market fills+cost, OOS')
    print('=' * 96)
    for hold in HOLDS:
        print(f"\n===== hold = {hold} bars ({hold * 15 // 60}h) =====")
        per_pair = {}
        for pk in d:
            bars = _bars_norm(d.get(pk, {}).get(TF, []))
            if len(bars) < 400:
                continue
            r = run_pair(bars, hold)
            if r:
                per_pair[pk] = r
        allrows = [r for pk in per_pair for r in per_pair[pk]]
        crypto = [r for pk in per_pair if pk in CRYPTO for r in per_pair[pk]]
        other = [r for pk in per_pair if pk not in CRYPTO for r in per_pair[pk]]
        line('ALL', allrows)
        if crypto:
            line('crypto', crypto)
        if other:
            line('non-crypto', other)
        # per-pair — surface any pocket that passes both OOS halves on its own
        print("      -- per-pair (n>=40 only) --")
        strong = []
        for pk in sorted(per_pair, key=lambda k: -len(per_pair[k])):
            rows = per_pair[pk]
            if len(rows) < 40:
                continue
            rows_s = sorted(rows)
            n, wr, e = agg([r for _, r in rows_s])
            m = len(rows_s) // 2
            _, _, eh = agg([r for _, r in rows_s[:m]])
            _, _, es = agg([r for _, r in rows_s[m:]])
            tag = 'PASS' if (e > 0 and eh > 0 and es > 0) else 'fail'
            if tag == 'PASS':
                strong.append(pk)
            print(f"        {pk:<10} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {tag}")
        if strong:
            print(f"      >>> pockets passing both OOS halves at hold={hold}: {', '.join(strong)}")


if __name__ == '__main__':
    main()
