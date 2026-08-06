"""#b — port sweeprev (swept-extreme reversal, structural target) to m15.

sweeprev sweeps a prior swing extreme, enters on the break back through the last
counter pivot, and targets the PREVIOUS OPPOSITE swing (variable structural RR).
It's minor-4H live. This runs the EXACT core (now-extracted _sweeprev_signals) on
m15 across the universe, scored against its own structural target via the harness
score_sweeprev (bracket-honest: timeouts excluded), chronological OOS.

n>=40 + both OOS halves + = PASS. Reports ALL / crypto / per-pair pockets. Hold
swept. If a pocket passes it becomes intraday observer #5.

Run: python sweeprev_m15_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from unified_shadow_harness import _sweeprev_signals, score_sweeprev

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
HOLDS = [96, 192]     # 24h / 48h on m15
CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd', 'suiusd', 'taousd', 'nearusd'}


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<12} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")
    return n, e, eh, es


def run_pair(pk, m15, hold):
    rows = []
    for s in _sweeprev_signals(m15, pk, 'sweeprev_m15', 'm15'):
        st, o = score_sweeprev(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], hold)
        if st == 'resolved':
            rows.append((s['entry_ts'], o - cost(o, s['entry'], abs(s['entry'] - s['stop']))))
    return rows


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 96)
    print('sweeprev (swept-extreme reversal, structural target) ported to m15 — bracket-honest, OOS')
    print('=' * 96)
    for hold in HOLDS:
        print(f"\n===== hold = {hold} bars ({hold * 15 // 60}h) =====")
        per_pair = {}
        for pk in d:
            m15 = _bars_norm(d.get(pk, {}).get('m15', []))
            if len(m15) < 400:
                continue
            r = run_pair(pk, m15, hold)
            if r:
                per_pair[pk] = r
        allrows = [r for pk in per_pair for r in per_pair[pk]]
        crypto = [r for pk in per_pair if pk in CRYPTO for r in per_pair[pk]]
        line('ALL', allrows)
        line('crypto', crypto)
        strong = []
        for pk in sorted(per_pair, key=lambda k: -len(per_pair[k])):
            rows = per_pair[pk]
            if len(rows) < 40:
                continue
            n, e, eh, es = line('  ' + pk, rows)
            if e > 0 and eh > 0 and es > 0:
                strong.append(pk)
        if strong:
            print(f"      >>> pockets passing both OOS halves at hold={hold}: {', '.join(strong)}")


if __name__ == '__main__':
    main()
