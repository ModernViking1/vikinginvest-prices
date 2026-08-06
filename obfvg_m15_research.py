"""Port the OB+FVG retrace detector (obfvg) to m15 — #2 of the intraday bench build.

obfvg is an ICT order-block + fair-value-gap retrace: impulse leaves an order block
and a 3-bar gap; price retraces into the zone; enter in the impulse direction, stop
beyond the OB, RR2. It's the most intraday-native structural method we have (the ICT
day-range family) and its signal function already accepts a 15m timeframe. This runs
the EXACT live detector (_obfvg_signals, imported) on m15 across the universe.

Discipline: market fills at the retrace-bar close, dealing cost, chronological OOS,
bracket-honest scoring via the harness score() (timeouts excluded). Hold swept.
n>=40 + both OOS halves + = PASS. Reports ALL / crypto / per-pair pockets.

Run: python obfvg_m15_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from unified_shadow_harness import _obfvg_signals, score

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


def run_pair(pk, m15, daily, hold):
    rows = []
    for s in _obfvg_signals(pk, m15, 'obfvg_m15', 'm15', daily):
        st, o = score(m15, s['entry_ts'], s['entry'], s['stop'], s['dir'], hold)
        if st == 'resolved':
            rows.append((s['entry_ts'], o - cost(o, s['entry'], abs(s['entry'] - s['stop']))))
    return rows


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 96)
    print('obfvg (OB+FVG retrace) ported to m15 — RR2, market fills+cost, bracket-honest, OOS')
    print('=' * 96)
    for hold in HOLDS:
        print(f"\n===== hold = {hold} bars ({hold * 15 // 60}h) =====")
        per_pair = {}
        for pk in d:
            m15 = _bars_norm(d.get(pk, {}).get('m15', []))
            daily = _bars_norm(d.get(pk, {}).get('daily', []))
            if len(m15) < 400:
                continue
            r = run_pair(pk, m15, daily, hold)
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
