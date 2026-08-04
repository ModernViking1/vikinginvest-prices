"""Exit / profit-taking scheme comparison (book p.75: 'partial at 1:1, exit rest at 3R').

Takes a large, identical set of real continuation entries (order-block retraces, the family
where runners actually matter) across all pairs, and replays each entry under competing exit
rules — so the comparison is apples-to-apples on the SAME trades:

  fix1        single target 1:1
  fix2        single target 2:1  (current cBot default)
  fix3        single target 3:1
  book_1_3    50% off at 1R -> stop to breakeven -> runner (50%) to 3R   (the book's scheme)
  thirds123   1/3 at 1R, 1/3 at 2R, 1/3 at 3R, shared stop               (our gold scheme)
  thirds_be   thirds123 but stop -> breakeven after the 1R leg fills

Realistic cost, first-touch resolution (stop wins on an ambiguous bar = conservative for the
partial/runner schemes), mark-to-market at timeout. Reports mean R/trade (expectancy) per
scheme, overall + per class + chronological OOS halves.

Run: python exit_scheme_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import _obfvg_signals
from five_strategies_research import cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
HOLD = 120

SCHEMES = {
    'fix1':      {'legs': [(1.0, 1.0)], 'be': False},
    'fix2':      {'legs': [(1.0, 2.0)], 'be': False},
    'fix3':      {'legs': [(1.0, 3.0)], 'be': False},
    'book_1_3':  {'legs': [(0.5, 1.0), (0.5, 3.0)], 'be': True},
    'thirds123': {'legs': [(1 / 3, 1.0), (1 / 3, 2.0), (1 / 3, 3.0)], 'be': False},
    'thirds_be': {'legs': [(1 / 3, 1.0), (1 / 3, 2.0), (1 / 3, 3.0)], 'be': True},
}


def simulate(bars, i0, entry, stop0, d, legs, be):
    R0 = abs(entry - stop0)
    if R0 <= 0:
        return None
    def px(rmult):
        return entry + rmult * R0 if d == 'bull' else entry - rmult * R0
    remaining = list(legs); stop_R = -1.0; banked = 0.0
    frac_left = sum(f for f, _ in legs)
    end = min(i0 + HOLD, len(bars))
    for j in range(i0, end):
        b = bars[j]; sp = px(stop_R)
        hit_stop = (b['l'] <= sp) if d == 'bull' else (b['h'] >= sp)
        if hit_stop:                                   # conservative: adverse touch resolves first
            return banked + frac_left * stop_R
        prog = True
        while remaining and prog:
            prog = False
            f, tR = remaining[0]; tp = px(tR)
            hit = (b['h'] >= tp) if d == 'bull' else (b['l'] <= tp)
            if hit:
                banked += f * tR; frac_left -= f; remaining.pop(0); prog = True
                if be and stop_R < 0.0:
                    stop_R = 0.0
        if not remaining:
            return banked
    last = bars[end - 1]['c']                          # timeout -> mark remainder to market
    mtm = (last - entry) / R0 if d == 'bull' else (entry - last) / R0
    return banked + frac_left * max(mtm, stop_R)


def run():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    # store[scheme][class] -> list[(ts, netR)]
    store = {s: defaultdict(list) for s in SCHEMES}; nsig = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 500:
            continue
        ts = [b['_ts'] for b in h1]
        for s in _obfvg_signals(pk, h1, 'x'):
            i0 = bisect.bisect_left(ts, s['entry_ts'])
            if i0 >= len(h1):
                continue
            nsig += 1
            for name, cfg in SCHEMES.items():
                r = simulate(h1, i0, s['entry'], s['stop'], s['dir'], cfg['legs'], cfg['be'])
                if r is not None:
                    net = r - cost(1.0 if r > 0 else -1.0, s['entry'], abs(s['entry'] - s['stop']))
                    store[name][cls].append((s['entry_ts'], net))
    return store, nsig


def stats(rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n = len(seq)
    if not n:
        return 0, 0, 0, 0, 0
    e = sum(seq) / n; w = 100 * sum(1 for x in seq if x > 0) / n
    mid = n // 2
    eh = sum(x for _, x in rows[:mid]) / max(1, mid); es = sum(x for _, x in rows[mid:]) / max(1, n - mid)
    return n, w, e, eh, es


def main():
    print("=" * 96)
    print("Exit-scheme comparison on identical order-block continuation entries — realistic fills")
    print("=" * 96)
    store, nsig = run()
    print(f"entries evaluated: ~{nsig // len(SCHEMES)} per scheme\n")
    classes = ['comm', 'crypto', 'index', 'major', 'minor']
    print(f"  {'scheme':<11} {'ALL exp':>9} {'WR%':>6} {'OOS h1/h2':>16}   per-class expectancy")
    for name in ['fix1', 'fix2', 'fix3', 'book_1_3', 'thirds123', 'thirds_be']:
        allrows = [r for c in classes for r in store[name][c]]
        n, w, e, eh, es = stats(allrows)
        pc = '  '.join(f"{c[:4]}:{stats(store[name][c])[2]:+.2f}" for c in classes)
        star = '  *best' if False else ''
        print(f"  {name:<11} {e:>+8.3f}R {w:>5.1f}% [{eh:>+6.3f}/{es:>+6.3f}]   {pc}")
    # rank by ALL expectancy
    ranked = sorted(SCHEMES, key=lambda nm: -stats([r for c in classes for r in store[nm][c]])[2])
    print(f"\n  best expectancy: {ranked[0]}  (compare fix2 = current cBot default)")


if __name__ == '__main__':
    main()
