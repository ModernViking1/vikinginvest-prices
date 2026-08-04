"""Do indices / FX majors qualify for live or observer once regime-gated?

The daily-50-EMA regime gate is now built into _obfvg_signals. This re-runs the GATED obfvg
(aligned-with-daily-50-EMA entries only) on the pairs that 'almost made it' — indices and FX
majors — scored exactly as the harness scores obfvg (h1, RR2, OBFVG_HOLD, market fill, cost),
with a chronological OOS split. Per pair + per class.

Qualification (consistent with the session):
  LIVE     : per-pair PASS — n>=40, BOTH OOS halves positive (tradeable, robust)
  OBSERVER : class-level positive with both halves +, or per-pair base-positive but thin
  NEITHER  : negative / one-sided

Also prints gated-vs-ungated so the gate's effect is visible.

Run: python obfvg_regime_candidates.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
import unified_shadow_harness as U
from five_strategies_research import agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
HOLD = U.OBFVG_HOLD
RR = 2.0
INDICES = ['de40', 'dj30', 'ftse100', 'jp225', 'nas100', 'spx500']
MAJORS = ['audusd', 'eurusd', 'gbpusd', 'nzdusd', 'usdcad', 'usdchf', 'usdjpy']


def score(h1, s):
    ts = [b['_ts'] for b in h1]; i0 = bisect.bisect_left(ts, s['entry_ts'])
    R = abs(s['entry'] - s['stop'])
    if R <= 0 or i0 >= len(h1):
        return None
    tgt = s['entry'] + RR * R if s['dir'] == 'bull' else s['entry'] - RR * R
    for j in range(i0, min(i0 + HOLD, len(h1))):
        b = h1[j]
        if s['dir'] == 'bull':
            if b['l'] <= s['stop']: o = -1.0; break
            if b['h'] >= tgt: o = RR; break
        else:
            if b['h'] >= s['stop']: o = -1.0; break
            if b['l'] <= tgt: o = RR; break
    else:
        return None
    return o - cost(o, s['entry'], R)


def series(pk, gated):
    d = json.load(open(HIST))['pairs']
    h1 = _bars_norm(d[pk].get('h1', [])); daily = _bars_norm(d[pk].get('daily', []))
    if len(h1) < 500:
        return []
    sigs = U._obfvg_signals(pk, h1, 'x', daily=(daily if gated else None))
    out = []
    for s in sigs:
        r = score(h1, s)
        if r is not None:
            out.append((s['entry_ts'], r))
    return out


def verdict(rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    if n >= 40 and e > 0 and eh > 0 and es > 0:
        v = 'PASS'
    elif e > 0 and eh > 0 and es > 0:
        v = 'thin+'
    elif e > 0:
        v = 'pos'
    else:
        v = 'neg'
    return n, w, e, eh, es, v


def main():
    print("=" * 92)
    print(f"Regime-GATED obfvg on indices + FX majors — qualify for live / observer? (gate={U.OBFVG_REGIME_GATE})")
    print("=" * 92)
    for label, pairs in (('INDICES', INDICES), ('FX MAJORS', MAJORS)):
        print(f"\n===== {label} =====")
        print(f"  {'pair':<8} {'gated: n':>8} {'WR%':>6} {'exp':>8} {'OOS h1/h2':>17}  verdict   (ungated exp)")
        classrows = []
        for pk in pairs:
            g = series(pk, True); u = series(pk, False)
            if not g:
                print(f"  {pk:<8}  (no gated signals)"); continue
            n, w, e, eh, es, v = verdict(g)
            _, _, ue, _, _, _ = verdict(u) if u else (0, 0, 0, 0, 0, '')
            classrows += g
            print(f"  {pk:<8} {n:>8} {w:>5.1f}% {e:>+7.3f} [{eh:>+6.3f}/{es:>+6.3f}]  {v:<7}   {ue:+.3f}")
        n, w, e, eh, es, v = verdict(classrows)
        print(f"  {'CLASS':<8} {n:>8} {w:>5.1f}% {e:>+7.3f} [{eh:>+6.3f}/{es:>+6.3f}]  {v}")


if __name__ == '__main__':
    main()
