"""Does a relative-volume gate rescue any of the OTHER shelved methods?

Sweeps a relative-volume gate (signal-bar volume / its 20-bar average) over the
methods we shelved on price alone. IMPORTANT: uses STRICT BRACKET scoring
(target-or-stop, timeouts EXCLUDED) — the same discipline the harness and cBot
use — after discovering that a mark-to-market timeout convention had falsely
'passed' VWAP mean-reversion. So these numbers are directly comparable to the
observers.

Methods swept:
  mmove_m15   FVG retrace-continuation, m15 universe (RR2)   [scoped pockets already wired]
  ORB         opening-range breakout, m15 (target = range)
  Bollinger   band mean-reversion, m15 (target = mid band)
  volbreak    Williams volatility breakout, DAILY (RR2)

REL swept 0 (no gate) .. 2.0. Crypto (real volume) reported vs ALL. n>=40 +
both OOS halves + = PASS.

Run: python volume_sweep_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from intraday_industry_research import sig_orb, sig_bbmr, _day_index          # noqa: F401
from unified_shadow_harness import _mmove_signals
from volatility_breakout_research import signals as volbreak_signals, UNIVERSE as VOLBRK_UNIVERSE

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RELS = [0.0, 1.2, 1.5, 2.0]
VOL_LB = 20
CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd', 'suiusd', 'taousd', 'nearusd'}


def relvol(bars, i):
    if i < VOL_LB:
        return None
    avg = sum(b.get('v', 0) or 0 for b in bars[i - VOL_LB:i]) / VOL_LB
    if avg <= 0:
        return None
    return (bars[i].get('v', 0) or 0) / avg


def bracket(bars, i0, entry, stop, d, target, hold):
    """target-or-stop; timeout EXCLUDED (None). Realised RR vs stop distance."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= target:
                return (target - entry) / R
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= target:
                return (entry - target) / R
    return None


def _iter(method, bars):
    """Yield (signal_bar_idx, entry_idx, entry, stop, dir, target) for a method."""
    if method == 'mmove_m15':
        idx = {b['_ts']: k for k, b in enumerate(bars)}
        for s in _mmove_signals(bars, 'x', 'm', 'm15'):
            ei = idx.get(s['entry_ts'])
            if ei is None:
                continue
            R = abs(s['entry'] - s['stop'])
            tgt = s['entry'] + 2 * R if s['dir'] == 'bull' else s['entry'] - 2 * R
            yield ei - 1, ei, s['entry'], s['stop'], s['dir'], tgt
    elif method == 'ORB':
        for (ei, entry, stop, d, extra) in sig_orb(bars):
            tgt = entry + extra if d == 'bull' else entry - extra
            yield ei - 1, ei, entry, stop, d, tgt
    elif method == 'Bollinger':
        for (ei, entry, stop, d, extra) in sig_bbmr(bars):
            yield ei - 1, ei, entry, stop, d, extra       # extra = mid band
    elif method == 'volbreak':
        for (ei, entry, stop, d) in volbreak_signals(bars, 0.6, True):
            R = abs(entry - stop)
            tgt = entry + 2 * R if d == 'bull' else entry - 2 * R
            yield ei - 1, ei, entry, stop, d, tgt


HOLDS = {'mmove_m15': 192, 'ORB': 40, 'Bollinger': 40, 'volbreak': 5}
TFS = {'mmove_m15': 'm15', 'ORB': 'm15', 'Bollinger': 'm15', 'volbreak': 'daily'}


def run(d, method, rel):
    hold = HOLDS[method]; tf = TFS[method]
    minlen = 120 if tf == 'daily' else 400   # daily series are ~253 bars
    allrows, crypto = [], []
    for pk in d:
        # volbreak is a futures-like DAILY method — scope it to its universe, not
        # every pair (running it universe-wide was a false-positive scoping bug).
        if method == 'volbreak' and pk not in VOLBRK_UNIVERSE:
            continue
        bars = _bars_norm(d.get(pk, {}).get(tf, []))
        if len(bars) < minlen:
            continue
        rows = []
        for (sig_i, ei, entry, stop, dr, tgt) in _iter(method, bars):
            if ei >= len(bars):
                continue
            if rel > 0:
                rv = relvol(bars, sig_i)
                if rv is None or rv < rel:
                    continue
            o = bracket(bars, ei, entry, stop, dr, tgt, hold)
            if o is not None:
                rows.append((bars[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
        allrows.extend(rows)
        if pk in CRYPTO:
            crypto.extend(rows)
    return allrows, crypto


def stat(rows):
    rows = sorted(rows)
    n, wr, e = agg([r for _, r in rows])
    m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]])
    _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    return n, wr, e, eh, es, v


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 100)
    print('Volume-gate sweep over shelved methods — STRICT bracket scoring (timeouts excluded)')
    print('=' * 100)
    for method in ('mmove_m15', 'ORB', 'Bollinger', 'volbreak'):
        print(f"\n===== {method} =====")
        for scope in ('ALL', 'crypto'):
            if method == 'volbreak' and scope == 'crypto':
                continue   # daily futures-like universe has no crypto
            print(f"  -- {scope} --")
            for rel in RELS:
                allrows, crypto = run(d, method, rel)
                rows = crypto if scope == 'crypto' else allrows
                n, wr, e, eh, es, v = stat(rows)
                gate = 'no gate' if rel == 0 else f'relvol>={rel}'
                print(f"      {gate:<12} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


if __name__ == '__main__':
    main()
