"""Research spike — does a macro RATE-DIFFERENTIAL regime filter improve the forward
expectancy of the desk's FX signals? Lined up to run the day an Anansi (or any) rates
feed is available; validated end-to-end on synthetic rates until then.

HYPOTHESIS
  FX is driven by rate differentials (carry). For a pair BASE/QUOTE, the differential
  D = rate(BASE) - rate(QUOTE). A long trade is "carry-aligned" when D > 0 (you're long
  the higher-yielder), a short when D < 0. We also test a MOMENTUM variant: aligned when
  the differential is WIDENING in the trade's direction. If aligned signals materially
  out-expect the unfiltered pool (both chronological OOS halves positive) while counter
  signals lag, the filter is worth wiring as a gate on the live FX strategies.

DATA CONTRACT  (macro-rates.json — what an Anansi fetcher must produce)
  {
    "meta":  {"source": "...", "field": "central-bank policy rate, % p.a.", "as_of": "..."},
    "rates": {
       "USD": [{"date": "2025-01-31", "rate": 5.25}, {"date": "2025-02-28", "rate": 5.25}, ...],
       "EUR": [ ... ], "GBP": [...], "JPY": [...], "CAD": [...], "CHF": [...],
       "NZD": [...], "AUD": [...], "SGD": [...], "NOK": [...], "ZAR": [...]
    }
  }
  One short-term reference rate per currency, any cadence (monthly is fine — the spike
  uses last-value-on-or-before each signal). All 11 currencies below must be present.

Read-only: joins the resolved FX signals already in swing-shadow-log.json against the
rates. No live trading, no signal changes. Run: python macro_regime_spike.py
  --make-synthetic PATH   write a synthetic rates file (mechanics validation only)
  --rates PATH            rates file to use (default macro-rates.json)
"""
import json
import os
import sys
import bisect
import random
from datetime import datetime, timezone
from collections import defaultdict

from detect_triggers import PAIR_CLASS

_HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(_HERE, 'swing-shadow-log.json')
RATES_DEFAULT = os.path.join(_HERE, 'macro-rates.json')

FX = sorted(p for p, c in PAIR_CLASS.items() if c in ('major', 'minor'))
CCYS = sorted({p[:3].upper() for p in FX} | {p[3:].upper() for p in FX})
MOM_DAYS = 90            # look-back for the differential-momentum variant (~1 quarter)
# Only gate strategies that actually run live on FX (proven set); pooled across them.
LIVE_FX = {'hs', 's5_rsi', 'twob', 'obfvg', 'engulf_manip', 'asianglitch'}


def _ts(date_str):
    return datetime.strptime(date_str[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()


def load_rates(path):
    """{ccy: ([sorted ts], [rate])}. Empty on any problem (fail-open)."""
    try:
        raw = json.load(open(path)).get('rates', {})
    except Exception:
        return {}
    out = {}
    for ccy, series in raw.items():
        pts = sorted((_ts(p['date']), float(p['rate'])) for p in series if p.get('date') and p.get('rate') is not None)
        if pts:
            out[ccy.upper()] = ([t for t, _ in pts], [r for _, r in pts])
    return out


def rate_at(rates, ccy, ts):
    s = rates.get(ccy)
    if not s:
        return None
    xs, ys = s
    i = bisect.bisect_right(xs, ts) - 1
    return ys[i] if i >= 0 else None


def diff_at(rates, base, quote, ts):
    b, q = rate_at(rates, base, ts), rate_at(rates, quote, ts)
    return None if b is None or q is None else b - q


def fx_signals():
    """Resolved forward FX signals from live FX strategies: (entry_ts, pair, dir, r, strat)."""
    log = json.load(open(LOG))
    track = log.get('tracking', {})
    base = log.get('baseline_data_end') or 0
    out = []
    for s in log.get('signals', {}).values():
        st, pk = s.get('strategy'), s.get('pair')
        if pk not in PAIR_CLASS or PAIR_CLASS[pk] not in ('major', 'minor'):
            continue
        if st not in LIVE_FX or s.get('status') != 'resolved' or 'r' not in s:
            continue
        if s.get('entry_ts', 0) <= track.get(st, base):     # genuine forward only
            continue
        out.append((s['entry_ts'], pk, s.get('dir'), s['r'], st))
    return out


def stats(seq):
    seq = sorted(seq)
    rs = [r for _, r in seq]
    n = len(rs)
    if not n:
        return (0, 0.0, 0.0, 0.0, 0.0)
    wr = 100.0 * sum(1 for x in rs if x > 0) / n
    exp = sum(rs) / n
    m = n // 2
    eh = sum(rs[:m]) / m if m else 0.0
    es = sum(rs[m:]) / (n - m) if n - m else 0.0
    return (n, wr, exp, eh, es)


def line(label, seq):
    n, wr, exp, eh, es = stats(seq)
    tag = "  <== both OOS halves +" if (n >= 40 and eh > 0 and es > 0 and exp > 0) else ""
    print("  %-22s n=%4d  WR=%2.0f%%  exp=%+0.3fR  OOS[%+0.3f/%+0.3f]%s" % (label, n, wr, exp, eh, es, tag))


def run(rates_path):
    rates = load_rates(rates_path)
    missing = [c for c in CCYS if c not in rates]
    print("=" * 78)
    print("MACRO RATE-DIFFERENTIAL REGIME SPIKE — FX live strategies, genuine forward")
    print("=" * 78)
    if not rates:
        print(f"No rates at {rates_path}. Provide macro-rates.json per the data contract in")
        print("this file's docstring, then re-run. (Or --make-synthetic to validate mechanics.)")
        return
    print(f"rates loaded for {len(rates)}/{len(CCYS)} currencies" + (f" — MISSING {missing}" if missing else ""))
    if missing:
        print("  (signals on pairs touching a missing currency are dropped from the test)")

    sigs = fx_signals()
    allb, la, lc, ma, mc = [], [], [], [], []   # all / level-aligned / level-counter / mom-aligned / mom-counter
    for ts, pk, d, r, st in sigs:
        base, quote = pk[:3].upper(), pk[3:].upper()
        D = diff_at(rates, base, quote, ts)
        if D is None:
            continue
        allb.append((ts, r))
        lvl_ok = (d == 'bull' and D > 0) or (d == 'bear' and D < 0)
        (la if lvl_ok else lc).append((ts, r))
        Dp = diff_at(rates, base, quote, ts - MOM_DAYS * 86400)
        if Dp is not None:
            mom = D - Dp
            mom_ok = (d == 'bull' and mom > 0) or (d == 'bear' and mom < 0)
            (ma if mom_ok else mc).append((ts, r))

    print(f"\njoined {len(allb)} FX signals to a rate differential (of {len(sigs)} live-FX forward signals)\n")
    print("Baseline vs carry-LEVEL filter (long the higher-yielder / short the lower):")
    line("all FX (unfiltered)", allb)
    line("carry-aligned", la)
    line("carry-counter", lc)
    print("\nDifferential-MOMENTUM filter (differential widening in trade direction):")
    line("momentum-aligned", ma)
    line("momentum-counter", mc)

    # Verdict
    a_n, _, a_exp, a_eh, a_es = stats(la)
    b_n, _, b_exp, _, _ = stats(allb)
    _, _, c_exp, _, _ = stats(lc)
    print("\nVERDICT (carry-level):")
    if a_n >= 40 and a_exp > b_exp + 0.03 and a_eh > 0 and a_es > 0 and a_exp > c_exp:
        print("  PASS — carry-aligned beats the unfiltered pool with both OOS halves + and")
        print("  out-expects counter signals. Worth wiring as an FX signal gate. Re-validate")
        print("  per-strategy + fixed-RR before any promotion.")
    else:
        print("  NO EDGE on this data — carry-level filter does not clearly beat the pool.")
        print(f"  (aligned {a_exp:+.3f}R vs all {b_exp:+.3f}R vs counter {c_exp:+.3f}R)")


def make_synthetic(path):
    """Plausible synthetic rates (monthly, 2024->now) — MECHANICS VALIDATION ONLY.
    Not real data; results are meaningless. Real Anansi rates replace this file."""
    random.seed(7)
    anchor = {'USD': 5.0, 'EUR': 3.5, 'GBP': 4.75, 'JPY': 0.25, 'CAD': 4.0, 'CHF': 1.25,
              'NZD': 4.5, 'AUD': 4.1, 'SGD': 3.4, 'NOK': 4.4, 'ZAR': 8.0}
    rates = {}
    for ccy in CCYS:
        lvl = anchor.get(ccy, 3.0)
        series = []
        for yr in (2024, 2025, 2026):
            for mo in range(1, 13):
                if yr == 2026 and mo > 9:
                    break
                lvl = max(0.0, lvl + random.uniform(-0.15, 0.15))
                series.append({'date': f'{yr}-{mo:02d}-28', 'rate': round(lvl, 2)})
        rates[ccy] = series
    json.dump({'meta': {'source': 'SYNTHETIC — mechanics validation only, not real data',
                        'field': 'synthetic short rate, % p.a.'}, 'rates': rates}, open(path, 'w'), indent=1)
    print(f"wrote synthetic rates for {len(rates)} currencies -> {path}")


if __name__ == '__main__':
    args = sys.argv[1:]
    if '--make-synthetic' in args:
        make_synthetic(args[args.index('--make-synthetic') + 1])
    else:
        rp = args[args.index('--rates') + 1] if '--rates' in args else RATES_DEFAULT
        run(rp)
