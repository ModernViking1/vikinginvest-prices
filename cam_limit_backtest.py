"""Faithful 3-yr backtest of cam_rev under LIMIT execution (the new cBot behaviour).

The observer / existing stats model cam_rev as an IMMEDIATE fill at the rejection-candle
close (score_sess from entry_ts). Live now rests a LIMIT at ref_entry, which only fills if
price actually trades back to that level within the signal's validity window — a selection
effect the old model never captured. This script measures it:

For every real cam_rev signal (via the production detect_cam_rev), it simulates a resting
limit at ref_entry:
  • placement lag: start looking for the fill `lag` m15 bars after entry_ts (models the
    feed->cBot latency; swept 0 / 30 / 60 / 120 min).
  • fill: first bar in [entry_ts+lag, entry_ts+EXPIRY] whose range touches ref_entry
    (bull: low<=ref; bear: high>=ref). Fill price = ref_entry (the limit price).
  • no touch within the window  -> NO FILL (the trade the immediate model counts but the
    limit misses — usually a setup that ran straight to target without coming back).
  • once filled, score the SAME stop/target bracket from the fill bar (stop-first on a
    same-bar ambiguity = conservative), hold = CAM_HOLD, bracket-honest (expired excluded).

Baseline = score_sess from entry_ts (the existing immediate-fill model), so the two are
directly comparable. Prints per-class n / fill-rate / WR / expectancy / totR and both OOS
halves. Commits nothing. Run in CI after fetching deep m15 (see cam-limit-backtest.yml).
"""
import argparse
import bisect
import json
from collections import defaultdict

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS

CLASSES = {'major', 'minor', 'index', 'comm'}
CRYPTO = getattr(H, 'CAM_CRYPTO_PILOT', {'btcusd', 'ethusd', 'xrpusd', 'solusd'})
BLACKLIST = {'xptusd'}
EXPIRY_BARS = 48          # 12h at m15 — mirrors swing_signals EXPIRY_HOURS
LAGS = [0, 2, 4, 8]       # m15 bars of placement lag = 0 / 30 / 60 / 120 min


def sim_limit(bars, ts, sig, lag_bars, hold):
    """Simulate a resting limit at ref_entry. Returns ('nofill'|'resolved'|'expired', R|None)."""
    refpx, stop, target, d = sig['entry'], sig['stop'], sig['target'], sig['dir']
    R = abs(refpx - stop)
    if R <= 0:
        return ('nofill', None)
    i0 = bisect.bisect_left(ts, sig['entry_ts'])
    start = i0 + lag_bars
    end_fill = min(i0 + EXPIRY_BARS, len(bars))
    fill = None
    for j in range(start, end_fill):
        b = bars[j]
        if d == 'bull' and b['l'] <= refpx:
            fill = j; break
        if d == 'bear' and b['h'] >= refpx:
            fill = j; break
    if fill is None:
        return ('nofill', None)                       # price never returned to the level
    end = min(fill + hold, len(bars))
    for j in range(fill, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:   return ('resolved', -1.0)
            if b['h'] >= target: return ('resolved', (target - refpx) / R)
        else:
            if b['h'] >= stop:   return ('resolved', -1.0)
            if b['l'] <= target: return ('resolved', (refpx - target) / R)
    return ('expired', None)


def _signals(pairs):
    """All cam_rev signals across the eligible universe, tagged with class."""
    out = []
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        crypto = pk in CRYPTO
        if (cls not in CLASSES and not crypto) or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or [])
        daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]
        for s in H.detect_cam_rev(pk, m15, daily):
            out.append((('crypto' if crypto else cls), m15, ts, s))
    return out


def _agg(rows):
    """rows: list of (entry_ts, R or None, filled_bool). Bracket-honest WR on resolved."""
    rows = sorted(rows, key=lambda r: r[0])
    filled = [r for r in rows if r[2]]
    resolved = [r for r in rows if r[1] is not None]
    n = len(rows)
    if not n:
        return None
    rs = [r[1] for r in resolved]
    m = len(rs) // 2
    h1, h2 = rs[:m], rs[m:]
    return {
        'n': n, 'fill': 100.0 * len(filled) / n, 'nres': len(rs),
        'wr': (100.0 * sum(1 for r in rs if r > 0) / len(rs)) if rs else 0.0,
        'exp': (sum(rs) / len(rs)) if rs else 0.0,
        'totR': sum(rs),
        'oos1': (sum(h1) / len(h1)) if h1 else 0.0,
        'oos2': (sum(h2) / len(h2)) if h2 else 0.0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    sigs = _signals(pairs)
    hold = H.CAM_HOLD

    # Baseline: the EXISTING immediate-fill model (score_sess from entry_ts).
    base = defaultdict(list)
    for cls, m15, ts, s in sigs:
        st, o = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], hold)
        if st == 'resolved' and o is not None:
            base[cls].append((s['entry_ts'], o, True))
            base['ALL'].append((s['entry_ts'], o, True))

    # LIMIT model at each placement lag.
    lim = {lag: defaultdict(list) for lag in LAGS}
    for cls, m15, ts, s in sigs:
        for lag in LAGS:
            st, o = sim_limit(m15, ts, s, lag, hold)
            filled = st != 'nofill'
            r = o if st == 'resolved' else None
            lim[lag][cls].append((s['entry_ts'], r, filled))
            lim[lag]['ALL'].append((s['entry_ts'], r, filled))

    order = ['ALL', 'crypto', 'major', 'minor', 'index', 'comm']
    print(f"=== cam_rev · LIMIT-execution 3-yr backtest · {len(sigs)} raw signals ===")
    print("(bracket-honest WR on RESOLVED fills; break-even at RR1 is 50% WR)\n")
    print(">> BASELINE — existing immediate-fill model (score_sess from entry_ts):")
    for cls in order:
        v = _agg(base.get(cls, []))
        if v:
            print(f"  {cls:7} n={v['n']:<5} WR={v['wr']:>3.0f}%  exp={v['exp']:+.3f}R  "
                  f"totR={v['totR']:+.0f}  OOS1={v['oos1']:+.3f} OOS2={v['oos2']:+.3f}")
    for lag in LAGS:
        print(f"\n>> LIMIT — placement lag {lag*15} min:")
        for cls in order:
            v = _agg(lim[lag].get(cls, []))
            if v:
                print(f"  {cls:7} n={v['n']:<5} fill={v['fill']:>3.0f}%  (res={v['nres']:<4}) "
                      f"WR={v['wr']:>3.0f}%  exp={v['exp']:+.3f}R  totR={v['totR']:+.0f}  "
                      f"OOS1={v['oos1']:+.3f} OOS2={v['oos2']:+.3f}")
    print("\nReading it: fill% = share of signals the resting limit actually caught. A limit that")
    print("holds WR/exp near the baseline while filling a high % = execution is faithful; a big")
    print("WR/exp drop or low fill% = the limit misses the immediate-runners (selection effect).")


if __name__ == '__main__':
    main()
