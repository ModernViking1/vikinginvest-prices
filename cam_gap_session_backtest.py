"""cam_rev — gap-veto (A) + off-session (D) + clearance-to-target (B) + regime gate (C) backtest.

Raised from a live EURSGD long that stopped out filling a gap in thin liquidity. All four are
SELECTION filters (skip the signal — geometry / 1:1 RR / stop-based sizing untouched) except D,
which caps the resting limit to the session:

  A) GAP VETO           — skip a fade run into an unfilled adverse fair-value gap (FVG).
  D) OFF-SESSION FILL   — cap the resting limit to the trigger day's session close (22:00 UTC), so
                          it can only fill in-session. Faithful to the live guardrail in swing_signals.
  B) CLEARANCE-TO-TARGET— skip a fade whose path to the 1R target is blocked by an unfilled FVG.
  C) REGIME GATE        — cam_rev is a fade but (unlike twob/po3) is NOT in REVERSAL_STRATS, so it
                          bypasses the chop/ADX gate. C1 tests applying that gate; C2 tests daily
                          EMA50 trend-alignment (fade WITH vs AGAINST the higher-TF trend).

Faithful to live: production detect_cam_rev; resting-limit fill mirroring the cBot; bracket-honest
WR on RESOLVED fills; OOS halves on every kept set. Commits nothing.

  Fast:  python cam_gap_session_backtest.py                        # ~90d (historical-ohlc.json)
  3-yr:  python cam_gap_session_backtest.py --hist deep-ohlc.json  # in CI (restores deep cache)
"""
import argparse
import bisect
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from trend_regime import build_regime, CHOP_LO

CLASSES = {'major', 'minor', 'index', 'comm'}          # the LIVE cam_rev classes (crypto is demo)
CRYPTO = getattr(H, 'CAM_CRYPTO_PILOT', {'btcusd', 'ethusd', 'xrpusd', 'solusd'})
BLACKLIST = {'xptusd'}
EXPIRY_BARS = 48                                       # 12h at m15 — mirrors swing EXPIRY_HOURS
SESS_OPEN, SESS_CLOSE = H.CAM_SESS_OPEN, H.CAM_SESS_CLOSE   # 07-22 UTC
MAXLB = 288                                             # gap lookback horizon (3 trading days)

# A veto grid (settled NOT worth it on 3yr — kept as a one-line confirmation).
A_LB, A_DEPTH, A_SZ = 96, 1.0, 0.25
# B: path-blocking gap must be >= this tall (in R).
B_SIZES = [0.0, 0.15, 0.30]
# C2: daily EMA period for the trend-alignment test.
EMA_N = 50


def _sess_close_ts(entry_ts):
    dt = datetime.fromtimestamp(entry_ts, timezone.utc)
    c = dt.replace(hour=SESS_CLOSE, minute=0, second=0, microsecond=0)
    if entry_ts >= c.timestamp():
        c += timedelta(days=1)
    return c.timestamp()


def find_gaps(bars):
    """All 3-bar FVGs as (form_k, zlo, zhi, fill_k). fill_k = first bar within MAXLB after formation
    whose range re-enters the zone (None => unfilled within horizon). Geometry-based (both gap types)."""
    n = len(bars); gaps = []
    for k in range(1, n - 1):
        hprev, lprev = bars[k - 1]['h'], bars[k - 1]['l']
        if bars[k + 1]['l'] > hprev:
            zlo, zhi = hprev, bars[k + 1]['l']
        elif bars[k + 1]['h'] < lprev:
            zlo, zhi = bars[k + 1]['h'], lprev
        else:
            continue
        fill = None
        for j in range(k + 2, min(n, k + 2 + MAXLB)):
            b = bars[j]
            if b['l'] <= zhi and b['h'] >= zlo:
                fill = j; break
        gaps.append((k, zlo, zhi, fill))
    return gaps


def adverse_gap(gaps, it, entry, stop, d, lb, depth_min, sz_min):
    """A: unfilled adverse FVG (below entry for a long / above for a short), near edge >= depth_min·R
    adverse of entry, >= sz_min·R tall, formed within lb bars and unfilled at trigger `it`."""
    R = abs(entry - stop)
    if R <= 0:
        return False
    for (k, zlo, zhi, fill) in gaps:
        if k < it - lb or k + 1 > it or (fill is not None and fill <= it):
            continue
        if d == 'bull':
            if zhi > entry:
                continue
            depth = (entry - zhi) / R
        else:
            if zlo < entry:
                continue
            depth = (zlo - entry) / R
        if depth >= depth_min and (zhi - zlo) / R >= sz_min:
            return True
    return False


def gap_in_path(gaps, it, entry, target, d, sz_min):
    """B: unfilled FVG intersecting the path from entry to target (the profit side), >= sz_min·R tall."""
    R = abs(target - entry)
    if R <= 0:
        return False
    lo_p, hi_p = (entry, target) if d == 'bull' else (target, entry)
    for (k, zlo, zhi, fill) in gaps:
        if k < it - MAXLB or k + 1 > it or (fill is not None and fill <= it):
            continue
        if zhi > lo_p and zlo < hi_p and (zhi - zlo) >= sz_min * R:   # overlaps the path
            return True
    return False


def sim_limit(bars, ts, sig, hold, cap_ts=None):
    """Resting limit at ref_entry. cap_ts: stop accepting fills at/after this epoch (session cap).
    Returns (status, R|None, fill_hour_utc|None)."""
    refpx, stop, target, d = sig['entry'], sig['stop'], sig['target'], sig['dir']
    R = abs(refpx - stop)
    if R <= 0:
        return ('nofill', None, None)
    i0 = bisect.bisect_left(ts, sig['entry_ts'])
    fill = None
    for j in range(i0, min(i0 + EXPIRY_BARS, len(bars))):
        b = bars[j]
        if cap_ts is not None and b['_ts'] >= cap_ts:
            break
        if (d == 'bull' and b['l'] <= refpx) or (d == 'bear' and b['h'] >= refpx):
            fill = j; break
    if fill is None:
        return ('nofill', None, None)
    fhr = datetime.fromtimestamp(bars[fill]['_ts'], timezone.utc).hour
    end = min(fill + hold, len(bars))
    for j in range(fill, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:   return ('resolved', -1.0, fhr)
            if b['h'] >= target: return ('resolved', (target - refpx) / R, fhr)
        else:
            if b['h'] >= stop:   return ('resolved', -1.0, fhr)
            if b['l'] <= target: return ('resolved', (refpx - target) / R, fhr)
    return ('expired', None, fhr)


def agg(rows):
    """rows: (entry_ts, R|None, filled_bool). Bracket-honest on RESOLVED, OOS halves."""
    rows = sorted(rows, key=lambda r: r[0])
    n = len(rows)
    if not n:
        return None
    filled = sum(1 for r in rows if r[2])
    rs = [r[1] for r in rows if r[1] is not None]
    m = len(rs) // 2
    h1, h2 = rs[:m], rs[m:]
    return {'n': n, 'fill_pct': 100.0 * filled / n, 'nres': len(rs),
            'wr': (100.0 * sum(1 for r in rs if r > 0) / len(rs)) if rs else 0.0,
            'exp': (sum(rs) / len(rs)) if rs else 0.0, 'totR': sum(rs),
            'oos1': (sum(h1) / len(h1)) if h1 else 0.0, 'oos2': (sum(h2) / len(h2)) if h2 else 0.0}


def line(tag, v, base=None):
    if v:
        d = f"  (Δexp {v['exp']-base['exp']:+.3f})" if base else ""
        print(f"  {tag:24} n={v['n']:<5} res={v['nres']:<5} WR={v['wr']:>3.0f}%  "
              f"exp={v['exp']:+.3f}R  totR={v['totR']:+.0f}  OOS1={v['oos1']:+.3f} OOS2={v['oos2']:+.3f}{d}")


def _chop_at(regime, ets):
    ts, adx, chop = regime
    i = bisect.bisect_right(ts, ets) - 1
    return (chop[i] if 0 <= i < len(chop) else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='historical-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    hold = H.CAM_HOLD
    recs = []; n_raw = 0
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk); crypto = pk in CRYPTO
        if (cls not in CLASSES and not crypto) or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or []); daily = H._bars_norm(layers.get('daily') or [])
        h1 = H._bars_norm(layers.get('h1') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]
        gaps = find_gaps(m15)
        regime = build_regime(h1) if len(h1) >= 40 else None
        dts = [b['_ts'] for b in daily]; dcl = [b['c'] for b in daily]
        dema = H.ema(dcl, EMA_N) if len(dcl) >= EMA_N else [None] * len(dcl)
        for s in H.detect_cam_rev(pk, m15, daily):
            n_raw += 1
            it = bisect.bisect_left(ts, s['entry_ts']) - 1
            st, r, fhr = sim_limit(m15, ts, s, hold)                              # live: any-hour fill
            st_c, r_c, _ = sim_limit(m15, ts, s, hold, cap_ts=_sess_close_ts(s['entry_ts']))  # D guardrail
            chop = _chop_at(regime, s['entry_ts']) if regime else None
            di = bisect.bisect_left(dts, s['entry_ts']) - 1                       # last COMPLETED daily bar
            aligned = None
            if 0 <= di < len(dema) and dema[di] is not None:
                up = dcl[di] > dema[di]
                aligned = (up if s['dir'] == 'bull' else (not up))               # fade WITH the daily trend?
            recs.append({
                'cls': 'crypto' if crypto else cls, 'ets': s['entry_ts'],
                'st': st, 'r': r if st == 'resolved' else None, 'filled': st != 'nofill', 'fhr': fhr,
                'r_c': r_c if st_c == 'resolved' else None, 'filled_c': st_c != 'nofill',
                'A': adverse_gap(gaps, it, s['entry'], s['stop'], s['dir'], A_LB, A_DEPTH, A_SZ),
                'B': {sz: gap_in_path(gaps, it, s['entry'], s['target'], s['dir'], sz) for sz in B_SIZES},
                'chop': chop, 'aligned': aligned,
            })
    live = [x for x in recs if x['cls'] in CLASSES]
    R = lambda xs: [(x['ets'], x['r'], x['filled']) for x in xs]
    base = agg(R(live))
    print(f"=== cam_rev · A/B/C/D filters · {n_raw} raw signals · {args.hist} ===")
    print(f"(live classes {sorted(CLASSES)}; bracket-honest WR on resolved limit fills; RR1 break-even = 50%)\n")
    print(">> BASELINE — resting limit, any-hour fill (current live behaviour):")
    line('all', base)

    # A — confirmation only (settled: not worth it)
    print(f"\n>> A. GAP VETO (confirmation, config lb{A_LB}/depth{A_DEPTH}/sz{A_SZ}):")
    line('KEPT (no adverse gap)', agg(R([x for x in live if not x['A']])), base)
    line('VETOED (dropped)', agg(R([x for x in live if x['A']])), base)

    # B — clearance to target
    print("\n>> B. CLEARANCE-TO-TARGET (KEPT = clear path; VETOED = gap blocks path to TP):")
    for sz in B_SIZES:
        kept = [x for x in live if not x['B'][sz]]; veto = [x for x in live if x['B'][sz]]
        vv = agg(R(veto))
        if not vv or vv['nres'] < 20:
            continue
        retain = 100.0 * len(kept) / len(live)
        print(f"  [gap>= {sz}R]  retain {retain:.0f}%")
        line('  KEPT', agg(R(kept)), base); line('  VETOED(dropped)', vv, base)

    # C1 — chop/ADX reversal gate (cam_rev is currently NOT gated)
    print(f"\n>> C1. REGIME GATE — apply the reversal chop gate (keep only chop >= {CHOP_LO}):")
    gated = [x for x in live if x['chop'] is not None and x['chop'] >= CHOP_LO]
    skipd = [x for x in live if x['chop'] is not None and x['chop'] < CHOP_LO]
    print(f"   (retain {100.0*len(gated)/max(1,len(live)):.0f}% of signals)")
    line('GATED kept (chop>=LO)', agg(R(gated)), base)
    line('SKIPPED (strong trend)', agg(R(skipd)), base)

    # C2 — daily EMA50 trend alignment
    print(f"\n>> C2. TREND ALIGNMENT — fade WITH vs AGAINST the daily EMA{EMA_N}:")
    witht = [x for x in live if x['aligned'] is True]
    againt = [x for x in live if x['aligned'] is False]
    print(f"   (with-trend {100.0*len(witht)/max(1,len(live)):.0f}%  against-trend {100.0*len(againt)/max(1,len(live)):.0f}%)")
    line('WITH daily trend', agg(R(witht)), base)
    line('AGAINST daily trend', agg(R(againt)), base)

    # D — off-session fill + the guardrail actually implemented (session-close cap)
    print("\n>> D. OFF-SESSION FILL + guardrail:")
    res = [x for x in live if x['st'] == 'resolved']
    off = [x for x in res if x['fhr'] is not None and not (SESS_OPEN <= x['fhr'] < SESS_CLOSE)]
    ins = [x for x in res if x['fhr'] is not None and SESS_OPEN <= x['fhr'] < SESS_CLOSE]
    print(f"   resolved {len(res)} | in-session {len(ins)} off-session {len(off)} ({100.0*len(off)/max(1,len(res)):.1f}% off)")
    line('in-session fills', agg(R(ins)))
    line('off-session fills', agg(R(off)))
    line('GUARDRAIL (session-cap)', agg([(x['ets'], x['r_c'], x['filled_c']) for x in live]), base)


if __name__ == '__main__':
    main()
