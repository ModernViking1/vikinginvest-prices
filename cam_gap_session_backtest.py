"""cam_rev — gap-proximity veto (A) + off-session-fill (D) backtest.

Two hypotheses raised from a live EURSGD long that stopped out filling a gap:

  A) GAP VETO. A cam_rev fade run straight into an unfilled fair-value gap (FVG) on
     its ADVERSE side is fighting a liquidity magnet. Test a SELECTION filter (skip
     the signal — geometry untouched, so the 1:1 RR and stop-based sizing are
     unchanged) rather than moving the structurally-pinned S4/R4 stop.

  D) OFF-SESSION FILL. cam_rev triggers only in-session (07-22 UTC) but the resting
     LIMIT can fill any hour until expiry. A touch in thin off-session liquidity is a
     "not best fill" the in-session backtest never saw. Test (i) how off-session fills
     perform vs in-session, and (ii) a session-only fill rule.

Faithful to live: signals via the production detect_cam_rev; fills via a resting limit
at ref_entry (mirrors the cBot); bracket-honest WR on RESOLVED fills; OOS halves on the
kept set. Commits nothing.

  Fast pass:  python cam_gap_session_backtest.py                 # ~90d m15 (historical-ohlc.json)
  3-yr pass:  python cam_gap_session_backtest.py --hist deep-ohlc.json   # in CI after deep fetch
"""
import argparse
import bisect
import json
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS

CLASSES = {'major', 'minor', 'index', 'comm'}          # the LIVE cam_rev classes (crypto is demo)
CRYPTO = getattr(H, 'CAM_CRYPTO_PILOT', {'btcusd', 'ethusd', 'xrpusd', 'solusd'})
BLACKLIST = {'xptusd'}
EXPIRY_BARS = 48                                       # 12h at m15 — mirrors swing EXPIRY_HOURS
SESS_OPEN, SESS_CLOSE = H.CAM_SESS_OPEN, H.CAM_SESS_CLOSE   # 07-22 UTC
MAXLB = 288                                             # max gap-lookback horizon (3 trading days)

# Veto grid: gap must form within LOOKBACK m15 bars before the trigger, be UNFILLED at the
# trigger, sit on the adverse side, be at least MINSZ (in R) tall, and its near edge at least
# DEPTH (in R) adverse of entry.  DEPTH>=1.0 == the gap begins at/beyond the stop (price must
# blow the stop to fill it).
LOOKBACKS = [96, 288]        # ~1 day, ~3 days
DEPTHS = [0.0, 0.5, 1.0]     # near edge: any-below / >=halfway-to-stop / at-or-beyond-stop
MINSZS = [0.0, 0.25]         # min gap height in R


def find_gaps(bars):
    """All 3-bar FVGs, each as (form_k, zlo, zhi, fill_k). fill_k = first bar index within
    MAXLB after formation whose range re-enters the zone (None => unfilled within horizon).
    Geometry-based (both up- and down-gaps) so the adverse-side test is symmetric."""
    n = len(bars); gaps = []
    for k in range(1, n - 1):
        hprev, lprev = bars[k - 1]['h'], bars[k - 1]['l']
        if bars[k + 1]['l'] > hprev:            # up-gap: void below
            zlo, zhi = hprev, bars[k + 1]['l']
        elif bars[k + 1]['h'] < lprev:          # down-gap: void above
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


def adverse_gap(gaps, it, entry, stop, d, lookback, depth_min, sz_min):
    """True if an unfilled adverse FVG matches the veto config as of trigger index `it`."""
    R = abs(entry - stop)
    if R <= 0:
        return False
    lo_k = it - lookback
    for (k, zlo, zhi, fill) in gaps:
        if k < lo_k or k + 1 > it:                       # formed in window & before entry
            continue
        if fill is not None and fill <= it:              # already filled by trigger time
            continue
        if d == 'bull':
            if zhi > entry:                              # gap not fully below entry
                continue
            depth = (entry - zhi) / R
        else:
            if zlo < entry:                              # gap not fully above entry
                continue
            depth = (zlo - entry) / R
        if depth >= depth_min and (zhi - zlo) / R >= sz_min:
            return True
    return False


def sim_limit(bars, ts, sig, hold, session_only=False):
    """Resting limit at ref_entry. Returns (status, R|None, fill_hour_utc|None).
    session_only: only accept a touch on an in-session (07-22 UTC) bar."""
    refpx, stop, target, d = sig['entry'], sig['stop'], sig['target'], sig['dir']
    R = abs(refpx - stop)
    if R <= 0:
        return ('nofill', None, None)
    i0 = bisect.bisect_left(ts, sig['entry_ts'])
    fill = None
    for j in range(i0, min(i0 + EXPIRY_BARS, len(bars))):
        b = bars[j]
        touch = (d == 'bull' and b['l'] <= refpx) or (d == 'bear' and b['h'] >= refpx)
        if not touch:
            continue
        if session_only:
            hr = datetime.fromtimestamp(b['_ts'], timezone.utc).hour
            if not (SESS_OPEN <= hr < SESS_CLOSE):
                continue                                  # skip off-session touch, keep scanning
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
    """rows: list of (entry_ts, R|None, filled_bool). Bracket-honest on RESOLVED, OOS halves."""
    rows = sorted(rows, key=lambda r: r[0])
    n = len(rows)
    if not n:
        return None
    filled = sum(1 for r in rows if r[2])
    rs = [r[1] for r in rows if r[1] is not None]
    m = len(rs) // 2
    h1, h2 = rs[:m], rs[m:]
    return {
        'n': n, 'fill_pct': 100.0 * filled / n, 'nres': len(rs),
        'wr': (100.0 * sum(1 for r in rs if r > 0) / len(rs)) if rs else 0.0,
        'exp': (sum(rs) / len(rs)) if rs else 0.0, 'totR': sum(rs),
        'oos1': (sum(h1) / len(h1)) if h1 else 0.0,
        'oos2': (sum(h2) / len(h2)) if h2 else 0.0,
    }


def line(tag, v):
    if v:
        print(f"  {tag:22} n={v['n']:<5} res={v['nres']:<5} WR={v['wr']:>3.0f}%  "
              f"exp={v['exp']:+.3f}R  totR={v['totR']:+.0f}  OOS1={v['oos1']:+.3f} OOS2={v['oos2']:+.3f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='historical-ohlc.json')
    args = ap.parse_args()
    d = json.load(open(args.hist))
    pairs = d.get('pairs', d)
    hold = H.CAM_HOLD

    # Build the per-signal record set once: (cls, R-under-limit, fill_hour, resolved?, adverse-flags).
    recs = []                     # each: dict with cls, ets, lim_r, filled, fhr, sess_r(status), flags{}
    n_raw = 0
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        crypto = pk in CRYPTO
        if (cls not in CLASSES and not crypto) or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or []); daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]
        gaps = find_gaps(m15)
        for s in H.detect_cam_rev(pk, m15, daily):
            n_raw += 1
            it = bisect.bisect_left(ts, s['entry_ts']) - 1
            st, r, fhr = sim_limit(m15, ts, s, hold)                       # live behaviour (any-hour fill)
            st_s, r_s, _ = sim_limit(m15, ts, s, hold, session_only=True)  # D-fix (session-only fill)
            flags = {}
            for lb in LOOKBACKS:
                for T in DEPTHS:
                    for mz in MINSZS:
                        flags[(lb, T, mz)] = adverse_gap(gaps, it, s['entry'], s['stop'], s['dir'], lb, T, mz)
            recs.append({
                'cls': 'crypto' if crypto else cls, 'ets': s['entry_ts'],
                'st': st, 'r': r if st == 'resolved' else None, 'filled': st != 'nofill', 'fhr': fhr,
                'st_s': st_s, 'r_s': r_s if st_s == 'resolved' else None, 'filled_s': st_s != 'nofill',
                'flags': flags,
            })

    live = [x for x in recs if x['cls'] in ('major', 'minor', 'index', 'comm')]   # the live universe

    def rows_of(xs):
        return [(x['ets'], x['r'], x['filled']) for x in xs]

    print(f"=== cam_rev · GAP-VETO (A) + OFF-SESSION (D) · {n_raw} raw signals · {args.hist} ===")
    print(f"(live classes only: {sorted(CLASSES)}; bracket-honest WR on resolved limit fills; RR1 break-even = 50%)\n")

    base = agg(rows_of(live))
    print(">> BASELINE — resting limit, any-hour fill (current live behaviour):")
    line('all', base)

    # ---------- A: gap-proximity veto ----------
    print("\n>> A. GAP-PROXIMITY VETO  (KEPT = signals we'd still take; VETOED = dropped)")
    print("   want: VETOED markedly worse than baseline, KEPT >= baseline, high retention.")
    best = None
    for lb in LOOKBACKS:
        for T in DEPTHS:
            for mz in MINSZS:
                key = (lb, T, mz)
                kept = [x for x in live if not x['flags'][key]]
                veto = [x for x in live if x['flags'][key]]
                vk = agg(rows_of(kept)); vv = agg(rows_of(veto))
                if not vv or vv['nres'] < 8:
                    continue     # too few vetoed to be meaningful
                retain = 100.0 * len(kept) / len(live)
                tag = f"lb{lb} depth>={T} sz>={mz}"
                print(f"\n  [{tag}]  retain {retain:.0f}% of signals")
                line('  KEPT', vk); line('  VETOED(dropped)', vv)
                # score a config by how much it lifts KEPT exp while dropping >=1% and keeping OOS aligned
                if vk and retain >= 90 and vk['exp'] > (base['exp'] if base else 0):
                    gain = (vk['exp'] - base['exp']) if base else 0
                    if best is None or gain > best[1]:
                        best = (tag, gain, vk, vv, retain)
    if best:
        tag, gain, vk, vv, retain = best
        print(f"\n  >> best veto config: [{tag}]  KEPT exp {vk['exp']:+.3f}R vs base {base['exp']:+.3f}R "
              f"(+{gain:.3f}R), retain {retain:.0f}%, dropped set exp {vv['exp']:+.3f}R")
    else:
        print("\n  >> no veto config beat baseline at >=90% retention — gap veto NOT supported on this window.")

    # ---------- D: off-session fill ----------
    print("\n>> D. OFF-SESSION FILL")
    res = [x for x in live if x['st'] == 'resolved']
    ins = [x for x in res if x['fhr'] is not None and SESS_OPEN <= x['fhr'] < SESS_CLOSE]
    off = [x for x in res if x['fhr'] is not None and not (SESS_OPEN <= x['fhr'] < SESS_CLOSE)]
    print(f"   resolved limit fills: {len(res)}  |  in-session {len(ins)}  off-session {len(off)}"
          f"  ({100.0*len(off)/max(1,len(res)):.0f}% off-session)")
    line('in-session fills', agg(rows_of(ins)))
    line('off-session fills', agg(rows_of(off)))
    # D-fix: session-only fill rule across the whole live universe
    rows_fix = [(x['ets'], x['r_s'], x['filled_s']) for x in live]
    print("   D-fix — session-only fill rule (skip off-session touches):")
    line('all (session-only)', agg(rows_fix))
    line('all (baseline)', base)


if __name__ == '__main__':
    main()
