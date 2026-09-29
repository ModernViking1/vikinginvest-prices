"""cam_rev RECLAIM + FRESHNESS guards — faithful 3-yr backtest on the LIMIT-execution model.

Motivated by a live loser (EURAUD 2026-09-29): a calm R3 rejection whose resting SELL limit
filled ~2h later as price rallied back UP through the level, then ran to the R4 stop. The
momentum-of-break guard can't catch that (it scores the rejection candle, which was calm).
Two candidate guards act in the fill window instead:

  FRESHNESS cap  — the resting limit must fill within N m15 bars of the signal, else cancel.
                   Tests whether late fills (stale signal, different market state) are worse.
  RECLAIM exit   — once filled, if a bar CLOSES back beyond the faded level (bear: close>R3;
                   bull: close<S3) the fade thesis is dead -> exit at that close instead of
                   riding to the R4/S4 stop. Trades the occasional recovery for smaller losses.

Fill model = cam_limit_backtest.sim_limit (resting limit at ref_entry, touch-fill, placement
lag). Bracket-honest: unresolved-within-hold excluded; stop-first on same-bar ambiguity.
Baseline = the LIVE model today (limit, full 12h window, ride to stop/target). Commits nothing.

  python cam_reclaim_freshness_backtest.py --hist deep-ohlc.json     # 3yr (CI)
"""
import argparse
import bisect
import json
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS

CLASSES = {'major', 'minor', 'index', 'comm'}
CRYPTO = getattr(H, 'CAM_CRYPTO_PILOT', {'btcusd', 'ethusd', 'xrpusd', 'solusd'})
BLACKLIST = {'xptusd'}
EXPIRY_BARS = 48                       # 12h at m15 — the current live fill window (baseline)
LAG_BARS = 0                           # placement lag; freshness is measured from signal birth
FRESH_CAPS = [2, 4, 6, 8, 12, 16, 24, 48]   # m15 bars = 30m/1h/1.5h/2h/3h/4h/6h/12h


def _find_fill(bars, ts, sig, start_bar, end_bar):
    """First bar in [start_bar, end_bar) whose range touches ref_entry. Returns index or None."""
    refpx, d = sig['entry'], sig['dir']
    for j in range(start_bar, min(end_bar, len(bars))):
        b = bars[j]
        if d == 'bull' and b['l'] <= refpx:
            return j
        if d == 'bear' and b['h'] >= refpx:
            return j
    return None


def _score_from_fill(bars, fill, sig, level, hold, reclaim_frac):
    """Score the stop/target bracket from the fill bar. If reclaim_frac is not None, exit early
    when a bar closes past R3/S3 + reclaim_frac of the way toward the stop (frac 0 = at the level,
    1 = at the stop). Returns ('resolved', R) or ('expired', None)."""
    refpx, stop, target, d = sig['entry'], sig['stop'], sig['target'], sig['dir']
    R = abs(refpx - stop)
    rc_px = None
    if reclaim_frac is not None and level is not None:
        rc_px = level + reclaim_frac * (stop - level)     # bear: above R3; bull: below S3 (stop<level)
    end = min(fill + hold, len(bars))
    for j in range(fill, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:                       # stop first (conservative)
                return ('resolved', -1.0)
            if b['h'] >= target:
                return ('resolved', (target - refpx) / R)
            if rc_px is not None and b['c'] < rc_px:      # S3-side reclaimed on close
                return ('resolved', (b['c'] - refpx) / R)
        else:
            if b['h'] >= stop:
                return ('resolved', -1.0)
            if b['l'] <= target:
                return ('resolved', (refpx - target) / R)
            if rc_px is not None and b['c'] > rc_px:      # R3-side reclaimed on close
                return ('resolved', (refpx - b['c']) / R)
    return ('expired', None)


def sim(bars, ts, sig, level, fresh_bars, hold, reclaim_frac=None):
    """Resting limit, fill within `fresh_bars` of the signal, optional reclaim exit.
    Returns (status, R|None, filled_bool)."""
    if abs(sig['entry'] - sig['stop']) <= 0:
        return ('nofill', None, False)
    i0 = bisect.bisect_left(ts, sig['entry_ts'])
    fill = _find_fill(bars, ts, sig, i0 + LAG_BARS, i0 + fresh_bars)
    if fill is None:
        return ('nofill', None, False)
    st, o = _score_from_fill(bars, fill, sig, level, hold, reclaim_frac)
    return (st, o, True)


def _level_for(m15, ts, levels, sig):
    """R3 (bear) / S3 (bull) for the signal's rejection day, or None."""
    i0 = bisect.bisect_left(ts, sig['entry_ts'])
    rej = max(i0 - 1, 0)
    day = datetime.fromtimestamp(m15[rej]['_ts'], timezone.utc).strftime('%Y-%m-%d')
    L = levels.get(day)
    if not L:
        return None
    return L['R3'] if sig['dir'] == 'bear' else L['S3']


def _signals(pairs):
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
        levels = H._cam_levels(daily)
        for s in H.detect_cam_rev(pk, m15, daily):
            out.append((('crypto' if crypto else cls), m15, ts, s, _level_for(m15, ts, levels, s)))
    return out


def _agg(rows):
    rows = sorted(rows, key=lambda r: r[0])
    filled = [r for r in rows if r[2]]
    rs = [r[1] for r in rows if r[1] is not None]
    n = len(rows)
    if not n:
        return None
    m = len(rs) // 2
    h1, h2 = rs[:m], rs[m:]
    return dict(n=n, fill=100.0 * len(filled) / n, nres=len(rs),
                wr=(100.0 * sum(1 for r in rs if r > 0) / len(rs)) if rs else 0.0,
                exp=(sum(rs) / len(rs)) if rs else 0.0, totR=sum(rs),
                oos1=(sum(h1) / len(h1)) if h1 else 0.0,
                oos2=(sum(h2) / len(h2)) if h2 else 0.0)


def _line(label, v, base=None):
    if not v:
        print(f"  {label:34} (n<1)"); return
    d = f"  Δexp {v['exp']-base['exp']:+.3f}" if base else ""
    print(f"  {label:34} n={v['n']:<5} fill={v['fill']:>3.0f}% (res={v['nres']:<5}) "
          f"WR={v['wr']:>3.0f}%  exp={v['exp']:+.3f}R  totR={v['totR']:+6.0f}{d}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    sigs = _signals(pairs)
    hold = H.CAM_HOLD
    print(f"=== cam_rev · RECLAIM + FRESHNESS guards · {len(sigs)} raw signals · {args.hist} ===")
    print("(LIMIT fill model; bracket-honest WR on resolved fills; RR1 break-even = 50% WR)\n")

    # BASELINE = live model today: limit, full 12h window, ride to stop/target (no reclaim).
    base = defaultdict(list)
    for cls, m15, ts, s, lvl in sigs:
        st, o, f = sim(m15, ts, s, lvl, EXPIRY_BARS, hold, reclaim_frac=None)
        base[cls].append((s['entry_ts'], o if st == 'resolved' else None, f))
        base['ALL'].append((s['entry_ts'], o if st == 'resolved' else None, f))
    baseALL = _agg(base['ALL'])
    print(">> BASELINE — live limit model (12h window, ride to stop/target):")
    _line('all', baseALL)
    for cls in ['crypto', 'major', 'minor', 'index', 'comm']:
        _line(cls, _agg(base.get(cls, [])))
    print(f"    OOS halves: {baseALL['oos1']:+.3f} / {baseALL['oos2']:+.3f}\n")

    # GUARD 1 — FRESHNESS cap. KEPT = fills within N bars; also show the LATE cohort it drops.
    print(">> GUARD 1 — FRESHNESS cap (limit must fill within N m15 bars of the signal):")
    for N in FRESH_CAPS:
        kept = defaultdict(list)
        for cls, m15, ts, s, lvl in sigs:
            st, o, f = sim(m15, ts, s, lvl, N, hold, reclaim_frac=None)
            kept['ALL'].append((s['entry_ts'], o if st == 'resolved' else None, f))
        # LATE cohort = filled in (N, 48] — what the cap removes
        late = []
        for cls, m15, ts, s, lvl in sigs:
            i0 = bisect.bisect_left(ts, s['entry_ts'])
            fj = _find_fill(m15, ts, s, i0 + LAG_BARS, i0 + EXPIRY_BARS)
            if fj is not None and fj >= i0 + N:
                stt, oo = _score_from_fill(m15, fj, s, lvl, hold, reclaim_frac=None)
                late.append((s['entry_ts'], oo if stt == 'resolved' else None, True))
        v = _agg(kept['ALL'])
        print(f"  N={N:>2} ({N*15:>3}m):"); _line('   KEPT (fresh fills)', v, baseALL)
        _line('   dropped LATE cohort', _agg(late), baseALL)

    # GUARD 2 — RECLAIM early-exit, swept across the R3->stop band (frac 0 = at level, 1 = at stop).
    print("\n>> GUARD 2 — RECLAIM early-exit (exit on a close past R3/S3 + frac*(stop-level)):")
    for frac in [0.0, 0.25, 0.5, 0.75]:
        rc = defaultdict(list)
        for cls, m15, ts, s, lvl in sigs:
            st, o, f = sim(m15, ts, s, lvl, EXPIRY_BARS, hold, reclaim_frac=frac)
            rc[cls].append((s['entry_ts'], o if st == 'resolved' else None, f))
            rc['ALL'].append((s['entry_ts'], o if st == 'resolved' else None, f))
        rcALL = _agg(rc['ALL'])
        print(f"  frac={frac:.2f} (exit {'at level' if frac==0 else f'{int(frac*100)}% toward stop'}):")
        _line('   all', rcALL, baseALL)
        print(f"        OOS: {rcALL['oos1']:+.3f} / {rcALL['oos2']:+.3f}")

    # COMBO — freshness cap + a looser reclaim exit (frac 0.5).
    print("\n>> COMBO — freshness cap + reclaim exit @ frac 0.5:")
    for N in [4, 8, 16]:
        combo = []
        for cls, m15, ts, s, lvl in sigs:
            st, o, f = sim(m15, ts, s, lvl, N, hold, reclaim_frac=0.5)
            combo.append((s['entry_ts'], o if st == 'resolved' else None, f))
        print(f"  N={N:>2} + reclaim0.5:"); _line('   KEPT', _agg(combo), baseALL)

    print("\nReading it: baseline is what we run live now. A guard is worth wiring only if it lifts")
    print("expectancy AND holds in both OOS halves. Freshness trades fills for quality; reclaim")
    print("trades occasional recoveries for smaller losses on the run-overs.")


if __name__ == '__main__':
    main()
