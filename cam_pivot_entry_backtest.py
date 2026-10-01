"""cam_rev entry-price A/B — resting limit AT THE PIVOT (R3/S3) vs AT THE REJECTION CLOSE.

Our live cam_rev rests the limit at ref_entry = the rejection candle's CLOSE (below R3 for a
short). A sibling implementation (Camarilla Fade v2) instead rests it AT THE LEVEL (R3/S3).
Placing it at the level:
  - enters at a better price (sell at R3, not the lower close) with the geometry anchored to
    the pivot rather than to wherever the candle closed (less day-to-day noise), BUT
  - fills less often — price must retrace all the way back to the level; setups that reject and
    drop straight to target never fill.

This measures the trade-off faithfully on the SAME (live, momentum-guarded) signal set, through
one touch-fill model, at two fill windows (our 2h live cap and a generous 12h). Bracket-honest:
unresolved-within-hold excluded; stop-first on same-bar ambiguity. Commits nothing.

  CLOSE  (A, baseline = live):  entry=ref_entry,  stop=sig.stop,  R=stop-entry,  target=entry-/+R
  PIVOT  (B):                   entry=R3/S3,       stop=sig.stop,  R'=stop-level, target=level-/+R'

  python cam_pivot_entry_backtest.py --hist deep-ohlc.json    # 3yr (CI)
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
FILL_WINDOWS = [8, 16, 24, 48]        # m15 bars: 2h / 4h / 6h / 12h — pick the window that keeps pivot's fill without long stale/outage exposure
LAG_BARS = 0


def sim_at(bars, ts, entry_ts, entry_px, stop, target, d, fresh_bars, hold):
    """Resting limit at entry_px; fill on a touch within fresh_bars; then score the bracket.
    Returns (status, R|None, filled_bool)."""
    R = abs(entry_px - stop)
    if R <= 0:
        return ('nofill', None, False)
    i0 = bisect.bisect_left(ts, entry_ts)
    fill = None
    for j in range(i0 + LAG_BARS, min(i0 + fresh_bars, len(bars))):
        b = bars[j]
        if d == 'bull' and b['l'] <= entry_px:
            fill = j; break
        if d == 'bear' and b['h'] >= entry_px:
            fill = j; break
    if fill is None:
        return ('nofill', None, False)
    end = min(fill + hold, len(bars))
    for j in range(fill, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:   return ('resolved', -1.0, True)
            if b['h'] >= target: return ('resolved', (target - entry_px) / R, True)
        else:
            if b['h'] >= stop:   return ('resolved', -1.0, True)
            if b['l'] <= target: return ('resolved', (entry_px - target) / R, True)
    return ('expired', None, True)


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
        print(f"  {label:22} (n<1)"); return
    d = f"  Δexp {v['exp']-base['exp']:+.3f}" if base else ""
    print(f"  {label:22} n={v['n']:<5} fill={v['fill']:>3.0f}% (res={v['nres']:<5}) "
          f"WR={v['wr']:>3.0f}%  exp={v['exp']:+.3f}R  totR={v['totR']:+6.0f}{d}")


def _run(sigs, window, hold, mode):
    """mode='close' (baseline) or 'pivot'. Returns per-class defaultdict of rows."""
    rr = H.CAM_RR
    acc = defaultdict(list)
    for cls, m15, ts, s, lvl in sigs:
        if mode == 'close':
            entry_px, stop, target, d = s['entry'], s['stop'], s['target'], s['dir']
        else:
            if lvl is None:
                continue                      # can't place at the pivot without the level
            d, stop = s['dir'], s['stop']
            entry_px = lvl
            Rp = abs(entry_px - stop)
            if Rp <= 0:
                continue
            target = entry_px - rr * Rp if d == 'bear' else entry_px + rr * Rp
        st, o, f = sim_at(m15, ts, s['entry_ts'], entry_px, stop, target, d, window, hold)
        row = (s['entry_ts'], o if st == 'resolved' else None, f)
        acc[cls].append(row); acc['ALL'].append(row)
    return acc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    sigs = _signals(pairs)
    hold = H.CAM_HOLD
    order = ['ALL', 'crypto', 'major', 'minor', 'index', 'comm']
    print(f"=== cam_rev · ENTRY A/B: limit at CLOSE vs at PIVOT · {len(sigs)} signals · {args.hist} ===")
    print("(same signal set + touch-fill model; only the resting-limit PRICE differs)\n")
    for window in FILL_WINDOWS:
        cl = _run(sigs, window, hold, 'close')
        pv = _run(sigs, window, hold, 'pivot')
        baseALL = _agg(cl['ALL'])
        print(f">> FILL WINDOW {window} bars ({window*15}min):")
        print("   A — limit at REJECTION CLOSE (our live):")
        for c in order:
            _line('   '+c, _agg(cl.get(c, [])))
        print("   B — limit at PIVOT (R3/S3):")
        for c in order:
            _line('   '+c, _agg(pv.get(c, [])), baseALL if c == 'ALL' else None)
        a, b = _agg(cl['ALL']), _agg(pv['ALL'])
        print(f"   ALL OOS  close: {a['oos1']:+.3f}/{a['oos2']:+.3f}   pivot: {b['oos1']:+.3f}/{b['oos2']:+.3f}\n")
    print("Reading it: PIVOT wins only if its higher per-trade expectancy isn't paid for by too")
    print("many missed fills. Compare totR (captured edge) and fill%, and check both OOS halves.")


if __name__ == '__main__':
    main()
