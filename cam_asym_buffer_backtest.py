"""cam_rev — long/short asymmetry + stop-buffer sweep.

Two questions:
  A. Is there a long (S3) vs short (R3) asymmetry in WR / expectancy worth exploiting?
  B. Is the stop buffer (0.10 x the R3-R4 gap) justified, or arbitrary? Sweep it and see where
     expectancy peaks — overall and per side.

Faithful to the LIVE strategy: close entry (H.detect_cam_rev), fixed RR=CAM_RR, bracket-honest
score (H.score_sess). For each buffer the stop/target are RECOMPUTED from the day's Camarilla
levels (H._cam_levels), so the sweep is apples-to-apples. Resolved-only. Commits nothing.

  python cam_asym_buffer_backtest.py --hist deep-ohlc.json     # 3yr (CI)
"""
import argparse, bisect, json
from datetime import datetime, timezone
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'index', 'comm'}
BUFFERS = [0.0, 0.05, 0.10, 0.20, 0.35, 0.50]   # x the R3-R4 (S3-S4) gap; 0.10 = current live
RR = H.CAM_RR


def agg(rs):
    n = len(rs)
    if not n:
        return None
    w = sum(1 for r in rs if r > 0)
    return dict(n=n, wr=100.0*w/n, exp=sum(rs)/n, tot=sum(rs))


def oos(recs):
    recs = sorted(recs, key=lambda x: x[0]); m = len(recs)//2
    a = agg([r for _, r in recs[:m]]); b = agg([r for _, r in recs[m:]])
    return (a['exp'] if a else 0), (b['exp'] if b else 0)


def line(label, a):
    if not a:
        print(f"  {label:26} (n<1)"); return
    print(f"  {label:26} n={a['n']:<6} WR={a['wr']:4.0f}%  exp={a['exp']:+.3f}R  tot={a['tot']:+7.0f}")


def _levels_for(m15, ts, levels, s):
    i = bisect.bisect_left(ts, s['entry_ts']) - 1
    day = datetime.fromtimestamp(m15[max(i, 0)]['_ts'], timezone.utc).strftime('%Y-%m-%d')
    return levels.get(day)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    # recs[buf] = list of (entry_ts, R, dir, cls)
    recs = {b: [] for b in BUFFERS}
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        if cls not in CLASSES or not isinstance(layers, dict):
            continue
        m15 = _bars_norm(layers.get('m15') or []); daily = _bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]; levels = H._cam_levels(daily)
        for s in H.detect_cam_rev(pk, m15, daily):
            L = _levels_for(m15, ts, levels, s)
            if not L:
                continue
            entry, d = s['entry'], s['dir']
            for buf in BUFFERS:
                if d == 'bear':
                    stop = L['R4'] + buf * (L['R4'] - L['R3']); R = stop - entry; tgt = entry - RR * R
                else:
                    stop = L['S4'] - buf * (L['S3'] - L['S4']); R = entry - stop; tgt = entry + RR * R
                if R <= 0:
                    continue
                st, o = H.score_sess(m15, s['entry_ts'], entry, stop, tgt, d, H.CAM_HOLD)
                if st == 'resolved':
                    recs[buf].append((s['entry_ts'], o, d, cls))

    base = recs[0.10]
    print(f"=== cam_rev · LONG/SHORT asymmetry + STOP-BUFFER sweep · {len(base)} fills @buf0.10 · {args.hist} ===")
    print("(close entry, fixed 1:1, bracket-honest, resolved-only)\n")

    # ---- A. long/short asymmetry at the live buffer (0.10) ----
    print(">> A. LONG (S3) vs SHORT (R3) at buffer 0.10:")
    longs = [(t, r) for t, r, d, c in base if d == 'bull']
    shorts = [(t, r) for t, r, d, c in base if d == 'bear']
    lo1, lo2 = oos(longs); so1, so2 = oos(shorts)
    line('ALL long', agg([r for _, r in longs])); print(f"        OOS: {lo1:+.3f}/{lo2:+.3f}")
    line('ALL short', agg([r for _, r in shorts])); print(f"        OOS: {so1:+.3f}/{so2:+.3f}")
    print("   per class (long | short):")
    for c in ['major', 'minor', 'index', 'comm']:
        lc = agg([r for t, r, d, cc in base if d == 'bull' and cc == c])
        sc = agg([r for t, r, d, cc in base if d == 'bear' and cc == c])
        ls = f"L n={lc['n']:<4} WR={lc['wr']:3.0f}% exp={lc['exp']:+.3f}" if lc else "L (n<1)"
        ss = f"S n={sc['n']:<4} WR={sc['wr']:3.0f}% exp={sc['exp']:+.3f}" if sc else "S (n<1)"
        print(f"     {c:6} {ls}   |   {ss}")

    # ---- B. stop-buffer sweep ----
    print("\n>> B. STOP-BUFFER sweep (expectancy peak = the evidence-based buffer):")
    print(f"   {'buf':>5}  {'ALL':>22}   {'LONG exp':>9}  {'SHORT exp':>9}")
    for buf in BUFFERS:
        allr = [(t, r) for t, r, d, c in recs[buf]]
        a = agg([r for _, r in allr]); o1, o2 = oos(allr)
        lexp = agg([r for t, r, d, c in recs[buf] if d == 'bull'])
        sexp = agg([r for t, r, d, c in recs[buf] if d == 'bear'])
        tag = '  <- live' if abs(buf-0.10) < 1e-9 else ''
        print(f"   {buf:>5.2f}  n={a['n']:<6} WR={a['wr']:3.0f}% exp={a['exp']:+.3f}R  "
              f"OOS {o1:+.3f}/{o2:+.3f}  L={lexp['exp']:+.3f} S={sexp['exp']:+.3f}{tag}")
    print("\nReading it: a side with materially higher exp/WR in BOTH OOS halves is a real asymmetry")
    print("(size it up, or drop the weak side). For the buffer, the exp peak is the fitted value — if")
    print("0.10 isn't near it, it's convention not evidence; if it's flat, the buffer barely matters.")


if __name__ == '__main__':
    main()
