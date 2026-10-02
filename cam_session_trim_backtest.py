"""cam_rev session-trim test: does closing the session earlier (21:00 or 20:00 UTC instead of 22:00)
improve things by cutting the thin late-session hours? Scores all live cam_rev signals (close entry,
fixed 1:1, bracket-honest) and compares entry-hour cutoffs. Resolved-only. Commits nothing.

  python cam_session_trim_backtest.py --hist deep-ohlc.json
"""
import argparse, json
from datetime import datetime, timezone
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'index', 'comm'}
CUTOFFS = [22, 21, 20]   # session close hour (UTC); 22 = current live


def agg(rs):
    n = len(rs)
    if not n:
        return None
    return dict(n=n, wr=100.0*sum(1 for r in rs if r > 0)/n, exp=sum(rs)/n, tot=sum(rs))


def oos(recs):
    recs = sorted(recs, key=lambda x: x[0]); m = len(recs)//2
    a = agg([r for _, r in recs[:m]]); b = agg([r for _, r in recs[m:]])
    return (a['exp'] if a else 0), (b['exp'] if b else 0)


def line(label, a, base=None):
    if not a:
        print(f"  {label:26} (n<1)"); return
    d = f"  Δexp {a['exp']-base['exp']:+.3f}" if base else ""
    print(f"  {label:26} n={a['n']:<5} WR={a['wr']:4.0f}%  exp={a['exp']:+.3f}R  tot={a['tot']:+7.0f}{d}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    recs = []  # (entry_ts, R, hour)
    for pk, layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or not isinstance(layers, dict):
            continue
        m15 = _bars_norm(layers.get('m15') or []); daily = _bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        for s in H.detect_cam_rev(pk, m15, daily):
            st, o = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], H.CAM_HOLD)
            if st != 'resolved':
                continue
            recs.append((s['entry_ts'], o, datetime.fromtimestamp(s['entry_ts'], timezone.utc).hour))
    base = agg([r for _, r, _ in recs])
    print(f"=== cam_rev · SESSION-TRIM test · {len(recs)} resolved fills · {args.hist} ===")
    print("(close entry, fixed 1:1, bracket-honest; current live close = 22:00 UTC)\n")
    print(">> BASELINE (close 22:00 = all entries 07-21):"); line('all', base)
    b1, b2 = oos([(t, r) for t, r, _ in recs]); print(f"    OOS: {b1:+.3f}/{b2:+.3f}\n")
    for cut in CUTOFFS:
        kept = [(t, r) for t, r, h in recs if h < cut]
        dropped = [(t, r) for t, r, h in recs if h >= cut]
        k1, k2 = oos(kept)
        print(f">> CLOSE at {cut:02d}:00  (retain {100.0*len(kept)/len(recs):.0f}%)")
        line('   KEPT', agg([r for _, r in kept]), base); print(f"        OOS: {k1:+.3f}/{k2:+.3f}")
        line('   DROPPED (late hours)', agg([r for _, r in dropped]), base)
        print()
    print("Reading it: trimming helps only if the DROPPED late cohort is materially worse than baseline")
    print("AND KEPT lifts in both OOS halves. If the dropped cohort is still clearly positive, you're")
    print("just forgoing profit for a cosmetic expectancy bump.")


if __name__ == '__main__':
    main()
