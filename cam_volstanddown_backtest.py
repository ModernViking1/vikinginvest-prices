"""cam_rev HIGH-VOLATILITY STAND-DOWN overlay — from the 'optimised' Rust sibling (Overlay 4).

Idea: a fade needs the level to HOLD; in an elevated-volatility regime it doesn't. Measure the
current ATR(14) relative to its own rolling average (completed bars) and stand down (skip the fade)
when the ratio is in the top tier. Self-scaling across instruments/regimes.

This tests it ON TOP of the current live cam_rev — H.detect_cam_rev already bakes in the close entry,
the momentum-of-break guard and regime tiering — so it measures INCREMENTAL value. Faithful:
H.detect_cam_rev + H.score_sess (bracket-honest, resolved-only). Reports KEPT vs the SKIPPED high-vol
cohort across stand-down multiples, with OOS halves. Commits nothing.

  python cam_volstanddown_backtest.py --hist deep-ohlc.json     # 3yr (CI)
"""
import argparse, bisect, json
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'index', 'comm'}
ATR_N = 14
AVG_N = 20            # rolling-average lookback for the ATR ratio (Rust default vol_avg_len)
MULTS = [1.3, 1.5, 2.0]   # 1.3 = the Rust default vol_standdown_mult


def atr_series(bars, n):
    tr = [0.0] * len(bars)
    for i in range(1, len(bars)):
        h, l, pc = bars[i]['h'], bars[i]['l'], bars[i-1]['c']
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))
    out = [None] * len(bars)
    if len(bars) <= n:
        return out
    s = sum(tr[1:n+1]) / n; out[n] = s
    for i in range(n+1, len(bars)):
        s = (s*(n-1) + tr[i]) / n; out[i] = s
    return out


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


def line(label, a, base=None):
    if not a:
        print(f"  {label:32} (n<1)"); return
    d = f"  Δexp {a['exp']-base['exp']:+.3f}" if base else ""
    print(f"  {label:32} n={a['n']:<5} WR={a['wr']:4.0f}%  exp={a['exp']:+.3f}R  tot={a['tot']:+6.0f}{d}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {}); recs = []
    for pk, layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or not isinstance(layers, dict):
            continue
        m15 = _bars_norm(layers.get('m15') or []); daily = _bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]; atr = atr_series(m15, ATR_N)
        for s in H.detect_cam_rev(pk, m15, daily):
            st, o = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], H.CAM_HOLD)
            if st != 'resolved':
                continue
            i = bisect.bisect_left(ts, s['entry_ts']) - 1        # trigger (rejection) bar
            if i < ATR_N + AVG_N or atr[i] is None:
                continue
            prior = [a for a in atr[i-AVG_N:i] if a is not None]   # completed bars only
            if len(prior) < AVG_N:
                continue
            avg = sum(prior) / len(prior)
            if avg <= 0:
                continue
            recs.append((s['entry_ts'], o, atr[i] / avg))
    R = [x[1] for x in recs]; base = agg(R); o1, o2 = oos([(x[0], x[1]) for x in recs])
    print(f"=== cam_rev · HIGH-VOL STAND-DOWN overlay · {len(recs)} resolved fills · {args.hist} ===")
    print("(ATR(14) vs its own 20-bar average; stand down when ratio > mult. On top of live cam_rev.)\n")
    print(">> BASELINE (no stand-down):"); line('all', base)
    print(f"    OOS halves: {o1:+.3f} / {o2:+.3f}\n")
    for mult in MULTS:
        kept = [(x[0], x[1]) for x in recs if x[2] <= mult]
        skip = [(x[0], x[1]) for x in recs if x[2] > mult]
        ak = agg([r for _, r in kept]); as_ = agg([r for _, r in skip])
        ret = 100.0*len(kept)/len(recs) if recs else 0; k1, k2 = oos(kept)
        print(f">> STAND DOWN if ATR > {mult}x avg:   (retain {ret:.0f}%)")
        line('   KEPT (fade)', ak, base); print(f"        OOS: {k1:+.3f}/{k2:+.3f}")
        line('   SKIPPED (high-vol cohort)', as_, base)
        print()
    print("Reading it: the overlay helps only if the SKIPPED high-vol cohort is a genuine net loser")
    print("(materially worse than baseline) AND removing it lifts KEPT expectancy in both OOS halves.")


if __name__ == '__main__':
    main()
