"""cam_rev — event-window / time-of-day analysis (is it worth standing down around major releases?).

No economic calendar in the repo, so events are proxied from the clock (they're time-deterministic):
  NFP           = first Friday of the month, release 13:30 UTC (08:30 ET)
  US data window= weekday entries in the 12:00-15:00 UTC band (08:30 ET releases: NFP/CPI/PPI/claims)
  FOMC proxy    = Wednesday 17:00-19:00 UTC (statement 18:00/press-conf 18:30 UTC)

cam_rev FADES, so a violent directional release runs every open fade over at once — the filter
case is strongest here. Faithful to live: close entry, fixed 1:1, bracket-honest (H.detect_cam_rev
+ H.score_sess), resolved-only. Buckets by ENTRY time (cam_rev holds up to 24h, so entry-window is
a fair proxy for 'trading around the event'). Commits nothing.

  python cam_event_window_backtest.py --hist deep-ohlc.json     # 3yr (CI)
"""
import argparse, bisect, json
from datetime import datetime, timezone
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'index', 'comm'}


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
        print(f"  {label:32} (n<1)"); return
    d = f"  Δexp {a['exp']-base['exp']:+.3f}" if base else ""
    print(f"  {label:32} n={a['n']:<5} WR={a['wr']:4.0f}%  exp={a['exp']:+.3f}R  tot={a['tot']:+7.0f}{d}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    recs = []   # (entry_ts, R, hour, weekday, is_first_fri)
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
            dt = datetime.fromtimestamp(s['entry_ts'], timezone.utc)
            recs.append((s['entry_ts'], o, dt.hour, dt.weekday(), dt.weekday() == 4 and dt.day <= 7))
    base = agg([r for _, r, *_ in recs]); b1, b2 = oos([(t, r) for t, r, *_ in recs])
    print(f"=== cam_rev · EVENT-WINDOW analysis · {len(recs)} resolved fills · {args.hist} ===")
    print("(entry-time buckets; events proxied from the clock — no calendar)\n")
    print(">> BASELINE (all entries):"); line('all', base); print(f"    OOS: {b1:+.3f}/{b2:+.3f}\n")

    # per-hour
    print(">> By ENTRY HOUR (UTC)  [13:00 = US 08:30 ET release hour; 18:00 = FOMC]:")
    for h in range(24):
        a = agg([r for _, r, hr, _, _ in recs if hr == h])
        if a and a['n'] >= 20:
            flag = '  <<< US-release hour' if h == 13 else ('  <<< FOMC hour' if h == 18 else '')
            line(f'   {h:02d}:00', a, base);
    print()

    # NFP (first Friday) cohorts
    print(">> NFP proxy (first Friday of month):")
    nfp = [(t, r) for t, r, hr, wd, ff in recs if ff]
    nfp_win = [(t, r) for t, r, hr, wd, ff in recs if ff and 12 <= hr < 15]
    non_nfp = [(t, r) for t, r, hr, wd, ff in recs if not ff]
    line('first-Friday (all hrs)', agg([r for _, r in nfp]), base)
    line('first-Friday 12-15 UTC (release)', agg([r for _, r in nfp_win]), base)
    line('all other days', agg([r for _, r in non_nfp]), base)
    if nfp_win:
        n1, n2 = oos(nfp_win); print(f"    NFP-release-window OOS: {n1:+.3f}/{n2:+.3f}")
    print()

    # broad US release window (any weekday) + FOMC proxy
    print(">> Time-window proxies (any weekday):")
    us = [(t, r) for t, r, hr, wd, ff in recs if wd < 5 and 12 <= hr < 15]
    non_us = [(t, r) for t, r, hr, wd, ff in recs if not (wd < 5 and 12 <= hr < 15)]
    fomc = [(t, r) for t, r, hr, wd, ff in recs if wd == 2 and 17 <= hr < 19]
    line('US window 12-15 UTC', agg([r for _, r in us]), base)
    line('outside US window', agg([r for _, r in non_us]), base)
    line('Wed 17-19 UTC (FOMC proxy)', agg([r for _, r in fomc]), base)
    print("\nReading it: a filter is worth it only if an event window is MATERIALLY worse (ideally net")
    print("negative) AND removing it lifts the rest. Even a mildly-positive window can still be worth")
    print("standing down if the losses CLUSTER (every fade run over at once = concentrated drawdown).")


if __name__ == '__main__':
    main()
