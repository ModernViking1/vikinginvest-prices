"""9EMA/20SMA pullback-runner (ma_pull) — confirm the 9EMA number + orthogonality vs the
existing equity m15 strategies (holygrail_eq_m15, turtle_soup_eq_m15).

For each equity symbol's m15 bars:
  - generate ma_pull signals (scan from ma_pullback_research, 9EMA) and score with the trailing exit,
  - generate holygrail_eq_m15 + turtle_soup_eq_m15 signals (the live m15 book, trend-gated),
  - measure OVERLAP (same symbol, same direction, entry within OVERLAP_BARS) and the trailing
    expectancy of the NON-OVERLAPPING ma_pull signals — the truly additive edge.

    python ma_pull_check.py --equity <all_eq.json>
"""
import json, argparse, bisect
import unified_shadow_harness as H
from unified_shadow_harness import score_trail_open, TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST
from backtest_rsi_per_class import _bars_norm
from ma_pullback_research import scan        # uses FAST_EMA=9
from five_strategies_research import cost

OVERLAP_BARS = 8        # same sym+dir within this many m15 bars = overlapping


def _mw_m15(bars, pk, tag, gen):
    raw = H._mw_signals(bars, pk, tag, 'm15', gen)
    return H.trend_filter_signals(bars, raw)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--equity', required=True)
    args = ap.parse_args()
    eq = json.load(open(args.equity)).get('pairs', {})

    ma_all, ma_only = [], []            # trailing R-multiples
    n_ma = n_overlap = 0
    exist_total = 0
    for key, layers in eq.items():
        m15 = _bars_norm(layers.get('m15') or [])
        if len(m15) < 400:
            continue
        ts = [b['_ts'] for b in m15]
        # existing live m15 signals on this symbol -> sorted (ts, dir)
        exist = []
        for tag, gen in (('holygrail_eq_m15', H._holygrail_sig), ('turtle_soup_eq_m15', H._turtlesoup_sig)):
            for s in _mw_m15(m15, key, tag, gen):
                exist.append((s['entry_ts'], s['dir']))
        exist.sort()
        exist_total += len(exist)
        ets = [e[0] for e in exist]
        for d, ei, entry, stop, sig_ts in scan(m15):
            st, o = score_trail_open(m15, sig_ts, entry, stop, d, TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
            if st != 'resolved':
                continue
            r = o - cost(o, entry, abs(entry-stop))
            n_ma += 1; ma_all.append(r)
            # overlap: any existing signal same dir within OVERLAP_BARS bars (by ts proximity)
            lo = bisect.bisect_left(ets, sig_ts)
            hit = False
            for j in range(max(0, lo-3), min(len(exist), lo+3)):
                if exist[j][1] == d and abs(ets[j]-sig_ts) <= OVERLAP_BARS * 15*60:
                    hit = True; break
            if hit:
                n_overlap += 1
            else:
                ma_only.append(r)

    def stats(rs):
        n = len(rs); w = sum(1 for r in rs if r > 0); m = n//2
        return (n, 100*w/max(1, n), sum(rs)/max(1, n), sum(rs),
                sum(rs[:m])/max(1, m), sum(rs[m:])/max(1, n-m)) if n else None

    print(f"ma_pull (9EMA/20SMA pullback-runner) vs existing equity m15 book — orthogonality\n")
    a = stats(ma_all); print(f"  ma_pull ALL      n={a[0]:<6} WR={a[1]:.0f}% exp={a[2]:+.3f}R totR={a[3]:+.0f} OOS[{a[4]:+.2f}/{a[5]:+.2f}]")
    print(f"  existing m15 signals (holygrail+turtle m15): {exist_total}")
    print(f"  ma_pull OVERLAP with existing: {n_overlap}/{n_ma} = {100*n_overlap/max(1,n_ma):.0f}%")
    b = stats(ma_only)
    if b:
        print(f"  ma_pull NON-overlap (additive) n={b[0]:<6} WR={b[1]:.0f}% exp={b[2]:+.3f}R totR={b[3]:+.0f} OOS[{b[4]:+.2f}/{b[5]:+.2f}]")
    print("\nAdditive if overlap is low AND the non-overlap set keeps a positive, OOS-stable expectancy.")


if __name__ == '__main__':
    main()
