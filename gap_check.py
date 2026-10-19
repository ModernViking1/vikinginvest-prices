"""Orthogonality + sanity check for the wired gap_eq detector (H._gap_signals, GO trend-aligned).

(1) Re-scores H._gap_signals on the trailing exit to confirm it reproduces the gap_research GO-align
    cell (+0.06R all), i.e. the live detector == what we validated.
(2) Measures overlap vs the existing equity book (holygrail_eq/_m15, twob_eq, volbreak_eq,
    turtle_soup_eq/_m15): a gap_eq entry "overlaps" if the book fired the SAME symbol + SAME
    direction within OVERLAP_SEC. Reports overlap % and the NON-overlap expectancy (the genuinely
    additive slice). Runs on equity-ohlc.json (native daily) or a broker export (h1-synth daily).

  python gap_check.py --hist equity-ohlc.json
"""
import argparse, json, bisect
from collections import defaultdict

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

HOLD_GO = 52
OVERLAP_SEC = 86400          # same name+dir within this window = "the same move" the book already trades (CLI --win)
BOOK = [('holygrail_eq', 'h1', H._holygrail_sig), ('twob_eq', 'h1', H._twob_sig),
        ('volbreak_eq', 'h1', H._volbreak_sig), ('turtle_soup_eq', 'h1', H._turtlesoup_sig),
        ('holygrail_eq_m15', 'm15', H._holygrail_sig), ('turtle_soup_eq_m15', 'm15', H._turtlesoup_sig)]


def _score(m15, s):
    st, o = H.score_trail_open(m15, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                               HOLD_GO, H.TRAIL_ARM, H.TRAIL_DIST)
    if st != 'resolved':
        return None
    return o - H.cost(o, s['entry'], abs(s['entry'] - s['stop']))


def agg(rows):
    rows = sorted(rows); rs = [r for _, r in rows]; n = len(rs)
    if not n:
        return None
    m = n // 2
    return n, 100*sum(1 for r in rs if r > 0)/n, sum(rs)/n, (sum(rs[:m])/m if m else 0), (sum(rs[m:])/(n-m) if n-m else 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='equity-ohlc.json')
    ap.add_argument('--win', type=int, default=OVERLAP_SEC, help='overlap window in seconds')
    args = ap.parse_args()
    win = args.win
    pairs = json.load(open(args.hist)).get('pairs', {})
    all_rows, non_rows = [], []
    n_sig = n_overlap = n_sym = 0
    for pk, layers in pairs.items():
        m15 = _bars_norm(layers.get('m15') or [])
        h1 = _bars_norm(layers.get('h1') or [])
        daily = _bars_norm(layers.get('daily') or [])
        if len(daily) < 201 and len(h1) >= 1200:
            daily = H._h1_to_daily(h1)
        if len(m15) < 300 or len(daily) < 201:
            continue
        n_sym += 1
        # book signal times for this symbol, by direction
        book_ts = defaultdict(list)
        for tag, tf, gen in BOOK:
            bars = m15 if tf == 'm15' else h1
            if len(bars) < 400:
                continue
            raw = H.trend_filter_signals(bars, H._mw_signals(bars, pk, tag, tf, gen))
            for s in raw:
                book_ts[s['dir']].append(s['entry_ts'])
        for d in book_ts:
            book_ts[d].sort()
        for s in H._gap_signals(pk, m15, daily):
            r = _score(m15, s)
            if r is None:
                continue
            n_sig += 1
            all_rows.append((s['entry_ts'], r))
            lst = book_ts.get(s['dir'], [])
            k = bisect.bisect_left(lst, s['entry_ts'])
            near = (k < len(lst) and lst[k] - s['entry_ts'] <= win) or \
                   (k > 0 and s['entry_ts'] - lst[k-1] <= win)
            if near:
                n_overlap += 1
            else:
                non_rows.append((s['entry_ts'], r))

    print(f"gap_eq orthogonality — {n_sym} symbols, {n_sig} gap_eq signals scored (trail/{HOLD_GO}m15)\n")
    a = agg(all_rows)
    if a:
        print(f"  ALL gap_eq        n={a[0]:<5} WR={a[1]:.0f}% exp={a[2]:+.3f}R OOS[{a[3]:+.2f}/{a[4]:+.2f}]")
    if n_sig:
        print(f"  overlap w/ book   {n_overlap}/{n_sig} = {100*n_overlap/n_sig:.0f}% "
              f"(same name+dir within {win//3600}h)")
    b = agg(non_rows)
    if b:
        print(f"  NON-overlap only  n={b[0]:<5} WR={b[1]:.0f}% exp={b[2]:+.3f}R OOS[{b[3]:+.2f}/{b[4]:+.2f}]  <- the additive slice")


if __name__ == '__main__':
    main()
