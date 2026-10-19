"""Overnight / weekend holding study for the US equity book.

The book exits on a multi-day TRAILING runner, so it holds overnight and over weekends. Question:
is that holding accretive, or should we flatten at the session close (and especially before the
weekend)? Scores each live-book signal on the m15 stream under several forced-flat rules, all with
a GAP-AWARE stop (a bar opening past the stop fills at the open — the real overnight/weekend tail):

  eod_flat   close at the entry day's session close (intraday only)
  hold_1     close at the NEXT session's close (one overnight)
  trail      the book's multi-day trailing runner (BASELINE)
  fri_flat   trail, but force-flat at the first Friday session close (never hold over a weekend)
  hybrid     take profit at the entry-day close IF green, else keep trailing

Plus the weekend isolation: among baseline trail trades, spanning-a-weekend vs not, and the mean
signed R the Fri-close->Mon-open gap actually contributed.

  python eod_study.py --hist <broker ohlc>
"""
import argparse, json, bisect
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import STRATS

ARM, TRAIL, HOLD = H.TRAIL_ARM, H.TRAIL_DIST, H.TRAIL_HOLD


def _is_us(sym):
    return '.NAS' in (sym or '') or '.NYSE' in (sym or '')


def build_sessions(m15):
    sess = []; i = 0; n = len(m15)
    while i < n:
        d = m15[i]['t'][:10]; j = i
        while j < n and m15[j]['t'][:10] == d:
            j += 1
        wd = datetime.fromtimestamp(m15[i]['_ts'], timezone.utc).weekday()
        sess.append((i, j - 1, wd)); i = j
    return sess


def gap_trail(m15, i0, entry, stop, d, cap_idx=None, profit_flat_idx=None):
    """Gap-aware trailing runner. cap_idx: hard-exit at that bar's close if still open.
    profit_flat_idx: at that bar, exit at close IF in unrealized profit. Returns (R, exit_idx)."""
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(m15):
        return None
    es = stop; armed = False; peak = entry
    end = min(i0 + HOLD, len(m15))
    if cap_idx is not None:
        end = min(end, cap_idx + 1)
    for j in range(i0, end):
        b = m15[j]
        if d == 'bull':
            if b['o'] <= es: return ((b['o'] - entry) / R, j)      # gap through stop -> fill at open
            if b['l'] <= es: return ((es - entry) / R, j)
        else:
            if b['o'] >= es: return ((entry - b['o']) / R, j)      # gap through stop -> fill at open
            if b['h'] >= es: return ((entry - es) / R, j)
        if profit_flat_idx is not None and j == profit_flat_idx:
            mark = (b['c'] - entry) if d == 'bull' else (entry - b['c'])
            if mark > 0:
                return (mark / R, j)
        if d == 'bull':
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + ARM * R: armed = True
            if armed: es = max(es, peak - TRAIL * R)
        else:
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - ARM * R: armed = True
            if armed: es = min(es, peak + TRAIL * R)
    lc = m15[end - 1]['c']
    return (((lc - entry) if d == 'bull' else (entry - lc)) / R, end - 1)


def agg(rs):
    n = len(rs); w = sum(1 for r in rs if r > 0)
    worst = min(rs) if rs else 0.0
    return (n, 100*w/n, sum(rs)/n, sum(rs), worst) if n else (0, 0, 0, 0, 0)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})

    store = defaultdict(list)
    wknd_span, wknd_nospan, wknd_gap = [], [], []
    nsym = 0
    for key, layers in pairs.items():
        sym = layers.get('sym') or key
        if layers.get('sym') and not _is_us(sym):
            continue
        h1 = _bars_norm(layers.get('h1') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(h1) < 400 or len(m15) < 300:
            continue
        nsym += 1
        sess = build_sessions(m15); starts = [s[0] for s in sess]
        mts = [b['_ts'] for b in m15]
        wk_bounds = [(sess[k][1], sess[k+1][0]) for k in range(len(sess)-1)
                     if sess[k][2] == 4 and sess[k+1][2] == 0]      # (Fri end, Mon start)
        b4 = H.agg4h(h1)

        def cst(r, entry, R):
            return r - H.cost(r, entry, R)

        def run(raw):
            for s in raw:
                entry, stop, d = s['entry'], s['stop'], s['dir']
                R = abs(entry - stop)
                if R <= 0:
                    continue
                i0 = bisect.bisect_left(mts, s['entry_ts'])
                if i0 >= len(m15) or abs(mts[i0] - s['entry_ts']) > 3600:
                    continue                 # entry outside the m15 coverage window -> skip (would mis-map)
                si = bisect.bisect_right(starts, i0) - 1
                if si < 0:
                    continue
                eod = sess[si][1]
                nxt = sess[si+1][1] if si+1 < len(sess) else eod
                fri = next((sess[k][1] for k in range(si, len(sess)) if sess[k][2] == 4), None)
                for name, cap, pf in (('eod_flat', eod, None), ('hold_1', nxt, None),
                                      ('trail', None, None), ('fri_flat', fri, None),
                                      ('hybrid', None, eod)):
                    res = gap_trail(m15, i0, entry, stop, d, cap_idx=cap, profit_flat_idx=pf)
                    if res:
                        store[name].append(cst(res[0], entry, R))
                # weekend isolation on the baseline trail
                res = gap_trail(m15, i0, entry, stop, d)
                if res:
                    r, xidx = res
                    spans = [(fe, ms) for (fe, ms) in wk_bounds if i0 <= fe and xidx >= ms]
                    (wknd_span if spans else wknd_nospan).append(cst(r, entry, R))
                    for (fe, ms) in spans:
                        g = (m15[ms]['o'] - m15[fe]['c']) / R
                        wknd_gap.append(g if d == 'bull' else -g)

        for tagn, tf, gen in STRATS:
            bars = b4 if tf == '4h' else (m15 if tf == 'm15' else h1)
            if len(bars) < (150 if tf == '4h' else 400):
                continue
            raw = H._mw_signals(bars, key, tagn, tf, gen)
            if tagn != 'threepush_eq':
                raw = H.trend_filter_signals(bars, raw)
            run(raw)
        for tf in ('h1', '4h'):
            bars = b4 if tf == '4h' else h1
            if len(bars) >= (250 if tf == '4h' else 400):
                run(H._fib_eq_signals(key, bars, tf))
        for tf in ('m15', 'h1'):
            bars = m15 if tf == 'm15' else h1
            if len(bars) >= 400:
                run(H._ma_pull_signals(key, bars, tf))

    print(f"Overnight/weekend holding study — {nsym} US names. Gap-aware, net of cost.\n")
    print(f"  {'exit rule':<12}{'n':>7}{'WR':>6}{'expR':>9}{'totR':>8}{'worst':>8}")
    for name in ('eod_flat', 'hold_1', 'trail', 'fri_flat', 'hybrid'):
        n, wr, e, t, worst = agg(store[name])
        if n:
            tag = '  <- BASELINE' if name == 'trail' else ''
            print(f"  {name:<12}{n:>7}{wr:>5.0f}%{e:>+9.3f}{t:>+8.0f}{worst:>+8.2f}{tag}")

    print("\nWEEKEND isolation (baseline trail):")
    for lbl, rows in (('spans weekend', wknd_span), ('no weekend', wknd_nospan)):
        n, wr, e, t, worst = agg(rows)
        if n:
            print(f"  {lbl:<16}{n:>7}{wr:>5.0f}%{e:>+9.3f}{t:>+8.0f}{worst:>+8.2f}")
    if wknd_gap:
        ng = len(wknd_gap); mg = sum(wknd_gap)/ng; tot = sum(wknd_gap)
        wins = 100*sum(1 for g in wknd_gap if g > 0)/ng
        print(f"  Fri->Mon GAP contribution: n={ng}  mean={mg:+.3f}R  total={tot:+.0f}R  gap-up(for dir)={wins:.0f}%")
        print("  (negative mean => weekend gaps hurt on net => a Friday-flat rule would help)")


if __name__ == '__main__':
    main()
