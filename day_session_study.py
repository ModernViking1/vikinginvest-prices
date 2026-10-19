"""Session-timing + day-of-week study for the US equity book.

Answers: (1) does the book do better in the AFTERNOON than the MORNING, and (2) is it worth
skipping MONDAYS (or at least Monday mornings)?

Each live-book signal (STRATS + fib_eq + ma_pull_eq, US names) is scored on the trailing exit
net of cost, then bucketed by:
  - MINUTES SINCE THE SESSION OPEN (TZ/DST-robust: we read each day's first m15 bar as the open),
    binned, plus a simple morning/afternoon split at the session midpoint.
  - WEEKDAY (Mon..Fri), with Monday-morning / Monday-afternoon called out.

  python day_session_study.py --hist <broker ohlc>
"""
import argparse, json, bisect
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import STRATS

HALF_MIN = 195          # session midpoint (~12:45 ET on a 390-min RTH day) -> morning/afternoon split
BINS = [(0, 60, 'open 0-60m'), (60, 150, 'late-morn 60-150'), (150, 270, 'midday 150-270'),
        (270, 9999, 'afternoon 270+')]
WD = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def _is_us(sym):
    return '.NAS' in (sym or '') or '.NYSE' in (sym or '')


def session_opens(m15):
    """date 'YYYY-MM-DD' -> ts of that day's first m15 bar (the RTH open)."""
    out = {}
    for b in m15:
        d = b['t'][:10]
        if d not in out:
            out[d] = b['_ts']
    return out


def _score(bars, s):
    st, o = H.score_trail_open(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                               H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
    if st != 'resolved':
        return None
    return o - H.cost(o, s['entry'], abs(s['entry'] - s['stop']))


def agg(rs):
    n = len(rs); w = sum(1 for r in rs if r > 0)
    return (n, 100*w/n, sum(rs)/n, sum(rs)) if n else (0, 0, 0, 0)


def binname(mins):
    for lo, hi, nm in BINS:
        if lo <= mins < hi:
            return nm
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})

    by_bin = defaultdict(list); by_half = defaultdict(list)
    by_wd = defaultdict(list); by_wd_half = defaultdict(list)
    nsym = 0
    for key, layers in pairs.items():
        sym = layers.get('sym') or key
        if layers.get('sym') and not _is_us(sym):   # broker export: skip non-US (bare-key 5-name file is US)
            continue
        h1 = _bars_norm(layers.get('h1') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(h1) < 400 or len(m15) < 300:
            continue
        nsym += 1
        opens = session_opens(m15)
        b4 = H.agg4h(h1)

        def run(bars, raw):
            for s in raw:
                r = _score(bars, s)
                if r is None:
                    continue
                ts = s['entry_ts']
                dtt = datetime.fromtimestamp(ts, timezone.utc)
                wd = dtt.weekday()
                day = dtt.strftime('%Y-%m-%d')
                op = opens.get(day)
                mins = (ts - op) / 60.0 if op is not None else None
                by_wd[wd].append(r)
                if mins is not None and mins >= -5:
                    half = 'morning' if mins < HALF_MIN else 'afternoon'
                    bn = binname(max(0, mins))
                    if bn:
                        by_bin[bn].append(r)
                    by_half[half].append(r)
                    by_wd_half[(wd, half)].append(r)

        for tagn, tf, gen in STRATS:
            bars = b4 if tf == '4h' else (m15 if tf == 'm15' else h1)
            if len(bars) < (150 if tf == '4h' else 400):
                continue
            raw = H._mw_signals(bars, key, tagn, tf, gen)
            if tagn != 'threepush_eq':
                raw = H.trend_filter_signals(bars, raw)
            run(bars, raw)
        for tf in ('h1', '4h'):
            bars = b4 if tf == '4h' else h1
            if len(bars) >= (250 if tf == '4h' else 400):
                run(bars, H._fib_eq_signals(key, bars, tf))
        for tf in ('m15', 'h1'):
            bars = m15 if tf == 'm15' else h1
            if len(bars) >= 400:
                run(bars, H._ma_pull_signals(key, bars, tf))

    print(f"Session-timing + weekday study — {nsym} US names. Trailing exit, net of cost.\n")

    print("BY SESSION PHASE (minutes since the RTH open):")
    print(f"  {'phase':<20}{'n':>7}{'WR':>6}{'expR':>9}{'totR':>8}")
    for _, _, nm in BINS:
        n, wr, e, t = agg(by_bin[nm])
        if n:
            print(f"  {nm:<20}{n:>7}{wr:>5.0f}%{e:>+9.3f}{t:>+8.0f}")
    print("  " + "-"*48)
    for half in ('morning', 'afternoon'):
        n, wr, e, t = agg(by_half[half])
        if n:
            print(f"  {half:<20}{n:>7}{wr:>5.0f}%{e:>+9.3f}{t:>+8.0f}")

    print("\nBY WEEKDAY:")
    print(f"  {'day':<20}{'n':>7}{'WR':>6}{'expR':>9}{'totR':>8}")
    for wd in range(5):
        n, wr, e, t = agg(by_wd[wd])
        if n:
            print(f"  {WD[wd]:<20}{n:>7}{wr:>5.0f}%{e:>+9.3f}{t:>+8.0f}")

    print("\nMONDAY FOCUS:")
    print(f"  {'slice':<20}{'n':>7}{'WR':>6}{'expR':>9}{'totR':>8}")
    for lbl, rows in (('Mon morning', by_wd_half[(0, 'morning')]),
                      ('Mon afternoon', by_wd_half[(0, 'afternoon')]),
                      ('Mon (all)', by_wd[0]),
                      ('Tue-Fri (all)', [r for wd in range(1, 5) for r in by_wd[wd]])):
        n, wr, e, t = agg(rows)
        if n:
            print(f"  {lbl:<20}{n:>7}{wr:>5.0f}%{e:>+9.3f}{t:>+8.0f}")


if __name__ == '__main__':
    main()
