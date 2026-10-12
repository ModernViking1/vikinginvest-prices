"""Do the live-loss fixes actually improve the EDGE, or just curve-fit 2 weeks of a bad regime?

Takes the broker-exported 3yr OHLC and, for each .EQ strategy, generates signals ONCE, then
replays them under a set of candidate FILTERS/EXITS motivated by the live demo losses:

  baseline        current live rule: trailing runner (arm +1R, trail 0.75R, 200-bar), all signals.
  no_entry_close  drop signals whose entry bar is in the last 2 bars of its session day
                  (the LSE/BARC close-gap blowups: -3.7R / -3.1R).
  no_entry_open   drop signals on the first bar of the session day (open-spike).
  flat_by_close   exit at the entry day's last bar instead of trailing overnight (intraday-only).
  trend_filter    take longs only when close>SMA200, shorts only when close<SMA200 (no counter-trend;
                  the live bear book was -0.405R into a rising market).
  cooldown        after a losing trade on a symbol+strategy, skip new entries for 5 bars (anti-whipsaw;
                  live m15 re-entered the same name 2-3x/day and got chopped).
  trend+close     trend_filter AND no_entry_close (the two most defensible, combined).

All scored net of the harness cost model, resolved/time-exit identical across variants, reported
overall + both OOS halves + by market, with the delta vs baseline. A fix only earns deployment if
it improves expectancy over the 3yr AND holds in both OOS halves — not merely in the recent weeks.
threepush_eq (4h, US-only swing) is excluded — the intraday filters don't apply to it.

  python equity_filter_research.py --hist viking_intl_ohlc.json
"""
import argparse
import bisect
import datetime as dt
import json
from collections import defaultdict

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

STRATS = [('holygrail_eq', 'h1', H._holygrail_sig), ('twob_eq', 'h1', H._twob_sig),
          ('volbreak_eq', 'h1', H._volbreak_sig), ('holygrail_eq_m15', 'm15', H._holygrail_sig),
          ('turtle_soup_eq', 'h1', H._turtlesoup_sig), ('turtle_soup_eq_m15', 'm15', H._turtlesoup_sig)]
ARM, DIST, HOLD = H.TRAIL_ARM, H.TRAIL_DIST, H.TRAIL_HOLD
SMA_W = 200
COOLDOWN_BARS = 5


def market(sym):
    s = (sym or '').upper()
    for suf, m in (('.NAS', 'US'), ('.NYSE', 'US'), ('.ETR', 'DE'), ('.LSE', 'UK'),
                   ('.PAR', 'FR'), ('.TSE', 'JP'), ('.HK', 'HK'), ('.MAD', 'ES')):
        if suf in s:
            return m
    return '?'


def enrich(bars):
    """Per-bar: calendar day, first/last-of-day flags, SMA200 of close."""
    n = len(bars)
    days = [dt.datetime.fromtimestamp(b['_ts'], dt.timezone.utc).date() for b in bars]
    first = [i == 0 or days[i] != days[i - 1] for i in range(n)]
    last = [i == n - 1 or days[i] != days[i + 1] for i in range(n)]
    closes = [b['c'] for b in bars]
    sma = [None] * n
    run = 0.0
    for i in range(n):
        run += closes[i]
        if i >= SMA_W:
            run -= closes[i - SMA_W]
        if i >= SMA_W - 1:
            sma[i] = run / SMA_W
    # bars remaining until end of this bar's day (0 == last bar of day)
    to_eod = [0] * n
    c = 0
    for i in range(n - 1, -1, -1):
        to_eod[i] = 0 if last[i] else to_eod[i + 1] + 1
    return days, first, last, sma, to_eod


def walk(bars, i0, entry, stop, d, hard_exit=None):
    """Trailing-runner walk from bar index i0. hard_exit = force-close at this bar index (flat-by-
    close). Returns gross R, or None if unresolved with no data."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    end = min(i0 + HOLD, len(bars))
    if hard_exit is not None:
        end = min(end, hard_exit + 1)
    if end <= i0:
        return None
    es = stop
    armed = False
    best = entry
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es:
                return (es - entry) / R
            best = max(best, b['h'])
            if not armed and b['h'] >= entry + ARM * R:
                armed = True
            if armed:
                es = max(es, best - DIST * R)
        else:
            if b['h'] >= es:
                return (entry - es) / R
            best = min(best, b['l'])
            if not armed and b['l'] <= entry - ARM * R:
                armed = True
            if armed:
                es = min(es, best + DIST * R)
    b = bars[end - 1]
    return (b['c'] - entry) / R if d == 'bull' else (entry - b['c']) / R


def net(o, entry, R):
    try:
        return o - H.cost(o, entry, R)
    except Exception:
        return o


def agg(rows):
    rows = sorted(rows)
    rs = [r for _, r in rows]
    n = len(rs)
    if not n:
        return None
    m = n // 2
    return dict(n=n, wr=100.0 * sum(1 for r in rs if r > 0) / n, exp=sum(rs) / n,
                oos1=sum(rs[:m]) / m if m else 0.0, oos2=sum(rs[m:]) / (n - m) if (n - m) else 0.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist))['pairs']

    VARIANTS = ['baseline', 'no_entry_close', 'no_entry_open', 'flat_by_close',
                'trend_filter', 'cooldown', 'trend+close']
    rows = {v: [] for v in VARIANTS}          # variant -> [(entry_ts, netR)]
    rows_mkt = {v: defaultdict(list) for v in VARIANTS}
    taken = {v: 0 for v in VARIANTS}

    for key, layers in pairs.items():
        sym = layers.get('sym', key)
        mkt = market(sym)
        for tag, tf, gen in STRATS:
            bars = _bars_norm(layers.get(tf) or [])
            if len(bars) < 400:
                continue
            days, first, last, sma, to_eod = enrich(bars)
            ts = [b['_ts'] for b in bars]
            sigs = H._mw_signals(bars, key, tag, tf, gen)
            sigs = [s for s in sigs if s['entry_ts'] >= ts[0]]
            sigs.sort(key=lambda s: s['entry_ts'])

            # cooldown needs sequential processing with knowledge of exit bar; precompute each
            # signal's entry index + baseline exit index.
            def idx(s):
                i = bisect.bisect_left(ts, s['entry_ts'])
                return i if i < len(bars) else None

            cd_block_until = -1
            for s in sigs:
                i0 = idx(s)
                if i0 is None:
                    continue
                entry, stop, d = s['entry'], s['stop'], s['dir']
                R = abs(entry - stop)
                if R <= 0:
                    continue
                base = walk(bars, i0, entry, stop, d)
                if base is None:
                    continue
                base_n = net(base, entry, R)

                def add(v, r):
                    if r is None:
                        return
                    rows[v].append((s['entry_ts'], r)); rows_mkt[v][mkt].append((s['entry_ts'], r)); taken[v] += 1

                add('baseline', base_n)
                # no_entry_close: skip if entry within last 2 bars of its day
                if to_eod[i0] > 1:
                    add('no_entry_close', base_n)
                # no_entry_open: skip first bar of day
                if not first[i0]:
                    add('no_entry_open', base_n)
                # flat_by_close: exit at end of entry's day
                eod = i0 + to_eod[i0]
                fb = walk(bars, i0, entry, stop, d, hard_exit=eod)
                add('flat_by_close', net(fb, entry, R) if fb is not None else None)
                # trend_filter: align with SMA200
                al = sma[i0] is not None and ((d == 'bull' and bars[i0]['c'] > sma[i0]) or
                                              (d == 'bear' and bars[i0]['c'] < sma[i0]))
                if al:
                    add('trend_filter', base_n)
                # trend+close
                if al and to_eod[i0] > 1:
                    add('trend+close', base_n)
                # cooldown: skip if still blocked; else take, and block after a loss
                if i0 >= cd_block_until:
                    add('cooldown', base_n)
                    if base_n < 0:
                        exit_idx = min(i0 + HOLD, len(bars) - 1)   # conservative block window
                        cd_block_until = i0 + COOLDOWN_BARS

    base = agg(rows['baseline'])
    print('=' * 78)
    print(f"EQUITY FILTER RESEARCH · {args.hist} · trail-runner, net of cost, both-OOS")
    print(f"baseline n={base['n']}  exp={base['exp']:+.3f}R")
    print('=' * 78)
    print(f"\n{'variant':<16}{'n':>7}{'exp':>9}{'OOS1':>8}{'OOS2':>8}{'Δexp':>9}  verdict")
    print('-' * 78)
    for v in VARIANTS:
        a = agg(rows[v])
        if not a:
            print(f'{v:<16} (none)'); continue
        delta = a['exp'] - base['exp']
        ok = v == 'baseline' or (delta > 0 and a['oos1'] > 0 and a['oos2'] > 0)
        verd = '—' if v == 'baseline' else ('✓ improves' if ok else ('~ flat' if abs(delta) < 0.005 else '✗ worse'))
        print(f"{v:<16}{a['n']:>7}{a['exp']:>+8.3f}R{a['oos1']:>+7.3f}{a['oos2']:>+7.3f}{delta:>+8.3f}R  {verd}")
    print('-' * 78)
    print("\nBY MARKET (exp R / n) per variant:")
    mkts = ['US', 'DE', 'UK', 'FR', 'JP', 'HK']
    print(f"  {'variant':<16}" + ''.join(f'{m:>12}' for m in mkts))
    for v in VARIANTS:
        cells = []
        for m in mkts:
            a = agg(rows_mkt[v][m])
            cells.append(f"{a['exp']:+.3f}/{a['n']}" if a else '—')
        print(f"  {v:<16}" + ''.join(f'{c:>12}' for c in cells))
    print("\nA variant earns deployment only if it beats baseline overall AND both OOS halves stay")
    print("positive — and ideally helps the market it targets (EU). Recent-weeks improvement alone")
    print("is not enough; these run over the full 3yr to avoid curve-fitting the live sample.")


if __name__ == '__main__':
    raise SystemExit(main())
