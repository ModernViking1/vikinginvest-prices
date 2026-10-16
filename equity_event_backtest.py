"""How are the equity strategies impacted by NFP over 3 years?

Replays the .EQ strategies on the broker OHLC, resolves each trade with the live
trailing-runner exit (capturing the EXIT time), and tags every trade by its
relation to the deterministic NFP schedule (event_schedule.nfp_times):
  across   — an NFP release falls inside the position's lifetime [entry, exit]
             (held THROUGH the spike)
  post     — entered within POST_H hours AFTER an NFP (traded into the aftermath)
  clear    — neither
Then compares WR / expR / totR per bucket, overall and per strategy, so we can see
whether holding across / entering around NFP helps or hurts.

    python equity_event_backtest.py --hist <broker ohlc>
"""
import json, argparse, bisect
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import market, STRATS
from event_schedule import nfp_epochs

PRE_H = 2.0       # a position counts as "across" if an NFP is within [entry, exit]
POST_H = 24.0     # "post": entered within this many hours after an NFP
NFP = sorted(nfp_epochs(2023, 2026))


def resolve_trail(bars, entry_ts, entry, stop, d, hold, arm, trail):
    """Trailing-runner resolve returning (R_multiple, exit_ts) — mirrors
    H.score_trail_open but also reports WHEN the trade exits."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return None
    es = stop; armed = False; peak = entry; end = i0 + hold
    for j in range(i0, min(end, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es: return ((es - entry) / R, b['_ts'])
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + arm * R: armed = True
            if armed: es = max(es, peak - trail * R)
        else:
            if b['h'] >= es: return ((entry - es) / R, b['_ts'])
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - arm * R: armed = True
            if armed: es = min(es, peak + trail * R)
    if end <= len(bars):
        lc = bars[end - 1]['c']
        return (((lc - entry) if d == 'bull' else (entry - lc)) / R, bars[end - 1]['_ts'])
    return None


def _nfp_between(t0, t1):
    i = bisect.bisect_left(NFP, t0)
    return i < len(NFP) and NFP[i] <= t1


def _entered_post(t):
    i = bisect.bisect_right(NFP, t) - 1
    return i >= 0 and (t - NFP[i]) <= POST_H * 3600


def tag(entry_ts, exit_ts):
    # entry_ts/exit_ts are epoch SECONDS
    if _nfp_between(entry_ts - PRE_H * 3600, exit_ts):
        return 'across'
    if _entered_post(entry_ts):
        return 'post'
    return 'clear'


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    # rows: (bucket, strat, r) ; also overall per bucket
    by_bucket = defaultdict(list); by_sb = defaultdict(list); nsym = 0
    for key, layers in pairs.items():
        mkt = market(layers.get('sym', key))
        h1 = _bars_norm(layers.get('h1') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(h1) < 400:
            continue
        nsym += 1
        b4 = H.agg4h(h1)
        def run(strat, bars, raw):
            for s in raw:
                res = resolve_trail(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                                    H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
                if res is None:
                    continue
                rmult, exit_ts = res
                r = rmult - H.cost(rmult, s['entry'], abs(s['entry'] - s['stop']))
                # entry_ts/exit are in SECONDS already in broker files (verified earlier)
                bkt = tag(s['entry_ts'], exit_ts)
                by_bucket[bkt].append(r)
                by_sb[(strat, bkt)].append(r)
        for tagn, tf, gen in STRATS:
            bars = b4 if tf == '4h' else (m15 if tf == 'm15' else h1)
            if len(bars) < (150 if tf == '4h' else 400):
                continue
            raw = H._mw_signals(bars, key, tagn, tf, gen)
            if tagn != 'threepush_eq':
                raw = H.trend_filter_signals(bars, raw)
            run(tagn, bars, raw)
        for tf in (['h1', '4h'] if mkt == 'US' else (['h1'] if mkt in ('UK', 'FR') else [])):
            bars = b4 if tf == '4h' else h1
            if len(bars) < (250 if tf == '4h' else 400):
                continue
            run('fib_eq', bars, H._fib_eq_signals(key, bars, tf))

    def agg(rs):
        n = len(rs); w = sum(1 for r in rs if r > 0)
        return n, (100*w/n if n else 0), (sum(rs)/n if n else 0), sum(rs)

    print(f"Equity strategies vs NFP — {nsym} symbols, {len(NFP)} NFP releases 2023-2026, trailing exit.")
    print(f"'across' = held through an NFP; 'post' = entered <= {POST_H:.0f}h after NFP; 'clear' = neither.\n")
    print(f"{'bucket':<10}{'n':>7}{'WR':>6}{'expR':>8}{'totR':>9}")
    tot = sum(len(v) for v in by_bucket.values())
    for bkt in ('across', 'post', 'clear'):
        n, wr, e, t = agg(by_bucket[bkt])
        print(f"{bkt:<10}{n:>7}{wr:>5.0f}%{e:>+8.3f}{t:>+9.0f}   ({100*n/max(1,tot):.0f}% of trades)")

    print(f"\nPER STRATEGY (expR by bucket):")
    print(f"{'strategy':<20}{'across':>14}{'post':>14}{'clear':>14}")
    str16 = sorted({s for (s, _) in by_sb})
    for s in str16:
        cells = []
        for bkt in ('across', 'post', 'clear'):
            n, wr, e, t = agg(by_sb[(s, bkt)])
            cells.append(f"{e:+.3f}/{n}" if n else "—")
        print(f"{s:<20}" + "".join(f"{c:>14}" for c in cells))

    print("\nRead: if 'across' expR << 'clear', holding through NFP hurts that strategy -> consider")
    print("a flat-before-NFP or no-new-entry-around-NFP rule. If similar, NFP is not a material risk.")


if __name__ == '__main__':
    main()
