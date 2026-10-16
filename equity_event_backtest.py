"""How are the equity strategies impacted by NFP / FOMC / CPI over 3 years?

Replays the .EQ strategies on the broker OHLC, resolves each trade with the live
trailing-runner exit (capturing the EXIT time), and tags every trade PER EVENT TYPE:
  across   — an event release falls inside the position's lifetime [entry, exit]
  post     — entered within POST_H hours AFTER an event
  clear    — neither
Event times come from the persisted store (macro-events.json via event_schedule.load_store).
Compares WR / expR / totR per bucket, overall and per strategy, so we can see whether
holding across / entering around each event helps or hurts.

    python equity_event_backtest.py --hist <broker ohlc>
"""
import json, argparse, bisect
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import market, STRATS
from event_schedule import load_store

PRE_H = 2.0
POST_H = 24.0
EV = {k: sorted(v) for k, v in load_store().items()}      # {'nfp':[epochs],'fomc':[...],'cpi':[...]}


def resolve_trail(bars, entry_ts, entry, stop, d, hold, arm, trail):
    """Trailing-runner resolve returning (R_multiple, exit_ts)."""
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


def _across(evs, t0, t1):
    i = bisect.bisect_left(evs, t0)
    return i < len(evs) and evs[i] <= t1


def _post(evs, t):
    i = bisect.bisect_right(evs, t) - 1
    return i >= 0 and (t - evs[i]) <= POST_H * 3600


def bucket(evs, entry_ts, exit_ts):
    if _across(evs, entry_ts - PRE_H * 3600, exit_ts):
        return 'across'
    if _post(evs, entry_ts):
        return 'post'
    return 'clear'


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    evtypes = ['nfp', 'fomc', 'cpi', 'tw', 'pce', 'ism']
    by_eb = defaultdict(list)          # (evtype, bucket) -> [r]
    by_seb = defaultdict(list)         # (strat, evtype, bucket) -> [r]
    nsym = 0
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
                for ev in evtypes:
                    bkt = bucket(EV[ev], s['entry_ts'], exit_ts)
                    by_eb[(ev, bkt)].append(r)
                    by_seb[(strat, ev, bkt)].append(r)
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

    print(f"Equity strategies vs US macro events — {nsym} symbols, trailing exit.")
    print("releases: " + "  ".join(f"{e.upper()}={len(EV.get(e,[]))}" for e in evtypes) + "  (macro-events.json)")
    print(f"'across'=held through the event; 'post'=entered <={POST_H:.0f}h after; 'clear'=neither.\n")

    print(f"OVERALL — 'across' (held through) vs 'clear':")
    print(f"  {'event':<6}{'n_across':>9}{'WR_a':>6}{'expR_a':>8}{'expR_clear':>12}{'delta':>8}")
    for ev in evtypes:
        na, wa, ea, _ = agg(by_eb[(ev, 'across')])
        nc, wc, ec, _ = agg(by_eb[(ev, 'clear')])
        print(f"  {ev.upper():<6}{na:>9}{wa:>5.0f}%{ea:>+8.3f}{ec:>+12.3f}{ea-ec:>+8.3f}")

    print(f"\nPER STRATEGY — 'across' expR by event (clear baseline in last col):")
    print(f"  {'strategy':<20}" + "".join(f"{e.upper():>8}" for e in evtypes) + f"{'clear':>8}")
    strs = sorted({s for (s, _, _) in by_seb})
    for s in strs:
        cells = []
        for ev in evtypes:
            _, _, ea, _ = agg(by_seb[(s, ev, 'across')])
            cells.append(f"{ea:+.2f}" if by_seb[(s, ev, 'across')] else "—")
        _, _, ec, _ = agg(by_seb[(s, evtypes[0], 'clear')])
        print(f"  {s:<20}" + "".join(f"{c:>8}" for c in cells) + f"{ec:>+8.2f}")

    print("\ndelta = across - clear. Positive = event is a tailwind when already positioned;")
    print("negative = holding through it hurts (consider flat-before / no-new-entry around it).")


if __name__ == '__main__':
    main()
