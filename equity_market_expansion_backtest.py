"""Do the .EQ edges hold beyond the 5 US pilot names — across US top-10 and the DE / UK / FR
large-caps? Fetches h1+m15 for an expanded universe and replays the SAME causal detectors the live
equity book uses (holygrail_eq / twob_eq / volbreak_eq on h1, holygrail_eq_m15 on m15), scored on
the shared trailing-runner exit (arm +1R, trail 1R, 200-bar), net of the harness cost model.

Causal by construction: these detectors signal on their own timeframe (no daily cross-TF lookup),
so there is no look-ahead to remove. Resolved-only, both-OOS reported. Commits nothing — result in
the log. Fail-open per symbol: anything TwelveData won't serve (free tier is US-only) is skipped and
shown in the coverage report, so a market with no data reads as 'no data', not a false negative.

  TWELVEDATA_API_KEY=... python equity_market_expansion_backtest.py [--intervals h1,m15]
"""
import argparse, os, time
from collections import defaultdict

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from fetch_equity_ohlc import fetch_series

# Top-10 per market. Values are TwelveData symbols; international use TICKER:EXCHANGE (XETR / LSE /
# XPAR). US top-10 = the 5 live pilot names + 5 more mega-caps.
UNIVERSE = {
    'US': {'aapl':'AAPL','msft':'MSFT','nvda':'NVDA','amzn':'AMZN','googl':'GOOGL',
           'meta':'META','avgo':'AVGO','tsla':'TSLA','jpm':'JPM','lly':'LLY'},
    'DE': {'sap':'SAP:XETR','sie':'SIE:XETR','alv':'ALV:XETR','dte':'DTE:XETR','air':'AIR:XETR',
           'mbg':'MBG:XETR','muv2':'MUV2:XETR','bmw':'BMW:XETR','bas':'BAS:XETR','vow3':'VOW3:XETR'},
    'UK': {'azn':'AZN:LSE','shel':'SHEL:LSE','hsba':'HSBA:LSE','ulvr':'ULVR:LSE','bp':'BP:LSE',
           'rio':'RIO:LSE','gsk':'GSK:LSE','dge':'DGE:LSE','bats':'BATS:LSE','rel':'REL:LSE'},
    'FR': {'mc':'MC:XPAR','or':'OR:XPAR','tte':'TTE:XPAR','san':'SAN:XPAR','su':'SU:XPAR',
           'el':'EL:XPAR','bnp':'BNP:XPAR','ai':'AI:XPAR','dg':'DG:XPAR','cap':'CAP:XPAR'},
}

# (tag, timeframe, generator) — identical to the live equity feed's .EQ book
STRATS = [('holygrail_eq','h1',H._holygrail_sig), ('twob_eq','h1',H._twob_sig),
          ('volbreak_eq','h1',H._volbreak_sig), ('holygrail_eq_m15','m15',H._holygrail_sig),
          ('turtle_soup_eq','h1',H._turtlesoup_sig), ('turtle_soup_eq_m15','m15',H._turtlesoup_sig),
          ('threepush_eq','4h',H.threepush_core)]
MIN_BARS = 400


def _score(bars, s):
    st, o = H.score_trail_open(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                               H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
    if st != 'resolved':
        return None
    try:
        return o - H.cost(o, s['entry'], abs(s['entry'] - s['stop']))
    except Exception:
        return o                      # frictionless fallback if no cost model


def _agg(rs):
    n = len(rs)
    if not n:
        return None
    w = 100.0 * sum(1 for r in rs if r > 0) / n
    m = n // 2
    eh = sum(rs[:m]) / m if m else 0.0
    es = sum(rs[m:]) / (n - m) if (n - m) else 0.0
    return dict(n=n, wr=w, exp=sum(rs) / n, tot=sum(rs), oos1=eh, oos2=es)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--intervals', default='h1,m15')
    args = ap.parse_args()
    key = os.environ.get('TWELVEDATA_API_KEY', '').strip()
    if not key:
        print('::error::TWELVEDATA_API_KEY not set'); return 1
    tfs = [t.strip() for t in args.intervals.split(',') if t.strip()]

    # by (market, strategy) -> [R...], by market, by strategy, and time-ordered for OOS split
    by_ms = defaultdict(list); by_m = defaultdict(list); by_s = defaultdict(list)
    served = defaultdict(int); skipped = defaultdict(list)

    for mkt, syms in UNIVERSE.items():
        for pk, td in syms.items():
            layers = {}
            ok = False
            for tf in tfs:
                try:
                    rows = fetch_series(td, tf, key)
                    time.sleep(9.0)                    # free-tier rate limit (8/min)
                    if rows:
                        layers[tf] = _bars_norm(rows); ok = True
                except Exception as e:
                    print(f"  {mkt}/{td} {tf}: skip ({str(e)[:70]})", flush=True)
            if not ok:
                skipped[mkt].append(pk); continue
            if layers.get('h1'):
                layers['4h'] = H.agg4h(layers['h1'])     # 4h aggregated from h1 (threepush_eq)
            served[mkt] += 1
            for tag, tf, gen in STRATS:
                bars = layers.get(tf) or []
                if len(bars) < (150 if tf == '4h' else MIN_BARS):
                    continue
                for s in H._mw_signals(bars, pk, tag, tf, gen):
                    r = _score(bars, s)
                    if r is None:
                        continue
                    by_ms[(mkt, tag)].append((s['entry_ts'], r))
                    by_m[mkt].append((s['entry_ts'], r))
                    by_s[tag].append((s['entry_ts'], r))

    def rows_sorted(d):
        d.sort(); return [r for _, r in d]

    print("\n================ RESULT ================")
    print("coverage (symbols with data / 10):")
    for mkt in UNIVERSE:
        sk = skipped.get(mkt, [])
        print(f"  {mkt}: {served.get(mkt,0)}/10 served" + (f"  skipped: {','.join(sk)}" if sk else ""))

    print("\nBY MARKET (all .EQ strategies pooled, net of costs):")
    print(f"  {'mkt':<5}{'n':>7}{'WR':>6}{'exp':>9}{'OOS1':>9}{'OOS2':>9}{'totR':>8}  edge?")
    for mkt in UNIVERSE:
        a = _agg(rows_sorted(by_m[mkt]))
        if not a:
            print(f"  {mkt:<5}  (no resolved trades)"); continue
        edge = 'REAL' if (a['oos1'] > 0 and a['oos2'] > 0 and a['exp'] > 0.02) else ('thin' if a['exp'] > 0 else '')
        print(f"  {mkt:<5}{a['n']:>7}{a['wr']:>5.0f}%{a['exp']:>+8.3f}R{a['oos1']:>+8.3f}{a['oos2']:>+8.3f}{a['tot']:>+7.0f}  {edge}")

    print("\nBY STRATEGY (all markets pooled):")
    for tag, _, _ in STRATS:
        a = _agg(rows_sorted(by_s[tag]))
        if a:
            print(f"  {tag:<18}{a['n']:>6}{a['wr']:>5.0f}%{a['exp']:>+8.3f}R  OOS[{a['oos1']:+.3f}/{a['oos2']:+.3f}]  totR {a['tot']:+.0f}")

    print("\nBY MARKET x STRATEGY (exp R, n):")
    print(f"  {'strategy':<18}" + "".join(f"{m:>14}" for m in UNIVERSE))
    for tag, _, _ in STRATS:
        cells = []
        for mkt in UNIVERSE:
            a = _agg(rows_sorted(by_ms[(mkt, tag)]))
            cells.append(f"{a['exp']:+.3f}/{a['n']}" if a else "—")
        print(f"  {tag:<18}" + "".join(f"{c:>14}" for c in cells))
    print("\nedge? REAL = exp>+0.02R AND both OOS halves + (net of costs). Compare each new market to")
    print("the US pilot: an edge that only shows in US is US-specific, not a portable equity edge.")
    print("========================================")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
