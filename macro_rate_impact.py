"""How do central-bank rate hikes/cuts move each asset class? (3yr)

Reads macro-rates.json (central-bank policy rates, from fetch_macro_rates.py) and the price
data, and measures per asset class:
  - REGIME returns: mean DAILY return (annualised) + volatility while the USD policy rate is
    HIKING (rising over the trailing LOOKBACK days) / CUTTING (falling) / HOLD (flat).
  - EVENT study: average forward return over the FWD days after a HIKE vs a CUT (USD rate-change
    days, which line up with FOMC decisions).
USD (the Fed) is the global driver, so regimes/events key off USD; the per-currency rates are
also loaded for FX context. Asset classes from historical-ohlc.json (major/minor FX, index, comm,
crypto); single-stock equities added from a broker file if given via --equity.

    python macro_rate_impact.py [--rates macro-rates.json] [--hist historical-ohlc.json] [--equity all_eq.json]
"""
import json, argparse, datetime as dt
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

LOOKBACK = 90       # days: regime = sign of (rate_now - rate_LOOKBACK_ago)
FWD = 5             # event study: forward return window (trading days) after a hike/cut
MIN_CHG = 0.0001    # ignore sub-bp noise in the rate series


def _d(s):
    return dt.date.fromisoformat(s[:10])


def build_rate_curve(rates_doc, ccy='USD'):
    """Daily step-function {date: rate} (forward-filled) + change events [(date, delta)]."""
    series = sorted(rates_doc['rates'][ccy], key=lambda r: r['date'])
    pts = [(_d(r['date']), r['rate']) for r in series]
    # forward-fill daily
    curve = {}; events = []
    start, end = pts[0][0], dt.date.today()
    i = 0; cur = pts[0][1]; prev = pts[0][1]
    d = start
    while d <= end:
        while i < len(pts) and pts[i][0] <= d:
            if abs(pts[i][1] - prev) > MIN_CHG:
                events.append((pts[i][0], pts[i][1] - prev))
                prev = pts[i][1]
            cur = pts[i][1]; i += 1
        curve[d] = cur
        d += dt.timedelta(days=1)
    return curve, events


def regime_at(curve, d):
    r_now = curve.get(d)
    r_past = curve.get(d - dt.timedelta(days=LOOKBACK))
    if r_now is None or r_past is None:
        return None
    if r_now - r_past > MIN_CHG: return 'hiking'
    if r_past - r_now > MIN_CHG: return 'cutting'
    return 'hold'


def daily_returns(bars):
    """[(date, pct_return)] close-to-close from daily bars."""
    out = []
    for i in range(1, len(bars)):
        p0 = bars[i-1]['c']
        if p0:
            out.append((dt.datetime.utcfromtimestamp(bars[i]['_ts'] if bars[i]['_ts'] < 1e12 else bars[i]['_ts']/1000).date(),
                        (bars[i]['c'] - p0) / p0))
    return out


def agg_daily_from_h1(h1):
    """Aggregate h1 -> daily close (last bar per UTC date) for equity broker files."""
    byday = {}
    for b in h1:
        day = dt.datetime.utcfromtimestamp(b['_ts'] if b['_ts'] < 1e12 else b['_ts']/1000).date()
        byday[day] = b          # last bar of the day wins (dict insertion order = chronological)
    days = sorted(byday)
    return [{'c': byday[d]['c'], '_ts': dt.datetime(d.year, d.month, d.day).timestamp()} for d in days]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rates', default='macro-rates.json')
    ap.add_argument('--hist', default='historical-ohlc.json')
    ap.add_argument('--equity', default='')
    args = ap.parse_args()
    rdoc = json.load(open(args.rates))
    curve, events = build_rate_curve(rdoc, 'USD')
    hikes = [e for e in events if e[1] > 0]; cuts = [e for e in events if e[1] < 0]
    print(f"USD policy rate ({rdoc['meta'].get('series_used', {}).get('USD','?')}): "
          f"{len(events)} changes — {len(hikes)} hikes, {len(cuts)} cuts "
          f"(range {min(curve.values()):.2f}-{max(curve.values()):.2f}%)\n")

    # ---- per-class daily returns tagged by regime + event windows ----
    pairs = json.load(open(args.hist)).get('pairs', {})
    cls_rets = defaultdict(lambda: defaultdict(list))     # class -> regime -> [ret]
    ev_fwd = defaultdict(lambda: defaultdict(list))        # class -> 'hike'/'cut' -> [fwd_ret]
    rseries = {}                                           # (class) pooled date->list of rets for event study

    def ingest(cls, rets):
        # rets: [(date, ret)] sorted
        dmap = defaultdict(list)
        for d, r in rets:
            reg = regime_at(curve, d)
            if reg:
                cls_rets[cls][reg].append(r)
            dmap[d].append(r)
        # event study: forward FWD-day cumulative return after each hike/cut day
        allds = sorted(dmap)
        idx = {d: i for i, d in enumerate(allds)}
        for evlist, lbl in ((hikes, 'hike'), (cuts, 'cut')):
            for ed, _ in evlist:
                # nearest trading day on/after the event
                j = None
                for k in range(0, 6):
                    if (ed + dt.timedelta(days=k)) in idx:
                        j = idx[ed + dt.timedelta(days=k)]; break
                if j is None or j + FWD >= len(allds):
                    continue
                fwd = 1.0
                for k in range(j, j + FWD):
                    for r in dmap[allds[k]]:
                        fwd *= (1 + r)
                ev_fwd[cls][lbl].append(fwd - 1)

    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        if not cls:
            continue
        daily = _bars_norm(layers.get('daily') or [])
        if len(daily) < 100:
            continue
        ingest(cls, daily_returns(daily))
    if args.equity:
        eq = json.load(open(args.equity)).get('pairs', {})
        for pk, layers in eq.items():
            h1 = _bars_norm(layers.get('h1') or [])
            if len(h1) < 400:
                continue
            ingest('equity', daily_returns(agg_daily_from_h1(h1)))

    def stats(rs):
        n = len(rs)
        if not n:
            return None
        m = sum(rs)/n
        var = sum((x-m)**2 for x in rs)/n
        return n, m*252*100, (var**0.5)*(252**0.5)*100      # annualised mean %, annualised vol %

    print("REGIME — annualised return % (vol %) by USD rate regime:")
    print(f"  {'class':<8}{'HIKING':>18}{'CUTTING':>18}{'HOLD':>18}")
    for cls in sorted(cls_rets):
        cells = []
        for reg in ('hiking', 'cutting', 'hold'):
            s = stats(cls_rets[cls][reg])
            cells.append(f"{s[1]:+.0f}% ({s[2]:.0f}%)" if s else "—")
        print(f"  {cls:<8}" + "".join(f"{c:>18}" for c in cells))

    print(f"\nEVENT — avg {FWD}-day return after a rate change:")
    print(f"  {'class':<8}{'after HIKE':>14}{'after CUT':>14}")
    for cls in sorted(ev_fwd):
        h = ev_fwd[cls]['hike']; c = ev_fwd[cls]['cut']
        hs = f"{100*sum(h)/len(h):+.2f}% (n{len(h)})" if h else "—"
        cs = f"{100*sum(c)/len(c):+.2f}% (n{len(c)})" if c else "—"
        print(f"  {cls:<8}{hs:>14}{cs:>14}")

    print("\nHIKING/CUTTING = USD policy rate higher/lower than 90d ago. Event n = (changes x pairs).")


if __name__ == '__main__':
    main()
