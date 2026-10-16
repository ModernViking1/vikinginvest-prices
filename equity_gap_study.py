"""Per-symbol earnings / gap-risk study for the equity book.

Single-stock EARNINGS are the biggest driver of one-name moves — and unlike the macro
events (which TREND, a tailwind for a trailing book), earnings GAP the price overnight
and can fill straight PAST the stop. We don't have an earnings calendar here (no
TwelveData key locally — see fetch_earnings.py to populate one in CI), so we detect
earnings-SCALE events directly from each symbol's bars: a session-boundary overnight gap
(time gap > GAP_HOURS) with |open - prev_close|/prev_close > GAP_PCT. For single stocks a
>=4% overnight gap landing ~quarterly is overwhelmingly earnings.

Crucially the exit is GAP-AWARE: if a bar OPENS beyond the stop, the fill is the gapped
OPEN, not the stop price — so a gap-through-stop realises WORSE than -1R (the real tail
risk the ordinary bar-backtest hides by assuming a fill exactly at the stop).

Compares trades held ACROSS a big gap vs CLEAR, per strategy, with the worst-loss tail.

    python equity_gap_study.py --hist <broker ohlc>
"""
import json, argparse, bisect
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import market, STRATS

GAP_HOURS = 6.0       # a bar gap longer than this = a session boundary (overnight)
GAP_PCT = 0.04        # >=4% overnight move = earnings-scale for a single stock


def find_gaps(bars):
    """Session-boundary overnight gaps >= GAP_PCT. Returns sorted list of (ts, signed_pct)."""
    out = []
    for j in range(1, len(bars)):
        dt = bars[j]['_ts'] - bars[j-1]['_ts']
        if dt < GAP_HOURS * 3600:
            continue
        pc = bars[j-1]['c']
        if pc <= 0:
            continue
        g = (bars[j]['o'] - pc) / pc
        if abs(g) >= GAP_PCT:
            out.append((bars[j]['_ts'], g))
    return out


def resolve_gap_aware(bars, entry_ts, entry, stop, d, hold, arm, trail):
    """Trailing resolve, but a bar that GAPS through the stop fills at the bar OPEN
    (realising worse than the stop). Returns (R_multiple, exit_ts)."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return None
    es = stop; armed = False; peak = entry; end = i0 + hold
    for j in range(i0, min(end, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['o'] <= es: return ((b['o'] - entry) / R, b['_ts'])     # gap-through-stop: fill at open
            if b['l'] <= es: return ((es - entry) / R, b['_ts'])
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + arm * R: armed = True
            if armed: es = max(es, peak - trail * R)
        else:
            if b['o'] >= es: return ((entry - b['o']) / R, b['_ts'])
            if b['h'] >= es: return ((entry - es) / R, b['_ts'])
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - arm * R: armed = True
            if armed: es = min(es, peak + trail * R)
    if end <= len(bars):
        lc = bars[end - 1]['c']
        return (((lc - entry) if d == 'bull' else (entry - lc)) / R, bars[end - 1]['_ts'])
    return None


def _gap_between(gaps_ts, t0, t1):
    i = bisect.bisect_left(gaps_ts, t0)
    return i < len(gaps_ts) and gaps_ts[i] <= t1


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    by_sb = defaultdict(list)       # (strat, 'across'|'clear') -> [r]
    gap_counts = {}; nsym = 0
    for key, layers in pairs.items():
        mkt = market(layers.get('sym', key)); sym = layers.get('sym', key)
        h1 = _bars_norm(layers.get('h1') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(h1) < 400:
            continue
        nsym += 1
        gaps = find_gaps(h1)                      # detect on h1 (session boundaries show here)
        gaps_ts = [g[0] for g in gaps]
        years = (h1[-1]['_ts'] - h1[0]['_ts']) / (365.25 * 86400)
        gap_counts[sym] = (len(gaps), len(gaps)/years if years else 0)
        b4 = H.agg4h(h1)
        def run(strat, bars, raw):
            for s in raw:
                res = resolve_gap_aware(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                                        H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
                if res is None:
                    continue
                rmult, exit_ts = res
                r = rmult - H.cost(rmult, s['entry'], abs(s['entry'] - s['stop']))
                bkt = 'across' if _gap_between(gaps_ts, s['entry_ts'], exit_ts) else 'clear'
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
        worst = min(rs) if rs else 0
        blown = sum(1 for r in rs if r < -1.05)       # worse than -1R => gap jumped the stop
        return n, (100*w/n if n else 0), (sum(rs)/n if n else 0), sum(rs), worst, (100*blown/n if n else 0)

    med_gaps = sorted(v[1] for v in gap_counts.values())
    print(f"Earnings/gap-risk study — {nsym} symbols. Gap = overnight >= {GAP_PCT*100:.0f}% move.")
    print(f"Median gaps/yr per symbol: {med_gaps[len(med_gaps)//2]:.1f}  (earnings ~4/yr; higher = earnings+news)\n")

    print(f"PER STRATEGY — trades held ACROSS a big gap vs CLEAR (gap-aware exit):")
    print(f"  {'strategy':<20}{'bucket':<8}{'n':>6}{'WR':>5}{'expR':>8}{'worst':>8}{'>1R loss%':>10}")
    strs = sorted({s for (s, _) in by_sb})
    for s in strs:
        for bkt in ('across', 'clear'):
            n, wr, e, t, worst, blown = agg(by_sb[(s, bkt)])
            if n:
                print(f"  {s:<20}{bkt:<8}{n:>6}{wr:>4.0f}%{e:>+8.3f}{worst:>+8.2f}{blown:>9.1f}%")
    # overall
    allacross = [r for (s, b), rs in by_sb.items() if b == 'across' for r in rs]
    allclear = [r for (s, b), rs in by_sb.items() if b == 'clear' for r in rs]
    na, wa, ea, _, worsta, blowna = agg(allacross)
    nc, wc, ec, _, worstc, blownc = agg(allclear)
    print(f"\n  {'OVERALL across':<28}{na:>6}{wa:>4.0f}%{ea:>+8.3f}{worsta:>+8.2f}{blowna:>9.1f}%")
    print(f"  {'OVERALL clear':<28}{nc:>6}{wc:>4.0f}%{ec:>+8.3f}{worstc:>+8.2f}{blownc:>9.1f}%")
    print("\n'>1R loss%' = share of trades that lost MORE than 1R — only possible when a gap jumps the")
    print("stop. If 'across' expR << 'clear' and its >1R-loss% is high, holding through earnings-scale")
    print("gaps is the book's real tail risk -> consider a flat-before-earnings / reduce-size rule.")


if __name__ == '__main__':
    main()
