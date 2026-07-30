"""Demand/supply-zone (order-block) retrace, with a higher-timeframe TREND filter.

Julien's setup: buy a pullback into a demand zone only when the medium-term trend is up
(mirror for supply/shorts in a downtrend). This tests whether that HTF-trend filter adds
edge over the raw zone entry.

Zone = order block: the last opposite candle before an impulse. Bullish impulse -> the
prior down-candle is the demand zone; when price retraces back into it, go long in the
impulse direction (MARKET entry at the retrace-bar close — no favourable-limit fill, so no
fill illusion), stop beyond the zone. Mirror for supply/shorts.

Filters (evaluated on PRIOR completed HTF bars — no look-ahead):
  daily up = daily close > daily EMA50 ; 4H up = 4H EMA20 > 4H EMA50.
Variants: unfiltered / +daily / +4H / +both-aligned. Longs need trend up, shorts down.

h1 entry, all pairs, per class, fixed RR, chronological OOS split (both halves +).

Run: python ob_htf_trend_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
IMP = 1.0            # impulse body >= IMP*ATR
OB_LOOK = 5          # bars back to find the order-block candle
RETR = 20            # bars to retrace into the zone
BUF = 0.10           # stop buffer beyond the zone (ATR)
COOLDOWN = 3
HOLD = 72
RRS = [1.5, 2.0]


def ema(vals, p):
    k = 2.0 / (p + 1); out = [None] * len(vals); e = None
    for i, v in enumerate(vals):
        e = v if e is None else v * k + e * (1 - k); out[i] = e
    return out


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def trend_series(bars, fast, slow):
    """'up'/'down' per bar: EMA(fast) > EMA(slow) (or close>EMA(slow) if fast is None)."""
    c = [b['c'] for b in bars]; es = ema(c, slow)
    ef = ema(c, fast) if fast else c
    out = []
    for i in range(len(bars)):
        if es[i] is None or (fast and ef[i] is None):
            out.append(None)
        else:
            out.append('up' if ef[i] > es[i] else 'down')
    return [b['_ts'] for b in bars], out


def prior_trend(ts_list, tr, ts, span):
    """Trend from the most recent HTF bar that CLOSED before ts (no look-ahead)."""
    bucket = (ts // span) * span
    idx = bisect.bisect_left(ts_list, bucket) - 1
    return tr[idx] if 0 <= idx < len(tr) else None


def walk(bars, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


VARIANTS = ('unfiltered', '+daily', '+4h', '+both')


def _ok(d, dt, ht):
    want = 'up' if d == 'bull' else 'down'
    return {'unfiltered': True, '+daily': dt == want, '+4h': ht == want,
            '+both': dt == want and ht == want}


def scan(h1, dts, dtr, hts, htr, store, cls):
    n = len(h1); last = -1
    for j in range(OB_LOOK + 2, n - 2):
        if j <= last:
            continue
        a = atr(h1, 14, j) or 0.0
        if a <= 0:
            continue
        body = h1[j]['c'] - h1[j]['o']
        d = None
        if body >= IMP * a:                       # bullish impulse -> demand zone (last down candle)
            ob = next((k for k in range(j - 1, max(-1, j - 1 - OB_LOOK), -1) if h1[k]['c'] < h1[k]['o']), None)
            if ob is None:
                continue
            z_top = h1[ob]['h']; z_bot = h1[ob]['l']; d = 'bull'
        elif -body >= IMP * a:                    # bearish impulse -> supply zone (last up candle)
            ob = next((k for k in range(j - 1, max(-1, j - 1 - OB_LOOK), -1) if h1[k]['c'] > h1[k]['o']), None)
            if ob is None:
                continue
            z_top = h1[ob]['h']; z_bot = h1[ob]['l']; d = 'bear'
        if d is None:
            continue
        for r in range(j + 1, min(j + 1 + RETR, n - 1)):
            b = h1[r]; hit = (b['l'] <= z_top) if d == 'bull' else (b['h'] >= z_bot)
            if not hit:
                continue
            entry = b['c']
            stop = z_bot - BUF * a if d == 'bull' else z_top + BUF * a
            if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
                last = r + COOLDOWN; break
            ts = h1[r + 1]['_ts']
            dt = prior_trend(dts, dtr, ts, 86400); ht = prior_trend(hts, htr, ts, 4 * 3600)
            allow = _ok(d, dt, ht)
            for rr in RRS:
                o = walk(h1, r + 1, entry, stop, d, rr)
                if o is not None:
                    net = o - cost(o, entry, abs(entry - stop))
                    for v in VARIANTS:
                        if allow[v]:
                            store[v][cls][rr].append((ts, net))
            last = r + COOLDOWN; break


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<8} RR{rr} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = {v: defaultdict(lambda: defaultdict(list)) for v in VARIANTS}; npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npr += 1
        dts, dtr = trend_series(daily, None, 50)             # daily close vs EMA50
        b4 = make_4h(h1); hts, htr = trend_series(b4, 20, 50)  # 4H EMA20 vs EMA50
        scan(h1, dts, dtr, hts, htr, store, cls)
    print("=" * 88)
    print(f"Demand/supply-zone retrace + HTF trend filter (h1 entry, MARKET fills) — {npr} pairs")
    print("=" * 88)
    for v in VARIANTS:
        print(f"\n### {v} ###")
        for c in ['comm', 'crypto', 'index', 'major', 'minor']:
            for rr in RRS:
                line(c, store[v][c][rr], rr)
        for rr in RRS:
            line('ALL', [r for c in store[v] for r in store[v][c][rr]], rr)


if __name__ == '__main__':
    main()
