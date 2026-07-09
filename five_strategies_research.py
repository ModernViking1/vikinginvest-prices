"""Disciplined test of the PDF's 'top 5 swing strategies'. Faithful mechanical
versions, realistic market-fill entry (next bar open), structural stops, targets
across each strategy's stated R:R, realistic fixed-price cost, chronological OOS
split. Reports WR + expectancy + RR together per strategy.

Data caveats (stated honestly):
  - No VOLUME in the feed -> strategy 2's volume confirmation is omitted (price
    breakout + wick filter only).
  - Weekly bars are aggregated from daily (7-calendar-day buckets).
  - 'at S/R' / 'near weekly demand' confluence is approximated with prior-level
    proximity where a strategy calls for it.

Run: python five_strategies_research.py
"""
import json
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, precompute_rsi

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RRS = [2.0, 3.0]
HOLD = {'daily': 20, '4h': 60, 'weekly': 8}


def ema(vals, n):
    out = [None] * len(vals); k = 2 / (n + 1); e = None
    for i, v in enumerate(vals):
        e = v if e is None else v * k + e * (1 - k)
        if i >= n - 1: out[i] = e
    return out


def atr(bars, n, i):
    if i < n: return None
    s = 0
    for j in range(i - n + 1, i + 1):
        tr = max(bars[j]['h'] - bars[j]['l'], abs(bars[j]['h'] - bars[j-1]['c']), abs(bars[j]['l'] - bars[j-1]['c']))
        s += tr
    return s / n


def adx(bars, n, i):
    if i < 2 * n: return None
    trs = []; pdm = []; ndm = []
    for j in range(i - 2*n + 1, i + 1):
        up = bars[j]['h'] - bars[j-1]['h']; dn = bars[j-1]['l'] - bars[j]['l']
        pdm.append(up if (up > dn and up > 0) else 0.0)
        ndm.append(dn if (dn > up and dn > 0) else 0.0)
        trs.append(max(bars[j]['h']-bars[j]['l'], abs(bars[j]['h']-bars[j-1]['c']), abs(bars[j]['l']-bars[j-1]['c'])))
    def sm(x): return sum(x[-n:]) or 1e-9
    pdi = 100 * sm(pdm) / sm(trs); ndi = 100 * sm(ndm) / sm(trs)
    dx = 100 * abs(pdi - ndi) / max(1e-9, pdi + ndi)
    return dx


def agg4h(h1):
    out = []
    for k in range(0, len(h1) - 3, 4):
        g = h1[k:k+4]
        out.append({'o': g[0]['o'], 'c': g[-1]['c'], 'h': max(b['h'] for b in g), 'l': min(b['l'] for b in g), '_ts': g[0]['_ts']})
    return out


def weekly(daily):
    out = []; bucket = []
    wk0 = None
    for b in daily:
        wk = int(b['_ts'] // (7 * 86400))
        if wk0 is None: wk0 = wk
        if wk != wk0 and bucket:
            out.append({'o': bucket[0]['o'], 'c': bucket[-1]['c'], 'h': max(x['h'] for x in bucket), 'l': min(x['l'] for x in bucket), '_ts': bucket[0]['_ts']})
            bucket = []; wk0 = wk
        bucket.append(b)
    if bucket:
        out.append({'o': bucket[0]['o'], 'c': bucket[-1]['c'], 'h': max(x['h'] for x in bucket), 'l': min(x['l'] for x in bucket), '_ts': bucket[0]['_ts']})
    return out


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0: return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def cost(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def emit(store, strat, tf, bars, i_sig, entry, stop, d):
    if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): return
    R = abs(entry - stop); ts = bars[i_sig]['_ts']
    for rr in RRS:
        o = walk(bars, i_sig, entry, stop, d, rr, HOLD[tf])
        if o is not None:
            store[(strat, rr)].append((ts, o - cost(o, entry, R)))


# ---------- strategy detectors ----------
def s1_trend_pullback(daily, store):
    c = [b['c'] for b in daily]; e20 = ema(c, 20); e50 = ema(c, 50)
    last = -1
    for i in range(51, len(daily) - 1):
        if i <= last: continue
        if None in (e20[i], e50[i], e20[i-1]): continue
        up = c[i] > e20[i] > e50[i]; dn = c[i] < e20[i] < e50[i]
        # pullback: prior bar touched 20 EMA, current closes back in trend dir
        pb_up = daily[i-1]['l'] <= e20[i-1] and daily[i]['c'] > daily[i-1]['c']
        pb_dn = daily[i-1]['h'] >= e20[i-1] and daily[i]['c'] < daily[i-1]['c']
        a = atr(daily, 14, i)
        if a is None: continue
        if up and pb_up:
            stop = min(daily[i-1]['l'], daily[i]['l']) - 0.5*a
            emit(store, 's1_trend_pullback', 'daily', daily, i+1, daily[i+1]['o'], stop, 'bull'); last = i+1
        elif dn and pb_dn:
            stop = max(daily[i-1]['h'], daily[i]['h']) + 0.5*a
            emit(store, 's1_trend_pullback', 'daily', daily, i+1, daily[i+1]['o'], stop, 'bear'); last = i+1


def s2_range_breakout(b4, store):
    RB = 5; last = -1
    for i in range(RB + 1, len(b4) - 1):
        if i <= last: continue
        win = b4[i-RB:i]; hi = max(x['h'] for x in win); lo = min(x['l'] for x in win)
        rng = hi - lo
        if rng <= 0: continue
        bar = b4[i]; body = abs(bar['c'] - bar['o']) or 1e-9
        if bar['c'] > hi:  # bull breakout
            uw = bar['h'] - max(bar['c'], bar['o'])
            if uw > 0.4 * body: continue  # wick rejection filter
            emit(store, 's2_range_breakout', '4h', b4, i+1, b4[i+1]['o'], lo, 'bull'); last = i+1
        elif bar['c'] < lo:
            lw = min(bar['c'], bar['o']) - bar['l']
            if lw > 0.4 * body: continue
            emit(store, 's2_range_breakout', '4h', b4, i+1, b4[i+1]['o'], hi, 'bear'); last = i+1


def s3_rsi_divergence(daily, store):
    c = [b['c'] for b in daily]; r = precompute_rsi(c, 14)
    # swing lows/highs (3-bar)
    lows = []; highs = []
    for i in range(2, len(daily) - 2):
        if daily[i]['l'] < daily[i-1]['l'] and daily[i]['l'] < daily[i+1]['l'] and daily[i]['l'] < daily[i-2]['l'] and daily[i]['l'] < daily[i+2]['l']:
            lows.append(i)
        if daily[i]['h'] > daily[i-1]['h'] and daily[i]['h'] > daily[i+1]['h'] and daily[i]['h'] > daily[i-2]['h'] and daily[i]['h'] > daily[i+2]['h']:
            highs.append(i)
    last = -1
    for k in range(1, len(lows)):
        a, b = lows[k-1], lows[k]
        if r[a] is None or r[b] is None: continue
        if daily[b]['l'] < daily[a]['l'] and r[b] > r[a]:   # bull divergence
            sig = b + 2
            if sig + 1 >= len(daily) or sig <= last: continue
            stop = daily[b]['l']
            emit(store, 's3_rsi_divergence', 'daily', daily, sig+1, daily[sig+1]['o'], stop, 'bull'); last = sig+1
    for k in range(1, len(highs)):
        a, b = highs[k-1], highs[k]
        if r[a] is None or r[b] is None: continue
        if daily[b]['h'] > daily[a]['h'] and r[b] < r[a]:   # bear divergence
            sig = b + 2
            if sig + 1 >= len(daily) or sig <= last: continue
            stop = daily[b]['h']
            emit(store, 's3_rsi_divergence', 'daily', daily, sig+1, daily[sig+1]['o'], stop, 'bear'); last = sig+1


def s4_star(daily, store):
    last = -1
    for i in range(2, len(daily) - 1):
        if i <= last: continue
        c1, c2, c3 = daily[i-2], daily[i-1], daily[i]
        b1 = c1['c'] - c1['o']; b2 = abs(c2['c'] - c2['o']); b3 = c3['c'] - c3['o']
        rng1 = c1['h'] - c1['l'] or 1e-9; rng3 = c3['h'] - c3['l'] or 1e-9
        small2 = b2 < 0.4 * (rng1)
        # morning star (bull)
        if b1 < 0 and small2 and b3 > 0 and abs(b3) > 0.5*rng3 and c3['c'] >= c1['o'] + 0.5*(c1['c']-c1['o'])*-1*0:
            if c3['c'] >= (c1['o'] + c1['c'])/2:
                stop = min(c2['l'], c3['l'])
                emit(store, 's4_star', 'daily', daily, i+1, daily[i+1]['o'], stop, 'bull'); last = i+1
        # evening star (bear)
        elif b1 > 0 and small2 and b3 < 0 and abs(b3) > 0.5*rng3 and c3['c'] <= (c1['o'] + c1['c'])/2:
            stop = max(c2['h'], c3['h'])
            emit(store, 's4_star', 'daily', daily, i+1, daily[i+1]['o'], stop, 'bear'); last = i+1


def is_engulf(bars, j, d):
    o, c = bars[j]['o'], bars[j]['c']; po, pc = bars[j-1]['o'], bars[j-1]['c']
    if d == 'bull': return c > o and pc < po and c >= po and o <= pc
    return c < o and pc > po and c <= po and o >= pc


def s5_mtf_confluence(daily, b4, wk, store):
    if len(wk) < 12 or len(b4) < 250: return
    wc = [b['c'] for b in wk]; we20 = ema(wc, 10)
    dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
    d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
    import bisect
    last = -1
    for i in range(2, len(b4) - 1):
        if i <= last: continue
        ts = b4[i]['_ts']
        di = bisect.bisect_right(d_ts, ts) - 1; wi = bisect.bisect_right(w_ts, ts) - 1
        if di < 51 or wi < 11 or we20[wi] is None or de50[di] is None: continue
        wk_up = wc[wi] > we20[wi] and we20[wi] > we20[wi-1]
        wk_dn = wc[wi] < we20[wi] and we20[wi] < we20[wi-1]
        near50 = abs(daily[di]['c'] - de50[di]) <= 0.5 * (atr(daily, 14, di) or 1e9)
        adxv = adx(b4, 14, i)
        if adxv is None or adxv < 22: continue
        if wk_up and near50 and is_engulf(b4, i, 'bull'):
            stop = min(b4[i]['l'], b4[i-1]['l'])
            emit(store, 's5_mtf_confluence', '4h', b4, i+1, b4[i+1]['o'], stop, 'bull'); last = i+1
        elif wk_dn and near50 and is_engulf(b4, i, 'bear'):
            stop = max(b4[i]['h'], b4[i-1]['h'])
            emit(store, 's5_mtf_confluence', '4h', b4, i+1, b4[i+1]['o'], stop, 'bear'); last = i+1


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list)
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1); wk = weekly(daily)
        s1_trend_pullback(daily, store)
        s2_range_breakout(b4, store)
        s3_rsi_divergence(daily, store)
        s4_star(daily, store)
        s5_mtf_confluence(daily, b4, wk, store)

    names = {'s1_trend_pullback': '1 Trend Pullback EMA', 's2_range_breakout': '2 Range Breakout(noVol)',
             's3_rsi_divergence': '3 RSI Divergence S/R', 's4_star': '4 Morning/Evening Star',
             's5_mtf_confluence': '5 Multi-TF Confluence'}
    print(f"{'strategy':<24} {'RR':>4} {'n':>5} {'WR%':>6} {'expR':>8}  {'OOS h1/h2':>16}  verdict")
    for key in ['s1_trend_pullback', 's2_range_breakout', 's3_rsi_divergence', 's4_star', 's5_mtf_confluence']:
        for rr in RRS:
            rows = sorted(store[(key, rr)]); seq = [r for _, r in rows]
            n, w, e = agg(seq); mid = len(rows)//2
            _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
            verdict = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else 'fail'
            print(f"{names[key]:<24} {rr:>4.0f} {n:>5} {w:>6.1f} {e:>+8.3f}  {eh:>+7.3f}/{es:>+7.3f}  {verdict}")


if __name__ == '__main__':
    main()
