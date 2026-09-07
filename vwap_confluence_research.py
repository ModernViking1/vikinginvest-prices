"""Can any confluence indicator rescue VWAP mean-reversion at 5m — reduce N and lift the
NET (cost-adjusted) win rate/expectancy enough to survive? Tests 12 indicator filters on
the vwap_mr fade, per asset class, across all pairs in m5-sample-ohlc.json.

Method: generate the base vwap_mr fades once (close beyond VWAP +/- K*sigma), score each
NET OF COSTS at fixed RR2, and snapshot indicator values at each signal bar. Each filter
is then just a subset — so all 12 are compared on identical trades. A filter "works" only
if it turns the class NET-positive with BOTH chronological OOS halves + at a still-usable N
(and doesn't just cherry-pick one thin cell — with 12x5 cells, some pass by chance).

Run: python vwap_confluence_research.py   (needs m5-sample-ohlc.json)
"""
import json
import os
from datetime import datetime, timezone
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm, precompute_rsi
from detect_triggers import PAIR_CLASS
from five_strategies_research import cost
from vwap_research import session_vwap, WARMUP

_HERE = os.path.dirname(os.path.abspath(__file__))
M5 = os.path.join(_HERE, 'm5-sample-ohlc.json')
K = 2.0            # base sigma-band for the fade
STOP_ATR = 1.5     # stop = 1.5*ATR (a touch wider than raw ATR — cost drag on 1.0*ATR is hopeless)
RR = 2.0
HOLD = 288


def _ema_arr(v, n):
    out = [None] * len(v); k = 2 / (n + 1); e = None
    for i, x in enumerate(v):
        e = x if e is None else x * k + e * (1 - k)
        out[i] = e
    return out


def _atr_arr(bars, n=14):
    tr = [0.0] * len(bars)
    for i, b in enumerate(bars):
        tr[i] = b['h'] - b['l'] if i == 0 else max(b['h'] - b['l'], abs(b['h'] - bars[i-1]['c']), abs(b['l'] - bars[i-1]['c']))
    out = [None] * len(bars); s = 0.0
    for i in range(len(bars)):
        s += tr[i]
        if i >= n:
            s -= tr[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


def _bb_arr(c, n=20, k=2.0):
    up = [None] * len(c); lo = [None] * len(c)
    for i in range(len(c)):
        if i >= n - 1:
            w = c[i - n + 1:i + 1]; m = sum(w) / n
            sd = (sum((x - m) ** 2 for x in w) / n) ** 0.5
            up[i] = m + k * sd; lo[i] = m - k * sd
    return up, lo


def _stoch_arr(h, l, c, n=14):
    out = [None] * len(c)
    for i in range(len(c)):
        if i >= n - 1:
            hh = max(h[i - n + 1:i + 1]); ll = min(l[i - n + 1:i + 1])
            out[i] = 100 * (c[i] - ll) / (hh - ll) if hh > ll else 50.0
    return out


def _macd_hist(c):
    e12 = _ema_arr(c, 12); e26 = _ema_arr(c, 26)
    macd = [(a - b) if a is not None and b is not None else 0.0 for a, b in zip(e12, e26)]
    sig = _ema_arr(macd, 9)
    return [m - (s or 0) for m, s in zip(macd, sig)]


def _adx_arr(bars, n=14):
    N = len(bars); tr = [0.0] * N; pdm = [0.0] * N; ndm = [0.0] * N
    for i in range(1, N):
        up = bars[i]['h'] - bars[i-1]['h']; dn = bars[i-1]['l'] - bars[i]['l']
        pdm[i] = up if (up > dn and up > 0) else 0.0
        ndm[i] = dn if (dn > up and dn > 0) else 0.0
        tr[i] = max(bars[i]['h'] - bars[i]['l'], abs(bars[i]['h'] - bars[i-1]['c']), abs(bars[i]['l'] - bars[i-1]['c']))
    out = [None] * N
    if N < 2 * n:
        return out
    atr = sum(tr[1:n+1]); sp = sum(pdm[1:n+1]); sn = sum(ndm[1:n+1]); dxs = []
    for i in range(n + 1, N):
        atr = atr - atr / n + tr[i]; sp = sp - sp / n + pdm[i]; sn = sn - sn / n + ndm[i]
        if atr <= 0:
            continue
        pdi = 100 * sp / atr; ndi = 100 * sn / atr
        dx = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) else 0.0
        dxs.append(dx)
        if len(dxs) >= n:
            out[i] = sum(dxs[-n:]) / n
    return out


def _score_net(bars, ei, entry, stop, d):
    R = abs(entry - stop)
    if R <= 0 or ei >= len(bars):
        return None
    tgt = entry + RR * R if d == 'bull' else entry - RR * R
    o = None
    for j in range(ei + 1, min(ei + 1 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                o = -1.0; break
            if b['h'] >= tgt:
                o = RR; break
        else:
            if b['h'] >= stop:
                o = -1.0; break
            if b['l'] <= tgt:
                o = RR; break
    if o is None:
        return None
    return o - cost(o, entry, R)


def _stat(seq):
    seq = sorted(seq); rs = [r for _, r in seq]; n = len(rs)
    if not n:
        return (0, 0, 0, 0, 0)
    wr = 100 * sum(1 for x in rs if x > 0) / n
    exp = sum(rs) / n; m = n // 2
    return (n, wr, exp, sum(rs[:m]) / m if m else 0, sum(rs[m:]) / (n - m) if n - m else 0)


# filter name -> predicate(snapshot) ; snapshot carries dir + indicator values
FILTERS = {
    'rsi_extreme':   lambda s: (s['rsi'] is not None) and (s['rsi'] > 70 if s['d'] == 'bear' else s['rsi'] < 30),
    'bollinger':     lambda s: s['bb_out'],
    'volume_spike':  lambda s: s['vol_ratio'] >= 1.5,
    'stoch_extreme': lambda s: (s['stoch'] is not None) and (s['stoch'] > 80 if s['d'] == 'bear' else s['stoch'] < 20),
    'adx_low<20':    lambda s: (s['adx'] is not None) and s['adx'] < 20,
    'sigma>=3':      lambda s: s['sig_mult'] >= 3.0,
    'atrdist>=2':    lambda s: s['atr_dist'] >= 2.0,
    'rejection_wick': lambda s: s['wick'],
    'macd_fading':   lambda s: s['macd_fade'],
    'ema_far>=2atr': lambda s: s['ema_dist'] >= 2.0,
    'consec>=3':     lambda s: s['consec'] >= 3,
    'midsession':    lambda s: 7 <= s['hour'] <= 20,
}


def main():
    if not os.path.exists(M5):
        print("no m5-sample-ohlc.json — run m5-sample-fetch first"); return
    pairs = json.load(open(M5)).get('pairs', {})

    # base signals with per-signal net-R + indicator snapshot
    rows = []          # (cls, ts, net_r, snapshot)
    for pk, v in pairs.items():
        bars = _bars_norm(v.get('m5', []))
        if len(bars) < 400:
            continue
        cls = PAIR_CLASS.get(pk, '?')
        c = [b['c'] for b in bars]; h = [b['h'] for b in bars]; lo = [b['l'] for b in bars]
        vol = [b.get('v', 0) or 0 for b in bars]
        vw = session_vwap(bars); rsi = precompute_rsi(c); atr = _atr_arr(bars)
        bbu, bbl = _bb_arr(c); stoch = _stoch_arr(h, lo, c); mh = _macd_hist(c)
        adx = _adx_arr(bars); ema20 = _ema_arr(c, 20)
        last = -1
        for i in range(len(bars) - 1):
            if i <= last:
                continue
            vwap, sigma, kbar = vw[i]
            if kbar < WARMUP or sigma <= 0 or atr[i] is None or atr[i] <= 0:
                continue
            b = bars[i]
            if b['c'] > vwap + K * sigma:
                d = 'bear'
            elif b['c'] < vwap - K * sigma:
                d = 'bull'
            else:
                continue
            entry = b['c']; stop = entry + STOP_ATR * atr[i] if d == 'bear' else entry - STOP_ATR * atr[i]
            r = _score_net(bars, i + 1, entry, stop, d)
            last = i + 2
            if r is None:
                continue
            # snapshot
            body = abs(b['c'] - b['o']) or 1e-9
            wick = (b['h'] - max(b['o'], b['c'])) > body if d == 'bear' else (min(b['o'], b['c']) - b['l']) > body
            vm = sorted(vol[max(0, i - 20):i]); med = vm[len(vm) // 2] if vm else 0
            consec = 0
            for j in range(i, 0, -1):
                up = bars[j]['c'] > bars[j]['o']
                if (d == 'bear' and up) or (d == 'bull' and not up):
                    consec += 1
                else:
                    break
            macd_fade = abs(mh[i]) < abs(mh[i - 1]) if i > 0 else False
            snap = {'d': d, 'rsi': rsi[i], 'stoch': stoch[i], 'adx': adx[i],
                    'bb_out': (bbu[i] is not None and (b['c'] > bbu[i] if d == 'bear' else b['c'] < bbl[i])),
                    'vol_ratio': (vol[i] / med if med > 0 else 0), 'sig_mult': abs(b['c'] - vwap) / sigma,
                    'atr_dist': abs(b['c'] - vwap) / atr[i], 'wick': wick, 'macd_fade': macd_fade,
                    'ema_dist': (abs(b['c'] - ema20[i]) / atr[i] if ema20[i] is not None else 0),
                    'consec': consec, 'hour': datetime.fromtimestamp(b['_ts'], timezone.utc).hour}
            rows.append((cls, b['_ts'], r, snap))

    classes = ['index', 'comm', 'crypto', 'major', 'minor']

    def report(label, subset):
        by = defaultdict(list)
        for cls, ts, r, _ in subset:
            by[cls].append((ts, r))
        allv = [(ts, r) for _, ts, r, _ in subset]
        n, wr, exp, eh, es = _stat(allv)
        print("\n%-16s ALL n=%5d WR=%2.0f%% net=%+0.3fR [%+0.3f/%+0.3f]" % (label, n, wr, exp, eh, es))
        for cls in classes:
            if by.get(cls):
                n, wr, exp, eh, es = _stat(by[cls])
                gate = n >= 40 and eh > 0 and es > 0 and exp > 0
                if gate:
                    print("   %-8s n=%4d WR=%2.0f%% net=%+0.3fR [%+0.3f/%+0.3f]  <== NET GATE PASS" % (cls, n, wr, exp, eh, es))

    print("=" * 92)
    print("VWAP mean-reversion @ 5m + confluence filters — NET OF COSTS (K=2.0, stop=1.5xATR, RR2)")
    print("=" * 92)
    report('BASE (no filter)', rows)
    for name, pred in FILTERS.items():
        sub = [row for row in rows if pred(row[3])]
        report(name, sub)
    print("\n(only NET GATE PASS cells shown per filter: n>=40, both OOS halves +, net exp>0)")


if __name__ == '__main__':
    main()
