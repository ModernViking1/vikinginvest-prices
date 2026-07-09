"""Does MACD / RSI / Wyckoff / Golden-Cross time the Strategy-5 entry better than
the 4H engulfing? Keep the validated S5 CONTEXT (weekly trend + daily-50EMA
pullback + ADX>22); swap only the 4H entry TRIGGER. Compare each trigger's
selected entries (realistic fill, structural stop, RR2/RR3, OOS split).
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS, macd_series
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import ema, atr, adx, agg4h, weekly, walk, cost, is_engulf, HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RRS = [2.0, 3.0]
TRIGGERS = ('engulf', 'macd', 'rsi', 'wyckoff', 'golden')


def sma(vals, n, i):
    return None if i + 1 < n else sum(vals[i-n+1:i+1]) / n


def fires(b4, i, d, trig, pre):
    macd, sig, rsi, closes = pre
    if trig == 'engulf':
        return is_engulf(b4, i, d)
    if trig == 'macd':
        if None in (macd[i-1], macd[i], sig[i-1], sig[i]): return False
        return (d == 'bull' and macd[i-1] <= sig[i-1] and macd[i] > sig[i]) or \
               (d == 'bear' and macd[i-1] >= sig[i-1] and macd[i] < sig[i])
    if trig == 'rsi':
        if rsi[i-1] is None or rsi[i] is None: return False
        return (d == 'bull' and rsi[i-1] <= 50 < rsi[i]) or (d == 'bear' and rsi[i-1] >= 50 > rsi[i])
    if trig == 'wyckoff':
        LB = 20
        if i < LB: return False
        sup = min(b['l'] for b in b4[i-LB:i]); res = max(b['h'] for b in b4[i-LB:i])
        return (d == 'bull' and b4[i]['l'] < sup and b4[i]['c'] > sup) or \
               (d == 'bear' and b4[i]['h'] > res and b4[i]['c'] < res)
    if trig == 'golden':
        a0, a1 = sma(closes, 10, i-1), sma(closes, 10, i)
        b0, b1 = sma(closes, 50, i-1), sma(closes, 50, i)
        if None in (a0, a1, b0, b1): return False
        return (d == 'bull' and a0 <= b0 and a1 > b1) or (d == 'bear' and a0 >= b0 and a1 < b1)
    return False


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list)   # (trigger, rr) -> [(ts, r)]
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1); wk = weekly(daily)
        if len(wk) < 12 or len(b4) < 250: continue
        wc = [b['c'] for b in wk]; we20 = ema(wc, 10)
        dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
        d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
        closes4 = [b['c'] for b in b4]; m4, s4 = macd_series(closes4, 12, 26, 9); r4 = precompute_rsi(closes4, 14)
        pre = (m4, s4, r4, closes4)
        last = {t: -1 for t in TRIGGERS}
        for i in range(2, len(b4) - 1):
            ts = b4[i]['_ts']
            di = bisect.bisect_right(d_ts, ts) - 1; wi = bisect.bisect_right(w_ts, ts) - 1
            if di < 51 or wi < 11 or we20[wi] is None or de50[di] is None: continue
            wk_up = wc[wi] > we20[wi] and we20[wi] > we20[wi-1]
            wk_dn = wc[wi] < we20[wi] and we20[wi] < we20[wi-1]
            if not (wk_up or wk_dn): continue
            a = atr(daily, 14, di)
            if a is None or abs(daily[di]['c'] - de50[di]) > 0.5 * a: continue
            av = adx(b4, 14, i)
            if av is None or av < 22: continue
            d = 'bull' if wk_up else 'bear'
            for trig in TRIGGERS:
                if i <= last[trig]: continue
                if not fires(b4, i, d, trig, pre): continue
                stop = min(b4[i]['l'], b4[i-1]['l']) if d == 'bull' else max(b4[i]['h'], b4[i-1]['h'])
                entry = b4[i+1]['o']
                if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
                R = abs(entry - stop)
                for rr in RRS:
                    o = walk(b4, i+1, entry, stop, d, rr, HOLD['4h'])
                    if o is not None:
                        store[(trig, rr)].append((ts, PAIR_CLASS.get(pk), o - cost(o, entry, R)))
                last[trig] = i + 1

    print("STRATEGY-5 ENTRY TRIGGER comparison (context held; only the trigger changes)")
    print(f"  {'trigger':<10} {'RR':>3} {'n':>5} {'WR%':>6} {'expR':>8}  {'OOS h1/h2':>16} verdict")
    for trig in TRIGGERS:
        for rr in RRS:
            rows = sorted(store[(trig, rr)]); seq = [r for _, _, r in rows]
            n, w, e = agg(seq); mid = len(rows)//2
            _, _, eh = agg([r for _, _, r in rows[:mid]]); _, _, es = agg([r for _, _, r in rows[mid:]])
            v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else 'fail'
            tag = '  <- baseline' if trig == 'engulf' else ''
            print(f"  {trig:<10} {rr:>3.0f} {n:>5} {w:>6.1f} {e:>+8.3f}  {eh:>+7.3f}/{es:>+7.3f} {v}{tag}")

    # ---- rigorous deep-dive on the RSI trigger (the standout) ----
    import datetime
    def dt(ms): return datetime.datetime.fromtimestamp(ms, datetime.timezone.utc).strftime('%m-%d')
    print("\nDEEP-DIVE — RSI-trigger S5: rolling walk-forward (6 folds):")
    for rr in RRS:
        rows = sorted(store[('rsi', rr)]); K = 6; n = len(rows); fold = n // K; pos = 0
        cells = []
        for f in range(K):
            seg = rows[f*fold:(f+1)*fold if f < K-1 else n]
            _, w, e = agg([r for _, _, r in seg])
            cells.append(f"{e:>+.2f}")
            if e > 0: pos += 1
        print(f"  RR{rr:.0f}: folds=[{'  '.join(cells)}]  positive={pos}/{K}")
    print("DEEP-DIVE — RSI-trigger S5: per-class (RR2 / RR3):")
    for cls in ('comm', 'crypto', 'index', 'major', 'minor'):
        r2 = [r for _, c, r in store[('rsi', 2.0)] if c == cls]
        r3 = [r for _, c, r in store[('rsi', 3.0)] if c == cls]
        n2, w2, e2 = agg(r2); _, w3, e3 = agg(r3)
        print(f"  {cls:<8} n={n2:>3}  RR2 {e2:>+.3f}/{w2:>4.1f}%   RR3 {e3:>+.3f}/{w3:>4.1f}%")
    # compare RSI vs engulf on the SAME setups (does rsi just pick different bars?)
    eng_ts = set(t for t, _, _ in store[('engulf', 2.0)])
    rsi_ts = set(t for t, _, _ in store[('rsi', 2.0)])
    print(f"\n  entry-bar overlap engulf∩rsi: {len(eng_ts & rsi_ts)} of engulf={len(eng_ts)} rsi={len(rsi_ts)} "
          f"(they mostly select DIFFERENT bars)" )


if __name__ == '__main__':
    main()
