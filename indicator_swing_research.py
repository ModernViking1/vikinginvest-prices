"""Apply MACD / RSI / Wyckoff(spring-UTAD) / Golden-Cross to the DAILY and 4H
timeframe, two ways:

  INDEPENDENT : each indicator as a standalone swing signal (entry next bar open =
                realistic market fill, structural stop, 1:1 / 1:2 targets).
  INCLUSIVE   : each indicator as a directional CONFLUENCE filter on the validated
                H&S + macro-EW-opposition cohort — does requiring daily-TF
                agreement improve the edge?

All results reported with a chronological OOS split. Realistic fixed-price cost.
Nothing is committed until the numbers are reviewed.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import (
    PAIR_CLASS, macd_series, auto_detect_ew,
    AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from hs_swing_research import scan as hs_scan, MAX_HOLD as HS_HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RRS = [1.0, 2.0]
SWING_LB = 10
HOLD = {'daily': 20, '4h': 60}


def agg4h(h1):
    out = []
    for k in range(0, len(h1) - 3, 4):
        grp = h1[k:k + 4]
        out.append({'o': grp[0]['o'], 'c': grp[-1]['c'],
                    'h': max(b['h'] for b in grp), 'l': min(b['l'] for b in grp),
                    '_ts': grp[0]['_ts']})
    return out


def sma(vals, n, i):
    if i + 1 < n: return None
    return sum(vals[i - n + 1:i + 1]) / n


def struct_stop(bars, i, d):
    lo = bars[max(0, i - SWING_LB):i + 1]
    return min(b['l'] for b in lo) if d == 'bull' else max(b['h'] for b in lo)


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0: return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
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
    frac = R / abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def signals(bars, indicator):
    """Yield (i_signal, dir) for a given indicator on `bars`."""
    closes = [b['c'] for b in bars]
    n = len(bars)
    if indicator == 'macd':
        m, s = macd_series(closes, 12, 26, 9)
        for i in range(1, n - 1):
            if None in (m[i-1], m[i], s[i-1], s[i]): continue
            if m[i-1] <= s[i-1] and m[i] > s[i]: yield i, 'bull'
            elif m[i-1] >= s[i-1] and m[i] < s[i]: yield i, 'bear'
    elif indicator == 'rsi':
        r = precompute_rsi(closes, 14)
        for i in range(1, n - 1):
            if r[i-1] is None or r[i] is None: continue
            if r[i-1] <= 30 and r[i] > 30: yield i, 'bull'      # oversold reversal
            elif r[i-1] >= 70 and r[i] < 70: yield i, 'bear'    # overbought reversal
    elif indicator == 'wyckoff':
        LB = 20
        for i in range(LB, n - 1):
            sup = min(b['l'] for b in bars[i-LB:i]); res = max(b['h'] for b in bars[i-LB:i])
            if bars[i]['l'] < sup and bars[i]['c'] > sup: yield i, 'bull'    # spring
            elif bars[i]['h'] > res and bars[i]['c'] < res: yield i, 'bear'  # upthrust
    elif indicator == 'golden':
        for i in range(1, n - 1):
            a0, a1 = sma(closes, 50, i-1), sma(closes, 50, i)
            b0, b1 = sma(closes, 200, i-1), sma(closes, 200, i)
            if None in (a0, a1, b0, b1): continue
            if a0 <= b0 and a1 > b1: yield i, 'bull'
            elif a0 >= b0 and a1 < b1: yield i, 'bear'


def indicator_dir(bars, i, indicator):
    """Directional STATE of the indicator as of bar i (for the inclusive filter)."""
    closes = [b['c'] for b in bars]
    if indicator == 'macd':
        m, s = macd_series(closes, 12, 26, 9)
        if m[i] is None or s[i] is None: return None
        return 'bull' if m[i] > s[i] else 'bear'
    if indicator == 'rsi':
        r = precompute_rsi(closes, 14)
        if r[i] is None: return None
        return 'bull' if r[i] > 50 else 'bear'
    if indicator == 'golden':
        a, b = sma(closes, 50, i), sma(closes, 200, i)
        if a is None or b is None: return None
        return 'bull' if a > b else 'bear'
    if indicator == 'wyckoff':
        LB = 20
        if i < LB: return None
        sup = min(bb['l'] for bb in bars[i-LB:i]); res = max(bb['h'] for bb in bars[i-LB:i])
        for j in range(max(LB, i-3), i+1):
            if bars[j]['l'] < sup and bars[j]['c'] > sup: return 'bull'
            if bars[j]['h'] > res and bars[j]['c'] < res: return 'bear'
        return None


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def oosline(label, rows, key):
    n, w, e = agg([t[key] for t in rows])
    mid = len(rows)//2
    _, _, eh = agg([t[key] for t in rows[:mid]])
    _, _, es = agg([t[key] for t in rows[mid:]])
    ok = 'PASS' if (eh > 0 and es > 0 and n >= 40) else 'fail'
    print(f"  {label:<26} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}  OOS[{eh:>+6.3f}/{es:>+6.3f}] {ok}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    indep = defaultdict(list)          # (indicator, tf) -> outcomes at 1:2
    indep1 = defaultdict(list)         # at 1:1
    hs_rows = []                       # H&S macro-opposes cohort + per-indicator daily agree flags
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 60: continue
        tfbars = {'daily': daily, '4h': agg4h(h1)}
        # ---- INDEPENDENT ----
        for tf, bars in tfbars.items():
            for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
                last = -1
                for (i, dr) in signals(bars, ind):
                    if i <= last or i + 1 >= len(bars): continue
                    entry = bars[i+1]['o']; stop = struct_stop(bars, i, dr)
                    if (dr == 'bull' and stop >= entry) or (dr == 'bear' and stop <= entry): continue
                    R = abs(entry - stop); ts = bars[i+1]['_ts']
                    o2 = walk(bars, i+1, entry, stop, dr, 2.0, HOLD[tf])
                    o1 = walk(bars, i+1, entry, stop, dr, 1.0, HOLD[tf])
                    if o2 is not None:
                        indep[(ind, tf)].append((ts, o2 - cost(o2, entry, R)))
                    if o1 is not None:
                        indep1[(ind, tf)].append((ts, o1 - cost(o1, entry, R)))
                    last = i + 2
        # ---- INCLUSIVE (H&S macro-opposes + daily indicator agreement) ----
        d_ts = [b['_ts'] for b in daily]; cache = {}
        def aew(dd):
            if dd not in cache:
                try:
                    r = auto_detect_ew(draw[:dd+1]); e = r.get('ew') if r.get('ok') else None
                    cache[dd] = e['dir'] if (e and e.get('dir') in ('bull','bear') and e.get('confidence',0) >= AUTO_EW_MIN_CONFIDENCE and e.get('pattern') in AUTO_EW_VALID_PATTERNS) else None
                except Exception:
                    cache[dd] = None
            return cache[dd]
        for kind in ('bear', 'bull'):
            for tr in hs_scan(h1, kind):
                dd = bisect.bisect_right(d_ts, tr['ts']) - 2
                macro = aew(dd); tdir = 'bear' if tr['kind'] == 'bear' else 'bull'
                if not (macro is not None and macro != tdir): continue
                if dd < 1: continue
                R = tr['R']
                o2 = walk(h1, tr['entry_idx'], tr['entry'], tr['stop'], tdir, 2.0, HS_HOLD)
                if o2 is None: continue
                row = {'ts': tr['ts'], 'r': o2 - cost(o2, tr['entry'], R)}
                for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
                    idir = indicator_dir(daily, dd, ind)
                    row['agree_' + ind] = (idir == tdir)
                hs_rows.append(row)

    print("=" * 78)
    print("INDEPENDENT — each indicator as a standalone daily/4h swing signal")
    print("=" * 78)
    print("1:2 target:")
    for tf in ('daily', '4h'):
        for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
            rows = sorted(indep[(ind, tf)]); rows = [{'r': r, 'ts': t} for (t, r) in rows]
            oosline(f"{ind} · {tf}", rows, 'r')
    print("1:1 target:")
    for tf in ('daily', '4h'):
        for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
            rows = sorted(indep1[(ind, tf)]); rows = [{'r': r, 'ts': t} for (t, r) in rows]
            oosline(f"{ind} · {tf}", rows, 'r')

    print("\n" + "=" * 78)
    print("INCLUSIVE — H&S+macro edge, filtered by DAILY indicator agreement (1:2)")
    print("=" * 78)
    hs_rows.sort(key=lambda x: x['ts'])
    oosline("H&S+macro BASELINE", hs_rows, 'r')
    for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
        sub = [t for t in hs_rows if t.get('agree_' + ind)]
        oosline(f"  AND daily {ind} agrees", sub, 'r')


if __name__ == '__main__':
    main()
