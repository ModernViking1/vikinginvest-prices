"""Indicators as an ENTRY-TIMING trigger on the H&S+macro setup (not a directional
filter). The H&S gives the setup (direction + structural stop); instead of entering
immediately on the neckline break, WAIT for the indicator to fire on H1 within a
window, then enter. Does momentum-timed entry beat immediate entry?

Triggers (H1, in the trade direction), searched in [break, break+WINDOW]:
  macd    : MACD(12,26,9) / signal cross
  rsi     : RSI(14) centerline (50) cross
  wyckoff : spring (bull) / upthrust (bear) vs 20-bar level
  golden  : SMA10 / SMA50 cross (fast 'golden-style' cross; 50/200 is far too slow
            to time an intraday H&S, so a faster proxy is used and labelled as such)

Entry = trigger bar's next open; stop = original structural (RS) stop; target = RR
from the new entry. Realistic cost, OOS split. Reports fill-rate (setups that got
a trigger), mean entry delay, and WR/EV vs the immediate-entry BASELINE.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import (
    PAIR_CLASS, macd_series, auto_detect_ew,
    AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from hs_swing_research import scan as hs_scan, MAX_HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
WINDOW = 24          # h1 bars after the break to wait for a trigger (~1 day)
WYCK_LB = 20
RR = 2.0


def sma(vals, n, i):
    return None if i + 1 < n else sum(vals[i - n + 1:i + 1]) / n


def walk(h1, i0, entry, stop, d, rr):
    R = abs(entry - stop)
    if R <= 0: return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + MAX_HOLD, len(h1))):
        b = h1[j]
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


def trigger_idx(h1, brk, d, ind, pre):
    """First h1 index in [brk, brk+WINDOW] where `ind` fires in direction d."""
    macd, sig, rsi, closes = pre
    hi = min(brk + WINDOW, len(h1) - 1)
    for i in range(brk, hi):
        if ind == 'macd':
            if None in (macd[i-1], macd[i], sig[i-1], sig[i]): continue
            if d == 'bull' and macd[i-1] <= sig[i-1] and macd[i] > sig[i]: return i
            if d == 'bear' and macd[i-1] >= sig[i-1] and macd[i] < sig[i]: return i
        elif ind == 'rsi':
            if rsi[i-1] is None or rsi[i] is None: continue
            if d == 'bull' and rsi[i-1] <= 50 < rsi[i]: return i
            if d == 'bear' and rsi[i-1] >= 50 > rsi[i]: return i
        elif ind == 'wyckoff':
            if i < WYCK_LB: continue
            sup = min(b['l'] for b in h1[i-WYCK_LB:i]); res = max(b['h'] for b in h1[i-WYCK_LB:i])
            if d == 'bull' and h1[i]['l'] < sup and h1[i]['c'] > sup: return i
            if d == 'bear' and h1[i]['h'] > res and h1[i]['c'] < res: return i
        elif ind == 'golden':
            a0, a1 = sma(closes, 10, i-1), sma(closes, 10, i)
            b0, b1 = sma(closes, 50, i-1), sma(closes, 50, i)
            if None in (a0, a1, b0, b1): continue
            if d == 'bull' and a0 <= b0 and a1 > b1: return i
            if d == 'bear' and a0 >= b0 and a1 < b1: return i
    return None


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def oos(label, rows, extra=''):
    rows = sorted(rows, key=lambda z: z[0])
    seq = [r for (_, r) in rows]
    n, w, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for (_, r) in rows[:mid]])
    _, _, es = agg([r for (_, r) in rows[mid:]])
    ok = 'PASS' if (eh > 0 and es > 0 and n >= 40) else 'fail'
    print(f"  {label:<22} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}  OOS[{eh:>+6.3f}/{es:>+6.3f}] {ok} {extra}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    base = []
    timed = defaultdict(list)
    delays = defaultdict(list)
    total_setups = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 60: continue
        closes = [b['c'] for b in h1]
        macd, sig = macd_series(closes, 12, 26, 9)
        rsi = precompute_rsi(closes, 14)
        pre = (macd, sig, rsi, closes)
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
                macro = aew(dd); tdir = 'bear' if kind == 'bear' else 'bull'
                if not (macro is not None and macro != tdir): continue
                total_setups += 1
                brk = tr['entry_idx'] - 1
                # baseline immediate entry
                ob = walk(h1, tr['entry_idx'], tr['entry'], tr['stop'], tdir, RR)
                if ob is not None:
                    base.append((tr['ts'], ob - cost(ob, tr['entry'], tr['R'])))
                # indicator-timed entries
                for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
                    ti = trigger_idx(h1, brk, tdir, ind, pre)
                    if ti is None or ti + 1 >= len(h1): continue
                    entry = h1[ti + 1]['o']; stop = tr['stop']
                    if (tdir == 'bull' and stop >= entry) or (tdir == 'bear' and stop <= entry):
                        continue
                    R = abs(entry - stop)
                    o = walk(h1, ti + 1, entry, stop, tdir, RR)
                    if o is None: continue
                    timed[ind].append((tr['ts'], o - cost(o, entry, R)))
                    delays[ind].append(ti - brk)

    print(f"H&S · macro-OPPOSES setups: {total_setups}   (target 1:{RR:.0f})\n")
    print("ENTRY TIMING — immediate break vs indicator-timed entry:")
    oos("IMMEDIATE (baseline)", base)
    for ind in ('macd', 'rsi', 'wyckoff', 'golden'):
        fill = len(timed[ind])
        md = (sorted(delays[ind])[len(delays[ind])//2]) if delays[ind] else 0
        pct = 100 * fill / max(1, total_setups)
        oos(f"timed:{ind}", timed[ind], extra=f"fill={pct:.0f}% delay~{md}bar")
    print("\nNote: golden = SMA10/50 fast proxy (50/200 too slow to time an intraday H&S).")


if __name__ == '__main__':
    main()
