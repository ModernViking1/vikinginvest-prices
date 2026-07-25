"""Trend-pullback strategy (user's full spec): daily EMA200 trend + entry-TF EMA20
pullback + RSI/ADX filters + rejection-candle break entry + structure stop + partial
at 1R / final at 2R + trend-failure exit.

Faithful build:
  - Trend (daily): close_D > ema200_D + 0.20*ATR_D (up) / mirror (down).
  - Entry TF (4H or 1D): RSI>50 & ADX>=22 & pullback to EMA20 (low<=ema20+0.10*ATR) &
    bull rejection (close>open, close>high[1], close>ema20).
  - Entry = STOP-buy on the break of the rejection candle high (momentum fill, not a
    favourable limit). Stop = min(low, 5-bar structure low) - 0.10*ATR.
  - Manage: partial 50% at +1R -> stop to breakeven; final 50% at +2R; exit remainder
    on close<EMA20 or close<10-bar swing low. Blended R per trade.
Realistic fills (stop entry, gap-adjusted), fixed cost, both-OOS-halves gate, per
class. Tested with entry TF = 4H and 1D.

Run: python trend_pullback_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import agg4h, ema, atr, adx, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
TREND_BUF, ENTRY_BUF, STOP_BUF = 0.20, 0.10, 0.10
RSI_MIN, ADX_MIN, MIN_RR = 50, 22, 2.0
SETUP_LB, SWING_LB = 5, 10
TRIG_WIN = 6
HOLD = {'4h': 120, '1d': 45}


def walk_manage(bars, ei, entry, stop, d, ema20, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tp1 = entry + R if d == 'bull' else entry - R
    tp2 = entry + MIN_RR*R if d == 'bull' else entry - MIN_RR*R
    half = False; cur_stop = stop; n = len(bars)
    for j in range(ei, min(ei+hold, n)):
        b = bars[j]
        seg = bars[max(0, j-SWING_LB+1):j+1]
        swing_lo = min(x['l'] for x in seg); swing_hi = max(x['h'] for x in seg)
        if d == 'bull':
            if b['l'] <= cur_stop:
                return -1.0 if not half else 0.5*1.0 + 0.5*((cur_stop-entry)/R)
            if not half and b['h'] >= tp1:
                half = True; cur_stop = entry
            if half and b['h'] >= tp2:
                return 0.5*1.0 + 0.5*MIN_RR
            if ema20[j] is not None and (b['c'] < ema20[j] or b['c'] < swing_lo):
                rem = (b['c']-entry)/R
                return (0.5*1.0 + 0.5*rem) if half else rem
        else:
            if b['h'] >= cur_stop:
                return -1.0 if not half else 0.5*1.0 + 0.5*((entry-cur_stop)/R)
            if not half and b['l'] <= tp1:
                half = True; cur_stop = entry
            if half and b['l'] <= tp2:
                return 0.5*1.0 + 0.5*MIN_RR
            if ema20[j] is not None and (b['c'] > ema20[j] or b['c'] > swing_hi):
                rem = (entry-b['c'])/R
                return (0.5*1.0 + 0.5*rem) if half else rem
    b = bars[min(ei+hold, n)-1]
    rem = (b['c']-entry)/R if d == 'bull' else (entry-b['c'])/R
    return (0.5*1.0 + 0.5*rem) if half else rem


def scan(ebars, daily, tf, store, cls, store_cls):
    ec = [b['c'] for b in ebars]; ema20 = ema(ec, 20); rsi = precompute_rsi(ec, 14)
    dc = [b['c'] for b in daily]; ema200d = ema(dc, 200); dts = [b['_ts'] for b in daily]
    n = len(ebars); last = -1
    for i in range(SETUP_LB+2, n-1):
        if i <= last or ema20[i] is None or rsi[i] is None:
            continue
        a = atr(ebars, 14, i) or 0.0
        if a <= 0:
            continue
        # daily trend as of this entry bar (last completed daily)
        di = bisect.bisect_right(dts, ebars[i]['_ts'] - 86400) - 1
        if di < 0 or ema200d[di] is None:
            continue
        ad_ = atr(daily, 14, di) or 0.0
        trend_up = dc[di] > ema200d[di] + TREND_BUF*ad_
        trend_dn = dc[di] < ema200d[di] - TREND_BUF*ad_
        ax = adx(ebars, 14, i)
        if ax is None or ax < ADX_MIN:
            continue
        b = ebars[i]; sl = min(x['l'] for x in ebars[i-SETUP_LB+1:i+1]); sh = max(x['h'] for x in ebars[i-SETUP_LB+1:i+1])
        d = None
        if (trend_up and rsi[i] > RSI_MIN and b['l'] <= ema20[i] + ENTRY_BUF*a
                and b['c'] > b['o'] and b['c'] > ebars[i-1]['h'] and b['c'] > ema20[i]):
            d = 'bull'; trig = b['h']; stop = min(b['l'], sl) - STOP_BUF*a
        elif (trend_dn and rsi[i] < RSI_MIN and b['h'] >= ema20[i] - ENTRY_BUF*a
                and b['c'] < b['o'] and b['c'] < ebars[i-1]['l'] and b['c'] < ema20[i]):
            d = 'bear'; trig = b['l']; stop = max(b['h'], sh) + STOP_BUF*a
        if d is None:
            continue
        # stop-order entry on the break of the rejection candle
        ei = None; entry = None
        for j in range(i+1, min(i+1+TRIG_WIN, n)):
            if d == 'bull' and ebars[j]['h'] >= trig:
                ei = j; entry = max(trig, ebars[j]['o']); break
            if d == 'bear' and ebars[j]['l'] <= trig:
                ei = j; entry = min(trig, ebars[j]['o']); break
            if (d == 'bull' and ebars[j]['c'] < ema20[j]) or (d == 'bear' and ebars[j]['c'] > ema20[j]):
                break
        if ei is None:
            continue
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        o = walk_manage(ebars, ei, entry, stop, d, ema20, HOLD[tf])
        if o is not None:
            R = abs(entry-stop); net = o - cost(o, entry, R)
            store[tf].append((ebars[ei]['_ts'], net)); store_cls[cls][tf].append((ebars[ei]['_ts'], net))
        last = ei + 2


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<12} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 220:
            continue
        npairs += 1
        scan(agg4h(h1), daily, '4h', store, cls, store_cls)
        scan(daily, daily, '1d', store, cls, store_cls)

    print(f"Trend-pullback (D trend + EMA20 pullback + RSI/ADX + rejection, 2R + partials) — {npairs} pairs\n")
    for tf in ('4h', '1d'):
        line(f"{tf} ALL", store[tf])
    print("\n=== per class (4H) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c]['4h'])
    print("=== per class (1D) ===")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c]['1d'])


if __name__ == '__main__':
    main()
