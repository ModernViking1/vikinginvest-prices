"""RSI/MACD mean-reversion strategy (user-proposed, IMG_6672/6673).

  SHORT: RSI(14) > 70  AND  MACD line crosses DOWN through signal  -> short
  LONG : RSI(14) < 30  AND  MACD line crosses UP   through signal  -> long
  Both conditions on the same (signal) bar. Entry = next bar open (realistic).
  STOP  : beyond the recent swing extreme (above the high / below the low) + ATR buf.
  EXIT  : on the CLOSE of the first bar where RSI returns to 50 (mean reversion).
          If price hits the stop first -> -1R. If RSI never reaches 50 within the
          hold -> exit at the hold-end close (time stop).

This is NOT a fixed-RR strategy, so outcomes are continuous R-multiples (small
mean-reversion wins vs -1R stops). Reported: WR (r>0), mean expectancy in R,
chronological OOS split, per class, on daily / 4h / 1h. Realistic fixed cost.

Run: python rsi_macd_meanrev_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS, macd_series
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import agg4h, atr, cost, agg

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
SWING = 5
ATR_BUF = 0.25
COOLDOWN = 3
MAXHOLD = {'daily': 20, '4h': 30, 'h1': 48}


def walk_meanrev(bars, rsi, ei, entry, stop, d, maxhold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    end = min(ei + maxhold, len(bars))
    for j in range(ei, end):
        b = bars[j]
        if d == 'short':
            if b['h'] >= stop:
                return -1.0
            if rsi[j] is not None and rsi[j] <= 50:
                return (entry - b['c']) / R
        else:
            if b['l'] <= stop:
                return -1.0
            if rsi[j] is not None and rsi[j] >= 50:
                return (b['c'] - entry) / R
    if end > ei:
        b = bars[end - 1]
        return (entry - b['c']) / R if d == 'short' else (b['c'] - entry) / R
    return None


def scan(bars, tf, rsi_win, store, cls, store_cls):
    closes = [b['c'] for b in bars]
    rsi = precompute_rsi(closes, 14)
    macd, sig = macd_series(closes, 12, 26, 9)
    n = len(bars); last = -1; hold = MAXHOLD[tf]
    for i in range(30, n - 1):
        if i <= last:
            continue
        if rsi[i] is None or None in (macd[i], macd[i-1], sig[i], sig[i-1]):
            continue
        cross_dn = macd[i-1] >= sig[i-1] and macd[i] < sig[i]
        cross_up = macd[i-1] <= sig[i-1] and macd[i] > sig[i]
        win = [rsi[k] for k in range(max(0, i-rsi_win+1), i+1) if rsi[k] is not None]
        ob = any(v > 70 for v in win); os_ = any(v < 30 for v in win)
        d = None
        if ob and cross_dn:
            d = 'short'
        elif os_ and cross_up:
            d = 'long'
        if d is None:
            continue
        ei = i + 1; entry = bars[ei]['o']; a = atr(bars, 14, i) or 0.0
        if d == 'short':
            stop = max(b['h'] for b in bars[max(0, i-SWING):i+1]) + ATR_BUF * a
            if stop <= entry:
                continue
        else:
            stop = min(b['l'] for b in bars[max(0, i-SWING):i+1]) - ATR_BUF * a
            if stop >= entry:
                continue
        r = walk_meanrev(bars, rsi, ei, entry, stop, d, hold)
        if r is None:
            continue
        net = r - cost(r, entry, abs(entry - stop))
        ts = bars[ei]['_ts']
        store[tf].append((ts, net)); store_cls[cls][tf].append((ts, net))
        last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<14} n={n:>5} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    print(f"RSI(>70/<30)+MACD-cross mean-reversion, exit on RSI-50 close — realistic cost, OOS\n")
    best = None
    for rsi_win in (1, 3, 5):
        store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
        for pk in [x for x in PAIR_CLASS if x in pairs]:
            cls = PAIR_CLASS.get(pk)
            h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
            if len(h1) < 400 or len(daily) < 80:
                continue
            npairs += 1
            for tf, bars in {'daily': daily, '4h': agg4h(h1), 'h1': h1}.items():
                if len(bars) < 120:
                    continue
                scan(bars, tf, rsi_win, store, cls, store_cls)
        print(f"=== RSI-overbought within last {rsi_win} bar(s) of the MACD cross ({npairs} pairs) ===")
        for tf in ('daily', '4h', 'h1'):
            line(tf, store[tf])
        if rsi_win == 3:
            best = store_cls
        print()
    print("Per-class (RSI window = 3):")
    for tf in ('daily', '4h', 'h1'):
        print(f"  --- {tf} ---")
        for c in ['comm', 'crypto', 'index', 'major', 'minor']:
            line(c, best[c][tf])


if __name__ == '__main__':
    main()
