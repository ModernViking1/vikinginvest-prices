"""Real-delta BTC research — absorption / delta-flip / VWAP / ORB on Binance data.

Uses binance-btc-ohlcv.json (fetch_binance_btc.py), whose bars carry REAL aggressor
delta (2*taker_buy - volume). This is the data the Whale Pivot Model actually needs, so
unlike volume_footprint_research.py (candle-direction proxy) these tests are faithful for
the delta-based ideas:

  absorption   strong one-sided delta that price FAILS to follow (opposite side absorbs)
               -> fade. Net buying but the bar closes down = buyers absorbed -> short.
  delta_flip   running delta over a window flips sign and price confirms -> go with it.
  vwap_revert  price stretched k*ATR from the rolling volume-weighted price -> revert.
  orb          opening-range (first hour, UTC) breakout, delta-confirmed.

BTC only (that's the Binance feed), on the native interval and resampled up. Market
fills, dealing cost, bracket-honest, chronological OOS (both halves + and n>=40 = PASS).

Run: python crypto_delta_research.py
"""
import json
import os
import datetime as dt
from collections import defaultdict

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "binance-btc-ohlcv.json")
RRS = [1.5, 2.0]
HOLD = 160
BUF = 0.10


def resample(bars, factor):
    out = []
    for i in range(0, len(bars) - factor + 1, factor):
        g = bars[i:i + factor]
        out.append({'_ts': g[0]['_ts'], 'o': g[0]['o'], 'h': max(x['h'] for x in g),
                    'l': min(x['l'] for x in g), 'c': g[-1]['c'],
                    'v': sum(x['v'] for x in g), 'delta': sum(x.get('delta', 0) for x in g)})
    return out


def _avg_abs_delta(bars, i, lb=20):
    s = sum(abs(bars[j].get('delta', 0)) for j in range(max(0, i - lb), i))
    return s / max(1, min(lb, i))


def absorption_signals(bars, k=1.5):
    out = []; n = len(bars); last = -1
    for i in range(25, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        b = bars[i]; d = b.get('delta', 0); avg = _avg_abs_delta(bars, i)
        if avg <= 0:
            continue
        if d >= k * avg and b['c'] < b['o']:        # aggressive BUYING absorbed (closed down) -> short
            entry = b['c']; stop = b['h'] + BUF * a
            if stop > entry: out.append((i + 1, entry, stop, 'bear')); last = i + 3
        elif d <= -k * avg and b['c'] > b['o']:     # aggressive SELLING absorbed (closed up) -> long
            entry = b['c']; stop = b['l'] - BUF * a
            if stop < entry: out.append((i + 1, entry, stop, 'bull')); last = i + 3
    return out


def deltaflip_signals(bars, look=12):
    out = []; n = len(bars); last = -1
    cum = [0.0] * n
    run = 0.0
    for i in range(n):
        run += bars[i].get('delta', 0)
        cum[i] = run
    for i in range(look + 15, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        win = cum[i] - cum[i - look]; prev = cum[i - 1] - cum[i - 1 - look]
        b = bars[i]
        if prev <= 0 and win > 0 and b['c'] > b['o']:      # delta flipped UP + price confirms -> long
            entry = b['c']; stop = min(x['l'] for x in bars[i - 3:i + 1]) - BUF * a
            if stop < entry: out.append((i + 1, entry, stop, 'bull')); last = i + 3
        elif prev >= 0 and win < 0 and b['c'] < b['o']:    # flipped DOWN -> short
            entry = b['c']; stop = max(x['h'] for x in bars[i - 3:i + 1]) + BUF * a
            if stop > entry: out.append((i + 1, entry, stop, 'bear')); last = i + 3
    return out


def vwap_revert_signals(bars, look=48, k=2.0):
    out = []; n = len(bars); last = -1
    for i in range(look + 15, n - 1):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        seg = bars[i - look:i]
        vol = sum(x['v'] for x in seg)
        if vol <= 0:
            continue
        vwap = sum(((x['h'] + x['l'] + x['c']) / 3) * x['v'] for x in seg) / vol
        c = bars[i]['c']
        if c >= vwap + k * a:                       # stretched above VWAP -> short back toward it
            entry = c; stop = c + a
            out.append((i + 1, entry, stop, 'bear')); last = i + 3
        elif c <= vwap - k * a:
            entry = c; stop = c - a
            out.append((i + 1, entry, stop, 'bull')); last = i + 3
    return out


def orb_signals(bars, open_hour=0, rng_len=12, win=48):
    out = []
    by_day = defaultdict(list)
    for i, b in enumerate(bars):
        if dt.datetime.utcfromtimestamp(b['_ts']).hour == open_hour:
            by_day[dt.datetime.utcfromtimestamp(b['_ts']).date()].append(i)
    for day, idxs in sorted(by_day.items()):
        if not idxs:
            continue
        i0 = idxs[0]; rng = bars[i0:i0 + rng_len]
        if len(rng) < rng_len:
            continue
        rhi = max(x['h'] for x in rng); rlo = min(x['l'] for x in rng)
        e_idx = i0 + rng_len
        a = atr(bars, 14, e_idx) if e_idx < len(bars) else None
        if not a or a <= 0 or rhi <= rlo:
            continue
        for j in range(e_idx, min(e_idx + win, len(bars) - 1)):
            b = bars[j]; d = b.get('delta', 0)
            if b['c'] > rhi and d > 0:              # break up, delta confirms
                entry = b['c']; stop = rlo - BUF * a
                if stop < entry: out.append((j + 1, entry, stop, 'bull')); break
            if b['c'] < rlo and d < 0:
                entry = b['c']; stop = rhi + BUF * a
                if stop > entry: out.append((j + 1, entry, stop, 'bear')); break
    return out


def walk(bars, i0, entry, stop, dr, rr):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if dr == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if dr == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, wr, e = agg(seq); m = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:m]]); _, _, es = agg([r for _, r in rows[m:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<10} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(bars, tf, name, fn):
    print(f"\n===== {name} · {tf} =====")
    store = defaultdict(list)
    for (ei, entry, stop, dr) in fn(bars):
        if ei >= len(bars):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        for rr in RRS:
            o = walk(bars, ei, entry, stop, dr, rr)
            if o is not None:
                store[rr].append((ts, o - cost(o, entry, R)))
    for rr in RRS:
        line(f'RR {rr}', store[rr])


def main():
    if not os.path.exists(SRC):
        print("no binance-btc-ohlcv.json — run the binance-btc-fetch workflow first"); return
    d = json.load(open(SRC))
    base = _bars_norm(d['pairs']['btcusd'][d['interval']])
    tfs = {d['interval']: base, '15m': resample(base, 3), '1h': resample(base, 12)}
    print("=" * 92)
    print(f"BTC real-delta research · {d['interval']} native ({len(base)} bars) + resampled · OOS")
    print("=" * 92)
    for tf, bars in tfs.items():
        if len(bars) < 400:
            continue
        run(bars, tf, "ABSORPTION (delta-fade)", absorption_signals)
        run(bars, tf, "DELTA-FLIP (go-with)", deltaflip_signals)
        run(bars, tf, "VWAP mean-revert", vwap_revert_signals)
        run(bars, tf, "ORB + delta confirm", orb_signals)


if __name__ == '__main__':
    main()
