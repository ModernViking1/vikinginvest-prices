"""Crypto FOOTPRINT backtest — does REAL delta (from Binance aggTrades) carry a signal?

Reads crypto-footprint.json (m15 bars with true delta = aggressive-buy - aggressive-sell)
and tests the order-flow ideas from the Whale-Pivot clips that a delta PROXY couldn't:
  A  cumulative-delta FLIP (momentum): the running delta over a window crosses zero ->
     trade the new side (buyers/sellers taking control).
  B  delta divergence / ABSORPTION: a new high made on NEGATIVE delta (buyers can't push;
     sellers absorbing) -> reversal short. Mirror for a new low on positive delta.

Market fills, dealing cost, bracket-honest, chronological OOS (both halves + / n>=40 =
PASS). Single symbol (BTC) — a proof-of-concept before paying for futures order flow.

Run: python crypto_footprint_research.py   (needs crypto-footprint.json from CI)
"""
import json
import os

from five_strategies_research import agg, cost

FP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crypto-footprint.json")
RRS = [1.5, 2.0]
HOLD = 96


def _atr(bars, n, i):
    if i < n:
        return None
    s = 0.0
    for j in range(i - n + 1, i + 1):
        s += max(bars[j]['h'] - bars[j]['l'], abs(bars[j]['h'] - bars[j - 1]['c']),
                 abs(bars[j]['l'] - bars[j - 1]['c']))
    return s / n


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


def sig_delta_flip(bars, W=8):
    """Cumulative-delta flip over W bars -> trade the new side."""
    out = []; n = len(bars); last = -1
    cum = [0.0] * n
    for i in range(1, n):
        cum[i] = cum[i - 1] + bars[i]['delta']
    for i in range(W + 15, n - 1):
        if i <= last:
            continue
        a = _atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        d_now = cum[i] - cum[i - W]; d_prev = cum[i - 1] - cum[i - 1 - W]
        if d_prev <= 0 and d_now > 0:
            out.append((i + 1, bars[i]['c'], bars[i]['c'] - a, 'bull')); last = i + 3
        elif d_prev >= 0 and d_now < 0:
            out.append((i + 1, bars[i]['c'], bars[i]['c'] + a, 'bear')); last = i + 3
    return out


def sig_absorption(bars, LOOK=20):
    """New extreme on opposing delta = absorption -> reversal."""
    out = []; n = len(bars); last = -1
    for i in range(LOOK + 15, n - 1):
        if i <= last:
            continue
        a = _atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        hh = max(x['h'] for x in bars[i - LOOK:i]); ll = min(x['l'] for x in bars[i - LOOK:i])
        b = bars[i]
        if b['h'] >= hh and b['delta'] < 0 and b['c'] < b['o']:          # new high, sellers absorbing
            entry = b['c']; stop = b['h'] + 0.1 * a
            if stop > entry:
                out.append((i + 1, entry, stop, 'bear')); last = i + 3
        elif b['l'] <= ll and b['delta'] > 0 and b['c'] > b['o']:        # new low, buyers absorbing
            entry = b['c']; stop = b['l'] - 0.1 * a
            if stop < entry:
                out.append((i + 1, entry, stop, 'bull')); last = i + 3
    return out


def run(name, fn, bars):
    store = {rr: [] for rr in RRS}
    sigs = fn(bars)
    for (ei, entry, stop, dr) in sigs:
        if ei >= len(bars):
            continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        for rr in RRS:
            o = walk(bars, ei, entry, stop, dr, rr)
            if o is not None:
                store[rr].append((ts, o - cost(o, entry, R)))
    print(f"\n===== {name} — {len(sigs)} setups =====")
    for rr in RRS:
        r = sorted(store[rr]); seq = [x for _, x in r]; n, wr, e = agg(seq); m = len(r) // 2
        _, _, eh = agg([x for _, x in r[:m]]); _, _, es = agg([x for _, x in r[m:]])
        v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
        print(f"      RR {rr:<4} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    if not os.path.exists(FP):
        print("no crypto-footprint.json — run the crypto-footprint workflow (fetch) first")
        return
    d = json.load(open(FP)); bars = d['bars']
    for b in bars:
        b['_ts'] = b['t'] / 1000.0
    print("=" * 84)
    print(f"CRYPTO FOOTPRINT pilot · {d.get('symbol', '?')} · {len(bars)} m15 bars · REAL delta")
    print("=" * 84)
    run("A · cumulative-delta flip (momentum)", sig_delta_flip, bars)
    run("B · delta divergence / absorption reversal", sig_absorption, bars)


if __name__ == "__main__":
    main()
