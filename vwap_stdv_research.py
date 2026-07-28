"""'VWAP + LQ sweep + STDV' mean-reversion (90%-win-rate claim, gold-desk screenshots).

Setup: anchored VWAP with standard-deviation bands (the marked ones are x2 and x3).
Price sweeps a liquidity level = a spike beyond the 2σ or 3σ band, then reverts to the
mean (VWAP). Fade the sweep back to the mean: SHORT a sweep above the upper band that
closes back inside; LONG a sweep below the lower band that closes back inside. Target =
the mean; stop just beyond the sweep extreme. High RR because the band is far from the
mean (screenshots show RR 3.0 and 5.7). Marketed ~90% win rate.

DATA NOTE: our OHLC has no volume, so 'VWAP' here is an equal-weighted rolling mean of
hlc3 (== a Bollinger mid-line) with σ bands — a faithful proxy for FX/index CFDs, which
carry no centralised volume anyway. The tested mechanic (sweep the band, revert to mean)
is unchanged.

Bands from the trailing N bars ending BEFORE the signal bar (no look-ahead). Target =
the fixed mean at entry (the screenshots' targets are fixed horizontal lines). Realistic
fills, dealing cost, per-trade RR (target=mean), chronological OOS split, per class.

Run: python vwap_stdv_research.py
"""
import json, os, math
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
BUF = 0.20          # stop buffer beyond the sweep extreme, in σ
COOLDOWN = 3
HOLD = 60           # bars to revert to the mean


def _prefix(bars):
    n = len(bars); S1 = [0.0] * (n + 1); S2 = [0.0] * (n + 1)
    tp = [(b['h'] + b['l'] + b['c']) / 3 for b in bars]
    for i in range(n):
        S1[i + 1] = S1[i] + tp[i]; S2[i + 1] = S2[i] + tp[i] * tp[i]
    return S1, S2


def band(S1, S2, i, N):
    """mean, sigma over the N bars ending at i-1 (excludes bar i)."""
    if i < N:
        return None
    s1 = S1[i] - S1[i - N]; s2 = S2[i] - S2[i - N]
    mean = s1 / N; var = s2 / N - mean * mean
    return (mean, math.sqrt(var)) if var > 0 else None


def walk(bars, i0, entry, stop, target, d):
    R = abs(entry - stop)
    if R <= 0:
        return None
    rr = abs(target - entry) / R
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return (-1.0, rr)
            if b['h'] >= target: return (rr, rr)
        else:
            if b['h'] >= stop: return (-1.0, rr)
            if b['l'] <= target: return (rr, rr)
    return None


def scan(bars, N, k, store_cls, cls, store_pair, pk):
    S1, S2 = _prefix(bars); n = len(bars); last = -1
    for i in range(N, n - 1):
        if i <= last:
            continue
        bd = band(S1, S2, i, N)
        if not bd:
            continue
        mean, sig = bd
        if sig <= 0:
            continue
        up = mean + k * sig; lo = mean - k * sig; b = bars[i]
        d = None
        if b['h'] > up and b['c'] < up:
            d = 'bear'; entry = b['c']; stop = b['h'] + BUF * sig; target = mean
        elif b['l'] < lo and b['c'] > lo:
            d = 'bull'; entry = b['c']; stop = b['l'] - BUF * sig; target = mean
        if not d:
            continue
        if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
            continue
        res = walk(bars, i + 1, entry, stop, target, d)
        if res is not None:
            o, rr = res; net = o - cost(o, entry, abs(entry - stop))
            store_cls[cls].append((bars[i + 1]['_ts'], net, rr))
            store_pair[pk].append((bars[i + 1]['_ts'], net, rr))
            last = i + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r, _ in rows]; rrs = [rr for _, _, rr in rows]
    n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r, _ in rows[:mid]]); _, _, es = agg([r for _, r, _ in rows[mid:]])
    medrr = sorted(rrs)[len(rrs) // 2] if rrs else 0
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<12} n={n:>4} WR={w:>5.1f}% medRR={medrr:>4.1f} exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def run(tf, N, k):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    sc = defaultdict(list); sp = defaultdict(list); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        if tf == '4h':
            h1 = _bars_norm(pairs[pk].get('h1', []))
            if len(h1) < 300:
                continue
            bars = make_4h(h1)
        else:
            bars = _bars_norm(pairs[pk].get(tf, []))
        if len(bars) < 300:
            continue
        npr += 1
        scan(bars, N, k, sc, cls, sp, pk)
    print(f"\n===== {tf}  VWAP±{k}σ (N={N}) — {npr} pairs =====")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, sc[c])
    line('ALL', [r for c in sc for r in sc[c]])
    return sp


def main():
    print("=" * 92)
    print("VWAP + STDV band mean-reversion — fade the 2σ / 3σ sweep back to the mean")
    print("=" * 92)
    for tf in ('m15', 'h1', '4h'):
        for k in (2, 3):
            run(tf, 30, k)


if __name__ == '__main__':
    main()
