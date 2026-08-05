"""Does Julien's short-timeframe gold breakout have a durable edge? (m15 vs h1)

Julien (XAU XAG Private) posted an M5 XAUUSD BUY: entry 4038, SL 4023 (15pt), scaled TPs at
4048/4058/4068 = 0.67R / 1.33R / 2R across 3 positions. A momentum/continuation breakout long
on a very short timeframe. We have no M5 data, so this tests the closest we hold — m15 — using
our OWN validated gbreak logic (close beyond the recent Donchian range WITH expanding ATR;
market entry at the break-bar close; stop 1 ATR back), and runs the SAME logic on h1 as the
control (h1 gbreak is live and robust). If Julien's timeframe carried a durable edge, the m15
cell should clear cost like h1 does.

Two exits: single RR2 (comparable to live gbreak) and Julien's scaled 0.67/1.33/2R (1/3 each,
shared stop). Realistic MARKET fills, fixed dealing cost, chronological OOS split (both halves
positive + n>=40 = PASS). Gold + silver + platinum (the commodity corroboration gbreak used).

Run: python gold_m5_breakout_research.py
"""
import json, os
from collections import defaultdict
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
LOOK = 48          # Donchian lookback (bars) — same bar-count as live gbreak
ATRLB = 10         # ATR-expansion comparison lookback (bars)
COOLDOWN = 4
PAIRS = ['xauusd', 'xagusd', 'xptusd']
SCALE = [(1/3, 0.67), (1/3, 1.33), (1/3, 2.0)]   # Julien's TP1/TP2/TP3 as R-multiples


def donch(bars, i, look):
    if i < look:
        return None
    seg = bars[i - look:i]
    return (min(x['l'] for x in seg), max(x['h'] for x in seg))


def signals(bars):
    out = []; n = len(bars); last = -1
    for i in range(LOOK + 2, n - 1):
        if i <= last:
            continue
        band = donch(bars, i, LOOK)
        if not band:
            continue
        lo, hi = band; a = atr(bars, 14, i); ap = atr(bars, 14, i - ATRLB)
        if a is None or ap is None or a <= 0 or a <= ap:      # expanding volatility (gbreak filter)
            continue
        b = bars[i]
        if b['c'] > hi:
            entry = b['c']; stop = hi - 1.0 * a
            if stop < entry:
                out.append((i + 1, entry, stop, 'bull')); last = i + COOLDOWN
        elif b['c'] < lo:
            entry = b['c']; stop = lo + 1.0 * a
            if stop > entry:
                out.append((i + 1, entry, stop, 'bear')); last = i + COOLDOWN
    return out


def walk_single(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
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


def walk_scaled(bars, i0, entry, stop, d, hold):
    """Julien's 3 scaled legs, shared stop, first-touch. Returns net R (sum of leg fracs)."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    def px(m): return entry + m * R if d == 'bull' else entry - m * R
    legs = list(SCALE); banked = 0.0; frac_left = sum(f for f, _ in legs)
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        hit_stop = (b['l'] <= px(-1)) if d == 'bull' else (b['h'] >= px(-1))
        if hit_stop:
            return banked + frac_left * (-1.0)
        prog = True
        while legs and prog:
            prog = False
            f, tR = legs[0]; tp = px(tR)
            hit = (b['h'] >= tp) if d == 'bull' else (b['l'] <= tp)
            if hit:
                banked += f * tR; frac_left -= f; legs.pop(0); prog = True
        if not legs:
            return banked
    last = bars[min(i0 + hold, len(bars) - 1)]['c']
    mtm = (last - entry) / R if d == 'bull' else (entry - last) / R
    return banked + frac_left * max(mtm, -1.0)


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<22} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(tf):
    d = json.load(open(HIST))['pairs']
    hold = 192 if tf == 'm15' else 80
    single = defaultdict(list); scaled = defaultdict(list)
    for pk in PAIRS:
        bars = _bars_norm(d.get(pk, {}).get(tf, []))
        if len(bars) < LOOK + 200:
            continue
        for (ei, entry, stop, dr) in signals(bars):
            if ei >= len(bars):
                continue
            o = walk_single(bars, ei, entry, stop, dr, 2.0, hold)
            if o is not None:
                single[pk].append((bars[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
            s = walk_scaled(bars, ei, entry, stop, dr, hold)
            if s is not None:
                scaled[pk].append((bars[ei]['_ts'], s - cost(1.0 if s > 0 else -1.0, entry, abs(entry - stop))))
    print(f"\n===== gold breakout (gbreak logic) · {tf} · MARKET fills =====")
    print("  -- single RR2 (comparable to live gbreak) --")
    for pk in PAIRS:
        if single[pk]: line(pk, single[pk])
    line('ALL', [r for pk in PAIRS for r in single[pk]])
    print("  -- Julien scaled exit (0.67 / 1.33 / 2R, 1/3 each) --")
    for pk in PAIRS:
        if scaled[pk]: line(pk, scaled[pk])
    line('ALL', [r for pk in PAIRS for r in scaled[pk]])


def main():
    print("=" * 96)
    print("Julien's short-TF gold breakout — durable edge? m15 (his TF proxy) vs h1 (validated control)")
    print("=" * 96)
    run('m15')
    run('h1')


if __name__ == '__main__':
    main()
