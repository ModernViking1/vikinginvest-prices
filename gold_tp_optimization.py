"""Gold take-profit / exit-management study — does a tighter TP (or profit-locking) help?

Motivated by two XAUUSD longs that ran to large open profit then reversed to a loss. Tests
the wired gold signals (gbreak h1, gtrend 4h, asianglitch h1) under different exits:
  - fixed RR sweep {0.5, 1.0, 1.5, 2.0, 3.0}
  - breakeven-after-+1R, then let it run to RR3 (a winner can no longer become a full loss)
  - ATR trailing stop (trail k*ATR from the running peak)
Reports expectancy per exit, plus GIVEBACK% = share of trades that reached >=+2R at some
point but closed <= 0 (the '+9K -> -2K' outcome). Realistic: R = |entry-stop|, dealing cost.

Run: python gold_tp_optimization.py
"""
import json, os, bisect
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import agg4h, atr, detect_gbreak, detect_gtrend, detect_asianglitch, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
GOLD = 'xauusd'
HOLD = 120
RRS = [0.5, 1.0, 1.5, 2.0, 3.0]


def _mfe_and(bars, ts, entry, stop, d):
    """Max favourable excursion (in R) reached over the hold, for the giveback stat."""
    i0 = bisect.bisect_left([b['_ts'] for b in bars], ts); R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return 0.0
    mfe = 0.0
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        fav = (b['h'] - entry) if d == 'bull' else (entry - b['l'])
        mfe = max(mfe, fav / R)
        adv = (b['l'] <= stop) if d == 'bull' else (b['h'] >= stop)
        if adv:
            break
    return mfe


def sc_fixed(bars, ts, entry, stop, d, rr):
    i0 = bisect.bisect_left([b['_ts'] for b in bars], ts); R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
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


def sc_be(bars, ts, entry, stop, d, tgt_rr, be_at=1.0):
    i0 = bisect.bisect_left([b['_ts'] for b in bars], ts); R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return None
    tgt = entry + tgt_rr * R if d == 'bull' else entry - tgt_rr * R
    cur = stop; be = False
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if not be:
            reached = (b['h'] >= entry + be_at * R) if d == 'bull' else (b['l'] <= entry - be_at * R)
            if reached:
                be = True; cur = entry
        if d == 'bull':
            if b['l'] <= cur: return (cur - entry) / R
            if b['h'] >= tgt: return tgt_rr
        else:
            if b['h'] >= cur: return (entry - cur) / R
            if b['l'] <= tgt: return tgt_rr
    return None


def sc_trail(bars, ts, entry, stop, d, a0, k=2.0):
    i0 = bisect.bisect_left([b['_ts'] for b in bars], ts); R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars) or a0 <= 0:
        return None
    trail = stop; peak = entry
    for j in range(i0, min(i0 + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            peak = max(peak, b['h']); trail = max(trail, peak - k * a0)
            if b['l'] <= trail: return (trail - entry) / R
        else:
            peak = min(peak, b['l']); trail = min(trail, peak + k * a0)
            if b['h'] >= trail: return (entry - trail) / R
    return None


def agg(seq):
    seq = [x for x in seq if x is not None]; n = len(seq)
    return n, (100 * sum(1 for x in seq if x > 0) / n if n else 0), (sum(seq) / n if n else 0)


def report(name, sigs, bars, atr_lookup):
    print(f"\n### {name} — {len(sigs)} signals ###")
    # fixed RR sweep
    for rr in RRS:
        outs = []
        for s in sigs:
            o = sc_fixed(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'], rr)
            if o is not None:
                outs.append(o - cost(o, s['entry'], abs(s['entry'] - s['stop'])))
        n, w, e = agg(outs)
        print(f"  fixed RR{rr:<4} n={n:>3} WR={w:>5.1f}% exp={e:>+6.3f}R  (breakeven WR {100/(1+rr):.0f}%)")
    # scaled exit: 1/3 at 1R, 1/3 at 2R, 1/3 at 3R (shared original stop) — banks early
    outs = []
    for s in sigs:
        parts = [sc_fixed(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'], rr) for rr in (1.0, 2.0, 3.0)]
        if all(p is not None for p in parts):
            R = abs(s['entry'] - s['stop'])
            outs.append(sum(p - cost(p, s['entry'], R) for p in parts) / 3)
    n, w, e = agg(outs)
    print(f"  SCALED 1/2/3 n={n:>3} WR={w:>5.1f}% exp={e:>+6.3f}R  (banks 1/3 at +1R, 1/3 at +2R, runs 1/3 to +3R)")
    # breakeven-after-1R, run to RR3
    outs = []
    for s in sigs:
        o = sc_be(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'], 3.0)
        if o is not None:
            outs.append(o - cost(o, s['entry'], abs(s['entry'] - s['stop'])))
    n, w, e = agg(outs)
    print(f"  BE@1R->RR3  n={n:>3} WR={w:>5.1f}% exp={e:>+6.3f}R")
    # ATR trail
    outs = []
    for s in sigs:
        a0 = atr_lookup(s['entry_ts'])
        o = sc_trail(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'], a0, 2.0)
        if o is not None:
            outs.append(o - cost(o, s['entry'], abs(s['entry'] - s['stop'])))
    n, w, e = agg(outs)
    print(f"  ATR-trail2  n={n:>3} WR={w:>5.1f}% exp={e:>+6.3f}R")
    # giveback stat (path-based, independent of exit)
    gb = tot = 0
    for s in sigs:
        mfe = _mfe_and(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'])
        final = sc_fixed(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'], 3.0)  # current live-ish far target
        if final is not None:
            tot += 1
            if mfe >= 2.0 and final <= 0:
                gb += 1
    if tot:
        print(f"  GIVEBACK: {gb}/{tot} ({100*gb/tot:.0f}%) reached >=+2R then closed <=0 at the far (RR3) target")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    h1 = _bars_norm(pairs[GOLD].get('h1', [])); daily = _bars_norm(pairs[GOLD].get('daily', []))
    b4 = agg4h(h1)
    h1_ts = [b['_ts'] for b in h1]; b4_ts = [b['_ts'] for b in b4]

    def atr_h1(ts):
        i = bisect.bisect_left(h1_ts, ts) - 1; return atr(h1, 14, i) or 0.0 if i >= 14 else 0.0

    def atr_4h(ts):
        i = bisect.bisect_left(b4_ts, ts) - 1; return atr(b4, 14, i) or 0.0 if i >= 14 else 0.0

    print("=" * 84)
    print("GOLD exit-management study — tighter TP vs breakeven / trailing (XAUUSD)")
    print("=" * 84)
    report("gbreak (breakout, h1)", detect_gbreak(GOLD, h1, daily), h1, atr_h1)
    report("gtrend (trend pullback, 4h)", detect_gtrend(GOLD, h1, daily), b4, atr_4h)
    report("asianglitch (session sweep, h1, wired RR3)", detect_asianglitch(GOLD, h1, daily), h1, atr_h1)


if __name__ == '__main__':
    main()
