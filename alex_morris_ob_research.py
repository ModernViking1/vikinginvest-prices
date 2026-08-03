"""Alex Morris 'winning setups' — faithful test of HIS exact method.

The screenshots he posted are labelled "4HR Bullish OB" — i.e. ORDER BLOCKS: an
impulse move, take the last opposite-colour candle before it as the zone, wait for a
retest, ride the continuation to a measured (often extended) target. That is EXACTLY the
logic already wired live as `obfvg` (_obfvg_signals) — except his charts are 4H, mine ran
h1, and his targets run further than 2:1 (the dashed diagonal = a runner).

So this reuses the harness's real OB detector unchanged and tests his precise variant:
  - zone TF = 4h (his label) and h1 (my live cell) for reference
  - MARKET fill on the retest bar (no favourable limit)
  - RR 2 / 3 / 5 (the last = the 'let it run' target his charts show)
  - FX minor+major (the pairs in the screenshots: GBPAUD/GBPCHF/EURJPY/AUDUSD/EURUSD)
  - chronological OOS split; both halves positive + n>=40 = PASS

IMPORTANT framing: the screenshots are SELECTED WINNERS. A highlight reel has no losers
and no denominator, so it cannot move a base rate by itself. What it legitimately refines
is the METHOD (4H order blocks + runner target), so we test THAT honestly here.

Run: python alex_morris_ob_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import _obfvg_signals, agg4h, atr, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RRS = [2.0, 3.0, 5.0]
HOLD = 160          # entry-TF bars allowed to reach target


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def bracket(bars, entry_ts, entry, stop, d, rr):
    """MARKET-fill bracket from the retest bar: first-touch stop/target, realistic cost."""
    ts = [b['_ts'] for b in bars]; s = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or s >= len(bars):
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(s, min(s + HOLD, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0 - cost(-1.0, entry, R)
            if b['h'] >= tgt: return rr - cost(rr, entry, R)
        else:
            if b['h'] >= stop: return -1.0 - cost(-1.0, entry, R)
            if b['l'] <= tgt: return rr - cost(rr, entry, R)
    return None


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<8} RR{rr:g} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(zone_tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 500:
            continue
        bars = agg4h(h1) if zone_tf == '4h' else h1
        if len(bars) < 300:
            continue
        npr += 1
        for sig in _obfvg_signals(pk, bars, 'ob', zone_tf):
            for rr in RRS:
                o = bracket(bars, sig['entry_ts'], sig['entry'], sig['stop'], sig['dir'], rr)
                if o is not None:
                    store[cls][rr].append((sig['entry_ts'], o))
    print(f"\n===== {zone_tf} order-block retest (Alex Morris method, MARKET fills) — {npr} pairs =====")
    for c in ['minor', 'major', 'comm', 'crypto', 'index']:
        for rr in RRS:
            line(c, store[c][rr], rr)
    for rr in RRS:
        line('FX(m+M)', store['minor'][rr] + store['major'][rr], rr)


def main():
    print("=" * 94)
    print("Alex Morris 4H order-block setups — faithful systematic test, market fills, FX focus")
    print("=" * 94)
    run('4h')
    run('h1')


if __name__ == '__main__':
    main()
