"""Deepen the one edge that survived: H&S fired AGAINST a high-confidence macro-EW
read. Three rigor upgrades before this could ever be considered for deployment:

  1. SWAP / overnight financing cost for multi-day holds (the one cost the swing
     research hadn't modelled) — with a sensitivity sweep.
  2. ROLLING WALK-FORWARD (K sequential folds) instead of a single split — a real
     edge should be positive in most folds, not just 'first vs second half'.
  3. Days-held distribution, so we know the actual holding period + swap exposure.

Reuses the H&S detector + auto-EW macro read from hs_swing_research.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import (
    PAIR_CLASS, auto_detect_ew, AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm, precompute_break_dirs
from hs_swing_research import scan, MAX_HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
WIN_COST_PCT = 0.0045 / 100
LOSS_COST_PCT = 0.0105 / 100
SWAP_BPS_DAY_SWEEP = [0.0, 0.5, 1.0, 2.0]   # bps of price per calendar day held
BARS_PER_DAY = 24
KFOLDS = 6
TARGETS = [1.0, 2.0, 'mm']


def resolve(h1, tr, target):
    """Return (gross_R, bars_held) or (None, None) if unresolved."""
    d = 'bear' if tr['kind'] == 'bear' else 'bull'
    entry, stop, R = tr['entry'], tr['stop'], tr['R']
    rr = abs(target - entry) / R if R else 0
    i0 = tr['entry_idx']
    for j in range(i0, min(i0 + MAX_HOLD, len(h1))):
        b = h1[j]
        if d == 'bear':
            if b['h'] >= stop: return -1.0, j - i0
            if b['l'] <= target: return rr, j - i0
        else:
            if b['l'] <= stop: return -1.0, j - i0
            if b['h'] >= target: return rr, j - i0
    return None, None


def net(o, entry, R, bars, swap_bps):
    if o is None: return None
    frac = R / abs(entry) if entry else 0
    if frac <= 0: return None
    fixed = (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac
    days = bars / BARS_PER_DAY
    swap = (swap_bps / 10000.0 * days) / frac    # swap as fraction of price / (R/price)
    return o - fixed - swap


def agg(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def build():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    trades = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 300 or len(daily) < 35: continue
        d_ts = [b['_ts'] for b in daily]
        cache = {}
        def aew(dd):
            if dd not in cache:
                try:
                    r = auto_detect_ew(draw[:dd + 1]); ewp = r.get('ew') if r.get('ok') else None
                    cache[dd] = ewp['dir'] if (ewp and ewp.get('dir') in ('bull', 'bear') and ewp.get('confidence', 0) >= AUTO_EW_MIN_CONFIDENCE and ewp.get('pattern') in AUTO_EW_VALID_PATTERNS) else None
                except Exception:
                    cache[dd] = None
            return cache[dd]
        for kind in ('bear', 'bull'):
            for tr in scan(h1, kind):
                dd = bisect.bisect_right(d_ts, tr['ts']) - 2
                macro = aew(dd); tdir = 'bear' if kind == 'bear' else 'bull'
                if not (macro is not None and macro != tdir):   # macro-OPPOSES only (validated cohort)
                    continue
                tr['h1'] = h1; tr['cls'] = PAIR_CLASS.get(pk); tr['pair'] = pk
                outs = {}
                for tg in TARGETS:
                    target = tr['mm_target'] if tg == 'mm' else (tr['entry'] - tg * tr['R'] if kind == 'bear' else tr['entry'] + tg * tr['R'])
                    o, bars = resolve(h1, tr, target)
                    outs[tg] = (o, bars)
                tr['outs'] = outs
                trades.append(tr)
    trades.sort(key=lambda t: t['ts'])
    return trades


def main():
    trades = build()
    print(f"H&S · macro-OPPOSES cohort (the validated edge): {len(trades)} trades\n")

    # Days held
    held = [b / BARS_PER_DAY for t in trades for (o, b) in [t['outs'][2.0]] if b is not None]
    if held:
        held.sort()
        print(f"Holding period (target 1:2): median {held[len(held)//2]:.1f}d  "
              f"mean {sum(held)/len(held):.1f}d  90th pct {held[int(0.9*len(held))]:.1f}d\n")

    def lbl(t, w=10):
        s = ('1:%s' % t) if t != 'mm' else 'measured'
        return s.rjust(w)

    # Swap sensitivity (full sample)
    print("SWAP SENSITIVITY (full sample, net expectancy):")
    print(f"  {'swap/day':>9} | " + "  ".join(lbl(t) for t in TARGETS))
    for sb in SWAP_BPS_DAY_SWEEP:
        cells = []
        for tg in TARGETS:
            seq = [net(o, t['entry'], t['R'], b, sb) for t in trades for (o, b) in [t['outs'][tg]]]
            n, w, e = agg(seq)
            cells.append(f"{e:>+7.3f}({w:>2.0f}%)")
        print(f"  {sb:>6.1f}bps | " + "  ".join(f"{c:>10}" for c in cells))

    # Rolling walk-forward (swap = 1.0 bps/day, a mid estimate)
    SB = 1.0
    print(f"\nROLLING WALK-FORWARD — {KFOLDS} folds, swap {SB}bps/day:")
    n = len(trades); fold = n // KFOLDS
    print(f"  {'fold':>5} {'period':>21} | " + "  ".join(lbl(t, 12) for t in TARGETS))
    import datetime
    def dt(ms): return datetime.datetime.fromtimestamp(ms, datetime.timezone.utc).strftime('%m-%d')
    pos = defaultdict(int); tot = defaultdict(int)
    for f in range(KFOLDS):
        seg = trades[f*fold: (f+1)*fold if f < KFOLDS-1 else n]
        if not seg: continue
        per = f"{dt(seg[0]['ts'])}..{dt(seg[-1]['ts'])}"
        cells = []
        for tg in TARGETS:
            seq = [net(o, t['entry'], t['R'], b, SB) for t in seg for (o, b) in [t['outs'][tg]]]
            k, w, e = agg(seq)
            cells.append(f"{e:>+6.3f}/{w:>2.0f}%")
            tot[tg] += 1
            if e > 0: pos[tg] += 1
        print(f"  {f+1:>5} {per:>21} | " + "  ".join(f"{c:>12}" for c in cells))
    print("  " + "-" * 60)
    print("  folds POSITIVE: " + "  ".join(f"{('1:%s'%t if t!='mm' else 'meas')}={pos[t]}/{tot[t]}" for t in TARGETS))


if __name__ == '__main__':
    main()
