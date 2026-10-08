"""Would a 0.75R trail help the SWING book, which (unlike the .EQ pure runner) has a FIXED +2R target?

The .EQ test showed a 0.75R trail beats 1.0R for a pure runner (no target). The swing exit is
different: fixed RR2 target + a 1R trail that arms at +1R (trail_partial_research). The +2R cap
changes the trade-off — a tighter trail may just scratch more of the +1R→+2R trades instead of
letting them reach target. So we TEST it on the same RR2 breakout population the swing trail was
validated on, comparing trail distances with OOS halves + by-class. Reuses trail_partial_research's
exact walk model. Commits nothing.

  python swing_trail_backtest.py --hist historical-ohlc.json
  python swing_trail_backtest.py --hist deep-ohlc.json
"""
import argparse, json, os
from collections import defaultdict
from backtest_rsi_per_class import _bars_norm
from trail_partial_research import walk_trail, walk_baseline, walk_be
from be_stop_research import signals, LOOK, HOLD, RR
from detect_triggers import PAIR_CLASS

VARIANTS = {
    'baseline':   lambda b, i, e, s, d: walk_baseline(b, i, e, s, d),      # target-or-stop, no trail
    'BE@60':      lambda b, i, e, s, d: walk_be(b, i, e, s, d, 0.60),
    'trail 1.0R': lambda b, i, e, s, d: walk_trail(b, i, e, s, d, 1.0),    # current live
    'trail 0.75R':lambda b, i, e, s, d: walk_trail(b, i, e, s, d, 0.75),   # the candidate
    'trail 0.5R': lambda b, i, e, s, d: walk_trail(b, i, e, s, d, 0.5),
    'trail 1.5R': lambda b, i, e, s, d: walk_trail(b, i, e, s, d, 1.5),
}
ORDER = ['baseline', 'BE@60', 'trail 1.0R', 'trail 0.75R', 'trail 0.5R', 'trail 1.5R']


def stats(rs):
    n = len(rs)
    if not n:
        return None
    w = [x for x in rs if x > 0.02]
    return dict(n=n, wr=100.0 * len(w) / n, exp=sum(rs) / n, tot=sum(rs))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='historical-ohlc.json')
    args = ap.parse_args()
    dpairs = json.load(open(args.hist))['pairs']
    recs = []                                           # {'t':entry_ts, variant:r}
    by_class = defaultdict(lambda: defaultdict(list))
    for pk in [x for x in PAIR_CLASS if x in dpairs]:
        cls = PAIR_CLASS.get(pk)
        bars = _bars_norm(dpairs[pk].get('h1', []))
        if len(bars) < LOOK + 300:
            continue
        for (ei, entry, stop, dr) in signals(bars):
            if ei >= len(bars):
                continue
            base = walk_baseline(bars, ei, entry, stop, dr)
            if base is None:
                continue                                # unresolved -> exclude from every variant
            rec = {'t': bars[ei]['_ts']}
            for v, fn in VARIANTS.items():
                rec[v] = fn(bars, ei, entry, stop, dr)
            recs.append(rec)
            for v in VARIANTS:
                if rec[v] is not None:
                    by_class[cls][v].append(rec[v])
    recs.sort(key=lambda r: r['t'])
    mid = len(recs) // 2

    def col(rows, v):
        xs = [r[v] for r in rows if r.get(v) is not None]
        return (sum(xs) / len(xs)) if xs else 0.0

    print(f"=== swing-trail backtest · {args.hist} · RR2 breakout population · {len(recs)} signals "
          f"(target +{RR:.0f}R, trail arms +1R) ===\n")
    print(f"{'scheme':<13}{'n':>7}{'WR':>6}{'exp':>9}{'totR':>8}{'OOS1':>9}{'OOS2':>9}{'vs 1.0R':>9}")
    allr = {v: [r[v] for r in recs if r.get(v) is not None] for v in VARIANTS}
    t1 = stats(allr['trail 1.0R'])['exp']
    for v in ORDER:
        s = stats(allr[v])
        if not s:
            continue
        print(f"{v:<13}{s['n']:>7}{s['wr']:>5.0f}%{s['exp']:>+8.3f}R{s['tot']:>+7.0f}"
              f"{col(recs[:mid], v):>+8.3f}R{col(recs[mid:], v):>+8.3f}R{s['exp']-t1:>+8.3f}R")

    print("\nBY ASSET CLASS (exp per trade)")
    print(f"{'class':<8}{'n':>6}  " + "".join(f"{v:>13}" for v in ['trail 1.0R', 'trail 0.75R']))
    for cls in sorted(by_class, key=lambda c: -len(by_class[c]['trail 1.0R'])):
        d = by_class[cls]; n = len(d['trail 1.0R'])
        if n < 30:
            continue
        cells = "".join(f"{(sum(d[v]) / len(d[v]) if d[v] else 0):>+12.3f}R" for v in ['trail 1.0R', 'trail 0.75R'])
        print(f"{cls:<8}{n:>6}  {cells}")

    print("\nKeep the swing trail at 1.0R unless 0.75R clearly beats it overall AND in both OOS halves")
    print("AND across classes. The +2R target caps upside, so a tighter trail can hurt here even though")
    print("it helped the uncapped .EQ runner — that's why we test, not assume.")


if __name__ == '__main__':
    main()
