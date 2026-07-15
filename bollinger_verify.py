"""Stress-test the one strong Bollinger finding: taking swing entries when the
bands are WIDE (volatility already expanding) rather than in a squeeze.

Focus on s5_rsi (the standout: baseline +0.48R -> WIDE +0.94R) plus a check that
hs and s5_engulf improve too. Tests:
  1) baseline vs WIDE per strategy
  2) 6-fold walk-forward on s5_rsi WIDE
  3) parameter sensitivity (BB period / K / wide threshold)
  4) per-class of s5_rsi WIDE
'WIDE' = bandwidth[i] >= THRESH x trailing-100 mean (no lookahead).
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, walk, cost, agg, HOLD
from hs_swing_research import MAX_HOLD as HS_HOLD
from unified_shadow_harness import detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
RR = 2.0


def bw_series(bars, n, k):
    c = [b['c'] for b in bars]; N = len(c); bw = [None]*N
    for i in range(n-1, N):
        win = c[i-n+1:i+1]; m = sum(win)/n; sd = (sum((x-m)**2 for x in win)/n)**0.5
        bw[i] = (2*k*sd)/m if m else None
    return bw


def is_wide(bw, i, thresh):
    if i < 0 or i >= len(bw) or bw[i] is None:
        return None
    prev = [x for x in bw[max(0, i-100):i] if x is not None]
    if not prev:
        return None
    return bw[i] >= thresh*(sum(prev)/len(prev))


# ---- collect signals once (r at RR2 is independent of BB params) ----
def collect():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    out = defaultdict(list)      # strat -> list of (pk, tf, ei, ts, r, cls)
    seriesbank = {}              # (pk, tf) -> bars
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80:
            continue
        b4 = agg4h(h1); series = {'h1': h1, '4h': b4, 'daily': daily}
        ts = {tf: [b['_ts'] for b in bars] for tf, bars in series.items()}
        holds = {'h1': HS_HOLD, '4h': HOLD['4h'], 'daily': 20}
        for tf, bars in series.items():
            seriesbank[(pk, tf)] = bars
        sigs = {'hs': (detect_hs(pk, h1, daily, draw), 'h1'),
                's5_rsi': (detect_s5(pk, h1, daily, 'rsi'), '4h'),
                's5_engulf': (detect_s5(pk, h1, daily, 'engulf'), '4h')}
        for strat, (found, tf) in sigs.items():
            bars = series[tf]; hold = holds[tf]
            for s in found:
                ei = bisect.bisect_left(ts[tf], s['entry_ts'])
                if ei >= len(bars) or ei < 1:
                    continue
                o = walk(bars, ei, s['entry'], s['stop'], s['dir'], RR, hold)
                if o is None:
                    continue
                r = o - cost(o, s['entry'], abs(s['entry']-s['stop']))
                out[strat].append((pk, tf, ei, s['entry_ts'], r, cls))
    return out, seriesbank


def wide_rows(sig, seriesbank, n, k, thresh, cls_filter=None):
    bwcache = {}
    rows = []
    for pk, tf, ei, ts, r, cls in sig:
        if cls_filter and cls != cls_filter:
            continue
        key = (pk, tf)
        if key not in bwcache:
            bwcache[key] = bw_series(seriesbank[key], n, k)
        if is_wide(bwcache[key], ei-1, thresh):
            rows.append((ts, r, cls))
    return rows


def rep(label, rows):
    rows = sorted(rows); seq = [r for _, r, *_ in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r, *_ in rows[:mid]]); _, _, es = agg([r for _, r, *_ in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<22} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    sig, sb = collect()

    print("1) baseline vs WIDE (BB 20,2 · thresh 0.85):")
    for strat in ('s5_rsi', 'hs', 's5_engulf'):
        base = [(t, r) for _, _, _, t, r, _ in sig[strat]]
        _, bwr, bexp = agg([r for _, r in base])
        wide = wide_rows(sig[strat], sb, 20, 2.0, 0.85)
        print(f"  {strat}: baseline n={len(base)} exp={bexp:+.3f}R")
        rep("   WIDE", [(t, r) for t, r, _ in wide])
    print()

    print("2) s5_rsi WIDE — 6-fold walk-forward:")
    wide = sorted([(t, r) for t, r, _ in wide_rows(sig['s5_rsi'], sb, 20, 2.0, 0.85)])
    k = 6; sz = len(wide)//k; passed = 0
    for f in range(k):
        lo = f*sz; hi = (f+1)*sz if f < k-1 else len(wide)
        fold = [r for _, r in wide[lo:hi]]; fn, fw, fe = agg(fold); ok = fe > 0; passed += ok
        print(f"   fold {f+1}: n={fn:>3} WR={fw:>5.1f}% exp={fe:>+7.3f}R {'ok' if ok else 'NEG'}")
    print(f"   -> {passed}/{k} folds positive\n")

    print("3) s5_rsi WIDE — parameter sensitivity:")
    for (n, kk, th) in [(20,2.0,0.85),(14,2.0,0.85),(26,2.0,0.85),(20,1.5,0.85),(20,2.5,0.85),(20,2.0,0.80),(20,2.0,1.00)]:
        rows = [(t, r) for t, r, _ in wide_rows(sig['s5_rsi'], sb, n, kk, th)]
        nn, ww, ee = agg([r for _, r in rows])
        print(f"   BB({n},{kk}) thresh={th}: n={nn:>4} WR={ww:>5.1f}% exp={ee:>+7.3f}R")
    print()

    print("4) s5_rsi WIDE — per class:")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        rep(c, wide_rows(sig['s5_rsi'], sb, 20, 2.0, 0.85, cls_filter=c))


if __name__ == '__main__':
    main()
