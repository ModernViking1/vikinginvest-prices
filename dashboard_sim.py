"""Simulate what the dashboard Backtest cards + Performance tab WOULD show if
they modelled a realistic fill instead of the idealized best-tick fill. Nothing
is written to the platform — this only prints the corrected numbers for review.

Three views:
  1. Backtest cards  — per-class WR/expectancy, IDEAL vs REALISTIC.
  2. Performance tab — headline aggregate (trades, WR, expectancy, total R,
                       max drawdown) IDEAL vs REALISTIC, on the full universe.
  3. Deployed cohort — the strategy now live (4/4, comm+crypto) under realistic
                       fill+cost, with a chronological equity curve summary.

IDEAL      = filled at the setup bar's best tick, walk from next bar (today's cards)
REALISTIC  = cBot limit fill (level must trade within EXPIRY bars) + live-calibrated
             fixed-price cost (winner ~0.0045% / loser ~0.0105% of price).
"""
import json, bisect
from collections import defaultdict
from detect_triggers import RSI_GATE_BY_CLASS, PAIR_CLASS
from backtest_rsi_per_class import (
    _bars_norm, _min_prom, precompute_break_dirs, precompute_cl_dir,
    precompute_rsi, _find_struct_high, _find_struct_low,
)
from backtest_school_run_full import classify_setup

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
NW_LB, TL_LB, EW_LB, BOS_LB, WALK = 5, 8, 8, 24, 48
EXPIRY = 3
WIN_COST_PCT = 0.0045 / 100
LOSS_COST_PCT = 0.0105 / 100
MIN_STOP_REL = 0.0015   # deployed cost-per-R floor (non-FX)


def walk(m15, start, entry, stop, target, d):
    if abs(entry - stop) <= 0: return None
    for j in range(start, min(start + WALK, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return 1.0
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return 1.0
    return None


def cost_R(o, stop_frac):
    if stop_frac <= 0: return 0.0
    return (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / stop_frac


def collect(pd, pk, rows):
    """Append (class, conf, ts, ideal_R, real_R_or_None, stop_frac) per setup."""
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', [])); daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    gate = RSI_GATE_BY_CLASS.get(PAIR_CLASS.get(pk), {'hi': 80, 'lo': 20}); cls = PAIR_CLASS.get(pk)
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last: continue
        ts = m15[i]['_ts']
        # honest closed-bar HTF indexing (what live can actually see)
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < TL_LB or dd < EW_LB: continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        if None in (ew, tl, nw, cl): continue
        conf, d = classify_setup(ew, tl, nw, cl)
        if conf < 2 or d is None: continue
        lb = m15[max(0, i - 8):i]
        if len(lb) < 5: continue
        shi = max(b['h'] for b in lb); slo = min(b['l'] for b in lb); mp = _min_prom(m15[i]['c'])
        if d == 'bull':
            if not (m15[i]['c'] > shi and (m15[i]['c'] - shi) >= mp): continue
        else:
            if not (m15[i]['c'] < slo and (slo - m15[i]['c']) >= mp): continue
        bos = m15[max(0, i - BOS_LB):i]; prom = _min_prom(m15[i]['c'])
        if d == 'bear':
            entry = m15[i]['h']; stop = _find_struct_high(bos, prom)
            if stop <= entry: continue
        else:
            entry = m15[i]['l']; stop = _find_struct_low(bos, prom)
            if stop >= entry: continue
        atr = m15[max(0, i - 20):i]
        if len(atr) >= 14:
            a20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
            fxf = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if (stop - entry if d == 'bear' else entry - stop) < max(0.5 * a20, fxf): continue
        rv = rsi_arr[h] if h < len(rsi_arr) else None
        if rv is not None:
            if d == 'bull' and rv >= gate['hi']: continue
            if d == 'bear' and rv <= gate['lo']: continue
        S = abs(entry - stop); stop_frac = S / abs(entry) if entry else 0
        target = entry + S if d == 'bull' else entry - S
        # IDEAL — filled at struct level, walk from next bar
        ideal = walk(m15, i + 1, entry, stop, target, d)
        if ideal is None:
            last = i + WALK; continue
        # REALISTIC — limit must fill within EXPIRY, then walk
        real = None
        fj = None
        for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
            b = m15[j]
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                fj = j; break
        if fj is not None:
            real = walk(m15, fj + 1, entry, stop, target, d)
        rows.append((cls, conf, ts, ideal, real, stop_frac))
        last = i + 1


def eq_stats(seq):
    """seq: list of (ts, R). Returns (n, wr, exp, totalR, maxDD, curve_pts)."""
    seq = sorted(seq)
    n = len(seq)
    if not n: return (0, 0, 0, 0, 0, [])
    wins = sum(1 for _, r in seq if r > 0)
    tot = sum(r for _, r in seq)
    cum = peak = mdd = 0.0; curve = []
    for ts, r in seq:
        cum += r; peak = max(peak, cum); mdd = min(mdd, cum - peak)
        curve.append((ts, cum))
    return (n, 100 * wins / n, tot / n, tot, mdd, curve)


def fmt_ts(sec):
    import datetime
    return datetime.datetime.fromtimestamp(sec, datetime.timezone.utc).strftime('%Y-%m-%d')


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    rows = []
    for p in [x for x in PAIR_CLASS if x in pairs]: collect(pairs[p], p, rows)

    # ---- View 1: backtest cards, per class (conf>=2, full universe) ----
    print("=" * 78)
    print("1. BACKTEST CARDS — per class (structural conf>=2, full universe)")
    print("   IDEAL = what the cards show today   ·   REAL = realistic limit fill + cost")
    print("=" * 78)
    print(f"{'class':<8} | {'n':>5} {'WR_ideal':>9} {'exp_ideal':>10} | {'n_fill':>7} {'WR_real':>8} {'exp_real':>9}")
    by_cls = defaultdict(list)
    for cls, conf, ts, ideal, real, sf in rows:
        by_cls[cls].append((conf, ts, ideal, real, sf))
    for cls in sorted(by_cls):
        r = by_cls[cls]
        idl = [x[2] for x in r]
        realv = [(x[1], x[3] - cost_R(x[3], x[4])) for x in r if x[3] is not None]
        ni, wri = len(idl), 100 * sum(1 for v in idl if v > 0) / max(1, len(idl))
        expi = sum(idl) / max(1, len(idl))
        nr, wrr, expr, *_ = eq_stats(realv)
        print(f"{cls:<8} | {ni:>5} {wri:>8.1f}% {expi:>+10.3f} | {nr:>7} {wrr:>7.1f}% {expr:>+9.3f}")

    # ---- View 2: Performance headline, full universe ----
    print("\n" + "=" * 78)
    print("2. PERFORMANCE TAB — headline aggregate (full universe, conf>=2)")
    print("=" * 78)
    idl = [x[3] for x in rows]
    ni = len(idl); wri = 100 * sum(1 for v in idl if v > 0) / ni; toti = sum(idl)
    cum = peak = mddi = 0.0
    for _, _, ts, v, _, _ in sorted(rows, key=lambda z: z[2]):
        cum += v; peak = max(peak, cum); mddi = min(mddi, cum - peak)
    realv = [(x[2], x[4] - cost_R(x[4], x[5])) for x in rows if x[4] is not None]
    nr, wrr, expr, totr, mddr, _ = eq_stats(realv)
    print(f"{'metric':<16} {'IDEAL (shown today)':>22} {'REALISTIC':>16}")
    print(f"{'trades':<16} {ni:>22} {nr:>16}")
    print(f"{'win rate':<16} {wri:>21.1f}% {wrr:>15.1f}%")
    print(f"{'expectancy/trade':<16} {toti/ni:>+21.3f}R {expr:>+15.3f}R")
    print(f"{'total R':<16} {toti:>+21.1f}R {totr:>+15.1f}R")
    print(f"{'max drawdown':<16} {mddi:>+21.1f}R {mddr:>+15.1f}R")

    # ---- View 3: deployed cohort (4/4, comm+crypto) realistic + equity curve ----
    print("\n" + "=" * 78)
    print("3. DEPLOYED STRATEGY — 4/4 confluence, comm+crypto live — REALISTIC")
    print("   (min-stop floor 0.15% applied, as deployed)")
    print("=" * 78)
    dep = []
    for cls, conf, ts, ideal, real, sf in rows:
        if conf == 4 and cls in ('comm', 'crypto') and real is not None and sf >= MIN_STOP_REL:
            dep.append((cls, ts, real - cost_R(real, sf)))
    print(f"{'class':<10} {'n':>5} {'WR':>7} {'exp':>9} {'totalR':>9} {'maxDD':>8}")
    for cls in ('comm', 'crypto'):
        seq = [(ts, r) for c, ts, r in dep if c == cls]
        n, wr, exp, tot, mdd, _ = eq_stats(seq)
        if n: print(f"{cls:<10} {n:>5} {wr:>6.1f}% {exp:>+9.3f} {tot:>+9.1f} {mdd:>+8.1f}")
    allseq = [(ts, r) for _, ts, r in dep]
    n, wr, exp, tot, mdd, curve = eq_stats(allseq)
    print(f"{'COMBINED':<10} {n:>5} {wr:>6.1f}% {exp:>+9.3f} {tot:>+9.1f} {mdd:>+8.1f}")
    if curve:
        peak_pt = max(curve, key=lambda z: z[1]); fin = curve[-1]
        trough = min(curve, key=lambda z: z[1])
        print(f"\nEquity curve (realistic, deployed cohort):")
        print(f"  start {fmt_ts(curve[0][0])}  ->  end {fmt_ts(fin[0])}   final cum = {fin[1]:+.1f}R")
        print(f"  peak {peak_pt[1]:+.1f}R @ {fmt_ts(peak_pt[0])}   ·   trough {trough[1]:+.1f}R @ {fmt_ts(trough[0])}")
        # sampled curve (10 points)
        step = max(1, len(curve) // 10)
        print("  sampled: " + "  ".join(f"{fmt_ts(curve[k][0])[5:]}:{curve[k][1]:+.0f}R" for k in range(0, len(curve), step)))


if __name__ == '__main__':
    main()
