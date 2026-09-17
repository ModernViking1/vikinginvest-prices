"""Realistic-fill retest of the three intraday strategies (structural, macdp
candidate, macd_divergence). Settles whether their optimistic-fill strength is
real once the entry limit must actually FILL and cost is charged.

Scratchpad only — reads repo files, writes nothing but stdout.

Method (per strategy, ONE signal set, three resolution models so the fill/cost
effect is isolated):
  A  optimistic  — enter at the signal bar's wick, immediate forward-walk to
                   target/stop within WALK_OPT bars, no cost (the current model
                   that inflates macdp to ~74%).
  B  realistic   — place a LIMIT at entry; it fills only if a later bar within
                   EXPIRY_BARS trades to it; then resolve within RESOLVE_CAP.
                   No-fill and no-resolve are EXCLUDED (a no-fill is NOT a loss).
                   Reuses backtest_h7_confluence.simulate exactly. No cost.
  C  realistic+cost — B minus a per-trade cost_R.

Non-overlap: signals are deduped by the realistic resolution bar (live-accurate
— you cannot arm a new limit while one is pending), the same set feeds A/B/C.

Cost model (reused from five_strategies_research / the detect_triggers live-cost
note): cost is ~FIXED IN PRICE — 0.0045% of price on winners, 0.0105% on losers
(spread + commission + stop slippage, calibrated from executions.json). Hence
cost_R = (that fraction x entry) / |entry - stop|, i.e. it shrinks as the stop
widens. Auto-scales across classes via price, so no per-class pip table needed;
realized mean cost_R is reported per strategy so the magnitude is visible.
"""
import bisect
import os
import sys

REPO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO)

import json
import backtest_intraday as B
from backtest_rsi_per_class import (
    _bars_norm, precompute_break_dirs, precompute_cl_dir, precompute_rsi,
)
from detect_triggers import (
    PAIR_CLASS, macd_series, MACDP_HTF_FILTER, _htf_blocks, _stop_too_tight,
    _find_structural_high, _find_structural_low, _bos_min_prominence, rsi_gate_for,
)

HIST = os.path.join(REPO, 'historical-ohlc.json')
EXPIRY_BARS = 64          # bars to fill the limit (h7)
RESOLVE_CAP = 96          # bars to resolve once filled (h7)
WALK_OPT = 48             # optimistic immediate-walk horizon (audit)
THIN = 40

# fixed-price cost calibration (reused from five_strategies_research.py)
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100


def cost_R(o, entry, R):
    """Per-trade cost in R (five_strategies_research.cost)."""
    if not entry or R <= 0:
        return 0.0
    frac = R / abs(entry)
    return (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def simulate(m15, i, d, entry, stop, target):
    """backtest_h7_confluence.simulate — limit fills within EXPIRY_BARS, then
    resolves within RESOLVE_CAP. Returns (outcome, last_bar_idx)."""
    n = len(m15)
    filled = False
    for j in range(i + 1, min(i + 1 + EXPIRY_BARS, n)):
        b = m15[j]
        if not filled:
            if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
                filled = True
        if filled:
            for jj in range(j, min(j + RESOLVE_CAP, n)):
                bb = m15[jj]
                if d == 'bull':
                    if bb['l'] <= stop:
                        return 'loss', jj
                    if bb['h'] >= target:
                        return 'win', jj
                else:
                    if bb['h'] >= stop:
                        return 'loss', jj
                    if bb['l'] <= target:
                        return 'win', jj
            return 'expired', jj
    return 'nofill', i


def opt_outcome(m15, i, d, entry, stop, target):
    """Optimistic: assume immediate fill at entry, walk WALK_OPT bars."""
    return B._outcome_macdp(m15, i, entry, stop, target, d)   # WALK is 48 in B


# ── signal generators: yield fully-formed signals + realistic outcome ──
# each item: dict(pair, i, ts, dir, entry, stop, target, R, conf, r_out, jj)

def _finish(m15, i, pk, d, entry, stop, target, conf, out):
    r_out, jj = simulate(m15, i, d, entry, stop, target)
    out.append(dict(pair=pk, i=i, ts=m15[i]['_ts'], dir=d, entry=entry, stop=stop,
                    target=target, R=abs(entry - stop), conf=conf,
                    r_out=r_out, jj=jj))
    return jj


def gen_macdp(pd, pk):
    """audit-engine macd-primary: all eligible classes, conf>=1, no Faytterro."""
    cls = PAIR_CLASS.get(pk)
    if cls not in B._ELIGIBLE:
        return []
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', []))
    daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_a = precompute_break_dirs(daily, 8); tl_a = precompute_break_dirs(h1, 8)
    nw_a = precompute_break_dirs(m15, 5); cl_a = precompute_cl_dir(h1)
    rsi_a = precompute_rsi([b['c'] for b in h1], 14)
    ml, sl = macd_series([b['c'] for b in m15], 12, 26, 9)
    out = []; last = -1
    for i in range(40, len(m15) - 1):
        if i <= last:
            continue
        m0, m1, s0, s1 = ml[i - 1], ml[i], sl[i - 1], sl[i]
        if None in (m0, m1, s0, s1):
            continue
        if m0 <= s0 and m1 > s1:
            d = 'bull'
        elif m0 >= s0 and m1 < s1:
            d = 'bear'
        else:
            continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < 8 or dd < 8:
            continue
        ew, tl, nw, cl = ew_a[dd], tl_a[h], nw_a[i], cl_a[h]
        if _htf_blocks(d, cl, enabled=MACDP_HTF_FILTER):
            continue
        r = rsi_a[h] if h < len(rsi_a) else None
        if r is None:
            continue
        if d == 'bull' and r >= 50:
            continue
        if d == 'bear' and r <= 50:
            continue
        conf = sum(1 for L in (ew, tl, nw, cl) if L == d)
        if conf < 1:
            continue
        ss = m15[max(0, i - 8):i]
        if d == 'bull':
            entry = m15[i]['l']; stop = min((b['l'] for b in ss), default=None)
            if stop is None or stop >= entry:
                continue
            rr = entry - stop
        else:
            entry = m15[i]['h']; stop = max((b['h'] for b in ss), default=None)
            if stop is None or stop <= entry:
                continue
            rr = stop - entry
        if rr <= 0 or _stop_too_tight(rr, entry, cls):
            continue
        target = entry + rr if d == 'bull' else entry - rr
        last = _finish(m15, i, pk, d, entry, stop, target, conf, out)
    return out


def gen_divergence(pd, pk):
    cls = PAIR_CLASS.get(pk)
    if cls != 'index':
        return []
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', []))
    daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_a = precompute_break_dirs(daily, 8); tl_a = precompute_break_dirs(h1, 8)
    nw_a = precompute_break_dirs(m15, 5); cl_a = precompute_cl_dir(h1)
    ml, _ = macd_series([b['c'] for b in m15], 12, 26, 9)
    lookback, lb_struct = 30, 8
    out = []; last = -1
    for i in range(40, len(m15) - 1):
        if i <= last:
            continue
        start = max(2, i - lookback); end = i - 2
        if end < start + 1:
            continue
        lows, highs = [], []
        for j in range(start, end + 1):
            b, pb1, pb2, fb1, fb2 = m15[j], m15[j-1], m15[j-2], m15[j+1], m15[j+2]
            if b['l'] < pb1['l'] and b['l'] < pb2['l'] and b['l'] < fb1['l'] and b['l'] < fb2['l']:
                lows.append({'idx': j, 'val': b['l'], 'macd': ml[j]})
            if b['h'] > pb1['h'] and b['h'] > pb2['h'] and b['h'] > fb1['h'] and b['h'] > fb2['h']:
                highs.append({'idx': j, 'val': b['h'], 'macd': ml[j]})
        d = None
        if len(lows) >= 2:
            a, prev = lows[-1], lows[-2]
            if (a['val'] < prev['val'] and a['macd'] is not None and prev['macd'] is not None
                    and a['macd'] > prev['macd'] and (i - a['idx']) <= 5):
                d = 'bull'
        if d is None and len(highs) >= 2:
            a, prev = highs[-1], highs[-2]
            if (a['val'] > prev['val'] and a['macd'] is not None and prev['macd'] is not None
                    and a['macd'] < prev['macd'] and (i - a['idx']) <= 5):
                d = 'bear'
        if d is None:
            continue
        ts = m15[i]['_ts']
        hh = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if hh < 8 or dd < 8:
            continue
        ew, tl, nw, cl = ew_a[dd], tl_a[hh], nw_a[i], cl_a[hh]
        conf = sum(1 for L in (ew, tl, nw, cl) if L == d)
        if conf == 0 or conf == 4:
            continue
        ss = m15[max(0, i - lb_struct):i]
        if d == 'bull':
            entry = m15[i]['l']; stop = min((b['l'] for b in ss), default=None)
            if stop is None or stop >= entry:
                continue
            rr = entry - stop
        else:
            entry = m15[i]['h']; stop = max((b['h'] for b in ss), default=None)
            if stop is None or stop <= entry:
                continue
            rr = stop - entry
        if rr <= 0 or _stop_too_tight(rr, entry, cls):
            continue
        target = entry + rr if d == 'bull' else entry - rr
        last = _finish(m15, i, pk, d, entry, stop, target, conf, out)
    return out


def gen_structural(pd, pk):
    """4/4 wick signal — same construction/filters as backtest_intraday
    (BoS-24 stop, RSI hard-gate, min-R floor, comm/index max-R cap, index MACD
    confirm), but the outcome is the realistic limit-fill simulate."""
    cls = PAIR_CLASS.get(pk)
    if cls not in B._ELIGIBLE:
        return []
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', []))
    daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_a = precompute_break_dirs(daily, 8); tl_a = precompute_break_dirs(h1, 8)
    nw_a = precompute_break_dirs(m15, 5); cl_a = precompute_cl_dir(h1)
    rsi_a = precompute_rsi([b['c'] for b in h1], 14)
    ml, sl = macd_series([b['c'] for b in m15], 12, 26, 9)
    g = rsi_gate_for(pk); rsi_hi, rsi_lo = g['hi'], g['lo']
    out = []; last = -1
    for i in range(40, len(m15) - 1):
        if i <= last:
            continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < 8 or dd < 8:
            continue
        ew, tl, nw, cl = ew_a[dd], tl_a[h], nw_a[i], cl_a[h]
        if not ew or ew not in ('bull', 'bear') or not (ew == tl == nw == cl):
            continue
        d = ew
        lb = m15[max(0, i - 8):i]
        if len(lb) < 5:
            continue
        c = m15[i]['c']
        if d == 'bull':
            if not c > max(b['h'] for b in lb):
                continue
        else:
            if not c < min(b['l'] for b in lb):
                continue
        if cls == 'index':
            found = False
            for j in range(max(1, i - 2), i + 1):
                m0, m1, s0, s1 = ml[j - 1], ml[j], sl[j - 1], sl[j]
                if None in (m0, m1, s0, s1):
                    continue
                if d == 'bull' and m0 <= s0 and m1 > s1:
                    found = True; break
                if d == 'bear' and m0 >= s0 and m1 < s1:
                    found = True; break
            if not found:
                continue
        r = rsi_a[h] if h < len(rsi_a) else None
        if r is not None:
            if d == 'bull' and r >= rsi_hi:
                continue
            if d == 'bear' and r <= rsi_lo:
                continue
        bos = m15[max(0, i - 24):i]; mp = _bos_min_prominence(c)
        if d == 'bull':
            entry = m15[i]['l']; stop = _find_structural_low(bos, mp)
            if stop is None or stop >= entry:
                continue
        else:
            entry = m15[i]['h']; stop = _find_structural_high(bos, mp)
            if stop is None or stop <= entry:
                continue
        R = abs(stop - entry)
        if R <= 0:
            continue
        atr_s = m15[max(0, i - 10):i]
        atr20 = sum(abs(b['h'] - b['l']) for b in atr_s) / len(atr_s) if atr_s else 0
        is_fx = cls in ('major', 'minor')
        fx_floor = (0.12 if abs(entry) > 50 else 0.0012) if is_fx else 0
        min_R = max(0.5 * atr20, fx_floor)
        if min_R > 0 and R < min_R:
            continue
        if cls in ('comm', 'index'):
            max_R = 2.5 * atr20
            if max_R > 0 and min_R > 0 and max_R < min_R:
                max_R = min_R
            if max_R > 0 and R > max_R:
                stop = entry - max_R if d == 'bull' else entry + max_R
                R = max_R
        target = entry + R if d == 'bull' else entry - R
        last = _finish(m15, i, pk, d, entry, stop, target, 4, out)
    return out


# ── metrics ──

def _agg(seq):
    n = len(seq)
    if not n:
        return 0, None, None
    return n, 100 * sum(1 for r in seq if r > 0) / n, sum(seq) / n


def _oos(rows):
    """rows: [(ts, r)]. -> (n, wr, exp, oos1, oos2)."""
    s = sorted(rows)
    seq = [r for _, r in s]
    n, wr, exp = _agg(seq)
    m = len(seq) // 2
    _, _, e1 = _agg(seq[:m]); _, _, e2 = _agg(seq[m:])
    return n, wr, exp, e1, e2


def _f(x, w=8, d=4):
    return (f'{x:+.{d}f}'.rjust(w)) if x is not None else '  n/a  '.rjust(w)


def durable(rows):
    n, _, exp, e1, e2 = _oos(rows)
    return n >= THIN and exp is not None and exp > 0 and e1 and e1 > 0 and e2 and e2 > 0


def main():
    doc = json.load(open(HIST)); pairs = doc.get('pairs', {})
    gens = {'structural': gen_structural, 'macdp': gen_macdp, 'macd_divergence': gen_divergence}
    sigs = {s: [] for s in gens}
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        pd = pairs[pk]
        m15 = _bars_norm(pd.get('m15', []))
        for s, fn in gens.items():
            try:
                items = fn(pd, pk)
            except Exception as e:
                print(f'  WARN {s} failed for {pk}: {e}'); continue
            for it in items:
                # A: optimistic immediate-fill walk on the same signal
                a = opt_outcome(m15, it['i'], it['dir'], it['entry'], it['stop'], it['target'])
                it['a'] = a
            sigs[s].extend(items)

    print(f"realistic-fill model: limit fills within {EXPIRY_BARS} m15 bars, resolves within "
          f"{RESOLVE_CAP}; no-fill excluded (NOT a loss).")
    print(f"cost: {WIN_COST_PCT*100:.4f}% of price on wins / {LOSS_COST_PCT*100:.4f}% on losses "
          f"(fixed-price, from executions.json calibration) -> cost_R = pct*entry/|entry-stop|.")

    verdicts = {}
    for s in ('structural', 'macdp', 'macd_divergence'):
        items = sigs[s]
        n_sig = len(items)
        # A — optimistic (drop unresolved-within-walk, i.e. a is None)
        A = [(it['ts'], it['a']) for it in items if it['a'] is not None]
        # B — realistic fill, no cost
        filled = [it for it in items if it['r_out'] in ('win', 'loss')]
        B_rows = [(it['ts'], 1.0 if it['r_out'] == 'win' else -1.0) for it in filled]
        n_fill_or_resolve = sum(1 for it in items if it['r_out'] != 'nofill')
        n_filled_total = sum(1 for it in items if it['r_out'] in ('win', 'loss', 'expired'))
        # C — realistic + cost
        C_rows = []; costs = []
        for it in filled:
            o = 1.0 if it['r_out'] == 'win' else -1.0
            cr = cost_R(o, it['entry'], it['R'])
            costs.append(cr)
            C_rows.append((it['ts'], o - cr))
        nofill = sum(1 for it in items if it['r_out'] == 'nofill')
        expired = sum(1 for it in items if it['r_out'] == 'expired')
        fillpct = 100 * n_filled_total / n_sig if n_sig else 0
        meancost = sum(costs) / len(costs) if costs else 0

        nA, wA, eA, _, _ = _oos(A)
        nB, wB, eB, b1, b2 = _oos(B_rows)
        nC, wC, eC, c1, c2 = _oos(C_rows)

        print("\n" + "=" * 82)
        print(f"STRATEGY: {s}   signals={n_sig}  fills(any-resolve)={n_filled_total} "
              f"({fillpct:.0f}%)  nofill={nofill}  expired={expired}  mean cost_R={meancost:.4f}")
        print("-" * 82)
        print(f"  {'model':<26} {'n':>5} {'WR%':>7} {'exp':>9} {'oos1':>9} {'oos2':>9}")
        print(f"  {'A optimistic (no cost)':<26} {nA:>5} {(wA or 0):>6.1f}% {_f(eA,9)} {'—':>9} {'—':>9}")
        print(f"  {'B realistic fill (no cost)':<26} {nB:>5} {(wB or 0):>6.1f}% {_f(eB,9)} {_f(b1,9)} {_f(b2,9)}"
              f"{'  <thin' if nB < THIN else ''}")
        print(f"  {'C realistic fill + cost':<26} {nC:>5} {(wC or 0):>6.1f}% {_f(eC,9)} {_f(c1,9)} {_f(c2,9)}"
              f"{'  <thin' if nC < THIN else ''}")
        verdicts[s] = ('SURVIVES' if durable(C_rows) else 'COLLAPSES', nC, eC, c1, c2)

    print("\n" + "=" * 82)
    print("VERDICT  (survives = realistic fill + cost: exp>0 AND both OOS halves>0 AND n>=40)")
    print("=" * 82)
    for s in ('structural', 'macdp', 'macd_divergence'):
        v, nC, eC, c1, c2 = verdicts[s]
        print(f"  {s:<16}: {v}  (C: n={nC} exp={_f(eC,8)} oos={_f(c1,8)}/{_f(c2,8)})")
    print("\nContext: A here is the pure signal-bar-wick optimistic fill on the SAME signal set "
          "(so A->B isolates the fill effect). Production numbers reported earlier: structural "
          "52.6% uses an intermediate lift+retest fill; macdp 74.5% / divergence 63.6% use A. "
          "m15 ~90 days -> cells <thin (n<40) are unreliable.")


if __name__ == '__main__':
    main()
