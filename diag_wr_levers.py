"""Detailed WR diagnosis: which lever genuinely lifts the REALISTIC-fill win rate
of the 4/4 comm+crypto cohort above the ~50% coin flip?

All outcomes use honest closed-bar HTF indexing. Every trade is scored under the
realistic fill for its entry model. We compare entry models and slice the base
(limit) cohort by RSI, volatility regime, fill speed, and macro-trend strength.

Entry models (same structural stop, 1:1 target, EXPIRY-bar fill window):
  LIMIT  (pullback)     : buy at the creator bar LOW  / sell at its HIGH — today's model
  STOP   (continuation) : buy-stop above the creator bar HIGH / sell-stop below its LOW
  MARKET (next open)    : plain market at next bar open

Prints WR + expectancy + n per bucket so thin cells are visible and not trusted.
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
MIN_STOP_REL = 0.0015
LIVE = ('comm', 'crypto')


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


def limit_fill(m15, i, entry, d):
    for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
        b = m15[j]
        if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
            return j
    return None


def stop_fill(m15, i, trig, d):
    # continuation: fill when price breaks BEYOND trig in the trade direction
    for j in range(i + 1, min(i + 1 + EXPIRY, len(m15))):
        b = m15[j]
        if (d == 'bull' and b['h'] >= trig) or (d == 'bear' and b['l'] <= trig):
            return j
    return None


def collect(pd, pk, out):
    cls = PAIR_CLASS.get(pk)
    h1 = _bars_norm(pd.get('h1', [])); m15 = _bars_norm(pd.get('m15', [])); daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35: return
    h1_ts = [b['_ts'] for b in h1]; d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB); tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB); cl_arr = precompute_cl_dir(h1)
    rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    gate = RSI_GATE_BY_CLASS.get(cls, {'hi': 80, 'lo': 20})
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last: continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2; dd = bisect.bisect_right(d_ts, ts) - 2
        if h < TL_LB or dd < EW_LB: continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        if None in (ew, tl, nw, cl): continue
        conf, d = classify_setup(ew, tl, nw, cl)
        if conf != 4 or d is None: continue
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
        a20 = None
        if len(atr) >= 14:
            a20 = sum(max(b['h'] - b['l'], 1e-9) for b in atr) / len(atr)
            fxf = (0.12 if abs(entry) > 50 else 0.0012) if cls in ('major', 'minor') else 0
            if (stop - entry if d == 'bear' else entry - stop) < max(0.5 * a20, fxf): continue
        rv = rsi_arr[h] if h < len(rsi_arr) else None
        if rv is not None:
            if d == 'bull' and rv >= gate['hi']: continue
            if d == 'bear' and rv <= gate['lo']: continue
        S = abs(entry - stop); stop_frac = S / abs(entry) if entry else 0
        if stop_frac < MIN_STOP_REL:  # deployed floor
            last = i + WALK; continue
        target = entry + S if d == 'bull' else entry - S

        # ----- entry models -----
        # LIMIT (pullback) — today's model
        fj = limit_fill(m15, i, entry, d)
        lim = walk(m15, fj + 1, entry, stop, target, d) if fj is not None else None
        # STOP (continuation) — buy-stop above creator high / sell-stop below its low
        ctrig = m15[i]['h'] if d == 'bull' else m15[i]['l']
        cstop = _find_struct_low(bos, prom) if d == 'bull' else _find_struct_high(bos, prom)
        sj = stop_fill(m15, i, ctrig, d)
        con = None
        if sj is not None and ((d == 'bull' and cstop < ctrig) or (d == 'bear' and cstop > ctrig)):
            cR = abs(ctrig - cstop)
            ctgt = ctrig + cR if d == 'bull' else ctrig - cR
            con = walk(m15, sj + 1, ctrig, cstop, ctgt, d)
        # MARKET (next open)
        mkt = None
        if i + 1 < len(m15):
            mo = m15[i + 1]['o']; mR = (mo - stop) if d == 'bull' else (stop - mo)
            if mR > 0:
                mtg = mo + mR if d == 'bull' else mo - mR
                mkt = walk(m15, i + 2, mo, stop, mtg, d)

        out.append({
            'cls': cls, 'lim': lim, 'con': con, 'mkt': mkt,
            'rsi': rv, 'stop_frac': stop_frac,
            'atr_rel': (a20 / abs(entry)) if a20 and entry else None,
            'fill_bar': (fj - i) if fj is not None else None,
        })
        last = i + 1


def wr(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def line(label, seq):
    n, w, e = wr(seq)
    print(f"  {label:<26} n={n:>4}  WR={w:>5.1f}%  exp={e:>+.3f}R")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    rows = []
    for p in [x for x in PAIR_CLASS if x in pairs]: collect(pairs[p], p, rows)
    live = [r for r in rows if r['cls'] in LIVE]
    print(f"4/4 comm+crypto setups (deployed floor applied): {len(live)}\n")

    print("TEST A — ENTRY MODEL (the core lever):")
    line("LIMIT (pullback, today)", [r['lim'] for r in live])
    line("STOP (continuation)", [r['con'] for r in live])
    line("MARKET (next open)", [r['mkt'] for r in live])

    print("\nTEST A2 — continuation entry, per class:")
    for c in LIVE:
        line(f"STOP {c}", [r['con'] for r in live if r['cls'] == c])

    print("\nTEST B — base LIMIT WR by h1 RSI band (bull+bear folded to 'with-trend distance'):")
    for lo, hi in [(0, 30), (30, 45), (45, 55), (55, 70), (70, 100)]:
        seq = [r['lim'] for r in live if r['rsi'] is not None and lo <= r['rsi'] < hi]
        line(f"RSI [{lo},{hi})", seq)

    print("\nTEST C — base LIMIT WR by fill speed (bars to fill):")
    for fb in (1, 2, 3):
        line(f"filled on bar +{fb}", [r['lim'] for r in live if r['fill_bar'] == fb])

    print("\nTEST D — base LIMIT WR by volatility regime (m15 ATR as % of price):")
    vals = sorted(r['atr_rel'] for r in live if r['atr_rel'])
    if vals:
        q1, q2, q3 = vals[len(vals)//4], vals[len(vals)//2], vals[3*len(vals)//4]
        buckets = [('very low', 0, q1), ('low', q1, q2), ('high', q2, q3), ('very high', q3, 9)]
        for name, a, b in buckets:
            line(f"ATR {name}", [r['lim'] for r in live if r['atr_rel'] and a <= r['atr_rel'] < b])

    print("\nTEST E — continuation (STOP) entry sliced the same way (if A shows promise):")
    for lo, hi in [(0, 45), (45, 55), (55, 100)]:
        seq = [r['con'] for r in live if r['rsi'] is not None and lo <= r['rsi'] < hi]
        line(f"STOP · RSI [{lo},{hi})", seq)


if __name__ == '__main__':
    main()
