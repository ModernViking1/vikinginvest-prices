"""Does the screenshot H&S strategy SUPPLEMENT our existing `hs` detector?

The screenshot setup is the SAME base pattern we already trade (1H head &
shoulders / inverse), so it is not an orthogonal new edge — it is two
mechanizable REFINEMENTS layered on top of the pattern we already detect:

  (A) RSI-DIVERGENCE confirmation at the head.
      bear top      : HEAD high > LS high  BUT  RSI(head) < RSI(LS)   (momentum fades)
      bull inverse  : HEAD low  < LS low   BUT  RSI(head) > RSI(LS)

  (B) FIBONACCI-RETRACE entry (0/0.5/1) instead of chasing the neckline-break
      candle (the screenshots' own note: "chasing inefficient candles").
      After the neckline breaks, anchor a fib from the swing extreme to the
      developing break-impulse extreme; enter a limit at the 0.5 retrace.
      Stop stays at the right shoulder, so the tighter entry lifts the
      R-multiple to the SAME measured-move / fixed targets. Trades that never
      retrace to 0.5 are simply missed (the cost of the method).

This reuses hs_swing_research.scan()/resolve() verbatim for the baseline so
the comparison is apples-to-apples on the identical detected patterns, over
the same 3yr historical-ohlc.json, reported full-sample + OOS halves +
per-class, exactly like the trend-filter work.

    python hs_fib_rsi_research.py            # standard pivots
    python hs_fib_rsi_research.py --strict   # tighter pivot/shoulder tol
"""
import json, bisect, sys
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, precompute_rsi
import hs_swing_research as HS
from hs_swing_research import scan, resolve, cost_R, neck_val, RRS, MAX_HOLD

RETRACE_WIN = 18     # bars after the break to find the impulse extreme + 0.5 retrace fill
RSI_PERIOD = 14


def rsi_divergence(h1, rsi, ls, head, kind):
    """Classic H&S divergence: price makes a more-extreme head, momentum does not."""
    il, ih = ls[0], head[0]
    if rsi[il] is None or rsi[ih] is None:
        return False
    if kind == 'bear':
        return head[1] > ls[1] and rsi[ih] < rsi[il]       # higher head, lower RSI
    return head[1] < ls[1] and rsi[ih] > rsi[il]            # lower head, higher RSI


def fib_retrace_entry(h1, tr, kind):
    """Re-enter the SAME pattern on a 0.5 fib retrace of the break impulse rather
    than at the next-bar open. Returns a new (entry, R, entry_idx) or None if price
    never retraces to 0.5 within RETRACE_WIN bars (trade missed). No lookahead: the
    impulse extreme is tracked bar-by-bar and the fill is the first touch of the
    current 0.5 level."""
    brk1 = tr['entry_idx']                       # the baseline break+1 bar
    swing = tr['stop']                           # RS extreme = fib 1.0 anchor (= stop)
    ext = None
    for j in range(brk1, min(brk1 + RETRACE_WIN, len(h1))):
        b = h1[j]
        if kind == 'bear':
            ext = b['l'] if ext is None else min(ext, b['l'])
            lvl = swing - 0.5 * (swing - ext)    # 0.5 retrace back UP toward the shoulder
            if b['h'] >= lvl and j > brk1:       # retrace tags the 0.5 level
                entry = lvl
                R = swing - entry
                return (entry, R, j) if R > 0 else None
        else:
            ext = b['h'] if ext is None else max(ext, b['h'])
            lvl = swing + 0.5 * (ext - swing)
            if b['l'] <= lvl and j > brk1:
                entry = lvl
                R = entry - swing
                return (entry, R, j) if R > 0 else None
    return None


def resolve_from(h1, kind, entry, stop, R, entry_idx, target):
    rr = abs(target - entry) / R if R else 0
    for j in range(entry_idx, min(entry_idx + MAX_HOLD, len(h1))):
        b = h1[j]
        if kind == 'bear':
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return rr
        else:
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return rr
    return None


def agg(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def main():
    d = json.load(open(HS.HIST)); pairs = d.get('pairs', {})
    rows = []   # one dict per detected pattern, carrying every variant's outcome
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 300 or len(daily) < 35:
            continue
        rsi = precompute_rsi([b['c'] for b in h1], RSI_PERIOD)
        # re-run the pivot scan so we can map each trade back to its LS/head pivots
        piv = HS.find_pivots(h1)
        for kind in ('bear', 'bull'):
            want = ['H','L','H','L','H'] if kind == 'bear' else ['L','H','L','H','L']
            # rebuild the exact trade list AND keep the pivot segment for RSI-div
            for k in range(len(piv) - 4):
                seg = piv[k:k+5]
                if [p[2] for p in seg] != want:
                    continue
                ls, t1, head, t2, rs = seg
                # reconstruct via scan() semantics by filtering scan()'s output on entry window
                # simpler: reuse scan() once and match by rs index
                pass
            # Use scan() for the vetted geometry, then recover LS/head by nearest pivot
            for tr in scan(h1, kind):
                # recover the LS & head pivots for this trade (rs is the pivot just before break window)
                # find the 5-pivot seg whose RS index is closest below entry_idx-1
                best = None
                for k in range(len(piv) - 4):
                    seg = piv[k:k+5]
                    if [p[2] for p in seg] != want:
                        continue
                    rs_idx = seg[4][0]
                    if rs_idx < tr['entry_idx'] and (best is None or rs_idx > best[4][0]):
                        best = seg
                if best is None:
                    continue
                ls_p, head_p = best[0], best[2]
                div = rsi_divergence(h1, rsi, ls_p, head_p, kind)
                rec = {'pair': pk, 'cls': PAIR_CLASS.get(pk), 'ts': tr['ts'],
                       'kind': kind, 'rsi_div': div}
                # baseline (neckline-break entry) outcomes at each target
                rec['base'] = {}
                for rr in RRS:
                    tgt = tr['entry'] - rr*tr['R'] if kind == 'bear' else tr['entry'] + rr*tr['R']
                    o = resolve(h1, tr, tgt)
                    rec['base'][rr] = (o - cost_R(o, tr)) if o is not None else None
                omm = resolve(h1, tr, tr['mm_target'])
                rec['base']['mm'] = (omm - cost_R(omm, tr)) if omm is not None else None
                # fib-retrace entry outcomes (same measured-move target distance)
                fib = fib_retrace_entry(h1, tr, kind)
                rec['fib'] = {}
                if fib:
                    fe, fR, fidx = fib
                    mm = (fe - tr['height']) if kind == 'bear' else (fe + tr['height'])
                    for rr in RRS:
                        tgt = fe - rr*fR if kind == 'bear' else fe + rr*fR
                        o = resolve_from(h1, kind, fe, tr['stop'], fR, fidx, tgt)
                        trc = {'R': fR, 'entry': fe}
                        rec['fib'][rr] = (o - cost_R(o, trc)) if o is not None else None
                    o = resolve_from(h1, kind, fe, tr['stop'], fR, fidx, mm)
                    rec['fib']['mm'] = (o - cost_R(o, {'R': fR, 'entry': fe})) if o is not None else None
                    rec['fib_filled'] = True
                else:
                    rec['fib_filled'] = False
                rows.append(rec)
    rows.sort(key=lambda r: r['ts'])

    def block(title, rs, src, key='mm'):
        sub = [r for r in rs if src in r and key in r[src]]
        k, w, e = agg([r[src][key] for r in sub])
        tot = sum(x for x in (r[src][key] for r in sub) if x is not None)
        print(f"  {title:<34} n={k:>4}  WR={w:>5.1f}%  expR={e:>+6.3f}  totR={tot:>+7.2f}")

    n = len(rows)
    filled = sum(1 for r in rows if r['fib_filled'])
    ndiv = sum(1 for r in rows if r['rsi_div'])
    print(f"Detected H&S patterns (both dirs, all classes): {n}")
    print(f"  RSI divergence present : {ndiv}  ({100*ndiv/max(1,n):.0f}%)")
    print(f"  Fib-0.5 retrace filled : {filled}  ({100*filled/max(1,n):.0f}%)  "
          f"[the rest never pulled back = missed]")
    mid = n // 2

    for tgt in (2.0, 'mm'):
        tl = '1:2 target' if tgt == 2.0 else 'measured-move target'
        print(f"\n===== {tl} =====")
        print("-- BASELINE  neckline-break entry, no RSI filter (= our current `hs`) --")
        block("full sample", rows, 'base', tgt)
        block("OOS first half", rows[:mid], 'base', tgt)
        block("OOS second half", rows[mid:], 'base', tgt)
        print("-- (A) + RSI-DIVERGENCE filter --")
        dv = [r for r in rows if r['rsi_div']]; dmid = len(dv)//2
        block("full sample", dv, 'base', tgt)
        block("OOS first half", dv[:dmid], 'base', tgt)
        block("OOS second half", dv[dmid:], 'base', tgt)
        print("-- (B) + FIB-0.5 retrace entry (filled only) --")
        fb = [r for r in rows if r['fib_filled']]; fmid = len(fb)//2
        block("full sample", fb, 'fib', tgt)
        block("OOS first half", fb[:fmid], 'fib', tgt)
        block("OOS second half", fb[fmid:], 'fib', tgt)
        print("-- (C) + BOTH  RSI-divergence AND fib-0.5 entry --")
        bo = [r for r in rows if r['fib_filled'] and r['rsi_div']]; bmid = len(bo)//2
        block("full sample", bo, 'fib', tgt)
        block("OOS first half", bo[:bmid], 'fib', tgt)
        block("OOS second half", bo[bmid:], 'fib', tgt)

    # per-class, measured move, baseline vs both
    from collections import defaultdict
    byc = defaultdict(list)
    for r in rows:
        byc[r['cls']].append(r)
    print("\n===== PER-CLASS (measured-move) baseline  vs  RSI+fib =====")
    print(f"  {'class':<8} {'base_n':>6} {'base_exp':>9} {'both_n':>7} {'both_exp':>9}")
    for c in sorted(byc):
        rs = byc[c]
        bk, bw, be = agg([r['base']['mm'] for r in rs])
        bo = [r for r in rs if r['fib_filled'] and r['rsi_div']]
        ok, ow, oe = agg([r['fib']['mm'] for r in bo])
        print(f"  {c:<8} {bk:>6} {be:>+9.3f} {ok:>7} {oe:>+9.3f}")


if __name__ == '__main__':
    main()
