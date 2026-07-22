"""Swept-extreme reversal + counter-trendline-break trigger, reverting to the
opposite swing (user screenshots, daylotrading).

SHORT: price sweeps ABOVE a previous swing high (higher-high / liquidity grab
above prior structure). The rally into that high rode an ascending trendline;
when price BREAKS BACK DOWN through it (proxied here as a close below the last
higher-low of the rally) -> SELL. Stop above the swept high. Target = the
PREVIOUS SWING LOW (the base the rally launched from) — a large structural,
variable-RR target.

LONG (mirror): sweep BELOW a previous swing low, break UP through the descending
trendline (close above the last lower-high) -> BUY. Stop below the swept low.
Target = the previous swing high.

Multi-TF in spirit (HTF structure + LTF trigger) is encoded on one series via
swing pivots confirmed k bars out (decisions only act AFTER confirmation — no
lookahead). Realistic next-bar-open fills, ATR-buffered structural stop, fixed
cost, chronological OOS split (both halves must be +), per class, h1/4h/daily.
Structural target is variable-RR; a capped-RR2 variant is shown alongside so the
"does the immediate reversal have any edge" question is separable from "does the
full swing-to-swing target get reached".

Run: python sweep_trendline_revert_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
BUF = 0.15          # stop buffer beyond the swept extreme, in ATR
TRIG_WIN = 15       # bars after the sweep peak to wait for the trendline break
COOLDOWN = 4
HOLD = {'h1': 120, '4h': 90, 'daily': 40}   # structural targets need room
RR_CAP = 2.0        # capped-RR comparison variant


def pivots(bars, k):
    n = len(bars); ph = [False]*n; pl = [False]*n
    for i in range(k, n-k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)): ph[i] = True
        if all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)): pl[i] = True
    return ph, pl


def walk_to_target(bars, i0, entry, stop, target, d, hold):
    """Realistic bar walk to a FIXED-PRICE target. Returns (realized_R, rr_avail)
    where rr_avail = structural reward:risk on offer. Timeout marks to close."""
    R = abs(entry - stop)
    if R <= 0: return None
    rr_avail = abs(target - entry) / R
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return (-1.0, rr_avail)
            if b['h'] >= target: return (rr_avail, rr_avail)
        else:
            if b['h'] >= stop: return (-1.0, rr_avail)
            if b['l'] <= target: return (rr_avail, rr_avail)
    if end <= i0: return None
    c = bars[end-1]['c']
    r = (c - entry)/R if d == 'bull' else (entry - c)/R
    return (max(-1.0, r), rr_avail)


def walk_capped(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0: return None
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def scan(bars, tf, k, store, store_cap, cls, store_cls, store_cls_cap, rr_avail_acc):
    n = len(bars)
    if n < 2*k + 30: return
    ph, pl = pivots(bars, k)
    ph_idx = [i for i in range(n) if ph[i]]
    pl_idx = [i for i in range(n) if pl[i]]
    last = -1

    # ---- SHORTS: sweep above prior swing high, break the rally's last HL ----
    for bi in range(1, len(ph_idx)):
        idxB = ph_idx[bi]; idxA = ph_idx[bi-1]
        if bars[idxB]['h'] <= bars[idxA]['h']:      # must be a HIGHER high (swept A)
            continue
        # previous swing low = deepest low of the leg A..B (rally base) -> target
        seg = bars[idxA:idxB+1]
        if len(seg) < 3: continue
        tgt_low = min(b['l'] for b in seg)
        # last higher-low before the peak = trendline-break trigger level
        hls = [j for j in pl_idx if idxA < j < idxB]
        if not hls: continue
        hl = hls[-1]; hl_lvl = bars[hl]['l']
        if hl_lvl <= tgt_low: continue              # need room between trigger and target
        confirm = idxB + k                          # peak confirmed here (no lookahead)
        t = None
        for j in range(max(confirm, hl + k + 1), min(idxB + TRIG_WIN, n-1)):
            if j <= last: break
            if bars[j]['c'] < hl_lvl:                # broke down through the rally structure
                t = j; break
            if bars[j]['h'] > bars[idxB]['h'] + 0.0: # peak extended further -> chase the new high
                idxB_h = bars[idxB]['h']             # (keep original swept high as stop ref)
        if t is None or t + 1 >= n: continue
        ei = t + 1; entry = bars[ei]['o']; a = atr(bars, 14, t) or 0.0
        stop = bars[idxB]['h'] + BUF*a
        if stop <= entry or tgt_low >= entry: continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        res = walk_to_target(bars, ei, entry, stop, tgt_low, 'bear', HOLD[tf])
        if res is not None:
            o, rr_avail = res
            store[(tf,)].append((ts, o - cost(o, entry, R)))
            store_cls[cls][(tf,)].append((ts, o - cost(o, entry, R)))
            rr_avail_acc[tf].append(rr_avail)
        oc = walk_capped(bars, ei, entry, stop, 'bear', RR_CAP, HOLD[tf])
        if oc is not None:
            store_cap[(tf,)].append((ts, oc - cost(oc, entry, R)))
            store_cls_cap[cls][(tf,)].append((ts, oc - cost(oc, entry, R)))
        last = ei + COOLDOWN

    # ---- LONGS: sweep below prior swing low, break the fall's last LH ----
    last = -1
    for bi in range(1, len(pl_idx)):
        idxB = pl_idx[bi]; idxA = pl_idx[bi-1]
        if bars[idxB]['l'] >= bars[idxA]['l']:       # must be a LOWER low (swept A)
            continue
        seg = bars[idxA:idxB+1]
        if len(seg) < 3: continue
        tgt_high = max(b['h'] for b in seg)
        lhs = [j for j in ph_idx if idxA < j < idxB]
        if not lhs: continue
        lh = lhs[-1]; lh_lvl = bars[lh]['h']
        if lh_lvl >= tgt_high: continue
        confirm = idxB + k
        t = None
        for j in range(max(confirm, lh + k + 1), min(idxB + TRIG_WIN, n-1)):
            if j <= last: break
            if bars[j]['c'] > lh_lvl:
                t = j; break
        if t is None or t + 1 >= n: continue
        ei = t + 1; entry = bars[ei]['o']; a = atr(bars, 14, t) or 0.0
        stop = bars[idxB]['l'] - BUF*a
        if stop >= entry or tgt_high <= entry: continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        res = walk_to_target(bars, ei, entry, stop, tgt_high, 'bull', HOLD[tf])
        if res is not None:
            o, rr_avail = res
            store[(tf,)].append((ts, o - cost(o, entry, R)))
            store_cls[cls][(tf,)].append((ts, o - cost(o, entry, R)))
            rr_avail_acc[tf].append(rr_avail)
        oc = walk_capped(bars, ei, entry, stop, 'bull', RR_CAP, HOLD[tf])
        if oc is not None:
            store_cap[(tf,)].append((ts, oc - cost(oc, entry, R)))
            store_cls_cap[cls][(tf,)].append((ts, oc - cost(oc, entry, R)))
        last = ei + COOLDOWN


def line(label, rows, rr_avail=None):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    extra = ''
    if rr_avail:
        srt = sorted(rr_avail); med = srt[len(srt)//2] if srt else 0
        extra = f" medRR={med:>4.1f}"
    print(f"  {label:<12} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}]{extra} {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    for k in (2, 3, 4):
        store = defaultdict(list); store_cap = defaultdict(list)
        store_cls = defaultdict(lambda: defaultdict(list)); store_cls_cap = defaultdict(lambda: defaultdict(list))
        rr_acc = defaultdict(list); npairs = 0
        for pk in [x for x in PAIR_CLASS if x in pairs]:
            cls = PAIR_CLASS.get(pk)
            h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
            if len(h1) < 400 or len(daily) < 80: continue
            npairs += 1
            for tf, bars in {'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
                if len(bars) < 150: continue
                scan(bars, tf, k, store, store_cap, cls, store_cls, store_cls_cap, rr_acc)

        print(f"\n===== pivot k={k} — {npairs} pairs — sweep+trendline-break revert =====")
        print("STRUCTURAL target (previous opposite swing, variable RR):")
        for tf in ('4h', 'h1', 'daily'):
            line(f"{tf} struct", store[(tf,)], rr_acc[tf])
        print(f"CAPPED target (RR{RR_CAP}, breakeven WR {100/(1+RR_CAP):.0f}%):")
        for tf in ('4h', 'h1', 'daily'):
            line(f"{tf} cap", store_cap[(tf,)])
        print("STRUCTURAL per class (4H):")
        for c in ['comm', 'crypto', 'index', 'major', 'minor']:
            line(f"{c}", store_cls[c][('4h',)])


if __name__ == '__main__':
    main()
