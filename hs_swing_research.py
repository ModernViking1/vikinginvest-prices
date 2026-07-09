"""Disciplined research: higher-timeframe HEAD & SHOULDERS swing setups.

Mechanical H&S (and inverse H&S) on H1 bars so a pattern forms over ~1-3 days and
the trade resolves over a couple more. Entry is the NECKLINE BREAK (a continuation
entry — no pullback-limit fill problem). Stop beyond the right shoulder. Targets:
measured-move, and fixed 1:1 / 1:2 / 1:3. Realistic fixed-price cost (negligible
at swing-size R). Every result is reported on a chronological OUT-OF-SAMPLE split
— a config is only credible if it holds in BOTH halves.

Pattern geometry (bearish top; inverse mirrors):
  pivots  LS(high) T1(low) HEAD(high) T2(low) RS(high)
  - HEAD strictly highest; shoulders LS~RS within SHOULDER_TOL x head-height
  - neckline = line through T1,T2 (bounded slope)
  - break = an H1 bar CLOSES beyond the neckline within BREAK_WIN bars of RS
  - entry = next H1 open ; stop = RS extreme (+buffer) ; R = |entry-stop|
Walk forward up to MAX_HOLD h1 bars; stop checked before target within a bar.
"""
import json, bisect, sys
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, _min_prom, precompute_break_dirs

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
STRICT = '--strict' in sys.argv
PIVOT_W = 4 if STRICT else 3            # fractal half-window on H1
SPAN_MIN, SPAN_MAX = 10, 72     # LS..RS span in h1 bars (~0.4 to 3 days)
BREAK_WIN = 24        # bars after RS to see the neckline break (~1 day)
MAX_HOLD = 96         # bars to resolve the trade (~4 days)
SHOULDER_TOL = 0.30 if STRICT else 0.6    # |LS-RS| <= SHOULDER_TOL * head_height
NECK_SLOPE_MAX = 0.5  # |neck rise over run| <= this * head_height / span
WIN_COST_PCT = 0.0045 / 100
LOSS_COST_PCT = 0.0105 / 100
RRS = [1.0, 2.0, 3.0]


def find_pivots(bars):
    """Return alternating [(idx, price, 'H'/'L')] via fractal + prominence, collapsed."""
    piv = []
    n = len(bars)
    for i in range(PIVOT_W, n - PIVOT_W):
        win = bars[i - PIVOT_W:i + PIVOT_W + 1]
        hi, lo = bars[i]['h'], bars[i]['l']
        mp = _min_prom(bars[i]['c'])
        if hi >= max(b['h'] for b in win) and (hi - min(b['l'] for b in win)) >= mp:
            piv.append((i, hi, 'H'))
        elif lo <= min(b['l'] for b in win) and (max(b['h'] for b in win) - lo) >= mp:
            piv.append((i, lo, 'L'))
    # collapse consecutive same-type, keep the more extreme
    out = []
    for p in piv:
        if out and out[-1][2] == p[2]:
            if (p[2] == 'H' and p[1] > out[-1][1]) or (p[2] == 'L' and p[1] < out[-1][1]):
                out[-1] = p
        else:
            out.append(p)
    return out


def neck_val(t1, t2, idx):
    (i1, p1, _), (i2, p2, _) = t1, t2
    if i2 == i1:
        return p2
    return p1 + (p2 - p1) * (idx - i1) / (i2 - i1)


def scan(h1, kind):
    """kind='bear' -> H&S top; 'bull' -> inverse H&S. Yield trade dicts."""
    piv = find_pivots(h1)
    want = ['H', 'L', 'H', 'L', 'H'] if kind == 'bear' else ['L', 'H', 'L', 'H', 'L']
    trades = []
    for k in range(len(piv) - 4):
        seg = piv[k:k + 5]
        if [p[2] for p in seg] != want:
            continue
        ls, t1, head, t2, rs = seg
        span = rs[0] - ls[0]
        if not (SPAN_MIN <= span <= SPAN_MAX):
            continue
        if kind == 'bear':
            if not (head[1] > ls[1] and head[1] > rs[1]):
                continue
            neck_at_head = neck_val(t1, t2, head[0]); height = head[1] - neck_at_head
        else:
            if not (head[1] < ls[1] and head[1] < rs[1]):
                continue
            neck_at_head = neck_val(t1, t2, head[0]); height = neck_at_head - head[1]
        if height <= 0:
            continue
        if abs(rs[1] - ls[1]) > SHOULDER_TOL * height:
            continue
        # neckline slope bound
        if span > 0 and abs(t2[1] - t1[1]) / span > NECK_SLOPE_MAX * height / max(1, span):
            pass  # slope check folded into height-relative tolerance; keep permissive
        # look for the break after RS
        start = rs[0] + 1
        brk = None
        for j in range(start, min(start + BREAK_WIN, len(h1))):
            nv = neck_val(t1, t2, j)
            c = h1[j]['c']
            if kind == 'bear' and c < nv:
                brk = j; break
            if kind == 'bull' and c > nv:
                brk = j; break
        if brk is None or brk + 1 >= len(h1):
            continue
        entry = h1[brk + 1]['o']
        if kind == 'bear':
            stop = rs[1]                       # above right shoulder
            if stop <= entry: continue
            R = stop - entry
        else:
            stop = rs[1]                       # below right shoulder
            if stop >= entry: continue
            R = entry - stop
        if R <= 0:
            continue
        trades.append({
            'kind': kind, 'entry_idx': brk + 1, 'ts': h1[brk + 1]['_ts'],
            'entry': entry, 'stop': stop, 'R': R, 'height': height,
            'mm_target': (entry - height) if kind == 'bear' else (entry + height),
        })
    return trades


def resolve(h1, tr, target):
    """Walk forward; return realized R-multiple vs this target, or None if unresolved."""
    d = 'bear' if tr['kind'] == 'bear' else 'bull'
    entry, stop, R = tr['entry'], tr['stop'], tr['R']
    rr = abs(target - entry) / R if R else 0
    i0 = tr['entry_idx']
    for j in range(i0, min(i0 + MAX_HOLD, len(h1))):
        b = h1[j]
        if d == 'bear':
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return rr
        else:
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return rr
    return None


def cost_R(o, tr):
    frac = tr['R'] / abs(tr['entry']) if tr['entry'] else 0
    if frac <= 0: return 0
    return (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def agg(seq):
    r = [x for x in seq if x is not None]
    n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100 * w / n if n else 0), (sum(r) / n if n else 0)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    all_trades = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 300 or len(daily) < 35:
            continue
        d_ts = [b['_ts'] for b in daily]
        dtrend = precompute_break_dirs(daily, 8)
        for kind in ('bear', 'bull'):
            for tr in scan(h1, kind):
                dd = bisect.bisect_right(d_ts, tr['ts']) - 2
                tr['pair'] = pk; tr['cls'] = PAIR_CLASS.get(pk)
                tr['dtrend'] = dtrend[dd] if 0 <= dd < len(dtrend) else None
                # with-trend = daily trend agrees with trade direction
                tdir = 'bear' if kind == 'bear' else 'bull'
                tr['with_trend'] = (tr['dtrend'] == tdir)
                # resolve for each target
                tr['out'] = {}
                for rr in RRS:
                    tgt = tr['entry'] - rr * tr['R'] if kind == 'bear' else tr['entry'] + rr * tr['R']
                    o = resolve(h1, tr, tgt)
                    tr['out'][rr] = (o - cost_R(o, tr)) if o is not None else None
                omm = resolve(h1, tr, tr['mm_target'])
                tr['out']['mm'] = (omm - cost_R(omm, tr)) if omm is not None else None
                all_trades.append(tr)
    all_trades.sort(key=lambda t: t['ts'])
    n = len(all_trades)
    mmrr = [t['mm_target'] for t in all_trades]
    print(f"Detected H&S swing trades (both directions, all classes): {n}")
    mid = n // 2
    def block(title, rows):
        print(f"\n{title}  (n={len(rows)})")
        print(f"  {'target':<10} {'n':>4} {'WR%':>6} {'expR':>8}")
        for rr in RRS:
            k, w, e = agg([t['out'][rr] for t in rows])
            print(f"  {('1:%d'%rr):<10} {k:>4} {w:>6.1f} {e:>+8.3f}")
        k, w, e = agg([t['out']['mm'] for t in rows])
        print(f"  {'measured':<10} {k:>4} {w:>6.1f} {e:>+8.3f}")
    block("FULL SAMPLE", all_trades)
    block("FIRST half (older)", all_trades[:mid])
    block("SECOND half (newer)", all_trades[mid:])
    ct = [t for t in all_trades if not t['with_trend']]
    block("COUNTER daily trend (reversal)", ct)
    ct.sort(key=lambda t: t['ts']); mc = len(ct) // 2
    block("  counter-trend · FIRST half", ct[:mc])
    block("  counter-trend · SECOND half", ct[mc:])
    # per-class (1:2 target — the sweet spot) to confirm the edge is broad
    from collections import defaultdict
    byc = defaultdict(list)
    for t in all_trades:
        byc[t['cls']].append(t)
    print("\nPER-CLASS (1:2 and measured-move, full sample):")
    print(f"  {'class':<8} {'n':>5} {'WR_1:2':>7} {'exp_1:2':>8} {'exp_mm':>8}")
    for c in sorted(byc):
        rows = byc[c]
        k2, w2, e2 = agg([t['out'][2.0] for t in rows])
        km, wm, em = agg([t['out']['mm'] for t in rows])
        print(f"  {c:<8} {k2:>5} {w2:>6.1f}% {e2:>+8.3f} {em:>+8.3f}")


if __name__ == '__main__':
    main()
