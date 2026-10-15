"""Can Deni's (Denislav Dantev) WINNING parameters rescue our mechanical fib?

Baseline dantev_fib_research.py is negative on every tf/target/class — the
52.5% WR / PF 2.39 published record does NOT reproduce mechanically. Deni's
300-trade spreadsheet (66% WR, +1.5R) is discretionary; the question is whether
the parameters that characterise his winners, applied as mechanical FILTERS,
flip our golden-zone scan positive. The parameters his winners share that our
scan ignores:

  (T) TREND ALIGNMENT  — the 3SMMA(50/100/200) stack on his charts; he only
      takes golden-pocket pullbacks WITH the dominant trend. Our scan fires on
      every zigzag leg, counter-trend included. Gate: SMA50>SMA100>SMA200 (bull)
      / <(<) (bear) at entry, trade direction must agree.
  (S) SESSION  — his spreadsheet expR peaks 12-20h UTC (London/NY). Gate the
      intraday entry hour to that window.
  (R) BIGGER TARGET — his winners average ~2.8R (fib -0.272/-0.618 extensions),
      bigger than our RR2. Tested as RR2.5 / RR3 to see if trend-filtered legs
      can carry a larger target.

Only FILTERS are applied; the entry/stop geometry is the same golden-zone scan.
PASS = exp>0 AND both OOS halves>0 AND n>=40.

    python dantev_winner_overlay.py
"""
import json, os
from collections import defaultdict
from datetime import datetime, timezone
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, cost
from dantev_fib_research import zigzag, walk_to, HIST, PRD, Z_HI, Z_LO, STOP_LVL, ATR_BUF, BREAK_WIN, COOLDOWN, HOLD

EXTS = {'127%': 1.272, '162%': 1.618}
FIXED_RR = {'RR2': 2.0, 'RR2.5': 2.5, 'RR3': 3.0}
SESSION_UTC = range(12, 20)        # London/NY window from his spreadsheet


def smas(bars, periods=(50, 100, 200)):
    """Return dict period->list of SMA values (None until warm)."""
    closes = [b['c'] for b in bars]; n = len(closes)
    out = {}
    for p in periods:
        arr = [None]*n
        s = 0.0
        for i in range(n):
            s += closes[i]
            if i >= p: s -= closes[i-p]
            if i >= p-1: arr[i] = s/p
        out[p] = arr
    return out


def scan(bars, tf, store, cls, store_cls):
    piv = zigzag(bars, PRD); n = len(bars); hold = HOLD[tf]; last = -1
    sm = smas(bars)
    s50, s100, s200 = sm[50], sm[100], sm[200]
    for k in range(1, len(piv)):
        p0, p1 = piv[k-1], piv[k]
        if p0[2] == 'L' and p1[2] == 'H':
            d = 'bull'; L, H = p0[1], p1[1]
        elif p0[2] == 'H' and p1[2] == 'L':
            d = 'bear'; H, L = p0[1], p1[1]
        else:
            continue
        rng = H - L
        if rng <= 0: continue
        if d == 'bull':
            zhi = H - Z_HI*rng; zlo = H - Z_LO*rng; void = H - STOP_LVL*rng
        else:
            zlo = L + Z_HI*rng; zhi = L + Z_LO*rng; void = L + STOP_LVL*rng
        start = p1[0] + PRD + 1; ei = None
        for j in range(start, min(start + BREAK_WIN, n - 1)):
            b = bars[j]
            if d == 'bull':
                if b['l'] < void: break
                if b['l'] <= zhi and b['c'] > b['o'] and b['c'] > zlo:
                    ei = j + 1; break
            else:
                if b['h'] > void: break
                if b['h'] >= zlo and b['c'] < b['o'] and b['c'] < zhi:
                    ei = j + 1; break
        if ei is None or ei <= last or ei >= n: continue
        entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
        stop = (void - ATR_BUF*a) if d == 'bull' else (void + ATR_BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        R = abs(entry - stop); ts = bars[ei]['_ts']
        # ---- filter flags ----
        i = ei - 1
        trend_ok = False
        if s200[i] is not None:
            if d == 'bull': trend_ok = s50[i] > s100[i] > s200[i]
            else: trend_ok = s50[i] < s100[i] < s200[i]
        hr = datetime.fromtimestamp(ts/1000, timezone.utc).hour
        sess_ok = hr in SESSION_UTC
        flags = {'base': True, 'trend': trend_ok, 'sess': sess_ok,
                 'trend+sess': trend_ok and sess_ok}
        targets = {name: ((L + ext*rng) if d == 'bull' else (H - ext*rng)) for name, ext in EXTS.items()}
        for name, rr in FIXED_RR.items():
            targets[name] = (entry + rr*R) if d == 'bull' else (entry - rr*R)
        for name, target in targets.items():
            if (d == 'bull' and target <= entry) or (d == 'bear' and target >= entry): continue
            o = walk_to(bars, ei, entry, stop, target, d, hold)
            if o is None: continue
            net = o - cost(o, entry, R)
            for fl, ok in flags.items():
                if ok:
                    store[(tf, name, fl)].append((ts, net))
                    store_cls[cls][(tf, name, fl)].append((ts, net))
        last = ei + COOLDOWN


def stats(rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n = len(seq)
    if not n: return 0,0,0,0,0,0
    w = sum(1 for x in seq if x > 0); wr = 100*w/n; exp = sum(seq)/n
    gw = sum(x for x in seq if x > 0); gl = -sum(x for x in seq if x < 0)
    pf = (gw/gl) if gl > 0 else float('inf')
    mid = n//2
    eh = (sum(seq[:mid])/mid) if mid else 0
    es = (sum(seq[mid:])/(n-mid)) if n-mid else 0
    return n, wr, exp, pf, eh, es


def line(label, rows, only_pass=False):
    n, wr, exp, pf, eh, es = stats(rows)
    v = 'PASS' if (exp > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    if only_pass and v != 'PASS': return
    print(f"  {label:<26} n={n:>4} WR={wr:>5.1f}% exp={exp:>+7.3f}R PF={pf:>4.2f} OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        npairs += 1
        for tf, bars in {'daily': daily, '4h': agg4h(h1), 'h1': h1}.items():
            if len(bars) < 250: continue
            scan(bars, tf, store, cls, store_cls)

    tgts = list(EXTS) + list(FIXED_RR)
    fls = ['base', 'trend', 'sess', 'trend+sess']
    print(f"Deni winning-parameter overlay on the golden-zone fib — {npairs} pairs, OOS, realistic cost")
    print("Filters: trend=SMA50>100>200 align, sess=12-20h UTC, +bigger RR targets\n")
    print("=== ALL PAIRS (session filter only applies to intraday) ===")
    for tf in ('daily', '4h', 'h1'):
        for name in tgts:
            for fl in fls:
                if fl in ('sess', 'trend+sess') and tf == 'daily': continue
                line(f"{tf} {name} [{fl}]", store[(tf, name, fl)])
        print()

    print("=== FX classes only (his universe: major/minor/yen) ===")
    for c in ['major', 'minor']:
        print(f"--- {c} ---")
        for tf in ('4h', 'h1'):
            for name in tgts:
                for fl in fls:
                    line(f"{tf} {name} [{fl}]", store_cls[c][(tf, name, fl)])
        print()

    print("=== ONLY PASSING cohorts across every class ===")
    for c in ['comm','crypto','index','major','minor']:
        for tf in ('daily','4h','h1'):
            for name in tgts:
                for fl in fls:
                    if fl in ('sess','trend+sess') and tf=='daily': continue
                    line(f"{c} {tf} {name} [{fl}]", store_cls[c][(tf, name, fl)], only_pass=True)


if __name__ == '__main__':
    main()
