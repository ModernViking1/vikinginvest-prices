"""Opening-gap feasibility screen for the equity book — is the overnight close->open move a
real, ORTHOGONAL edge worth a demo-first pilot, or noise?

None of the 9 live .EQ strategies key on the session open; they're intraday trend/reversal/
breakout on a continuous bar stream. A gap strategy enters at the FIRST bar of the RTH session
on the overnight gap, so it is structurally orthogonal by construction (different trigger clock).

Tested on broker m15 (execution) + daily (SMA200 trend filter, same as the rest of the book, and
ATR for gap sizing / stop unit). Two direction logics, size buckets, trend slices:
  - GO   (continuation): gap up -> long / gap down -> short; trailing-runner exit (the book's native
                         exit), horizon HOLD_GO m15 bars.
  - FADE (reversion):    gap up -> short toward the prior close (the "fill") / gap down -> long;
                         INTRADAY only (target = prior close, stop = open +/- R, else MTM at session close).
Stop unit R = R_ATR * daily ATR(14). Scored net of costs. Resolved-only, both-OOS halves reported.

  python gap_research.py --hist equity-ohlc.json
"""
import argparse, json, bisect
from collections import defaultdict
from datetime import datetime

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, cost
from unified_shadow_harness import score_trail_open, TRAIL_ARM, TRAIL_DIST

R_ATR = 0.5            # stop distance = 0.5 * daily ATR -> the R unit (comparable across variants)
MIN_GAP = 0.25         # ignore gaps < 0.25 ATR (noise)
HOLD_GO = 52           # GO trailing horizon in m15 bars (~2 RTH sessions)
BUCKETS = [('sm', 0.25, 0.6), ('md', 0.6, 1.2), ('lg', 1.2, 99.0)]


def _sma(closes, p):
    n = len(closes); out = [None]*n; s = 0.0
    for i in range(n):
        s += closes[i]
        if i >= p: s -= closes[i-p]
        if i >= p-1: out[i] = s/p
    return out


def _day(t):
    return t[:10]


def h1_to_daily(h1):
    """Synthesize an RTH daily layer from h1 bars (broker exports carry only h1+m15, no daily).
    Group by calendar date: open=first, high=max, low=min, close=last. Timestamp = date 00:00:00
    so the causal daily lookup (strictly-before the session date) still works."""
    out = []; i = 0; n = len(h1)
    while i < n:
        d = _day(h1[i]['t']); j = i
        hi = h1[i]['h']; lo = h1[i]['l']
        while j < n and _day(h1[j]['t']) == d:
            hi = max(hi, h1[j]['h']); lo = min(lo, h1[j]['l']); j += 1
        out.append({'o': h1[i]['o'], 'h': hi, 'l': lo, 'c': h1[j-1]['c'], 'v': 0,
                    't': d, '_ts': datetime.fromisoformat(d + ' 00:00:00').timestamp()})
        i = j
    return out


def sessions(m15):
    """Group m15 bars into RTH sessions by calendar date. Returns list of
    (date, open_idx, open_price, prior_close, end_idx_exclusive)."""
    out = []; i = 0; n = len(m15); prev_close = None
    while i < n:
        d = _day(m15[i]['t']); j = i
        while j < n and _day(m15[j]['t']) == d:
            j += 1
        if prev_close is not None:
            out.append((d, i, m15[i]['o'], prev_close, j))
        prev_close = m15[j-1]['c']
        i = j
    return out


def fade_walk(m15, i0, i_end, entry, stop, target, d):
    """Intraday fade: within [i0, i_end) does target (fill) or stop hit first? Pessimistic on
    same-bar ambiguity (assume stop). If neither, mark-to-market at session close. Returns R."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, i_end):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return (target - entry) / R
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return (entry - target) / R
    lc = m15[i_end-1]['c']
    return ((lc - entry) if d == 'bull' else (entry - lc)) / R


def agg(rows):
    rows = sorted(rows); rs = [r for _, r in rows]; n = len(rs)
    if not n:
        return None
    m = n//2; w = sum(1 for r in rs if r > 0)
    return n, 100*w/n, sum(rs)/n, sum(rs), (sum(rs[:m])/m if m else 0), (sum(rs[m:])/(n-m) if n-m else 0)


def bucket(g_atr):
    for name, lo, hi in BUCKETS:
        if lo <= g_atr < hi:
            return name
    return None


def run(args):
    data = json.load(open(args.hist)).get('pairs', {})
    # store keyed (logic, bucket, trend_slice) -> rows; trend_slice in {'all','align','counter'}
    store = defaultdict(list)
    n_sym = 0; n_gaps = 0
    for key, layers in data.items():
        daily = _bars_norm(layers.get('daily') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        h1 = _bars_norm(layers.get('h1') or [])
        if len(daily) < 210 and len(h1) >= 1200:   # broker export: no daily layer -> build from h1
            daily = h1_to_daily(h1)
        if len(daily) < 210 or len(m15) < 300:
            continue
        n_sym += 1
        dcloses = [b['c'] for b in daily]
        dsma = _sma(dcloses, 200)
        dts = [b['_ts'] for b in daily]
        for (d_date, i0, op, prev_close, i_end) in sessions(m15):
            # causal daily index = last daily bar strictly before this session date
            ts0 = datetime.fromisoformat(d_date + ' 00:00:00').timestamp()
            di = bisect.bisect_left(dts, ts0) - 1
            if di < 200 or dsma[di] is None:
                continue
            a = atr(daily, 14, di)
            if not a or a <= 0:
                continue
            gap = op - prev_close
            g_atr = abs(gap) / a
            if g_atr < MIN_GAP:
                continue
            bk = bucket(g_atr)
            if bk is None:
                continue
            n_gaps += 1
            up = gap > 0
            R = R_ATR * a
            trend_bull = dcloses[di] > dsma[di]
            entry_ts = m15[i0]['_ts']
            # ---- GO (continuation) ----
            gdir = 'bull' if up else 'bear'
            gstop = op - R if up else op + R
            st, o = score_trail_open(m15, entry_ts, op, gstop, gdir, HOLD_GO, TRAIL_ARM, TRAIL_DIST)
            if st == 'resolved':
                net = o - cost(o, op, R)
                align = (gdir == 'bull') == trend_bull
                for sl in ('all', 'align' if align else 'counter'):
                    store[('go', bk, sl)].append((entry_ts, net))
                    store[('go', 'ALL', sl)].append((entry_ts, net))
            # ---- FADE (reversion to the prior close) ----
            fdir = 'bear' if up else 'bull'
            fstop = op + R if up else op - R
            o2 = fade_walk(m15, i0, i_end, op, fstop, prev_close, fdir)
            if o2 is not None:
                net2 = o2 - cost(o2, op, R)
                align = (fdir == 'bull') == trend_bull
                for sl in ('all', 'align' if align else 'counter'):
                    store[('fade', bk, sl)].append((entry_ts, net2))
                    store[('fade', 'ALL', sl)].append((entry_ts, net2))

    print(f"opening-gap screen — {n_sym} symbols, {n_gaps} qualifying gaps "
          f"(>= {MIN_GAP} ATR). R unit = {R_ATR}*dailyATR. GO=trailing/{HOLD_GO}m15, FADE=intraday.\n")
    for logic in ('go', 'fade'):
        print(f"===== {logic.upper()} =====")
        print(f"  {'bucket':<7}{'trend':<9}{'n':>7}{'WR%':>7}{'expR':>9}{'totR':>8}{'OOS1':>8}{'OOS2':>8}")
        for bk in ['ALL'] + [b[0] for b in BUCKETS]:
            for sl in ('all', 'align', 'counter'):
                a = agg(store[(logic, bk, sl)])
                if not a:
                    continue
                flag = '  <<' if (a[0] >= 40 and a[2] > 0.05 and a[4] > 0 and a[5] > 0) else ''
                print(f"  {bk:<7}{sl:<9}{a[0]:>7}{a[1]:>6.0f}%{a[2]:>+9.3f}{a[3]:>+8.0f}{a[4]:>+8.2f}{a[5]:>+8.2f}{flag}")
        print()
    print("'<<' = n>=40, exp>+0.05R, BOTH OOS halves positive (the durability bar the book uses).")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='equity-ohlc.json')
    run(ap.parse_args())
