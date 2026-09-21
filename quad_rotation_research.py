"""Recreate + backtest the HPS 'Quad Divergence' (aka Holy Grail) super-signal.

Mechanical spec (from the Futures Trade Alert Network 'Anatomy of a HPS Trade' flowchart),
long side (short is the mirror). Four stochastics on every bar:
    S1 FastStoch(9,3)   S2 FastStoch(14,3)   S3 FastStoch(40,4)   S4 FullStoch(60,10,1)
Each is the smoothed %D line; the 9-3 is the trigger band.

LONG 'quad divergence':
  1. Stage 1: a swing low L1 where ALL FOUR stochastics are < 20 (deep oversold, organized pullback).
  2. Stage 2: a later swing low L2 with price <= L1 (lower low / double bottom) BUT the 9-3 stoch
     puts in a HIGHER low than at L1 and holds above 20 -> classic Lane divergence.
  3. Trigger: the 9-3 stoch turns back up (activates the divergence). Enter at that bar's close.
  4. Stop: just under the L2 pattern low.
  5. Exit: first target = the first 9-3 rotation back ABOVE 80 (then trail; we model first target only,
     which is the conservative, testable piece). Stop-out = -1R.

Discretionary overlays in the course (channels, wedges, 1-2-3, hand-drawn trend lines, VWAP) are NOT
mechanized — this tests the objective core. NOTE the original trades 1-5 min; our data floor is m15,
so m15/h1 results are a PROXY, not the 1-min system. Research-only; prints to the log, commits nothing.
"""
import argparse
import bisect
import json
from collections import defaultdict, deque
from datetime import datetime, timezone

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS

OVERSOLD, OVERBOUGHT = 20.0, 80.0
PIV = 2                 # swing-pivot half-width (bars each side)
PAIR_LOOKBACK = 60      # max bars between L1 (stage-1) and L2 (divergent low)
TRIG_WINDOW = 8         # bars after L2 to allow the 9-3 turn-up trigger
DBL_TOL = 0.001         # L2 may be up to 0.1% above L1 and still count (double-bottom tolerance)
STOP_BUF = 0.0003       # stop a hair under the pattern low
HOLD = 200              # max bars to resolve (else excluded, bracket-honest)


def _rolling_stoch_d(bars, kp, kslow, dp):
    """Smoothed stochastic %D line. %K over kp, slowed by kslow, %D = SMA(dp). O(n) via monotonic deques."""
    n = len(bars); rawk = [50.0] * n
    lo_dq, hi_dq = deque(), deque()   # indices, increasing lows / decreasing highs
    for i in range(n):
        l, h = bars[i]['l'], bars[i]['h']
        while lo_dq and bars[lo_dq[-1]]['l'] >= l: lo_dq.pop()
        lo_dq.append(i)
        while hi_dq and bars[hi_dq[-1]]['h'] <= h: hi_dq.pop()
        hi_dq.append(i)
        lb = i - kp + 1
        while lo_dq[0] < lb: lo_dq.popleft()
        while hi_dq[0] < lb: hi_dq.popleft()
        ll, hh = bars[lo_dq[0]]['l'], bars[hi_dq[0]]['h']
        rawk[i] = 100.0 * (bars[i]['c'] - ll) / (hh - ll) if hh > ll else 50.0
    k = _sma(rawk, kslow) if kslow > 1 else rawk
    return _sma(k, dp) if dp > 1 else k


def _sma(a, n):
    if n <= 1: return list(a)
    out = [a[0]] * len(a); s = 0.0; from_i = 0
    for i in range(len(a)):
        s += a[i]
        if i - from_i + 1 > n: s -= a[from_i]; from_i += 1
        out[i] = s / (i - from_i + 1)
    return out


def _ema(vals, period):
    out = [vals[0]] * len(vals); a = 2.0 / (period + 1)
    for i in range(1, len(vals)):
        out[i] = a * vals[i] + (1 - a) * out[i - 1]
    return out


def _swing_lows(bars, piv):
    """Confirmed pivot lows: (pivot_index, confirm_index=pivot+piv). Causal at confirm_index."""
    out = []
    for i in range(piv, len(bars) - piv):
        lo = bars[i]['l']
        if all(bars[i]['l'] <= bars[i - k]['l'] and bars[i]['l'] <= bars[i + k]['l'] for k in range(1, piv + 1)):
            out.append((i, i + piv))
    return out


def _swing_highs(bars, piv):
    out = []
    for i in range(piv, len(bars) - piv):
        if all(bars[i]['h'] >= bars[i - k]['h'] and bars[i]['h'] >= bars[i + k]['h'] for k in range(1, piv + 1)):
            out.append((i, i + piv))
    return out


def detect_quad_div(bars):
    """Return list of signals: {dir, entry_ts, entry, stop, entry_i}."""
    if len(bars) < 120:
        return []
    s1 = _rolling_stoch_d(bars, 9, 1, 3)
    s2 = _rolling_stoch_d(bars, 14, 1, 3)
    s3 = _rolling_stoch_d(bars, 40, 1, 4)
    s4 = _rolling_stoch_d(bars, 60, 10, 1)
    ema200 = _ema([b['c'] for b in bars], 200)
    out = []

    # ---- LONG ----
    lows = _swing_lows(bars, PIV)
    for a in range(1, len(lows)):
        (i1, _), (i2, c2) = lows[a - 1], lows[a]
        if i2 - i1 > PAIR_LOOKBACK:
            continue
        # stage 1: all four oversold at L1
        if not (s1[i1] < OVERSOLD and s2[i1] < OVERSOLD and s3[i1] < OVERSOLD and s4[i1] < OVERSOLD):
            continue
        # price lower-low / double bottom, 9-3 higher low (divergence), 9-3 low held above ~20
        if bars[i2]['l'] > bars[i1]['l'] * (1 + DBL_TOL):
            continue
        if not (s1[i2] > s1[i1]):
            continue
        # trigger: first 9-3 turn-up after L2 (within the window), above 20
        trig = None
        for j in range(c2, min(c2 + TRIG_WINDOW, len(bars))):
            if j >= 1 and s1[j] > s1[j - 1] and s1[j] > OVERSOLD:
                trig = j; break
        if trig is None:
            continue
        entry = bars[trig]['c']
        stop = bars[i2]['l'] * (1 - STOP_BUF)
        if entry <= stop:
            continue
        out.append({'dir': 'bull', 'entry_ts': bars[trig]['_ts'], 'entry': entry, 'stop': stop,
                    'entry_i': trig, 's1': s1, 's4': s4,
                    'trend': 'up' if entry > ema200[trig] else 'down'})

    # ---- SHORT (mirror) ----
    highs = _swing_highs(bars, PIV)
    for a in range(1, len(highs)):
        (i1, _), (i2, c2) = highs[a - 1], highs[a]
        if i2 - i1 > PAIR_LOOKBACK:
            continue
        if not (s1[i1] > OVERBOUGHT and s2[i1] > OVERBOUGHT and s3[i1] > OVERBOUGHT and s4[i1] > OVERBOUGHT):
            continue
        if bars[i2]['h'] < bars[i1]['h'] * (1 - DBL_TOL):
            continue
        if not (s1[i2] < s1[i1]):
            continue
        trig = None
        for j in range(c2, min(c2 + TRIG_WINDOW, len(bars))):
            if j >= 1 and s1[j] < s1[j - 1] and s1[j] < OVERBOUGHT:
                trig = j; break
        if trig is None:
            continue
        entry = bars[trig]['c']
        stop = bars[i2]['h'] * (1 + STOP_BUF)
        if entry >= stop:
            continue
        out.append({'dir': 'bear', 'entry_ts': bars[trig]['_ts'], 'entry': entry, 'stop': stop,
                    'entry_i': trig, 's1': s1, 's4': s4,
                    'trend': 'down' if entry < ema200[trig] else 'up'})

    return out


def score(bars, sig):
    """Exit on first 9-3 rotation to the opposite band (>=80 long / <=20 short); stop = -1R.
    Returns ('resolved'|'expired', R)."""
    s1 = sig['s1']; i0 = sig['entry_i']; entry = sig['entry']; stop = sig['stop']; d = sig['dir']
    R = abs(entry - stop)
    if R <= 0:
        return ('expired', None)
    end = min(i0 + 1 + HOLD, len(bars))
    for j in range(i0 + 1, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if s1[j] >= OVERBOUGHT: return ('resolved', (b['c'] - entry) / R)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if s1[j] <= OVERSOLD: return ('resolved', (entry - b['c']) / R)
    return ('expired', None)


def detect_bull_flag(bars):
    """HPS '20/20 Bull Flag' — trend-continuation. LONG: strong uptrend (close>50EMA, 20EMA>50EMA)
    with the 60-10 slow stoch HOLDING >=85 (the key trend gauge), a fresh pullback (9-3 came from
    strength then drops toward the 20 line) into the 20EMA. Enter buying weakness at the 20EMA touch;
    stop under the pullback low. SHORT is the mirror. Cooldown so one flag doesn't fire every bar."""
    if len(bars) < 220:
        return []
    s1 = _rolling_stoch_d(bars, 9, 1, 3)
    s4 = _rolling_stoch_d(bars, 60, 10, 1)
    closes = [b['c'] for b in bars]
    ema20 = _ema(closes, 20); ema50 = _ema(closes, 50)
    out = []; last_l = last_s = -999
    for i in range(60, len(bars) - 1):
        # LONG bull flag
        if closes[i] > ema50[i] and ema20[i] > ema50[i] and s4[i] >= 85 and i - last_l > 12:
            from_strength = max(s1[i - 12:i]) >= 60          # 9-3 was recently high (the pole)
            near_20ema = bars[i]['l'] <= ema20[i] * 1.001     # pullback into the 20EMA
            dip93 = s1[i] <= 30                               # 9-3 dropped toward the 20 line
            if from_strength and near_20ema and dip93:
                entry = closes[i]; stop = min(b['l'] for b in bars[i - 10:i + 1]) * (1 - STOP_BUF)
                if entry > stop:
                    out.append({'dir': 'bull', 'entry_ts': bars[i]['_ts'], 'entry': entry, 'stop': stop,
                                'entry_i': i, 's1': s1, 's4': s4, 'trend': 'up'})
                    last_l = i
        # SHORT bear flag (mirror)
        if closes[i] < ema50[i] and ema20[i] < ema50[i] and s4[i] <= 15 and i - last_s > 12:
            from_weak = min(s1[i - 12:i]) <= 40
            near_20ema = bars[i]['h'] >= ema20[i] * 0.999
            pop93 = s1[i] >= 70
            if from_weak and near_20ema and pop93:
                entry = closes[i]; stop = max(b['h'] for b in bars[i - 10:i + 1]) * (1 + STOP_BUF)
                if entry < stop:
                    out.append({'dir': 'bear', 'entry_ts': bars[i]['_ts'], 'entry': entry, 'stop': stop,
                                'entry_i': i, 's1': s1, 's4': s4, 'trend': 'down'})
                    last_s = i
    return out


def score_trend(bars, sig):
    """Faithful trend-aware exit. WITH-trend (super-signal aligned with the 200EMA trend): 'let it
    play out' — ride until the 60-10 (slow stoch) rotates back through 50 (trend rolls over).
    COUNTER-trend: take the first target (9-3 to the opposite band). Stop = pattern low (-1R)."""
    s1, s4 = sig['s1'], sig['s4']; i0 = sig['entry_i']; entry = sig['entry']; stop = sig['stop']; d = sig['dir']
    R = abs(entry - stop)
    if R <= 0:
        return ('expired', None)
    with_trend = (d == 'bull' and sig['trend'] == 'up') or (d == 'bear' and sig['trend'] == 'down')
    end = min(i0 + 1 + HOLD, len(bars))
    for j in range(i0 + 1, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if with_trend:
                if s4[j] < 50 and s4[j - 1] >= 50: return ('resolved', (b['c'] - entry) / R)
            elif s1[j] >= OVERBOUGHT:
                return ('resolved', (b['c'] - entry) / R)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if with_trend:
                if s4[j] > 50 and s4[j - 1] <= 50: return ('resolved', (entry - b['c']) / R)
            elif s1[j] <= OVERSOLD:
                return ('resolved', (entry - b['c']) / R)
    return ('expired', None)


def _agg(rows):
    rows = sorted(rows, key=lambda r: r[0]); rs = [r[1] for r in rows]
    n = len(rs)
    if not n: return None
    m = n // 2
    return {'n': n, 'wr': 100.0 * sum(1 for r in rs if r > 0) / n, 'exp': sum(rs) / n,
            'totR': sum(rs), 'oos1': sum(rs[:m]) / m if m else 0.0,
            'oos2': sum(rs[m:]) / (n - m) if n - m else 0.0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='historical-ohlc.json')
    ap.add_argument('--tf', default='m15', choices=['m1', 'm5', 'm15', 'h1', 'daily'])
    ap.add_argument('--setup', default='quaddiv', choices=['quaddiv', 'bullflag'])
    args = ap.parse_args()
    detect = detect_bull_flag if args.setup == 'bullflag' else detect_quad_div
    label = '20/20 Bull Flag' if args.setup == 'bullflag' else 'Quad-Divergence (Holy Grail)'
    pairs = json.load(open(args.hist)).get('pairs', {})
    A = defaultdict(list)          # Exit A: first-target (9-3 to opposite band)
    B = defaultdict(list)          # Exit B: trend-aware (let with-trend run on the 60-10)
    Bwt = defaultdict(list)        # Exit B, WITH-TREND signals only
    n_sig = 0; used = []
    order = ['ALL', 'major', 'minor', 'index', 'comm', 'metal', 'crypto', 'other']
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk) or ('crypto' if pk in getattr(H, 'CAM_CRYPTO_PILOT', set()) else None)
        cls = cls or 'other'
        if not isinstance(layers, dict): continue
        bars = H._bars_norm(layers.get(args.tf) or [])
        if len(bars) < 300: continue
        got = False
        for s in detect(bars):
            n_sig += 1
            stA, oA = score(bars, s)
            if stA == 'resolved' and oA is not None:
                A[cls].append((s['entry_ts'], oA)); A['ALL'].append((s['entry_ts'], oA)); got = True
            stB, oB = score_trend(bars, s)
            if stB == 'resolved' and oB is not None:
                B[cls].append((s['entry_ts'], oB)); B['ALL'].append((s['entry_ts'], oB))
                wt = (s['dir'] == 'bull' and s['trend'] == 'up') or (s['dir'] == 'bear' and s['trend'] == 'down')
                if wt:
                    Bwt[cls].append((s['entry_ts'], oB)); Bwt['ALL'].append((s['entry_ts'], oB))
        if got: used.append(pk)

    def show(book, title):
        print(f"\n>> {title}")
        for cls in order:
            v = _agg(book.get(cls, []))
            if v:
                print(f"  {cls:7} n={v['n']:<5} WR={v['wr']:>3.0f}%  exp={v['exp']:+.3f}R  "
                      f"totR={v['totR']:+.0f}  OOS1={v['oos1']:+.3f} OOS2={v['oos2']:+.3f}")

    print(f"=== HPS {label} backtest · tf={args.tf} · {len(used)} pairs · {n_sig} raw signals ===")
    print("Stop = pattern low (-1R). Bracket-honest (unresolved excluded).")
    show(A, "Exit A — first-target: exit on first 9-3 rotation to the opposite band (counter-trend style)")
    show(B, "Exit B — trend-aware: with-trend rides the 60-10 rollover, counter-trend takes first target")
    show(Bwt, "Exit B — WITH-TREND signals only (super-signal aligned with the 200EMA trend)")
    print(f"\npairs: {', '.join(sorted(used))}")


if __name__ == '__main__':
    main()
