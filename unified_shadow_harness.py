"""Unified shadow forward-test harness for the validated swing edges:
   hs       = H&S fired against a high-confidence macro-EW read (H1, 1:2)
   s5_engulf= Multi-TF Confluence, 4H engulfing trigger (4H, 1:2)
   s5_rsi   = Multi-TF Confluence, 4H RSI-50-cross trigger (4H, 1:2)  [strongest]

Run periodically against the latest published data. Each run detects current
signals, logs any NEW ones (dedup) with the data-end at first sight, re-scores
all logged signals on the latest bars, and reports IN-SAMPLE backfill vs GENUINE
FORWARD (entry after the harness's first run). Writes only swing-shadow-log.json,
which nothing on the platform reads. NO deployment — evidence-gathering only.
"""
import json, os, bisect
from detect_triggers import (
    PAIR_CLASS, macd_series, auto_detect_ew, AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from hs_swing_research import scan as hs_scan, MAX_HOLD as HS_HOLD
from five_strategies_research import ema, atr, adx, agg4h, weekly, is_engulf, HOLD

_HERE = os.path.dirname(os.path.abspath(__file__))   # repo root — works in CI and locally
HIST = os.path.join(_HERE, 'historical-ohlc.json')
LOG = os.path.join(_HERE, 'swing-shadow-log.json')
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RR = 2.0


def detect_hs(pk, h1, daily, draw):
    d_ts = [b['_ts'] for b in daily]; cache = {}
    def aew(dd):
        if dd not in cache:
            try:
                r = auto_detect_ew(draw[:dd+1]); e = r.get('ew') if r.get('ok') else None
                cache[dd] = e['dir'] if (e and e.get('dir') in ('bull','bear') and e.get('confidence',0) >= AUTO_EW_MIN_CONFIDENCE and e.get('pattern') in AUTO_EW_VALID_PATTERNS) else None
            except Exception:
                cache[dd] = None
        return cache[dd]
    out = []
    for kind in ('bear', 'bull'):
        for tr in hs_scan(h1, kind):
            dd = bisect.bisect_right(d_ts, tr['ts']) - 2
            macro = aew(dd); tdir = 'bear' if kind == 'bear' else 'bull'
            if not (macro is not None and macro != tdir): continue
            out.append({'strategy': 'hs', 'tf': 'h1', 'pair': pk, 'dir': tdir,
                        'entry_ts': tr['ts'], 'entry': tr['entry'], 'stop': tr['stop']})
    return out


def detect_s5(pk, h1, daily, trigger):
    b4 = agg4h(h1); wk = weekly(daily)
    if len(wk) < 12 or len(b4) < 250: return []
    wc = [b['c'] for b in wk]; we20 = ema(wc, 10)
    dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
    d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
    c4 = [b['c'] for b in b4]; m4, s4 = macd_series(c4, 12, 26, 9); r4 = precompute_rsi(c4, 14)
    out = []; last = -1
    for i in range(2, len(b4) - 1):
        if i <= last: continue
        ts = b4[i]['_ts']
        di = bisect.bisect_right(d_ts, ts) - 1; wi = bisect.bisect_right(w_ts, ts) - 1
        if di < 51 or wi < 11 or we20[wi] is None or de50[di] is None: continue
        wk_up = wc[wi] > we20[wi] and we20[wi] > we20[wi-1]; wk_dn = wc[wi] < we20[wi] and we20[wi] < we20[wi-1]
        if not (wk_up or wk_dn): continue
        a = atr(daily, 14, di)
        if a is None or abs(daily[di]['c'] - de50[di]) > 0.5*a: continue
        av = adx(b4, 14, i)
        if av is None or av < 22: continue
        d = 'bull' if wk_up else 'bear'
        fire = is_engulf(b4, i, d) if trigger == 'engulf' else (
            r4[i-1] is not None and r4[i] is not None and ((d == 'bull' and r4[i-1] <= 50 < r4[i]) or (d == 'bear' and r4[i-1] >= 50 > r4[i])))
        if not fire: continue
        stop = min(b4[i]['l'], b4[i-1]['l']) if d == 'bull' else max(b4[i]['h'], b4[i-1]['h'])
        entry = b4[i+1]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        out.append({'strategy': 's5_' + trigger, 'tf': '4h', 'pair': pk, 'dir': d,
                    'entry_ts': b4[i+1]['_ts'], 'entry': entry, 'stop': stop})
        last = i + 1
    return out


def detect_ob(pk, h1, daily):
    """Bonus #7 — Order Blocks (SMC), DAILY only (4H fails validation). Last
    opposite-colour candle before a break-of-structure impulse; entry on
    retrace-into-zone confirmation, stop beyond the zone (RR2 via the harness)."""
    bars = daily; BOS_LB, OB_SCAN, MITIG_WIN = 10, 6, 40
    n = len(bars); out = []; last = -1
    for i in range(BOS_LB + OB_SCAN, n - 1):
        if i <= last: continue
        prior = bars[i-BOS_LB:i]
        phi = max(b['h'] for b in prior); plo = min(b['l'] for b in prior)
        if bars[i]['c'] > phi:
            ob = None
            for k in range(i-1, max(-1, i-1-OB_SCAN), -1):
                if bars[k]['c'] < bars[k]['o']: ob = k; break
            if ob is None: continue
            zlo, zhi = bars[ob]['l'], bars[ob]['h']
            if zhi <= zlo: continue
            buf = 0.05*(zhi-zlo)
            for j in range(i+1, min(i+1+MITIG_WIN, n-1)):
                if bars[j]['l'] <= zhi and bars[j]['c'] > zlo and bars[j]['c'] > bars[j]['o']:
                    entry, stop = bars[j+1]['o'], zlo-buf
                    if stop < entry:
                        out.append({'strategy':'ob','tf':'daily','pair':pk,'dir':'bull','entry_ts':bars[j+1]['_ts'],'entry':entry,'stop':stop})
                    last = j+1; break
        elif bars[i]['c'] < plo:
            ob = None
            for k in range(i-1, max(-1, i-1-OB_SCAN), -1):
                if bars[k]['c'] > bars[k]['o']: ob = k; break
            if ob is None: continue
            zlo, zhi = bars[ob]['l'], bars[ob]['h']
            if zhi <= zlo: continue
            buf = 0.05*(zhi-zlo)
            for j in range(i+1, min(i+1+MITIG_WIN, n-1)):
                if bars[j]['h'] >= zlo and bars[j]['c'] < zhi and bars[j]['c'] < bars[j]['o']:
                    entry, stop = bars[j+1]['o'], zhi+buf
                    if stop > entry:
                        out.append({'strategy':'ob','tf':'daily','pair':pk,'dir':'bear','entry_ts':bars[j+1]['_ts'],'entry':entry,'stop':stop})
                    last = j+1; break
    return out


TL_PIVOT_L = 3
TL_RETEST_WIN = 12
TL_COOLDOWN = 8
TL_BODY_MIN = 0.65
TL_REJ_MAX = 0.20
TL_ATR_BUF = 0.25


def _tl_pivots(bars, L):
    highs, lows = [], []
    n = len(bars)
    for i in range(L, n - L):
        if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, L+1)):
            highs.append(i)
        if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, L+1)):
            lows.append(i)
    return highs, lows


def _tl_nowick(bar, d):
    rng = bar['h'] - bar['l']
    if rng <= 0:
        return False
    body = abs(bar['c'] - bar['o'])
    if d == 'bull':
        rej = min(bar['o'], bar['c']) - bar['l']; directional = bar['c'] > bar['o']
    else:
        rej = bar['h'] - max(bar['o'], bar['c']); directional = bar['c'] < bar['o']
    return directional and (body / rng >= TL_BODY_MIN) and (rej / rng <= TL_REJ_MAX)


def detect_tl(pk, h1, daily):
    """Bonus #8 — Trendline break-and-retest, 4H, with a no-wick confirmation
    candle at the retest. Break of a descending pivot-high line (bull) / ascending
    pivot-low line (bear) with a directional close beyond it, then a pullback whose
    entry candle is a decisive no-wick (marubozu-ish) candle. Stop beyond the
    retest extreme (+ATR buffer); RR2 via the harness. The plain version (no
    no-wick filter) fails on every timeframe; only this 4H no-wick variant survives
    OOS + 5/6 walk-forward + every parameter perturbation."""
    bars = agg4h(h1)
    if len(bars) < 120:
        return []
    ph, pl = _tl_pivots(bars, TL_PIVOT_L); n = len(bars); out = []; last_fired = -1
    for b in range(2, n - 1):
        if b <= last_fired:
            continue
        for d in ('bull', 'bear'):
            piv = ph if d == 'bull' else pl
            hi = bisect.bisect_right(piv, b - TL_PIVOT_L - 1) - 1
            if hi < 1:
                continue
            p2 = piv[hi]; p1 = piv[hi - 1]
            v1 = bars[p1]['h'] if d == 'bull' else bars[p1]['l']
            v2 = bars[p2]['h'] if d == 'bull' else bars[p2]['l']
            if d == 'bull' and not (v2 < v1):
                continue
            if d == 'bear' and not (v2 > v1):
                continue
            slope = (v2 - v1) / (p2 - p1)
            def line(x, _v1=v1, _p1=p1, _s=slope):
                return _v1 + _s * (x - _p1)
            if d == 'bull':
                broke = bars[b-1]['c'] <= line(b-1) and bars[b]['c'] > line(b) and bars[b]['c'] > bars[b]['o']
            else:
                broke = bars[b-1]['c'] >= line(b-1) and bars[b]['c'] < line(b) and bars[b]['c'] < bars[b]['o']
            if not broke:
                continue
            for r in range(b + 1, min(b + 1 + TL_RETEST_WIN, n - 1)):
                lr = line(r)
                touched = (bars[r]['l'] <= lr and bars[r]['c'] > lr) if d == 'bull' else (bars[r]['h'] >= lr and bars[r]['c'] < lr)
                if not touched:
                    continue
                ei = r + 1; entry = bars[ei]['o']; a = atr(bars, 14, r) or 0.0
                if d == 'bull':
                    stop = min(bars[r]['l'], lr) - TL_ATR_BUF * a
                    if stop >= entry:
                        break
                else:
                    stop = max(bars[r]['h'], lr) + TL_ATR_BUF * a
                    if stop <= entry:
                        break
                if _tl_nowick(bars[r], d):
                    out.append({'strategy': 'tl_nowick', 'tf': '4h', 'pair': pk, 'dir': d,
                                'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
                last_fired = ei + TL_COOLDOWN
                break
            if b <= last_fired:
                break
    return out


W5_PRD = 5
W5_BREAK_WIN = 40
W5_ATR_BUF = 0.25
W5_ZONE_LO, W5_ZONE_HI = 0.382, 0.618
W5_DEEP = 0.786


def _w5_zigzag(bars, prd):
    n = len(bars); piv = []
    for i in range(prd, n - prd):
        if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, prd+1)):
            piv.append((i, bars[i]['h'], 'H'))
        if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, prd+1)):
            piv.append((i, bars[i]['l'], 'L'))
    piv.sort()
    out = []
    for p in piv:
        if out and p[2] == out[-1][2]:
            if (p[2] == 'H' and p[1] > out[-1][1]) or (p[2] == 'L' and p[1] < out[-1][1]):
                out[-1] = p
        else:
            out.append(p)
    return out


def detect_w5pb(pk, h1, daily):
    """Bonus #9 — Elliott wave-5 entry, Bratby 'Trade the Fifth' PULLBACK version,
    4H. After a partial impulse 0-1-2-3, buy the wave-4 dip into the 38.2-61.8%
    fib retrace of wave 3 on a bounce; stop below the dip low; RR2 via the harness.
    WEAKEST of the observed candidates — aggregate 4H is only breakeven, and both
    the breakout variant and daily tf failed walk-forward. Kept because comm/crypto
    at 4H were positive on both OOS halves; the live feed scopes it to those two."""
    bars = agg4h(h1)
    if len(bars) < 120:
        return []
    piv = _w5_zigzag(bars, W5_PRD); n = len(bars); out = []; last = -1
    for k in range(len(piv) - 3):
        p0v, p1v, p2v, p3v = piv[k:k+4]
        for d, kinds in (('bull', ('L', 'H', 'L', 'H')), ('bear', ('H', 'L', 'H', 'L'))):
            if (p0v[2], p1v[2], p2v[2], p3v[2]) != kinds:
                continue
            p0, p1, p2, p3 = p0v[1], p1v[1], p2v[1], p3v[1]
            w1 = abs(p1 - p0); w3 = abs(p3 - p2)
            ok = (p2 > p0 and p3 > p1 and w3 >= w1) if d == 'bull' else (p2 < p0 and p3 < p1 and w3 >= w1)
            if not ok or w3 <= 0:
                continue
            if d == 'bull':
                z_hi = p3 - W5_ZONE_LO * w3; z_lo = p3 - W5_ZONE_HI * w3; void = p3 - W5_DEEP * w3
            else:
                z_lo = p3 + W5_ZONE_LO * w3; z_hi = p3 + W5_ZONE_HI * w3; void = p3 + W5_DEEP * w3
            start = p3v[0] + W5_PRD + 1; ei = None; dip = None
            for j in range(start, min(start + W5_BREAK_WIN, n - 1)):
                b = bars[j]
                if d == 'bull':
                    dip = b['l'] if dip is None else min(dip, b['l'])
                    if b['l'] < void or b['l'] < p1:
                        break
                    if b['l'] <= z_hi and b['c'] > b['o'] and b['c'] > z_lo:
                        ei = j + 1; break
                else:
                    dip = b['h'] if dip is None else max(dip, b['h'])
                    if b['h'] > void or b['h'] > p1:
                        break
                    if b['h'] >= z_lo and b['c'] < b['o'] and b['c'] < z_hi:
                        ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei - 1) or 0.0
            stop = (dip - W5_ATR_BUF * a) if d == 'bull' else (dip + W5_ATR_BUF * a)
            if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
                continue
            out.append({'strategy': 'w5_pullback', 'tf': '4h', 'pair': pk, 'dir': d,
                        'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
            last = ei + 6
    return out


BB_N = 20
BB_K = 2.0
BB_WIDE_THRESH = 0.85


def _bb_bandwidth(bars, n=BB_N, k=BB_K):
    c = [b['c'] for b in bars]; N = len(c); bw = [None] * N
    for i in range(n - 1, N):
        win = c[i-n+1:i+1]; m = sum(win) / n
        sd = (sum((x - m) ** 2 for x in win) / n) ** 0.5
        bw[i] = (2 * k * sd) / m if m else None
    return bw


def _bb_is_wide(bw, i, thresh=BB_WIDE_THRESH):
    if i < 0 or i >= len(bw) or bw[i] is None:
        return False
    prev = [x for x in bw[max(0, i - 100):i] if x is not None]
    if not prev:
        return False
    return bw[i] >= thresh * (sum(prev) / len(prev))


def detect_s5_rsi_wide(pk, h1, daily):
    """Supplement candidate #7 — s5_rsi gated by a WIDE Bollinger-bandwidth regime
    at the 4H signal bar (bandwidth >= 0.85x its trailing-100 mean). Backtest
    roughly doubles s5_rsi expectancy (+0.48R -> +0.94R), 6/6 walk-forward folds,
    robust across BB period/K/threshold. A SUBSET of s5_rsi (same entry/stop) — runs
    in parallel so the demo can compare wide vs plain on real fills."""
    sigs = detect_s5(pk, h1, daily, 'rsi')
    if not sigs:
        return []
    b4 = agg4h(h1); bw = _bb_bandwidth(b4); bts = [b['_ts'] for b in b4]
    out = []
    for s in sigs:
        ei = bisect.bisect_left(bts, s['entry_ts'])
        if _bb_is_wide(bw, ei - 1):
            out.append({**s, 'strategy': 's5_rsi_wide'})
    return out


RSIMR_HOLD = 30
RSIMR_SWING = 5
RSIMR_ATR_BUF = 0.25
RSIMR_RSI_WIN = 3


def detect_rsimr(pk, h1, daily):
    """Observed candidate #8 — RSI/MACD mean-reversion, MAJORS + 4H only. Short when
    RSI was >70 within the last 3 bars AND MACD crosses down; long on the <30 +
    MACD-cross-up mirror. Stop beyond the recent swing extreme. EXIT IS NOT RR2 —
    the position closes on the bar-close when RSI returns to 50 (scored by
    score_meanrev, not score()). WEAKEST candidate: aggregate 4H is breakeven, only
    majors showed +0.30R (5/6 folds, thin n=56). MODEL-ONLY — the strategy-agnostic
    demo cBot cannot execute the RSI-50 exit without a rebuild, so this is NOT
    emitted to the live feed."""
    if PAIR_CLASS.get(pk) != 'major':
        return []
    bars = agg4h(h1)
    if len(bars) < 120:
        return []
    closes = [b['c'] for b in bars]
    rsi = precompute_rsi(closes, 14); macd, sig = macd_series(closes, 12, 26, 9)
    n = len(bars); out = []; last = -1
    for i in range(30, n - 1):
        if i <= last:
            continue
        if rsi[i] is None or None in (macd[i], macd[i-1], sig[i], sig[i-1]):
            continue
        cross_dn = macd[i-1] >= sig[i-1] and macd[i] < sig[i]
        cross_up = macd[i-1] <= sig[i-1] and macd[i] > sig[i]
        win = [rsi[k] for k in range(max(0, i - RSIMR_RSI_WIN + 1), i + 1) if rsi[k] is not None]
        d = None
        if any(v > 70 for v in win) and cross_dn:
            d = 'bear'
        elif any(v < 30 for v in win) and cross_up:
            d = 'bull'
        if d is None:
            continue
        ei = i + 1; entry = bars[ei]['o']; a = atr(bars, 14, i) or 0.0
        if d == 'bear':
            stop = max(b['h'] for b in bars[max(0, i - RSIMR_SWING):i+1]) + RSIMR_ATR_BUF * a
            if stop <= entry:
                continue
        else:
            stop = min(b['l'] for b in bars[max(0, i - RSIMR_SWING):i+1]) - RSIMR_ATR_BUF * a
            if stop >= entry:
                continue
        out.append({'strategy': 'rsimr', 'tf': '4h', 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
        last = ei + 3
    return out


def score_meanrev(bars, rsi, entry_ts, entry, stop, d, hold):
    """RSI-50 mean-reversion exit scorer (continuous R). ('resolved', r) on stop OR
    RSI-50 close OR hold-end; ('pending', None) if data runs out first."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return ('pending', None)
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bear':
            if b['h'] >= stop:
                return ('resolved', -1.0)
            if rsi[j] is not None and rsi[j] <= 50:
                return ('resolved', (entry - b['c']) / R)
        else:
            if b['l'] <= stop:
                return ('resolved', -1.0)
            if rsi[j] is not None and rsi[j] >= 50:
                return ('resolved', (b['c'] - entry) / R)
    if end >= len(bars):
        return ('pending', None)
    b = bars[end - 1]
    return ('resolved', (entry - b['c']) / R if d == 'bear' else (b['c'] - entry) / R)


FIBGZ_PRD = 5
FIBGZ_ZHI, FIBGZ_ZLO = 0.5, 0.618
FIBGZ_STOP_LVL = 0.786
FIBGZ_ATR_BUF = 0.25
FIBGZ_BREAK_WIN = 40
FIBGZ_COOLDOWN = 4


def detect_fibgz(pk, h1, daily):
    """Observed candidate #9 — Dantev-style Fibonacci golden-zone reversal,
    COMMODITIES + H1 only, fixed RR2 target (cBot-executable). Retrace into the
    50-61.8% golden zone of a clean pivot swing leg, enter on the in-trend reversal
    close; stop just beyond the 78.6% level. The ONLY out-of-sample-robust cell in
    the Dantev class breakdown (+0.06R, PF ~1.1, both OOS halves positive) — a thin
    edge, scoped to commodities. Not 'minor FX' (that cell failed OOS)."""
    if PAIR_CLASS.get(pk) != 'comm':
        return []
    bars = h1
    if len(bars) < 200:
        return []
    piv = _w5_zigzag(bars, FIBGZ_PRD); n = len(bars); out = []; last = -1
    for k in range(1, len(piv)):
        p0, p1 = piv[k-1], piv[k]
        if p0[2] == 'L' and p1[2] == 'H':
            d = 'bull'; L, H = p0[1], p1[1]
        elif p0[2] == 'H' and p1[2] == 'L':
            d = 'bear'; H, L = p0[1], p1[1]
        else:
            continue
        rng = H - L
        if rng <= 0:
            continue
        if d == 'bull':
            zhi = H - FIBGZ_ZHI*rng; zlo = H - FIBGZ_ZLO*rng; void = H - FIBGZ_STOP_LVL*rng
        else:
            zlo = L + FIBGZ_ZHI*rng; zhi = L + FIBGZ_ZLO*rng; void = L + FIBGZ_STOP_LVL*rng
        start = p1[0] + FIBGZ_PRD + 1; ei = None
        for j in range(start, min(start + FIBGZ_BREAK_WIN, n - 1)):
            b = bars[j]
            if d == 'bull':
                if b['l'] < void:
                    break
                if b['l'] <= zhi and b['c'] > b['o'] and b['c'] > zlo:
                    ei = j + 1; break
            else:
                if b['h'] > void:
                    break
                if b['h'] >= zlo and b['c'] < b['o'] and b['c'] < zhi:
                    ei = j + 1; break
        if ei is None or ei <= last or ei >= n:
            continue
        entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
        stop = (void - FIBGZ_ATR_BUF*a) if d == 'bull' else (void + FIBGZ_ATR_BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        out.append({'strategy': 'fib_gz', 'tf': 'h1', 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
        last = ei + FIBGZ_COOLDOWN
    return out


FREDTL_PRD = 4
FREDTL_TOUCH_WIN = 50
FREDTL_NEAR = 0.25
FREDTL_BREAK_TOL = 0.5
FREDTL_ATR_BUF = 0.25
FREDTL_COOLDOWN = 6


def detect_fredtl(pk, h1, daily):
    """Observed candidate #10 — Fred Trading (Fredtradingdk) trendline-BOUNCE,
    XAUUSD + 4H only, fixed RR2. Two pivots define an intact trendline (ascending
    support / descending resistance); price retraces to it and bounces in-trend;
    stop beyond the line. VERY THIN (n=25 backtest, +0.31R, both OOS halves up).
    Observed on request — treat the panel row as a curiosity, not an edge. Lowest
    dedup priority, so real xauusd fills will be rare (higher-conviction strategies
    usually win the per-pair dedup); the model row is the primary evidence."""
    if pk != 'xauusd':
        return []
    bars = agg4h(h1); n = len(bars)
    if n < 150:
        return []
    zz = _w5_zigzag(bars, FREDTL_PRD)
    lows = [(i, p) for (i, p, k) in zz if k == 'L']
    highs = [(i, p) for (i, p, k) in zz if k == 'H']
    out = []; last = -1
    for d, seq in (('bull', lows), ('bear', highs)):
        for m in range(1, len(seq)):
            (i1, v1), (i2, v2) = seq[m-1], seq[m]
            if i2 <= i1:
                continue
            slope = (v2 - v1) / (i2 - i1)
            if d == 'bull' and slope <= 0:
                continue
            if d == 'bear' and slope >= 0:
                continue

            def line(x, _v1=v1, _i1=i1, _s=slope):
                return _v1 + _s * (x - _i1)

            start = i2 + FREDTL_PRD + 1; ei = None
            for j in range(start, min(start + FREDTL_TOUCH_WIN, n - 1)):
                b = bars[j]; lv = line(j); a = atr(bars, 14, j) or 0.0
                if a <= 0:
                    continue
                if d == 'bull':
                    if b['c'] < lv - FREDTL_BREAK_TOL * a:
                        break
                    if b['l'] <= lv + FREDTL_NEAR * a and b['c'] > lv and b['c'] > b['o']:
                        ei = j + 1; break
                else:
                    if b['c'] > lv + FREDTL_BREAK_TOL * a:
                        break
                    if b['h'] >= lv - FREDTL_NEAR * a and b['c'] < lv and b['c'] < b['o']:
                        ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0; lv = line(ei)
            if d == 'bull':
                stop = min(bars[ei-1]['l'], lv) - FREDTL_ATR_BUF * a
                if stop >= entry:
                    continue
            else:
                stop = max(bars[ei-1]['h'], lv) + FREDTL_ATR_BUF * a
                if stop <= entry:
                    continue
            out.append({'strategy': 'fred_tl', 'tf': '4h', 'pair': pk, 'dir': d,
                        'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
            last = ei + FREDTL_COOLDOWN
    return out


def score(bars, entry_ts, entry, stop, d, hold):
    """Return ('resolved', r) | ('pending', None) | ('expired', None).
    Matches the research walk(): unresolved within the hold is EXCLUDED (not a
    loss). 'pending' = ran out of data (will resolve later); 'expired' = hold
    elapsed with data available (time-stop, excluded from WR like the backtest)."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars): return ('pending', None)
    tgt = entry + RR*R if d == 'bull' else entry - RR*R
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= tgt: return ('resolved', RR)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= tgt: return ('resolved', RR)
    return ('pending', None) if end >= len(bars) else ('expired', None)


def cost(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1); data_end = max(data_end, h1[-1]['_ts'])
        rsi4 = precompute_rsi([b['c'] for b in b4], 14)
        found = (detect_hs(pk, h1, daily, draw) + detect_s5(pk, h1, daily, 'engulf')
                 + detect_s5(pk, h1, daily, 'rsi') + detect_ob(pk, h1, daily)
                 + detect_tl(pk, h1, daily) + detect_w5pb(pk, h1, daily)
                 + detect_s5_rsi_wide(pk, h1, daily) + detect_rsimr(pk, h1, daily)
                 + detect_fibgz(pk, h1, daily) + detect_fredtl(pk, h1, daily))
        for s in found:
            detected += 1
            k = f"{s['strategy']}:{s['pair']}:{int(s['entry_ts'])}"
            if k not in sigs:
                s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
            rec = sigs[k]
            tf = rec['tf']
            bars = h1 if tf == 'h1' else (b4 if tf == '4h' else daily)
            hold = HS_HOLD if tf == 'h1' else (HOLD['4h'] if tf == '4h' else 20)
            if rec['strategy'] == 'rsimr':
                st, o = score_meanrev(b4, rsi4, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], RSIMR_HOLD)
            else:
                st, o = score(bars, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], hold)
            rec['status'] = st
            if st == 'resolved':
                rec['r'] = o - cost(o, rec['entry'], abs(rec['entry']-rec['stop']))
            else:
                rec.pop('r', None)
    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())
    def rep(title, rows):
        print(f"\n{title}")
        for strat in ('hs', 's5_engulf', 's5_rsi', 'ob', 'tl_nowick', 'w5_pullback', 's5_rsi_wide', 'rsimr', 'fib_gz', 'fred_tl'):
            sub = [s for s in rows if s['strategy'] == strat and s['status'] == 'resolved' and 'r' in s]
            pend = sum(1 for s in rows if s['strategy'] == strat and s['status'] == 'pending')
            if sub:
                w = sum(1 for s in sub if s['r'] > 0)
                print(f"  {strat:<10} resolved={len(sub):>3} pending={pend:>3} WR={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R")
            else:
                print(f"  {strat:<10} resolved=0 pending={pend}")
    print(f"harness run · data_end {int(data_end)} · detected {detected} signals this pass")
    rep("ALL logged (incl. in-sample backfill):", allv)
    rep("GENUINE FORWARD (entry after first run):", [s for s in allv if s['entry_ts'] > base])
    if all(s['entry_ts'] <= base for s in allv):
        print("\n  (baseline just set — re-run as new bars publish to accumulate forward signals)")


if __name__ == '__main__':
    main()
