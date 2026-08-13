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
from session_2h_reversal_research import find_signals as _sess_signals, GEO as _SESS_GEO, SESSIONS as _SESS_HOURS
from fma_sweep_reversal_research import fma_signals as _fma_signals
from astongill_orb_po3_research import po3_signals as _po3_signals, SESS as _PO3_SESS
from crypto_delta_research import absorption_signals as _absorb_signals, _norm as _delta_norm
from liquidity_sweep_fvg_research import variant_B as _sweepfvg_signals

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
    retrace-into-zone confirmation, stop beyond the zone (RR2 via the harness).
    2026-07-23: the retrace bar must now be a REVERSAL CANDLE (engulfing / 3-bar /
    pin bar) in the trade direction, not just any in-trend close. Validated as a
    filter: it cut the passive zone-entries and lifted ob from fragile (+0.087R,
    fails walk-forward) to robust (+0.257R, WR 42%, both OOS halves +). ~1/3 the
    volume, ~3x the expectancy."""
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
                if bars[j]['l'] <= zhi and bars[j]['c'] > zlo and bars[j]['c'] > bars[j]['o']:   # first retrace
                    if _rev_candle(bars, j, 'bull'):                                            # emit only if confirmed
                        entry, stop = bars[j+1]['o'], zlo-buf
                        if stop < entry:
                            out.append({'strategy':'ob','tf':'daily','pair':pk,'dir':'bull','entry_ts':bars[j+1]['_ts'],'entry':entry,'stop':stop})
                    last = j+1; break                                                          # break on first retrace regardless
        elif bars[i]['c'] < plo:
            ob = None
            for k in range(i-1, max(-1, i-1-OB_SCAN), -1):
                if bars[k]['c'] > bars[k]['o']: ob = k; break
            if ob is None: continue
            zlo, zhi = bars[ob]['l'], bars[ob]['h']
            if zhi <= zlo: continue
            buf = 0.05*(zhi-zlo)
            for j in range(i+1, min(i+1+MITIG_WIN, n-1)):
                if bars[j]['h'] >= zlo and bars[j]['c'] < zhi and bars[j]['c'] < bars[j]['o']:   # first retrace
                    if _rev_candle(bars, j, 'bear'):                                            # emit only if confirmed
                        entry, stop = bars[j+1]['o'], zhi+buf
                        if stop > entry:
                            out.append({'strategy':'ob','tf':'daily','pair':pk,'dir':'bear','entry_ts':bars[j+1]['_ts'],'entry':entry,'stop':stop})
                    last = j+1; break                                                          # break on first retrace regardless
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


SID_LO = 35          # steadier than the book's 30/70 (walk-forward-robust on index-4H)
SID_HI = 65
SID_CONF_WIN = 10    # bars after the RSI extreme to wait for the MACD cross
SID_BUF = 0.10
SID_HOLD = 90
SID_COOLDOWN = 3


def detect_sid(pk, h1, daily):
    """Observed candidate #16 — 'Sid' RSI+MACD mean-reversion, INDEX + 4H only,
    RSI-50 exit. Long when RSI<35 then a MACD(12,26,9) cross UP within 10 bars (RSI
    still <50); short mirror at RSI>65 + cross DOWN. Stop below the low made while
    oversold / above the high while overbought. Exit when RSI returns to 50 (scored
    by score_meanrev). Indices are the book's own instrument domain and the only
    cell with an edge (+0.10..+0.19R, WR ~60%, positive across the RSI/window grid);
    the aggregate strategy is a high-WR / ~zero-expectancy mirage. 35/65 chosen over
    the book's 30/70 because it holds up in BOTH out-of-sample halves where 30/70 is
    borderline. MODEL-ONLY — the RSI-50 exit isn't fixed-RR-cBot-executable, so this
    is shadow-observed, not emitted to the demo feed (like rsimr)."""
    if PAIR_CLASS.get(pk) != 'index':
        return []
    bars = agg4h(h1); n = len(bars)
    if n < 120:
        return []
    closes = [b['c'] for b in bars]
    rsi = precompute_rsi(closes, 14); macd, sig = macd_series(closes, 12, 26, 9)
    out = []; last = -1; i = 30
    while i < n - 1:
        if i <= last or rsi[i] is None:
            i += 1; continue
        d = 'bull' if rsi[i] < SID_LO else ('bear' if rsi[i] > SID_HI else None)
        if d is None:
            i += 1; continue
        ext = bars[i]['l'] if d == 'bull' else bars[i]['h']; ei = None
        for j in range(i, min(i + SID_CONF_WIN, n - 1)):
            ext = min(ext, bars[j]['l']) if d == 'bull' else max(ext, bars[j]['h'])
            if None in (macd[j], sig[j], macd[j-1], sig[j-1]) or rsi[j] is None:
                continue
            if d == 'bull' and macd[j-1] <= sig[j-1] and macd[j] > sig[j] and rsi[j] < 50:
                ei = j + 1; break
            if d == 'bear' and macd[j-1] >= sig[j-1] and macd[j] < sig[j] and rsi[j] > 50:
                ei = j + 1; break
        if ei is None:
            i += 1; continue
        a = atr(bars, 14, ei-1) or 0.0; entry = bars[ei]['o']
        stop = (ext - SID_BUF*a) if d == 'bull' else (ext + SID_BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            i += 1; continue
        out.append({'strategy': 'sid', 'tf': '4h', 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
        last = ei + SID_COOLDOWN; i = last + 1
    return out


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


TP_PRD = 3
TP_BOS_WIN = 30
TP_RETEST_WIN = 20
TP_BUF = 0.25
TP_COOLDOWN = 5


def detect_threepush(pk, h1, daily):
    """Observed candidate #11 — 3-push + break-of-structure + retest reversal
    (user-drawn), COMMODITIES + 4H only, fixed RR2. Three higher highs on rising
    structure -> close below the last higher low (BOS) -> retest of the broken level
    -> sell (mirror 3-lows-down for longs). Only cell that held: comm 4H +0.19R, both
    OOS halves positive, robust to every parameter perturbation — but thin (n=40) and
    only 3/6 walk-forward folds. Observe, don't trust."""
    if PAIR_CLASS.get(pk) != 'comm':
        return []
    bars = agg4h(h1); n = len(bars)
    if n < 150:
        return []
    zz = _w5_zigzag(bars, TP_PRD); out = []; last = -1
    for k in range(len(zz) - 5):
        w = zz[k:k+6]; kinds = tuple(x[2] for x in w)
        if kinds == ('L', 'H', 'L', 'H', 'L', 'H'):
            l0, h1v, l1, h2, l2, h3 = (x[1] for x in w)
            if not (h1v < h2 < h3 and l0 < l1 < l2):
                continue
            struct = l2; start = w[5][0] + TP_PRD + 1; bos = None
            for j in range(start, min(start + TP_BOS_WIN, n - 1)):
                if bars[j]['h'] > h3:
                    break
                if bars[j]['c'] < struct:
                    bos = j; break
            if bos is None:
                continue
            ei = None
            for j in range(bos + 1, min(bos + 1 + TP_RETEST_WIN, n - 1)):
                if bars[j]['c'] > h3:
                    break
                if bars[j]['h'] >= struct and bars[j]['c'] < struct:
                    ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
            stop = max(bars[ei-1]['h'], struct) + TP_BUF * a; d = 'bear'
        elif kinds == ('H', 'L', 'H', 'L', 'H', 'L'):
            h0, l1, h1b, l2b, h2b, l3 = (x[1] for x in w)
            if not (l1 > l2b > l3 and h0 > h1b > h2b):
                continue
            struct = h2b; start = w[5][0] + TP_PRD + 1; bos = None
            for j in range(start, min(start + TP_BOS_WIN, n - 1)):
                if bars[j]['l'] < l3:
                    break
                if bars[j]['c'] > struct:
                    bos = j; break
            if bos is None:
                continue
            ei = None
            for j in range(bos + 1, min(bos + 1 + TP_RETEST_WIN, n - 1)):
                if bars[j]['c'] < l3:
                    break
                if bars[j]['l'] <= struct and bars[j]['c'] > struct:
                    ei = j + 1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
            stop = min(bars[ei-1]['l'], struct) - TP_BUF * a; d = 'bull'
        else:
            continue
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        out.append({'strategy': 'threepush', 'tf': '4h', 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
        last = ei + TP_COOLDOWN
    return out


EM_LB = 3
EM_BUF = 0.10
EM_COOLDOWN = 3


def detect_engulf_manip(pk, h1, daily):
    """Observed candidate #12 — 4H 'manipulation' engulfing reversal, CRYPTO only.
    A bullish engulfing candle whose low sweeps recent lows (the manipulation) then
    closes bullish -> buy; bearish mirror. Stop beyond the swept extreme, RR2. Crypto
    4H was the one class that cleared breakeven: +0.14R, WR 38%, n=315, robust to every
    parameter perturbation, 4/6 walk-forward folds, both OOS halves positive. Observe —
    stronger evidence than the other thin adds, but single-class + 4/6 folds."""
    if PAIR_CLASS.get(pk) != 'crypto':
        return []
    bars = agg4h(h1); n = len(bars)
    if n < 150:
        return []
    out = []; last = -1
    for i in range(EM_LB + 2, n - 1):
        if i <= last:
            continue
        prior = bars[i-EM_LB:i]; d = None
        if is_engulf(bars, i, 'bull') and bars[i]['l'] < min(b['l'] for b in prior):
            d = 'bull'
        elif is_engulf(bars, i, 'bear') and bars[i]['h'] > max(b['h'] for b in prior):
            d = 'bear'
        if d is None:
            continue
        ei = i + 1; entry = bars[ei]['o']; a = atr(bars, 14, i) or 0.0
        stop = (bars[i]['l'] - EM_BUF*a) if d == 'bull' else (bars[i]['h'] + EM_BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        out.append({'strategy': 'engulf_manip', 'tf': '4h', 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
        last = ei + EM_COOLDOWN
    return out


SWEEPREV_K = 2
SWEEPREV_BUF = 0.15
SWEEPREV_TRIG = 15
SWEEPREV_COOLDOWN = 4
SWEEPREV_HOLD = 90


def _pivots_hl(bars, k):
    n = len(bars); ph = [False]*n; pl = [False]*n
    for i in range(k, n-k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)): ph[i] = True
        if all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)): pl[i] = True
    return ph, pl


def detect_sweeprev(pk, h1, daily):
    """Observed candidate #13 — swept-extreme reversal + counter-trendline break,
    reverting to the opposite swing (MINOR FX + 4H only, STRUCTURAL target).
    Sweep a prior swing high (higher-high) / swing low (lower-low), enter on the
    break back through the rally's last higher-low / fall's last lower-high, stop
    beyond the swept extreme, target the PREVIOUS OPPOSITE swing. MODEL-ONLY: the
    edge lives at a low structural RR (~0.25R median) that sits BELOW the measured
    execution gap, and the target is structural (not fixed-RR) — so it is shadow-
    observed against its own target, NOT placed on the demo cBot (kept out of the
    swing feed, like rsimr). Minor-4H is the only parameter-robust, both-OOS-
    positive cell (+0.08..+0.13R across the k/buffer/window grid); every other
    class fails walk-forward and majors are negative (the sweep gets run over)."""
    if PAIR_CLASS.get(pk) != 'minor':
        return []
    return _sweeprev_signals(agg4h(h1), pk, 'sweeprev', '4h')


def _sweeprev_signals(bars, pk, tag, tf):
    """Bar-agnostic sweeprev core (extracted 2026-08-06 so the same swept-extreme
    reversal can be run on any timeframe, e.g. m15). Identical logic to the live 4H
    version — only the bars/tag/tf come from the caller."""
    n = len(bars); k = SWEEPREV_K
    if n < 2*k + 30:
        return []
    ph, pl = _pivots_hl(bars, k)
    ph_idx = [i for i in range(n) if ph[i]]; pl_idx = [i for i in range(n) if pl[i]]
    out = []
    # SHORTS: sweep above prior swing high, break the rally's last higher-low.
    last = -1
    for bi in range(1, len(ph_idx)):
        idxB = ph_idx[bi]; idxA = ph_idx[bi-1]
        if bars[idxB]['h'] <= bars[idxA]['h']:
            continue
        seg = bars[idxA:idxB+1]
        if len(seg) < 3:
            continue
        tgt_low = min(b['l'] for b in seg)                    # previous swing low = target
        hls = [j for j in pl_idx if idxA < j < idxB]
        if not hls:
            continue
        hl_lvl = bars[hls[-1]]['l']                           # last higher-low = trigger level
        if hl_lvl <= tgt_low:
            continue
        t = None
        for j in range(max(idxB + k, hls[-1] + k + 1), min(idxB + SWEEPREV_TRIG, n-1)):
            if j <= last:
                break
            if bars[j]['c'] < hl_lvl:
                t = j; break
        if t is None or t + 1 >= n:
            continue
        ei = t + 1; entry = bars[ei]['o']; a = atr(bars, 14, t) or 0.0
        stop = bars[idxB]['h'] + SWEEPREV_BUF*a
        if stop <= entry or tgt_low >= entry:
            continue
        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': 'bear',
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': tgt_low})
        last = ei + SWEEPREV_COOLDOWN
    # LONGS: sweep below prior swing low, break the fall's last lower-high.
    last = -1
    for bi in range(1, len(pl_idx)):
        idxB = pl_idx[bi]; idxA = pl_idx[bi-1]
        if bars[idxB]['l'] >= bars[idxA]['l']:
            continue
        seg = bars[idxA:idxB+1]
        if len(seg) < 3:
            continue
        tgt_high = max(b['h'] for b in seg)                   # previous swing high = target
        lhs = [j for j in ph_idx if idxA < j < idxB]
        if not lhs:
            continue
        lh_lvl = bars[lhs[-1]]['h']                           # last lower-high = trigger level
        if lh_lvl >= tgt_high:
            continue
        t = None
        for j in range(max(idxB + k, lhs[-1] + k + 1), min(idxB + SWEEPREV_TRIG, n-1)):
            if j <= last:
                break
            if bars[j]['c'] > lh_lvl:
                t = j; break
        if t is None or t + 1 >= n:
            continue
        ei = t + 1; entry = bars[ei]['o']; a = atr(bars, 14, t) or 0.0
        stop = bars[idxB]['l'] - SWEEPREV_BUF*a
        if stop >= entry or tgt_high <= entry:
            continue
        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': 'bull',
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': tgt_high})
        last = ei + SWEEPREV_COOLDOWN
    return out


def score_sweeprev(bars, entry_ts, entry, stop, target, d, hold):
    """Score sweeprev against its STRUCTURAL target price (variable RR), not the
    global fixed RR. Win = realized structural RR, stop = -1R; timeouts excluded
    ('expired') exactly like score(). Mirrors the research walk_to_target."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return ('pending', None)
    rr_avail = abs(target - entry) / R
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= target: return ('resolved', rr_avail)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= target: return ('resolved', rr_avail)
    return ('pending', None) if end >= len(bars) else ('expired', None)


ASIANGLITCH_US_HOUR = 20                       # UTC bar defining the US last-hour high/low
ASIANGLITCH_ASIA = {23, 0, 1, 2, 3, 4, 5}      # Asian-session sweep+reversal window
ASIANGLITCH_MAX = 12                           # bars after the ref bar to keep the episode alive
ASIANGLITCH_BUF = 0.10                          # stop buffer beyond the swept extreme, in ATR
ASIANGLITCH_RR = 3.0                            # target reward:risk (beats 2:1 at every realistic hold)
ASIANGLITCH_HOLD = 120                          # generous hold — mirrors the cBot's hold-to-bracket behaviour


def detect_asianglitch(pk, h1, daily):
    """Observed candidate #14 — 'Asian-session gold glitch' (session-timed
    liquidity-sweep reversal, GOLD only, cBot-executable at RR3). Mark the high/
    low of the US last-hour bar (20:00 UTC); during the Asian window (23:00-06:00
    UTC) fade the FIRST sweep of that level on the reclaim (close back inside),
    stop beyond the swept extreme, target 3R. GOLD-SPECIFIC: XAUUSD is +0.18..
    +0.28R at 3:1 with both OOS halves positive, robust across the reference-hour
    / buffer / hold grid; every other pair and class is negative (the source's
    gold-only framing is correct). RR3 chosen over 2 by a fixed-entry-set sweep —
    expectancy scales with reward and 3:1 beats 2:1 at every realistic hold."""
    if pk != 'xauusd':
        return []
    bars = h1; n = len(bars); out = []; hh = ASIANGLITCH_US_HOUR
    for r in range(14, n - 2):
        if (bars[r]['_ts'] // 3600) % 24 != hh:
            continue
        ref_hi = bars[r]['h']; ref_lo = bars[r]['l']; swept = None; ext = None
        for j in range(r + 1, min(r + 1 + ASIANGLITCH_MAX, n - 1)):
            hr = (bars[j]['_ts'] // 3600) % 24
            if hr == hh:
                break
            if hr not in ASIANGLITCH_ASIA:
                continue
            b = bars[j]
            if swept is None:
                if b['h'] > ref_hi:
                    swept = 'high'; ext = b['h']
                elif b['l'] < ref_lo:
                    swept = 'low'; ext = b['l']
                if swept == 'high' and b['c'] < ref_hi:
                    d = 'bear'
                elif swept == 'low' and b['c'] > ref_lo:
                    d = 'bull'
                else:
                    continue
            else:
                ext = max(ext, b['h']) if swept == 'high' else min(ext, b['l'])
                if swept == 'high' and b['c'] < ref_hi:
                    d = 'bear'
                elif swept == 'low' and b['c'] > ref_lo:
                    d = 'bull'
                else:
                    continue
            ei = j + 1
            if ei >= n:
                break
            entry = bars[ei]['o']; a = atr(bars, 14, j) or 0.0
            stop = (ext + ASIANGLITCH_BUF*a) if d == 'bear' else (ext - ASIANGLITCH_BUF*a)
            if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
                break
            out.append({'strategy': 'asianglitch', 'tf': 'h1', 'pair': pk, 'dir': d,
                        'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop,
                        'rr': ASIANGLITCH_RR})
            break
    return out


def score_asianglitch(bars, entry_ts, entry, stop, d, hold, rr):
    """Score asianglitch against its fixed RR target (bracket: target-or-stop,
    unresolved excluded like score()). Matches the cBot's hold-to-bracket
    behaviour with a generous hold."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return ('pending', None)
    tgt = entry + rr*R if d == 'bull' else entry - rr*R
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= tgt: return ('resolved', rr)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= tgt: return ('resolved', rr)
    return ('pending', None) if end >= len(bars) else ('expired', None)


WM_PRD = 3
WM_PEAK_TOL = 0.40
WM_MAX_WAIT = 15
WM_BUF = 0.10
WM_RR = 1.0
WM_HOLD = 96
WM_COOLDOWN = 4


def _wm_pivots(bars, k):
    n = len(bars); out = []
    for i in range(k, n - k):
        h = bars[i]['h']; l = bars[i]['l']
        if all(h >= bars[i-j]['h'] and h >= bars[i+j]['h'] for j in range(1, k+1)):
            out.append((i, h, 'H'))
        elif all(l <= bars[i-j]['l'] and l <= bars[i+j]['l'] for j in range(1, k+1)):
            out.append((i, l, 'L'))
    return out


def detect_wm(pk, h1, daily):
    """Observed candidate #15 — W-bottom / M-top neckline-break reversal (CRYPTO +
    H1 only, fixed 1:1). Two roughly-equal peaks (M) / troughs (W) after a run;
    neckline = the middle trough/peak. Signal fires on the BREAK of the neckline
    (sell-stop for M / buy-stop for W) — a momentum entry, so entry is recorded at
    the worse of neckline/break-bar open (the realistic fill, NOT the optimistic
    exact-neckline fill). Stop beyond the pattern extreme; target 1:1. Cancel if the
    far extreme breaks first or >15 bars pass sideways. CRYPTO-only: on a realistic
    fill it is the sole class that survives walk-forward (+0.09R, both OOS halves +,
    robust across pivot/tolerance); every other class and timeframe fails."""
    if PAIR_CLASS.get(pk) != 'crypto':
        return []
    bars = h1; n = len(bars)
    if n < 2*WM_PRD + 20:
        return []
    piv = _wm_pivots(bars, WM_PRD); out = []; last = -1
    for p in range(3, len(piv)):
        a4, b3, c2, d1 = piv[p-3], piv[p-2], piv[p-1], piv[p]
        if b3[2] == 'H' and c2[2] == 'L' and d1[2] == 'H' and a4[2] == 'L':
            d = 'bear'; peak1, peak2, neck, run0 = b3[1], d1[1], c2[1], a4[1]
            hgt = max(peak1, peak2) - neck
            if hgt <= 0 or abs(peak1 - peak2) > WM_PEAK_TOL*hgt or run0 >= neck:
                continue
            extreme = max(peak1, peak2)
        elif b3[2] == 'L' and c2[2] == 'H' and d1[2] == 'L' and a4[2] == 'H':
            d = 'bull'; tr1, tr2, neck, run0 = b3[1], d1[1], c2[1], a4[1]
            hgt = neck - min(tr1, tr2)
            if hgt <= 0 or abs(tr1 - tr2) > WM_PEAK_TOL*hgt or run0 <= neck:
                continue
            extreme = min(tr1, tr2)
        else:
            continue
        confirm = d1[0] + WM_PRD
        if confirm <= last or confirm >= n - 1:
            continue
        a = atr(bars, WM_PRD*3, d1[0]) or atr(bars, 14, min(d1[0], n-1)) or 0.0
        stop = (extreme + WM_BUF*a) if d == 'bear' else (extreme - WM_BUF*a)
        ei = None
        for k in range(confirm, min(confirm + WM_MAX_WAIT, n)):
            bk = bars[k]
            if d == 'bear':
                if bk['h'] > extreme:               # broke the top of the M -> cancel
                    break
                if bk['l'] <= neck:                 # sell-stop triggers
                    ei = k; break
            else:
                if bk['l'] < extreme:               # broke the bottom of the W -> cancel
                    break
                if bk['h'] >= neck:                 # buy-stop triggers
                    ei = k; break
        if ei is None:
            continue
        entry = min(neck, bars[ei]['o']) if d == 'bear' else max(neck, bars[ei]['o'])
        if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
            continue
        out.append({'strategy': 'wm', 'tf': 'h1', 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop, 'rr': WM_RR})
        last = ei + WM_COOLDOWN
    return out


def _rc_engulf(bars, i, d):
    if i < 1: return False
    o, c = bars[i]['o'], bars[i]['c']; po, pc = bars[i-1]['o'], bars[i-1]['c']
    lo1, hi1 = min(po, pc), max(po, pc)
    if d == 'bull': return c > o and o <= lo1 and c >= hi1
    return c < o and o >= hi1 and c <= lo1


def _rc_pin(b, d):
    rng = b['h'] - b['l']
    if rng <= 0: return False
    if d == 'bull': return (min(b['o'], b['c']) - b['l']) >= 0.5*rng
    return (b['h'] - max(b['o'], b['c'])) >= 0.5*rng


def _rc_3bar(bars, i, d):
    if i < 2: return False
    if d == 'bull':
        return bars[i-2]['c'] < bars[i-2]['o'] and bars[i]['c'] > bars[i]['o'] and bars[i]['c'] > bars[i-2]['h']
    return bars[i-2]['c'] > bars[i-2]['o'] and bars[i]['c'] < bars[i]['o'] and bars[i]['c'] < bars[i-2]['l']


def _rev_candle(bars, i, d):
    if i < 0 or i >= len(bars): return False
    return _rc_engulf(bars, i, d) or _rc_pin(bars[i], d) or _rc_3bar(bars, i, d)


OBFVG_IMP = 1.0
OBFVG_OB_LOOK = 4
OBFVG_RETR = 20
OBFVG_BUF = 0.10
OBFVG_COOLDOWN = 5
OBFVG_HOLD = 72
OBFVG_LIVE = {'xrpusd', 'usdcad'}      # parameter-robust -> live feed
OBFVG_WATCH = {'ftse100', 'btcusd', 'jp225'}   # base-positive but grid-borderline -> shadow-only WATCH.
                                       # jp225 added 2026-08-04: only index/major that newly qualifies
                                       # once regime-gated (both OOS halves +, +0.255R, n=21 -> thin,
                                       # so observer not live). See obfvg_regime_candidates.py.
OBFVG_REGIME_GATE = True               # daily-50-EMA regime filter (validated per-pair 2026-08-04,
                                       # ma_reclaim_research.py): only take obfvg entries ALIGNED with
                                       # the daily-50-EMA side. Counter-trend entries are negative in
                                       # aggregate and in most pairs; aligned beats counter in 31/39.
                                       # Only removes trades -> more selective. Live cells net improve
                                       # (xrpusd/usdcad combined +0.235R -> +0.358R gated).
OBFVG_REGIME_MA = 50


def _obfvg_signals(pk, h1, tag, tf='h1', daily=None):
    """OB+FVG retrace, MARKET entry (tag-bar close), fixed RR2 (scored by score()).
    Impulse -> order block (last opposite candle) + FVG (3-bar gap) = zone; retrace
    into the zone -> enter in the impulse direction; stop beyond the OB. The robust
    cell across the parameter grid (not the fat-tailed day-range target). `tf` is the
    entry timeframe of the bars passed in (h1 for the swing cells, 15m for intraday).
    When `daily` is supplied and OBFVG_REGIME_GATE is on, counter-trend entries (against
    the daily-50-EMA side) are suppressed — a validated regime filter."""
    bars = h1; n = len(bars); out = []; last = -1
    de = dts = None
    if OBFVG_REGIME_GATE and daily and len(daily) > OBFVG_REGIME_MA:
        de = ema([b['c'] for b in daily], OBFVG_REGIME_MA); dts = [b['_ts'] for b in daily]

    def _aligned(entry_ts, d):
        if de is None:                                          # no daily / too little -> don't gate
            return True
        di = bisect.bisect_right(dts, entry_ts) - 1
        if di < OBFVG_REGIME_MA or de[di] is None:
            return True
        up = daily[di]['c'] > de[di]
        return (up and d == 'bull') or ((not up) and d == 'bear')

    for j in range(OBFVG_OB_LOOK+2, n-2):
        if j <= last:
            continue
        a = atr(bars, 14, j) or 0.0
        if a <= 0:
            continue
        rng = bars[j]['h'] - bars[j]['l']; body = bars[j]['c'] - bars[j]['o']
        if -body >= OBFVG_IMP*a and rng >= OBFVG_IMP*a:              # bearish impulse -> supply
            ob = None
            for k in range(j-1, max(-1, j-1-OBFVG_OB_LOOK), -1):
                if bars[k]['c'] > bars[k]['o']: ob = k; break
            if ob is None or not (j >= 2 and bars[j-2]['l'] > bars[j]['h']):
                continue
            zone_lo = bars[j]['h']; ob_top = bars[ob]['h']
            if ob_top <= zone_lo:
                continue
            for r in range(j+1, min(j+1+OBFVG_RETR, n-1)):
                if bars[r]['h'] >= zone_lo:
                    entry = bars[r]['c']; stop = ob_top + OBFVG_BUF*a
                    if stop > entry and _aligned(bars[r+1]['_ts'], 'bear'):
                        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': 'bear',
                                    'entry_ts': bars[r+1]['_ts'], 'entry': entry, 'stop': stop})
                    last = r + OBFVG_COOLDOWN; break
        elif body >= OBFVG_IMP*a and rng >= OBFVG_IMP*a:            # bullish impulse -> demand
            ob = None
            for k in range(j-1, max(-1, j-1-OBFVG_OB_LOOK), -1):
                if bars[k]['c'] < bars[k]['o']: ob = k; break
            if ob is None or not (j >= 2 and bars[j-2]['h'] < bars[j]['l']):
                continue
            zone_hi = bars[j]['l']; ob_bot = bars[ob]['l']
            if ob_bot >= zone_hi:
                continue
            for r in range(j+1, min(j+1+OBFVG_RETR, n-1)):
                if bars[r]['l'] <= zone_hi:
                    entry = bars[r]['c']; stop = ob_bot - OBFVG_BUF*a
                    if stop < entry and _aligned(bars[r+1]['_ts'], 'bull'):
                        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': 'bull',
                                    'entry_ts': bars[r+1]['_ts'], 'entry': entry, 'stop': stop})
                    last = r + OBFVG_COOLDOWN; break
    return out


def detect_obfvg(pk, h1, daily):
    """Observed candidate #18 — OB+FVG retrace (market entry, fixed 2:1), XRPUSD +
    USDCAD H1 only. Parameter-robust across the impulse/OB/retrace grid (22/26 and
    20/26 combos PASS; +0.23..+0.29R median) — found by per-pair scan, so pair-scoped
    and watch-forward. cBot-executable."""
    if pk not in OBFVG_LIVE:
        return []
    return _obfvg_signals(pk, h1, 'obfvg', daily=daily)


def detect_obfvg_watch(pk, h1, daily):
    """WATCH cells — FTSE100 + BTCUSD H1. Base-positive but grid-borderline (3/19 and
    9/25 combos PASS); shadow-logged only (NOT fed to the cBot) to decide edge vs
    noise on forward data."""
    if pk not in OBFVG_WATCH:
        return []
    return _obfvg_signals(pk, h1, 'obfvg_w', daily=daily)


OBFVG_FX4_HOLD = 60          # 4h bars (~10 trading days) to reach the RR2 target


def detect_obfvg_fx4(pk, h1, daily):
    """SHADOW OBSERVER — Alex Morris / Trading Cafe 4H order-block setup on FX
    minor+major (his universe). Identical OB+FVG retrace logic as the live obfvg
    cell, but on 4h zones and OBSERVE-ONLY. Systematic realistic-fill testing showed
    FX negative at every RR and both zone TFs (alex_morris_ob_research.py), and the
    posted 'winning setups' are a selected-winner highlight reel with no base rate —
    so this is logged, NOT fed to the cBot. It accumulates forward, out-of-sample
    evidence on his exact method so we can revisit on live data, not backtest."""
    if PAIR_CLASS.get(pk) not in ('minor', 'major'):
        return []
    return _obfvg_signals(pk, agg4h(h1), 'obfvg_fx4', '4h', daily=daily)


# ── Gold playbook (Audacity Capital list) — tested in gold_strategies_research.py ──
# XAUUSD-only. #3 breakout + #1 trend go LIVE (cBot feed); #7 fib is MONITOR-only.
GOLD = 'xauusd'
GBUF = 0.25                 # structural stop buffer, in ATR
GBREAK_LOOK = 48            # #3 recent-range Donchian lookback (h1 bars)
GBREAK_ATRLB = 10           # #3 ATR-expansion comparison lookback
GBREAK_HOLD = 80            # #3/#7 h1 bars to reach the RR2 target
GTREND_HOLD = 80            # #1 4h bars to reach the RR2 target
GFIB_W = 5                  # #7 pivot half-window


def _ema(vals, period):
    k = 2.0 / (period + 1); out = [None] * len(vals); e = None
    for i, v in enumerate(vals):
        e = v if e is None else v * k + e * (1 - k); out[i] = e
    return out


def _donchian(bars, i, look):
    """Recent range [lo,hi] over the `look` bars ending just before i (a 20-DAY
    daily envelope is useless in trending gold — price never leaves it)."""
    if i < look:
        return None
    seg = bars[i - look:i]
    return (min(x['l'] for x in seg), max(x['h'] for x in seg))


def detect_gbreak(pk, h1, daily):
    """#3 Breakout + expanding-ATR (XAUUSD H1) — LIVE. Close (not wick) beyond the
    recent Donchian range WITH rising ATR; stop 1 ATR back inside the level; market
    entry, RR2. Robust: 15/15 parameter cells PASS both OOS halves on gold, silver
    corroborates, and the ATR filter ~doubles expectancy."""
    if pk != GOLD:
        return []
    out = []; n = len(h1)
    for i in range(GBREAK_LOOK + 2, n - 1):
        band = _donchian(h1, i, GBREAK_LOOK)
        if not band:
            continue
        lo, hi = band; a = atr(h1, 14, i); ap = atr(h1, 14, i - GBREAK_ATRLB)
        if a is None or ap is None or a <= 0 or a <= ap:   # require expanding volatility
            continue
        b = h1[i]
        if b['c'] > hi:
            entry = b['c']; stop = hi - 1.0 * a
            if stop < entry:
                out.append({'strategy': 'gbreak', 'tf': 'h1', 'pair': pk, 'dir': 'bull',
                            'entry_ts': h1[i + 1]['_ts'], 'entry': entry, 'stop': stop})
        elif b['c'] < lo:
            entry = b['c']; stop = lo + 1.0 * a
            if stop > entry:
                out.append({'strategy': 'gbreak', 'tf': 'h1', 'pair': pk, 'dir': 'bear',
                            'entry_ts': h1[i + 1]['_ts'], 'entry': entry, 'stop': stop})
    return out


def detect_gtrend(pk, h1, daily):
    """#1 Trend-following (XAUUSD H4, 50/200 EMA, pullback-to-50) — LIVE. Choppiness
    filter REMOVED (it hurt expectancy in testing). Longs only above both EMAs / shorts
    below; entry when a bar pulls back to touch the 50 EMA and holds in-trend on close;
    stop beyond the pullback extreme; market entry, RR2. Positive both OOS halves,
    gold + silver."""
    if pk != GOLD:
        return []
    b4 = agg4h(h1); n = len(b4)
    e50 = _ema([x['c'] for x in b4], 50); e200 = _ema([x['c'] for x in b4], 200)
    out = []
    for i in range(201, n - 1):
        if e50[i] is None or e200[i] is None:
            continue
        b = b4[i]
        bull = e50[i] > e200[i] and b['c'] > e50[i] and b['c'] > e200[i]
        bear = e50[i] < e200[i] and b['c'] < e50[i] and b['c'] < e200[i]
        if not (bull or bear):
            continue
        touched = b['l'] <= e50[i] <= b['h']
        held = (b['c'] > e50[i]) if bull else (b['c'] < e50[i])
        if not (touched and held):
            continue
        a = atr(b4, 14, i) or 0.0; d = 'bull' if bull else 'bear'
        entry = b['c']; stop = (b['l'] - GBUF * a) if bull else (b['h'] + GBUF * a)
        if (bull and stop < entry) or (bear and stop > entry):
            out.append({'strategy': 'gtrend', 'tf': '4h', 'pair': pk, 'dir': d,
                        'entry_ts': b4[i + 1]['_ts'], 'entry': entry, 'stop': stop})
    return out


def detect_gtrend_inv(pk, h1, daily):
    """OBSERVER — inverted gtrend: buy<->sell, stop mirrored to the opposite side (same R
    distance), same entry/time, scored at the same RR/hold. 2026-08-12: gtrend collapsed
    forward (fwd -0.92R, 3% WR) while +ve in-sample — it's fighting the current gold regime.
    Track its mirror to judge whether the inverse is a durable edge or just curve-fit to the
    recent trend. NEVER sent to the cBot (observer only)."""
    out = []
    for s in detect_gtrend(pk, h1, daily):
        e = s['entry']
        out.append({'strategy': 'gtrend_inv', 'tf': '4h', 'pair': pk,
                    'dir': 'bear' if s['dir'] == 'bull' else 'bull',
                    'entry_ts': s['entry_ts'], 'entry': e, 'stop': 2 * e - s['stop']})
    return out


def detect_gfib(pk, h1, daily):
    """#7 Fibonacci pullback (XAUUSD H1) — MONITOR ONLY (not fed to the cBot yet).
    After a clear leg, continuation entry on a retrace into the 38.2-61.8 zone; the leg
    is failed (skip) if the retrace exceeds 78.6; stop beyond the swing origin; market
    entry, RR2. Thin but both OOS halves + on gold H1 — watch forward before wiring."""
    if pk != GOLD:
        return []
    bars = h1; n = len(bars); out = []
    ph = []; pl = []
    for i in range(GFIB_W, n - GFIB_W):
        seg = bars[i - GFIB_W:i + GFIB_W + 1]
        if bars[i]['h'] == max(x['h'] for x in seg): ph.append(i)
        if bars[i]['l'] == min(x['l'] for x in seg): pl.append(i)
    for i in range(2 * GFIB_W + 5, n - 1):
        b = bars[i]; a = atr(bars, 14, i) or 0.0
        li = bisect.bisect_left(pl, i - GFIB_W) - 1; hi_ = bisect.bisect_left(ph, i - GFIB_W) - 1
        L = pl[li] if li >= 0 else None; H = ph[hi_] if hi_ >= 0 else None
        if L is None or H is None:
            continue
        if L < H:   # up-leg, bull continuation on the retrace down
            lo = bars[L]['l']; hh = bars[H]['h']; rng = hh - lo
            if rng > 0 and (hh - 0.618 * rng) <= b['l'] <= (hh - 0.382 * rng) \
               and b['l'] >= hh - 0.786 * rng and b['c'] > b['o']:
                entry = b['c']; stop = lo - GBUF * a
                if stop < entry:
                    out.append({'strategy': 'gfib', 'tf': 'h1', 'pair': pk, 'dir': 'bull',
                                'entry_ts': bars[i + 1]['_ts'], 'entry': entry, 'stop': stop})
        elif H < L:  # down-leg, bear continuation on the retrace up
            hh = bars[H]['h']; lo = bars[L]['l']; rng = hh - lo
            if rng > 0 and (lo + 0.382 * rng) <= b['h'] <= (lo + 0.618 * rng) \
               and b['h'] <= lo + 0.786 * rng and b['c'] < b['o']:
                entry = b['c']; stop = hh + GBUF * a
                if stop > entry:
                    out.append({'strategy': 'gfib', 'tf': 'h1', 'pair': pk, 'dir': 'bear',
                                'entry_ts': bars[i + 1]['_ts'], 'entry': entry, 'stop': stop})
    return out


# ── 90-EMA range break (TikTok 'reversal vs not') — CRYPTO-ONLY H1 OBSERVER at 1.5:1 ──
# Consolidation around the 90 EMA forms a range; a close beyond the range is the entry
# (long on an up-break, short on a down-break), stop the opposite side, target 1.5R.
# Universe-wide it has no edge, but crypto-h1 is a thin real edge (BTC/ETH/XRP/SUI pass
# both OOS halves, +0.069R pooled) — shadow-only, not fed to the cBot.
E90_EMA = 90
E90_LOOK = 12
E90_MAXATR = 3.0
E90_BUF = 0.10
E90_RR = 1.5
E90_HOLD = 90
E90_COOLDOWN = 5


def detect_e90break(pk, h1, daily):
    if PAIR_CLASS.get(pk) != 'crypto':
        return []
    e = _ema([b['c'] for b in h1], E90_EMA); n = len(h1); out = []; last = -1
    for i in range(E90_EMA + E90_LOOK, n - 1):
        if i <= last or e[i] is None:
            continue
        a = atr(h1, 14, i) or 0.0
        if a <= 0:
            continue
        seg = h1[i - E90_LOOK:i]
        box_hi = max(x['h'] for x in seg); box_lo = min(x['l'] for x in seg)
        if not (box_lo <= e[i] <= box_hi):           # EMA inside the range = consolidation
            continue
        if (box_hi - box_lo) > E90_MAXATR * a:        # tight consolidation only
            continue
        c = h1[i]['c']; d = None
        if c > box_hi:
            d = 'bull'; entry = c; stop = box_lo - E90_BUF * a
        elif c < box_lo:
            d = 'bear'; entry = c; stop = box_hi + E90_BUF * a
        if not d:
            continue
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            last = i + E90_COOLDOWN; continue
        out.append({'strategy': 'e90break', 'tf': 'h1', 'pair': pk, 'dir': d,
                    'entry_ts': h1[i + 1]['_ts'], 'entry': entry, 'stop': stop, 'rr': E90_RR})
        last = i + E90_COOLDOWN
    return out


# ── 'Money move' — imbalance / fair-value-gap retrace continuation — SCOPED OBSERVERS (RR2) ──
# Strong impulse leaves a 3-candle FVG; price retraces INTO the gap and holds (test); enter in
# the impulse direction, stop just beyond the gap's far edge, target 2:1 (the money move). FVG
# family (cousin of obfvg). Universe-wide it has no edge, but per-pair-verified pockets pass
# both OOS halves — wired as MONITOR-ONLY observers, not fed to the cBot:
#   mmove     crypto H1 (5/7 coins pass incl. BTC)
#   mmove_ix  index H1 — DE40 + JP225 only
#   mmove_ix4 index 4H — FTSE / DJ30 / NAS100 / JP225
#   mmove_c4  commodities 4H — silver / WTI / natgas
MMOVE_IMP = 1.0
MMOVE_RETR = 24
MMOVE_BUF = 0.10
MMOVE_COOLDOWN = 3
MMOVE_HOLD = 80
MMOVE_IX1 = {'de40', 'jp225'}
MMOVE_IX4 = {'ftse100', 'dj30', 'nas100', 'jp225'}
MMOVE_C4 = {'xagusd', 'wtiusd', 'natgas'}
# m15 port (mmove_m15_research.py, 2026-08-06). Universe-wide the FVG edge has no
# edge at 15m, but four per-pair pockets pass BOTH OOS halves at every tested hold
# (20/48/96h): XRPUSD, XAUUSD, XAGUSD, FRA40. MONITOR-ONLY observers — thin
# (+0.02..0.09R), never fed to the cBot; logged here to build genuine forward
# evidence before any promotion call.
MMOVE_M15 = {'xrpusd', 'xauusd', 'xagusd', 'fra40'}
MMOVE_M15_HOLD = 192   # 48h on m15 (edge stable across the swept holds in research)


def _mmove_signals(bars, pk, tag, tf):
    n = len(bars); out = []; last = -1
    for i in range(15, n - 2):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        body = bars[i]['c'] - bars[i]['o']
        if body >= MMOVE_IMP * a and bars[i + 1]['l'] > bars[i - 1]['h']:      # bullish FVG
            g_bot = bars[i - 1]['h']; g_top = bars[i + 1]['l']
            for r in range(i + 2, min(i + 2 + MMOVE_RETR, n - 1)):
                b = bars[r]
                if b['l'] <= g_top and b['c'] > g_bot:
                    entry = b['c']; stop = g_bot - MMOVE_BUF * a
                    if stop < entry:
                        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': 'bull',
                                    'entry_ts': bars[r + 1]['_ts'], 'entry': entry, 'stop': stop})
                    last = r + MMOVE_COOLDOWN; break
                if b['c'] < g_bot:
                    break
        elif -body >= MMOVE_IMP * a and bars[i + 1]['h'] < bars[i - 1]['l']:   # bearish FVG
            g_top = bars[i - 1]['l']; g_bot = bars[i + 1]['h']
            for r in range(i + 2, min(i + 2 + MMOVE_RETR, n - 1)):
                b = bars[r]
                if b['h'] >= g_bot and b['c'] < g_top:
                    entry = b['c']; stop = g_top + MMOVE_BUF * a
                    if stop > entry:
                        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': 'bear',
                                    'entry_ts': bars[r + 1]['_ts'], 'entry': entry, 'stop': stop})
                    last = r + MMOVE_COOLDOWN; break
                if b['c'] > g_top:
                    break
    return out


def detect_mmove(pk, h1, daily):
    out = []
    if PAIR_CLASS.get(pk) == 'crypto':
        out += _mmove_signals(h1, pk, 'mmove', 'h1')
    if pk in MMOVE_IX1:
        out += _mmove_signals(h1, pk, 'mmove_ix', 'h1')
    if pk in MMOVE_IX4 or pk in MMOVE_C4:
        b4 = agg4h(h1)
        if pk in MMOVE_IX4:
            out += _mmove_signals(b4, pk, 'mmove_ix4', '4h')
        if pk in MMOVE_C4:
            out += _mmove_signals(b4, pk, 'mmove_c4', '4h')
    return out


def detect_mmove_m15(pk, m15):
    """m15 FVG retrace-continuation, scoped to the four passing pockets. Observer-only."""
    if pk not in MMOVE_M15 or len(m15) < 400:
        return []
    return _mmove_signals(m15, pk, 'mmove_m15', 'm15')


# OB+FVG retrace ported to m15 (obfvg_m15_research.py, 2026-08-06). Crypto passes
# (+0.10R, both OOS halves +); BTCUSD + XRPUSD pass individually at 24h and 48h holds.
# MONITOR-ONLY observer, scoped to those two pockets. RR2, 24h hold.
OBFVG_M15 = {'btcusd', 'xrpusd'}
OBFVG_M15_HOLD = 96


def detect_obfvg_m15(pk, m15, daily):
    """m15 OB+FVG retrace, scoped to the two passing crypto pockets. Observer-only."""
    if pk not in OBFVG_M15 or len(m15) < 400:
        return []
    return _obfvg_signals(pk, m15, 'obfvg_m15', 'm15', daily)


# ── Equity Opening-Range Breakout (equity_orb_research.py, 2026-08-06) — the first
# EQUITY intraday edge. On real US-equity m15 with REAL share volume, ORB gated to
# high relative-volume breakouts passes both OOS halves (relvol>=1.5, +0.018R ALL;
# TSLA/AMZN per-pair). Reads equity-ohlc.json when the equity-pilot workflow has
# committed it. Observer-only; ORB is a day-trade so it's scored to SESSION CLOSE.
EQUITY_HIST = os.path.join(_HERE, 'equity-ohlc.json')
EQUITY_SYMBOLS = ['aapl', 'nvda', 'tsla', 'msft', 'amzn']
ORB_OR_BARS = 2          # opening range = first 30 min (RTH-only bars → first-of-day = the open)
ORB_REL = 1.5            # relative-volume gate on the breakout bar (the passing variant)
ORB_VOL_LB = 20


def _orb_eq_signals(pk, m15):
    if len(m15) < 200:
        return []
    by_day = {}
    for i, b in enumerate(m15):
        by_day.setdefault(int(b['_ts'] // 86400) if b['_ts'] else 0, []).append(i)
    out = []
    for day in sorted(by_day):
        idxs = by_day[day]
        if len(idxs) < ORB_OR_BARS + 3:
            continue
        seg = idxs[:ORB_OR_BARS]
        hi = max(m15[j]['h'] for j in seg); lo = min(m15[j]['l'] for j in seg)
        if hi <= lo:
            continue
        h = hi - lo; last_idx = idxs[-1]
        for j in idxs[ORB_OR_BARS:]:
            b = m15[j]
            d = 'bull' if b['c'] > hi else ('bear' if b['c'] < lo else None)
            if not d:
                continue
            if j < ORB_VOL_LB or j + 1 >= len(m15):
                break
            avg = sum((m15[x].get('v', 0) or 0) for x in range(j - ORB_VOL_LB, j)) / ORB_VOL_LB
            if avg <= 0 or (m15[j].get('v', 0) or 0) / avg < ORB_REL:     # participation gate
                break
            entry = b['c']; stop = lo if d == 'bull' else hi
            if abs(entry - stop) <= 0:
                break
            tgt = entry + h if d == 'bull' else entry - h                 # target = 1x opening range
            out.append({'strategy': 'orb_eq', 'tf': 'm15', 'pair': pk, 'dir': d,
                        'entry_ts': m15[j + 1]['_ts'], 'entry': entry, 'stop': stop,
                        'target': tgt, 'session_end_ts': m15[last_idx]['_ts']})
            break                                                         # one ORB trade per session
    return out


def score_orb(bars, entry_ts, entry, stop, target, d, session_end_ts):
    """Target-or-stop first-touch, else mark-to-market at SESSION CLOSE (ORB is a
    day-trade — the close exit IS the strategy, not a hold-timeout to exclude)."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return ('pending', None)
    se = bisect.bisect_left(ts, session_end_ts)
    if se >= len(bars):
        return ('pending', None)                       # session not complete in data yet
    for j in range(i0, se + 1):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= target: return ('resolved', (target - entry) / R)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= target: return ('resolved', (entry - target) / R)
    cl = bars[se]['c']
    return ('resolved', ((cl - entry) if d == 'bull' else (entry - cl)) / R)


# ── Value-area failed-breakout reversal (fabervaale) — the REAL volume version
# (value_area_volume_research.py, 2026-08-06). Now that we carry volume, the profile is
# VOLUME-at-price and the filter is real declining volume. Universe-wide it fails, but two
# index pockets pass both OOS halves on RR2: DJ30 (+0.395R) and FRA40 (+0.166R). Uses
# OANDA tick volume (index proxy). MONITOR-ONLY observer, scoped to those two pockets.
VAREV_IX = {'dj30', 'fra40'}
VAREV_L = 96; VAREV_BINS = 40; VAREV_VA_PCT = 0.70; VAREV_STEP = 2
VAREV_DECL_VOL = 0.90; VAREV_VOL_LB = 20; VAREV_RECLAIM = 24; VAREV_BUF = 0.10; VAREV_HOLD = 120


def _va_profile(win):
    lo = min(b['l'] for b in win); hi = max(b['h'] for b in win)
    if hi <= lo:
        return None
    w = (hi - lo) / VAREV_BINS; counts = [0.0] * VAREV_BINS
    for b in win:
        v = b.get('v', 0) or 0.0
        if v <= 0:
            continue
        b0 = int((b['l'] - lo) / w); b1 = int((b['h'] - lo) / w)
        b0 = 0 if b0 < 0 else (VAREV_BINS - 1 if b0 > VAREV_BINS - 1 else b0)
        b1 = 0 if b1 < 0 else (VAREV_BINS - 1 if b1 > VAREV_BINS - 1 else b1)
        share = v / (b1 - b0 + 1)
        for k in range(b0, b1 + 1):
            counts[k] += share
    total = sum(counts)
    if total <= 0:
        return None
    poc = max(range(VAREV_BINS), key=lambda k: counts[k])
    acc = counts[poc]; lo_i = hi_i = poc; tgt = VAREV_VA_PCT * total
    while acc < tgt and (lo_i > 0 or hi_i < VAREV_BINS - 1):
        left = counts[lo_i - 1] if lo_i > 0 else -1
        right = counts[hi_i + 1] if hi_i < VAREV_BINS - 1 else -1
        if right >= left:
            hi_i += 1; acc += counts[hi_i]
        else:
            lo_i -= 1; acc += counts[lo_i]
    return lo + lo_i * w, lo + (hi_i + 1) * w


def _varev_signals(bars, pk, tag):
    n = len(bars); out = []; last = -1; va = None
    for i in range(VAREV_L, n - 1):
        if i <= last:
            continue
        if (i % VAREV_STEP) == 0 or va is None:
            va = _va_profile(bars[i - VAREV_L:i])
        if va is None:
            continue
        val, vah = va; c = bars[i]['c']; a = atr(bars, 14, i) or 0.0
        if a <= 0 or not (val < c < vah):
            continue
        for j in range(i + 1, min(i + 1 + VAREV_RECLAIM, n - 1)):
            bj = bars[j]; up = bj['c'] > vah; dn = bj['c'] < val
            if not (up or dn):
                continue
            if j < VAREV_VOL_LB:
                last = j; break
            avgv = sum((bars[x].get('v', 0) or 0) for x in range(j - VAREV_VOL_LB, j)) / VAREV_VOL_LB
            weak = avgv > 0 and (bj.get('v', 0) or 0) <= VAREV_DECL_VOL * avgv
            if not weak:
                last = j; break
            side = 'up' if up else 'dn'; ext = bj['h'] if up else bj['l']
            for k in range(j + 1, min(j + 1 + VAREV_RECLAIM, n - 1)):
                bk = bars[k]
                ext = max(ext, bk['h']) if side == 'up' else min(ext, bk['l'])
                reclaim = (bk['c'] < vah) if side == 'up' else (bk['c'] > val)
                if reclaim:
                    d = 'bear' if side == 'up' else 'bull'; entry = bk['c']
                    stop = ext + VAREV_BUF * a if d == 'bear' else ext - VAREV_BUF * a
                    if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry):
                        break
                    if k + 1 < n:
                        out.append({'strategy': tag, 'tf': 'h1', 'pair': pk, 'dir': d,
                                    'entry_ts': bars[k + 1]['_ts'], 'entry': entry, 'stop': stop})
                    last = k + 6; break
            break
    return out


def detect_varev_ix(pk, h1):
    """Value-area failed-breakout reversal, scoped to the two passing index pockets. Observer-only."""
    if pk not in VAREV_IX or len(h1) < VAREV_L + 300:
        return []
    return _varev_signals(h1, pk, 'varev_ix')


# ── Volume observer (m15, CRYPTO only — real Coinbase volume) ─────────────────────
# EMA9/20 pullback GATED to high relative-volume bars. volume_gate_research.py
# (2026-08-06): the relvol gate rescued this from -0.016R to a PASS on crypto, and
# it survives strict bracket scoring (+0.022R, both OOS halves +) — the same
# discipline the cBot trades under. Thin → MONITOR-ONLY, never fed to the cBot.
#
# NB: VWAP mean-reversion was ALSO a research "pass", but only under a mark-to-
# market timeout convention; under the harness's bracket scoring (target-or-stop,
# timeouts excluded) it is -0.150R, so it was NOT wired — a robust edge must not
# depend on the exit convention.
EMA920V_REL = 1.5         # relative-volume gate that rescued EMA9/20 on crypto
EMA920V_VOL_LB = 20
EMA920V_RR = 1.5
EMA920V_HOLD = 40
VWVOL_COOLDOWN = 2


def detect_ema920v_m15(pk, m15):
    """9/20-EMA pullback-in-trend, gated to bars with relvol >= EMA920V_REL. RR1.5."""
    if PAIR_CLASS.get(pk) != 'crypto' or len(m15) < 400:
        return []
    c = [x['c'] for x in m15]; e9 = ema(c, 9); e20 = ema(c, 20)
    out = []; last = -1
    for i in range(max(25, EMA920V_VOL_LB), len(m15) - 1):
        if i <= last or e9[i] is None or e20[i] is None:
            continue
        a = atr(m15, 14, i)
        if not a or a <= 0:
            continue
        seg = m15[i - EMA920V_VOL_LB:i]
        avg = sum(x.get('v', 0) or 0 for x in seg) / EMA920V_VOL_LB
        if avg <= 0 or (m15[i].get('v', 0) or 0) / avg < EMA920V_REL:   # participation gate
            continue
        b = m15[i]
        if e9[i] > e20[i] and b['l'] <= e20[i] and b['c'] > e9[i]:
            stop = e20[i] - a
            if stop < b['c']:
                out.append({'strategy': 'ema920v', 'tf': 'm15', 'pair': pk, 'dir': 'bull',
                            'entry_ts': m15[i + 1]['_ts'], 'entry': b['c'], 'stop': stop})
                last = i + VWVOL_COOLDOWN
        elif e9[i] < e20[i] and b['h'] >= e20[i] and b['c'] < e9[i]:
            stop = e20[i] + a
            if stop > b['c']:
                out.append({'strategy': 'ema920v', 'tf': 'm15', 'pair': pk, 'dir': 'bear',
                            'entry_ts': m15[i + 1]['_ts'], 'entry': b['c'], 'stop': stop})
                last = i + VWVOL_COOLDOWN
    return out


# NB: a volume-gated DAILY volatility breakout looked like a strong pass in the
# sweep (+0.121R), but that was a scoping bug — the sweep ran it over all 40 pairs,
# not its intended 18-pair futures universe. Re-scoped correctly and bracket-scored,
# it fails at every gate level (first OOS half negative). NOT wired. (Second
# false-positive caught this session by cross-checking against harness scoring.)


# ── Market Wizards continuation setups — TRAILING-runner observers, per class ──
# Raschke's 'Holy Grail' (ADX>25 strong trend + a pullback that tags the 20-EMA and
# closes back through it), the volume-breakout 'breakthrough' (break the 20-bar range
# by a margin on >=1.5x average volume, stop below the breakout bar), and Sperandeo's
# '2B' (a marginal new extreme that fails and closes back inside the prior swing).
# All are momentum/continuation entries: at a fixed RR2 target they die (WR too low to
# pay the target), but with a trailing RUNNER exit — arm +1R, trail 1R behind the best
# price, mark-to-market at the horizon — several classes pass BOTH OOS halves.
#
# The per-class study (market_wizards_breakdown.py) is the scoping authority. Only the
# (setup × class) combos that pass BOTH OOS halves + n>=40 are wired; the two that fail
# the OOS split are deliberately held back:
#   holy_grail  crypto ✓ equity ✓ comm ✓ | index ✗ (first half -0.015) — dropped
#   volbreak    crypto ✓ equity ✓ index ✓ | comm ✗ (split +0.146/-0.018) — dropped
#   2B          crypto ✓ equity ✓ index ✓ comm ✓
# FX majors/minors fail everything (spread is a big fraction of ATR and their ranges
# mean-revert through the trail) — never wired. On m15 holy_grail's CRYPTO edge decays
# (second half -0.034) so crypto stays h1-only, but equity m15 (+0.215R) and commodity
# m15 (+0.107R) pass and ARE wired as intraday observers. All MONITOR-ONLY; per-class
# tags (mmove_ix convention) so each combo can be promoted independently on its own
# forward evidence. Detectors reuse the researched signal generators.
from market_wizards_research import (holy_grail as _holygrail_sig,
                                     volbreak as _volbreak_sig, two_b as _twob_sig)
TRAIL_ARM = 1.0            # arm the trail at +1R
TRAIL_DIST = 1.0          # ride 1R behind the best price
TRAIL_HOLD = 200          # runner horizon (bars) — matches the breakdown study
# H1 pair-loop scoping (crypto/index/comm live in historical-ohlc.json). Equity lives
# in equity-ohlc.json and is wired in the equity block below.
HOLYGRAIL_H1 = {'crypto': 'holygrail', 'comm': 'holygrail_cm'}
VOLBREAK_H1 = {'crypto': 'volbreak', 'index': 'volbreak_ix'}
TWOB_H1 = {'crypto': 'twob', 'index': 'twob_ix', 'comm': 'twob_cm'}
HOLYGRAIL_M15 = {'comm': 'holygrail_cm_m15'}   # equity m15 wired in the equity block


def _mw_signals(bars, pk, tag, tf, gen):
    """Wrap a researched (ei, entry, stop, dir) generator into harness signal dicts."""
    out = []
    for (ei, entry, stop, d) in gen(bars):
        if ei >= len(bars):
            continue
        out.append({'strategy': tag, 'tf': tf, 'pair': pk, 'dir': d,
                    'entry_ts': bars[ei]['_ts'], 'entry': entry, 'stop': stop})
    return out


def detect_holygrail(pk, h1):
    tag = HOLYGRAIL_H1.get(PAIR_CLASS.get(pk))
    return _mw_signals(h1, pk, tag, 'h1', _holygrail_sig) if tag and len(h1) >= 400 else []


def detect_volbreak(pk, h1):
    tag = VOLBREAK_H1.get(PAIR_CLASS.get(pk))
    return _mw_signals(h1, pk, tag, 'h1', _volbreak_sig) if tag and len(h1) >= 400 else []


def detect_twob(pk, h1):
    tag = TWOB_H1.get(PAIR_CLASS.get(pk))
    return _mw_signals(h1, pk, tag, 'h1', _twob_sig) if tag and len(h1) >= 400 else []


def detect_holygrail_m15(pk, m15):
    tag = HOLYGRAIL_M15.get(PAIR_CLASS.get(pk))
    return _mw_signals(m15, pk, tag, 'm15', _holygrail_sig) if tag and len(m15) >= 400 else []


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


def score_trail_open(bars, entry_ts, entry, stop, d, hold, arm, trail):
    """Trailing-runner scorer for the continuation setups (holygrail / volbreak).
    Unlike the bracket score(), there is no fixed target: arm the trail at +arm*R,
    then ride the best price with the stop trail*R behind it; mark-to-market at the
    hold horizon (a runner is still an OPEN position at timeout, not a time-stop to
    exclude). 'resolved' once stopped out OR the horizon is reached with data in
    hand; 'pending' only while we lack enough bars past entry to reach the horizon."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars): return ('pending', None)
    es = stop; armed = False; peak = entry; end = i0 + hold
    for j in range(i0, min(end, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es: return ('resolved', (es - entry) / R)
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry + arm * R: armed = True
            if armed: es = max(es, peak - trail * R)
        else:
            if b['h'] >= es: return ('resolved', (entry - es) / R)
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry - arm * R: armed = True
            if armed: es = min(es, peak + trail * R)
    if end <= len(bars):                                   # horizon reached, data in hand
        lc = bars[end - 1]['c']
        return ('resolved', ((lc - entry) if d == 'bull' else (entry - lc)) / R)
    return ('pending', None)                               # ran out of data before horizon


# ── Gold US-session 2nd-hour reversal — TRUE 5-minute observer ──────────────────
# itstomtrades setup (session_2h_reversal_research.py): in the 2nd hour of a session,
# an expansion + rising-volume directional move, then a structure-shift reversal
# against it; stop beyond the swing, target the breakout range. The faithful m5 test
# (12 months of real XAU_USD 5-minute candles in gold-m5-ohlc.json) found only the US
# 2nd hour @ RR1.5 durable — BOTH OOS halves positive, and it was ALSO positive on the
# independent full-year h1 sample. Asia loses and London's edge decays out-of-sample,
# so only US is wired. Thin (n~30) — MONITOR-ONLY, accruing forward evidence toward the
# n>=40 gate as the monthly m5 backfill grows the window.
GOLD_M5_HIST = os.path.join(_HERE, 'gold-m5-ohlc.json')
SESS_US_HOUR = _SESS_HOURS['us']            # 2nd US hour = 14:00 UTC
SESS_RR = 1.5                               # the durable reward:risk from the m5 study
SESS_HOLD = _SESS_GEO['m5']['HOLD']         # 288 m5 bars (~24h) bracket horizon

# ── BTC absorption (real-delta) observer — separate data source ──────────────────
# The Whale-Pivot-Model absorption idea (fade aggressive one-sided delta that price
# fails to follow), tested on Binance real delta. Only BTC 15m passed both OOS halves
# (n~851); ETH marginal, XRP/SOL failed — so it's BTC-specific and MONITOR-ONLY, kept
# here to accrue forward evidence before any conviction. Reads binance-crypto-ohlcv.json
# (refreshed MONTHLY by the Binance fetch), so forward evidence grows in monthly steps
# unless a live delta ingester / daily-dump fetch is added. Needs the delta key, which
# _bars_norm strips — hence the delta-preserving _delta_norm.
BINANCE_CRYPTO = os.path.join(_HERE, 'binance-crypto-ohlcv.json')
ABSORB_RR = 2.0                             # the passing reward:risk on BTC 15m
ABSORB_HOLD = 160                           # m15 bars bracket horizon


def _absorb_btc_signals(m15):
    out = []
    for (ei, entry, stop, d) in _absorb_signals(m15):
        if ei >= len(m15):
            continue
        R = abs(entry - stop)
        target = entry + ABSORB_RR * R if d == 'bull' else entry - ABSORB_RR * R
        out.append({'strategy': 'absorb_btc', 'tf': 'm15', 'pair': 'btcusd', 'dir': d,
                    'entry_ts': m15[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': target})
    return out


def _gold_us2h_signals(m5):
    """US 2nd-hour session-reversal signals on true m5, targeted at RR1.5."""
    out = []
    for (ei, entry, stop, td, _rng) in _sess_signals(m5, SESS_US_HOUR, _SESS_GEO['m5']):
        if ei >= len(m5):
            continue
        R = abs(entry - stop)
        d = 'bear' if td == 'short' else 'bull'
        target = entry - SESS_RR * R if td == 'short' else entry + SESS_RR * R
        out.append({'strategy': 'gold_us2h', 'tf': 'm5', 'pair': 'xauusd', 'dir': d,
                    'entry_ts': m5[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': target})
    return out


# ── FMA ($100->$1M Millionaire Trading Academy) sweep + 50-EMA-reclaim reversal ──
# m15: sweep a 20-bar swing extreme that closes back inside (liquidity grab), then a
# 50-EMA reclaim confirms the reversal; stop beyond the sweep, fixed RR2 target. The
# research (fma_sweep_reversal_research.py) found it passes BOTH OOS halves on gold
# (cross-validated on native m15 AND 12-month m5->m15), commodities and index at RR2;
# FX majors/minors fail. Wired to comm (incl. gold) + index per the scoping decision,
# gold on its own tag for visibility. MONITOR-ONLY m15 observers.
FMA_CLASSES = {'comm', 'index'}
FMA_RR = 2.0
FMA_HOLD = 192          # m15 bars (~2 days) bracket horizon


def detect_fma(pk, m15):
    cls = PAIR_CLASS.get(pk)
    if cls not in FMA_CLASSES or len(m15) < 400:
        return []
    tag = 'fma_gold' if pk == 'xauusd' else ('fma_sweep_ix' if cls == 'index' else 'fma_sweep_cm')
    out = []
    for (ei, entry, stop, d, _opp) in _fma_signals(m15):
        if ei >= len(m15):
            continue
        R = abs(entry - stop)
        target = entry + FMA_RR * R if d == 'bull' else entry - FMA_RR * R
        out.append({'strategy': tag, 'tf': 'm15', 'pair': pk, 'dir': d,
                    'entry_ts': m15[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': target})
    return out


# ── PO3 (Power-of-Three) London-open sweep-reversal — NON-GOLD commodities ──────
# From the astongilltrading ensemble (astongill_orb_po3_research.py): the London-open
# opening range is 'accumulation', a sweep beyond it that closes back inside is
# 'manipulation', enter the 'distribution' move the other way (RR2). The research found
# a real edge on energy/metals commodities EXCLUDING gold (silver/oil/natgas/platinum)
# — +0.18R, both OOS halves + (n=270); gold alone fails. MONITOR-ONLY m15 observer,
# scoped to non-gold commodities to diversify the observer set.
PO3_HOUR = _PO3_SESS['LN']      # London open = 07:00 UTC
PO3_RR = 2.0
PO3_HOLD = 192                  # m15 bars (~2 days)


def detect_po3(pk, m15):
    if PAIR_CLASS.get(pk) != 'comm' or pk == 'xauusd' or len(m15) < 400:
        return []
    out = []
    for (ei, entry, stop, d) in _po3_signals(m15, PO3_HOUR):
        if ei >= len(m15):
            continue
        R = abs(entry - stop)
        target = entry + PO3_RR * R if d == 'bull' else entry - PO3_RR * R
        out.append({'strategy': 'po3_cm', 'tf': 'm15', 'pair': pk, 'dir': d,
                    'entry_ts': m15[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': target})
    return out


# ── Liquidity-sweep reversal + FVG+fib continuation (nefarioustrades) — INDEX m15 ──
# Sweep a swing extreme that closes back inside, an opposite FVG forms (displacement),
# then price retraces into the 50-61.8% fib zone -> continuation entry, RR3 ('let it run
# to the drawn liquidity'). The per-class study (liquidity_sweep_fvg_research.py) found
# it durable ONLY on index m15 at RR3 (+0.058R, both OOS halves +, n=895); everything
# else fails. MONITOR-ONLY, scoped to indices.
SWEEPFVG_RR = 3.0
SWEEPFVG_HOLD = 160         # m15 bars


def detect_sweepfvg(pk, m15):
    if PAIR_CLASS.get(pk) != 'index' or len(m15) < 400:
        return []
    out = []
    for (ei, entry, stop, d) in _sweepfvg_signals(m15):
        if ei >= len(m15):
            continue
        R = abs(entry - stop)
        target = entry + SWEEPFVG_RR * R if d == 'bull' else entry - SWEEPFVG_RR * R
        out.append({'strategy': 'sweepfvg_ix', 'tf': 'm15', 'pair': pk, 'dir': d,
                    'entry_ts': m15[ei]['_ts'], 'entry': entry, 'stop': stop, 'target': target})
    return out


def score_sess(bars, entry_ts, entry, stop, target, d, hold):
    """Target-bracket scorer (explicit target, not RR-derived). Bracket-honest:
    unresolved within the hold is EXCLUDED, mirroring score()."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars): return ('pending', None)
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= target: return ('resolved', (target - entry) / R)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= target: return ('resolved', (entry - target) / R)
    return ('pending', None) if end >= len(bars) else ('expired', None)


def _bos_dir(bars, prd=3):
    """Most-recent break-of-structure direction as of each bar (swing pivots
    confirmed prd bars right, no lookahead). Used to tag signals bos_aligned."""
    n = len(bars)
    pl = sorted((i + prd, bars[i]['l']) for i in range(prd, n - prd)
                if all(bars[i]['l'] < bars[i-k]['l'] and bars[i]['l'] < bars[i+k]['l'] for k in range(1, prd+1)))
    ph = sorted((i + prd, bars[i]['h']) for i in range(prd, n - prd)
                if all(bars[i]['h'] > bars[i-k]['h'] and bars[i]['h'] > bars[i+k]['h'] for k in range(1, prd+1)))
    last_pl = [None]*n; last_ph = [None]*n; a = 0; b = 0
    for i in range(n):
        while a < len(pl) and pl[a][0] <= i:
            last_pl[i] = pl[a][1]; a += 1
        if i and last_pl[i] is None:
            last_pl[i] = last_pl[i-1]
        while b < len(ph) and ph[b][0] <= i:
            last_ph[i] = ph[b][1]; b += 1
        if i and last_ph[i] is None:
            last_ph[i] = last_ph[i-1]
    bdir = [None]*n; cd = None
    for i in range(n):
        if last_pl[i] is not None and bars[i]['c'] < last_pl[i]:
            cd = 'bear'
        if last_ph[i] is not None and bars[i]['c'] > last_ph[i]:
            cd = 'bull'
        bdir[i] = cd
    return bdir


def _nowick_side(b, tol=0.10, body_min=0.5):
    """A decisive candle with ~no wick on one side: bull = no lower wick (opened at
    the low, bullish body), bear = no upper wick. Generic candle shape, used to tag
    signals nowick_aligned (momentum-confirmation confluence)."""
    rng = b['h'] - b['l']
    if rng <= 0 or abs(b['c'] - b['o']) < body_min * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= tol * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= tol * rng:
        return 'bear'
    return None


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))   # for the scoped mmove_m15 observer only
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1); data_end = max(data_end, h1[-1]['_ts'])
        rsi4 = precompute_rsi([b['c'] for b in b4], 14)
        bos = {'h1': _bos_dir(h1), '4h': _bos_dir(b4), 'daily': _bos_dir(daily)}
        bts = {'h1': [x['_ts'] for x in h1], '4h': [x['_ts'] for x in b4], 'daily': [x['_ts'] for x in daily]}
        found = (detect_hs(pk, h1, daily, draw) + detect_s5(pk, h1, daily, 'engulf')
                 + detect_s5(pk, h1, daily, 'rsi') + detect_ob(pk, h1, daily)
                 + detect_tl(pk, h1, daily) + detect_w5pb(pk, h1, daily)
                 + detect_s5_rsi_wide(pk, h1, daily) + detect_rsimr(pk, h1, daily)
                 + detect_fibgz(pk, h1, daily) + detect_fredtl(pk, h1, daily)
                 + detect_threepush(pk, h1, daily) + detect_engulf_manip(pk, h1, daily)
                 + detect_sweeprev(pk, h1, daily) + detect_asianglitch(pk, h1, daily)
                 + detect_wm(pk, h1, daily) + detect_sid(pk, h1, daily)
                 + detect_obfvg(pk, h1, daily) + detect_obfvg_watch(pk, h1, daily)
                 + detect_gbreak(pk, h1, daily) + detect_gtrend(pk, h1, daily)
                 + detect_gtrend_inv(pk, h1, daily)
                 + detect_gfib(pk, h1, daily) + detect_e90break(pk, h1, daily)
                 + detect_mmove(pk, h1, daily) + detect_obfvg_fx4(pk, h1, daily)
                 + detect_mmove_m15(pk, m15) + detect_ema920v_m15(pk, m15)
                 + detect_obfvg_m15(pk, m15, daily) + detect_varev_ix(pk, h1)
                 + detect_holygrail(pk, h1) + detect_volbreak(pk, h1)
                 + detect_twob(pk, h1) + detect_holygrail_m15(pk, m15)
                 + detect_fma(pk, m15) + detect_po3(pk, m15) + detect_sweepfvg(pk, m15))
        for s in found:
            detected += 1
            k = f"{s['strategy']}:{s['pair']}:{int(s['entry_ts'])}"
            if k not in sigs:
                s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
            rec = sigs[k]
            # tag confluence: does the break-of-structure at entry agree with the trade?
            # computed once per signal (entry structure is fixed); backfills old signals.
            if 'bos_aligned' not in rec:
                bd = bos.get(rec['tf']); tl = bts.get(rec['tf'])
                al = False
                if bd is not None and tl:
                    ci = bisect.bisect_left(tl, rec['entry_ts'])
                    al = ci >= 1 and ci <= len(bd) and bd[ci-1] == rec['dir']
                rec['bos_aligned'] = bool(al)
            # tag confluence: is the pre-entry bar a no-wick momentum candle in-trend?
            if 'nowick_aligned' not in rec:
                bn = {'h1': h1, '4h': b4, 'daily': daily}.get(rec['tf']); tl = bts.get(rec['tf'])
                nwa = False
                if bn and tl:
                    ci = bisect.bisect_left(tl, rec['entry_ts'])
                    nwa = ci >= 1 and ci <= len(bn) and _nowick_side(bn[ci-1]) == rec['dir']
                rec['nowick_aligned'] = bool(nwa)
            tf = rec['tf']
            bars = h1 if tf == 'h1' else (b4 if tf == '4h' else daily)
            hold = HS_HOLD if tf == 'h1' else (HOLD['4h'] if tf == '4h' else 20)
            if rec['strategy'] == 'rsimr':
                st, o = score_meanrev(b4, rsi4, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], RSIMR_HOLD)
            elif rec['strategy'] == 'sid':
                st, o = score_meanrev(b4, rsi4, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], SID_HOLD)
            elif rec['strategy'] == 'sweeprev':
                st, o = score_sweeprev(b4, rec['entry_ts'], rec['entry'], rec['stop'], rec['target'], rec['dir'], SWEEPREV_HOLD)
            elif rec['strategy'] == 'asianglitch':
                st, o = score_asianglitch(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], ASIANGLITCH_HOLD, rec.get('rr', ASIANGLITCH_RR))
            elif rec['strategy'] == 'wm':
                st, o = score_asianglitch(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], WM_HOLD, rec.get('rr', WM_RR))
            elif rec['strategy'] in ('obfvg', 'obfvg_w'):
                st, o = score(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], OBFVG_HOLD)
            elif rec['strategy'] == 'obfvg_fx4':
                st, o = score(b4, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], OBFVG_FX4_HOLD)
            elif rec['strategy'] in ('gbreak', 'gfib'):
                st, o = score(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], GBREAK_HOLD)
            elif rec['strategy'] in ('gtrend', 'gtrend_inv'):
                st, o = score(b4, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], GTREND_HOLD)
            elif rec['strategy'] == 'e90break':
                st, o = score_asianglitch(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], E90_HOLD, rec.get('rr', E90_RR))
            elif rec['strategy'] in ('mmove', 'mmove_ix'):
                st, o = score(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], MMOVE_HOLD)
            elif rec['strategy'] in ('mmove_ix4', 'mmove_c4'):
                st, o = score(b4, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], MMOVE_HOLD)
            elif rec['strategy'] == 'mmove_m15':
                st, o = score(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], MMOVE_M15_HOLD)
            elif rec['strategy'] == 'ema920v':
                st, o = score_asianglitch(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], EMA920V_HOLD, EMA920V_RR)
            elif rec['strategy'] == 'obfvg_m15':
                st, o = score(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], OBFVG_M15_HOLD)
            elif rec['strategy'] == 'varev_ix':
                st, o = score(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], VAREV_HOLD)
            elif rec['strategy'] in ('holygrail', 'holygrail_cm', 'volbreak', 'volbreak_ix', 'twob', 'twob_ix', 'twob_cm'):
                st, o = score_trail_open(h1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
            elif rec['strategy'] == 'holygrail_cm_m15':
                st, o = score_trail_open(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
            elif rec['strategy'] in ('fma_gold', 'fma_sweep_cm', 'fma_sweep_ix'):
                st, o = score_sess(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['target'], rec['dir'], FMA_HOLD)
            elif rec['strategy'] == 'po3_cm':
                st, o = score_sess(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['target'], rec['dir'], PO3_HOLD)
            elif rec['strategy'] == 'sweepfvg_ix':
                st, o = score_sess(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['target'], rec['dir'], SWEEPFVG_HOLD)
            else:
                st, o = score(bars, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], hold)
            rec['status'] = st
            if st == 'resolved':
                rec['r'] = o - cost(o, rec['entry'], abs(rec['entry']-rec['stop']))
            else:
                rec.pop('r', None)

    # ── Equity observers — separate data source (equity-ohlc.json, committed by the
    #    equity-pilot workflow). Processed here, outside the FX/crypto pair loop.
    #    orb_eq (m15 opening-range breakout, bracket-scored) + the Market Wizards
    #    trailing-runner setups that pass on US equities: holy_grail / volbreak / 2B on
    #    h1, and holy_grail on m15 (equity's intraday edge holds where crypto's decays).
    #    All monitor-only, _eq-suffixed tags. ──
    if os.path.exists(EQUITY_HIST):
        try:
            eqp = json.load(open(EQUITY_HIST)).get('pairs', {})
            for pk in EQUITY_SYMBOLS:
                em15 = _bars_norm(eqp.get(pk, {}).get('m15', []))
                eh1 = _bars_norm(eqp.get(pk, {}).get('h1', []))
                if len(em15) < 200:
                    continue
                data_end = max(data_end, em15[-1]['_ts'])
                eqsigs = list(_orb_eq_signals(pk, em15))
                if len(eh1) >= 400:
                    eqsigs += _mw_signals(eh1, pk, 'holygrail_eq', 'h1', _holygrail_sig)
                    eqsigs += _mw_signals(eh1, pk, 'volbreak_eq', 'h1', _volbreak_sig)
                    eqsigs += _mw_signals(eh1, pk, 'twob_eq', 'h1', _twob_sig)
                if len(em15) >= 400:
                    eqsigs += _mw_signals(em15, pk, 'holygrail_eq_m15', 'm15', _holygrail_sig)
                for s in eqsigs:
                    detected += 1
                    k = f"{s['strategy']}:{s['pair']}:{int(s['entry_ts'])}"
                    if k not in sigs:
                        s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
                    rec = sigs[k]
                    if rec['strategy'] == 'orb_eq':
                        st, o = score_orb(em15, rec['entry_ts'], rec['entry'], rec['stop'],
                                          rec['target'], rec['dir'], rec['session_end_ts'])
                    elif rec['strategy'] == 'holygrail_eq_m15':
                        st, o = score_trail_open(em15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
                    else:                       # holygrail_eq / volbreak_eq / twob_eq — h1 runner
                        st, o = score_trail_open(eh1, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], TRAIL_HOLD, TRAIL_ARM, TRAIL_DIST)
                    rec['status'] = st
                    if st == 'resolved':
                        rec['r'] = o - cost(o, rec['entry'], abs(rec['entry'] - rec['stop']))
                    else:
                        rec.pop('r', None)
        except Exception as e:
            print(f"equity observers skipped: {e}")

    # ── Gold m5 observer — separate data source (gold-m5-ohlc.json, committed by the
    #    gold-m5-fetch workflow). True 5-minute US 2nd-hour session reversal. ──
    if os.path.exists(GOLD_M5_HIST):
        try:
            gm5 = _bars_norm(json.load(open(GOLD_M5_HIST)).get('pairs', {}).get('xauusd', {}).get('m5', []))
            if len(gm5) >= 400:
                data_end = max(data_end, gm5[-1]['_ts'])
                for s in _gold_us2h_signals(gm5):
                    detected += 1
                    k = f"{s['strategy']}:{s['pair']}:{int(s['entry_ts'])}"
                    if k not in sigs:
                        s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
                    rec = sigs[k]
                    st, o = score_sess(gm5, rec['entry_ts'], rec['entry'], rec['stop'],
                                       rec['target'], rec['dir'], SESS_HOLD)
                    rec['status'] = st
                    if st == 'resolved':
                        rec['r'] = o - cost(o, rec['entry'], abs(rec['entry'] - rec['stop']))
                    else:
                        rec.pop('r', None)
        except Exception as e:
            print(f"gold m5 observer skipped: {e}")

    # ── BTC absorption observer — Binance real-delta (binance-crypto-ohlcv.json). ──
    if os.path.exists(BINANCE_CRYPTO):
        try:
            bc = json.load(open(BINANCE_CRYPTO)); iv = bc.get('interval', '15m')
            btc = _delta_norm(bc.get('pairs', {}).get('btcusd', {}).get(iv, []))
            if iv == '15m' and len(btc) >= 400:
                data_end = max(data_end, btc[-1]['_ts'])
                for s in _absorb_btc_signals(btc):
                    detected += 1
                    k = f"{s['strategy']}:{s['pair']}:{int(s['entry_ts'])}"
                    if k not in sigs:
                        s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
                    rec = sigs[k]
                    st, o = score_sess(btc, rec['entry_ts'], rec['entry'], rec['stop'],
                                       rec['target'], rec['dir'], ABSORB_HOLD)
                    rec['status'] = st
                    if st == 'resolved':
                        rec['r'] = o - cost(o, rec['entry'], abs(rec['entry'] - rec['stop']))
                    else:
                        rec.pop('r', None)
        except Exception as e:
            print(f"absorb_btc observer skipped: {e}")

    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    # Per-strategy tracking-start = the earliest first_seen we have for it (recovers
    # each candidate's true add-date; brand-new strategies get this run's data_end).
    # Lets the dashboard/report show HOW LONG each candidate has been tracked.
    tracking = log.setdefault('tracking', {})
    fs_by_strat = {}
    for s in sigs.values():
        st = s.get('strategy'); fs = s.get('first_seen')
        if st and fs is not None:
            fs_by_strat[st] = fs if st not in fs_by_strat else min(fs_by_strat[st], fs)
    for st, fs in fs_by_strat.items():
        if st not in tracking:
            tracking[st] = int(fs)
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())
    def rep(title, rows):
        print(f"\n{title}")
        for strat in ('hs', 's5_engulf', 's5_rsi', 'ob', 'tl_nowick', 'w5_pullback', 's5_rsi_wide', 'rsimr', 'fib_gz', 'fred_tl', 'threepush', 'engulf_manip', 'sweeprev', 'asianglitch', 'wm', 'sid', 'obfvg', 'obfvg_w', 'obfvg_fx4', 'gbreak', 'gtrend', 'gtrend_inv', 'gfib', 'e90break', 'mmove', 'mmove_ix', 'mmove_ix4', 'mmove_c4', 'mmove_m15', 'ema920v', 'obfvg_m15', 'orb_eq', 'varev_ix', 'holygrail', 'holygrail_cm', 'holygrail_eq', 'volbreak', 'volbreak_ix', 'volbreak_eq', 'twob', 'twob_ix', 'twob_cm', 'twob_eq', 'holygrail_cm_m15', 'holygrail_eq_m15', 'gold_us2h', 'fma_gold', 'fma_sweep_cm', 'fma_sweep_ix', 'po3_cm', 'sweepfvg_ix', 'absorb_btc'):
            sub = [s for s in rows if s['strategy'] == strat and s['status'] == 'resolved' and 'r' in s]
            pend = sum(1 for s in rows if s['strategy'] == strat and s['status'] == 'pending')
            ts0 = tracking.get(strat)
            td = f" · tracked {int((data_end - ts0)/86400)}d" if ts0 else ""
            if sub:
                w = sum(1 for s in sub if s['r'] > 0)
                print(f"  {strat:<10} resolved={len(sub):>3} pending={pend:>3} WR={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R{td}")
            else:
                print(f"  {strat:<10} resolved=0 pending={pend}{td}")
    print(f"harness run · data_end {int(data_end)} · detected {detected} signals this pass")
    rep("ALL logged (incl. in-sample backfill):", allv)
    rep("GENUINE FORWARD (entry after first run):", [s for s in allv if s['entry_ts'] > base])
    if all(s['entry_ts'] <= base for s in allv):
        print("\n  (baseline just set — re-run as new bars publish to accumulate forward signals)")


if __name__ == '__main__':
    main()
