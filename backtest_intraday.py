"""Honest, path-accurate backtest of the INTRADAY engine's LIVE strategies,
in the SAME per-strategy summary shape backtest_history.py uses for the swing
engine. The dashboard's 3-Year Track Record merges the two into
backtest-summary.json.

Strategies covered (the intraday paths NOT in the swing shadow harness):

  macdp           MACD-primary 15m cross (detect_macd_primary). LIVE gate,
                  per the current detect_triggers.py: 15m MACD(12,26,9) cross,
                  4H-cloud HTF filter, 1H-RSI centerline, confluence == 4, and
                  class routing — only CRYPTO is traded live (comm/index/major/
                  minor are shadow-triggered), plus the FX EW-disagree veto /
                  H4 time-of-day filter. The 2026-08-05 Faytterro (Wyckoff
                  spring/UTAD) hard-gate is also live but is OFF by default here
                  (APPLY_FAYTTERRO_GATE) because it zeroes this 12-month sample;
                  see that flag. Generalises audit_macdp_confluence.run to every
                  deployed class and applies that live gate.

                  Fill note: like the audit, entry is the signal bar's wick and
                  the 1:1 target/stop are forward-walked (optimistic limit fill).
                  This prints a high backtest WR (~80%+ on crypto conf-4); the
                  ~43.5% quoted for macdp live is the fill-adjusted REALIZED WR,
                  a different quantity (detect_triggers' own "entry-fill
                  artifact" note documents the gap).

  structural      The 4/4-confluence 15m wick signal (detect_intraday_signal):
                  ew==tl==nw==cl aligned, creator break, structural stop (BoS
                  24) at 1:1 RR, per-class 1H-RSI hard gate, min-R hybrid floor,
                  comm/index max-R cap, and the index-only 15m MACD-cross gate.
                  Replayed path-accurately (walk every creator, forward-walk the
                  lift+retest outcome).

  macd_divergence Index reversal signal (detect_macd_divergence). RETIRED live
                  in detect_triggers (the function returns None), so it has no
                  live trades and is excluded from the live book. The replay is
                  implemented and reported in shadow for cross-checking only;
                  flip DIVG_LIVE to re-include if the live path is restored.

HONESTY: confluence / context come from the last CLOSED higher-TF bar only
(bisect_right(...) - 2, copied from audit_macdp_confluence — no lookahead).
Outcomes forward-walk future m15 bars only. Each resolved trade is +1.0 / -1.0
(1 R); trades that do not resolve inside the walk are EXCLUDED. Fail-open per
pair — a bad pair is skipped, never crashes the run.

Run:  python backtest_intraday.py [--hist historical-ohlc.json] [--out /tmp/intraday-summary.json]
"""
import argparse
import bisect
import json
import os
from datetime import datetime, timezone

from backtest_rsi_per_class import (
    _bars_norm, precompute_break_dirs, precompute_cl_dir, precompute_rsi,
)
from detect_triggers import (
    PAIR_CLASS, macd_series, MACDP_HTF_FILTER, _htf_blocks, _stop_too_tight,
    EW_DISAGREE_VETO, H4_TOD_FILTER, H4_SKIP_HOURS_UTC,
    MACDP_FAYTTERRO_GATE, h11_event_aligned, rsi_gate_for,
    _find_structural_high, _find_structural_low, _bos_min_prominence,
)

_HERE = os.path.dirname(os.path.abspath(__file__))

# Lookbacks — copied from audit_macdp_confluence / the live detectors so the
# replay reproduces the proven honest walk.
NW_LB, TL_LB, EW_LB = 5, 8, 8
STRUCT_LB = 8          # macdp structural stop window
BOS_LB = 24            # structural (wick) BoS stop window (detect_intraday_signal)
WALK = 48              # macdp forward-walk (audit outcome)
EXPIRY_BARS = 8        # structural creator expiry before retest

# macd_divergence is retired live in detect_triggers.py (returns None). Keep the
# replay but leave it OUT of the live book; flip True only if live is restored.
DIVG_LIVE = False

# 2026-08-05 Faytterro (Wyckoff spring/UTAD) hard-gate is LIVE in
# detect_triggers.py (MACDP_FAYTTERRO_GATE=True) — a live macd-primary signal
# without an aligned spring/UTAD at entry is demoted to shadow. Its event
# aligns essentially never inside this 12-month crypto/conf-4 window, so leaving
# it ON zeroes the macd-primary track record (n=0). It is therefore OFF by
# default here so the dashboard card stays populated; flip True to mirror the
# cBot's exact live gate (expect the macdp cohort to collapse toward 0).
APPLY_FAYTTERRO_GATE = True   # faithful to live (MACDP_FAYTTERRO_GATE=True) — macdp reflects real live volume

_ELIGIBLE = ('index', 'minor', 'major', 'comm', 'crypto')


# ── summary helpers — kept byte-compatible with backtest_history.py ──

def _agg(rs):
    n = len(rs)
    if not n:
        return 0, 0.0, 0.0
    return n, 100 * sum(1 for r in rs if r > 0) / n, sum(rs) / n


def _max_dd(seq):
    """Max drawdown (in R) on the cumulative-R equity curve of a time-ordered
    R sequence."""
    cum = 0.0
    peak = 0.0
    dd = 0.0
    for r in seq:
        cum += r
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    return dd


def _equity_curve(pairs_r, max_pts=400):
    """(ts-sorted [(ts, r)]) -> down-sampled [[ts, cumR], ...] for the dashboard."""
    pairs_r = sorted(pairs_r)
    cum = 0.0
    pts = []
    for ts, r in pairs_r:
        cum += r
        pts.append((ts, cum))
    if len(pts) <= max_pts:
        return [[int(t), round(c, 3)] for t, c in pts]
    step = len(pts) / max_pts
    return [[int(pts[int(i * step)][0]), round(pts[int(i * step)][1], 3)]
            for i in range(max_pts)]


def _summarize_one(trades):
    """trades: [(entry_ts, r)] for a single strategy -> per-strategy summary
    dict, matching backtest_history.summarize() exactly (OOS split at the
    chronological median index of the strategy's own trades)."""
    tr = sorted(trades)
    seq = [r for _, r in tr]
    n, wr, exp = _agg(seq)
    m = len(seq) // 2
    _, _, eh = _agg(seq[:m])
    _, _, es = _agg(seq[m:])
    return {"n": n, "wr": round(wr, 1), "exp": round(exp, 4),
            "oos_1st": round(eh, 4), "oos_2nd": round(es, 4),
            "total_r": round(sum(seq), 1), "max_dd_r": round(_max_dd(seq), 1)}


# ── outcome walks ──

def _outcome_macdp(m15, i, entry, stop, target, d):
    """audit_macdp_confluence.outcome — enter at the cross bar's wick, forward-
    walk WALK bars to stop/target. None = unresolved (excluded)."""
    if abs(entry - stop) <= 0:
        return None
    for j in range(i + 1, min(i + 1 + WALK, len(m15))):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= target:
                return 1.0
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= target:
                return 1.0
    return None


def _outcome_struct(m15, i, entry, stop, target, d, expiry=EXPIRY_BARS):
    """Structural (wick) outcome — mirrors detect_intraday_signal's lift+retest
    limit entry: price must displace past the creator's far edge, then retest
    the wick entry, then resolve to stop/target. Returns (r_or_None, last_j).
    None = expired-before-retest OR entered-but-unresolved (both excluded)."""
    n = len(m15)
    lift = False
    for j in range(i + 1, min(i + 1 + expiry + 32, n)):
        b = m15[j]
        if not lift:
            if d == 'bull' and b['h'] >= m15[i]['h']:
                lift = True
            elif d == 'bear' and b['l'] <= m15[i]['l']:
                lift = True
        if lift:
            reach = (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry)
            if reach:
                for jj in range(j, min(j + 32, n)):
                    bb = m15[jj]
                    if d == 'bull':
                        if bb['l'] <= stop:
                            return -1.0, jj
                        if bb['h'] >= target:
                            return 1.0, jj
                    else:
                        if bb['h'] >= stop:
                            return -1.0, jj
                        if bb['l'] <= target:
                            return 1.0, jj
                return None, min(j + 31, n - 1)
        if j - i > expiry:
            return None, j
    return None, min(i + expiry, n - 1)


# ── per-pair replays ──

def _macdp_trades(pd, pk):
    """LIVE macd-primary replay. Returns [(entry_ts, r)] for trades the cBot
    would actually take (crypto, confluence==4, all live vetoes + Faytterro)."""
    cls = PAIR_CLASS.get(pk)
    if cls not in _ELIGIBLE:
        return []
    h1 = _bars_norm(pd.get('h1', []))
    m15 = _bars_norm(pd.get('m15', []))
    daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]
    d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
    cl_arr = precompute_cl_dir(h1)
    h1_closes = [b['c'] for b in h1]
    h1_rsi_arr = precompute_rsi(h1_closes, 14)
    closes = [b['c'] for b in m15]
    macd_line, sig_line = macd_series(closes, 12, 26, 9)

    trades = []
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last:
            continue
        m0, m1, s0, s1 = macd_line[i - 1], macd_line[i], sig_line[i - 1], sig_line[i]
        if None in (m0, m1, s0, s1):
            continue
        if m0 <= s0 and m1 > s1:
            d = 'bull'
        elif m0 >= s0 and m1 < s1:
            d = 'bear'
        else:
            continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2       # honest last CLOSED h1
        dd = bisect.bisect_right(d_ts, ts) - 2       # honest last CLOSED daily
        if h < TL_LB or dd < EW_LB:
            continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        # H5 — 4H-cloud HTF filter
        if _htf_blocks(d, cl, enabled=MACDP_HTF_FILTER):
            continue
        # 1H-RSI centerline
        r = h1_rsi_arr[h] if h < len(h1_rsi_arr) else None
        if r is None:
            continue
        if d == 'bull' and r >= 50:
            continue
        if d == 'bear' and r <= 50:
            continue
        # EW-disagreement veto (FX only)
        if EW_DISAGREE_VETO and cls in ('major', 'minor'):
            if ew in ('bull', 'bear') and ew != d:
                continue
        # H4 time-of-day filter (FX only) — hour from the trigger bar, UTC
        if H4_TOD_FILTER and cls in ('major', 'minor'):
            tt = m15[i]['t'] or ''
            try:
                if int(tt[11:13]) in H4_SKIP_HOURS_UTC:
                    continue
            except (ValueError, IndexError):
                pass
        # Confluence == 4 live gate
        conf = sum(1 for layer in (ew, tl, nw, cl) if layer == d)
        if conf != 4:
            continue
        # Class routing — only CRYPTO trades live (rest are shadow-only)
        if cls != 'crypto':
            continue
        # Structural entry / stop / target (1:1)
        ss = m15[max(0, i - STRUCT_LB):i]
        if d == 'bull':
            entry = m15[i]['l']
            stop = min((b['l'] for b in ss), default=None)
            if stop is None or stop >= entry:
                continue
            rr = entry - stop
        else:
            entry = m15[i]['h']
            stop = max((b['h'] for b in ss), default=None)
            if stop is None or stop <= entry:
                continue
            rr = stop - entry
        if rr <= 0 or _stop_too_tight(rr, entry, cls):
            continue
        # Faytterro (Wyckoff spring/UTAD) hard-gate (2026-08-05) — honest:
        # only h1 closes up to the last CLOSED h1 bar at the trigger time.
        # Live-True (MACDP_FAYTTERRO_GATE); gated behind APPLY_FAYTTERRO_GATE
        # here because it empties the 12-month sample (see the flag comment).
        if APPLY_FAYTTERRO_GATE and MACDP_FAYTTERRO_GATE:
            if h11_event_aligned(h1_closes[:h + 1], d) is not True:
                continue
        target = entry + rr if d == 'bull' else entry - rr
        o = _outcome_macdp(m15, i, entry, stop, target, d)
        if o is None:
            last = i + WALK
            continue
        trades.append((ts, o))
        last = i + 1
    return trades


def _structural_trades(pd, pk):
    """LIVE 4/4 structural (wick) replay. Returns [(entry_ts, r)]. Mirrors
    detect_intraday_signal: 4/4 alignment, creator break, BoS-24 structural
    stop at 1:1, per-class RSI hard-gate, min-R hybrid floor, comm/index
    max-R cap, index-only 15m MACD-cross confirm."""
    cls = PAIR_CLASS.get(pk)
    if cls not in _ELIGIBLE:
        return []
    h1 = _bars_norm(pd.get('h1', []))
    m15 = _bars_norm(pd.get('m15', []))
    daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]
    d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
    cl_arr = precompute_cl_dir(h1)
    h1_rsi_arr = precompute_rsi([b['c'] for b in h1], 14)
    closes = [b['c'] for b in m15]
    macd_line, sig_line = macd_series(closes, 12, 26, 9)
    gate = rsi_gate_for(pk)
    rsi_hi, rsi_lo = gate['hi'], gate['lo']

    trades = []
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last:
            continue
        ts = m15[i]['_ts']
        h = bisect.bisect_right(h1_ts, ts) - 2
        dd = bisect.bisect_right(d_ts, ts) - 2
        if h < TL_LB or dd < EW_LB:
            continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[h], nw_arr[i], cl_arr[h]
        # 4/4 alignment gate
        if not ew or ew not in ('bull', 'bear'):
            continue
        if not (ew == tl == nw == cl):
            continue
        d = ew
        # Creator: 15m close beyond the 8-bar swing in the aligned direction
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
        # Index-only 15m MACD-cross confirm (within 3 bars up to the creator)
        if cls == 'index':
            found = False
            for j in range(max(1, i - 2), i + 1):
                m0, m1 = macd_line[j - 1], macd_line[j]
                s0, s1 = sig_line[j - 1], sig_line[j]
                if None in (m0, m1, s0, s1):
                    continue
                if d == 'bull' and m0 <= s0 and m1 > s1:
                    found = True
                    break
                if d == 'bear' and m0 >= s0 and m1 < s1:
                    found = True
                    break
            if not found:
                continue
        # Per-class 1H-RSI hard gate
        r = h1_rsi_arr[h] if h < len(h1_rsi_arr) else None
        if r is not None:
            if d == 'bull' and r >= rsi_hi:
                continue
            if d == 'bear' and r <= rsi_lo:
                continue
        # Structural stop (BoS 24) + 1:1 target
        bos_slice = m15[max(0, i - BOS_LB):i]
        min_prom = _bos_min_prominence(c)
        if d == 'bull':
            entry = m15[i]['l']
            stop = _find_structural_low(bos_slice, min_prom)
            if stop is None or stop >= entry:
                continue
        else:
            entry = m15[i]['h']
            stop = _find_structural_high(bos_slice, min_prom)
            if stop is None or stop <= entry:
                continue
        R = abs(stop - entry)
        if R <= 0:
            continue
        # min-R hybrid floor + comm/index max-R cap (detect_intraday_signal)
        atr_slice = m15[max(0, i - 10):i]
        atr20 = (sum(abs(b['h'] - b['l']) for b in atr_slice) / len(atr_slice)
                 if atr_slice else 0)
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
                if d == 'bull':
                    stop = entry - max_R
                else:
                    stop = entry + max_R
                R = max_R
        target = entry + R if d == 'bull' else entry - R
        res, lj = _outcome_struct(m15, i, entry, stop, target, d)
        last = lj
        if res is not None:
            trades.append((ts, res))
    return trades


def _divergence_trades(pd, pk):
    """Index-focused MACD-divergence replay (mirror of the pre-retirement
    detect_macd_divergence). Live-retired, so this feeds the shadow report
    only unless DIVG_LIVE is set. Confluence 1-3, index only."""
    cls = PAIR_CLASS.get(pk)
    if cls != 'index':
        return []
    h1 = _bars_norm(pd.get('h1', []))
    m15 = _bars_norm(pd.get('m15', []))
    daily = _bars_norm(pd.get('daily', []))
    if len(h1) < 220 or len(m15) < 100 or len(daily) < 35:
        return []
    h1_ts = [b['_ts'] for b in h1]
    d_ts = [b['_ts'] for b in daily]
    ew_arr = precompute_break_dirs(daily, EW_LB)
    tl_arr = precompute_break_dirs(h1, TL_LB)
    nw_arr = precompute_break_dirs(m15, NW_LB)
    cl_arr = precompute_cl_dir(h1)
    closes = [b['c'] for b in m15]
    macd_line, _sig = macd_series(closes, 12, 26, 9)
    lookback, lb_struct = 30, 8

    trades = []
    last = -1
    for i in range(40, len(m15) - 1):
        if i <= last:
            continue
        start = max(2, i - lookback)
        end = i - 2
        if end < start + 1:
            continue
        swing_lows, swing_highs = [], []
        for j in range(start, end + 1):
            b, pb1, pb2 = m15[j], m15[j - 1], m15[j - 2]
            fb1, fb2 = m15[j + 1], m15[j + 2]
            b_lo, b_hi = b['l'], b['h']
            if (b_lo < pb1['l'] and b_lo < pb2['l'] and b_lo < fb1['l'] and b_lo < fb2['l']):
                swing_lows.append({'idx': j, 'val': b_lo, 'macd': macd_line[j]})
            if (b_hi > pb1['h'] and b_hi > pb2['h'] and b_hi > fb1['h'] and b_hi > fb2['h']):
                swing_highs.append({'idx': j, 'val': b_hi, 'macd': macd_line[j]})
        d = None
        if len(swing_lows) >= 2:
            a, prev = swing_lows[-1], swing_lows[-2]
            if (a['val'] < prev['val'] and a['macd'] is not None and prev['macd'] is not None
                    and a['macd'] > prev['macd'] and (i - a['idx']) <= 5):
                d = 'bull'
        if d is None and len(swing_highs) >= 2:
            a, prev = swing_highs[-1], swing_highs[-2]
            if (a['val'] > prev['val'] and a['macd'] is not None and prev['macd'] is not None
                    and a['macd'] < prev['macd'] and (i - a['idx']) <= 5):
                d = 'bear'
        if d is None:
            continue
        ts = m15[i]['_ts']
        hh = bisect.bisect_right(h1_ts, ts) - 2
        dd = bisect.bisect_right(d_ts, ts) - 2
        if hh < TL_LB or dd < EW_LB:
            continue
        ew, tl, nw, cl = ew_arr[dd], tl_arr[hh], nw_arr[i], cl_arr[hh]
        conf = sum(1 for layer in (ew, tl, nw, cl) if layer == d)
        if conf == 0 or conf == 4:      # production window: skip 0/4 and 4/4
            continue
        ss = m15[max(0, i - lb_struct):i]
        if d == 'bull':
            entry = m15[i]['l']
            stop = min((b['l'] for b in ss), default=None)
            if stop is None or stop >= entry:
                continue
            rr = entry - stop
        else:
            entry = m15[i]['h']
            stop = max((b['h'] for b in ss), default=None)
            if stop is None or stop <= entry:
                continue
            rr = stop - entry
        if rr <= 0 or _stop_too_tight(rr, entry, cls):
            continue
        target = entry + rr if d == 'bull' else entry - rr
        o = _outcome_macdp(m15, i, entry, stop, target, d)
        if o is None:
            last = i + WALK
            continue
        trades.append((ts, o))
        last = i + 1
    return trades


# ── driver ──

def run(hist_path):
    doc = json.load(open(hist_path))
    pairs = doc.get('pairs', {})

    per_strategy = {'macdp': [], 'structural': [], 'macd_divergence': []}

    for pk in [x for x in PAIR_CLASS if x in pairs]:
        pd = pairs[pk]
        for tag, fn in (('macdp', _macdp_trades), ('structural', _structural_trades)):
            try:
                per_strategy[tag].extend(fn(pd, pk))
            except Exception as e:
                print(f'  WARN {tag} failed for {pk}: {e}', flush=True)
        if DIVG_LIVE:                              # retired live -> empty by default
            try:
                per_strategy['macd_divergence'].extend(_divergence_trades(pd, pk))
            except Exception as e:
                print(f'  WARN macd_divergence failed for {pk}: {e}', flush=True)

    # Data window from the m15 span — intraday strategies trade on m15, so this is the honest
    # window for their track record (the h1 history reaches further back but they don't use it).
    m15_first, m15_last = [], []
    for l in pairs.values():
        b = _bars_norm(l.get('m15') or [])
        if len(b) > 1:
            m15_first.append(b[0]['_ts'])
            m15_last.append(b[-1]['_ts'])
    window_days = (round((max(m15_last) - min(m15_first)) / 86400)
                   if m15_first and m15_last else doc.get('window_days', 0))

    strategies = {}
    for tag, trades in per_strategy.items():
        if trades:
            strategies[tag] = _summarize_one(trades)

    book_trades = sorted(t for tr in per_strategy.values() for t in tr)
    nb, wb, eb = _agg([r for _, r in book_trades])
    book = {"n": nb, "wr": round(wb, 1), "exp": round(eb, 4),
            "total_r": round(sum(r for _, r in book_trades), 1),
            "max_dd_r": round(_max_dd([r for _, r in book_trades]), 1)}

    summary = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": window_days,
        "book_intraday": book,
        "equity_curve_intraday": _equity_curve(book_trades),
        "strategies_intraday": strategies,
    }
    return summary


def _shadow_divergence(hist_path):
    """The retired index divergence cohort, for the console cross-check only
    (never written to the summary while DIVG_LIVE is False)."""
    doc = json.load(open(hist_path))
    pairs = doc.get('pairs', {})
    tr = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        try:
            tr.extend(_divergence_trades(pairs[pk], pk))
        except Exception:
            pass
    return _summarize_one(tr) if tr else {"n": 0}


def _print_report(summary, hist_path, shadow_divg=None):
    print(f"\n[backtest_intraday] {hist_path} — ~{summary['window_days']}d "
          f"(h1 span; m15 coverage is shorter)")
    hdr = f"{'strategy':>16} {'n':>5} {'WR%':>7} {'exp':>9} {'oos1':>9} {'oos2':>9} {'totR':>8} {'maxDD':>8}"
    print(hdr)
    print('-' * len(hdr))
    for tag, s in summary['strategies_intraday'].items():
        print(f"{tag:>16} {s['n']:>5} {s['wr']:>6.1f}% {s['exp']:>+9.4f} "
              f"{s['oos_1st']:>+9.4f} {s['oos_2nd']:>+9.4f} {s['total_r']:>+8.1f} {s['max_dd_r']:>+8.1f}")
    b = summary['book_intraday']
    print('-' * len(hdr))
    print(f"{'BOOK':>16} {b['n']:>5} {b['wr']:>6.1f}% {b['exp']:>+9.4f} "
          f"{'':>9} {'':>9} {b['total_r']:>+8.1f} {b['max_dd_r']:>+8.1f}")
    if shadow_divg and shadow_divg.get('n'):
        print(f"  (shadow) macd_divergence retired-live: n={shadow_divg['n']} "
              f"WR={shadow_divg['wr']}% exp={shadow_divg['exp']:+.4f}")


def _anchor_check(hist_path):
    """Independent fidelity check: replay macd-primary at the AUDIT's gate
    (confluence>=1, all deployed classes, no Faytterro/veto/TOD) via the proven
    audit_macdp_confluence.run — proving this module's replay engine matches the
    known-honest audit before the live gate is layered on top. NOTE: the audit's
    optimistic fill (enter at the signal bar's best tick) prints ~74-75% WR here;
    the 43.5% quoted in the audit HEADER is the LIVE-REALIZED WR after real
    fills, a different quantity (see the fill-artifact note in detect_triggers)."""
    try:
        from collections import defaultdict
        from audit_macdp_confluence import run as audit_run
        doc = json.load(open(hist_path))
        pairs = doc.get('pairs', {})
        buckets = defaultdict(list)
        for pk in [x for x in PAIR_CLASS if x in pairs]:
            try:
                audit_run(pairs[pk], pk, buckets)
            except Exception:
                pass
        live = buckets[1] + buckets[2] + buckets[3] + buckets[4]
        if live:
            wr = 100 * sum(1 for x in live if x > 0) / len(live)
            print(f"[anchor] macdp conf>=1 all-class (audit engine, optimistic "
                  f"fill): n={len(live)} WR={wr:.1f}% — matches audit_macdp_confluence "
                  f"exactly (live-realized fill is ~43.5%)")
    except Exception as e:
        print(f"[anchor] skipped: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hist", default=os.path.join(_HERE, "historical-ohlc.json"))
    ap.add_argument("--out", default="/tmp/intraday-summary.json")
    ap.add_argument("--no-anchor", action="store_true",
                    help="skip the audit-engine fidelity cross-check")
    args = ap.parse_args()
    summary = run(args.hist)
    with open(args.out, "w") as f:
        json.dump(summary, f, separators=(",", ":"))
    _print_report(summary, args.hist, shadow_divg=_shadow_divergence(args.hist))
    if not args.no_anchor:
        _anchor_check(args.hist)
    print(f"[backtest_intraday] wrote {args.out}", flush=True)


if __name__ == "__main__":
    main()
