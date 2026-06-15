#!/usr/bin/env python3
"""
Detect 4/4-confluence transitions and send Telegram alerts.

Runs after every fetch-data workflow run. Computes EW (daily) / TL
(hourly) / NW (15m) direction for each pair using the same
calcIndependentDir structural-break logic the live dashboard uses,
detects pairs that just transitioned to 4/4 alignment, and sends a
Telegram message for each new alignment.

State is persisted in alerts-state.json so we don't re-alert on every
workflow run while a pair remains aligned. Transitions tracked:
  - prev=None, current='bull'/'bear'  -> ALERT (newly aligned)
  - prev='bull', current='bear'        -> ALERT (full flip)
  - prev='bull', current='bull'        -> no alert (still aligned)
  - prev='bull', current=None          -> no alert (lost alignment, not a setup)

Phase 2 — intraday trigger alerts: for every 4/4-aligned pair, the 15m
1:1-RR signal is tracked (armed -> triggered). A TRIGGERED alert fires
exactly once per distinct setup, deduped on the creator-bar timestamp.
This fires whether or not the intermediate 'armed' state was observed —
the ~10-min detector cadence routinely misses it.

First run (no state file): compute baseline, NO alerts. Subsequent runs
detect transitions from that baseline.

Env vars (GitHub Actions secrets):
  TELEGRAM_BOT_TOKEN   - bot token from @BotFather
  TELEGRAM_CHAT_ID     - chat id to receive alerts
Both optional — without them the script still computes + persists state
but skips the send. Useful for first-run baselining.
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
try:
    import requests
except ImportError:
    print('ERROR: requests not installed. Run: pip install requests')
    sys.exit(1)


# Pretty-print pair symbol from internal key (eurusd -> EUR/USD).
PAIR_DISPLAY = {
    'eurusd': 'EUR/USD', 'gbpusd': 'GBP/USD', 'usdjpy': 'USD/JPY',
    'usdcad': 'USD/CAD', 'usdchf': 'USD/CHF', 'audusd': 'AUD/USD',
    'nzdusd': 'NZD/USD', 'usdsgd': 'USD/SGD', 'cadjpy': 'CAD/JPY',
    'eurnzd': 'EUR/NZD', 'euraud': 'EUR/AUD',
    # gbpaud removed 2026-06-10h — chronic ~50% WR.
    'audnzd': 'AUD/NZD', 'eurgbp': 'EUR/GBP',
    # audchf removed 2026-06-08 — low win-rate drag on aggregate.
    # Re-add: "'audchf': 'AUD/CHF',"
    # v5 FX additions
    'gbpcad': 'GBP/CAD', 'nzdjpy': 'NZD/JPY',
    # audcad removed 2026-06-10i — low win-rate drag.
    'gbpnzd': 'GBP/NZD',
    # usdnok / eursek removed 2026-06-08 — low win-rate drag.
    # Re-add: "'usdnok': 'USD/NOK', 'eursek': 'EUR/SEK',"
    # Commodities / indices
    'xauusd': 'XAU/USD', 'xagusd': 'XAG/USD', 'usoil': 'BRENT',
    'wtiusd': 'WTI', 'natgas': 'NAT GAS', 'xptusd': 'XPT/USD',
    'ftse100': 'FTSE 100',
    # de40 reinstated 2026-06-15lll — inferred macro seed retired
    # client-side, targets auto-derived. Re-evaluate WR after one
    # full backtest cycle.
    'de40':   'DAX 40',
    'dj30':   'DJ 30',   'nas100':  'NAS 100', 'spx500':  'S&P 500',
    # v7 FX minors (2026-06-03)
    # nzdcad removed 2026-06-10 — low win-rate drag.
    'eurnok': 'EUR/NOK', 'nzdchf': 'NZD/CHF',
    # gbpchf / usdcnh removed 2026-06-10 — low win-rate drag.
    # Re-add: "'gbpchf': 'GBP/CHF', 'usdcnh': 'USD/CNH',"
    'usdzar': 'USD/ZAR',
    'eursgd': 'EUR/SGD',
    # v7 indices (2026-06-03)
    'jp225':  'Nikkei 225',
    # fra40 (CAC 40) removed 2026-06-10 — low win-rate drag.
    # Re-add: "'fra40': 'CAC 40',"
    # Crypto
    'btcusd': 'BTC/USD', 'suiusd': 'SUI/USD',
    'ethusd': 'ETH/USD', 'solusd': 'SOL/USD',
    'xrpusd': 'XRP/USD', 'taousd': 'TAO/USD',
    'nearusd': 'NEAR/USD', 'ondousd': 'ONDO/USD',
    # hypeusd removed 2026-06-10 — low win-rate drag.
    # ltcusd removed 2026-06-10 — low win-rate drag.
    # Re-add: "'ltcusd': 'LTC/USD',"
    'dxy':    'DXY',
}


# Pair → asset class. Mirrors MKTS[k].t in the dashboard
# (Viking_Invest_Trading_v69.html). Used by the A1 RSI hard gate to look
# up the per-class threshold from RSI_GATE_BY_CLASS. If a pair is added
# without a class entry, the gate falls back to 80/20 — same default the
# JS _rsiGateFor(k) uses.
PAIR_CLASS = {
    # FX majors
    'eurusd': 'major', 'gbpusd': 'major', 'usdjpy': 'major',
    'usdcad': 'major', 'usdchf': 'major', 'nzdusd': 'major',
    'audusd': 'major',
    # FX minors / crosses
    'cadjpy': 'minor', 'eurnzd': 'minor',
    # gbpaud removed 2026-06-10h — chronic ~50% WR.
    'euraud': 'minor', 'usdsgd': 'minor', 'audnzd': 'minor',
    'eurgbp': 'minor', 'gbpcad': 'minor',
    # audcad removed 2026-06-10i — low win-rate drag.
    'nzdjpy': 'minor', 'gbpnzd': 'minor',
    # nzdcad removed 2026-06-10 — low win-rate drag.
    'eurnok': 'minor', 'nzdchf': 'minor',
    # gbpchf / usdcnh removed 2026-06-10 — low win-rate drag.
    'usdzar': 'minor', 'eursgd': 'minor',
    # Commodities
    'xauusd': 'comm', 'xagusd': 'comm', 'usoil': 'comm',
    'wtiusd': 'comm', 'natgas': 'comm', 'xptusd': 'comm',
    # Indices (incl. DXY — confluence reference, not tradeable but
    # classified for completeness so any lookup gets a deterministic
    # answer instead of falling back to 80/20).
    'ftse100': 'index', 'dj30': 'index',
    'de40': 'index',  # reinstated 2026-06-15lll
    'nas100': 'index', 'spx500': 'index', 'jp225': 'index',
    # fra40 removed 2026-06-10 — low win-rate drag.
    'dxy': 'index',
    # Crypto
    'btcusd': 'crypto', 'ethusd': 'crypto', 'solusd': 'crypto',
    'xrpusd': 'crypto', 'suiusd': 'crypto',
    # ltcusd removed 2026-06-10 — low win-rate drag.
    'taousd': 'crypto', 'nearusd': 'crypto',
    # hypeusd removed 2026-06-10 — low win-rate drag.
    'ondousd': 'crypto',
}


# A1 per-class RSI hard-gate thresholds (RULES_VERSION 2026-06-10a,
# minors tightened 2026-06-10h after per-pair drill-down on GBP minors
# showed 70/30 wasn't filtering aggressively enough).
#
# Sweep results: minors/commodities/indices preferred a tighter 70/30
# gate over the previous 80/20 default. Drill-down then showed minors
# benefited from going further to 65/35 — GBP/CAD wick WR 46.7→85.7%,
# GBP/NZD 50.0→71.4% on the next round. Majors and crypto remain
# threshold-insensitive (90/10 effectively disables for majors; 75/25
# keeps a mild crypto guard). Commodities and indices regress at
# 65/35 so they stay at 70/30. See backtest_rsi_per_class.py.
RSI_GATE_BY_CLASS = {
    'major':  {'hi': 90, 'lo': 10},
    'minor':  {'hi': 65, 'lo': 35},
    'comm':   {'hi': 70, 'lo': 30},
    'index':  {'hi': 70, 'lo': 30},
    'crypto': {'hi': 75, 'lo': 25},
}


def rsi_gate_for(pair):
    """Return {'hi': N, 'lo': M} for a pair. Falls back to 80/20 when
    the class is unknown — same default _rsiGateFor in the JS uses."""
    cls = PAIR_CLASS.get(pair)
    return RSI_GATE_BY_CLASS.get(cls, {'hi': 80, 'lo': 20})


# ── School Run (SR) — Tom Hougaard's opening-range pattern adapted as
# a 5th confluence layer. DE40 + DJ30 — the two indices with reliable
# cash-session opens that match the SR mechanic. (RULES_VERSION
# 2026-06-10k; DE40 reinstated 2026-06-15lll after seed retirement.)
# Mirror of getSRTier / _findRefCandle / _computeSRState
# in Viking_Invest_Trading_v69.html — kept lockstep so the Telegram
# alert tier (written to directions.json) matches the dashboard pill
# the user sees.
SR_REF_TIMES = {
    # de40 reinstated 2026-06-15lll. SR window times unchanged from the
    # original 2026-06-10k spec — opens 09:00 / 10:00 Frankfurt local
    # (08:00 / 09:00 UTC during summertime).
    'de40': {'open_times': ('08:00', '09:00'), 'session_label': 'DE40 09:00 CET', 'window_bars': 8},
    'dj30': {'open_times': ('13:30', '14:30'), 'session_label': 'DJ30 09:30 ET', 'window_bars': 8},
}


# Per-tier daily Telegram alert limits (per pair). 5/5 is the rarest +
# highest quality so 1/day is a hard ceiling; 4/5 gets 2 (the noisiest
# tier so most likely to need the limit); 3/5 stays tight at 1 because
# it's the speculative tier we don't want spamming on choppy days.
SR_ALERT_LIMITS = {
    '5/5': 1,
    '4/5': 2,
    '3/5': 1,
}

# Map SR tier → alert kind + Supabase alert_classes value (for the
# per-user opt-in routing in the per-user Telegram pipeline). Same
# strings appear in the dashboard profile modal checkboxes.
SR_TIER_TO_KIND = {
    '5/5': 'sr_5_5',
    '4/5': 'sr_4_5',
    '3/5': 'sr_3_5',
}


def _find_sr_ref_candle(pair, m15):
    """Most recent reference candle within window_bars of the latest
    m15 bar for this pair. Returns {idx, ref_high, ref_low, open_time,
    date, bars_since} or None."""
    spec = SR_REF_TIMES.get(pair)
    if not spec or not m15:
        return None
    latest = len(m15) - 1
    for i in range(latest, -1, -1):
        if (latest - i) > (spec['window_bars'] + 2):
            break
        b = m15[i]
        ts = b.get('t') if b else None
        if not ts or len(ts) < 16:
            continue
        if ts[11:16] not in spec['open_times']:
            continue
        return {
            'idx': i,
            'ref_high': b.get('h', b.get('c')),
            'ref_low':  b.get('l', b.get('c')),
            'open_time': ts[11:16],
            'date': ts[:10],
            'bars_since': latest - i,
        }
    return None


def _compute_sr_state(m15, ref):
    """Walk bars after ref.idx and return the SR state."""
    if not ref or not m15:
        return 'pending'
    lo = min(ref['ref_low'], ref['ref_high'])
    hi = max(ref['ref_low'], ref['ref_high'])
    state = 'pending'
    for j in range(ref['idx'] + 1, len(m15)):
        b = m15[j]
        c = b.get('c') if b else None
        if c is None:
            continue
        if state == 'pending':
            if c > hi:
                state = 'bull_broken'
            elif c < lo:
                state = 'bear_broken'
        elif state == 'bull_broken' and c <= hi:
            state = 'bull_failed'
        elif state == 'bear_broken' and c >= lo:
            state = 'bear_failed'
    return state


def get_sr_tier(pair, m15, ew_dir, confluence_score):
    """Return SR info dict or None.

    confluence_score is the tot from the 4-layer scorer (0-4); ew_dir is
    the macro engine direction. Tiers:
      5/5 = 4/4 confluence + SR aligned
      4/5 = 3/4 confluence + SR aligned
      3/5 = 2/4 confluence + SR aligned
      SR_WINDOW = in window but no alignment (informational)
    Returns None when the pair isn't an SR pair or no active session.
    """
    if pair not in SR_REF_TIMES:
        return None
    ref = _find_sr_ref_candle(pair, m15)
    if not ref:
        return None
    spec = SR_REF_TIMES[pair]
    if ref['bars_since'] > spec['window_bars']:
        return None
    state = _compute_sr_state(m15, ref)
    info = {
        'tier': 'SR_WINDOW',
        'state': state,
        'ref_high': ref['ref_high'],
        'ref_low': ref['ref_low'],
        'engine_dir': ew_dir,
        'confluence': confluence_score,
        'session_label': spec['session_label'],
        'bars_since_ref': ref['bars_since'],
    }
    aligned = ((state == 'bull_broken' and ew_dir == 'bull') or
               (state == 'bear_broken' and ew_dir == 'bear'))
    if not aligned:
        return info
    if confluence_score == 4:
        info['tier'] = '5/5'
    elif confluence_score == 3:
        info['tier'] = '4/5'
    elif confluence_score == 2:
        info['tier'] = '3/5'
    return info


# ── Direction detection (ports calcIndependentDir from dashboard.html) ──

def calc_independent_dir(bars, lookback=8):
    """Walks the bar sequence, tracking the most recent significant
    structural break (close beyond `lookback`-bar swing by a
    prominence-aware margin). The latest break direction = current
    direction. Mirrors the JS in dashboard.html (~L8070).
    Returns 'bull', 'bear', 'neutral', or None.
    """
    if not bars or len(bars) < 8:
        return None
    n = len(bars)
    px_abs = abs(bars[-1].get('c') or bars[-1].get('h') or 0)
    if px_abs > 1000:
        min_prom = px_abs * 0.001
    elif px_abs > 50:
        min_prom = px_abs * 0.0008
    elif px_abs > 5:
        min_prom = px_abs * 0.0008
    else:
        min_prom = 0.0005

    last_break_dir = None
    last_break_idx = -1
    for i in range(lookback, n):
        slc = bars[max(0, i - lookback):i]
        if len(slc) < 5:
            continue
        swing_hi = max(b['h'] for b in slc)
        swing_lo = min(b['l'] for b in slc)
        bar = bars[i]
        c = bar.get('c')
        if c is None:
            continue
        if c > swing_hi and (c - swing_hi) >= min_prom:
            last_break_dir = 'bull'
            last_break_idx = i
        elif c < swing_lo and (swing_lo - c) >= min_prom:
            last_break_dir = 'bear'
            last_break_idx = i

    # Staleness decay: if the most recent break is older than 2× lookback
    # bars (i.e., price has stopped printing new highs/lows in that
    # direction for 16+ hours on 1H), the structural trend has lost
    # momentum. Fall through to the slope check / neutral rather than
    # reporting a sticky last-known direction.
    #
    # Without this decay GBPAUD + USDCHF (2026-06-08) reported tl=bull
    # from a breakout 19-21 hours earlier, while the last 8 hourly closes
    # were chopping inside the prior swing range — exactly the
    # disagreement-with-TradingView the user flagged. A sustained trend
    # keeps refreshing last_break_idx on each new HH/LL, so this only
    # bites range-bound consolidations after a truly old break. 2× was
    # chosen over 1× because the latter clobbered legitimate trend reads
    # on pairs that had broken structure 8-15 bars ago and were still
    # trending (NZD/USD, USD/JPY, EUR/USD).
    if last_break_dir is not None and last_break_idx >= 0:
        bars_since_break = (n - 1) - last_break_idx
        if bars_since_break >= 2 * lookback:
            last_break_dir = None

    if last_break_dir is None:
        # Fall back to recent slope (widened deadband — 0.0005 was too
        # tight: USDCHF showed +6 pips drift over 8 hours and read as
        # 'bull' despite the broader TradingView chart being clearly
        # bearish. 0.0015 (15 bps over the lookback window) needs a
        # clearer net move before claiming a direction.)
        if n >= lookback:
            first_c = bars[-lookback].get('c')
            last_c = bars[-1].get('c')
            if first_c and last_c:
                change = (last_c - first_c) / first_c
                if change > 0.0015:
                    return 'bull'
                if change < -0.0015:
                    return 'bear'
        return 'neutral'
    return last_break_dir


def calc_rsi(closes, period=14):
    """Wilder RSI — byte-for-byte port of _rsiSeries(closes, 14) in the
    dashboard (lines ~14566). Returns the LAST RSI value or None if the
    series is shorter than period+1. Used by the A1 hard gate
    (RULES_VERSION 2026-06-09d) to block setups whose 1H RSI is at the
    80/20 extreme.
    """
    if not closes:
        return None
    n = len(closes)
    if n < period + 1:
        return None
    gain = 0.0
    loss = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gain += d
        else:
            loss -= d
    avg_gain = gain / period
    avg_loss = loss / period
    rsi = 100.0 if avg_loss == 0 else (100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0.0
        l = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
        rsi = 100.0 if avg_loss == 0 else (100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    return rsi


def _bos_min_prominence(px):
    """Prominence threshold for structural pivot detection. Mirrors the JS
    findStructuralHigh/Low prominence math used by the dashboard so the
    Telegram alert's stop matches the displayed 15m intraday stop."""
    ap = abs(px) if px is not None else 0
    if ap > 1000:
        return ap * 0.001
    if ap > 50:
        return ap * 0.0008
    if ap > 5:
        return ap * 0.0008
    return 0.0005


def _find_structural_high(bars_slice, min_prom):
    """Return the most-recent prominent swing high in bars_slice (BoS
    reference for a bear stop). Falls back to the slice max."""
    n = len(bars_slice)
    for j in range(n - 2, 1, -1):
        if j < 1 or j >= n - 1:
            continue
        this_h = bars_slice[j].get('h')
        if this_h is None:
            continue
        prev1 = bars_slice[j - 1].get('h', this_h)
        prev2 = bars_slice[j - 2].get('h', prev1) if j >= 2 else prev1
        left_h = max(prev1, prev2)
        right_h = bars_slice[j + 1].get('h', this_h)
        if this_h > left_h and this_h > right_h and (this_h - max(left_h, right_h)) >= min_prom:
            return this_h
    highs = [b.get('h') for b in bars_slice if b.get('h') is not None]
    return max(highs) if highs else None


def _find_structural_low(bars_slice, min_prom):
    """Return the most-recent prominent swing low (BoS reference for a bull
    stop). Falls back to the slice min."""
    n = len(bars_slice)
    for j in range(n - 2, 1, -1):
        if j < 1 or j >= n - 1:
            continue
        this_l = bars_slice[j].get('l')
        if this_l is None:
            continue
        prev1 = bars_slice[j - 1].get('l', this_l)
        prev2 = bars_slice[j - 2].get('l', prev1) if j >= 2 else prev1
        left_l = min(prev1, prev2)
        right_l = bars_slice[j + 1].get('l', this_l)
        if this_l < left_l and this_l < right_l and (min(left_l, right_l) - this_l) >= min_prom:
            return this_l
    lows = [b.get('l') for b in bars_slice if b.get('l') is not None]
    return min(lows) if lows else None


# ── Invalidation helpers (port of detectOpposingCHoCH +
# detectConsecutiveCounterBars from the dashboard, 2026-06-08).
#
# Until this port landed, detect_triggers.py only knew about
# create-then-retest detection — it could not see that the trade
# had been invalidated between the creator and the retest. Symptom:
# Telegram alerts fired for setups the dashboard had already
# CANCELLED. These helpers close that gap so the Telegram pipeline
# and the dashboard converge on the same answer.
#
# Both functions return the bar index where the invalidation fired
# (so the caller can compare against trigger_bar_idx to discriminate
# pre- vs post-trigger), or -1 if no invalidation.

def detect_opposing_choch(bars, creator_idx, current_idx, setup_dir, min_prominence):
    """Multi-bar swing-flip detection. For a BEAR setup, fires when:
       1. A post-creator low forms
       2. Price bounces to a swing peak (high with lower next-high)
          AND the bounce is at least `chochProm` above the low
       3. A subsequent bar's close exceeds that swing peak
    Mirror logic for BULL setups (high -> swing trough -> close below).
    """
    if not bars or creator_idx < 0 or current_idx <= creator_idx + 2:
        return -1
    if current_idx >= len(bars):
        current_idx = len(bars) - 1
    if current_idx - creator_idx < 3:
        return -1

    choch_prom = (min_prominence or 0) * 0.3

    if setup_dir == 'bear':
        low_idx, low_val = -1, float('inf')
        for i in range(creator_idx + 1, current_idx - 1):
            l = bars[i].get('l')
            if l is None:
                continue
            if l < low_val:
                low_val = l
                low_idx = i
        if low_idx == -1:
            return -1
        swing_peak = None
        swing_peak_idx = -1
        for i in range(low_idx + 1, current_idx + 1):
            this_h = bars[i].get('h')
            this_c = bars[i].get('c')
            next_h = bars[i + 1].get('h') if (i + 1) < len(bars) else None
            if this_h is None or this_c is None:
                continue
            if swing_peak is None and next_h is not None and this_h > next_h:
                if (this_h - low_val) >= choch_prom:
                    swing_peak = this_h
                    swing_peak_idx = i
            if swing_peak is not None and i > swing_peak_idx:
                if this_c > swing_peak:
                    return i
                if (next_h is not None and this_h > next_h
                    and (this_h - low_val) >= choch_prom
                    and this_h > swing_peak):
                    swing_peak = this_h
                    swing_peak_idx = i
        return -1

    if setup_dir == 'bull':
        high_idx, high_val = -1, float('-inf')
        for i in range(creator_idx + 1, current_idx - 1):
            h = bars[i].get('h')
            if h is None:
                continue
            if h > high_val:
                high_val = h
                high_idx = i
        if high_idx == -1:
            return -1
        swing_trough = None
        swing_trough_idx = -1
        for i in range(high_idx + 1, current_idx + 1):
            this_l = bars[i].get('l')
            this_c = bars[i].get('c')
            next_l = bars[i + 1].get('l') if (i + 1) < len(bars) else None
            if this_l is None or this_c is None:
                continue
            if swing_trough is None and next_l is not None and this_l < next_l:
                if (high_val - this_l) >= choch_prom:
                    swing_trough = this_l
                    swing_trough_idx = i
            if swing_trough is not None and i > swing_trough_idx:
                if this_c < swing_trough:
                    return i
                if (next_l is not None and this_l < next_l
                    and (high_val - this_l) >= choch_prom
                    and this_l < swing_trough):
                    swing_trough = this_l
                    swing_trough_idx = i
        return -1

    return -1


def detect_consecutive_counter_bars(bars, from_idx, current_idx, setup_dir, min_prominence):
    """Two consecutive opposite-body candles with rising/falling closes,
    gated by a close-beyond check against bars[from_idx]'s wick extreme.

    Bear setup: two bull-body bars with body >= 25% of the noise floor,
    second bar's close > first's, AND second bar's close > the
    creator's wick high. Mirror for bull. Returns current_idx when
    the pair forms at (current_idx-1, current_idx), else -1.

    Matches the JS detectConsecutiveCounterBars
    (Viking_Invest_Trading_v69.html ~L13227) after the 2026-06-11ee
    rule change. The close-beyond gate was the only variant of seven
    A/B'd against the production walker that projected green ≥70%
    aggregate WR — see the dry-run history at the top of the JS
    function. Trades like the XAG/USD 2026-06-11 case (counter bars
    at +1/+2 past creator with closes well below the entry wick) no
    longer false-invalidate.
    """
    if not bars or from_idx is None or from_idx < 0:
        return -1
    if current_idx <= from_idx + 1:
        return -1
    if current_idx < 1 or current_idx >= len(bars):
        return -1
    prev = bars[current_idx - 1]
    curr = bars[current_idx]
    min_body = (min_prominence or 0) * 0.25

    def is_counter(b):
        o, c = b.get('o'), b.get('c')
        if o is None or c is None:
            return False
        if setup_dir == 'bear':
            if not (c > o):
                return False
            body = c - o
        elif setup_dir == 'bull':
            if not (c < o):
                return False
            body = o - c
        else:
            return False
        return body >= min_body

    if not is_counter(prev) or not is_counter(curr):
        return -1

    pc, cc = prev.get('c'), curr.get('c')
    if pc is None or cc is None:
        return -1

    # close-beyond gate (2026-06-11ee). Second counter bar's close
    # must push past bars[from_idx]'s wick extreme — the trade
    # entry zone. A pullback that stays below entry (bear) or above
    # entry (bull) is not enough to invalidate.
    from_bar = bars[from_idx] if 0 <= from_idx < len(bars) else None
    if from_bar:
        if setup_dir == 'bear':
            from_h = from_bar.get('h')
            if from_h is None or not (cc > from_h):
                return -1
        elif setup_dir == 'bull':
            from_l = from_bar.get('l')
            if from_l is None or not (cc < from_l):
                return -1

    if setup_dir == 'bear' and cc > pc:
        return current_idx
    if setup_dir == 'bull' and cc < pc:
        return current_idx
    return -1


# ── 1.618 Fib extension helper ─────────────────────────────────
# Mirror of autoFibTarget in Viking_Invest_Trading_v69.html (~L12676),
# locked to the 1.618 level only. The user-facing "next major target"
# requested in the Telegram alert (2026-06-10) — surfaces a stretch
# target so traders can scale out in two legs (1:1 R:R then 1.618 ext).
# Lookback 60×15m = 15h of macro context, same window the dashboard
# uses for its TV-style Auto Fib Retracement panel.
#
# 2026-06-11 algorithm fix:
#   Original logic took the GLOBAL extremes inside the lookback window
#   and required SH-before-SL for bear (SL-before-SH for bull). XAU/USD
#   and XAG/USD 2026-06-10 night dropped the line because both pairs
#   had down-then-up-then-down structure: global SL formed BEFORE the
#   counter-trend rally that printed the global SH, so the check
#   rejected what was actually a legitimate setup.
#
#   Anchor the second pivot at the CREATOR's wick extreme — for a bear
#   creator that's its LOW (the bar that broke structure DOWN), for a
#   bull creator that's its HIGH. Then search BEFORE the creator for
#   the opposite pivot. This matches Wickator's "creator is the W3
#   start" framing and validates as long as there's a higher high
#   (bull→SL) / lower low (bear→SH) somewhere in the lookback window.
AUTO_FIB_LOOKBACK = 60


def auto_fib_1618(bars, anchor_idx, entry, stop, setup_dir):
    """Project the 1.618 Fibonacci extension beyond the creator pivot.

    Returns {'target', 'leg', 'sh', 'sl', 'R', 'reward', 'rr'} or None
    when no valid pivot exists before the creator (no higher high before
    a bear creator's low / no lower low before a bull creator's high).
    """
    if not bars or anchor_idx is None or anchor_idx < 5:
        return None
    start = max(0, anchor_idx - AUTO_FIB_LOOKBACK)
    end = min(len(bars) - 1, anchor_idx)
    if end - start < 10:
        return None
    creator = bars[anchor_idx]
    if not creator:
        return None
    if setup_dir == 'bear':
        # Anchor: creator low (the swing low that broke structure).
        sl = creator.get('l')
        if sl is None:
            return None
        # Find the highest high BEFORE the creator in the lookback.
        sh = float('-inf'); sh_idx = -1
        for i in range(start, anchor_idx):
            h = bars[i].get('h')
            if h is None:
                continue
            if h > sh:
                sh = h; sh_idx = i
        if sh_idx < 0 or sh <= sl:
            return None
        leg = sh - sl
        target_1618 = sl - 0.618 * leg
        if entry is not None and target_1618 >= entry:
            return None
    else:
        # Bull mirror: anchor at creator high, find lowest low before.
        sh = creator.get('h')
        if sh is None:
            return None
        sl = float('inf'); sl_idx = -1
        for i in range(start, anchor_idx):
            l = bars[i].get('l')
            if l is None:
                continue
            if l < sl:
                sl = l; sl_idx = i
        if sl_idx < 0 or sl >= sh:
            return None
        leg = sh - sl
        target_1618 = sh + 0.618 * leg
        if entry is not None and target_1618 <= entry:
            return None
    R = abs(stop - entry) if stop is not None and entry is not None else 0
    reward = abs(target_1618 - entry) if entry is not None else 0
    rr = (reward / R) if R > 0 else None
    return {
        'target': target_1618,
        'leg': leg,
        'sh': sh,
        'sl': sl,
        'R': R,
        'reward': reward,
        'rr': rr,
    }


def detect_intraday_signal(bars, aligned_dir, lookback=8, search_bars=16,
                           expiry_bars=8, h1_rsi=None,
                           rsi_hi=80, rsi_lo=20, pair_class=None):
    """Port of the essential parts of detectIntradaySignal from
    dashboard.html (~L9290). Detects the state of the 1:1-RR intraday
    signal for a 4/4-aligned pair on the 15m timeframe.

    `h1_rsi` is the most recent 1H RSI(14) for this pair (or None when
    h1 history is too short to compute). Drives the A1 hard gate
    (RULES_VERSION 2026-06-10a, per-class refined) — bull setups with
    rsi ≥ rsi_hi / bear with ≤ rsi_lo are flagged invalidated with
    reason 'rsi-extreme' so the Telegram alert pipeline skips the
    setup, matching the dashboard's `rsiBlocked` collapse.

    Thresholds default to 80/20 (the pre-2026-06-10a behaviour). Caller
    should resolve the per-class values via rsi_gate_for(pair) and pass
    them in.

    Returns dict with:
      state: 'armed' | 'triggered' | 'expired' | None
      creator_idx: index of the creator bar (most recent CHoCH in alignedDir)
      creator_ts: timestamp of creator bar
      creator_high / creator_low: wick extremes used as entry
      entry: float — wick extreme on the entry side
      trigger_bar_idx / trigger_ts: if state == 'triggered'

    Returns None if no creator found in the search window or bars are
    too short.
    """
    n = len(bars)
    if n < lookback + 2:
        return None

    # Find most recent creator bar (close beyond 8-bar swing in alignedDir).
    creator_idx = -1
    search_start = max(lookback, n - search_bars)
    for i in range(search_start, n):
        lb = bars[max(0, i - lookback):i]
        if len(lb) < 5:
            continue
        swing_hi = max(b.get('h', float('-inf')) for b in lb)
        swing_lo = min(b.get('l', float('inf')) for b in lb)
        c = bars[i].get('c')
        if c is None:
            continue
        if aligned_dir == 'bull' and c > swing_hi:
            creator_idx = i
        elif aligned_dir == 'bear' and c < swing_lo:
            creator_idx = i

    if creator_idx == -1:
        return None

    creator = bars[creator_idx]
    bars_ago = n - 1 - creator_idx
    creator_high = creator.get('h')
    creator_low = creator.get('l')

    # Expiry check — matches detectIntradaySignal's EXPIRY_BARS=8.
    if bars_ago > expiry_bars:
        return {
            'state': 'expired',
            'creator_idx': creator_idx,
            'creator_ts': creator.get('t'),
            'creator_high': creator_high,
            'creator_low': creator_low,
            'entry': None,
            'trigger_bar_idx': -1,
            'trigger_ts': None,
        }

    # Entry is the wick extreme on the entry side: high for bear, low for bull.
    entry = creator_high if aligned_dir == 'bear' else creator_low

    # Fib-zone variant: 38% retrace of the creator candle, taken at HALF size.
    # Captures the "expired-no-retest" failure-mode cohort. Mirrors the
    # dashboard's detectIntradaySignal — see the comment block there.
    fib_entry = None
    if creator_high is not None and creator_low is not None and creator_high > creator_low:
        creator_range = creator_high - creator_low
        if aligned_dir == 'bear':
            fib_entry = creator_low + creator_range * 0.382  # 38% retrace from creator low
        else:
            fib_entry = creator_high - creator_range * 0.382

    # Structural stop + 1:1 target (mirrors detectIntradaySignal in
    # dashboard.html — BoS lookback 24 with prominence-aware swing
    # finder, target at 1:1 R:R from entry). Bundled into the result so
    # the Telegram alert and the dashboard reference the same levels.
    BOS_LOOKBACK = 24
    bos_slice = bars[max(0, creator_idx - BOS_LOOKBACK):creator_idx]
    creator_close = creator.get('c')
    px_for_prom = creator_close if creator_close is not None else creator_high
    min_prom = _bos_min_prominence(px_for_prom)
    stop = None
    target = None
    fib_target = None
    if bos_slice and entry is not None:
        if aligned_dir == 'bear':
            stop = _find_structural_high(bos_slice, min_prom)
            if stop is not None and stop > entry:
                target = entry - (stop - entry)
                if fib_entry is not None and stop > fib_entry:
                    fib_target = fib_entry - (stop - fib_entry)
        else:
            stop = _find_structural_low(bos_slice, min_prom)
            if stop is not None and stop < entry:
                target = entry + (entry - stop)
                if fib_entry is not None and stop < fib_entry:
                    fib_target = fib_entry + (fib_entry - stop)

    # ── Hybrid min-R floor ─────────────────────────────────────
    # Mirror of the JS detectIntradaySignal floor (Viking_Invest_Trading_v69
    # ~L13162). R must clear BOTH the ATR-relative threshold AND an
    # absolute FX pip floor; non-FX classes use ATR alone. Added to the
    # Python detector 2026-06-10n after a NZD/USD setup with R=8.5 pips
    # made it into a Telegram alert — the JS dashboard would have
    # rejected it via the same hybrid check, but the Python detector
    # had no min-R check at all, breaking dashboard-vs-Telegram parity.
    if stop is not None and entry is not None:
        R = abs(stop - entry)
        # Recent-range ATR (10 bars before creator) as a noise proxy.
        # Use abs(h - l) ≈ true range; close enough for the floor.
        atr_slice = bars[max(0, creator_idx - 10):creator_idx]
        atr_total = sum(abs(b.get('h', 0) - b.get('l', 0)) for b in atr_slice
                        if b.get('h') is not None and b.get('l') is not None)
        atr20 = atr_total / len(atr_slice) if atr_slice else 0
        atr_floor = 0.5 * atr20
        # Absolute FX floor — 12 pips on any FX pair (= 0.0012 non-JPY,
        # = 0.12 on JPY pairs at 2-decimal pricing). Catches the noise
        # tier that survives the ATR check when ATR itself has collapsed.
        is_fx = pair_class in ('major', 'minor')
        fx_floor = 0
        if is_fx:
            fx_floor = 0.12 if abs(entry) > 50 else 0.0012
        min_R = max(atr_floor, fx_floor)
        if min_R > 0 and R < min_R:
            return {
                'state': 'invalidated',
                'creator_idx': creator_idx,
                'creator_ts': creator.get('t'),
                'creator_high': creator_high,
                'creator_low': creator_low,
                'entry': None,
                'stop': stop,
                'target': None,
                'trigger_bar_idx': -1,
                'trigger_ts': None,
                'fib_state': 'invalidated',
                'fib_entry': None,
                'fib_target': None,
                'fib_trigger_bar_idx': -1,
                'fib_trigger_ts': None,
                'invalidation_reason': 'min-R-floor',
                'invalidation_bar_idx': creator_idx,
                'invalidation_ts': creator.get('t'),
            }
        # ── Max R cap (2026-06-10z, XAG/USD 226-pip case) ──────────
        # Mirror of the JS detectIntradaySignal cap. Scoped to
        # commodities + indices only after the comparison backtest
        # (backtest_max_r_cap.py) showed FX would lose 5.4pp on
        # minors with no gain elsewhere. The silver problem is
        # specific to FIB-class instruments whose structural stops
        # sit hours back on a strong trend — FX stops are already
        # tight, so the cap there just clips legitimate breathing
        # room. Auto-scales to pair volatility via ATR(20); floor
        # cannot be undercut.
        MAX_R_ATR_MULT = 2.5
        if pair_class in ('comm', 'index'):
            max_R = MAX_R_ATR_MULT * atr20
            if max_R > 0 and min_R > 0 and max_R < min_R:
                max_R = min_R  # never shrink below floor
            if max_R > 0 and R > max_R:
                if aligned_dir == 'bear':
                    stop = entry + max_R
                    target = entry - max_R
                    if fib_entry is not None and stop > fib_entry:
                        fib_target = fib_entry - (stop - fib_entry)
                else:
                    stop = entry - max_R
                    target = entry + max_R
                    if fib_entry is not None and stop < fib_entry:
                        fib_target = fib_entry + (fib_entry - stop)

    # Trigger walk: track BOTH wick and fib trigger bars independently. Both
    # variants share the same round-trip lift gate — neither fires until
    # price has displaced past the creator's far edge on a strictly earlier
    # bar (prevents false triggers on consecutive trend candles).
    #
    # Three invalidation paths are checked PRE-trigger on every bar
    # (mirror of detectIntradaySignal in the dashboard):
    #   1. close beyond the stop level (stop-breached pre-trigger)
    #   2. opposing CHoCH — multi-bar swing flip
    #   3. two consecutive NOWICK counter-bars heading away from us
    # If any of these fires before the wick/fib retest, the setup is
    # invalidated and BOTH triggers are suppressed. This is the
    # critical gap the JS dashboard handles but detect_triggers.py
    # didn't until 2026-06-08 — symptom was Telegram alerts on
    # setups the dashboard had already CANCELLED.
    trigger_bar_idx = -1
    fib_trigger_bar_idx = -1
    invalidated_pre_trigger = False
    invalidation_reason = None
    invalidation_bar_idx = -1

    # A1 hard gate (RULES_VERSION 2026-06-10a) — 1H RSI extreme, per
    # asset class. Block before the wick-walk so the trigger search
    # short-circuits. h1_rsi is None during the h1 warm-up window
    # (<16 bars) — fall through and let the alignment / wick logic
    # decide; matches the dashboard's behaviour when STATE.rsi1H is null.
    if h1_rsi is not None and isinstance(h1_rsi, (int, float)):
        if aligned_dir == 'bull' and h1_rsi >= rsi_hi:
            invalidated_pre_trigger = True
            invalidation_reason = 'rsi-extreme'
            invalidation_bar_idx = creator_idx
        elif aligned_dir == 'bear' and h1_rsi <= rsi_lo:
            invalidated_pre_trigger = True
            invalidation_reason = 'rsi-extreme'
            invalidation_bar_idx = creator_idx

    # min_prominence for the invalidation helpers — reuse the BoS
    # prominence floor so the chochProm guard matches the dashboard.
    _inval_prom = _bos_min_prominence(px_for_prom)

    if entry is not None and stop is not None and not invalidated_pre_trigger:
        lift_reached = False
        for j in range(creator_idx + 1, n):
            b = bars[j]
            bh, bl, bc = b.get('h'), b.get('l'), b.get('c')
            if bh is None or bl is None:
                continue
            # ── PRE-TRIGGER INVALIDATIONS ──────────────────────
            # Skip once either entry has fired — the live detector
            # handles post-trigger invalidations as the exit
            # mechanic, but the Telegram pipeline only needs to
            # decide whether to fire at all.
            if trigger_bar_idx < 0 and fib_trigger_bar_idx < 0:
                # 1. Close beyond stop
                if bc is not None:
                    if aligned_dir == 'bear' and bc > stop:
                        invalidated_pre_trigger = True
                        invalidation_reason = 'stop-breached'
                        invalidation_bar_idx = j
                        break
                    if aligned_dir == 'bull' and bc < stop:
                        invalidated_pre_trigger = True
                        invalidation_reason = 'stop-breached'
                        invalidation_bar_idx = j
                        break
                # 2. Opposing CHoCH
                opp_idx = detect_opposing_choch(bars, creator_idx, j,
                                                aligned_dir, _inval_prom)
                if opp_idx >= 0:
                    invalidated_pre_trigger = True
                    invalidation_reason = 'opposing-choch'
                    invalidation_bar_idx = opp_idx
                    break
                # 3. Two consecutive counter-bars (close-beyond gate
                #    — 2026-06-11ee — requires the 2nd bar's close to
                #    push past creator's wick extreme)
                cb_idx = detect_consecutive_counter_bars(bars, creator_idx, j,
                                                         aligned_dir, _inval_prom)
                if cb_idx >= 0:
                    invalidated_pre_trigger = True
                    invalidation_reason = 'counter-bars'
                    invalidation_bar_idx = cb_idx
                    break
            # Fib-zone trigger — shallower retrace, fires earlier than wick.
            if lift_reached and fib_trigger_bar_idx < 0 and fib_entry is not None:
                fib_reached = (
                    (aligned_dir == 'bear' and bh >= fib_entry) or
                    (aligned_dir == 'bull' and bl <= fib_entry)
                )
                if fib_reached:
                    fib_trigger_bar_idx = j
            # Wick trigger — deeper retrace, full size.
            if lift_reached and trigger_bar_idx < 0:
                reaches = (
                    (aligned_dir == 'bear' and bh >= entry) or
                    (aligned_dir == 'bull' and bl <= entry)
                )
                if reaches:
                    trigger_bar_idx = j
                    # Don't break — fib path may still be 'armed' on the
                    # same bar; we record it too. Most setups won't reach
                    # this branch because fib usually fires first.
            # Update lift flag AFTER the retest checks (lift must precede).
            if not lift_reached:
                if aligned_dir == 'bull' and creator_high is not None and bh >= creator_high:
                    lift_reached = True
                elif aligned_dir == 'bear' and creator_low is not None and bl <= creator_low:
                    lift_reached = True

    # If we invalidated pre-trigger, BOTH triggers are nullified so
    # the Telegram pipeline doesn't fire on the setup at all.
    if invalidated_pre_trigger:
        state = 'invalidated'
        fib_state = 'invalidated'
    else:
        state = 'triggered' if trigger_bar_idx >= 0 else 'armed'
        fib_state = 'triggered' if fib_trigger_bar_idx >= 0 else 'armed'

    # ── POST-TRIGGER OUTCOME WALK (2026-06-11ff) ───────────────
    # The Telegram pipeline now fires an EXIT alert when a previously
    # alerted triggered trade hits a structural invalidation (opposing
    # CHoCH or close-beyond counter-bars). Letting the user know they
    # fired = giving them a chance to bail before stop. Target / stop
    # hits don't produce an alert (target = win, stop = loss already
    # realised).
    #
    # Pre-2026-06-11ff the detector explicitly skipped post-trigger
    # invalidation tracking — comment at L948 said "the live detector
    # handles post-trigger invalidations as the exit mechanic, but the
    # Telegram pipeline only needs to decide whether to fire at all".
    # That changes with the exit alert. The walk is bounded by
    # TRIGGERED_MAX_BARS (16 = 4h on 15m) so an unresolved trade past
    # that window is marked 'stale-expired' and stops being checked.
    def _walk_post_trigger_outcome(trig_idx, lvl_stop, lvl_target):
        if trig_idx < 0:
            return None, None, -1, None
        TRIGGERED_MAX_BARS = 16
        for jj in range(trig_idx + 1, n):
            bb = bars[jj]
            bbh, bbl = bb.get('h'), bb.get('l')
            # Target / stop check — close-of-bar fills the wick logic
            # used in the live detector. Use range-touch for parity
            # with the JS dashboard.
            if aligned_dir == 'bear':
                if lvl_target is not None and bbl is not None and bbl <= lvl_target:
                    return 'target-hit', None, jj, bb.get('t')
                if lvl_stop is not None and bbh is not None and bbh >= lvl_stop:
                    return 'stop-hit', None, jj, bb.get('t')
            else:
                if lvl_target is not None and bbh is not None and bbh >= lvl_target:
                    return 'target-hit', None, jj, bb.get('t')
                if lvl_stop is not None and bbl is not None and bbl <= lvl_stop:
                    return 'stop-hit', None, jj, bb.get('t')
            # Opposing CHoCH (close-through past creator+1 swing)
            opp_post = detect_opposing_choch(bars, creator_idx, jj,
                                              aligned_dir, _inval_prom)
            if opp_post >= 0 and opp_post > trig_idx:
                return ('choch-invalidated', 'opposing-choch', opp_post,
                        bars[opp_post].get('t'))
            # 2-bar counter momentum (close-beyond gated since 11ee)
            cb_post = detect_consecutive_counter_bars(bars, trig_idx, jj,
                                                     aligned_dir, _inval_prom)
            if cb_post >= 0 and cb_post > trig_idx:
                return ('choch-invalidated', 'counter-bars', cb_post,
                        bars[cb_post].get('t'))
            # Time-based expiry
            if jj - trig_idx > TRIGGERED_MAX_BARS:
                return 'stale-expired', None, jj, bb.get('t')
        return None, None, -1, None

    outcome = outcome_reason = outcome_ts = None
    outcome_bar_idx = -1
    fib_outcome = fib_outcome_reason = fib_outcome_ts = None
    fib_outcome_bar_idx = -1
    if state == 'triggered':
        outcome, outcome_reason, outcome_bar_idx, outcome_ts = \
            _walk_post_trigger_outcome(trigger_bar_idx, stop, target)
    if fib_state == 'triggered':
        fib_outcome, fib_outcome_reason, fib_outcome_bar_idx, fib_outcome_ts = \
            _walk_post_trigger_outcome(fib_trigger_bar_idx, stop, fib_target)

    # Compute the 1.618 Fib extension off the swing high / swing low
    # found in the AUTO_FIB_LOOKBACK window ending at the creator. This
    # is the same projection TradingView's Auto Fib Retracement
    # indicator draws — used as a *stretch* target in the Telegram alert
    # so traders can scale out in two legs (1:1 R:R first, then 1.618).
    # Geometry can fail (e.g. bull setup where swing low came AFTER the
    # high in the window) — fib_ext_target stays None and the alert
    # line is dropped.
    fib_ext = auto_fib_1618(bars, creator_idx, entry, stop, aligned_dir)
    fib_ext_target = fib_ext['target'] if fib_ext else None
    fib_ext_rr = fib_ext['rr'] if fib_ext else None
    return {
        'state': state,
        'creator_idx': creator_idx,
        'creator_ts': creator.get('t'),
        'creator_high': creator_high,
        'creator_low': creator_low,
        'entry': entry,
        'stop': stop,
        'target': target,
        'trigger_bar_idx': trigger_bar_idx,
        'trigger_ts': bars[trigger_bar_idx].get('t') if trigger_bar_idx >= 0 else None,
        # Fib-zone half-size variant
        'fib_state': fib_state,
        'fib_entry': fib_entry,
        'fib_target': fib_target,
        'fib_trigger_bar_idx': fib_trigger_bar_idx,
        'fib_trigger_ts': bars[fib_trigger_bar_idx].get('t') if fib_trigger_bar_idx >= 0 else None,
        # 1.618 Fib extension target (advisory stretch target — separate
        # from the 1:1 R:R wick/fib targets above, which still drive
        # invalidation/expiry logic).
        'fib_ext_target': fib_ext_target,
        'fib_ext_rr': fib_ext_rr,
        # Invalidation metadata (added 2026-06-08). state == 'invalidated'
        # means the setup was killed pre-trigger by one of the three
        # invalidation paths checked above; the alert pipeline must NOT
        # send Telegram messages for these. reason is one of
        # stop-breached / opposing-choch / counter-bars / None.
        'invalidation_reason': invalidation_reason,
        'invalidation_bar_idx': invalidation_bar_idx,
        'invalidation_ts': (
            bars[invalidation_bar_idx].get('t')
            if invalidation_bar_idx >= 0 and invalidation_bar_idx < len(bars)
            else None
        ),
        # Post-trigger outcome (added 2026-06-11ff). state stays
        # 'triggered' regardless — outcome tells the main loop whether
        # to fire an exit alert. Values: 'target-hit', 'stop-hit',
        # 'choch-invalidated', 'stale-expired', or None (in flight).
        # Exit alert only fires on 'choch-invalidated' — the others
        # are already realised at the broker (target = win, stop =
        # locked-in loss, stale-expired = passive timeout).
        'outcome': outcome,
        'outcome_reason': outcome_reason,
        'outcome_bar_idx': outcome_bar_idx,
        'outcome_ts': outcome_ts,
        'fib_outcome': fib_outcome,
        'fib_outcome_reason': fib_outcome_reason,
        'fib_outcome_bar_idx': fib_outcome_bar_idx,
        'fib_outcome_ts': fib_outcome_ts,
    }


# Trigger alerts older than this are suppressed (the trigger likely fired
# in an earlier cycle that the detector missed; alerting now would
# misrepresent a stale price level the user can no longer act on).
#
# 2026-06-11ii: raised 30 → 45 min after the user reported missing
# trigger alerts on GBP/USD and NZD/USD while the dashboard correctly
# showed them as triggered. Most likely cause was a GitHub Actions cron
# throttle gap (the inline run lives inside fetch-data.yml, which itself
# can be skipped/delayed by Actions). 45 min covers 4-5 missed 10-min
# cron cycles, which is the worst observed throttle case. If the
# alert is genuinely > 45 min stale the price has moved past entry
# anyway, so the alert would be more confusing than useful.
MAX_TRIGGER_AGE_MIN = 45


def _trigger_age_minutes(trigger_ts):
    """Return minutes since `trigger_ts` (an ISO 8601 string), or None."""
    if not trigger_ts:
        return None
    try:
        ts = str(trigger_ts).replace('Z', '+00:00')
        dt = datetime.fromisoformat(ts)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
    except (ValueError, AttributeError, TypeError):
        return None


def _fmt_macro_px(p):
    """Format a price for the macro setup display strings (server-side
    counterpart of dashboard.html's fmtPx — chooses decimals by magnitude
    so the same string would be produced for a given pair regardless of
    whether the renderer is JS or Python)."""
    if p is None or p != p or p in (float('inf'), float('-inf')):
        return '—'
    ap = abs(p)
    if ap >= 1000:
        return f'{p:,.0f}'
    if ap >= 100:
        return f'{p:.3f}'
    if ap >= 10:
        return f'{p:.3f}'
    if ap >= 1:
        return f'{p:.4f}'
    return f'{p:.5f}'


def calc_macro_auto_setup(daily_bars, ew_dir):
    """Server-side port of dashboard.html's getAutoSetup. Derives an
    approximate macro setup from recent daily structure (38.2-61.8%
    retrace zone, structural stop, measured-move targets) aligned to
    ew_dir. Returns dict with display strings + numeric levels, or None.

    Shipped in directions.json so pairs without a manual WickatorFX seed
    (the v5-added FX/indices/crypto) still get real numbers in the
    dashboard's Watch Zone / Stop Reference / Target / Inv-Reward fields
    even when the browser hasn't loaded the deep daily history (mobile
    quota limits make that load unreliable).
    """
    if ew_dir not in ('bull', 'bear'):
        return None
    if not daily_bars or len(daily_bars) < 20:
        return None
    window = daily_bars[-60:]
    hi = float('-inf')
    lo = float('inf')
    for b in window:
        h = b.get('h') if b.get('h') is not None else b.get('c')
        l = b.get('l') if b.get('l') is not None else b.get('c')
        if h is not None and h > hi:
            hi = h
        if l is not None and l < lo:
            lo = l
    if hi == float('-inf') or lo == float('inf') or hi <= lo:
        return None
    rng = hi - lo

    if ew_dir == 'bull':
        z_hi = hi - rng * 0.382
        z_lo = hi - rng * 0.618
        stop_lvl = lo - rng * 0.05
        tp1 = hi + rng * 0.5
        tp2 = hi + rng * 1.0
        entry_edge = z_hi
        zone_type = 'BUY ZONE'
        stop_word = 'Below'
        inv_sym = '<'
    else:
        z_lo = lo + rng * 0.382
        z_hi = lo + rng * 0.618
        stop_lvl = hi + rng * 0.05
        tp1 = lo - rng * 0.5
        tp2 = lo - rng * 1.0
        if tp2 <= 0:
            tp2 = lo * 0.5
        if tp1 <= 0:
            tp1 = lo * 0.75
        entry_edge = z_lo
        zone_type = 'SELL ZONE'
        stop_word = 'Above'
        inv_sym = '>'

    risk = abs(entry_edge - stop_lvl)
    reward = abs(tp1 - entry_edge)
    if risk > 0 and risk == risk:  # finite & non-NaN
        rr = f'1:{reward / risk:.1f}'
    else:
        rr = '1:1.5'

    return {
        'dir': ew_dir,
        'approx': True,
        'anchor': lo if ew_dir == 'bull' else hi,
        'pivot': hi if ew_dir == 'bull' else lo,
        'w2Zone': [z_lo, z_hi],
        'invalid': stop_lvl,
        'tp1': tp1,
        'tp2': tp2,
        'entry': f'{_fmt_macro_px(z_lo)}–{_fmt_macro_px(z_hi)} {zone_type} (approx)',
        'stop':  f'{stop_word} {_fmt_macro_px(stop_lvl)}',
        'inv':   f'Close{inv_sym}{_fmt_macro_px(stop_lvl)}',
        'tgt':   f'{_fmt_macro_px(tp1)} / {_fmt_macro_px(tp2)} (approx)',
        'rr':    rr,
    }


# ── AUTO-EW: pivot-hierarchy Elliott Wave detector ─────────────
# Faithful port of dashboard.html's autoDetectEW stack. Same multi-degree
# ZigZag pivot detection + 5-wave / ABC / WXY / in-progress-W2 validators
# + macro-priority resolver. Used by scan_pairs to set the EW direction
# when a high-confidence pattern is detected (≥ AUTO_EW_MIN_CONFIDENCE),
# matching the dashboard's blend so the Telegram alert and the dashboard
# Macro pill agree on direction.

AUTO_EW_MIN_CONFIDENCE = 0.70
AUTO_EW_VALID_PATTERNS = {
    '5-wave-impulse-complete',
    '5-wave-diagonal-complete',
    '5-wave-impulse-truncated',
    'WXY-double-zigzag-complete',
    'ABC-correction-complete',
    'in-progress-impulse-w2',
}
AUTO_EW_THRESHOLDS = [0.5, 0.8, 1.0, 1.5, 2.5, 4.0, 6.0, 8.0, 10.0, 12.0]


# ── PER-INSTRUMENT-CLASS ENTRY METHODOLOGY ─────────────────────
# Mirrors dashboard.html's _btMethodFor(k). Per the May 2026 backtest
# comparison (struct vs auto-EW vs auto-EW + 50/200), two entry
# methodologies are active — chosen per instrument class:
#
#   FX (majors + minors) + crypto   -> WICK extreme-candle entry,
#                                      full 1R risk
#   Commodities (xau, xag, brent)
#   + indices (DAX, NAS, etc.)      -> FIB 38% retrace entry,
#                                      half 0.5R risk
#
# Telegram triggers fire only on the entry that matches the pair's
# class (no wick alerts on Brent; no Fib alerts on EUR/USD).
FIB_ENTRY_PAIRS = {
    # Commodities
    'xauusd', 'xagusd', 'usoil', 'wtiusd', 'natgas', 'xptusd',
    # Indices (pair keys must match fetch-prices.js and MKTS — earlier
    # versions used 'us30'/'uk100' which never matched the actual
    # dj30/ftse100 keys used everywhere else, so server-side gating
    # silently fell through to no-fib-class on those two pairs).
    # de40, dj30 briefly moved to wick methodology in 2026-06-10i based
    # on a simplified backtest showing +9-12pp wick lift (n=6-8). 10j
    # reverted both after production realised 50% / 47.8% on the larger
    # auto-EW-driven sample. Awaiting auto-EW-integrated backtest before
    # any further methodology change.
    # de40 dropped 2026-06-13kk (chronic negative E[R]).
    # de40 REINSTATED 2026-06-15lll — earlier drop was driven in part by
    # the stale `dir:'bear'` inferred macro seed forcing aggressive bear
    # targets that didn't match recent price action. With the seed
    # retired client-side and auto-derived targets in play, re-evaluate
    # WR after one full backtest cycle.
    'de40', 'nas100', 'dj30', 'ftse100', 'spx500',
    # v7 additions (2026-06-03)
    'jp225',
    # fra40 (CAC 40) removed 2026-06-10 — low win-rate drag.
    # IBEX 35 (esp35) was added then removed 2026-06-08 — OANDA ticker
    # never validated. See removal notes in fetch-prices.js.
}

def uses_fib_entry(pair):
    return pair in FIB_ENTRY_PAIRS


def _detect_pivots_refined(prices, pct_threshold):
    """ZigZag pivot detector. Walks the close series, emits a pivot
    whenever price reverses by `pct_threshold`% from the last extreme."""
    if not prices or len(prices) < 5:
        return []
    thresh = pct_threshold / 100.0
    pivots = []
    direction = 1 if prices[1] > prices[0] else -1
    last_extreme = prices[0]
    last_idx = 0
    pivots.append({'type': 'L' if direction == 1 else 'H',
                   'price': prices[0], 'idx': 0, 'isOrigin': True})
    for i in range(1, len(prices)):
        if last_extreme == 0:
            dist = 0
        else:
            dist = abs(prices[i] - last_extreme) / abs(last_extreme)
        if direction == 1:
            if prices[i] > last_extreme:
                last_extreme = prices[i]
                last_idx = i
            elif dist >= thresh:
                pivots.append({'type': 'H', 'price': last_extreme, 'idx': last_idx})
                direction = -1
                last_extreme = prices[i]
                last_idx = i
        else:
            if prices[i] < last_extreme:
                last_extreme = prices[i]
                last_idx = i
            elif dist >= thresh:
                pivots.append({'type': 'L', 'price': last_extreme, 'idx': last_idx})
                direction = 1
                last_extreme = prices[i]
                last_idx = i
    pivots.append({'type': 'H' if direction == 1 else 'L',
                   'price': last_extreme, 'idx': last_idx})
    # Deduplicate the origin if same-type as next.
    if len(pivots) >= 2 and pivots[0].get('isOrigin') and pivots[0]['type'] == pivots[1]['type']:
        pivots = pivots[1:]
    return pivots


def _build_pivot_hierarchy(prices):
    """Run the ZigZag at 10 increasing thresholds; each level captures a
    different fractal degree. Used by the macro-priority resolver."""
    levels = []
    for i, t in enumerate(AUTO_EW_THRESHOLDS):
        pivs = _detect_pivots_refined(prices, t)
        if pivs and len(pivs) >= 2:
            levels.append({
                'degree': i,
                'thresholdPct': t,
                'pivots': pivs,
                'pivotCount': len(pivs),
            })
    return levels


def _check_w41_overlap(p, direction):
    if len(p) < 5:
        return {'isImpulse': True, 'isDiagonal': False, 'overlapPct': 0}
    w1_end = p[1]['price']
    w4_end = p[4]['price']
    w1_start = p[0]['price']
    w1_range = abs(w1_end - w1_start)
    if w1_range == 0:
        return {'isImpulse': True, 'isDiagonal': False, 'overlapPct': 0}
    if direction == 'bull':
        overlap = w1_end - w4_end
    else:
        overlap = w4_end - w1_end
    overlap_pct = overlap / w1_range
    if overlap_pct <= 0:
        return {'isImpulse': True, 'isDiagonal': False, 'overlapPct': 0}
    if overlap_pct < 0.30:
        return {'isImpulse': False, 'isDiagonal': True, 'overlapPct': overlap_pct}
    return {'isImpulse': False, 'isDiagonal': False, 'overlapPct': overlap_pct}


def _check_wedge_shape(p, direction):
    if len(p) < 5:
        return {'isWedge': False, 'convergenceRatio': 1.0}
    x1, y1 = p[1]['idx'], p[1]['price']
    x3, y3 = p[3]['idx'], p[3]['price']
    x2, y2 = p[2]['idx'], p[2]['price']
    x4, y4 = p[4]['idx'], p[4]['price']
    if x3 == x1 or x4 == x2:
        return {'isWedge': False, 'convergenceRatio': 1.0}
    slope_13 = (y3 - y1) / (x3 - x1)
    slope_24 = (y4 - y2) / (x4 - x2)
    dist1 = abs(y1 - (y2 + slope_24 * (x1 - x2)))
    dist4 = abs((y1 + slope_13 * (x4 - x1)) - y4)
    if dist1 == 0:
        return {'isWedge': False, 'convergenceRatio': 1.0}
    ratio = dist4 / dist1
    return {'isWedge': ratio < 0.70, 'convergenceRatio': ratio}


def _check_truncation(p, direction):
    if len(p) < 6:
        return {'isTruncated': False, 'truncationPct': 0}
    w3_end = p[3]['price']
    w5_end = p[5]['price']
    w4_end = p[4]['price']
    w4_length = abs(w4_end - w3_end)
    if w4_length == 0:
        return {'isTruncated': False, 'truncationPct': 0}
    if direction == 'bull':
        truncated = w5_end < w3_end
        shortfall = (w3_end - w5_end) / w4_length if truncated else 0
    else:
        truncated = w5_end > w3_end
        shortfall = (w5_end - w3_end) / w4_length if truncated else 0
    return {'isTruncated': truncated, 'truncationPct': shortfall}


def _check_channel_projection(p, direction):
    if len(p) < 5:
        return {'withinChannel': True, 'deviation': 0}
    x1, y1 = p[1]['idx'], p[1]['price']
    x3, y3 = p[3]['idx'], p[3]['price']
    x4, y4 = p[4]['idx'], p[4]['price']
    x2, y2 = p[2]['idx'], p[2]['price']
    if x3 == x1:
        return {'withinChannel': True, 'deviation': 0}
    slope = (y3 - y1) / (x3 - x1)
    lower_at_x4 = y2 + slope * (x4 - x2)
    w1 = abs(p[1]['price'] - p[0]['price'])
    if w1 == 0:
        return {'withinChannel': True, 'deviation': 0}
    if direction == 'bull':
        deviation = (lower_at_x4 - y4) / w1 if y4 < lower_at_x4 else 0
    else:
        deviation = (y4 - lower_at_x4) / w1 if y4 > lower_at_x4 else 0
    return {'withinChannel': deviation < 0.20, 'deviation': deviation}


def _check_wave_five_fib(p, direction):
    if len(p) < 6:
        return None
    w1 = abs(p[1]['price'] - p[0]['price'])
    if w1 == 0:
        return None
    w5 = abs(p[5]['price'] - p[4]['price'])
    ratio = w5 / w1
    fibs = [0.618, 1.0, 1.618]
    nearest = fibs[0]
    min_dist = abs(ratio - fibs[0])
    for f in fibs[1:]:
        d = abs(ratio - f)
        if d < min_dist:
            min_dist = d
            nearest = f
    return {'ratio': ratio, 'nearestFib': nearest, 'distancePct': min_dist / nearest}


def _validate_five_wave_impulse(pivots):
    if len(pivots) < 6:
        return None
    p = pivots[-6:]
    bull = (p[0]['type'] == 'L' and p[1]['type'] == 'H' and p[2]['type'] == 'L'
            and p[3]['type'] == 'H' and p[4]['type'] == 'L' and p[5]['type'] == 'H')
    bear = (p[0]['type'] == 'H' and p[1]['type'] == 'L' and p[2]['type'] == 'H'
            and p[3]['type'] == 'L' and p[4]['type'] == 'H' and p[5]['type'] == 'L')
    if not bull and not bear:
        return None
    direction = 'bull' if bull else 'bear'
    w1 = abs(p[1]['price'] - p[0]['price'])
    w3 = abs(p[3]['price'] - p[2]['price'])
    w5 = abs(p[5]['price'] - p[4]['price'])
    w2 = abs(p[2]['price'] - p[1]['price'])
    w4 = abs(p[4]['price'] - p[3]['price'])
    # Rule: W2 does not retrace past W1 start
    if bull and p[2]['price'] <= p[0]['price']:
        return None
    if bear and p[2]['price'] >= p[0]['price']:
        return None
    # Rule: W4 does not overlap W1 territory (with diagonal carve-out)
    overlap = _check_w41_overlap(p, direction)
    wedge = _check_wedge_shape(p, direction)
    is_diagonal = False
    if not overlap['isImpulse']:
        if overlap['isDiagonal'] and wedge['isWedge']:
            is_diagonal = True
        elif overlap['isDiagonal']:
            is_diagonal = True
        else:
            return None
    # Rule: W3 not shortest
    if w3 < w1 and w3 < w5:
        return None
    truncation = _check_truncation(p, direction)
    if truncation['isTruncated'] and truncation['truncationPct'] > 0.30:
        return None
    w2_ratio = w2 / w1 if w1 else 0
    w4_ratio = w4 / w3 if w3 else 0
    channel = _check_channel_projection(p, direction)
    w5_fib = _check_wave_five_fib(p, direction)
    conf = 0.5
    if 0.382 <= w2_ratio <= 0.786:
        conf += 0.10
    if 0.236 <= w4_ratio <= 0.500:
        conf += 0.10
    if w3 >= w1 and w3 >= w5:
        conf += 0.15
    if channel.get('withinChannel'):
        conf += 0.10
    if w5_fib and w5_fib['distancePct'] < 0.20:
        conf += 0.10
    elif w5_fib and w5_fib['distancePct'] < 0.40:
        conf += 0.05
    if is_diagonal:
        conf -= 0.15
    if truncation['isTruncated']:
        conf -= 0.10
        if truncation['truncationPct'] < 0.10:
            conf += 0.03
    conf = max(0.1, min(1.0, conf))
    if is_diagonal:
        pattern_label = '5-wave-diagonal-complete'
    elif truncation['isTruncated']:
        pattern_label = '5-wave-impulse-truncated'
    else:
        pattern_label = '5-wave-impulse-complete'
    return {'dir': direction, 'confidence': conf, 'pattern': pattern_label}


def _validate_abc_correction(pivots):
    if len(pivots) < 4:
        return None
    p = pivots[-4:]
    bear_abc = (p[0]['type'] == 'H' and p[1]['type'] == 'L'
                and p[2]['type'] == 'H' and p[3]['type'] == 'L')
    bull_abc = (p[0]['type'] == 'L' and p[1]['type'] == 'H'
                and p[2]['type'] == 'L' and p[3]['type'] == 'H')
    if not bear_abc and not bull_abc:
        return None
    direction = 'bear' if bear_abc else 'bull'
    wa = abs(p[1]['price'] - p[0]['price'])
    wb = abs(p[2]['price'] - p[1]['price'])
    wc = abs(p[3]['price'] - p[2]['price'])
    if wa == 0:
        return None
    b_ratio = wb / wa
    if b_ratio < 0.382 or b_ratio > 0.786:
        return None
    c_ratio = wc / wa
    if c_ratio < 0.618:
        return None
    conf = 0.5
    if 0.50 <= b_ratio <= 0.618:
        conf += 0.20
    if 1.0 <= c_ratio <= 1.618:
        conf += 0.20
    conf = min(1.0, conf)
    return {'dir': direction, 'confidence': conf, 'pattern': 'ABC-correction-complete'}


def _validate_wxy_double(pivots):
    if len(pivots) < 8:
        return None
    p = pivots[-8:]
    bear_wxy = all(p[i]['type'] == ('H' if i % 2 == 0 else 'L') for i in range(8))
    bull_wxy = all(p[i]['type'] == ('L' if i % 2 == 0 else 'H') for i in range(8))
    if not bear_wxy and not bull_wxy:
        return None
    direction = 'bear' if bear_wxy else 'bull'
    wa = abs(p[1]['price'] - p[0]['price'])
    wb = abs(p[2]['price'] - p[1]['price'])
    xw = abs(p[4]['price'] - p[3]['price'])
    ya = abs(p[5]['price'] - p[4]['price'])
    yb = abs(p[6]['price'] - p[5]['price'])
    w_span = abs(p[3]['price'] - p[0]['price'])
    y_span = abs(p[7]['price'] - p[4]['price'])
    wb_ratio = (wb / wa) if wa else 0
    yb_ratio = (yb / ya) if ya else 0
    x_ratio = (xw / w_span) if w_span else 0
    if wb_ratio < 0.382 or wb_ratio > 0.786:
        return None
    if yb_ratio < 0.382 or yb_ratio > 0.786:
        return None
    if x_ratio < 0.236 or x_ratio > 1.0:
        return None
    conf = 0.45
    if 0.50 <= yb_ratio <= 0.618:
        conf += 0.15
    if w_span > 0 and w_span * 0.8 <= y_span <= w_span * 1.5:
        conf += 0.20
    if 0.382 <= x_ratio <= 0.618:
        conf += 0.10
    conf = min(1.0, conf)
    return {'dir': direction, 'confidence': conf, 'pattern': 'WXY-double-zigzag-complete'}


def _validate_in_progress_impulse(pivots, current_px):
    if len(pivots) < 2 or current_px is None:
        return None
    p = pivots[-2:]
    bull = p[0]['type'] == 'L' and p[1]['type'] == 'H'
    bear = p[0]['type'] == 'H' and p[1]['type'] == 'L'
    if not bull and not bear:
        return None
    direction = 'bull' if bull else 'bear'
    w1_start = p[0]['price']
    w1_end = p[1]['price']
    w1_length = abs(w1_end - w1_start)
    if w1_length == 0:
        return None
    if w1_start == 0:
        return None
    w1_pct = w1_length / abs(w1_start)
    if w1_pct < 0.015:
        return None
    if bull and current_px > w1_end * 1.001:
        return None
    if bear and current_px < w1_end * 0.999:
        return None
    if bull:
        w2_retrace = (w1_end - current_px) / w1_length
    else:
        w2_retrace = (current_px - w1_end) / w1_length
    if w2_retrace < 0.236:
        return None
    conf = 0.30
    if w1_pct >= 0.025:
        conf += 0.10
    if 0.382 <= w2_retrace <= 0.618:
        conf += 0.15
    if 0.5 <= w2_retrace <= 0.618:
        conf += 0.10
    conf = min(0.70, conf)
    return {'dir': direction, 'confidence': conf, 'pattern': 'in-progress-impulse-w2'}


def _detect_at_degree(level, last_px):
    pivs = level['pivots']
    if len(pivs) < 2:
        return None
    candidates = []
    if len(pivs) >= 6:
        c = _validate_five_wave_impulse(pivs)
        if c:
            candidates.append(c)
    if len(pivs) >= 8:
        c = _validate_wxy_double(pivs)
        if c:
            candidates.append(c)
    if len(pivs) >= 4:
        c = _validate_abc_correction(pivs)
        if c:
            candidates.append(c)
    c = _validate_in_progress_impulse(pivs, last_px)
    if c:
        candidates.append(c)
    if not candidates:
        return None
    type_score_map = {
        '5-wave-impulse-complete': 4,
        '5-wave-diagonal-complete': 3.7,
        '5-wave-impulse-truncated': 3.5,
        'WXY-double-zigzag-complete': 3,
        'ABC-correction-complete': 2,
        'in-progress-impulse-w2': 1,
    }
    candidates.sort(key=lambda c: type_score_map.get(c['pattern'], 0) + c['confidence'],
                    reverse=True)
    best = candidates[0]
    best['degree'] = level['degree']
    best['pivotCount'] = len(pivs)
    return best


def auto_detect_ew(daily_bars):
    """Multi-degree pivot-hierarchy EW detector — Python port of
    autoDetectEW from dashboard.html. Returns dict matching the JS shape:
        {'ok': bool, 'ew': {dir, pattern, confidence, degree, ...}}
    Returns {'ok': False} on insufficient data or no valid pattern.
    """
    if not daily_bars or len(daily_bars) < 30:
        return {'ok': False, 'reason': 'insufficient_history'}
    prices = [b.get('c') for b in daily_bars if b.get('c') is not None]
    if len(prices) < 30:
        return {'ok': False, 'reason': 'insufficient_history'}
    last_px = prices[-1]
    hierarchy = _build_pivot_hierarchy(prices)
    if not hierarchy:
        return {'ok': False, 'reason': 'no_valid_pattern'}
    all_results = []
    for level in hierarchy:
        r = _detect_at_degree(level, last_px)
        if r:
            all_results.append(r)
    if not all_results:
        return {'ok': False, 'reason': 'no_valid_pattern'}
    type_score_map = {
        '5-wave-impulse-complete': 4,
        '5-wave-diagonal-complete': 3.7,
        '5-wave-impulse-truncated': 3.5,
        'WXY-double-zigzag-complete': 3,
        'ABC-correction-complete': 2,
        'in-progress-impulse-w2': 1,
    }

    def rank(r):
        ts = type_score_map.get(r['pattern'], 0)
        freshness_ok = True
        if r['pattern'] == 'in-progress-impulse-w2' and r.get('pivotCount', 0) < 3:
            freshness_ok = False
        return ts * 100 + r['degree'] * 10 + r['confidence'] + (0 if freshness_ok else -50)

    all_results.sort(key=rank, reverse=True)
    return {'ok': True, 'ew': all_results[0], 'allLevels': all_results}


def aggregate_h1_to_h4(h1_bars):
    """Group h1 OHLC bars into UTC-aligned 4H buckets (00, 04, 08, 12, 16, 20)."""
    groups = defaultdict(list)
    for b in h1_bars:
        ts = b.get('t') or ''
        try:
            hour = int(ts[11:13])
        except (ValueError, IndexError):
            continue
        bucket_hour = (hour // 4) * 4
        bucket_key = ts[:11] + f'{bucket_hour:02d}'
        groups[bucket_key].append(b)
    h4 = []
    for bk in sorted(groups.keys()):
        bars = groups[bk]
        if not bars:
            continue
        bars_sorted = sorted(bars, key=lambda b: b.get('t', ''))
        try:
            o = bars_sorted[0].get('o') or bars_sorted[0].get('c')
            c = bars_sorted[-1].get('c')
            hi = max(b['h'] for b in bars_sorted if b.get('h') is not None)
            lo = min(b['l'] for b in bars_sorted if b.get('l') is not None)
        except (KeyError, ValueError):
            continue
        h4.append({'t': bk + ':00:00Z', 'o': o, 'h': hi, 'l': lo, 'c': c})
    return h4


def ema(values, period):
    """Standard EMA. Seed = SMA of the first `period` values."""
    if not values or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def calc_4h_cloud_dir(h1_bars, fast=21, slow=55):
    """4H EMA cloud direction (21/55 by default).

    Aggregates h1 → 4H, computes EMA(fast) and EMA(slow) on closes:
      'bull'    if EMA(fast) > EMA(slow) AND price is not below both EMAs
                AND last 3 4H closes are not monotonically lower
      'bear'    if EMA(fast) < EMA(slow) AND price is not above both EMAs
                AND last 3 4H closes are not monotonically higher
      'neutral' if equal / insufficient data / price has crossed the cloud
                / momentum-reversal sequence detected

    Two guards on top of the EMA stack:

    1. Price-through-cloud (2026-06-08): bull stack with current price
       below BOTH EMAs means price has broken through the cloud — the
       stack is lagging, the move has already happened. Mirror for bear.

    2. Momentum-reversal sequence (2026-06-10f): bull stack with the
       last 3 4H closes monotonically lower (`<<<` pattern) means the
       cloud is rolling over even though the slow EMAs haven't crossed
       yet. The EMA21 already loses momentum 1-2 bars before the cross;
       this guard catches that early-warning state instead of waiting
       for the lagging cross. Mirror for bear stack with monotonically
       higher closes (`>>>`). Sanity-tested across the live universe:
       9/40 pairs (22%) flip from a stale stack-direction to neutral
       when this fires, all on pairs visibly rolling over on TV (the
       EURAUD case the user flagged was one of them).
    """
    h4 = aggregate_h1_to_h4(h1_bars)
    closes = [b['c'] for b in h4 if b.get('c') is not None]
    if len(closes) < slow:
        return 'neutral'
    e_fast = ema(closes, fast)
    e_slow = ema(closes, slow)
    if e_fast is None or e_slow is None:
        return 'neutral'
    last_c = closes[-1]
    # Momentum-reversal sequence — 3 monotonic 4H closes against the
    # stack. None when we don't have 4 closes to derive 3 deltas.
    rev_against_bull = False
    rev_against_bear = False
    if len(closes) >= 4:
        c4, c3, c2, c1 = closes[-4], closes[-3], closes[-2], closes[-1]
        rev_against_bull = (c3 < c4 and c2 < c3 and c1 < c2)
        rev_against_bear = (c3 > c4 and c2 > c3 and c1 > c2)
    if e_fast > e_slow:
        if last_c < e_fast and last_c < e_slow:
            return 'neutral'
        if rev_against_bull:
            return 'neutral'
        return 'bull'
    if e_fast < e_slow:
        if last_c > e_fast and last_c > e_slow:
            return 'neutral'
        if rev_against_bear:
            return 'neutral'
        return 'bear'
    return 'neutral'


def aggregate_m15_to_h1(m15_bars):
    """Group consecutive m15 bars into h1 OHLC bars by hour key."""
    groups = defaultdict(list)
    for b in m15_bars:
        ts = b.get('t')
        if not ts:
            continue
        # Normalise to ISO and pull the hour prefix YYYY-MM-DDTHH
        hour_key = ts[:13]
        groups[hour_key].append(b)
    h1 = []
    for hk in sorted(groups.keys()):
        bars = groups[hk]
        if not bars:
            continue
        # Bars within an hour are already in m15 order
        bars_sorted = sorted(bars, key=lambda b: b.get('t', ''))
        h1.append({
            't': hk + ':00:00Z',
            'o': bars_sorted[0].get('o') or bars_sorted[0].get('c'),
            'h': max(b['h'] for b in bars_sorted),
            'l': min(b['l'] for b in bars_sorted),
            'c': bars_sorted[-1].get('c'),
        })
    return h1


def build_h1_series(pair, m15, historical_pairs):
    """Build the freshest h1 OHLC series for the TL (hourly) direction
    calc. Uses the native h1 bars from historical-ohlc.json (~6000 bars,
    deterministic) and appends recent hours aggregated from the intraday
    15m bars on top.

    Previously the detector aggregated h1 only from intraday-ohlc.json's
    ~50 m15 bars (~12 h1 bars) — too thin and divergent from the
    dashboard, which (after the DEEP_HIST fallback fix in
    calcIndependentTLDir) reads the same deep h1 data. Matching the
    source is what keeps detector alignment in step with the dashboard.
    """
    hist_h1 = list(historical_pairs.get(pair, {}).get('h1', []))
    recent_h1 = aggregate_m15_to_h1(m15)
    if not hist_h1:
        return recent_h1
    last_prefix = (hist_h1[-1].get('t') or '')[:13]  # "YYYY-MM-DDTHH"
    merged = list(hist_h1)
    for b in recent_h1:
        if (b.get('t') or '')[:13] > last_prefix:
            merged.append(b)
    return merged


# ── Data loading ──

def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        print(f'WARN: {path} failed to parse: {e}')
        return None


def load_alerts_state(path='alerts-state.json'):
    """Returns ({pair: {...}}, sr_alerts_state, is_first_run).

    sr_alerts_state is {YYYY-MM-DD: {pair: {tier: count}}} tracking the
    per-tier daily SR alert counters. Older date keys are pruned at
    save time so the file doesn't grow unboundedly.
    """
    state = load_json(path)
    if state is None:
        return {}, {}, True
    return state.get('pairs', {}), state.get('sr_alerts', {}), False


# ── Pair scanner ──

def scan_pairs(intraday_data, historical_data):
    """For each pair available in intraday-ohlc.json, compute current
    EW / TL / NW direction and whether 4/4 aligned. Returns dict:
        { pair: { ew, tl, nw, aligned_dir, price } }
    where aligned_dir is 'bull'/'bear' if all three agree, else None.
    """
    out = {}
    intraday_pairs = (intraday_data or {}).get('intraday', {})
    historical_pairs = (historical_data or {}).get('pairs', {})

    for pair, m15 in intraday_pairs.items():
        if not m15 or len(m15) < 24:
            continue

        h1 = build_h1_series(pair, m15, historical_pairs)
        daily = historical_pairs.get(pair, {}).get('daily', [])

        # Need at least minimal history on each timeframe
        if len(daily) < 30 or len(h1) < 12 or len(m15) < 16:
            continue

        # NW (15m) uses lookback=5 — matches the dashboard's
        # calcIndependentNWDir change on 2026-05-30. The 8-bar window
        # was missing CHoCH-style pivot breaks that TradingView's 3-5
        # bar pivot detection picks up (DAX 30 2026-05-30 case). TL
        # (1H) and EW (daily) keep lookback=8 — longer-timeframe
        # structure needs deeper breaks before flipping.
        nw = calc_independent_dir(m15, lookback=5)
        tl = calc_independent_dir(h1, lookback=8)
        ew_structural = calc_independent_dir(daily, lookback=8)
        cl = calc_4h_cloud_dir(h1)

        # Blended EW — auto-EW (Lux-Algo-inspired pivot-hierarchy detector)
        # outranks the structural-break direction when it returns a high-
        # confidence completed/in-progress pattern. Same priority as the
        # dashboard's refreshAutoEW gate, so directions.json matches what
        # the dashboard shows and the Telegram alert won't fire on a 4/4
        # the user can't see on the dashboard.
        ew = ew_structural
        auto_ew_used = False
        ew_pattern = None       # which auto-EW pattern carried the dir
        ew_confidence = None    # the auto-EW conf at this scan, if used
        try:
            auto = auto_detect_ew(daily)
            ewp = auto.get('ew') if auto.get('ok') else None
            if ewp and ewp.get('dir') in ('bull', 'bear') \
                    and ewp.get('confidence', 0) >= AUTO_EW_MIN_CONFIDENCE \
                    and ewp.get('pattern') in AUTO_EW_VALID_PATTERNS:
                ew = ewp['dir']
                auto_ew_used = True
                ew_pattern = ewp.get('pattern')
                ew_confidence = ewp.get('confidence')
        except Exception:
            pass  # defensive — never let auto-EW break the scan

        # ew_source mirrors the dashboard's ewSource so directions.json
        # consumers (Telegram messages, the upcoming /track-record page,
        # any external tooling) can show the same "EW carrier" badge the
        # dashboard does. Priority: auto-EW > structural (Python detector
        # has no WICKATOR_EW seeds — those are browser-side only).
        ew_source = 'auto-EW' if auto_ew_used else 'structural'

        macro_setup = calc_macro_auto_setup(daily, ew)

        # v6 — 4/4 confluence gate: macro (EW), hourly (TL), 15m (NW)
        # and the 4H EMA21/55 cloud (CL) must all agree before a setup
        # is "aligned". The cloud was added to filter against trades that
        # are structurally aligned but counter to the 4H trend.
        aligned = (
            ew is not None and ew in ('bull', 'bear')
            and ew == tl == nw == cl
        )
        aligned_dir = ew if aligned else None

        last_price = None
        last_bar = m15[-1] if m15 else None
        if last_bar:
            last_price = last_bar.get('c') or last_bar.get('p')

        # Phase 2: if 4/4 aligned, detect intraday signal state (armed /
        # triggered / expired). Ports the essential part of
        # detectIntradaySignal so we can alert when a setup triggers.
        sig = None
        if aligned_dir is not None:
            # Compute the most recent 1H RSI(14) so detect_intraday_signal
            # can apply the A1 hard gate. None during warm-up (<15
            # h1 closes) — detector falls through. Per-class threshold
            # (RULES_VERSION 2026-06-10a) resolved here so the gate
            # config lives next to PAIR_CLASS rather than inside the
            # signal walker.
            h1_closes = [b.get('c') for b in h1 if b.get('c') is not None]
            h1_rsi = calc_rsi(h1_closes, 14) if len(h1_closes) >= 15 else None
            gate = rsi_gate_for(pair)
            sig = detect_intraday_signal(m15, aligned_dir, h1_rsi=h1_rsi,
                                         rsi_hi=gate['hi'], rsi_lo=gate['lo'],
                                         pair_class=PAIR_CLASS.get(pair))

        # School Run tier — only computed for DE40 and DJ30 (the only
        # pairs in SR_REF_TIMES); for other pairs sr_info stays None
        # and the dashboard won't render the pill. Confluence score
        # mirrors sc(k) in the JS: max count of bull-or-bear across
        # the four layers. Engine direction = whichever wins.
        sr_info = None
        if pair in SR_REF_TIMES:
            layers = [ew, tl, nw, cl]
            bear_n = sum(1 for v in layers if v == 'bear')
            bull_n = sum(1 for v in layers if v == 'bull')
            score = max(bear_n, bull_n)
            engine_dir = 'bear' if bear_n > bull_n else 'bull' if bull_n > bear_n else None
            try:
                sr_info = get_sr_tier(pair, m15, engine_dir, score)
            except Exception:
                sr_info = None

        out[pair] = {
            'ew': ew,
            'tl': tl,
            'nw': nw,
            'cl': cl,
            'aligned_dir': aligned_dir,
            'price': last_price,
            'macro': macro_setup,
            'sig_state': sig.get('state') if sig else None,
            'sig_creator_ts': sig.get('creator_ts') if sig else None,
            'sig_entry': sig.get('entry') if sig else None,
            'sig_stop': sig.get('stop') if sig else None,
            'sig_target': sig.get('target') if sig else None,
            'sig_trigger_ts': sig.get('trigger_ts') if sig else None,
            'sig_fib_state': sig.get('fib_state') if sig else None,
            'sig_fib_entry': sig.get('fib_entry') if sig else None,
            'sig_fib_target': sig.get('fib_target') if sig else None,
            'sig_fib_trigger_ts': sig.get('fib_trigger_ts') if sig else None,
            'sig_fib_ext_target': sig.get('fib_ext_target') if sig else None,
            'sig_fib_ext_rr': sig.get('fib_ext_rr') if sig else None,
            # Post-trigger outcome metadata (2026-06-11ff) — main loop
            # uses this to fire EXIT alerts when a previously-alerted
            # trigger gets invalidated by counter-bars / opposing CHoCH.
            'sig_outcome': sig.get('outcome') if sig else None,
            'sig_outcome_reason': sig.get('outcome_reason') if sig else None,
            'sig_outcome_ts': sig.get('outcome_ts') if sig else None,
            'sig_fib_outcome': sig.get('fib_outcome') if sig else None,
            'sig_fib_outcome_reason': sig.get('fib_outcome_reason') if sig else None,
            'sig_fib_outcome_ts': sig.get('fib_outcome_ts') if sig else None,
            # Invalidation metadata — passed through to directions.json
            # so the dashboard can render a tooltip on cancelled setups
            # and so alert-state debugging is possible without a code
            # change. None when the setup is still armed / triggered.
            'sig_invalidation_reason': sig.get('invalidation_reason') if sig else None,
            'sig_invalidation_ts': sig.get('invalidation_ts') if sig else None,
            # EW carrier — surfaces which mechanism supplied the macro
            # direction so the dashboard + Telegram message can show
            # the same "EW carrier" badge (per-pattern WR documented
            # in RULES_VERSION_NOTES 2026-06-10d).
            'ew_source': ew_source,
            'ew_pattern': ew_pattern,
            'ew_confidence': ew_confidence,
            # School Run tier (DE40 / DJ30 only — null elsewhere).
            # Schema matches getSRTier() in the dashboard so the
            # downstream Telegram alert tier label is consistent with
            # the SR pill the user sees on the card.
            'sr': {
                'tier': sr_info.get('tier'),
                'state': sr_info.get('state'),
                'ref_high': sr_info.get('ref_high'),
                'ref_low': sr_info.get('ref_low'),
                'confluence': sr_info.get('confluence'),
                'engine_dir': sr_info.get('engine_dir'),
                'session_label': sr_info.get('session_label'),
                'bars_since_ref': sr_info.get('bars_since_ref'),
            } if sr_info else None,
        }
    return out


# ── Telegram ──

def send_telegram(token, chat_id, text):
    if not token or not chat_id:
        print(f'[skip] no Telegram credentials configured. Would have sent:\n{text}')
        return False
    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        # Use HTML parse mode for bold/italic
        resp = requests.post(url, data={
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': 'true',
        }, timeout=10)
        if resp.status_code == 200:
            print(f'[ok] Telegram message sent (chat_id={chat_id})')
            return True
        else:
            print(f'[err] Telegram API returned {resp.status_code}: {resp.text[:200]}')
            return False
    except requests.RequestException as e:
        print(f'[err] Telegram request failed: {e}')
        return False


def _fmt_price(p):
    if p is None:
        return '?'
    ap = abs(p)
    if ap < 100:
        return f"{p:.5f}"
    if ap < 1000:
        return f"{p:.3f}"
    return f"{p:,.0f}"


def format_alert(pair, info, kind):
    """kind: 'newly-aligned' | 'flipped' | 'currently-aligned' | 'triggered' | 'triggered-fib'
            | 'sr_5_5' | 'sr_4_5' | 'sr_3_5'.

    SR tier alerts (sr_5_5, sr_4_5, sr_3_5) fire for DE40 / DJ30 only
    when the School Run tier becomes aligned during the 2h post-open
    window. See SR_REF_TIMES + get_sr_tier.
    """
    sym = PAIR_DISPLAY.get(pair, pair.upper())
    layers = f"EW {info.get('ew')} · TL {info.get('tl')} · NW {info.get('nw')} · CL {info.get('cl') or '?'}"

    # School Run tier alerts — surface the new SR cohort to subscribers
    # who opted in via the profile modal's sr_* checkboxes. Tier-specific
    # styling so the user can read priority from the emoji alone.
    if kind in ('sr_5_5', 'sr_4_5', 'sr_3_5'):
        sr = info.get('sr') or {}
        state = sr.get('state', '')
        engine_dir = sr.get('engine_dir')
        action = 'BUY' if engine_dir == 'bull' else 'SELL' if engine_dir == 'bear' else '—'
        ref_hi = _fmt_price(sr.get('ref_high'))
        ref_lo = _fmt_price(sr.get('ref_low'))
        break_dir = ('above ' + ref_hi) if state == 'bull_broken' else ('below ' + ref_lo) if state == 'bear_broken' else state
        spec = {
            'sr_5_5': ('⭐', '5/5 SR-CONFIRMED', 'full confluence + School Run break'),
            'sr_4_5': ('🟡', '4/5 SR partial',   '3/4 partial confluence + School Run break'),
            'sr_3_5': ('🟠', '3/5 SR speculative','2/4 weak confluence + School Run break — speculative'),
        }[kind]
        emoji, title, subtitle = spec
        text = (
            f"{emoji} <b>{title} — {action} {sym}</b>\n"
            f"{subtitle}\n"
            f"Session: {sr.get('session_label', 'SR window')}\n"
            f"SR break: {break_dir} ({sr.get('bars_since_ref', '?')} bars since ref)\n"
            f"Ref range: <code>{ref_lo}</code> – <code>{ref_hi}</code>\n"
            f"Current px: <code>{_fmt_price(info.get('price'))}</code>\n"
            f"{layers}\n"
            f"<a href=\"https://modernviking1.github.io/vikinginvest-prices/dashboard.html\">Open dashboard</a>"
        )
        return text

    direction = info['aligned_dir']
    action = 'BUY' if direction == 'bull' else 'SELL'
    layers = f"EW {info['ew']} · TL {info['tl']} · NW {info['nw']} · CL {info.get('cl') or '?'}"

    # Helper: format the 1.618 Fib extension line shown under the 1:1
    # target. The R:R is computed from the alert's entry — wick for the
    # full-size alert, fib-zone for the half-size alert — so the number
    # reflects the actual reward per 1R risked for *this* alert kind.
    def _fib_ext_line(entry_val):
        ext_target = info.get('sig_fib_ext_target')
        if ext_target is None or entry_val is None:
            return ''
        stop_val = info.get('sig_stop')
        if stop_val is None or stop_val == entry_val:
            return ''
        R_local = abs(stop_val - entry_val)
        reward_local = abs(ext_target - entry_val)
        rr_local = reward_local / R_local if R_local > 0 else None
        rr_str = f"{rr_local:.2f}:1 R:R" if rr_local is not None else ''
        return (
            f"Fib 1.618: <code>{_fmt_price(ext_target)}</code>"
            f"{' (' + rr_str + ')' if rr_str else ''}\n"
        )

    if kind == 'triggered':
        # Wick retest — full size, deeper retrace (current behaviour).
        entry_val = info.get('sig_entry')
        entry_str  = _fmt_price(entry_val)
        stop_str   = _fmt_price(info.get('sig_stop'))
        target_str = _fmt_price(info.get('sig_target'))
        text = (
            f"🎯 <b>TRIGGERED — {action} {sym}</b> (full size)\n"
            f"Entry:  <code>{entry_str}</code> (creator wick)\n"
            f"Stop:   <code>{stop_str}</code>\n"
            f"Target: <code>{target_str}</code> (1:1 R:R)\n"
            f"{_fib_ext_line(entry_val)}"
            f"Current px: <code>{_fmt_price(info['price'])}</code>\n"
            f"{layers}\n"
            f"<a href=\"https://modernviking1.github.io/vikinginvest-prices/dashboard.html\">Open dashboard</a>"
        )
        return text

    if kind == 'triggered-fib':
        # Fib-zone entry — 38% retrace of the creator candle, HALF size.
        # Stop matches wick (structural BoS); target is 1:1 from fib entry.
        entry_val = info.get('sig_fib_entry')
        entry_str  = _fmt_price(entry_val)
        stop_str   = _fmt_price(info.get('sig_stop'))   # same stop as wick
        target_str = _fmt_price(info.get('sig_fib_target'))
        text = (
            f"🎯 <b>FIB-ZONE — {action} {sym}</b> (half size)\n"
            f"Entry:  <code>{entry_str}</code> (38% Fib of creator candle)\n"
            f"Stop:   <code>{stop_str}</code>\n"
            f"Target: <code>{target_str}</code> (1:1 R:R from fib entry)\n"
            f"{_fib_ext_line(entry_val)}"
            f"Current px: <code>{_fmt_price(info['price'])}</code>\n"
            f"{layers}\n"
            f"<a href=\"https://modernviking1.github.io/vikinginvest-prices/dashboard.html\">Open dashboard</a>"
        )
        return text

    if kind in ('invalidated', 'invalidated-fib'):
        # Post-trigger exit alert (2026-06-11ff). Triggered trade has
        # hit a structural invalidation (opposing CHoCH or close-beyond
        # counter-bars). Loss isn't realised yet — stop hasn't been
        # touched — so the user has a window to exit at current price
        # and limit the damage before the stop fills.
        is_fib = (kind == 'invalidated-fib')
        if is_fib:
            entry_val = info.get('sig_fib_entry')
            target_val = info.get('sig_fib_target')
            reason = info.get('sig_fib_outcome_reason') or 'structure-flip'
            kind_lbl = 'FIB-ZONE · half size'
        else:
            entry_val = info.get('sig_entry')
            target_val = info.get('sig_target')
            reason = info.get('sig_outcome_reason') or 'structure-flip'
            kind_lbl = 'WICK · full size'
        stop_val = info.get('sig_stop')
        # Translate reason into a trader-readable phrase
        reason_lbl = {
            'counter-bars': '2 counter-bars closed past entry (momentum reversal)',
            'opposing-choch': 'opposing CHoCH printed (structure flipped)',
        }.get(reason, reason or 'structure flip')
        # Optional: rough R loss at current price (positive = trade went
        # against us). Helps the user judge whether to exit or hold.
        loss_R_line = ''
        try:
            entry_f = float(entry_val) if entry_val is not None else None
            stop_f  = float(stop_val) if stop_val is not None else None
            curr_f  = float(info['price']) if info.get('price') is not None else None
            if entry_f is not None and stop_f is not None and curr_f is not None and stop_f != entry_f:
                R_size = abs(stop_f - entry_f)
                if direction == 'bull':
                    loss_R = (entry_f - curr_f) / R_size
                else:
                    loss_R = (curr_f - entry_f) / R_size
                loss_R_line = f"Current loss: <code>{loss_R:+.2f}R</code> (stop = -1.0R)\n"
        except Exception:
            loss_R_line = ''
        text = (
            f"⊘ <b>INVALIDATED — EXIT {sym}</b> ({kind_lbl})\n"
            f"Original entry: <code>{_fmt_price(entry_val)}</code>\n"
            f"Original stop:  <code>{_fmt_price(stop_val)}</code>\n"
            f"Original target: <code>{_fmt_price(target_val)}</code>\n"
            f"Current px: <code>{_fmt_price(info['price'])}</code>\n"
            f"{loss_R_line}"
            f"Reason: {reason_lbl}\n"
            f"Suggested: exit at market to limit loss before stop fills.\n"
            f"<a href=\"https://modernviking1.github.io/vikinginvest-prices/dashboard.html\">Open dashboard</a>"
        )
        return text

    arrow = '🟢▲' if direction == 'bull' else '🔴▼'
    if kind == 'newly-aligned':
        title = '4/4 ALIGNED'
    elif kind == 'flipped':
        title = '4/4 FLIPPED'
    else:
        title = '4/4 STATUS (catchup)'

    text = (
        f"{arrow} <b>{title} — {action} {sym}</b>\n"
        f"Price: <code>{_fmt_price(info['price'])}</code>\n"
        f"{layers}\n"
        f"<a href=\"https://modernviking1.github.io/vikinginvest-prices/dashboard.html\">Open dashboard</a>"
    )
    return text


# ── Main ──

def write_directions_json(current, path='directions.json'):
    """Write a tiny per-pair EW/TL/NW direction file for the dashboard.

    The dashboard's own direction calcs depend on loading the ~90 MB
    historical-ohlc.json into the browser — which exceeds localStorage
    quota, gets re-downloaded every session, and loads unreliably on
    mobile. So newly-added pairs intermittently show no Macro/Hourly
    direction.

    This sidesteps all of that: the GitHub Actions runner computes the
    directions from the repo files directly (where the data is always
    available) and ships them as a ~3 KB JSON the dashboard reads with
    zero size/quota concerns. The dashboard's live engine calcs still
    run and take priority — directions.json is the reliable baseline,
    especially for pairs whose deep history didn't load in-browser.
    """
    out = {}
    for pair, info in current.items():
        out[pair] = {
            'ew': info.get('ew'),
            'tl': info.get('tl'),
            'nw': info.get('nw'),
            'cl': info.get('cl'),
            'aligned_dir': info.get('aligned_dir'),
            'macro': info.get('macro'),
            # 1.618 Fib extension — same value that ships in the
            # Telegram alert below the 1:1 R:R target line. Dashboard
            # reads this back via fetchServerDirections so the per-pair
            # detail panel can render an identical stretch target +
            # R:R without recomputing in-browser. Both fields are
            # None when the lookback geometry doesn't validate (e.g.
            # bull setup with swing low after swing high) — the
            # dashboard hides the line in that case.
            'fib_ext_target': info.get('sig_fib_ext_target'),
            'fib_ext_rr': info.get('sig_fib_ext_rr'),
            # Wick + fib entry / stop / target — primarily here so the
            # dashboard can reconstruct the Telegram alert content
            # locally (currently used only by the 1.618 render path
            # but cheap to expose, ~6 floats per pair).
            'sig_entry': info.get('sig_entry'),
            'sig_stop': info.get('sig_stop'),
            'sig_target': info.get('sig_target'),
            'sig_fib_entry': info.get('sig_fib_entry'),
            'sig_fib_target': info.get('sig_fib_target'),
        }
    try:
        with open(path, 'w') as f:
            json.dump({
                'updated': datetime.now(timezone.utc).isoformat(),
                'pairs': out,
            }, f, indent=1, sort_keys=True)
        print(f'Wrote {path} — {len(out)} pairs')
    except OSError as e:
        print(f'WARN: could not write {path}: {e}')


def main():
    intraday = load_json('intraday-ohlc.json')
    historical = load_json('historical-ohlc.json')

    if not intraday:
        print('FATAL: intraday-ohlc.json missing or empty')
        sys.exit(1)
    if not historical:
        print('FATAL: historical-ohlc.json missing or empty')
        sys.exit(1)

    current = scan_pairs(intraday, historical)

    # Always publish the slim directions file — independent of whether
    # any Telegram alert fires this run.
    write_directions_json(current)
    print(f'Scanned {len(current)} pairs')

    prev_state, sr_state, is_first_run = load_alerts_state('alerts-state.json')
    # Prune SR alert counters older than 7 days so the state file stays
    # small. The per-tier daily limits only care about today; older
    # entries are dead weight.
    today_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    sr_state = {d: v for d, v in sr_state.items() if d >= today_iso}
    if is_first_run:
        print('First run — establishing baseline, no alerts sent')

    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    # Manual one-shot flag from workflow_dispatch — set to "true" to
    # also alert on every currently-aligned pair regardless of stored
    # state. Used to verify the Telegram path end-to-end after first
    # setup, or to catch up on alignments the baseline silenced.
    send_all_aligned = os.environ.get('SEND_ALL_ALIGNED', '').strip().lower() in ('1', 'true', 'yes')
    if send_all_aligned:
        print('SEND_ALL_ALIGNED=true — will alert on every currently-aligned pair')

    alerts_sent = 0
    new_state = {}
    now_iso = datetime.now(timezone.utc).isoformat()

    for pair, info in current.items():
        prev = prev_state.get(pair, {})
        prev_dir = prev.get('aligned_dir')
        cur_dir = info['aligned_dir']
        cur_sig_state = info.get('sig_state')

        # Alignment-level alerts — suppressed in normal scheduled runs.
        # Per user feedback: alignment alerts arrived premature (≥10 min
        # after the actual 4/4 click-over, with the setup often already
        # triggered or expired by the time the user clicked through). The
        # trigger alert below is the actionable signal and fires on the
        # genuine entry. Alignment alerts now only fire when SEND_ALL_ALIGNED
        # is explicitly set (manual one-shot catch-up via workflow_dispatch).
        should_alert_alignment = False
        align_kind = None
        if cur_dir is not None and send_all_aligned:
            if prev_dir is None:
                should_alert_alignment = True
                align_kind = 'newly-aligned'
            elif prev_dir != cur_dir:
                should_alert_alignment = True
                align_kind = 'flipped'
            else:
                should_alert_alignment = True
                align_kind = 'currently-aligned'

        if should_alert_alignment:
            print(f'  ALERT(align,catchup): {pair} {prev_dir} -> {cur_dir} ({align_kind})')
            text = format_alert(pair, info, align_kind)
            if send_telegram(token, chat_id, text):
                alerts_sent += 1

        # Phase 2: intraday-trigger alerts. Fires once when a pair's 15m
        # 1:1-RR signal reaches the 'triggered' state — price retested the
        # creator CHoCH wick.
        #
        # Dedup is keyed on the creator-bar timestamp, NOT on observing the
        # intermediate 'armed' state. The detector runs only every ~5 min,
        # so a 15m creator routinely forms AND gets retested inside a single
        # cycle (or a pair becomes 4/4-aligned only after the retest has
        # already happened). In those cases 'armed' is never seen, and the
        # old `prev_sig_state == 'armed'` gate silently dropped the alert.
        # Keying on creator_ts means every distinct triggered setup alerts
        # exactly once, regardless of whether 'armed' was observed.
        cur_creator_ts = info.get('sig_creator_ts')
        prev_alerted_creator = prev.get('alerted_trigger_creator_ts')
        # Carry forward the post-trigger exit dedup keys so they persist
        # even on cycles where the exit branch doesn't run. Reassigned
        # below if an exit alert fires this cycle.
        alerted_exit = prev.get('alerted_exit_creator_ts')
        alerted_fib_exit = prev.get('alerted_fib_exit_creator_ts')
        # Migration: state files written before alerted_trigger_creator_ts
        # existed have no dedup key. If the pair was already 'triggered' on
        # the same creator last run, treat that creator as already-alerted
        # so deploying this change doesn't replay pre-existing triggers.
        if prev_alerted_creator is None and prev.get('sig_state') == 'triggered':
            prev_alerted_creator = prev.get('sig_creator_ts')
        alerted_creator = prev_alerted_creator

        # Per-class entry methodology gate: wick alerts only on
        # FX/crypto pairs. For commodities/indices (FIB_ENTRY_PAIRS)
        # the wick path is silenced — those pairs send only the Fib
        # alert below.
        pair_uses_fib = uses_fib_entry(pair)

        if cur_sig_state == 'triggered' and cur_creator_ts is not None and not pair_uses_fib:
            is_new_trigger = (cur_creator_ts != prev_alerted_creator)
            # Freshness gate: don't alert on triggers older than
            # MAX_TRIGGER_AGE_MIN. The 5-min schedule means a fresh trigger
            # is normally ≤5 min old; >30 min means something delayed the
            # detection (data hiccup, workflow failure) and price has likely
            # already moved past entry — alerting would be misleading.
            trigger_age = _trigger_age_minutes(info.get('sig_trigger_ts'))
            is_stale = trigger_age is not None and trigger_age > MAX_TRIGGER_AGE_MIN
            if send_all_aligned:
                print(f'  ALERT(trigger,catchup): {pair} triggered (creator={cur_creator_ts}, entry={info.get("sig_entry")})')
                text = format_alert(pair, info, 'triggered')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                alerted_creator = cur_creator_ts
            elif is_new_trigger and is_first_run:
                # Baseline run — record the trigger as already-seen so we
                # don't alert on this pre-existing setup next cycle.
                print(f'  baseline(trigger): {pair} already triggered (creator={cur_creator_ts}) — recorded, not alerting')
                alerted_creator = cur_creator_ts
            elif is_new_trigger and is_stale:
                # Stale trigger — record as alerted (dedup) so we don't keep
                # checking, but skip the Telegram send.
                print(f'  skip(trigger,stale): {pair} creator={cur_creator_ts} trigger_ts={info.get("sig_trigger_ts")} age={trigger_age:.1f}min > {MAX_TRIGGER_AGE_MIN} — not alerting')
                alerted_creator = cur_creator_ts
            elif is_new_trigger:
                age_tag = f' (fresh, age={trigger_age:.1f}min)' if trigger_age is not None else ''
                print(f'  ALERT(trigger): {pair} -> triggered (creator={cur_creator_ts}, entry={info.get("sig_entry")}){age_tag}')
                text = format_alert(pair, info, 'triggered')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                alerted_creator = cur_creator_ts

        # ── FIB-ZONE TRIGGER ALERTS (half size) ─────────────────────
        # Mirror of the wick path above, with an independent dedup key.
        # Captures setups that triggered at the 38% Fib retrace without
        # reaching the deeper wick — the "expired-no-retest" cohort the
        # failure-mode analysis flagged at ~56%. Half size by convention.
        cur_fib_state = info.get('sig_fib_state')
        prev_alerted_fib = prev.get('alerted_fib_creator_ts')
        if prev_alerted_fib is None and prev.get('sig_fib_state') == 'triggered':
            prev_alerted_fib = prev.get('sig_creator_ts')
        alerted_fib = prev_alerted_fib

        # Per-class entry methodology gate: Fib alerts only on
        # commodities/indices. For FX/crypto pairs the Fib path is
        # silenced — those pairs send only the wick alert above.
        if cur_fib_state == 'triggered' and cur_creator_ts is not None and pair_uses_fib:
            is_new_fib = (cur_creator_ts != prev_alerted_fib)
            # If the wick trade also triggered on the same setup, the wick
            # alert above already covered the entry — suppress the fib
            # alert to avoid double-notifying the user. The fib state stays
            # recorded so we still know it fired in the state file.
            wick_also_fired = (cur_sig_state == 'triggered' and cur_creator_ts == alerted_creator)
            fib_age = _trigger_age_minutes(info.get('sig_fib_trigger_ts'))
            fib_stale = fib_age is not None and fib_age > MAX_TRIGGER_AGE_MIN
            if send_all_aligned and is_new_fib and not wick_also_fired:
                print(f'  ALERT(fib,catchup): {pair} fib-triggered (creator={cur_creator_ts}, entry={info.get("sig_fib_entry")})')
                text = format_alert(pair, info, 'triggered-fib')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                alerted_fib = cur_creator_ts
            elif is_new_fib and is_first_run:
                print(f'  baseline(fib): {pair} already fib-triggered (creator={cur_creator_ts}) — recorded, not alerting')
                alerted_fib = cur_creator_ts
            elif is_new_fib and fib_stale:
                print(f'  skip(fib,stale): {pair} creator={cur_creator_ts} age={fib_age:.1f}min — not alerting')
                alerted_fib = cur_creator_ts
            elif is_new_fib and wick_also_fired:
                # Quietly mark as alerted (wick alert already covered it).
                print(f'  skip(fib,wick-also): {pair} creator={cur_creator_ts} wick alert sent — fib suppressed')
                alerted_fib = cur_creator_ts
            elif is_new_fib:
                fib_age_tag = f' (fresh, age={fib_age:.1f}min)' if fib_age is not None else ''
                print(f'  ALERT(fib): {pair} -> fib-triggered (creator={cur_creator_ts}, entry={info.get("sig_fib_entry")}){fib_age_tag}')
                text = format_alert(pair, info, 'triggered-fib')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                alerted_fib = cur_creator_ts

        # ── POST-TRIGGER EXIT ALERTS (2026-06-11ff) ─────────────────
        # Fire an EXIT alert when a trade we previously alerted on has
        # been invalidated post-trigger by counter-bars / opposing CHoCH
        # (NOT target-hit / stop-hit / stale-expired — those are
        # outcomes the user already sees at the broker). One alert per
        # alerted trigger so we don't spam every cycle while the
        # outcome persists.
        alerted_exit = prev.get('alerted_exit_creator_ts')
        cur_outcome = info.get('sig_outcome')
        if (alerted_creator == cur_creator_ts
            and cur_creator_ts is not None
            and cur_outcome == 'choch-invalidated'
            and alerted_exit != cur_creator_ts
            and not is_first_run):
            # Freshness gate — if the invalidation bar is stale (the
            # workflow lagged or the trade was hours back), still send
            # but flag it. We pick a generous 60 min window; older than
            # that and the loss is likely already past stop.
            exit_age = _trigger_age_minutes(info.get('sig_outcome_ts'))
            if exit_age is not None and exit_age > 60:
                print(f'  skip(exit,stale): {pair} outcome_ts={info.get("sig_outcome_ts")} age={exit_age:.1f}min > 60 — not alerting')
                alerted_exit = cur_creator_ts
            else:
                age_tag = f' (fresh, age={exit_age:.1f}min)' if exit_age is not None else ''
                print(f'  ALERT(exit): {pair} -> invalidated (creator={cur_creator_ts}, reason={info.get("sig_outcome_reason")}){age_tag}')
                text = format_alert(pair, info, 'invalidated')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                alerted_exit = cur_creator_ts

        # Mirror for fib variant — independent dedup key
        alerted_fib_exit = prev.get('alerted_fib_exit_creator_ts')
        cur_fib_outcome = info.get('sig_fib_outcome')
        if (alerted_fib == cur_creator_ts
            and cur_creator_ts is not None
            and cur_fib_outcome == 'choch-invalidated'
            and alerted_fib_exit != cur_creator_ts
            and not is_first_run):
            fib_exit_age = _trigger_age_minutes(info.get('sig_fib_outcome_ts'))
            if fib_exit_age is not None and fib_exit_age > 60:
                print(f'  skip(fib-exit,stale): {pair} age={fib_exit_age:.1f}min > 60 — not alerting')
                alerted_fib_exit = cur_creator_ts
            else:
                age_tag = f' (fresh, age={fib_exit_age:.1f}min)' if fib_exit_age is not None else ''
                print(f'  ALERT(fib-exit): {pair} -> fib invalidated (creator={cur_creator_ts}, reason={info.get("sig_fib_outcome_reason")}){age_tag}')
                text = format_alert(pair, info, 'invalidated-fib')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                alerted_fib_exit = cur_creator_ts

        # ── SCHOOL RUN TIER ALERTS (DE40 / DJ30 only) ───────────────
        # Tier-classified alerts surfacing 5/5, 4/5, and 3/5 cohorts
        # (RULES_VERSION 2026-06-10l). Engine gating is unchanged —
        # the regular 4/4 trigger/fib alerts above STILL fire on
        # 4/4 + SR aligned setups; SR tier alerts are layered on top
        # with tier-specific styling and per-tier daily rate limits.
        # Dedup keys are date-scoped so the limits reset each UTC day.
        sr_info = info.get('sr') if isinstance(info, dict) else None
        if sr_info and sr_info.get('tier') in SR_ALERT_LIMITS:
            tier = sr_info['tier']
            today = now_iso[:10]  # YYYY-MM-DD UTC
            # Per-tier dedup state: {today: {pair: {tier: count}}}
            today_counts = sr_state.get(today, {}).get(pair, {}).get(tier, 0)
            limit = SR_ALERT_LIMITS[tier]
            kind = SR_TIER_TO_KIND[tier]
            # Sample-size honesty: baseline run records existing tier
            # state without alerting, mirroring the 4/4 baseline pattern
            # above, so deploying SR alerts doesn't replay yesterday's
            # window on every subscriber's first cycle.
            if is_first_run:
                print(f'  baseline(sr): {pair} tier={tier} state={sr_info.get("state")} — recorded, not alerting')
                sr_state.setdefault(today, {}).setdefault(pair, {})[tier] = today_counts
            elif today_counts >= limit:
                # Quiet skip — daily ceiling reached.
                pass
            else:
                action = 'BUY' if sr_info.get('engine_dir') == 'bull' else 'SELL'
                print(f'  ALERT(sr,{tier}): {pair} {action} state={sr_info.get("state")} '
                      f'confluence={sr_info.get("confluence")}/4 '
                      f'(today_counts={today_counts}/{limit})')
                text = format_alert(pair, info, kind)
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1
                sr_state.setdefault(today, {}).setdefault(pair, {})[tier] = today_counts + 1

        new_state[pair] = {
            'aligned_dir': cur_dir,
            'ew': info['ew'],
            'tl': info['tl'],
            'nw': info['nw'],
            'cl': info.get('cl'),
            'price': info['price'],
            'sig_state': cur_sig_state,
            'sig_creator_ts': cur_creator_ts,
            'sig_entry': info.get('sig_entry'),
            'sig_trigger_ts': info.get('sig_trigger_ts'),
            'sig_fib_state': cur_fib_state,
            'sig_fib_entry': info.get('sig_fib_entry'),
            'sig_fib_trigger_ts': info.get('sig_fib_trigger_ts'),
            'alerted_trigger_creator_ts': alerted_creator,
            'alerted_fib_creator_ts': alerted_fib,
            # Post-trigger exit-alert dedup (2026-06-11ff). Independent
            # of trigger dedup so a single creator can have both a
            # trigger alert and an exit alert recorded.
            'alerted_exit_creator_ts': alerted_exit,
            'alerted_fib_exit_creator_ts': alerted_fib_exit,
            'last_check': now_iso,
        }

    # Preserve entries for pairs no longer in the scan (e.g. data hiccup)
    # so we don't accidentally re-alert if they reappear.
    for pair, prev in prev_state.items():
        if pair not in new_state:
            new_state[pair] = prev

    # Save updated state
    with open('alerts-state.json', 'w') as f:
        json.dump({
            'updated': now_iso,
            'pairs': new_state,
            'sr_alerts': sr_state,
        }, f, indent=2, sort_keys=True)

    print(f'\nSummary: {alerts_sent} alert(s) sent, {len(new_state)} pairs tracked')


if __name__ == '__main__':
    main()
