#!/usr/bin/env python3
"""
Detect 3/3-confluence transitions and send Telegram alerts.

Runs after every fetch-data workflow run. Computes EW (daily) / TL
(hourly) / NW (15m) direction for each pair using the same
calcIndependentDir structural-break logic the live dashboard uses,
detects pairs that just transitioned to 3/3 alignment, and sends a
Telegram message for each new alignment.

State is persisted in alerts-state.json so we don't re-alert on every
workflow run while a pair remains aligned. Transitions tracked:
  - prev=None, current='bull'/'bear'  -> ALERT (newly aligned)
  - prev='bull', current='bear'        -> ALERT (full flip)
  - prev='bull', current='bull'        -> no alert (still aligned)
  - prev='bull', current=None          -> no alert (lost alignment, not a setup)

Phase 2 — intraday trigger alerts: for every 3/3-aligned pair, the 15m
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
    'eurnzd': 'EUR/NZD', 'gbpaud': 'GBP/AUD', 'euraud': 'EUR/AUD',
    'audnzd': 'AUD/NZD', 'audchf': 'AUD/CHF', 'eurgbp': 'EUR/GBP',
    # v5 FX additions
    'audcad': 'AUD/CAD', 'gbpcad': 'GBP/CAD', 'nzdjpy': 'NZD/JPY',
    'usdnok': 'USD/NOK', 'gbpnzd': 'GBP/NZD', 'eursek': 'EUR/SEK',
    # Commodities / indices
    'xauusd': 'XAU/USD', 'xagusd': 'XAG/USD', 'usoil': 'BRENT',
    'de40':   'DAX 30',  'ftse100': 'FTSE 100',
    'dj30':   'DJ 30',   'nas100':  'NAS 100',
    # Crypto
    'btcusd': 'BTC/USD', 'suiusd': 'SUI/USD',
    'ethusd': 'ETH/USD', 'solusd': 'SOL/USD',
    'xrpusd': 'XRP/USD', 'taousd': 'TAO/USD',
    'nearusd': 'NEAR/USD', 'hypeusd': 'HYPE/USD', 'ondousd': 'ONDO/USD',
    'dxy':    'DXY',
}


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
        elif c < swing_lo and (swing_lo - c) >= min_prom:
            last_break_dir = 'bear'

    if last_break_dir is None:
        # Fall back to recent slope
        if n >= lookback:
            first_c = bars[-lookback].get('c')
            last_c = bars[-1].get('c')
            if first_c and last_c:
                change = (last_c - first_c) / first_c
                if change > 0.0005:
                    return 'bull'
                if change < -0.0005:
                    return 'bear'
        return 'neutral'
    return last_break_dir


def detect_intraday_signal(bars, aligned_dir, lookback=8, search_bars=16, expiry_bars=8):
    """Port of the essential parts of detectIntradaySignal from
    dashboard.html (~L9290). Detects the state of the 1:1-RR intraday
    signal for a 3/3-aligned pair on the 15m timeframe.

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

    # Trigger: walk forward from creator+1, check if any bar's range reaches
    # entry — gated on a round-trip lift (mirrors detectIntradaySignal in
    # dashboard.html). A retest only counts once price has first displaced
    # PAST the creator's far edge on a strictly earlier bar; without that
    # gate the next bar's ordinary low "retests" the entry with no real
    # pullback, producing false triggers on consecutive trend candles.
    trigger_bar_idx = -1
    if entry is not None:
        lift_reached = False
        for j in range(creator_idx + 1, n):
            b = bars[j]
            bh, bl = b.get('h'), b.get('l')
            if bh is None or bl is None:
                continue
            reaches = (
                (aligned_dir == 'bear' and bh >= entry) or
                (aligned_dir == 'bull' and bl <= entry)
            )
            if reaches and lift_reached:
                trigger_bar_idx = j
                break
            # Confirm the lift AFTER the retest check so the displacement
            # must precede the retest bar (a real round trip).
            if not lift_reached:
                if aligned_dir == 'bull' and creator_high is not None and bh >= creator_high:
                    lift_reached = True
                elif aligned_dir == 'bear' and creator_low is not None and bl <= creator_low:
                    lift_reached = True

    state = 'triggered' if trigger_bar_idx >= 0 else 'armed'
    return {
        'state': state,
        'creator_idx': creator_idx,
        'creator_ts': creator.get('t'),
        'creator_high': creator_high,
        'creator_low': creator_low,
        'entry': entry,
        'trigger_bar_idx': trigger_bar_idx,
        'trigger_ts': bars[trigger_bar_idx].get('t') if trigger_bar_idx >= 0 else None,
    }


# Trigger alerts older than this are suppressed (the trigger likely fired
# in an earlier cycle that the detector missed; alerting now would
# misrepresent a stale price level the user can no longer act on).
MAX_TRIGGER_AGE_MIN = 30


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
      'bull'    if EMA(fast) > EMA(slow)
      'bear'    if EMA(fast) < EMA(slow)
      'neutral' if equal / insufficient data

    Ported byte-identically to dashboard.html's calc4HCloudDir(k).
    """
    h4 = aggregate_h1_to_h4(h1_bars)
    closes = [b['c'] for b in h4 if b.get('c') is not None]
    if len(closes) < slow:
        return 'neutral'
    e_fast = ema(closes, fast)
    e_slow = ema(closes, slow)
    if e_fast is None or e_slow is None:
        return 'neutral'
    if e_fast > e_slow:
        return 'bull'
    if e_fast < e_slow:
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
    """Returns ({pair: {...}}, is_first_run). On first run, the file
    doesn't exist and we treat the run as baseline (no alerts sent)."""
    state = load_json(path)
    if state is None:
        return {}, True
    return state.get('pairs', {}), False


# ── Pair scanner ──

def scan_pairs(intraday_data, historical_data):
    """For each pair available in intraday-ohlc.json, compute current
    EW / TL / NW direction and whether 3/3 aligned. Returns dict:
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

        nw = calc_independent_dir(m15, lookback=8)
        tl = calc_independent_dir(h1, lookback=8)
        ew = calc_independent_dir(daily, lookback=8)
        cl = calc_4h_cloud_dir(h1)

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
            sig = detect_intraday_signal(m15, aligned_dir)

        out[pair] = {
            'ew': ew,
            'tl': tl,
            'nw': nw,
            'cl': cl,
            'aligned_dir': aligned_dir,
            'price': last_price,
            'sig_state': sig.get('state') if sig else None,
            'sig_creator_ts': sig.get('creator_ts') if sig else None,
            'sig_entry': sig.get('entry') if sig else None,
            'sig_trigger_ts': sig.get('trigger_ts') if sig else None,
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
    """kind: 'newly-aligned' | 'flipped' | 'currently-aligned' | 'triggered'."""
    sym = PAIR_DISPLAY.get(pair, pair.upper())
    direction = info['aligned_dir']
    action = 'BUY' if direction == 'bull' else 'SELL'

    if kind == 'triggered':
        # Phase 2 — intraday 1:1 retest fired. Higher-priority alert
        # with the entry price and a louder header.
        entry_str = _fmt_price(info.get('sig_entry'))
        text = (
            f"🎯 <b>TRIGGERED — {action} {sym}</b>\n"
            f"Entry hit: <code>{entry_str}</code> (creator wick)\n"
            f"Current px: <code>{_fmt_price(info['price'])}</code>\n"
            f"EW {info['ew']} · TL {info['tl']} · NW {info['nw']}\n"
            f"<a href=\"https://modernviking1.github.io/vikinginvest-prices/dashboard.html\">Open dashboard</a>"
        )
        return text

    arrow = '🟢▲' if direction == 'bull' else '🔴▼'
    if kind == 'newly-aligned':
        title = '3/3 ALIGNED'
    elif kind == 'flipped':
        title = '3/3 FLIPPED'
    else:
        title = '3/3 STATUS (catchup)'

    text = (
        f"{arrow} <b>{title} — {action} {sym}</b>\n"
        f"Price: <code>{_fmt_price(info['price'])}</code>\n"
        f"EW {info['ew']} · TL {info['tl']} · NW {info['nw']}\n"
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

    prev_state, is_first_run = load_alerts_state('alerts-state.json')
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
        # Migration: state files written before alerted_trigger_creator_ts
        # existed have no dedup key. If the pair was already 'triggered' on
        # the same creator last run, treat that creator as already-alerted
        # so deploying this change doesn't replay pre-existing triggers.
        if prev_alerted_creator is None and prev.get('sig_state') == 'triggered':
            prev_alerted_creator = prev.get('sig_creator_ts')
        alerted_creator = prev_alerted_creator

        if cur_sig_state == 'triggered' and cur_creator_ts is not None:
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
            'alerted_trigger_creator_ts': alerted_creator,
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
        }, f, indent=2, sort_keys=True)

    print(f'\nSummary: {alerts_sent} alert(s) sent, {len(new_state)} pairs tracked')


if __name__ == '__main__':
    main()
