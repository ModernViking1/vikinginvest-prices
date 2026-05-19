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

    # Trigger: walk forward from creator+1, check if any bar's range reaches entry.
    trigger_bar_idx = -1
    if entry is not None:
        for j in range(creator_idx + 1, n):
            b = bars[j]
            bh, bl = b.get('h'), b.get('l')
            if bh is None or bl is None:
                continue
            reaches = (
                (aligned_dir == 'bear' and bh >= entry) or
                (aligned_dir == 'bull' and bl <= entry)
            )
            if reaches:
                trigger_bar_idx = j
                break

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

        h1 = aggregate_m15_to_h1(m15)
        daily = historical_pairs.get(pair, {}).get('daily', [])

        # Need at least minimal history on each timeframe
        if len(daily) < 30 or len(h1) < 12 or len(m15) < 16:
            continue

        nw = calc_independent_dir(m15, lookback=8)
        tl = calc_independent_dir(h1, lookback=8)
        ew = calc_independent_dir(daily, lookback=8)

        aligned = (
            ew is not None and ew in ('bull', 'bear')
            and ew == tl == nw
        )
        aligned_dir = ew if aligned else None

        last_price = None
        last_bar = m15[-1] if m15 else None
        if last_bar:
            last_price = last_bar.get('c') or last_bar.get('p')

        # Phase 2: if 3/3 aligned, detect intraday signal state (armed /
        # triggered / expired). Ports the essential part of
        # detectIntradaySignal so we can alert when a setup triggers.
        sig = None
        if aligned_dir is not None:
            sig = detect_intraday_signal(m15, aligned_dir)

        out[pair] = {
            'ew': ew,
            'tl': tl,
            'nw': nw,
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
        prev_sig_state = prev.get('sig_state')
        cur_sig_state = info.get('sig_state')

        # Alignment-level alerts (existing behaviour).
        should_alert_alignment = False
        align_kind = None
        if cur_dir is not None:
            if prev_dir is None:
                should_alert_alignment = True
                align_kind = 'newly-aligned'
            elif prev_dir != cur_dir:
                should_alert_alignment = True
                align_kind = 'flipped'
            elif send_all_aligned:
                should_alert_alignment = True
                align_kind = 'currently-aligned'

        if should_alert_alignment and (not is_first_run or send_all_aligned):
            print(f'  ALERT(align): {pair} {prev_dir} -> {cur_dir} ({align_kind})')
            text = format_alert(pair, info, align_kind)
            if send_telegram(token, chat_id, text):
                alerts_sent += 1

        # Phase 2: intraday-trigger alerts. Fires when a previously-armed
        # signal newly reaches the creator's wick entry. We deliberately
        # gate on prev_sig_state == 'armed' AND cur_sig_state == 'triggered'
        # so we only push once per setup — if the user reloads the workflow
        # or we re-run, we won't re-alert as long as the state file
        # remembers it was already triggered.
        if cur_sig_state == 'triggered' and prev_sig_state == 'armed':
            if not is_first_run or send_all_aligned:
                print(f'  ALERT(trigger): {pair} armed -> triggered (creator={info.get("sig_creator_ts")}, entry={info.get("sig_entry")})')
                text = format_alert(pair, info, 'triggered')
                if send_telegram(token, chat_id, text):
                    alerts_sent += 1

        new_state[pair] = {
            'aligned_dir': cur_dir,
            'ew': info['ew'],
            'tl': info['tl'],
            'nw': info['nw'],
            'price': info['price'],
            'sig_state': cur_sig_state,
            'sig_creator_ts': info.get('sig_creator_ts'),
            'sig_entry': info.get('sig_entry'),
            'sig_trigger_ts': info.get('sig_trigger_ts'),
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
