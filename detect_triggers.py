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
    'xauusd': 'XAU/USD', 'xagusd': 'XAG/USD', 'usoil': 'BRENT',
    'de40':   'DAX 30',  'btcusd': 'BTC/USD', 'suiusd': 'SUI/USD',
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

        out[pair] = {
            'ew': ew,
            'tl': tl,
            'nw': nw,
            'aligned_dir': aligned_dir,
            'price': last_price,
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


def format_alert(pair, info, kind):
    """kind: 'newly-aligned' or 'flipped'."""
    sym = PAIR_DISPLAY.get(pair, pair.upper())
    direction = info['aligned_dir']
    arrow = '🟢▲' if direction == 'bull' else '🔴▼'
    action = 'BUY' if direction == 'bull' else 'SELL'
    title = '3/3 ALIGNED' if kind == 'newly-aligned' else '3/3 FLIPPED'

    price_str = f"{info['price']:.5f}" if info['price'] and abs(info['price']) < 100 else \
                f"{info['price']:.3f}" if info['price'] and abs(info['price']) < 1000 else \
                f"{info['price']:,.0f}" if info['price'] else '?'

    text = (
        f"{arrow} <b>{title} — {action} {sym}</b>\n"
        f"Price: <code>{price_str}</code>\n"
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

    alerts_sent = 0
    new_state = {}
    now_iso = datetime.now(timezone.utc).isoformat()

    for pair, info in current.items():
        prev = prev_state.get(pair, {})
        prev_dir = prev.get('aligned_dir')
        cur_dir = info['aligned_dir']

        # Decide whether to alert
        should_alert = False
        kind = None
        if cur_dir is not None:
            if prev_dir is None:
                should_alert = True
                kind = 'newly-aligned'
            elif prev_dir != cur_dir:
                should_alert = True
                kind = 'flipped'

        if should_alert and not is_first_run:
            print(f'  ALERT: {pair} {prev_dir} -> {cur_dir} ({kind})')
            text = format_alert(pair, info, kind)
            if send_telegram(token, chat_id, text):
                alerts_sent += 1

        new_state[pair] = {
            'aligned_dir': cur_dir,
            'ew': info['ew'],
            'tl': info['tl'],
            'nw': info['nw'],
            'price': info['price'],
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
