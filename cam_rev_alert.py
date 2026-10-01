"""Feed-side cam_rev Telegram alerter — fires the moment a live cam_rev signal is published to
swing-signals.json, INDEPENDENT of the cBot. So it alerts even during a cBot outage (the gap that
let the 2026-09-30 EURNOK/NZDCHF fills go unseen). Dedupes on signal id via a committed state file
so each signal alerts once, not every 5-min feed cycle.

Scope: strategy == cam_rev, state == triggered, NOT demo_only (live FX / index / comm — the crypto
pilot is demo/observer and stays silent). Fail-open: any error prints and exits 0, never breaking
the feed. Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (same chat as the outage watchdog).

Wired into swing-signals-feed.yml after swing_signals.py; cam-alert-state.json is committed with the
feed so dedup persists across runs.
"""
import json
import os
import time
import urllib.parse
import urllib.request

_HERE = os.path.dirname(os.path.abspath(__file__))
SWING = os.path.join(_HERE, 'swing-signals.json')
STATE = os.path.join(_HERE, 'cam-alert-state.json')
PRUNE_SECS = 3 * 24 * 3600          # forget alerted ids older than 3 days (keeps the file small)


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _load_state():
    try:
        d = json.load(open(STATE))
        a = d.get('alerted') or {}
        return {k: float(v) for k, v in a.items()}
    except Exception:
        return {}


def _save_state(alerted):
    try:
        json.dump({'alerted': alerted, 'updated': int(time.time())},
                  open(STATE, 'w'), separators=(',', ':'), sort_keys=True)
    except Exception as e:
        print(f"[cam_rev_alert] state save failed: {e}")


def _fmt(r):
    sym = (r.get('symbol') or r.get('pair') or '').upper()
    bull = r.get('dir') == 'bull'
    side = '🟢 LONG' if bull else '🔴 SHORT'
    reg = r.get('regime')
    rtag = ' · strong-trend (R+)' if reg == 'strong' else (' · range (R-)' if reg == 'range' else '')
    entry, stop = _num(r.get('ref_entry')), _num(r.get('stop'))
    rr = _num(r.get('rr')) or 1.0
    tgt = None
    if entry is not None and stop is not None and entry != stop:
        R = abs(entry - stop)
        tgt = entry + rr * R if bull else entry - rr * R

    def px(v):
        if v is None:
            return '—'
        a = abs(v)
        d = 2 if a >= 1000 else 4 if a >= 1 else 6 if a >= 0.01 else 8
        return f"{v:,.{d}f}".rstrip('0').rstrip('.')

    return (
        f"🚨 <b>cam_rev signal — {sym}</b>\n"
        f"{side}{rtag}\n\n"
        f"Entry (limit @ pivot): <b>{px(entry)}</b>\n"
        f"Target: {px(tgt)}   (+{rr:g}R)\n"
        f"Stop:   {px(stop)}\n\n"
        f"<i>Camarilla reversal fade · demo/observed — not advice.</i>"
    )


def _send(token, chat, text):
    if not token or not chat:
        print(f"[cam_rev_alert] no Telegram creds — would have sent:\n{text}\n")
        return False
    try:
        data = urllib.parse.urlencode({'chat_id': chat, 'text': text, 'parse_mode': 'HTML',
                                       'disable_web_page_preview': 'true'}).encode()
        req = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage', data=data)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[cam_rev_alert] send failed: {e}")
        return False


def main():
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    try:
        rows = json.load(open(SWING)).get('signals', [])
    except Exception as e:
        print(f"[cam_rev_alert] cannot read feed: {e}")
        return
    live = [r for r in rows
            if r.get('strategy') == 'cam_rev' and r.get('state') == 'triggered' and not r.get('demo_only')]
    alerted = _load_state()
    now = time.time()
    sent = 0
    for r in live:
        sid = r.get('id')
        if not sid or sid in alerted:
            continue
        if _send(token, chat, _fmt(r)):
            sent += 1
        alerted[sid] = now        # mark seen whether or not the send succeeded (no alert storms on an API blip)
    # prune old ids so the state file stays small
    alerted = {k: v for k, v in alerted.items() if now - v < PRUNE_SECS}
    _save_state(alerted)
    print(f"[cam_rev_alert] live cam_rev={len(live)} new_alerts_sent={sent} tracked_ids={len(alerted)}")


if __name__ == '__main__':
    main()
