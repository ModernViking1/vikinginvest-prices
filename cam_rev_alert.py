"""Feed-side cam_rev Telegram alerter — fires the moment a live cam_rev signal is published to
swing-signals.json, INDEPENDENT of the cBot (so it alerts even during a cBot outage, the gap that
hid the 2026-09-30 EURNOK/NZDCHF fills).

Dedup is DIFF-BASED, not a separate state file: alert a cam_rev signal only if its id is in the NEW
feed but was NOT in the PREVIOUS published feed (the checked-out swing-signals.json, snapshotted
before swing_signals.py regenerates it). This rides the reliable swing-signals.json commit — no
extra race-prone state file to drop — so each signal alerts exactly once (the cycle it first
appears). A dropped publish at worst re-alerts once next cycle, never a storm.

Scope: strategy == cam_rev, state == triggered, NOT demo_only (live FX / index / comm; the crypto
pilot stays silent). Fail-open. Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (same chat as the outage
watchdog).

  python cam_rev_alert.py --prev <prev feed> --cur swing-signals.json
"""
import argparse
import json
import os
import urllib.parse
import urllib.request


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _live_cam_rev(path):
    """{id: row} for live cam_rev signals in a feed file (empty on any error)."""
    out = {}
    try:
        for r in json.load(open(path)).get('signals', []):
            if (r.get('strategy') == 'cam_rev' and r.get('state') == 'triggered'
                    and not r.get('demo_only') and r.get('id')):
                out[r['id']] = r
    except Exception as e:
        print(f"[cam_rev_alert] cannot read {path}: {e}")
    return out


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
    ap = argparse.ArgumentParser()
    ap.add_argument('--cur', default='swing-signals.json')
    ap.add_argument('--prev', default='')
    args = ap.parse_args()
    cur = _live_cam_rev(args.cur)
    if not args.prev or not os.path.exists(args.prev):
        # No previous feed to diff against — don't risk a backlog storm; alert nothing this cycle.
        print(f"[cam_rev_alert] no prev feed ({args.prev!r}) — skipping (cur live cam_rev={len(cur)})")
        return
    prev = _live_cam_rev(args.prev)
    new_ids = [sid for sid in cur if sid not in prev]
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    sent = 0
    for sid in new_ids:
        if _send(token, chat, _fmt(cur[sid])):
            sent += 1
    print(f"[cam_rev_alert] cur={len(cur)} prev={len(prev)} new={len(new_ids)} sent={sent}")


if __name__ == '__main__':
    main()
