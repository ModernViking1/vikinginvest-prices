"""Telegram CRYPTO alert — one message per fresh crypto signal from a PROVEN,
currently-profitable strategy, carrying entry / take-profit / stop-loss.

"Proven & profitable, not demoted" is derived, not hard-coded:
  proven = LIVE (the set promoted to the cBot — demoted strategies are excluded
           by construction, since demotion removes a strategy from LIVE)
           ∩ genuine forward expectancy > 0 (a profitable win rate, measured the
           same way the weekly observer review measures it).
So a strategy that decays to a negative forward record automatically stops
alerting, and a demoted one never alerts.

Reads the two signal feeds the proven strategies emit into:
  • signals.json       (intraday emitters: absorb_btc, mmove_m15, …) — explicit entry/stop/target
  • swing-signals.json (swing: twob, hs, engulf_manip, …)          — ref_entry/stop/rr (market entry)
keeps only class 'crypto' + state 'triggered', de-dupes via crypto-alerts-state.json
(fires each signal exactly once), and sends to CRYPTO_ALERT_CHAT_ID (falling back to
TELEGRAM_CHAT_ID) via TELEGRAM_BOT_TOKEN. Fail-open: any error sends nothing and never
blocks the pipeline. Run at the end of fetch-data.yml (every ~5 min).

Run: python crypto_alert.py
"""
import json
import os
import urllib.request
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
SIGNALS = os.path.join(_HERE, 'signals.json')
SWING = os.path.join(_HERE, 'swing-signals.json')
STATE = os.path.join(_HERE, 'crypto-alerts-state.json')
STATE_CAP = 800                      # keep the most recent N alerted ids
MIN_WR = 50.0                        # only alert on strategies whose forward win rate exceeds this.
                                     # NOTE: at fixed RR2 a strategy is profitable above ~33% WR, so
                                     # this is a HIT-RATE preference, not a profitability gate — it
                                     # drops profitable-but-lower-WR strategies (e.g. hs, engulf_manip).
                                     # Set to 0.0 to alert on every proven, positive-expectancy strategy.


def _proven_profitable():
    """{strategy: (wr, exp)} for LIVE strategies with a positive forward record.

    Uses observer_review's own genuine-forward measure so 'profitable win rate'
    means exactly what the weekly review means. Empty on any failure (fail-open →
    no alerts rather than a bad send)."""
    try:
        from observer_review import review, LIVE
        rows, _, _ = review()
    except Exception:
        return {}
    out = {}
    for r in rows:
        st = r.get('st')
        # promoted (not demoted) AND positive forward expectancy AND win rate over the threshold
        if st in LIVE and r.get('exp', -1) > 0 and r.get('wr', 0.0) > MIN_WR:
            out[st] = (r.get('wr', 0.0), r.get('exp', 0.0))
    return out


def _load_state():
    try:
        return set(json.load(open(STATE)).get('alerted', []))
    except Exception:
        return set()


def _save_state(ids):
    try:
        json.dump({'alerted': list(ids)[-STATE_CAP:]}, open(STATE, 'w'))
    except Exception:
        pass


def _num(x):
    try:
        return float(x)
    except Exception:
        return None


def _candidates(proven):
    """Fresh crypto signals from proven strategies, normalized to a common shape:
    {id, sym, strat, dir, entry, stop, target, rr}."""
    out = []

    # 1) intraday feed — explicit entry/stop/target
    try:
        for r in json.load(open(SIGNALS)).get('signals', []):
            if r.get('cls') != 'crypto' or r.get('state') != 'triggered':
                continue
            st = r.get('method') or (r.get('id', '').split(':')[2] if r.get('id', '').count(':') >= 2 else None)
            if st not in proven:
                continue
            entry, stop, tgt = _num(r.get('entry')), _num(r.get('stop')), _num(r.get('target'))
            if entry is None or stop is None or not tgt:
                continue
            rr = round(abs(tgt - entry) / abs(entry - stop), 2) if entry != stop else None
            out.append({'id': r.get('id'), 'sym': (r.get('sym') or r.get('pair', '')).upper(),
                        'strat': st, 'dir': r.get('dir'), 'entry': entry, 'stop': stop,
                        'target': tgt, 'rr': rr})
    except Exception:
        pass

    # 2) swing feed — market entry (ref_entry), target derived from rr
    try:
        for r in json.load(open(SWING)).get('signals', []):
            if r.get('class') != 'crypto' or r.get('state') != 'triggered':
                continue
            st = r.get('strategy')
            if st not in proven:
                continue
            entry, stop = _num(r.get('ref_entry')), _num(r.get('stop'))
            rr = _num(r.get('rr')) or 2.0
            if entry is None or stop is None or entry == stop:
                continue
            R = abs(entry - stop)
            tgt = entry + rr * R if r.get('dir') == 'bull' else entry - rr * R
            out.append({'id': r.get('id'), 'sym': (r.get('symbol') or r.get('pair', '')).upper(),
                        'strat': st, 'dir': r.get('dir'), 'entry': entry, 'stop': stop,
                        'target': round(tgt, 8), 'rr': round(rr, 2)})
    except Exception:
        pass

    return out


def _fmt(sig, wr, exp):
    side = '🟢 LONG' if sig['dir'] == 'bull' else '🔴 SHORT'
    rr = f"{sig['rr']:g}R" if sig.get('rr') else 'RR'

    def px(v):
        if v is None:
            return '—'
        a = abs(v)
        d = 2 if a >= 1000 else 4 if a >= 1 else 6 if a >= 0.01 else 8
        return f"{v:,.{d}f}".rstrip('0').rstrip('.')
    return (
        f"🚨 <b>Crypto signal — {sig['sym']}</b>\n"
        f"{side}  ·  <b>{sig['strat']}</b>  ({wr:.0f}% WR · {exp:+.2f}R fwd)\n\n"
        f"Entry:   <b>{px(sig['entry'])}</b>\n"
        f"Target:  {px(sig['target'])}   (+{rr})\n"
        f"Stop:    {px(sig['stop'])}\n\n"
        f"<i>Proven strategy, positive forward edge. Demo/observed — not advice.</i>"
    )


def _send(token, chat, text):
    if not token or not chat:
        print(f"[crypto_alert] no Telegram creds — would have sent:\n{text}\n")
        return False
    try:
        data = urllib.parse.urlencode({'chat_id': chat, 'text': text, 'parse_mode': 'HTML',
                                       'disable_web_page_preview': 'true'}).encode()
        req = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage', data=data)
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status == 200
    except Exception as e:
        print(f"[crypto_alert] send failed: {e}")
        return False


def main():
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = os.environ.get('CRYPTO_ALERT_CHAT_ID', '').strip() or os.environ.get('TELEGRAM_CHAT_ID', '').strip()

    proven = _proven_profitable()
    if not proven:
        print("[crypto_alert] no proven-profitable strategies (or review unavailable) — nothing to alert")
        return
    print(f"[crypto_alert] proven+profitable crypto-eligible strategies: {sorted(proven)}")

    alerted = _load_state()
    sent = 0
    for sig in _candidates(proven):
        sid = sig.get('id')
        if not sid or sid in alerted:
            continue
        wr, exp = proven[sig['strat']]
        if _send(token, chat, _fmt(sig, wr, exp)):
            sent += 1
        alerted.add(sid)         # mark seen even on send failure — avoid a retry storm
    _save_state(alerted)
    print(f"[crypto_alert] sent {sent} new crypto alert(s)")


if __name__ == '__main__':
    main()
