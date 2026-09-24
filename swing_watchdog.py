"""Swing-bot liveness watchdog — Telegram alert when the swing cBot goes silent mid-session.

The swing cBot can zombie after a broker/data disconnect (shows "running" but stops polling — no
logs, no orders, no user-stop line), as happened 2026-09-23. This catches it fast: if, during
session hours (07-22 UTC), the swing feed has been silent for > SWING_STALE_MIN while the intraday
feed is still fresh (proving the market — and the account — is active, so it's not a lull), it fires
one Telegram alert; and one "recovered" message when swing publishes again. State-deduped so it
never spams. Fail-open. Interim measure until the cBot heartbeat lands (see VPS_SETUP.md).

Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SWING = "swing-executions.json"
INTRA = "executions.json"
STATE = "swing-watchdog-state.json"
SESS_OPEN, SESS_CLOSE = 7, 22          # UTC session (matches the swing feed)
SWING_STALE_MIN = 90                    # swing silent longer than this ...
INTRADAY_FRESH_MIN = 90                 # ... while intraday published within this = swing-specific outage
# Whole-terminal fallback: both bots run in the SAME cTrader terminal, so a disconnect/zombie kills
# BOTH — and the swing-specific rule above stays silent (intraday isn't "fresh"). During core weekday
# hours a dual silence this long is a near-certain terminal outage, not a market lull.
CORE_OPEN, CORE_CLOSE = 8, 20           # peak London+NY hours (UTC)
DUAL_STALE_MIN = 150                    # BOTH feeds silent longer than this in core hours = terminal down


def _latest_ts(fn):
    try:
        d = json.load(open(fn))
        ex = d.get("executions") or d.get("rows") or (d if isinstance(d, list) else [])
        best = 0.0
        for e in ex:
            if not isinstance(e, dict):
                continue
            t = e.get("ts")
            if isinstance(t, (int, float)):
                t = t / 1000.0 if t > 1e11 else t
                best = max(best, t)
        return best
    except Exception:
        return 0.0


def _tg(text):
    tok = os.environ.get("TELEGRAM_BOT_TOKEN"); chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        print("no telegram creds; would send:\n" + text); return
    data = urllib.parse.urlencode({"chat_id": chat, "text": text, "parse_mode": "HTML",
                                   "disable_web_page_preview": "true"}).encode()
    try:
        urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage", data=data, timeout=10)
        print("watchdog alert sent")
    except Exception as e:
        print(f"::warning::telegram send failed: {e}")


def main():
    now = datetime.now(timezone.utc); nowt = now.timestamp()
    try:
        st = json.load(open(STATE))
    except Exception:
        st = {"alerted": False, "last_swing_ts": 0, "updated": None}

    swing = _latest_ts(SWING); intra = _latest_ts(INTRA)
    swing_min = (nowt - swing) / 60.0 if swing else 1e9
    intra_min = (nowt - intra) / 60.0 if intra else 1e9
    in_session = SESS_OPEN <= now.hour < SESS_CLOSE
    f = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%H:%MZ") if t else "never"

    swing_specific = in_session and swing_min > SWING_STALE_MIN and intra_min < INTRADAY_FRESH_MIN
    both_down = (now.weekday() < 5 and CORE_OPEN <= now.hour < CORE_CLOSE
                and swing_min > DUAL_STALE_MIN and intra_min > DUAL_STALE_MIN)
    down = swing_specific or both_down
    recovered = st.get("alerted") and swing > st.get("last_swing_ts", 0)

    if down and not st.get("alerted"):
        if both_down:
            msg = (f"🔴 <b>Both cBots silent — cTrader likely DOWN</b>\nNo swing ({swing_min:.0f} min, last "
                   f"{f(swing)}) AND no intraday ({intra_min:.0f} min, last {f(intra)}) — a whole-terminal "
                   f"zombie/disconnect. Restart cTrader + both cBots and re-enter the Publish params.")
        else:
            msg = (f"🟠 <b>Swing cBot may be DOWN</b>\nNo swing execution for <b>{swing_min:.0f} min</b> "
                   f"(last {f(swing)}) while intraday is live (last {f(intra)}).\nLikely a zombie-after-disconnect "
                   f"— restart the VikingSwingBridge instance and re-enter the Publish params.")
        _tg(msg)
        st = {"alerted": True, "last_swing_ts": swing, "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ")}
    elif recovered:
        _tg(f"🟢 <b>Swing cBot back up</b>\nPublishing resumed (row at {f(swing)}).")
        st = {"alerted": False, "last_swing_ts": swing, "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ")}
    else:
        st["last_swing_ts"] = swing; st["updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"ok — swing {swing_min:.0f}m (last {f(swing)}) intraday {intra_min:.0f}m "
              f"session={in_session} alerted={st.get('alerted')}")

    with open(STATE, "w") as fh:
        json.dump(st, fh, indent=1)
    return 0


if __name__ == "__main__":
    main()
