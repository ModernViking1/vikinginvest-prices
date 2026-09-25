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
HEARTBEAT_STALE_MIN = 25                # cBot pings every ~10 min; no ping this long = bot stopped (definitive)


def _heartbeat_age_min():
    """Minutes since the swing cBot's last heartbeat, via the heartbeat.yml workflow's last run.
    None when unavailable (no token, no runs yet — e.g. before the cBot rebuild ships the heartbeat)."""
    tok = os.environ.get("GITHUB_TOKEN"); repo = os.environ.get("GITHUB_REPOSITORY")
    if not tok or not repo:
        return None
    url = f"https://api.github.com/repos/{repo}/actions/workflows/heartbeat.yml/runs?per_page=1"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}",
                                "Accept": "application/vnd.github+json", "User-Agent": "viking-watchdog"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            runs = json.load(r).get("workflow_runs") or []
        if not runs:
            return None
        dt = datetime.fromisoformat(str(runs[0]["created_at"]).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
    except Exception:
        return None


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

    hb_min = _heartbeat_age_min()          # None if unavailable (no token / API error / no runs yet)
    hb_avail = hb_min is not None
    hb_fresh = hb_avail and hb_min <= HEARTBEAT_STALE_MIN
    hb_down = hb_avail and in_session and hb_min > HEARTBEAT_STALE_MIN
    # The heartbeat is DEFINITIVE liveness (the cBot pings every ~10 min while its poll thread is
    # alive). When it's available, trust ONLY it: the silence-based heuristics below are false-
    # positive-prone — a quiet session legitimately has no fills for >90 min while the bot is fine —
    # and exist only as a fallback for when the heartbeat can't be read (pre-heartbeat build / API
    # blip). This matters now the watchdog runs every ~5 min inline: without it, quiet spells would
    # spam a false "cBot DOWN".
    if hb_avail:
        swing_specific = both_down = False
        down = hb_down
    else:
        swing_specific = in_session and swing_min > SWING_STALE_MIN and intra_min < INTRADAY_FRESH_MIN
        both_down = (now.weekday() < 5 and CORE_OPEN <= now.hour < CORE_CLOSE
                    and swing_min > DUAL_STALE_MIN and intra_min > DUAL_STALE_MIN)
        down = swing_specific or both_down
    recovered = st.get("alerted") and (hb_fresh or swing > st.get("last_swing_ts", 0))

    if down and not st.get("alerted"):
        if hb_down:
            msg = (f"🔴 <b>Swing cBot STOPPED</b>\nNo heartbeat for <b>{hb_min:.0f} min</b> — the poll thread "
                   f"is dead (zombie/disconnect). Restart cTrader + the VikingSwingBridge instance and "
                   f"re-enter the Publish params.")
        elif both_down:
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
