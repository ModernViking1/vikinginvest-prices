"""Aggregate dashboard visitor analytics from Supabase usage_events into visitor-stats.json.

Reads the usage_events table with the SERVICE-ROLE key (bypasses RLS) via the PostgREST API,
then writes AGGREGATE COUNTS ONLY to visitor-stats.json — no PII (no emails, session ids or user
ids) ever lands in the committed file, since the repo/CDN is public. The dashboard's Traffic panel
renders this JSON.

Events logged by the dashboard (see Viking_Invest_Trading_v69.html):
  • page_load / dashboard        — every visit
  • page_view / <tab>            — tab opens: dash | performance | investor | bt (Backtest tab)
  • panel_view / backtest_3y     — (finer signal; only if deployed) 3-Year panel scrolled into view

Env:
  SUPABASE_URL          (default: the project URL below)
  SUPABASE_SERVICE_KEY  (required; a repo secret — service-role, NOT the anon key)

Fail-open: any error prints a warning and leaves the existing JSON untouched (exit 0), so a
transient Supabase hiccup never breaks CI or wipes the panel.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone, timedelta

URL = os.environ.get("SUPABASE_URL", "https://opwdsuusdmsaicoyqxti.supabase.co").rstrip("/")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
OUT = "visitor-stats.json"
WINDOW_DAYS = 30
PAGE = 1000


def _fetch_all():
    """Page through usage_events (newest-irrelevant; we need all in-window + all-time counts).
    Selects only the columns needed to COUNT — the raw ids are used for distinct-counting in memory
    and never written out."""
    rows, offset = [], 0
    cols = "event,target,session_id,user_id,created_at"
    while True:
        q = urllib.parse.urlencode({"select": cols, "order": "created_at.asc"})
        req = urllib.request.Request(f"{URL}/rest/v1/usage_events?{q}",
                                     headers={"apikey": KEY, "Authorization": f"Bearer {KEY}",
                                              "Range-Unit": "items", "Range": f"{offset}-{offset+PAGE-1}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            batch = json.load(r)
        rows.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
    return rows


def _parse_ts(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def main():
    if not KEY:
        print("::warning::SUPABASE_SERVICE_KEY not set — leaving visitor-stats.json untouched", flush=True)
        return 0
    try:
        rows = _fetch_all()
    except Exception as e:
        print(f"::warning::usage_events fetch failed ({e}) — leaving visitor-stats.json untouched", flush=True)
        return 0

    now = datetime.now(timezone.utc)
    win_start = now - timedelta(days=WINDOW_DAYS)
    for r in rows:
        r["_dt"] = _parse_ts(r.get("created_at"))
    inwin = [r for r in rows if r["_dt"] and r["_dt"] >= win_start]

    def uniq_sessions(rs):
        return len({r.get("session_id") for r in rs if r.get("session_id")})

    def signed_in(rs):
        return len({r.get("user_id") for r in rs if r.get("user_id")})

    # per-tab opens (page_view / <tab>) within the window
    tabs = defaultdict(list)
    for r in inwin:
        if r.get("event") == "page_view" and r.get("target"):
            tabs[r["target"]].append(r)
    tab_opens = {t: {"opens": len(v), "sessions": uniq_sessions(v),
                     "last": max(x["_dt"] for x in v).strftime("%Y-%m-%dT%H:%M:%SZ")}
                 for t, v in sorted(tabs.items(), key=lambda kv: -len(kv[1]))}

    # backtest-tab focus (page_view/bt, plus the finer panel_view/backtest_3y if it ever logs)
    bt = [r for r in inwin if r.get("event") in ("page_view", "panel_view")
          and r.get("target") in ("bt", "backtest_3y")]
    backtest = {"opens": len(bt), "unique_sessions": uniq_sessions(bt),
                "last_opened": (max(x["_dt"] for x in bt).strftime("%Y-%m-%dT%H:%M:%SZ") if bt else None)}

    # daily trend (last WINDOW_DAYS)
    by_day = defaultdict(lambda: {"loads": [], "views": 0, "sessions": set()})
    for r in inwin:
        d = r["_dt"].strftime("%Y-%m-%d")
        by_day[d]["sessions"].add(r.get("session_id"))
        if r.get("event") == "page_load":
            by_day[d]["loads"].append(r)
        if r.get("event") == "page_view" and r.get("target") == "bt":
            by_day[d]["views"] += 1
    daily = [{"day": d, "page_loads": len(v["loads"]),
              "unique_visitors": len([s for s in v["sessions"] if s]), "backtest_views": v["views"]}
             for d, v in sorted(by_day.items())]

    out = {
        "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": WINDOW_DAYS,
        "window": {
            "events": len(inwin),
            "unique_visitors": uniq_sessions(inwin),
            "signed_in_visitors": signed_in(inwin),
            "page_loads": sum(1 for r in inwin if r.get("event") == "page_load"),
        },
        "tab_opens": tab_opens,
        "backtest_tab": backtest,
        "daily": daily,
        "all_time": {
            "events": len(rows),
            "unique_visitors": uniq_sessions(rows),
            "signed_in_visitors": signed_in(rows),
            "first_event": (min(r["_dt"] for r in rows if r["_dt"]).strftime("%Y-%m-%dT%H:%M:%SZ") if rows else None),
            "last_event": (max(r["_dt"] for r in rows if r["_dt"]).strftime("%Y-%m-%dT%H:%M:%SZ") if rows else None),
        },
    }
    with open(OUT, "w") as f:
        json.dump(out, f, indent=1)
    w = out["window"]
    print(f"visitor-stats: {w['unique_visitors']} visitors / {w['page_loads']} loads / "
          f"backtest opens {backtest['opens']} (last {backtest['last_opened']}) over {WINDOW_DAYS}d", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
