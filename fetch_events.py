#!/usr/bin/env python3
"""
fetch_events.py — Bridge script that fetches the Forex Factory weekly
economic calendar XML, filters to high-impact events for currencies
relevant to the Viking Invest Trading dashboard, and writes events.json
to the public CDN repo.

Designed to be run by a GitHub Actions cron workflow on an hourly schedule.

Usage:
    python fetch_events.py --output events.json

Dependencies:
    pip install requests  # standard library xml.etree handles parsing

Notes:
- Forex Factory's XML feed at nfs.faireconomy.media is unofficial but
  has been stable for years. If it breaks, alternative sources include
  TradingEconomics API (requires key) or NinjaTrader's calendar feed.
- Times in the FF XML come in Eastern Time (US/Eastern) — we convert
  to UTC ISO 8601 for the dashboard's consumption.
- We only keep impact='High' events for the 9 fiat currencies traded
  on the dashboard. Crypto pairs (BTC, SUI) and indices (DAX, Brent)
  inherit risk from their underlying currency exposure.
"""
import argparse
import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Config ──────────────────────────────────────────────────────────
FF_XML_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"

# Currencies that affect at least one of the dashboard's 22 pairs.
# Other currencies are filtered out to keep events.json small and relevant.
RELEVANT_CURRENCIES = {
    "USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF", "SGD",
    # CNY occasionally appears for USD/CNH; we keep it in case dashboard
    # adds CNH later. Currently no pair uses it.
    "CNY",
}

# Forex Factory XML uses these impact values. We keep only High by default;
# flip to include Medium if you want a more conservative deferral set.
KEEP_IMPACTS = {"High"}  # Add "Medium" to widen filter

# 2026-07-02 — TIMEZONE FIX. This feed's times are already UTC, not ET.
# The prior code assumed Eastern Time and applied an ET->UTC conversion,
# which double-counted the offset and pushed every event +4h (EDT) / +5h
# (EST) too LATE — e.g. NFP landed at 16:30 UTC in events.json when the
# real 8:30-ET release (and the observed m15 volatility spike) is 12:30
# UTC. Verified against the price data: on NFP days the spike is at
# 12:00-13:00 UTC and 16:30 UTC is quiet. So we now stamp the feed's naive
# time as UTC directly. If Forex Factory ever changes the feed's account
# timezone, re-verify against a known 8:30-ET release and adjust FEED_TZ.
FEED_TZ = timezone.utc


def parse_ff_datetime(date_str: str, time_str: str) -> str | None:
    """Convert FF date+time strings to UTC ISO 8601.

    FF format examples:
      date: '04-30-2026' (mm-dd-yyyy)
      time: '8:30am', '2:00pm', 'All Day', 'Tentative'
    """
    if not date_str or not time_str:
        return None
    # Skip non-time entries
    if time_str.lower() in ("all day", "tentative", ""):
        return None
    try:
        dt_str = f"{date_str} {time_str}"
        # Try common FF formats
        for fmt in ("%m-%d-%Y %I:%M%p", "%m-%d-%Y %I:%M %p"):
            try:
                naive = datetime.strptime(dt_str, fmt)
                aware = naive.replace(tzinfo=FEED_TZ)
                return aware.astimezone(timezone.utc).isoformat()
            except ValueError:
                continue
        return None
    except Exception as exc:  # pragma: no cover
        print(f"WARN: parse failed for {date_str!r} {time_str!r}: {exc}",
              file=sys.stderr)
        return None


def fetch_events() -> list[dict]:
    """Fetch FF XML and return a filtered, normalised list of events."""
    resp = requests.get(FF_XML_URL, timeout=30,
                         headers={"User-Agent": "VikingInvestTrading/1.0"})
    resp.raise_for_status()

    # FF XML occasionally has BOM or encoding markers; tolerate both
    text = resp.content.decode("utf-8-sig", errors="replace")
    root = ET.fromstring(text)

    events = []
    for evt in root.findall("event"):
        impact = (evt.findtext("impact") or "").strip()
        if impact not in KEEP_IMPACTS:
            continue
        currency = (evt.findtext("country") or "").strip().upper()
        if currency not in RELEVANT_CURRENCIES:
            continue

        date_s = (evt.findtext("date") or "").strip()
        time_s = (evt.findtext("time") or "").strip()
        iso_utc = parse_ff_datetime(date_s, time_s)
        if iso_utc is None:
            # Skip "All Day" / "Tentative" / unparseable
            continue

        title = (evt.findtext("title") or "").strip()
        forecast = (evt.findtext("forecast") or "").strip() or None
        previous = (evt.findtext("previous") or "").strip() or None

        events.append({
            "time": iso_utc,
            "currency": currency,
            "impact": impact.lower(),
            "title": title,
            "forecast": forecast,
            "previous": previous,
        })

    # Sort chronologically
    events.sort(key=lambda e: e["time"])
    return events


def update_history(events: list[dict], history_path: Path,
                   retention_days: int = 400) -> int:
    """Accumulate weekly events into a growing, deduped history file.

    events.json only holds the CURRENT week, so it can't be backtested
    against. This appends each run's events into events-history.json
    (deduped by time+currency+title, latest fields win so post-release
    revisions are captured) and prunes beyond the retention window. Over
    weeks this builds a backtestable economic-event calendar — needed to
    validate any event trade-free-zone before deploying one. Returns the
    number of NEW rows added this run.
    """
    existing = {}
    if history_path.exists():
        try:
            for e in json.loads(history_path.read_text()).get("events", []):
                existing[(e.get("time"), e.get("currency"), e.get("title"))] = e
        except Exception as exc:  # corrupt/partial file — rebuild from scratch
            print(f"WARN: history unreadable ({exc}) — rebuilding", file=sys.stderr)
    before = len(existing)
    for e in events:
        existing[(e.get("time"), e.get("currency"), e.get("title"))] = e
    rows = list(existing.values())
    # Prune beyond retention (times are UTC ISO; lexicographic compare is safe).
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    rows = [e for e in rows if (e.get("time") or "") >= cutoff]
    rows.sort(key=lambda e: e.get("time") or "")
    history_path.write_text(json.dumps({
        "updated": datetime.now(timezone.utc).isoformat(),
        "retention_days": retention_days,
        "events": rows,
        "source": "forexfactory.com via nfs.faireconomy.media (accumulated)",
    }, indent=2))
    return len(existing) - before


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o", default="events.json",
                         help="Output path for events.json")
    parser.add_argument("--history", default="events-history.json",
                         help="Accumulated event-history path (deduped, pruned)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print events to stdout, don't write file")
    args = parser.parse_args()

    try:
        events = fetch_events()
    except requests.RequestException as exc:
        print(f"ERROR fetching FF XML: {exc}", file=sys.stderr)
        sys.exit(1)
    except ET.ParseError as exc:
        print(f"ERROR parsing FF XML: {exc}", file=sys.stderr)
        sys.exit(1)

    payload = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "events": events,
        "source": "forexfactory.com via nfs.faireconomy.media",
    }

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {len(events)} events to {out_path}")

    # Accumulate into the deduped history file (builds a backtestable calendar).
    try:
        added = update_history(events, Path(args.history))
        print(f"History: +{added} new events -> {args.history}")
    except Exception as exc:  # never let history-keeping break the main publish
        print(f"WARN: history update failed (non-fatal): {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
