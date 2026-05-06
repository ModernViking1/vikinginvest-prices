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

# Eastern Time offset. DST handling: FF XML times are reported in ET
# which switches between EST (UTC-5) and EDT (UTC-4). We use the zoneinfo
# database for accurate conversion — Python 3.9+ has it built in.
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except ImportError:
    # Fallback for older Python — uses fixed UTC-5 (will be off by 1h
    # during DST). Recommend upgrading to 3.9+.
    print("WARNING: zoneinfo unavailable, using fixed UTC-5 offset", file=sys.stderr)
    ET_TZ = timezone(timedelta(hours=-5))


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
                aware_et = naive.replace(tzinfo=ET_TZ)
                return aware_et.astimezone(timezone.utc).isoformat()
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o", default="events.json",
                         help="Output path for events.json")
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


if __name__ == "__main__":
    main()
