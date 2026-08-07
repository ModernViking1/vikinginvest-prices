"""Targeted 5-minute GOLD backfill (XAU_USD) — written to a SEPARATE file.

The main historical-ohlc.json intentionally stops at m15 to stay small; true
5-minute candles across 40 pairs would bloat it into tens of MB (slow mobile
loads, jsDelivr limits). This pulls ONLY gold M5 from OANDA into its own
gold-m5-ohlc.json so the itstomtrades 2nd-hour session-reversal setup can be
tested on a genuine lower-timeframe reversal instead of the m15 proxy.

Reuses the OANDA fetcher/chunker from fetch_historical_ohlc.py (tick volume
carried on each bar). Env: OANDA_TOKEN (required). Runs in CI where the token
lives — see .github/workflows/gold-m5-fetch.yml.

Run: python fetch_gold_m5.py [--months 12]
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from fetch_historical_ohlc import fetch_oanda_candles

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gold-m5-ohlc.json")
INSTRUMENT = "XAU_USD"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=12, help="how many months back to fetch")
    args = ap.parse_args()

    token = os.environ.get("OANDA_TOKEN", "").strip()
    if not token:
        print("ERROR: OANDA_TOKEN not set — cannot fetch gold M5", flush=True)
        sys.exit(1)

    to_ts = datetime.now(timezone.utc)
    from_ts = to_ts - timedelta(days=args.months * 31)
    print(f"Fetching {INSTRUMENT} M5 {from_ts:%Y-%m-%d} -> {to_ts:%Y-%m-%d} ...", flush=True)

    bars = fetch_oanda_candles(token, INSTRUMENT, "M5", from_ts, to_ts)
    print(f"  got {len(bars)} M5 bars", flush=True)
    if len(bars) < 1000:
        print("ERROR: too few bars returned — not overwriting the existing file", flush=True)
        sys.exit(1)

    out = {
        "generated": to_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "instrument": INSTRUMENT,
        "granularities": ["m5"],
        "pairs": {"xauusd": {"m5": bars}},
    }
    with open(OUT, "w") as f:
        json.dump(out, f)
    print(f"  wrote {OUT} ({os.path.getsize(OUT) // 1024} KB, {len(bars)} bars)", flush=True)


if __name__ == "__main__":
    main()
