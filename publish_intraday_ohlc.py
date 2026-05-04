#!/usr/bin/env python3
"""
publish_intraday_ohlc.py — Tick-aggregating intraday publisher.

Replaces the single-price-per-15m model with proper OHLC bars by
accumulating all observed ticks within each 15-minute window. At each
15m boundary the window closes and the OHLC bar is emitted to
intraday.json.

Designed to be run by a GitHub Actions cron workflow at high frequency
(every 1-2 minutes). State is persisted to a small JSON file between
runs so accumulated ticks survive across short-lived workflow runs.

USAGE:
    python publish_intraday_ohlc.py --state intraday-state.json --output intraday.json

DEPENDENCIES:
    pip install requests

NOTE:
    This is a reference implementation. Adapt the data source functions
    (fetch_twelvedata_price, fetch_oanda_price, fetch_coinbase_price)
    to match your existing setup. The aggregation logic and output
    schema are what matter — the data sourcing is up to you.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ── Config ──────────────────────────────────────────────────────────
# Map dashboard pair keys to data-source identifiers. Adapt to your
# actual keys / sources. Crypto and DXY excluded — handled separately
# or skipped (DXY is synthesised; crypto uses Coinbase).
PAIRS = {
    "eurusd":  {"twelvedata": "EUR/USD",  "oanda": "EUR_USD"},
    "gbpusd":  {"twelvedata": "GBP/USD",  "oanda": "GBP_USD"},
    "usdjpy":  {"twelvedata": "USD/JPY",  "oanda": "USD_JPY"},
    "usdcad":  {"twelvedata": "USD/CAD",  "oanda": "USD_CAD"},
    "usdchf":  {"twelvedata": "USD/CHF",  "oanda": "USD_CHF"},
    "audusd":  {"twelvedata": "AUD/USD",  "oanda": "AUD_USD"},
    "nzdusd":  {"twelvedata": "NZD/USD",  "oanda": "NZD_USD"},
    "cadjpy":  {"oanda": "CAD_JPY"},
    "eurnzd":  {"oanda": "EUR_NZD"},
    "gbpaud":  {"oanda": "GBP_AUD"},
    "euraud":  {"oanda": "EUR_AUD"},
    "usdsgd":  {"oanda": "USD_SGD"},
    "audnzd":  {"oanda": "AUD_NZD"},
    "audchf":  {"oanda": "AUD_CHF"},
    "eurgbp":  {"oanda": "EUR_GBP"},
    "xauusd":  {"oanda": "XAU_USD"},
    "xagusd":  {"oanda": "XAG_USD"},
    "usoil":   {"oanda": "BCO_USD"},  # Brent crude
    "de40":    {"oanda": "DE30_EUR"}, # DAX
    "btcusd":  {"coinbase": "BTC-USD"},
    "suiusd":  {"coinbase": "SUI-USD"},
    # dxy synthesised separately, no native price source
}

# Window length and how many windows to retain
WINDOW_MINUTES = 15
WINDOWS_TO_KEEP = 96  # 24 hours of 15m bars


# ── Data source stubs ─────────────────────────────────────────────
# Replace these with your actual fetch logic. Each should return the
# current price as a float, or None on error.

def fetch_twelvedata_price(symbol: str, api_key: str) -> float | None:
    """Fetch current price from TwelveData /price endpoint."""
    try:
        r = requests.get(
            "https://api.twelvedata.com/price",
            params={"symbol": symbol, "apikey": api_key},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if "price" in data:
            return float(data["price"])
    except (requests.RequestException, ValueError) as exc:
        print(f"WARN twelvedata {symbol}: {exc}", file=sys.stderr)
    return None


def fetch_oanda_price(instrument: str, api_key: str, account: str) -> float | None:
    """Fetch current price from OANDA v20 pricing endpoint."""
    try:
        r = requests.get(
            f"https://api-fxpractice.oanda.com/v3/accounts/{account}/pricing",
            params={"instruments": instrument},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("prices"):
            p = data["prices"][0]
            bid = float(p["bids"][0]["price"])
            ask = float(p["asks"][0]["price"])
            return (bid + ask) / 2
    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"WARN oanda {instrument}: {exc}", file=sys.stderr)
    return None


def fetch_coinbase_price(product: str) -> float | None:
    """Fetch current price from Coinbase public ticker."""
    try:
        r = requests.get(
            f"https://api.exchange.coinbase.com/products/{product}/ticker",
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return float(data["price"])
    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"WARN coinbase {product}: {exc}", file=sys.stderr)
    return None


def fetch_price_for_pair(key: str, sources: dict) -> float | None:
    """Try sources in order, return first successful price."""
    twelvedata_key = os.environ.get("TWELVEDATA_API_KEY")
    oanda_key = os.environ.get("OANDA_API_KEY")
    oanda_account = os.environ.get("OANDA_ACCOUNT_ID")

    if "twelvedata" in sources and twelvedata_key:
        p = fetch_twelvedata_price(sources["twelvedata"], twelvedata_key)
        if p is not None:
            return p
    if "oanda" in sources and oanda_key and oanda_account:
        p = fetch_oanda_price(sources["oanda"], oanda_key, oanda_account)
        if p is not None:
            return p
    if "coinbase" in sources:
        p = fetch_coinbase_price(sources["coinbase"])
        if p is not None:
            return p
    return None


# ── Window math ────────────────────────────────────────────────────

def current_window_start(now: datetime) -> datetime:
    """Return the start of the 15-minute window containing `now`."""
    minutes_into_hour = now.minute - (now.minute % WINDOW_MINUTES)
    return now.replace(minute=minutes_into_hour, second=0, microsecond=0)


# ── State management ───────────────────────────────────────────────
# State JSON shape:
# {
#   "pairs": {
#     "eurusd": {
#       "current_window": {
#         "t": "2026-05-04T13:00:00+00:00",
#         "ticks": [1.17220, 1.17225, 1.17240, ...]
#       },
#       "completed_bars": [
#         {"t": "...", "o": ..., "h": ..., "l": ..., "c": ..., "p": ...},
#         ...
#       ]
#     }
#   }
# }

def load_state(path: Path) -> dict:
    if not path.exists():
        return {"pairs": {}}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        print(f"WARN load_state failed, starting fresh: {exc}", file=sys.stderr)
        return {"pairs": {}}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))


def update_pair(state: dict, key: str, price: float, now: datetime) -> None:
    """Add a tick to the current window; close window if past boundary."""
    if key not in state["pairs"]:
        state["pairs"][key] = {"current_window": None, "completed_bars": []}
    pair_state = state["pairs"][key]

    win_start = current_window_start(now)
    win_start_iso = win_start.isoformat()

    cur = pair_state["current_window"]
    if cur is None or cur["t"] != win_start_iso:
        # New window — close out the old one if it exists
        if cur is not None and cur.get("ticks"):
            ticks = cur["ticks"]
            bar = {
                "t": cur["t"],
                "o": ticks[0],
                "h": max(ticks),
                "l": min(ticks),
                "c": ticks[-1],
                "p": ticks[-1],  # backwards compat with single-price schema
            }
            pair_state["completed_bars"].append(bar)
            # Keep only the last N bars
            if len(pair_state["completed_bars"]) > WINDOWS_TO_KEEP:
                pair_state["completed_bars"] = pair_state["completed_bars"][-WINDOWS_TO_KEEP:]
        # Start a fresh window
        pair_state["current_window"] = {"t": win_start_iso, "ticks": [price]}
    else:
        cur["ticks"].append(price)


def build_output(state: dict, now: datetime) -> dict:
    """Build the intraday.json output from accumulated state.

    Includes both completed bars AND the still-open current window
    (so the dashboard sees fresh data instead of waiting up to 15
    minutes for the bar to close).

    SCHEMA NOTE: the top-level field is `intraday` (not `data`) to
    match the existing dashboard's expectation in fetchVikingIntraday()
    where it reads `data.intraday`. The `ohlc: true` flag at the top
    level signals to the dashboard that bars include native OHLC and
    detect15mLevels() will switch from approximated to native mode
    automatically.
    """
    out = {"updated": now.isoformat(), "ohlc": True, "intraday": {}}
    for key, pair_state in state["pairs"].items():
        bars = list(pair_state.get("completed_bars", []))
        # Append the current (still-open) window as a "live" bar
        cur = pair_state.get("current_window")
        if cur and cur.get("ticks"):
            ticks = cur["ticks"]
            bars.append({
                "t": cur["t"],
                "o": ticks[0],
                "h": max(ticks),
                "l": min(ticks),
                "c": ticks[-1],
                "p": ticks[-1],
            })
        out["intraday"][key] = bars
    return out


# ── Main ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="intraday-state.json",
                        help="Path to persisted tick-accumulator state")
    parser.add_argument("--output", default="intraday.json",
                        help="Path to publish intraday.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print output instead of writing")
    args = parser.parse_args()

    state_path = Path(args.state)
    output_path = Path(args.output)
    state = load_state(state_path)

    now = datetime.now(timezone.utc)

    # Fetch and accumulate ticks for each configured pair
    fetched = 0
    for key, sources in PAIRS.items():
        price = fetch_price_for_pair(key, sources)
        if price is None:
            continue
        update_pair(state, key, price, now)
        fetched += 1

    # Save updated state for next run
    save_state(state_path, state)

    # Build and publish intraday.json
    output = build_output(state, now)
    if args.dry_run:
        print(json.dumps(output, indent=2))
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, indent=2))
        print(f"Wrote {fetched} pairs with OHLC bars to {output_path}")


if __name__ == "__main__":
    main()
