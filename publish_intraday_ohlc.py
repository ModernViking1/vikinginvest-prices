#!/usr/bin/env python3
"""
publish_intraday_ohlc.py — Tick-aggregating intraday publisher (v2).

CHANGES FROM v1:
    - Drops TwelveData dependency (lower API cost, simpler error path)
    - All FX pairs use OANDA v20 directly (free for our usage volume)
    - Designed for 1-minute polling cadence
    - Self-loop mode: runs N polls within a single workflow run, so
      cron schedule doesn't dictate cadence. Default loop = 1 poll.
    - Crypto pairs continue to use Coinbase public ticker (free)

USAGE (cron mode, 1 poll per run):
    python publish_intraday_ohlc.py --state intraday-state.json --output intraday-ohlc.json

USAGE (loop mode, multiple polls per workflow run):
    python publish_intraday_ohlc.py --loop 4 --interval 60

DEPENDENCIES:
    pip install requests

ENVIRONMENT:
    OANDA_TOKEN        — OANDA v20 practice token (required for FX pairs)
    OANDA_ACCOUNT_ID   — Optional; auto-discovered if not provided
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ──────────────────────────────────────────────────────────
# All FX pairs use OANDA. Crypto uses Coinbase. DXY synthesised separately.
PAIRS = {
    "eurusd":  {"oanda": "EUR_USD"},
    "gbpusd":  {"oanda": "GBP_USD"},
    "usdjpy":  {"oanda": "USD_JPY"},
    "usdcad":  {"oanda": "USD_CAD"},
    "usdchf":  {"oanda": "USD_CHF"},
    "audusd":  {"oanda": "AUD_USD"},
    "nzdusd":  {"oanda": "NZD_USD"},
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
    "usoil":   {"oanda": "BCO_USD"},
    "de40":    {"oanda": "DE30_EUR"},
    "btcusd":  {"coinbase": "BTC-USD"},
    "suiusd":  {"coinbase": "SUI-USD"},
}

WINDOW_MINUTES = 15
WINDOWS_TO_KEEP = 96


# ── OANDA batched fetch ────────────────────────────────────────────
# Fetch all FX prices in a single API call. OANDA's pricing endpoint
# accepts a comma-separated list of instruments, which is much more
# efficient than one call per pair.

_oanda_account_cache = None

def _resolve_oanda_account(api_key: str) -> str | None:
    global _oanda_account_cache
    if _oanda_account_cache:
        return _oanda_account_cache
    explicit = os.environ.get("OANDA_ACCOUNT_ID")
    if explicit:
        _oanda_account_cache = explicit
        return explicit
    try:
        r = requests.get(
            "https://api-fxpractice.oanda.com/v3/accounts",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        r.raise_for_status()
        accounts = r.json().get("accounts", [])
        if accounts:
            _oanda_account_cache = accounts[0]["id"]
            return _oanda_account_cache
    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"WARN oanda account discovery: {exc}", file=sys.stderr)
    return None


def fetch_oanda_batch(instruments: list[str], api_key: str) -> dict:
    """Fetch mid prices for multiple OANDA instruments in one call.

    Returns dict mapping instrument code → mid price. Pairs that
    failed are simply absent from the result.
    """
    if not instruments:
        return {}
    account = _resolve_oanda_account(api_key)
    if not account:
        return {}
    try:
        r = requests.get(
            f"https://api-fxpractice.oanda.com/v3/accounts/{account}/pricing",
            params={"instruments": ",".join(instruments)},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        prices = {}
        for p in data.get("prices", []):
            try:
                bid = float(p["bids"][0]["price"])
                ask = float(p["asks"][0]["price"])
                prices[p["instrument"]] = (bid + ask) / 2
            except (KeyError, IndexError, ValueError):
                continue
        return prices
    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"WARN oanda batch: {exc}", file=sys.stderr)
        return {}


def fetch_coinbase_price(product: str) -> float | None:
    try:
        r = requests.get(
            f"https://api.exchange.coinbase.com/products/{product}/ticker",
            timeout=10,
        )
        r.raise_for_status()
        return float(r.json()["price"])
    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"WARN coinbase {product}: {exc}", file=sys.stderr)
        return None


def fetch_all_prices() -> dict:
    """Return dict mapping our pair key → current price."""
    oanda_key = os.environ.get("OANDA_TOKEN") or os.environ.get("OANDA_API_KEY")
    out = {}

    # Batch OANDA fetch
    if oanda_key:
        oanda_pairs = {k: v["oanda"] for k, v in PAIRS.items() if "oanda" in v}
        instruments = list(oanda_pairs.values())
        prices = fetch_oanda_batch(instruments, oanda_key)
        # Reverse-map back to our keys
        rev = {v: k for k, v in oanda_pairs.items()}
        for instr, px in prices.items():
            if instr in rev:
                out[rev[instr]] = px

    # Coinbase one-by-one (only 2 pairs)
    for k, v in PAIRS.items():
        if "coinbase" in v:
            px = fetch_coinbase_price(v["coinbase"])
            if px is not None:
                out[k] = px

    return out


# ── Window math + state ────────────────────────────────────────────

def current_window_start(now: datetime) -> datetime:
    minutes_into_hour = now.minute - (now.minute % WINDOW_MINUTES)
    return now.replace(minute=minutes_into_hour, second=0, microsecond=0)


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"pairs": {}}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {"pairs": {}}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))


def update_pair(state: dict, key: str, price: float, now: datetime) -> None:
    if key not in state["pairs"]:
        state["pairs"][key] = {"current_window": None, "completed_bars": []}
    pair_state = state["pairs"][key]
    win_start_iso = current_window_start(now).isoformat()
    cur = pair_state["current_window"]
    if cur is None or cur["t"] != win_start_iso:
        # Window roll-over: close out old, start new
        if cur is not None and cur.get("ticks"):
            ticks = cur["ticks"]
            pair_state["completed_bars"].append({
                "t": cur["t"],
                "o": ticks[0],
                "h": max(ticks),
                "l": min(ticks),
                "c": ticks[-1],
                "p": ticks[-1],
            })
            if len(pair_state["completed_bars"]) > WINDOWS_TO_KEEP:
                pair_state["completed_bars"] = pair_state["completed_bars"][-WINDOWS_TO_KEEP:]
        pair_state["current_window"] = {"t": win_start_iso, "ticks": [price]}
    else:
        cur["ticks"].append(price)


def build_output(state: dict, now: datetime) -> dict:
    out = {"updated": now.isoformat(), "ohlc": True, "intraday": {}}
    for key, pair_state in state["pairs"].items():
        bars = list(pair_state.get("completed_bars", []))
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

def run_one_poll(state: dict, now: datetime) -> int:
    """Fetch all prices and update state. Returns count of pairs updated."""
    prices = fetch_all_prices()
    for key, price in prices.items():
        update_pair(state, key, price, now)
    return len(prices)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="intraday-state.json")
    parser.add_argument("--output", default="intraday-ohlc.json")
    parser.add_argument("--loop", type=int, default=1,
                        help="Number of poll cycles within this run (default 1)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Seconds between polls when --loop > 1 (default 60)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    state_path = Path(args.state)
    output_path = Path(args.output)
    state = load_state(state_path)

    total_updates = 0
    for cycle in range(args.loop):
        now = datetime.now(timezone.utc)
        count = run_one_poll(state, now)
        total_updates += count
        print(f"Cycle {cycle+1}/{args.loop}: fetched {count} pairs at {now.isoformat()}", flush=True)
        if cycle < args.loop - 1:
            time.sleep(args.interval)

    save_state(state_path, state)

    output = build_output(state, datetime.now(timezone.utc))
    if args.dry_run:
        print(json.dumps(output, indent=2))
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, indent=2))
        print(f"Wrote {len(output['intraday'])} pairs to {output_path}")


if __name__ == "__main__":
    main()
