"""US-equity OHLC fetcher for the swing pilot — writes the SAME schema as
historical-ohlc.json so the research harness can read it unchanged.

WHY A SEPARATE FETCHER: our production feed is OANDA (FX / metals / indices)
+ Coinbase (crypto). Neither carries individual US stocks, so the equity
pilot needs its own provider. This adapter uses Twelve Data (free tier:
8 req/min, 800/day — plenty for 5 symbols × 2 intervals), which returns
split/dividend-ADJUSTED daily + intraday bars for US equities.

    Set the key first:   export TWELVEDATA_API_KEY=xxxxx
    Run:                 python fetch_equity_ohlc.py --output equity-ohlc.json

OUTPUT SCHEMA (identical to historical-ohlc.json):
    {"granularities": ["h1","daily"],
     "pairs": {"aapl": {"h1":[{"t","o","h","l","c"}...], "daily":[...]}, ...}}

NOTE ON THIS SANDBOX: outbound HTTPS to data hosts is blocked by the egress
policy here, so this script is meant to run in CI (with the key as a repo
secret) or on your own machine — not inside the agent session. It fails
loudly with a clear message if the key is missing or the host is blocked.

Earnings dates (optional, improves the gap guard): if a sibling file
`equity-earnings.json` exists ({"aapl": ["2026-01-30", ...], ...}) the
research script uses it to blackout entries around reports. Twelve Data's
/earnings endpoint can populate it — see fetch_earnings_dates() below.
"""
import argparse
import json
import os
import sys
import time
from typing import Dict, List

# The pilot universe — 5 US mega-caps. Deliberately small: these are highly
# correlated (all track NAS100), so this is ~3-4 independent bets, which is the
# point of a cheap go/no-go before spending on EU/Asia data + calendars.
US_TOP5 = {
    "aapl": "AAPL",
    "nvda": "NVDA",
    "tsla": "TSLA",
    "msft": "MSFT",
    "amzn": "AMZN",
}

BASE = "https://api.twelvedata.com"
# Twelve Data interval codes → our timeframe keys. Daily + 1h only for the
# swing pilot (swing edges live on h1/daily; intraday m15 comes later once
# session logic is built).
INTERVALS = {"daily": "1day", "h1": "1h"}
OUTPUTSIZE = {"daily": 400, "h1": 5000}   # ~365d daily, ~200d of RTH hours


def _require_key() -> str:
    key = os.environ.get("TWELVEDATA_API_KEY", "").strip()
    if not key:
        sys.exit(
            "ERROR: TWELVEDATA_API_KEY not set.\n"
            "  Get a free key at https://twelvedata.com and:\n"
            "    export TWELVEDATA_API_KEY=xxxxx\n"
            "  (In CI, add it as a repository secret and export it in the workflow.)"
        )
    return key


def _get(path: str, params: Dict) -> Dict:
    """GET with a clear message if the sandbox egress policy blocks the host."""
    import requests  # imported lazily so --help works without the dep
    try:
        r = requests.get(BASE + path, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        sys.exit(
            f"ERROR: request to {BASE}{path} failed: {e}\n"
            "  If you are inside the agent sandbox, outbound data hosts are\n"
            "  blocked by the egress policy — run this in CI or locally instead."
        )
    if r.status_code == 403:
        sys.exit(
            "ERROR: 403 from the network egress policy (host not allowlisted).\n"
            "  Run this in CI / on your machine, not in the agent sandbox."
        )
    r.raise_for_status()
    return r.json()


def fetch_series(symbol: str, tf: str, key: str) -> List[Dict]:
    """One symbol, one timeframe → list of {t,o,h,l,c} oldest-first."""
    data = _get("/time_series", {
        "symbol": symbol,
        "interval": INTERVALS[tf],
        "outputsize": OUTPUTSIZE[tf],
        "apikey": key,
        "timezone": "UTC",
        "order": "ASC",
        # Twelve Data returns split/dividend-adjusted values for equities by
        # default; keeping this explicit documents the intent.
        "adjust": "all",
    })
    if data.get("status") == "error":
        raise RuntimeError(f"{symbol} {tf}: {data.get('message')}")
    out = []
    for row in data.get("values", []):
        try:
            out.append({
                "t": row["datetime"],
                "o": float(row["open"]),
                "h": float(row["high"]),
                "l": float(row["low"]),
                "c": float(row["close"]),
            })
        except (KeyError, ValueError):
            continue
    return out


def fetch_earnings_dates(symbol: str, key: str) -> List[str]:
    """Report dates (YYYY-MM-DD) for the earnings-gap blackout. Best-effort."""
    try:
        data = _get("/earnings", {"symbol": symbol, "apikey": key, "outputsize": 12})
        return [row["date"] for row in data.get("earnings", []) if row.get("date")]
    except SystemExit:
        raise
    except Exception:
        return []


def main():
    ap = argparse.ArgumentParser(description="Fetch US-equity OHLC for the swing pilot.")
    ap.add_argument("--output", default="equity-ohlc.json")
    ap.add_argument("--earnings-output", default="equity-earnings.json")
    ap.add_argument("--no-earnings", action="store_true", help="skip the earnings-date pull")
    args = ap.parse_args()

    key = _require_key()
    pairs: Dict[str, Dict[str, List[Dict]]] = {}
    earnings: Dict[str, List[str]] = {}

    for pk, sym in US_TOP5.items():
        pairs[pk] = {}
        for tf in ("daily", "h1"):
            print(f"  {sym:<5} {tf} …", flush=True)
            pairs[pk][tf] = fetch_series(sym, tf, key)
            time.sleep(8.0)   # free-tier rate limit: 8 req/min → 1 req / 7.5s
        if not args.no_earnings:
            earnings[pk] = fetch_earnings_dates(sym, key)
            time.sleep(8.0)

    with open(args.output, "w") as f:
        json.dump({"granularities": ["h1", "daily"], "pairs": pairs}, f)
    print(f"wrote {args.output} ({len(pairs)} symbols)")

    if not args.no_earnings and any(earnings.values()):
        with open(args.earnings_output, "w") as f:
            json.dump(earnings, f, indent=2)
        print(f"wrote {args.earnings_output}")


if __name__ == "__main__":
    main()
