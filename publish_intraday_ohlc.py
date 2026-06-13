#!/usr/bin/env python3
"""
publish_intraday_ohlc.py — Intraday 15m OHLC publisher (v3).

CHANGES FROM v2:
    - Fetches REAL M15 OHLC candles instead of reconstructing bars from
      sparse price polls. v2 polled the pricing endpoint a couple of
      times per run and accumulated those few samples into a "bar" — so
      every bar's high/low was the min/max of ~3 numbers and frequently
      produced artifact "no-wick" candles, which fed false 15m triggers.
      v3 pulls proper M15 candles (OANDA /candles, Coinbase /candles) so
      the OHLC is exchange-accurate.
    - No tick-accumulation state machine. Each run fetches the last ~97
      M15 candles directly and writes them. --state is accepted but
      unused; --loop/--interval are accepted but ignored (candles are
      authoritative — re-polling within a run returns the same data).

USAGE:
    python publish_intraday_ohlc.py --output intraday-ohlc.json

DEPENDENCIES:
    pip install requests

ENVIRONMENT:
    OANDA_TOKEN — OANDA v20 practice token (required for FX/index/commodity pairs)
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
# All FX, commodity, and index pairs use OANDA. Crypto uses Coinbase.
# Must stay in sync with fetch-prices.js OANDA_PAIRS / COINBASE_PAIRS and
# with fetch_historical_ohlc.py PAIRS.
PAIRS = {
    # FX majors
    "eurusd":  {"oanda": "EUR_USD"},
    "gbpusd":  {"oanda": "GBP_USD"},
    "usdjpy":  {"oanda": "USD_JPY"},
    "usdcad":  {"oanda": "USD_CAD"},
    "usdchf":  {"oanda": "USD_CHF"},
    "audusd":  {"oanda": "AUD_USD"},
    "nzdusd":  {"oanda": "NZD_USD"},
    # FX crosses (existing)
    "cadjpy":  {"oanda": "CAD_JPY"},
    "eurnzd":  {"oanda": "EUR_NZD"},
    # gbpaud removed 2026-06-10h — chronic ~50% WR.
    # Re-add: '"gbpaud": {"oanda": "GBP_AUD"},'
    "euraud":  {"oanda": "EUR_AUD"},
    "usdsgd":  {"oanda": "USD_SGD"},
    "audnzd":  {"oanda": "AUD_NZD"},
    # audchf removed 2026-06-08 — low win-rate drag on aggregate.
    # Re-add: '"audchf": {"oanda": "AUD_CHF"},'
    "eurgbp":  {"oanda": "EUR_GBP"},
    # FX additions (v5)
    # audcad removed 2026-06-10i — low win-rate drag.
    # Re-add: '"audcad": {"oanda": "AUD_CAD"},'
    "gbpcad":  {"oanda": "GBP_CAD"},
    "nzdjpy":  {"oanda": "NZD_JPY"},
    # usdnok removed 2026-06-08 — low win-rate drag. Re-add: '"usdnok": {"oanda": "USD_NOK"},'
    "gbpnzd":  {"oanda": "GBP_NZD"},
    # eursek removed 2026-06-08 — low win-rate drag. Re-add: '"eursek": {"oanda": "EUR_SEK"},'
    # v7 additions (2026-06-03 — minors)
    # nzdcad removed 2026-06-10 — low win-rate drag. Re-add: '"nzdcad": {"oanda": "NZD_CAD"},'
    "eurnok":  {"oanda": "EUR_NOK"},
    "nzdchf":  {"oanda": "NZD_CHF"},
    # gbpchf removed 2026-06-10 — low win-rate drag. Re-add: '"gbpchf": {"oanda": "GBP_CHF"},'
    "usdzar":  {"oanda": "USD_ZAR"},
    # usdcnh removed 2026-06-10 — low win-rate drag. Re-add: '"usdcnh": {"oanda": "USD_CNH"},'
    "eursgd":  {"oanda": "EUR_SGD"},
    # Commodities
    "xauusd":  {"oanda": "XAU_USD"},
    "xagusd":  {"oanda": "XAG_USD"},
    "usoil":   {"oanda": "BCO_USD"},
    "wtiusd":  {"oanda": "WTICO_USD"},   # WTI Crude (added 2026-06-10)
    "natgas":  {"oanda": "NATGAS_USD"},  # Natural Gas (Henry Hub)
    "xptusd":  {"oanda": "XPT_USD"},     # Platinum
    # Equity indices
    # de40 removed 2026-06-13kk — chronic negative E[R].
    "ftse100": {"oanda": "UK100_GBP"},
    "dj30":    {"oanda": "US30_USD"},
    "nas100":  {"oanda": "NAS100_USD"},
    "spx500":  {"oanda": "SPX500_USD"},  # was missing — present in MKTS, fetch-prices.js, fetch_historical_ohlc.py
    # v7 additions (2026-06-03 — indices)
    "jp225":   {"oanda": "JP225_USD"},
    # fra40 (CAC 40) removed 2026-06-10 — low win-rate drag.
    # Re-add: '"fra40": {"oanda": "FR40_EUR"},'
    # esp35 (IBEX 35) removed 2026-06-08 — OANDA practice endpoint
    # rejected both ES35_EUR and ESP35_EUR.
    # Crypto
    "btcusd":  {"coinbase": "BTC-USD"},
    "suiusd":  {"coinbase": "SUI-USD"},
    "ethusd":  {"coinbase": "ETH-USD"},
    "solusd":  {"coinbase": "SOL-USD"},
    "xrpusd":  {"coinbase": "XRP-USD"},
    "taousd":  {"coinbase": "TAO-USD"},
    "nearusd": {"coinbase": "NEAR-USD"},
    # hypeusd removed 2026-06-10 — low win-rate drag.
    # Re-add: '"hypeusd": {"coinbase": "HYPE-USD"},'
    "ondousd": {"coinbase": "ONDO-USD"},
    # ltcusd removed 2026-06-10 — low win-rate drag.
    # Re-add: '"ltcusd": {"coinbase": "LTC-USD"},'
}

WINDOW_MINUTES = 15
WINDOWS_TO_KEEP = 96
CANDLE_COUNT = WINDOWS_TO_KEEP + 1   # 96 closed bars + 1 in-progress
COINBASE_GRANULARITY = 900           # 15 minutes, in seconds


class OandaAuthError(Exception):
    """Raised when OANDA returns 401/403 — token has likely expired."""
    pass


# ── Timestamp helpers ──────────────────────────────────────────────
# Output bar timestamps must match the format the dashboard and
# detect_triggers.py already expect: "YYYY-MM-DDTHH:MM:SS+00:00".

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _parse_oanda_time(t: str) -> str:
    """OANDA candle time ('2026-05-20T15:00:00.000000000Z') -> normalised ISO.

    fromisoformat can't handle 9-digit fractional seconds, so trim them.
    """
    base = t.split(".")[0].rstrip("Z")
    dt = datetime.fromisoformat(base).replace(tzinfo=timezone.utc)
    return _iso(dt)


# ── OANDA M15 candles ──────────────────────────────────────────────
# The instrument candles endpoint is account-independent — it needs only
# the bearer token, so no account-id discovery is required.

def fetch_oanda_candles(instrument: str, api_key: str) -> list | None:
    """Fetch the last CANDLE_COUNT M15 mid candles for one instrument.

    Returns a list of {t,o,h,l,c,p} bars (oldest first), or None on a
    non-auth failure. Raises OandaAuthError on 401/403 so the caller can
    refuse to overwrite and preserve existing data.
    """
    url = f"https://api-fxpractice.oanda.com/v3/instruments/{instrument}/candles"
    params = {"granularity": "M15", "count": CANDLE_COUNT, "price": "M"}
    for attempt in range(3):
        try:
            r = requests.get(
                url, params=params,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15,
            )
            if r.status_code in (401, 403):
                msg = f"OANDA auth failed: HTTP {r.status_code} — token likely expired"
                print(f"FATAL: {msg}", file=sys.stderr)
                raise OandaAuthError(msg)
            r.raise_for_status()
            bars = []
            for c in r.json().get("candles", []):
                m = c.get("mid")
                if not m:
                    continue
                try:
                    o, h = float(m["o"]), float(m["h"])
                    lo, cl = float(m["l"]), float(m["c"])
                except (KeyError, ValueError):
                    continue
                bars.append({
                    "t": _parse_oanda_time(c.get("time", "")),
                    "o": o, "h": h, "l": lo, "c": cl, "p": cl,
                })
            return bars
        except OandaAuthError:
            raise
        except (requests.Timeout, requests.ConnectionError) as exc:
            wait = 2 ** attempt
            print(f"WARN oanda {instrument} transient (attempt {attempt+1}/3): {exc}",
                  file=sys.stderr)
            if attempt < 2:
                time.sleep(wait)
        except (requests.RequestException, ValueError, KeyError) as exc:
            print(f"WARN oanda {instrument}: {exc}", file=sys.stderr)
            return None
    return None


# ── Coinbase M15 candles ───────────────────────────────────────────

def fetch_coinbase_candles(product: str) -> list | None:
    """Fetch ~CANDLE_COUNT 15m candles for one Coinbase product.

    Coinbase returns [[time, low, high, open, close, volume], ...] newest
    first. Returns {t,o,h,l,c,p} bars (oldest first), or None on failure.
    """
    url = f"https://api.exchange.coinbase.com/products/{product}/candles"
    for attempt in range(3):
        try:
            r = requests.get(url, params={"granularity": COINBASE_GRANULARITY},
                             timeout=10)
            r.raise_for_status()
            rows = r.json()
            if not isinstance(rows, list):
                return None
            rows.sort(key=lambda x: x[0])   # oldest first
            bars = []
            for row in rows[-CANDLE_COUNT:]:
                try:
                    ts = int(row[0])
                    lo, hi = float(row[1]), float(row[2])
                    op, cl = float(row[3]), float(row[4])
                except (IndexError, ValueError, TypeError):
                    continue
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                bars.append({
                    "t": _iso(dt),
                    "o": op, "h": hi, "l": lo, "c": cl, "p": cl,
                })
            return bars
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            print(f"WARN coinbase {product}: {exc}", file=sys.stderr)
            return None
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            print(f"WARN coinbase {product}: {exc}", file=sys.stderr)
            return None
    return None


def fetch_all_candles() -> dict:
    """Return {pair_key: [bars]} for every pair we can fetch.

    Raises OandaAuthError if OANDA rejects the token (propagated so the
    caller refuses to overwrite the existing output).
    """
    oanda_key = os.environ.get("OANDA_TOKEN") or os.environ.get("OANDA_API_KEY")
    out = {}
    for key, src in PAIRS.items():
        bars = None
        if "oanda" in src:
            if oanda_key:
                bars = fetch_oanda_candles(src["oanda"], oanda_key)
        elif "coinbase" in src:
            bars = fetch_coinbase_candles(src["coinbase"])
        if bars:
            out[key] = bars[-CANDLE_COUNT:]
    return out


# ── Main ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="intraday-state.json",
                        help="Accepted for workflow compatibility; unused in v3.")
    parser.add_argument("--output", default="intraday-ohlc.json")
    parser.add_argument("--loop", type=int, default=1,
                        help="Accepted for compatibility; ignored "
                             "(real candles are authoritative — no self-polling).")
    parser.add_argument("--interval", type=int, default=60,
                        help="Accepted for compatibility; ignored.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-pairs", type=int, default=15,
                        help="Minimum pairs that must return candles, otherwise "
                             "refuse to overwrite the output and preserve "
                             "existing data. Set to 0 to disable. Default 15.")
    args = parser.parse_args()

    output_path = Path(args.output)

    try:
        intraday = fetch_all_candles()
    except OandaAuthError as exc:
        print(f"FATAL OANDA auth failure: {exc}", file=sys.stderr)
        print("Refusing to overwrite output to preserve existing data.", file=sys.stderr)
        print("ACTION REQUIRED: rotate OANDA_TOKEN in repo Settings → Secrets.",
              file=sys.stderr)
        sys.exit(1)

    print(f"Fetched {len(intraday)} pairs", flush=True)

    # Refuse-to-overwrite guard: a partial fetch (e.g. OANDA outage) must
    # not clobber a good intraday-ohlc.json with crypto-only data.
    if args.min_pairs > 0 and len(intraday) < args.min_pairs:
        print(f"WARN: only {len(intraday)} pairs fetched (min required: "
              f"{args.min_pairs}). Preserving existing output.", file=sys.stderr)
        sys.exit(1)

    output = {
        "updated": _iso(datetime.now(timezone.utc)),
        "ohlc": True,
        "intraday": intraday,
    }
    if args.dry_run:
        print(json.dumps(output, indent=2))
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, indent=2))
        print(f"Wrote {len(intraday)} pairs to {output_path}")


if __name__ == "__main__":
    main()
