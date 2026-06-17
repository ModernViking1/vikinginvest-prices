#!/usr/bin/env python3
"""
fetch_historical_ohlc.py — One-shot historical OHLC backfill (Option D)

PURPOSE
    Fetches deep historical 1H and 15M OHLC data for all 22 pairs from OANDA
    and Coinbase. Designed to run once via GitHub Actions manual-dispatch,
    producing a CDN-distributable JSON that the Viking Invest Trading
    dashboard reads to backtest 4/4 confluence over 12 months.

OUTPUT STRUCTURE
    historical-ohlc.json  (or per-pair files if --per-pair)

    Schema (combined):
    {
      "generated": "2026-05-11T06:00:00Z",
      "window_days": 365,
      "granularities": ["m15", "h1", "daily"],
      "pairs": {
        "eurusd": {
          "m15": [{"t": "...", "o":..., "h":..., "l":..., "c":...}, ...],
          "h1":  [{"t": "...", "o":..., "h":..., "l":..., "c":...}, ...],
          "daily": [{"t": "...", "o":..., "h":..., "l":..., "c":...}, ...]
        },
        ...
      }
    }

    Per-pair files: historical-ohlc-eurusd.json (same shape, single pair)

USAGE
    # 12-month full backfill, combined output
    python fetch_historical_ohlc.py --days 365 --output historical-ohlc.json

    # 6-month backfill, per-pair output
    python fetch_historical_ohlc.py --days 180 --per-pair --output-dir ohlc/

    # Hybrid mode: last 60 days at 15m, older period only at 1H
    python fetch_historical_ohlc.py --days 365 --m15-days 60 --output historical-ohlc.json

CONSTRAINTS
    OANDA returns max 5000 bars per request. We chunk by date range.
    Coinbase returns max 300 candles per request. More chunking needed.
    Approximate fetch time at full 365 days × 22 pairs:
        ~150 API calls × ~500ms latency each = ~75 seconds optimistic,
        2-3 minutes realistic including retries/backoff.

DEPENDENCIES
    pip install requests

ENVIRONMENT
    OANDA_TOKEN — required for FX/commodity/index pairs
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional, Any

import requests

# ── Pair configuration ──────────────────────────────────────────────
# Same 22 pairs as the live intraday bridge for consistency.
PAIRS: Dict[str, Dict[str, str]] = {
    # Must stay in sync with publish_intraday_ohlc.py + fetch-prices.js.
    "eurusd":  {"oanda": "EUR_USD"},
    "gbpusd":  {"oanda": "GBP_USD"},
    "usdjpy":  {"oanda": "USD_JPY"},
    "usdcad":  {"oanda": "USD_CAD"},
    "usdchf":  {"oanda": "USD_CHF"},
    "audusd":  {"oanda": "AUD_USD"},
    "nzdusd":  {"oanda": "NZD_USD"},
    "cadjpy":  {"oanda": "CAD_JPY"},
    "eurnzd":  {"oanda": "EUR_NZD"},
    # gbpaud removed 2026-06-10h — chronic ~50% WR (see backtest drill-down).
    # Re-add: '"gbpaud": {"oanda": "GBP_AUD"},'
    "euraud":  {"oanda": "EUR_AUD"},
    "usdsgd":  {"oanda": "USD_SGD"},
    "audnzd":  {"oanda": "AUD_NZD"},
    # audchf removed 2026-06-08 — low win-rate drag on aggregate.
    # Re-add: '"audchf": {"oanda": "AUD_CHF"},'
    "eurgbp":  {"oanda": "EUR_GBP"},
    # v5 additions
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
    # de40 REINSTATED 2026-06-15lll — see detect_triggers.py FIB_ENTRY_PAIRS
    # comment for rationale (inferred bear seed retired client-side).
    "de40":    {"oanda": "DE30_EUR"},
    "ftse100": {"oanda": "UK100_GBP"},
    # dj30 reinstated 2026-06-17 alongside the index-MACD gate deploys.
    "dj30":    {"oanda": "US30_USD"},
    "nas100":  {"oanda": "NAS100_USD"},
    "spx500":  {"oanda": "SPX500_USD"},
    # v7 additions (2026-06-03 — indices)
    "jp225":   {"oanda": "JP225_USD"},   # Nikkei 225
    # fra40 (CAC 40) removed 2026-06-10 — low win-rate drag.
    # Re-add: '"fra40": {"oanda": "FR40_EUR"},'
    # IBEX 35 (esp35) removed 2026-06-08 — OANDA practice endpoint
    # rejected both ES35_EUR and ESP35_EUR. Re-add when the correct
    # ticker is confirmed (ESPIX_EUR is a candidate).
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
    # ondousd dropped 2026-06-17 (user). Re-add: '"ondousd": {"coinbase": "ONDO-USD"},'
    # ltcusd removed 2026-06-10 — low win-rate drag.
    # Re-add: '"ltcusd": {"coinbase": "LTC-USD"},'
}

# OANDA granularity codes
OANDA_GRANULARITY = {
    "m15": "M15",
    "h1": "H1",
    "daily": "D",
}

# Approximate bar count per day (helps chunk planning)
BARS_PER_DAY = {
    "m15": 96,   # 24 hours × 4
    "h1": 24,
    "daily": 1,
}

# OANDA API limits
OANDA_MAX_BARS = 5000
COINBASE_MAX_CANDLES = 300

# ── HTTP helpers ────────────────────────────────────────────────────
def http_get_with_retry(url: str, headers: Dict[str, str] = None,
                        params: Dict[str, Any] = None,
                        max_retries: int = 3) -> Optional[requests.Response]:
    """GET with exponential backoff. Returns None on persistent failure."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                return r
            elif r.status_code == 429:
                # Rate limited — back off aggressively
                wait = 2 ** attempt + 5
                print(f"  Rate limited, sleeping {wait}s...", flush=True)
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code}: {r.text[:200]}", flush=True)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        except requests.RequestException as e:
            print(f"  Request error: {e}", flush=True)
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    return None


# ── OANDA fetcher ───────────────────────────────────────────────────
def fetch_oanda_candles(token: str, instrument: str, granularity: str,
                         from_ts: datetime, to_ts: datetime,
                         max_bars: int = OANDA_MAX_BARS) -> List[Dict[str, Any]]:
    """
    Fetch OHLC candles from OANDA in chunks. Returns chronological list.

    OANDA's /candles endpoint accepts either:
      - count=N (returns N bars ending at 'to')
      - from + to (returns all bars in range, up to 5000)
    We use from/to chunking to control window precisely.
    """
    url = f"https://api-fxpractice.oanda.com/v3/instruments/{instrument}/candles"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Datetime-Format": "RFC3339",
    }
    all_bars: List[Dict[str, Any]] = []

    # Compute chunk size: ~5000 bars per request, but stay well under limit.
    # For m15: 4000 bars = ~41 days per chunk
    # For h1:  4000 bars = ~166 days per chunk (whole period fits)
    # For D:   4000 bars covers years
    bars_per_day = BARS_PER_DAY[
        {"M15": "m15", "H1": "h1", "D": "daily"}[granularity]
    ]
    chunk_days = max(1, (max_bars - 200) // max(1, bars_per_day))

    cursor = from_ts
    while cursor < to_ts:
        chunk_end = min(cursor + timedelta(days=chunk_days), to_ts)
        params = {
            "granularity": granularity,
            "from": cursor.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to":   chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "price": "M",
        }
        r = http_get_with_retry(url, headers=headers, params=params)
        if not r:
            print(f"  WARN: skipped chunk {cursor} - {chunk_end} (HTTP failed)", flush=True)
            cursor = chunk_end
            continue
        data = r.json()
        candles = data.get("candles", [])
        for c in candles:
            if not c.get("complete", False):
                continue  # skip in-progress bar
            mid = c.get("mid") or {}
            try:
                all_bars.append({
                    "t": c["time"],
                    "o": float(mid["o"]),
                    "h": float(mid["h"]),
                    "l": float(mid["l"]),
                    "c": float(mid["c"]),
                })
            except (KeyError, ValueError, TypeError) as e:
                continue
        cursor = chunk_end
        # Small delay between chunks to be courteous
        time.sleep(0.2)

    return all_bars


# ── Coinbase fetcher (for crypto) ───────────────────────────────────
def fetch_coinbase_candles(product: str, granularity_seconds: int,
                            from_ts: datetime, to_ts: datetime) -> List[Dict[str, Any]]:
    """
    Fetch OHLC from Coinbase Exchange API. Returns chronological list.

    Coinbase returns max 300 candles per request. We chunk by date.
    Granularity in seconds: 60, 300, 900, 3600, 21600, 86400.
    """
    url = f"https://api.exchange.coinbase.com/products/{product}/candles"
    all_bars: List[Dict[str, Any]] = []

    # 300 candles per request × granularity seconds = chunk window
    chunk_seconds = (COINBASE_MAX_CANDLES - 10) * granularity_seconds
    chunk_delta = timedelta(seconds=chunk_seconds)

    cursor = from_ts
    while cursor < to_ts:
        chunk_end = min(cursor + chunk_delta, to_ts)
        params = {
            "start": cursor.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end":   chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "granularity": granularity_seconds,
        }
        r = http_get_with_retry(url, params=params)
        if not r:
            cursor = chunk_end
            continue
        # Coinbase returns [[time, low, high, open, close, volume], ...]
        # in REVERSE chronological order
        try:
            data = r.json()
            if isinstance(data, list):
                for row in data:
                    if len(row) >= 5:
                        ts = datetime.fromtimestamp(row[0], tz=timezone.utc)
                        all_bars.append({
                            "t": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "o": float(row[3]),
                            "h": float(row[2]),
                            "l": float(row[1]),
                            "c": float(row[4]),
                        })
        except (ValueError, KeyError, TypeError):
            pass
        cursor = chunk_end
        time.sleep(0.4)  # Coinbase rate limit is tighter

    # Sort chronologically + dedupe by timestamp
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for b in sorted(all_bars, key=lambda x: x["t"]):
        if b["t"] not in seen:
            seen.add(b["t"])
            deduped.append(b)
    return deduped


# ── Per-pair fetch orchestration ────────────────────────────────────
def fetch_pair_history(pair_key: str, pair_cfg: Dict[str, str],
                        days_total: int, m15_days: int,
                        oanda_token: Optional[str]) -> Dict[str, List]:
    """
    Fetches m15, h1, and daily bars for a single pair.

    days_total: full window (e.g. 365 for 12 months)
    m15_days:   how far back to fetch 15m bars. If less than days_total,
                older period is filled with 1H bars only (hybrid mode).
    """
    now = datetime.now(timezone.utc)
    from_full = now - timedelta(days=days_total)
    from_m15  = now - timedelta(days=m15_days)

    result = {"m15": [], "h1": [], "daily": []}

    if "oanda" in pair_cfg:
        if not oanda_token:
            print(f"  {pair_key}: skipped (no OANDA token)", flush=True)
            return result
        instr = pair_cfg["oanda"]
        print(f"  {pair_key}: fetching daily ({days_total}d)...", flush=True)
        result["daily"] = fetch_oanda_candles(oanda_token, instr, "D", from_full, now)
        print(f"    {len(result['daily'])} daily bars", flush=True)
        print(f"  {pair_key}: fetching h1 ({days_total}d)...", flush=True)
        result["h1"] = fetch_oanda_candles(oanda_token, instr, "H1", from_full, now)
        print(f"    {len(result['h1'])} h1 bars", flush=True)
        print(f"  {pair_key}: fetching m15 ({m15_days}d)...", flush=True)
        result["m15"] = fetch_oanda_candles(oanda_token, instr, "M15", from_m15, now)
        print(f"    {len(result['m15'])} m15 bars", flush=True)
    elif "coinbase" in pair_cfg:
        product = pair_cfg["coinbase"]
        print(f"  {pair_key}: fetching daily ({days_total}d)...", flush=True)
        result["daily"] = fetch_coinbase_candles(product, 86400, from_full, now)
        print(f"    {len(result['daily'])} daily bars", flush=True)
        print(f"  {pair_key}: fetching h1 ({days_total}d)...", flush=True)
        result["h1"] = fetch_coinbase_candles(product, 3600, from_full, now)
        print(f"    {len(result['h1'])} h1 bars", flush=True)
        print(f"  {pair_key}: fetching m15 ({m15_days}d)...", flush=True)
        result["m15"] = fetch_coinbase_candles(product, 900, from_m15, now)
        print(f"    {len(result['m15'])} m15 bars", flush=True)

    return result


# ── Main ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=365,
                    help="Total history window in days (default 365)")
    ap.add_argument("--m15-days", type=int, default=None,
                    help="If set, only fetch m15 bars for this many recent days. "
                         "Older period uses only h1 + daily (hybrid mode). "
                         "Defaults to --days (full m15 across whole window).")
    ap.add_argument("--output", default="historical-ohlc.json",
                    help="Output file (combined mode)")
    ap.add_argument("--per-pair", action="store_true",
                    help="Write one file per pair instead of combined")
    ap.add_argument("--output-dir", default=".",
                    help="Output directory (used with --per-pair)")
    ap.add_argument("--pairs", default=None,
                    help="Comma-separated subset of pair keys (for testing)")
    ap.add_argument("--merge", action="store_true",
                    help="Merge with existing --output file: pairs not in the "
                         "--pairs subset are copied from the existing file "
                         "rather than dropped. Lets you deep-fetch a small "
                         "subset (e.g. just new crypto) without losing the "
                         "other 30+ pairs already in historical-ohlc.json.")
    args = ap.parse_args()

    m15_days = args.m15_days if args.m15_days is not None else args.days
    if m15_days > args.days:
        m15_days = args.days

    oanda_token = os.environ.get("OANDA_TOKEN", "").strip() or None
    if not oanda_token:
        print("WARN: OANDA_TOKEN not set — FX/commodity/index pairs will be empty", flush=True)

    # Subset selection
    pairs_to_fetch = list(PAIRS.keys())
    if args.pairs:
        wanted = set(p.strip() for p in args.pairs.split(","))
        pairs_to_fetch = [p for p in pairs_to_fetch if p in wanted]

    print(f"Historical OHLC backfill starting", flush=True)
    print(f"  Window: {args.days} days ({m15_days} days at m15 granularity)", flush=True)
    print(f"  Pairs: {len(pairs_to_fetch)} ({', '.join(pairs_to_fetch[:5])}{'...' if len(pairs_to_fetch) > 5 else ''})", flush=True)
    print(f"  Mode: {'per-pair files' if args.per_pair else 'combined output'}", flush=True)
    print("", flush=True)

    start_time = time.time()
    output_data: Dict[str, Any] = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": args.days,
        "m15_days": m15_days,
        "granularities": ["m15", "h1", "daily"],
        "pairs": {},
    }

    out_dir = Path(args.output_dir)
    if args.per_pair:
        out_dir.mkdir(parents=True, exist_ok=True)

    # Merge mode: read existing output file FIRST, copy through every
    # pair that ISN'T in our fetch subset so the final combined file
    # still has full coverage. Pairs in the fetch subset will be
    # overwritten below with freshly-fetched data.
    if args.merge and not args.per_pair:
        existing_path = Path(args.output)
        if existing_path.exists():
            try:
                with open(existing_path, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
                existing_pairs = existing.get('pairs', {}) if isinstance(existing, dict) else {}
                preserved = 0
                fetch_set = set(pairs_to_fetch)
                for k, v in existing_pairs.items():
                    if k not in fetch_set:
                        output_data['pairs'][k] = v
                        preserved += 1
                print(f"  Merge: preserved {preserved} existing pairs from {existing_path}", flush=True)
            except Exception as e:
                print(f"  WARN: merge read failed ({e}); proceeding without merge", flush=True)
        else:
            print(f"  Merge: no existing {existing_path} to merge from", flush=True)

    for idx, pair_key in enumerate(pairs_to_fetch, 1):
        print(f"[{idx}/{len(pairs_to_fetch)}] {pair_key}", flush=True)
        try:
            pair_data = fetch_pair_history(
                pair_key, PAIRS[pair_key],
                args.days, m15_days, oanda_token
            )
        except Exception as e:
            print(f"  ERROR: {e}", flush=True)
            pair_data = {"m15": [], "h1": [], "daily": []}

        if args.per_pair:
            # Write individual file
            per_pair_path = out_dir / f"historical-ohlc-{pair_key}.json"
            per_pair_doc = {
                "generated": output_data["generated"],
                "window_days": args.days,
                "m15_days": m15_days,
                "pair": pair_key,
                **pair_data,
            }
            with open(per_pair_path, "w") as f:
                json.dump(per_pair_doc, f, separators=(",", ":"))
            size_kb = per_pair_path.stat().st_size / 1024
            print(f"  → {per_pair_path.name} ({size_kb:.1f} KB)", flush=True)
        else:
            output_data["pairs"][pair_key] = pair_data

    elapsed = time.time() - start_time
    print("", flush=True)
    print(f"Elapsed: {elapsed:.1f}s", flush=True)

    if not args.per_pair:
        out_path = Path(args.output)
        with open(out_path, "w") as f:
            json.dump(output_data, f, separators=(",", ":"))
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"Combined output: {out_path} ({size_mb:.2f} MB)", flush=True)

    # Sanity summary
    total_bars = sum(
        len(pair_data.get(g, []))
        for pair_data in (
            output_data["pairs"].values() if not args.per_pair else []
        )
        for g in ("m15", "h1", "daily")
    )
    if not args.per_pair:
        print(f"Total bars across all pairs: {total_bars:,}", flush=True)


if __name__ == "__main__":
    main()
