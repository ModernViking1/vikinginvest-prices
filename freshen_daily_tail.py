"""Freshen ONLY the daily tail of historical-ohlc.json — required by cam_rev (and any
daily-levels strategy) in the swing feed.

Why this exists
---------------
The swing feed (swing_signals.py) reads historical-ohlc.json. freshen_h1_tail.py keeps the
H1 layer current, but leaves the DAILY layer to the 6-hourly full backfill — so the daily
bars lag 1-2 days. cam_rev derives its Camarilla pivot levels from the PRIOR day's daily bar,
so a stale daily layer means the most recent days have no levels -> cam_rev produces no
signals inside the feed's 24h freshness window -> nothing reaches the cBot -> no trades.
(Confirmed 2026-09-19: cam_rev had 0 emitted despite fresh m15, because daily lagged to 09-16.)

This does the cheap fix: fetch the last few days of DAILY candles per pair from the SAME
source (OANDA / Coinbase) and convention as the dataset, and splice them onto each pair's
'daily' array. h1 / m15 and the long history are untouched.

Safety (mirrors freshen_h1_tail): only the 'daily' layer changes; dedup by bar time; never
shrink a pair's history; fail-open per pair and overall. The freshened file is discarded by
the workflow after the feed runs — the 6-hourly backfill stays the committed source of truth.

Run:  OANDA_TOKEN=... python freshen_daily_tail.py  [--days 10] [--file historical-ohlc.json]
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from fetch_historical_ohlc import PAIRS, fetch_oanda_candles, fetch_coinbase_candles

DEFAULT_DAYS = 10         # daily lookback — a handful of recent complete bars is plenty
MIN_KEEP_RATIO = 0.98     # never let a pair's daily history shrink


def _splice(existing, fresh):
    """Merge fresh daily bars onto existing, dedup by bar time 't', keep chronological.
    Fresh bars with a matching 't' finalise the stored one; existing bars are never dropped."""
    by_t = {b["t"]: b for b in existing if "t" in b}
    before = len(by_t)
    for b in fresh:
        if "t" in b:
            by_t[b["t"]] = b
    merged = [by_t[t] for t in sorted(by_t)]
    return merged, len(by_t) - before


def freshen(path, days):
    if not os.path.exists(path):
        print(f"::error::{path} not found — nothing to freshen", flush=True)
        return 1
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    pairs = doc.get("pairs")
    if not isinstance(pairs, dict) or not pairs:
        print(f"::error::{path} has no pairs — leaving untouched", flush=True)
        return 1

    now = datetime.now(timezone.utc)
    frm = now - timedelta(days=days)
    token = os.environ.get("OANDA_TOKEN", "").strip() or None

    total_added = 0
    refreshed = 0
    for pk, layers in pairs.items():
        cfg = PAIRS.get(pk)
        if not cfg or not isinstance(layers, dict) or "daily" not in layers:
            continue
        existing = layers.get("daily") or []
        try:
            if "oanda" in cfg:
                if not token:
                    continue
                fresh = fetch_oanda_candles(token, cfg["oanda"], "D", frm, now)
            elif "coinbase" in cfg:
                fresh = fetch_coinbase_candles(cfg["coinbase"], 86400, frm, now)
            else:
                continue
        except Exception as e:
            print(f"  {pk}: daily fetch failed ({e}) — leaving as-is", flush=True)
            continue

        if not fresh:
            print(f"  {pk}: no fresh daily bars returned — leaving as-is", flush=True)
            continue
        merged, added = _splice(existing, fresh)
        if len(merged) < len(existing) * MIN_KEEP_RATIO:
            print(f"  {pk}: splice would shrink daily {len(existing)}->{len(merged)} — skipped", flush=True)
            continue
        layers["daily"] = merged
        total_added += added
        refreshed += 1
        newest = merged[-1]["t"] if merged else "?"
        print(f"  {pk}: daily {len(existing)}->{len(merged)} (+{added}) newest {newest}", flush=True)

    if refreshed == 0:
        print("no pairs refreshed (no token / no data) — file unchanged", flush=True)
        return 0

    doc["daily_tail_refreshed"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    os.replace(tmp, path)
    print(f"freshened daily tail on {refreshed} pair(s), +{total_added} bars total", flush=True)
    return 0


def main():
    ap = argparse.ArgumentParser(description="Freshen only the daily tail of historical-ohlc.json")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS, help="daily lookback days to fetch")
    ap.add_argument("--file", default="historical-ohlc.json", help="target OHLC file")
    args = ap.parse_args()
    try:
        return freshen(args.file, args.days)
    except Exception as e:
        print(f"::error::freshen_daily_tail failed ({e}) — file left unchanged", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
