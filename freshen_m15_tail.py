"""Freshen ONLY the m15 tail of historical-ohlc.json — the layer the swing feed's
m15-triggered strategies (cam_rev and the crypto detectors) actually fire on.

Why this exists
---------------
The swing feed (swing_signals.py -> swing-signals.json) reads historical-ohlc.json, rebuilt
in full only on a 6-HOURLY cron. freshen_h1_tail.py keeps the H1 layer current and
freshen_daily_tail.py keeps the DAILY layer current — but BOTH deliberately leave m15 to the
6-hourly backfill. That was fine while swing setups triggered on H1. It is NOT fine for the
strategies that trigger on the m15 bar itself: cam_rev's rejection candle, and the 24/7
crypto detectors (twob / engulf_manip / mmove on btc/eth/xrp/sol). Their m15 goes 0-6h stale
between backfills, so every signal they emit is BORN hours old — already past the swing cBot's
entry-age guard (MaxSignalAgeMin, 120 min) by the time it reaches the bot. Result: crypto (and
any m15 swing setup) never fills, even when the setup is valid.
(Confirmed 2026-09-19: crypto m15 lagged to ~10:45 while the feed ran at 13:00, so the newest
btc/xrp signals were 8-16h old and the 2h cap rejected all of them — no crypto trades.)

Crypto is the acute case because it trades 24/7: FX/index/comm m15 only matters in-session and
those markets are closed on weekends (a weekend fetch just returns nothing and leaves the pair
untouched), whereas crypto keeps printing bars the backfill can't keep up with. This script
does the cheap half of the backfill on the feed's tight cadence: fetch the last day or two of
m15 per pair from the SAME source (OANDA / Coinbase) and convention as the dataset, and splice
them onto each pair's 'm15' array. h1 / daily and the long history are untouched.

Safety (mirrors freshen_h1_tail / freshen_daily_tail): only the 'm15' layer changes; dedup by
bar time; never shrink a pair's history (a short fetch leaves the pair exactly as it was);
fail-open per pair AND overall so it can never break the swing feed or the 6-hourly rebuild
that stays the committed source of truth. The freshened file is discarded after the feed runs.

Run:  OANDA_TOKEN=... python freshen_m15_tail.py  [--days 2] [--file historical-ohlc.json]
      [--skip-if-fresh-min 12]
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from fetch_historical_ohlc import PAIRS, fetch_oanda_candles, fetch_coinbase_candles

DEFAULT_DAYS = 2          # m15 lookback each cycle — covers the max backfill gap with margin
MIN_KEEP_RATIO = 0.98     # never let a pair's m15 history shrink below this fraction


def _splice(existing, fresh):
    """Merge fresh m15 bars onto existing, dedup by bar time 't', keep chronological.
    Fresh bars with a matching 't' finalise the stored one; existing bars are never dropped."""
    by_t = {b["t"]: b for b in existing if "t" in b}
    before = len(by_t)
    for b in fresh:
        if "t" in b:
            by_t[b["t"]] = b
    merged = [by_t[t] for t in sorted(by_t)]
    return merged, len(by_t) - before


def _newest_m15_age_min(pairs, now):
    """Minutes since the newest m15 bar across all pairs (None if none)."""
    newest = None
    for layers in pairs.values():
        if isinstance(layers, dict):
            m15 = layers.get("m15") or []
            if m15 and "t" in m15[-1] and (newest is None or m15[-1]["t"] > newest):
                newest = m15[-1]["t"]
    if not newest:
        return None
    try:
        dt = datetime.strptime(newest[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (now - dt).total_seconds() / 60.0


def freshen(path, days, skip_if_fresh_min=0):
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
    # Freshness gate: m15 bars close every 15 min, so re-fetching within the same bar just
    # re-pulls completed bars. Skip when the newest bar is younger than the gate.
    if skip_if_fresh_min > 0:
        age = _newest_m15_age_min(pairs, now)
        if age is not None and age < skip_if_fresh_min:
            print(f"m15 already fresh (newest {age:.0f} min old < {skip_if_fresh_min}) — skipping fetch", flush=True)
            return 0

    token = os.environ.get("OANDA_TOKEN", "").strip() or None
    frm = now - timedelta(days=days)

    total_added = 0
    refreshed = 0
    for pk, layers in pairs.items():
        cfg = PAIRS.get(pk)
        if not cfg or not isinstance(layers, dict) or "m15" not in layers:
            continue
        existing = layers.get("m15") or []
        try:
            if "oanda" in cfg:
                if not token:
                    continue
                fresh = fetch_oanda_candles(token, cfg["oanda"], "M15", frm, now)
            elif "coinbase" in cfg:
                fresh = fetch_coinbase_candles(cfg["coinbase"], 900, frm, now)
            else:
                continue
        except Exception as e:
            print(f"  {pk}: m15 fetch failed ({e}) — leaving as-is", flush=True)
            continue

        if not fresh:
            print(f"  {pk}: no fresh m15 bars returned — leaving as-is", flush=True)
            continue
        merged, added = _splice(existing, fresh)
        # Never shrink history: a good splice can only keep or grow the bar count.
        if len(merged) < len(existing) * MIN_KEEP_RATIO:
            print(f"  {pk}: splice would shrink m15 {len(existing)}->{len(merged)} — skipped", flush=True)
            continue
        layers["m15"] = merged
        total_added += added
        refreshed += 1
        newest = merged[-1]["t"] if merged else "?"
        print(f"  {pk}: m15 {len(existing)}->{len(merged)} (+{added}) newest {newest}", flush=True)

    if refreshed == 0:
        print("no pairs refreshed (no token / no data) — file unchanged", flush=True)
        return 0

    doc["m15_tail_refreshed"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    os.replace(tmp, path)
    print(f"freshened m15 tail on {refreshed} pair(s), +{total_added} bars total", flush=True)
    return 0


def main():
    ap = argparse.ArgumentParser(description="Freshen only the m15 tail of historical-ohlc.json")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS, help="m15 lookback days to fetch")
    ap.add_argument("--file", default="historical-ohlc.json", help="target OHLC file")
    ap.add_argument("--skip-if-fresh-min", type=int, default=0,
                    help="skip the fetch entirely if the newest m15 bar is younger than this "
                         "(minutes); 0 = always fetch")
    args = ap.parse_args()
    try:
        return freshen(args.file, args.days, args.skip_if_fresh_min)
    except Exception as e:
        print(f"::error::freshen_m15_tail failed ({e}) — file left unchanged", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
