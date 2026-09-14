"""Freshen ONLY the H1 tail of historical-ohlc.json — the swing feed's data source.

Why this exists
---------------
The swing feed (swing_signals.py -> swing-signals.json) reads historical-ohlc.json,
which is rebuilt in full (365d, all timeframes, every pair) by publish-historical-ohlc.yml
on a 6-HOURLY cron. That heavy job can't run often, and GitHub's scheduler throttles it
further, so the H1 bars the swing detectors trigger on go 0-6h stale. The swing cBot then
rejects those signals as older than its entry-age guard (MaxSignalAgeMin), and swing setups
— including the demo pilots — never fill. (The intraday feed doesn't have this problem: it's
fed by fetch-data every ~10 min.)

Swing detectors need the full 365d of H1 for CONTEXT, but only the NEWEST bar needs to be
current. So this script does the cheap half of the backfill on a tight cadence: fetch just
the last few days of H1 per pair and splice them onto each pair's existing h1 array,
leaving m15/daily and the long history untouched. ~40 one-shot requests, well under a minute.

Safety
------
- Only the "h1" layer of each pair is modified; "m15"/"daily" and every other pair key are
  copied through byte-for-byte.
- The splice DEDUPES by bar time and NEVER shrinks a pair's h1 history — if a fetch comes
  back short (network hiccup, provider gap), that pair is left exactly as it was.
- Fail-open per pair AND overall: any error leaves historical-ohlc.json unchanged so it can
  never break the swing feed or the 6-hourly full rebuild that remains the source of truth.

Run:  OANDA_TOKEN=... python freshen_h1_tail.py   [--days 3] [--file historical-ohlc.json]
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from fetch_historical_ohlc import PAIRS, fetch_oanda_candles, fetch_coinbase_candles

DEFAULT_DAYS = 3          # H1 lookback to fetch each cycle (>= the max stale gap we tolerate)
MIN_KEEP_RATIO = 0.98     # never let a pair's h1 shrink below this fraction of its prior length


def _splice(existing, fresh):
    """Merge `fresh` H1 bars onto `existing`, dedup by bar time 't', keep chronological.
    Returns (merged, added) where `added` counts genuinely new timestamps. Never drops
    existing bars: fresh bars with a matching 't' REPLACE (finalise) the stored one; all
    other existing bars are preserved."""
    by_t = {b["t"]: b for b in existing if "t" in b}
    before = len(by_t)
    for b in fresh:
        if "t" in b:
            by_t[b["t"]] = b
    merged = [by_t[t] for t in sorted(by_t)]
    return merged, len(by_t) - before


def _newest_h1_age_min(pairs, now):
    """Minutes since the newest completed H1 bar across all pairs (None if none)."""
    newest = None
    for layers in pairs.values():
        if isinstance(layers, dict):
            h1 = layers.get("h1") or []
            if h1 and "t" in h1[-1] and (newest is None or h1[-1]["t"] > newest):
                newest = h1[-1]["t"]
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
    # Freshness gate: H1 bars close hourly, so re-fetching within the same hour just re-pulls
    # the same completed bars. Skip the whole fetch when the newest bar is younger than the
    # gate — keeps API load to ~once per hour instead of once per feed cycle.
    if skip_if_fresh_min > 0:
        age = _newest_h1_age_min(pairs, now)
        if age is not None and age < skip_if_fresh_min:
            print(f"h1 already fresh (newest {age:.0f} min old < {skip_if_fresh_min}) — skipping fetch", flush=True)
            return 0

    token = os.environ.get("OANDA_TOKEN", "").strip() or None
    frm = now - timedelta(days=days)

    total_added = 0
    refreshed = 0
    for pk, layers in pairs.items():
        cfg = PAIRS.get(pk)
        if not cfg or not isinstance(layers, dict) or "h1" not in layers:
            continue
        existing = layers.get("h1") or []
        try:
            if "oanda" in cfg:
                if not token:
                    continue
                fresh = fetch_oanda_candles(token, cfg["oanda"], "H1", frm, now)
            elif "coinbase" in cfg:
                fresh = fetch_coinbase_candles(cfg["coinbase"], 3600, frm, now)
            else:
                continue
        except Exception as e:
            print(f"  {pk}: h1 fetch failed ({e}) — leaving as-is", flush=True)
            continue

        if not fresh:
            print(f"  {pk}: no fresh h1 bars returned — leaving as-is", flush=True)
            continue
        merged, added = _splice(existing, fresh)
        # Never shrink history: a good splice can only keep or grow the bar count.
        if len(merged) < len(existing) * MIN_KEEP_RATIO:
            print(f"  {pk}: splice would shrink h1 {len(existing)}->{len(merged)} — skipped", flush=True)
            continue
        layers["h1"] = merged
        total_added += added
        refreshed += 1
        newest = merged[-1]["t"] if merged else "?"
        print(f"  {pk}: h1 {len(existing)}->{len(merged)} (+{added}) newest {newest}", flush=True)

    if refreshed == 0:
        print("no pairs refreshed (no token / no data) — file unchanged", flush=True)
        return 0

    doc["h1_tail_refreshed"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    os.replace(tmp, path)
    print(f"freshened h1 tail on {refreshed} pair(s), +{total_added} bars total", flush=True)
    return 0


def main():
    ap = argparse.ArgumentParser(description="Freshen only the H1 tail of historical-ohlc.json")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS, help="H1 lookback days to fetch")
    ap.add_argument("--file", default="historical-ohlc.json", help="target OHLC file")
    ap.add_argument("--skip-if-fresh-min", type=int, default=0,
                    help="skip the fetch entirely if the newest H1 bar is younger than this "
                         "(minutes); 0 = always fetch")
    args = ap.parse_args()
    try:
        return freshen(args.file, args.days, args.skip_if_fresh_min)
    except Exception as e:
        print(f"::error::freshen_h1_tail failed ({e}) — file left unchanged", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
