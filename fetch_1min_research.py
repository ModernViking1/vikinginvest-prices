"""Throwaway 1-minute OHLC fetcher for the HPS quad-rotation native-TF test.

Pulls a bounded window (default 90d) of 1-min bars for a chosen pair subset — OANDA (M1) for
FX/index/metal/commodity, Coinbase (60s) for crypto — into {"pairs":{pk:{"m1":[...]}}}, the same
schema the research harness reads. Equities come from fetch_equity_ohlc.py --intervals m1 (Twelve
Data, ~13d cap) separately. Commits nothing; meant to run in CI where OANDA_TOKEN is set.

Run: OANDA_TOKEN=... python fetch_1min_research.py --pairs eurusd,gbpusd,... --days 90 --output ohlc-1m.json
"""
import argparse
import json
import os
from datetime import datetime, timedelta, timezone

from fetch_historical_ohlc import PAIRS, fetch_oanda_candles, fetch_coinbase_candles


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", required=True, help="comma-separated canonical pair keys")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--output", default="ohlc-1m.json")
    args = ap.parse_args()

    token = os.environ.get("OANDA_TOKEN", "").strip() or None
    now = datetime.now(timezone.utc)
    frm = now - timedelta(days=args.days)
    out = {"granularities": ["m1"], "days": args.days, "pairs": {}}

    for pk in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        cfg = PAIRS.get(pk)
        if not cfg:
            print(f"  {pk}: not in PAIRS — skipping", flush=True)
            continue
        try:
            if "oanda" in cfg:
                if not token:
                    print(f"  {pk}: no OANDA_TOKEN — skipping", flush=True)
                    continue
                bars = fetch_oanda_candles(token, cfg["oanda"], "M1", frm, now)
            elif "coinbase" in cfg:
                bars = fetch_coinbase_candles(cfg["coinbase"], 60, frm, now)
            else:
                print(f"  {pk}: no oanda/coinbase source — skipping", flush=True)
                continue
        except Exception as e:
            print(f"  {pk}: fetch failed ({e}) — skipping", flush=True)
            continue
        out["pairs"][pk] = {"m1": bars}
        print(f"  {pk}: {len(bars)} m1 bars" + (f" ({bars[0]['t']} .. {bars[-1]['t']})" if bars else " (EMPTY)"), flush=True)

    with open(args.output, "w") as f:
        json.dump(out, f)
    print(f"wrote {args.output} ({len(out['pairs'])} pairs)", flush=True)


if __name__ == "__main__":
    main()
