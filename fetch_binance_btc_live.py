"""Near-real-time BTC 15m delta feed for LIVE absorption trading.

Pulls the most recent ~1000 BTCUSDT 15m klines (with taker_buy volume -> real delta)
from data-api.binance.vision — Binance's PUBLIC REST mirror, which (unlike
api.binance.com) is NOT geo-blocked, so it works from US GitHub runners with no key.

Run every ~10 min from fetch-data.yml so a 15-minute absorption signal reaches the cBot
within minutes of the candle closing (tradeable). Writes binance-btc-live.json in the
SAME shape as the monthly research file, so the observer harness and absorb_live.py read
either one. NOT loaded by the dashboard.

Run: python fetch_binance_btc_live.py [--interval 15m] [--limit 1000]
"""
import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

REST = "https://data-api.binance.vision/api/v3/klines?symbol={s}&interval={iv}&limit={n}"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "binance-btc-live.json")
SYMBOL = "BTCUSDT"


def _norm_ts_ms(x):
    x = int(x)
    return x // 1000 if x > 1e14 else x        # microseconds -> ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", default="15m")
    ap.add_argument("--limit", type=int, default=1000)   # 1000 15m bars ~ 10.4 days
    args = ap.parse_args()

    url = REST.format(s=SYMBOL, iv=args.interval, n=min(1000, args.limit))
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            klines = json.load(r)
    except Exception as e:
        print(f"ERROR fetching live klines: {e}", flush=True)
        sys.exit(1)

    bars = []
    for k in klines:
        try:
            ot = _norm_ts_ms(k[0]); v = float(k[5]); tbv = float(k[9])
            bars.append({
                "t": datetime.fromtimestamp(ot / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]),
                "v": v, "tbv": tbv, "delta": 2 * tbv - v,
            })
        except (ValueError, IndexError, TypeError):
            continue
    # Drop the last (still-forming) candle so signals fire only on CLOSED bars.
    if bars:
        bars = bars[:-1]
    if len(bars) < 200:
        print(f"ERROR: only {len(bars)} closed bars — not writing", flush=True)
        sys.exit(1)

    out = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval": args.interval,
        "note": "Near-real-time BTC delta feed (data-api.binance.vision). Research/live-signal only; NOT loaded by the dashboard.",
        "pairs": {"btcusd": {args.interval: bars}},
    }
    with open(OUT, "w") as f:
        json.dump(out, f)
    print(f"wrote {OUT} ({len(bars)} closed {args.interval} bars, latest {bars[-1]['t']})", flush=True)


if __name__ == "__main__":
    main()
