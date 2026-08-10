"""Fetch 6-12 months of BTCUSDT klines from data.binance.vision — WITH real delta.

Binance klines carry taker_buy_volume, so per candle:
    delta = taker_buy_volume - (total_volume - taker_buy_volume) = 2*taker_buy - volume
This is REAL aggressor delta (market-buy vs market-sell volume) — the thing the
absorption / delta-flip model needs and that Coinbase/OANDA OHLCV cannot give. Written
to a SEPARATE file (binance-btc-ohlcv.json) that the dashboard does NOT load, so the
main dashboard is unaffected.

Source: data.binance.vision monthly kline zips — public, no API key, and NOT geo-blocked
(works from US GitHub runners where api.binance.com returns 451). Each CSV row is:
  open_time, open, high, low, close, volume, close_time, quote_volume, count,
  taker_buy_base, taker_buy_quote, ignore
Timestamps are ms on older files and microseconds on 2025+ files — normalised here.

Run: python fetch_binance_btc.py [--months 9] [--interval 5m]
"""
import argparse
import csv
import io
import json
import os
import sys
import zipfile
import urllib.request
from datetime import datetime, timezone, timedelta

SYMBOL = "BTCUSDT"
URL = "https://data.binance.vision/data/spot/monthly/klines/{s}/{iv}/{s}-{iv}-{ym}.zip"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "binance-btc-ohlcv.json")


def last_months(n):
    """The N most-recent COMPLETE months as 'YYYY-MM' (skip the partial current month)."""
    d = datetime.now(timezone.utc).replace(day=1)
    out = []
    for _ in range(n):
        d = (d - timedelta(days=1)).replace(day=1)
        out.append(d.strftime("%Y-%m"))
    return sorted(out)


def _norm_ts_ms(x):
    x = int(x)
    return x // 1000 if x > 1e14 else x        # microseconds -> ms


def fetch_month(interval, ym):
    url = URL.format(s=SYMBOL, iv=interval, ym=ym)
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            data = r.read()
    except Exception as e:
        print(f"  skip {ym}: {e}", flush=True)
        return []
    bars = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            for row in csv.reader(io.TextIOWrapper(f, "utf-8")):
                if not row or not row[0] or row[0][0].isalpha():   # skip any header row
                    continue
                try:
                    ot = _norm_ts_ms(row[0]); v = float(row[5]); tbv = float(row[9])
                    bars.append({
                        "t": datetime.fromtimestamp(ot / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "o": float(row[1]), "h": float(row[2]), "l": float(row[3]), "c": float(row[4]),
                        "v": v, "tbv": tbv, "delta": 2 * tbv - v,   # real aggressor delta
                    })
                except (ValueError, IndexError):
                    continue
    print(f"  {ym}: {len(bars)} bars", flush=True)
    return bars


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=9)
    ap.add_argument("--interval", default="5m")
    args = ap.parse_args()

    all_bars = []
    for ym in last_months(args.months):
        all_bars += fetch_month(args.interval, ym)
    all_bars.sort(key=lambda b: b["t"])
    if len(all_bars) < 1000:
        print(f"ERROR: only {len(all_bars)} bars — not writing", flush=True)
        sys.exit(1)

    out = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol": SYMBOL, "interval": args.interval,
        "note": "delta = 2*taker_buy_volume - volume (real aggressor delta). Research-only; NOT loaded by the dashboard.",
        "pairs": {"btcusd": {args.interval: all_bars}},
    }
    with open(OUT, "w") as f:
        json.dump(out, f)
    print(f"wrote {OUT} ({os.path.getsize(OUT) // 1024} KB, {len(all_bars)} {args.interval} bars)", flush=True)


if __name__ == "__main__":
    main()
