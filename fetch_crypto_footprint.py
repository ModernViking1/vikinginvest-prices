"""Crypto FOOTPRINT pilot — TRUE per-bar delta from Binance aggTrades (free data).

Spot FX / OANDA cannot provide order flow (no central exchange). Crypto CAN: Binance
publishes daily aggregated-trade dumps carrying the aggressor side (isBuyerMaker), so we
can compute real delta = aggressive-buy volume - aggressive-sell volume per bar. This
streams BTCUSDT aggTrades for a window, aggregates to m15 bars with delta/buyv/sellv, and
writes crypto-footprint.json for the footprint backtest.

Streams each day (download -> parse -> aggregate -> discard) so disk stays small even
though the raw dumps are large. Runs in CI (data.binance.vision is blocked from the dev
sandbox). Run: python fetch_crypto_footprint.py [--symbol BTCUSDT] [--days 45]
"""
import argparse
import datetime
import io
import json
import os
import sys
import zipfile

import requests

BASE = "https://data.binance.vision/data/spot/daily/aggTrades"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crypto-footprint.json")
M15_MS = 15 * 60 * 1000


def process_day(sym, d, bars):
    url = f"{BASE}/{sym}/{sym}-aggTrades-{d:%Y-%m-%d}.zip"
    try:
        r = requests.get(url, timeout=180)
    except Exception as e:
        print(f"  {d:%Y-%m-%d}: {e} — skip", flush=True); return
    if r.status_code != 200:
        print(f"  {d:%Y-%m-%d}: HTTP {r.status_code} — skip", flush=True); return
    z = zipfile.ZipFile(io.BytesIO(r.content))
    with z.open(z.namelist()[0]) as fh:
        for raw in io.TextIOWrapper(fh, encoding="utf-8"):
            parts = raw.rstrip("\n").split(",")
            if len(parts) < 7:
                continue
            try:
                price = float(parts[1]); qty = float(parts[2]); ts = int(parts[5])
            except ValueError:
                continue                                   # header row
            maker = parts[6].strip().lower() in ("true", "1")   # buyer is maker => seller aggressed
            bk = ts // M15_MS * M15_MS
            b = bars.get(bk)
            if b is None:
                b = bars[bk] = {"o": price, "h": price, "l": price, "c": price,
                                "v": 0.0, "buyv": 0.0, "sellv": 0.0}
            if price > b["h"]: b["h"] = price
            if price < b["l"]: b["l"] = price
            b["c"] = price; b["v"] += qty
            if maker:
                b["sellv"] += qty                          # aggressive sell
            else:
                b["buyv"] += qty                           # aggressive buy
    print(f"  {d:%Y-%m-%d}: ok — {len(bars)} m15 bars so far", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--days", type=int, default=45)
    a = ap.parse_args()
    end = datetime.date.today()
    bars = {}
    print(f"Fetching {a.symbol} aggTrades — {a.days} days ending {end} ...", flush=True)
    for i in range(a.days, 0, -1):
        process_day(a.symbol, end - datetime.timedelta(days=i), bars)
    rows = []
    for bk in sorted(bars):
        b = bars[bk]
        rows.append({"t": bk, "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"],
                     "v": round(b["v"], 8), "buyv": round(b["buyv"], 8),
                     "sellv": round(b["sellv"], 8), "delta": round(b["buyv"] - b["sellv"], 8)})
    if len(rows) < 200:
        print(f"ERROR: only {len(rows)} bars — not writing", file=sys.stderr); sys.exit(1)
    json.dump({"generated": datetime.datetime.utcnow().isoformat() + "Z",
               "symbol": a.symbol, "granularity": "m15", "bars": rows}, open(OUT, "w"))
    print(f"wrote {OUT}: {len(rows)} m15 bars ({os.path.getsize(OUT) // 1024} KB)")


if __name__ == "__main__":
    main()
