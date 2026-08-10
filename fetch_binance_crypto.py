"""Fetch 6-12 months of Binance crypto klines (BTC/ETH/XRP/SOL) — WITH real delta.

Binance klines carry taker_buy_volume, so per candle:
    delta = taker_buy_volume - (total_volume - taker_buy_volume) = 2*taker_buy - volume
= REAL aggressor delta (market-buy vs market-sell), the input the absorption / delta-flip
model needs. Written to a SEPARATE binance-crypto-ohlcv.json the dashboard does NOT load,
so the main dashboard is unaffected.

Source: data.binance.vision monthly kline zips — public, no key, NOT geo-blocked (works
from US GitHub runners where api.binance.com returns 451). Default 15m interval keeps the
multi-pair file manageable while covering the passing edges (absorption 15m, delta-flip 1h
resampled).

Run: python fetch_binance_crypto.py [--months 9] [--interval 15m] [--symbols BTCUSDT,ETHUSDT,...]
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

URL = "https://data.binance.vision/data/spot/monthly/klines/{s}/{iv}/{s}-{iv}-{ym}.zip"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "binance-crypto-ohlcv.json")
# Binance symbol -> our pair key
PAIR = {"BTCUSDT": "btcusd", "ETHUSDT": "ethusd", "XRPUSDT": "xrpusd", "SOLUSDT": "solusd"}


def last_months(n):
    d = datetime.now(timezone.utc).replace(day=1)
    out = []
    for _ in range(n):
        d = (d - timedelta(days=1)).replace(day=1)
        out.append(d.strftime("%Y-%m"))
    return sorted(out)


def _norm_ts_ms(x):
    x = int(x)
    return x // 1000 if x > 1e14 else x        # microseconds -> ms


def fetch_month(symbol, interval, ym):
    url = URL.format(s=symbol, iv=interval, ym=ym)
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            data = r.read()
    except Exception as e:
        print(f"    skip {symbol} {ym}: {e}", flush=True)
        return []
    bars = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        with z.open(z.namelist()[0]) as f:
            for row in csv.reader(io.TextIOWrapper(f, "utf-8")):
                if not row or not row[0] or row[0][0].isalpha():
                    continue
                try:
                    ot = _norm_ts_ms(row[0]); v = float(row[5]); tbv = float(row[9])
                    bars.append({
                        "t": datetime.fromtimestamp(ot / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "o": float(row[1]), "h": float(row[2]), "l": float(row[3]), "c": float(row[4]),
                        "v": v, "tbv": tbv, "delta": 2 * tbv - v,
                    })
                except (ValueError, IndexError):
                    continue
    return bars


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=9)
    ap.add_argument("--interval", default="15m")
    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT,XRPUSDT,SOLUSDT")
    args = ap.parse_args()

    months = last_months(args.months)
    pairs = {}
    for sym in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        pk = PAIR.get(sym, sym.lower())
        print(f"{sym} -> {pk}", flush=True)
        allb = []
        for ym in months:
            allb += fetch_month(sym, args.interval, ym)
        allb.sort(key=lambda b: b["t"])
        if len(allb) < 1000:
            print(f"    WARN: only {len(allb)} bars for {sym} — skipping", flush=True)
            continue
        print(f"    {len(allb)} {args.interval} bars", flush=True)
        pairs[pk] = {args.interval: allb}

    if not pairs:
        print("ERROR: no pairs fetched — not writing", flush=True); sys.exit(1)
    out = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval": args.interval,
        "note": "delta = 2*taker_buy_volume - volume (real aggressor delta). Research-only; NOT loaded by the dashboard.",
        "pairs": pairs,
    }
    with open(OUT, "w") as f:
        json.dump(out, f)
    print(f"wrote {OUT} ({os.path.getsize(OUT)//1024} KB, {len(pairs)} pairs)", flush=True)


if __name__ == "__main__":
    main()
