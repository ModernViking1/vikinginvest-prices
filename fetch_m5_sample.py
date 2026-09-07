"""Fetch true 5-minute OHLC for a handful of representative instruments (one+ per asset
class) into m5-sample-ohlc.json — to test the Box Theory strategy at its native 5m
timeframe, which the main feed (m15 minimum) can't do. Research-only, runs in CI.

Reuses the source mappings and paginating fetch helpers from fetch_historical_ohlc.py.
OANDA instruments need OANDA_TOKEN; Coinbase products are keyless. Kept in its own file
(not the 7 MB main historical-ohlc.json).

Run: python fetch_m5_sample.py --days 90 --output m5-sample-ohlc.json
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

from fetch_historical_ohlc import PAIRS, fetch_oanda_candles, fetch_coinbase_candles

# one+ per class: major / minor / index / comm(x2) / crypto(x2)
SUBSET = ['eurusd', 'eurgbp', 'nas100', 'xagusd', 'wtiusd', 'btcusd', 'solusd']


def _arg(flag, default):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def main():
    days = int(_arg('--days', '90'))
    out = _arg('--output', 'm5-sample-ohlc.json')
    token = os.environ.get('OANDA_TOKEN', '').strip() or None
    now = datetime.now(timezone.utc)
    frm = now - timedelta(days=days)
    data = {'generated': now.strftime('%Y-%m-%dT%H:%M:%SZ'), 'granularity': 'm5',
            'days': days, 'pairs': {}}
    for pk in SUBSET:
        cfg = PAIRS.get(pk)
        if not cfg:
            print(f'skip {pk}: not in PAIRS', flush=True)
            continue
        try:
            if 'oanda' in cfg:
                if not token:
                    print(f'skip {pk}: no OANDA_TOKEN', flush=True)
                    data['pairs'][pk] = {'m5': []}
                    continue
                bars = fetch_oanda_candles(token, cfg['oanda'], 'M5', frm, now)
            else:
                bars = fetch_coinbase_candles(cfg['coinbase'], 300, frm, now)
            data['pairs'][pk] = {'m5': bars}
            print(f'{pk}: {len(bars)} m5 bars', flush=True)
        except Exception as e:
            print(f'{pk}: ERROR {e}', flush=True)
            data['pairs'][pk] = {'m5': []}
    with open(out, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    print(f'wrote {out} ({sum(len(v.get("m5", [])) for v in data["pairs"].values())} bars total)', flush=True)


if __name__ == '__main__':
    main()
