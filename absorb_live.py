"""Live absorption signal emitter — BTC 15m, real Binance delta, for the cBot feed.

absorb_btc (fade aggressive one-sided delta that price fails to follow) passed both OOS
halves only on BTC 15m (n~851, +0.092R RR2); ETH marginal, XRP/SOL failed — so this is
BTC-only and the user has opted to trade it live despite that fragility (demo). Emits
signals.json rows for FRESH setups (entry within FRESH_MIN of the feed end) so a 15m
signal reaches the cBot within minutes of the candle closing.

Reads the near-real-time binance-btc-live.json (fetch_binance_btc_live.py, ~10-min
cadence); falls back to the monthly binance-crypto-ohlcv.json. Self-contained + fail-open
(any error -> no rows, never breaks build_signals_json).

Run: python absorb_live.py  (normally imported by build_signals_json.py)
"""
import json
import os
import datetime as dt

from crypto_delta_research import absorption_signals as _absorb_signals, _norm as _delta_norm

_HERE = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(_HERE, "binance-btc-live.json")
MONTHLY = os.path.join(_HERE, "binance-crypto-ohlcv.json")
FRESH_MIN = 20          # emit setups whose entry bar is within the last 20 minutes
RR = 2.0


def _load():
    for path in (LIVE, MONTHLY):
        if os.path.exists(path):
            try:
                d = json.load(open(path)); iv = d.get("interval", "15m")
                bars = d.get("pairs", {}).get("btcusd", {}).get(iv, [])
                if iv == "15m" and bars:
                    return _delta_norm(bars)
            except Exception:
                continue
    return []


def build_absorb_rows(now_ms):
    """Return signals.json rows for fresh BTC absorption setups (fail-open -> [])."""
    rows = []
    try:
        m15 = _load()
        if len(m15) < 400:
            return rows
        fresh_after = m15[-1]['_ts'] - FRESH_MIN * 60
        for (ei, entry, stop, d) in _absorb_signals(m15):
            if ei >= len(m15) or m15[ei]['_ts'] < fresh_after:
                continue
            R = abs(entry - stop)
            if R <= 0:
                continue
            tgt = entry + RR * R if d == 'bull' else entry - RR * R
            ets = m15[ei]['_ts']
            rows.append({
                'id': f"btcusd:{int(ets * 1000)}:absorb_btc",
                'pair': 'btcusd', 'sym': 'BTCUSD', 'cls': 'crypto', 'method': 'absorb_btc',
                'r_size': 1.0, 'dir': d, 'state': 'triggered',
                'entry': round(entry, 8), 'stop': round(stop, 8), 'target': round(tgt, 8),
                'ew': None, 'tl': None, 'nw': None, 'cl': None,
                'armedAt': int(ets * 1000), 'triggeredAt': int(ets * 1000), 'lastSeenAt': now_ms,
                'source': 'server-detector-absorb', 'event_aligned': None,
            })
    except Exception:
        return rows
    return rows


if __name__ == '__main__':
    import time
    r = build_absorb_rows(int(time.time() * 1000))
    print(f"absorb_btc fresh rows: {len(r)}")
    for x in r:
        print(x)
