"""Live absorption signal emitter — BTC 15m, real Binance delta, for the cBot feed.

absorb_btc (fade aggressive one-sided delta that price fails to follow) passed both OOS
halves only on BTC 15m (n~851, +0.092R RR2); ETH marginal, XRP/SOL failed — so this is
BTC-only and the user has opted to trade it live despite that fragility (demo).

PERSISTENCE (2026-08-10): the cBot flattens a position the moment its signal leaves
signals.json. A purely-transient emitter (emit only while the entry bar is "fresh") makes
the signal vanish ~20 min after entry, so the cBot force-exits at market long before the
+2R target / -1R stop — the live record then measures a time-exit, not the RR2 model. So
we keep a small state file (absorb-open.json) and RE-EMIT every open signal each cycle
until the feed shows its target or stop hit, exactly how detect_triggers rides macdp to
target-hit/stop-hit. A setup enters tracking only while fresh (no late entries); it's
dropped when resolved or after MAX_HOLD (safety, so signals.json can't grow unbounded).

Reads the near-real-time binance-btc-live.json (fetch_binance_btc_live.py, ~10-min
cadence); falls back to the monthly binance-crypto-ohlcv.json. Self-contained + fail-open
(any error -> emit nothing this cycle, never breaks build_signals_json).

Run: python absorb_live.py  (normally imported by build_signals_json.py)
"""
import json
import os

from crypto_delta_research import absorption_signals as _absorb_signals, _norm as _delta_norm

_HERE = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(_HERE, "binance-btc-live.json")
MONTHLY = os.path.join(_HERE, "binance-crypto-ohlcv.json")
STATE = os.path.join(_HERE, "absorb-open.json")
FRESH_MIN = 20            # a setup may ENTER tracking only within 20 min of the feed end
MAX_HOLD_MIN = 24 * 60    # stop tracking after 24h even if unresolved (safety bound)
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


def _load_state():
    try:
        return json.load(open(STATE)).get("open", {})
    except Exception:
        return {}


def _resolved(m15, armed_ts, stop, target, d):
    """True if any bar AFTER the entry bar hit the stop or the target (either => closed)."""
    for b in m15:
        if b["_ts"] <= armed_ts:
            continue
        if d == "bull":
            if b["l"] <= stop or b["h"] >= target:
                return True
        else:
            if b["h"] >= stop or b["l"] <= target:
                return True
    return False


def _row(pos, now_ms):
    return {
        "id": pos["id"], "pair": "btcusd", "sym": "BTCUSD", "cls": "crypto",
        "method": "absorb_btc", "r_size": 1.0, "dir": pos["dir"], "state": "triggered",
        "entry": pos["entry"], "stop": pos["stop"], "target": pos["target"],
        "ew": None, "tl": None, "nw": None, "cl": None,
        "armedAt": pos["armedAt"], "triggeredAt": pos["armedAt"], "lastSeenAt": now_ms,
        "source": "server-detector-absorb", "event_aligned": None,
    }


def build_absorb_rows(now_ms):
    """Rows for OPEN BTC absorption setups (fresh entries + persisted holds). Fail-open."""
    rows = []
    try:
        m15 = _load()
        if len(m15) < 400:
            return rows
        end_ts = m15[-1]["_ts"]
        open_state = _load_state()          # {id: {armedAt(ms), dir, entry, stop, target}}
        new_open = {}

        # 1) Carry forward still-open positions from prior cycles; drop resolved/expired.
        for sid, pos in open_state.items():
            armed_ts = pos["armedAt"] / 1000.0
            if end_ts - armed_ts > MAX_HOLD_MIN * 60:
                continue                    # aged out — stop tracking (cBot manages its own SL/TP)
            if _resolved(m15, armed_ts, pos["stop"], pos["target"], pos["dir"]):
                continue                    # target/stop hit — cBot closed it; stop emitting
            new_open[sid] = pos
            rows.append(_row(pos, now_ms))

        # 2) Admit NEW setups whose entry bar is still fresh (prevents late entries).
        fresh_after = end_ts - FRESH_MIN * 60
        for (ei, entry, stop, d) in _absorb_signals(m15):
            if ei >= len(m15) or m15[ei]["_ts"] < fresh_after:
                continue
            R = abs(entry - stop)
            if R <= 0:
                continue
            armed_ms = int(m15[ei]["_ts"] * 1000)
            sid = f"btcusd:{armed_ms}:absorb_btc"
            if sid in new_open:
                continue                    # already tracked/emitted this cycle
            pos = {"id": sid, "armedAt": armed_ms, "dir": d,
                   "entry": round(entry, 8), "stop": round(stop, 8),
                   "target": round(entry + (RR * R if d == "bull" else -RR * R), 8)}
            new_open[sid] = pos
            rows.append(_row(pos, now_ms))

        try:
            json.dump({"open": new_open}, open(STATE, "w"))
        except Exception:
            pass                            # state write is best-effort; never fail the build
    except Exception:
        return rows
    return rows


if __name__ == "__main__":
    import time
    r = build_absorb_rows(int(time.time() * 1000))
    print(f"absorb_btc open rows: {len(r)}")
    for x in r:
        print(x)
