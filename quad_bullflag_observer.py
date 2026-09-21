"""Forward observer for the HPS 20/20 Bull-Flag on 5-min GOLD + COMMODITIES — the only quad-rotation
pocket that was positive in BOTH out-of-sample halves of the backtest (gold +0.13R, comm +0.25R).

Observe-only: no capital, no orders. Each run fetches recent 5-min bars for the observed pairs,
detects new bull-flag signals with the SAME detector as the backtest, scores the ones that have
resolved (first 9-3 rotation to the opposite band vs pattern-low stop), and appends them to
quad-bullflag-shadow.json — deduped by signal id, so a signal is logged exactly once, when it
resolves. Runs on a schedule in CI. This is the honest 'prove it forward' step; the backtest edge is
marginal, so this is a tracking log, not a trade signal.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import unified_shadow_harness as H
from fetch_historical_ohlc import PAIRS, fetch_oanda_candles
from quad_rotation_research import detect_bull_flag, score

OBS_PAIRS = ["xauusd", "xagusd", "wtiusd", "usoil", "natgas"]   # gold + commodities
LOG = "quad-bullflag-shadow.json"
DAYS = 25            # rolling fetch window — long enough that recent signals resolve before it rolls off


def main():
    token = os.environ.get("OANDA_TOKEN", "").strip() or None
    if not token:
        print("no OANDA_TOKEN — nothing to observe", flush=True)
        return 0
    now = datetime.now(timezone.utc)
    frm = now - timedelta(days=DAYS)

    try:
        doc = json.load(open(LOG))
    except Exception:
        doc = {"strategy": "bullflag", "tf": "m5", "scope": "gold + commodity (5-min)",
               "note": "Observe-only forward test — no capital. Both-OOS-positive backtest pocket.",
               "signals": []}
    seen = {s["id"] for s in doc.get("signals", [])}

    added = 0
    for pk in OBS_PAIRS:
        cfg = PAIRS.get(pk)
        if not cfg or "oanda" not in cfg:
            print(f"  {pk}: no OANDA source — skipping", flush=True)
            continue
        try:
            bars = H._bars_norm(fetch_oanda_candles(token, cfg["oanda"], "M5", frm, now))
        except Exception as e:
            print(f"  {pk}: fetch failed ({e}) — skipping", flush=True)
            continue
        if len(bars) < 300:
            continue
        found = 0
        for s in detect_bull_flag(bars):
            sid = f"bullflag:{pk}:{int(s['entry_ts'])}"
            if sid in seen:
                continue
            st, o = score(bars, s)
            if st != "resolved" or o is None:
                continue          # not yet resolved — a later run will catch it
            doc["signals"].append({
                "id": sid, "pair": pk, "dir": s["dir"], "entry_ts": int(s["entry_ts"]),
                "entry_iso": datetime.fromtimestamp(s["entry_ts"], timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
                "entry": round(s["entry"], 5), "stop": round(s["stop"], 5), "r": round(o, 3),
            })
            seen.add(sid); added += 1; found += 1
        print(f"  {pk}: {len(bars)} m5 bars, +{found} new resolved", flush=True)

    doc["signals"].sort(key=lambda x: x["entry_ts"])
    doc["updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    rs = [s["r"] for s in doc["signals"]]
    doc["summary"] = {
        "n": len(rs),
        "wr": round(100.0 * sum(1 for r in rs if r > 0) / len(rs), 1) if rs else 0.0,
        "exp": round(sum(rs) / len(rs), 3) if rs else 0.0,
        "totR": round(sum(rs), 1),
    }
    with open(LOG, "w") as f:
        json.dump(doc, f, indent=1)
    print(f"added {added}; total {len(rs)} resolved | {doc['summary']}", flush=True)
    return 0


if __name__ == "__main__":
    main()
