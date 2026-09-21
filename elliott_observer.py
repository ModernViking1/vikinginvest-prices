"""Observe-only tracker for external Elliott Wave calls in elliott-watchlist.json.

We cannot mechanize full discretionary EW (see ELLIOTT_WAVE_NOTES.md), so instead we LOG the
external analyst calls and track each one forward against its own levels: watching -> active
(price reached the entry zone) -> target_hit / invalidated, recording realised R vs the call's
entry/invalidation/target. No capital, no orders. Runs on a schedule in CI; commits the watchlist.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import unified_shadow_harness as H
from fetch_historical_ohlc import PAIRS, fetch_oanda_candles

WL = "elliott-watchlist.json"


def evaluate(call, bars):
    """bars: chronological daily OHLC (normalised). Returns a status dict for this call."""
    d = call["dir"]; entry = call["entry"]; inv = call["invalidation"]; tgts = call.get("targets") or []
    zone = call.get("entry_zone") or [entry, entry]
    risk = abs(inv - entry)
    if risk <= 0:
        return {"status": "error", "note": "bad levels (risk<=0)"}
    # activation: has price traded into the entry zone?
    act = None
    for i, b in enumerate(bars):
        if (d == "short" and b["h"] >= min(zone)) or (d == "long" and b["l"] <= max(zone)):
            act = i; break
    if act is None:
        return {"status": "watching", "note": "price has not reached the zone yet"}
    # from activation forward: first of invalidation / a target (invalidation checked first = conservative)
    for b in bars[act:]:
        if d == "short":
            if b["h"] >= inv:
                return {"status": "invalidated", "r": -1.0}
            for j, t in enumerate(tgts):
                if b["l"] <= t:
                    return {"status": "target_hit", "target": t, "target_idx": j + 1,
                            "r": round((entry - t) / risk, 2)}
        else:
            if b["l"] <= inv:
                return {"status": "invalidated", "r": -1.0}
            for j, t in enumerate(tgts):
                if b["h"] >= t:
                    return {"status": "target_hit", "target": t, "target_idx": j + 1,
                            "r": round((t - entry) / risk, 2)}
    return {"status": "active", "note": "activated; no target or invalidation hit yet"}


def main():
    token = os.environ.get("OANDA_TOKEN", "").strip() or None
    if not token:
        print("no OANDA_TOKEN — nothing to observe", flush=True)
        return 0
    try:
        doc = json.load(open(WL))
    except Exception as e:
        print(f"cannot read {WL}: {e}", flush=True)
        return 1
    now = datetime.now(timezone.utc)
    for call in doc.get("calls", []):
        if call.get("status") in ("target_hit", "invalidated"):
            continue                                  # already resolved — leave it
        cfg = PAIRS.get(call["pair"])
        if not cfg or "oanda" not in cfg:
            print(f"  {call['pair']}: no OANDA source — skipping", flush=True)
            continue
        try:
            frm = datetime.strptime(call["added"], "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=3)
            bars = H._bars_norm(fetch_oanda_candles(token, cfg["oanda"], "D", frm, now))
        except Exception as e:
            print(f"  {call['pair']}: fetch failed ({e}) — skipping", flush=True)
            continue
        if not bars:
            continue
        res = evaluate(call, bars)
        call.update(res)
        call["current"] = round(bars[-1]["c"], 5)
        call["checked"] = now.strftime("%Y-%m-%dT%H:%MZ")
        print(f"  {call['pair']} {call['dir']}: {res.get('status')} "
              f"{('R=' + str(res['r'])) if 'r' in res else ''} (px {bars[-1]['c']})", flush=True)
    doc["updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(WL, "w") as f:
        json.dump(doc, f, indent=1)
    resolved = [c for c in doc.get("calls", []) if c.get("status") in ("target_hit", "invalidated")]
    if resolved:
        rs = [c["r"] for c in resolved if "r" in c]
        print(f"resolved {len(resolved)} | avg R {round(sum(rs)/len(rs),2) if rs else 0}", flush=True)
    return 0


if __name__ == "__main__":
    main()
