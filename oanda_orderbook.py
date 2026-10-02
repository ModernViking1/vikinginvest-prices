"""OANDA Order Book + Position Book -> retail liquidity map (orderbook.json).

WHY THIS EXISTS
---------------
We wanted a Bookmap-style "liquidity heatmap" (resting-order depth over price) for cam_rev
analysis. Spot FX has NO central limit order book, so a true Bookmap heatmap is impossible for
EUR/GBP et al. OANDA's own Order Book / Position Book API is the practical equivalent: a 20-minute
snapshot of where OANDA's retail clients hold resting ORDERS (pending entries + stops) and open
POSITIONS, bucketed by price. For a fade strategy that is arguably MORE useful than true depth,
because what cam_rev wants to know is *where the stops / resting orders are stacked* relative to
its Camarilla levels (R3/R4/S3/S4). A dense order shelf sitting on an R3 is confluence for the
short fade; an empty level is a weaker setup.

WHAT IT DOES
------------
For each FX / metal pair (order/position book is FX+metals only — not indices/crypto):
  1. GET /v3/instruments/{inst}/orderBook   (most recent snapshot)
  2. GET /v3/instruments/{inst}/positionBook (most recent snapshot)
  3. Compute today's Camarilla cam_rev levels from the daily layer of historical-ohlc.json
     (reusing unified_shadow_harness._cam_levels — single source of truth with the detector).
  4. For each level, measure the retail liquidity sitting around it: the % of all resting orders
     (and positions) within a window of +/- CONFLUENCE_BUCKETS bucket-widths, plus the long/short
     skew in that window -> a per-level "shelf" score.
  5. Surface the single biggest order cluster above and below current price.
Writes orderbook.json (compact) and prints a readable summary to stdout.

This commits nothing by itself; the workflow commits orderbook.json (like prices.json). Fail-open
per pair and overall: a pair OANDA doesn't serve (404/400) is skipped, never fatal.

NOTE ON HISTORY: OANDA only serves recent snapshots (optional ?time= within a rolling window), so a
multi-year confluence backtest isn't possible retroactively. This feed is the forward-collection
substrate: once we've accumulated snapshots we can backtest whether level-confluence improves
cam_rev R+ expectancy before wiring it into the dashboard or into live sizing.

Run:  OANDA_TOKEN=... python oanda_orderbook.py [--out orderbook.json] [--ohlc historical-ohlc.json]
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

from fetch_historical_ohlc import PAIRS
from detect_triggers import PAIR_CLASS

# Order/Position book is served for FX majors/minors and spot metals. Indices, energy CFDs and
# crypto (Coinbase) are excluded — OANDA returns no book (or a sparse one) for those.
OB_CLASSES = {'major', 'minor'}
OB_EXTRA = {'xauusd', 'xagusd'}          # metals OANDA does serve a book for
CONFLUENCE_BUCKETS = 3                    # +/- N bucket-widths counts as "at" a level
TOP_CLUSTERS = 3                          # biggest walls to surface each side

OANDA_BASE = os.environ.get("OANDA_BASE", "https://api-fxpractice.oanda.com").rstrip("/")


def _get(url, headers, params=None, max_retries=3):
    """GET with backoff. Returns (json|None, status, body_text). None json on failure."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                return r.json(), 200, ""
            if r.status_code in (400, 404):
                return None, r.status_code, r.text[:200]   # unsupported/invalid — don't retry
            if r.status_code == 429:
                time.sleep(2 ** attempt + 5); continue
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        except requests.RequestException:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    return None, -1, ""


def _fetch_book(inst, kind, headers):
    """kind: 'orderBook' | 'positionBook'. Returns (book_dict|None, status, body)."""
    url = f"{OANDA_BASE}/v3/instruments/{inst}/{kind}"
    data, status, body = _get(url, headers)
    if data is None:
        return None, status, body
    book = data.get(kind) or {}
    return book, status, body


def _buckets(book):
    """Normalise buckets to [{price, long, short}] floats, chronological by price."""
    out = []
    for b in book.get("buckets", []):
        try:
            out.append({
                "price": float(b["price"]),
                "long": float(b.get("longCountPercent", 0.0)),
                "short": float(b.get("shortCountPercent", 0.0)),
            })
        except (KeyError, ValueError, TypeError):
            continue
    out.sort(key=lambda x: x["price"])
    return out


def _band(buckets, level, width, n):
    """Sum order/position % within +/- n*width of a price level, plus long/short skew.
    Returns dict(order/pos pct total, long, short, skew) — here order/pos agnostic (caller labels)."""
    if not buckets or width <= 0 or level is None:
        return {"pct": 0.0, "long": 0.0, "short": 0.0}
    lo, hi = level - n * width, level + n * width
    L = sum(b["long"] for b in buckets if lo <= b["price"] <= hi)
    S = sum(b["short"] for b in buckets if lo <= b["price"] <= hi)
    return {"pct": round(L + S, 2), "long": round(L, 2), "short": round(S, 2)}


def _top_clusters(buckets, price, side_above, k):
    """The k buckets with the largest total (long+short) order % on one side of current price."""
    pool = [b for b in buckets if (b["price"] > price) == side_above]
    ranked = sorted(pool, key=lambda b: b["long"] + b["short"], reverse=True)[:k]
    return [{"price": round(b["price"], 5), "long": round(b["long"], 2),
             "short": round(b["short"], 2), "total": round(b["long"] + b["short"], 2)}
            for b in ranked]


def _current_cam_levels(ohlc_path):
    """{pair: {R4,R3,S3,S4, _from}} = the levels for the CURRENT/next trading session, computed
    from the LAST COMPLETE daily bar (prior-day Camarilla). Uses the detector's exact formula
    (R4=C+rng*1.1/2, R3=C+rng*1.1/4, S3/S4 mirror).

    Why compute directly instead of H._cam_levels()[max_key]: OANDA daily bars open at 21:00 UTC,
    so the bar covering the live session is in-progress (dropped as incomplete) and _cam_levels —
    which keys a level set by the NEXT day's bar — therefore has no entry for the session being
    traded now, leaving max_key one session stale. Deriving from the last complete bar gives the
    levels a manual trader actually fades today. Returns {} on any failure (fail-open)."""
    try:
        from backtest_rsi_per_class import _bars_norm
    except Exception as e:
        print(f"  (cam-level import failed: {e} — confluence disabled)", flush=True)
        return {}
    if not os.path.exists(ohlc_path):
        print(f"  ({ohlc_path} not found — confluence disabled)", flush=True)
        return {}
    try:
        pairs = json.load(open(ohlc_path)).get("pairs", {})
    except Exception as e:
        print(f"  ({ohlc_path} unreadable: {e} — confluence disabled)", flush=True)
        return {}
    out = {}
    for pk, layers in pairs.items():
        if not isinstance(layers, dict):
            continue
        daily = _bars_norm(layers.get("daily") or [])
        if len(daily) < 2:
            continue
        p = daily[-1]                       # last COMPLETE daily bar (freshen keeps complete only)
        rng = p["h"] - p["l"]
        if rng <= 0:
            continue
        C = p["c"]
        out[pk] = {
            "R4": C + rng * 1.1 / 2, "R3": C + rng * 1.1 / 4,
            "S3": C - rng * 1.1 / 4, "S4": C - rng * 1.1 / 2,
            "_from": datetime.fromtimestamp(p["_ts"], timezone.utc).strftime("%Y-%m-%d"),
        }
    return out


def main():
    ap = argparse.ArgumentParser(description="OANDA order/position book -> retail liquidity map")
    ap.add_argument("--out", default="orderbook.json")
    ap.add_argument("--ohlc", default="historical-ohlc.json")
    args = ap.parse_args()

    token = os.environ.get("OANDA_TOKEN", "").strip()
    if not token:
        print("::error::OANDA_TOKEN not set — cannot fetch order book", flush=True)
        return 1
    headers = {"Authorization": f"Bearer {token}", "Accept-Datetime-Format": "RFC3339"}

    cam = _current_cam_levels(args.ohlc)
    universe = [pk for pk in PAIRS
                if (PAIR_CLASS.get(pk) in OB_CLASSES or pk in OB_EXTRA) and "oanda" in PAIRS[pk]]

    now = datetime.now(timezone.utc)
    result = {}
    served = skipped = 0
    for pk in universe:
        inst = PAIRS[pk]["oanda"]
        ob, st_o, body_o = _fetch_book(inst, "orderBook", headers)
        pb, st_p, _ = _fetch_book(inst, "positionBook", headers)
        if ob is None and pb is None:
            print(f"  {pk:8} ({inst}): no book (order={st_o} pos={st_p}) — skipped"
                  + (f" :: {body_o}" if body_o else ""), flush=True)
            skipped += 1
            continue
        obk = _buckets(ob) if ob else []
        pbk = _buckets(pb) if pb else []
        price = None
        for src in (ob, pb):
            if src and src.get("price"):
                try:
                    price = float(src["price"]); break
                except (ValueError, TypeError):
                    pass
        ow = float(ob.get("bucketWidth", 0) or 0) if ob else 0.0
        pw = float(pb.get("bucketWidth", 0) or 0) if pb else 0.0

        rec = {
            "price": round(price, 5) if price is not None else None,
            "time": (ob or pb or {}).get("time"),
            "order_bucket_width": ow or None,
            "pos_bucket_width": pw or None,
            "order_clusters_above": _top_clusters(obk, price, True, TOP_CLUSTERS) if price else [],
            "order_clusters_below": _top_clusters(obk, price, False, TOP_CLUSTERS) if price else [],
        }

        lv = cam.get(pk)
        if lv and price is not None:
            conf = {}
            for name in ("R4", "R3", "S3", "S4"):
                level = lv.get(name)
                o = _band(obk, level, ow, CONFLUENCE_BUCKETS)
                p = _band(pbk, level, pw, CONFLUENCE_BUCKETS)
                conf[name] = {
                    "level": round(level, 5) if level is not None else None,
                    "order_pct": o["pct"], "order_long": o["long"], "order_short": o["short"],
                    "pos_pct": p["pct"], "pos_long": p["long"], "pos_short": p["short"],
                }
            rec["cam_from"] = lv.get("_from")   # last complete daily bar these levels derive from
            rec["confluence"] = conf
        result[pk] = rec
        served += 1

    if served == 0:
        print(f"\n::warning::OANDA served 0 order books ({skipped} skipped) on {OANDA_BASE} — "
              f"not writing {args.out}. If every pair is HTTP 400, the order/position book is "
              f"likely live-only (api-fxtrade) and needs a LIVE OANDA token.", flush=True)
        return 0

    doc = {
        "generated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "oanda_orderbook+positionbook",
        "base": OANDA_BASE,
        "confluence_buckets": CONFLUENCE_BUCKETS,
        "note": ("Retail order/position clusters from OANDA clients (20-min snapshot). "
                 "NOT full-market depth. order_pct/pos_pct = percent of all resting "
                 f"orders/positions within +/-{CONFLUENCE_BUCKETS} bucket-widths of each "
                 "cam_rev level."),
        "pairs": result,
    }
    tmp = args.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    os.replace(tmp, args.out)

    # ── readable summary ────────────────────────────────────────────
    print(f"\n=== OANDA retail liquidity map · {served} served / {skipped} skipped · {now:%Y-%m-%d %H:%M}Z ===")
    print(f"(order_pct = % of resting retail orders within +/-{CONFLUENCE_BUCKETS} buckets of the level; "
          f"a high value = a liquidity shelf ON that cam_rev level)\n")
    for pk in sorted(result):
        r = result[pk]
        if not r.get("confluence"):
            print(f"  {pk:8} px={r['price']}  (no cam levels — order clusters only)")
            continue
        parts = []
        for name in ("R4", "R3", "S3", "S4"):
            c = r["confluence"][name]
            parts.append(f"{name}@{c['level']} ord={c['order_pct']:.1f}% pos={c['pos_pct']:.1f}%")
        print(f"  {pk:8} px={r['price']} (cam from {r.get('cam_from')})")
        print(f"           " + "  ".join(parts))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"::error::oanda_orderbook failed ({e})", flush=True)
        sys.exit(1)
