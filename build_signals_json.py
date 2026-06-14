#!/usr/bin/env python3
"""
build_signals_json.py — Phase 1 of the broker-bridge roadmap.

Reads the live server-side detector output (alerts-state.json, produced
by detect_triggers.py inside fetch-data.yml every ~10 min) and emits
signals.json — the public, EA-friendly contract that the MT5 Expert
Advisor on a VPS polls every 30 seconds to decide whether to place a
demo-account order.

Schema match: matches the in-browser SIGNAL_LOG shape from
Viking_Invest_Trading_v69.html (RULES_VERSION 2026-06-14vv) so the
browser-recorded log and the server-published log can be merged into a
single Performance view without translation.

Idempotency contract: every signal carries a stable `id` of the form
    "{pair}:{creator_ts_epoch_ms}"
The EA must dedupe on this id — once it has placed an order for a given
id, subsequent polls returning the same id are no-ops.

Run: invoked automatically from .github/workflows/fetch-data.yml right
after detect_triggers.py commits the alerts-state. Manual:
    python build_signals_json.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ALERT_STATE_PATH = Path("alerts-state.json")
SIGNALS_OUT_PATH = Path("signals.json")
SCHEMA_VERSION = 1

# Per-class methodology — keep parity with _btMethodFor() in the
# dashboard JS. Wick pairs trade at 1.0R, Fib pairs at 0.5R.
WICK_CLASSES = {"major", "minor", "crypto"}
FIB_CLASSES = {"comm", "index"}

# Per-pair classification mirrors MKTS[k].t in the dashboard. Extracted
# from Viking_Invest_Trading_v69.html so the EA's risk sizing matches
# what the backtest engine computes. Kept inline (not imported) so this
# script has no run-time dependency on the HTML file — the EA must know
# which pairs to risk at half-size, so the class travels with the signal.
#
# Drift policy: when a pair is added to MKTS, add it here too. The
# `_classify` lookup returns None for unmapped pairs and they are
# silently skipped so an unmapped pair never produces a malformed
# signal — the workflow logs the skip but doesn't fail.
PAIR_CLASS = {
    # FX majors
    "eurusd": "major", "gbpusd": "major", "usdjpy": "major",
    "usdcad": "major", "usdchf": "major", "audusd": "major",
    "nzdusd": "major",
    # FX minors / crosses
    "audnzd": "minor", "cadjpy": "minor", "euraud": "minor",
    "eurgbp": "minor", "eurnok": "minor", "eurnzd": "minor",
    "eursgd": "minor", "gbpcad": "minor", "gbpnzd": "minor",
    "nzdchf": "minor", "nzdjpy": "minor", "usdsgd": "minor",
    "usdzar": "minor",
    # Commodities
    "natgas": "comm", "usoil": "comm", "wtiusd": "comm",
    "xagusd": "comm", "xauusd": "comm", "xptusd": "comm",
    # Indices
    "dj30": "index", "ftse100": "index", "jp225": "index",
    "nas100": "index", "spx500": "index",
    # Crypto (24/7 — primary use-case for the weekend bridge)
    "btcusd": "crypto", "ethusd": "crypto", "nearusd": "crypto",
    "ondousd": "crypto", "solusd": "crypto", "suiusd": "crypto",
    "taousd": "crypto", "xrpusd": "crypto",
}


def _to_epoch_ms(ts) -> int | None:
    """Accept either ISO-8601 string or epoch (ms or s) → epoch ms."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        # If it's > year-3000 in seconds, assume ms; else assume seconds.
        return int(ts) if ts > 1e11 else int(ts * 1000)
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    return None


def _classify(pair: str) -> str | None:
    return PAIR_CLASS.get(pair)


def _method_for(pair: str) -> str:
    cls = _classify(pair)
    return "fib" if cls in FIB_CLASSES else "wick"


def _R_per_trade(method: str) -> float:
    """Risk units per trade — wick pairs 1.0R, Fib pairs 0.5R half-size."""
    return 0.5 if method == "fib" else 1.0


def _signal_row(pair: str, info: dict, kind: str, now_ms: int) -> dict | None:
    """
    Build one row of the signals.json `signals` array from an
    alerts-state.json per-pair record.

    `kind` selects which path inside `info` to read:
      - 'wick' → sig_state / sig_entry / sig_creator_ts / sig_trigger_ts
      - 'fib'  → sig_fib_state / sig_fib_entry / etc.
    """
    if kind == "wick":
        state = info.get("sig_state")
        entry = info.get("sig_entry")
        creator_ts = info.get("sig_creator_ts")
        trigger_ts = info.get("sig_trigger_ts")
        prefix = "wick"
    elif kind == "fib":
        state = info.get("sig_fib_state")
        entry = info.get("sig_fib_entry")
        creator_ts = info.get("sig_fib_creator_ts") or info.get("alerted_fib_creator_ts")
        trigger_ts = info.get("sig_fib_trigger_ts")
        prefix = "fib"
    else:
        return None

    if not state or not creator_ts:
        return None

    # Only surface states the EA cares about — armed (pending entry),
    # triggered (live trade), invalidated (cancelled pre/post-trigger).
    if state not in ("armed", "triggered", "invalidated"):
        return None

    armed_at_ms = _to_epoch_ms(creator_ts)
    if armed_at_ms is None:
        return None
    trig_at_ms = _to_epoch_ms(trigger_ts)

    aligned = info.get("aligned_dir")
    if aligned not in ("bull", "bear"):
        # Without an aligned direction we can't place a directional
        # order. Skip — the detector will re-emit when alignment
        # confirms.
        return None

    cls = _classify(pair)
    if cls is None:
        # Pair not in our broker universe — skip silently.
        return None
    method = "fib" if kind == "fib" else _method_for(pair)
    r_size = _R_per_trade(method)

    # Stable idempotency key. The {kind} suffix lets a single setup
    # produce one wick signal AND one fib signal simultaneously without
    # collision (different entry/stop/target, different R sizing, but
    # the same creator candle).
    sig_id = f"{pair}:{armed_at_ms}:{prefix}"

    return {
        "id": sig_id,
        "pair": pair,
        "sym": pair.upper(),
        "cls": cls,
        "method": method,
        "r_size": r_size,
        "dir": aligned,
        "state": state,
        "entry": entry,
        # Stop / target are computed downstream by the detector; if
        # they're missing from alerts-state.json the EA must fall back
        # to its own ATR-based defaults (documented in the EA).
        "stop":    info.get("sig_stop")    if kind == "wick" else info.get("sig_fib_stop"),
        "target":  info.get("sig_target")  if kind == "wick" else info.get("sig_fib_target"),
        "ew":      info.get("ew"),
        "tl":      info.get("tl"),
        "nw":      info.get("nw"),
        "cl":      info.get("cl"),
        "armedAt":     armed_at_ms,
        "triggeredAt": trig_at_ms,
        "lastSeenAt":  now_ms,
        "source":      "server-detector",
    }


def build_signals(state: dict) -> dict:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    pairs = state.get("pairs", {}) or {}
    out: list[dict] = []

    for pair, info in pairs.items():
        if not isinstance(info, dict):
            continue
        for kind in ("wick", "fib"):
            row = _signal_row(pair, info, kind, now_ms)
            if row is not None:
                out.append(row)

    # Newest-first ordering so the EA can early-exit on the first
    # already-seen id without paging through stale rows.
    out.sort(key=lambda r: r.get("armedAt") or 0, reverse=True)

    # Summary stats — handy for monitoring + the dashboard's signal-log
    # diagnostic strip.
    armed = sum(1 for r in out if r["state"] == "armed")
    triggered = sum(1 for r in out if r["state"] == "triggered")
    invalidated = sum(1 for r in out if r["state"] == "invalidated")

    return {
        "schema_version": SCHEMA_VERSION,
        "generated": datetime.now(timezone.utc).isoformat(),
        "generated_ms": now_ms,
        "detector_last_check": state.get("updated"),
        "counts": {
            "total": len(out),
            "armed": armed,
            "triggered": triggered,
            "invalidated": invalidated,
        },
        "signals": out,
    }


def main() -> int:
    if not ALERT_STATE_PATH.exists():
        print(f"[build_signals_json] {ALERT_STATE_PATH} not found — nothing to do",
              file=sys.stderr)
        return 0
    try:
        state = json.loads(ALERT_STATE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[build_signals_json] failed to parse {ALERT_STATE_PATH}: {e}",
              file=sys.stderr)
        return 1
    payload = build_signals(state)
    SIGNALS_OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    c = payload["counts"]
    print(f"[build_signals_json] wrote {SIGNALS_OUT_PATH}: "
          f"{c['total']} signals "
          f"({c['armed']}A / {c['triggered']}T / {c['invalidated']}I)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
