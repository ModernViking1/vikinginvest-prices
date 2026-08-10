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
KILL_SWITCH_PATH = Path("kill-switch.json")
SIGNALS_OUT_PATH = Path("signals.json")
SCHEMA_VERSION = 2  # bumped 2026-06-14 — adds kill_switch field to envelope

# Per-class methodology — keep parity with _btMethodFor() in the
# dashboard JS. Wick pairs trade at 1.0R, Fib pairs at 0.5R.
WICK_CLASSES = {"major", "minor", "crypto"}
FIB_CLASSES = {"comm", "index"}

# 2026-07-09 — LIVE-CLASS gate. Only comm + crypto route to signals.json (the
# cBot's trade feed). The live-vs-backtest reconciliation showed the headline
# edge was an entry-fill artifact; under a realistic limit fill only the full
# 4/4 cohort clears cost, and only on comm + crypto (index/major 4/4 stay
# net-negative in the honest backtest, minor sample too thin). The macd-primary
# detector already sends index/major/minor to the shadow log; this gate applies
# the same policy to the structural wick/fib signals so nothing outside
# comm/crypto reaches the broker while we forward-confirm the cohort live. The
# other classes still appear in directions.json / the dashboard for tracking.
# 2026-08-04 — COMMODITIES DEMOTED to shadow. Realised live intraday fills were
# negative on comm across every method (fib/comm -0.34R, wick/comm -0.78R,
# macdp/comm -0.47R post-gate, n~64); crypto is the only net-positive live cell
# (wick/crypto +0.10R). Only crypto now reaches the cBot; comm reverts to
# observer/tracking (directions.json + dashboard), same as index/major/minor.
LIVE_CLASSES = {"crypto"}

# 2026-08-08 — macdp (MACD-cross) and wick (wick-reversal) DEMOTED from the cBot feed:
# live realised fills were clearly negative (macdp -44.4R / 45% WR, wick -18.0R / 31% WR
# at ~1:1 RR). Held back here (detector still runs + drives the dashboard/alerts).
# mmove_m15 promoted to the main live strategy in its place (see mmove_live.py).
DEMOTED_METHODS = {"macdp", "wick"}

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
    macdp_dir = None
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
    elif kind == "macdp":
        # 2026-06-16k — MACD-primary parallel trigger. Index-class only
        # at production; minor-class entries are 'shadow-triggered' and
        # skipped below so the cBot never sees them.
        if bool(info.get("sig_macdp_shadow")):
            return None  # MINOR shadow — never emit to signals.json
        state = info.get("sig_macdp_state")
        entry = info.get("sig_macdp_entry")
        # MACD-primary has no separate creator concept — the trigger bar
        # IS the creator. Use the trigger_ts for both fields so the EA's
        # idempotency key (armedAt) is stable.
        creator_ts = info.get("sig_macdp_trigger_ts")
        trigger_ts = info.get("sig_macdp_trigger_ts")
        prefix = "macdp"
        macdp_dir = info.get("sig_macdp_dir")
    elif kind == "divg":
        # 2026-06-17 — MACD-divergence parallel trigger. Index-only at
        # production. Same envelope shape as macdp — the divergence
        # trigger bar IS the creator.
        state = info.get("sig_divg_state")
        entry = info.get("sig_divg_entry")
        creator_ts = info.get("sig_divg_trigger_ts")
        trigger_ts = info.get("sig_divg_trigger_ts")
        prefix = "divg"
        macdp_dir = info.get("sig_divg_dir")
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

    # Direction sourcing: MACD-primary uses the MACD cross direction
    # (which lives in sig_macdp_dir); MACD-divergence uses sig_divg_dir
    # (assigned to macdp_dir above for code reuse). Everything else
    # uses aligned_dir.
    aligned = macdp_dir if kind in ("macdp", "divg") else info.get("aligned_dir")
    if aligned not in ("bull", "bear"):
        return None

    cls = _classify(pair)
    if cls is None:
        # Pair not in our broker universe — skip silently.
        return None
    # 2026-07-09 — LIVE-CLASS gate: only comm + crypto trade live. Everything
    # else (FX majors/minors, indices) is observation-only until the 4/4
    # cohort is live-confirmed. macdp index/major/minor are already dropped
    # above via the shadow flag; this also stops their structural wick/fib
    # signals from reaching the cBot.
    if cls not in LIVE_CLASSES:
        return None
    # MACD-primary + MACD-divergence sizing:
    # 2026-06-22 — macdp now fires on MAJOR/MINOR/CRYPTO too, not just
    # INDEX. Forcing "fib" half-size on every macdp signal was correct
    # when it was INDEX-only, but on FX pairs the wick variant runs at
    # full 1.0R and the user expects macdp to match. Route per-pair-
    # class instead so macdp on a MAJOR/MINOR/CRYPTO comes through as
    # full-size wick, while INDEX/COMM stays on the half-size fib
    # policy. divg is still INDEX-only at production so it remains
    # half-size (matches the INDEX fib policy).
    if kind == "fib":
        method = "fib"
    elif kind == "macdp":
        method = _method_for(pair)   # class-aware: fib on COMM/INDEX, wick elsewhere
    elif kind == "divg":
        method = "fib"               # INDEX-only at production; half-size matches policy
    else:
        method = _method_for(pair)
    r_size = _R_per_trade(method)

    # Stable idempotency key. The {kind} suffix lets a single setup
    # produce one wick signal AND one fib signal AND one macdp signal
    # simultaneously without collision.
    sig_id = f"{pair}:{armed_at_ms}:{prefix}"

    if kind == "wick":
        stop_val = info.get("sig_stop")
        target_val = info.get("sig_target")
    elif kind == "fib":
        stop_val = info.get("sig_fib_stop")
        target_val = info.get("sig_fib_target")
    elif kind == "macdp":
        stop_val = info.get("sig_macdp_stop")
        target_val = info.get("sig_macdp_target")
    else:  # divg
        stop_val = info.get("sig_divg_stop")
        target_val = info.get("sig_divg_target")

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
        "stop":    stop_val,
        "target":  target_val,
        "ew":      info.get("ew"),
        "tl":      info.get("tl"),
        "nw":      info.get("nw"),
        "cl":      info.get("cl"),
        "armedAt":     armed_at_ms,
        "triggeredAt": trig_at_ms,
        "lastSeenAt":  now_ms,
        "source":      "server-detector",
        # H11 faytterro alignment (macdp only; None for other kinds). The cBot
        # full-sizes when True/None, half-sizes when explicitly False.
        "event_aligned": (info.get("sig_macdp_event_aligned") if kind == "macdp" else None),
    }


def build_signals(state: dict) -> dict:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    pairs = state.get("pairs", {}) or {}
    out: list[dict] = []

    # 2026-06-25 — per-pair cool-off: skip TRIGGERED rows on any pair
    # currently paused. Same helper detect_triggers.py uses to gate
    # Telegram, so server + cBot agree without a state round-trip.
    # Invalidations / armed rows still flow — only triggered (which is
    # what the cBot acts on) is suppressed. Fails open on any error.
    try:
        from detect_triggers import compute_cooloff_pairs
        cooloff_pairs = compute_cooloff_pairs()
    except Exception:
        cooloff_pairs = {}

    for pair, info in pairs.items():
        if not isinstance(info, dict):
            continue
        for kind in ("wick", "fib", "macdp", "divg"):
            if kind in DEMOTED_METHODS:            # 2026-08-08 — held back from the cBot
                continue
            row = _signal_row(pair, info, kind, now_ms)
            if row is None:
                continue
            if pair in cooloff_pairs and row.get("state") == "triggered":
                # Don't append — cBot will not place. Armed / invalidated
                # rows on this pair continue to flow normally.
                continue
            out.append(row)

    # 2026-08-08 — mmove_m15 promoted to the MAIN live intraday strategy (demo). Emitted
    # from its own detector on the m15 OHLC (fail-open), exempt from LIVE_CLASSES so it
    # trades its full validated pocket set {xrpusd, xauusd, xagusd, fra40}, RR2.
    try:
        from mmove_live import build_mmove_rows
        for row in build_mmove_rows(now_ms):
            if row["pair"] in cooloff_pairs and row.get("state") == "triggered":
                continue
            out.append(row)
    except Exception as e:
        print(f"[build_signals_json] mmove_m15 emit skipped: {e}", file=sys.stderr)

    # Newest-first ordering so the EA can early-exit on the first
    # already-seen id without paging through stale rows.
    out.sort(key=lambda r: r.get("armedAt") or 0, reverse=True)

    # Summary stats — handy for monitoring + the dashboard's signal-log
    # diagnostic strip.
    armed = sum(1 for r in out if r["state"] == "armed")
    triggered = sum(1 for r in out if r["state"] == "triggered")
    invalidated = sum(1 for r in out if r["state"] == "invalidated")

    # Phase 2 — kill-switch mirror. The cBot polls kill-switch.json
    # directly, so the canonical "should I trade" answer lives there.
    # We also mirror the state into signals.json so the dashboard can
    # surface "bot paused" without an extra CDN fetch.
    ks_payload = {"killed": False, "reason": None, "updated": None, "updated_by": None}
    if KILL_SWITCH_PATH.exists():
        try:
            ks = json.loads(KILL_SWITCH_PATH.read_text(encoding="utf-8"))
            ks_payload = {
                "killed":     bool(ks.get("killed")),
                "reason":     ks.get("reason"),
                "updated":    ks.get("updated"),
                "updated_by": ks.get("updated_by"),
            }
        except Exception:
            # Tolerate parse failures — broker bridge stays operational
            # even if the kill-switch file is malformed (fail-open by
            # design; explicit kill is the safer default to require).
            pass

    return {
        "schema_version": SCHEMA_VERSION,
        "generated": datetime.now(timezone.utc).isoformat(),
        "generated_ms": now_ms,
        "detector_last_check": state.get("updated"),
        "kill_switch": ks_payload,
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
