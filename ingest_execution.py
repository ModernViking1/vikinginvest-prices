#!/usr/bin/env python3
"""
ingest_execution.py — Phase 3.5 of the broker-bridge roadmap.

Validates one cBot execution event (received via the
.github/workflows/ingest-cbot-execution.yml workflow) and appends it
to executions.json with dedup, bounded retention, and schema enforcement.

Run path:
    workflow → env PAYLOAD = JSON.stringify(github.event.client_payload)
             → this script reads PAYLOAD, validates, appends.

Idempotency:
    Dedup key = (signal_id, event, ts). The cBot is allowed to retry
    after a network failure — a duplicate dispatch is silently dropped.

Retention:
    Keep the most recent EXEC_RETENTION_DAYS days of events. Older
    rows are pruned on each ingestion so the file stays small enough
    to fit on the dashboard's CDN cache budget.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

EXEC_PATH = Path("executions.json")
SCHEMA_VERSION = 1
EXEC_RETENTION_DAYS = 365
HARD_CAP_ROWS = 10000  # bounded growth — well under any CDN/git push concern

REQUIRED_FIELDS = {"event", "signal_id", "pair", "dir", "ts"}
ALLOWED_EVENTS  = {"placed", "rejected", "closed"}
ALLOWED_MODES   = {"demo", "live", None, ""}


def _load_existing() -> dict:
    if not EXEC_PATH.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "generated": None,
            "counts": {"total": 0, "placed": 0, "rejected": 0, "closed": 0,
                       "target_hits": 0, "stop_hits": 0},
            "executions": [],
        }
    try:
        return json.loads(EXEC_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"::warning::existing executions.json could not be parsed ({e}) — rebuilding")
        return {
            "schema_version": SCHEMA_VERSION,
            "generated": None,
            "counts": {"total": 0, "placed": 0, "rejected": 0, "closed": 0,
                       "target_hits": 0, "stop_hits": 0},
            "executions": [],
        }


def _validate(row: dict) -> str | None:
    """Return None on success, error string on failure."""
    missing = REQUIRED_FIELDS - set(row.keys())
    if missing:
        return f"missing fields: {sorted(missing)}"
    if row["event"] not in ALLOWED_EVENTS:
        return f"event '{row['event']}' not in {ALLOWED_EVENTS}"
    if row["dir"] not in ("bull", "bear", None, ""):
        return f"dir '{row['dir']}' must be bull|bear"
    if row.get("account_mode") not in ALLOWED_MODES:
        return f"account_mode '{row.get('account_mode')}' must be demo|live"
    if not isinstance(row["ts"], (int, float)):
        return f"ts must be epoch ms (number), got {type(row['ts']).__name__}"
    if row["ts"] < 1_700_000_000_000 or row["ts"] > 4_000_000_000_000:
        return f"ts {row['ts']} out of plausible range (epoch ms expected)"
    return None


def _dedup_key(row: dict) -> tuple:
    return (row.get("signal_id"), row.get("event"), int(row.get("ts", 0)))


def _refresh_counts(payload: dict) -> None:
    c = {"total": 0, "placed": 0, "rejected": 0, "closed": 0,
         "target_hits": 0, "stop_hits": 0}
    for r in payload.get("executions", []):
        c["total"] += 1
        ev = r.get("event")
        if ev in c:
            c[ev] += 1
        reason = r.get("reason")
        if reason == "target-hit":
            c["target_hits"] += 1
        elif reason == "stop-hit":
            c["stop_hits"] += 1
    payload["counts"] = c


def _prune(payload: dict) -> int:
    """Drop rows older than the retention window. Returns number dropped."""
    if not payload.get("executions"):
        return 0
    cutoff_ms = int(
        (datetime.now(timezone.utc).timestamp() - EXEC_RETENTION_DAYS * 86400) * 1000
    )
    before = len(payload["executions"])
    payload["executions"] = [
        r for r in payload["executions"] if (r.get("ts") or 0) >= cutoff_ms
    ]
    # Hard cap as a defensive backstop.
    if len(payload["executions"]) > HARD_CAP_ROWS:
        payload["executions"] = payload["executions"][-HARD_CAP_ROWS:]
    return before - len(payload["executions"])


def main() -> int:
    raw = os.environ.get("PAYLOAD")
    if not raw:
        print("::error::PAYLOAD env not set — workflow misconfigured")
        return 1
    try:
        row = json.loads(raw)
    except Exception as e:
        print(f"::error::PAYLOAD is not valid JSON: {e}")
        return 1

    err = _validate(row)
    if err:
        print(f"::error::Invalid execution payload: {err}")
        return 1

    payload = _load_existing()
    payload["schema_version"] = SCHEMA_VERSION

    # Dedup — drop if we've already ingested this exact event.
    key = _dedup_key(row)
    if any(_dedup_key(r) == key for r in payload.get("executions", [])):
        print(f"[ingest_execution] duplicate dropped: signal_id={row.get('signal_id')} event={row.get('event')} ts={row.get('ts')}")
        # Return 0 + don't write the file → the commit step sees no
        # change and exits cleanly. The cBot's retry is idempotent.
        _refresh_counts(payload)
        return 0

    payload.setdefault("executions", []).append(row)
    # Keep ASC chronological for deterministic diffs.
    payload["executions"].sort(key=lambda r: r.get("ts") or 0)
    pruned = _prune(payload)
    _refresh_counts(payload)
    payload["generated"] = datetime.now(timezone.utc).isoformat()

    EXEC_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    c = payload["counts"]
    print(f"[ingest_execution] appended event={row['event']} signal_id={row['signal_id']} "
          f"(total={c['total']}, +{c['placed']}P/{c['rejected']}R/{c['closed']}C, "
          f"pruned={pruned})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
