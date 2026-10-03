"""cam_rev LIVE-vs-MODEL per-trade reconciliation.

WHY
---
The weekly review shows cam_rev live -0.226R / 43% WR against a model of +0.472R / 75%. A 30-point
WR gap is NOT explained by slippage (slippage shrinks R, it doesn't flip winners to losers). This
tool joins each real cBot fill (swing-executions.json) to the model signal it should correspond to
(swing-shadow-log.json) and classifies WHERE the gap is created:

  MATCHED  — same pair, same direction, trigger within +-1 m15 bar (900s) of a model signal.
             Apples-to-apples: live vs model on the SAME trade isolates pure execution cost.
  DRIFTED  — a model cam_rev exists on that pair that DAY, but the live fill triggered on a
             DIFFERENT bar (>1 bar away). The cBot took a different rejection than the model.
  ORPHAN   — no model cam_rev on that pair that day at all. The cBot traded a setup the model
             never generated (wrong levels / wrong session bar / stale daily pivots).

If MATCHED live WR ~= model WR but DRIFTED+ORPHAN are the losers, the gap is SELECTION (the cBot
is trading the wrong bars), and the fix is in the feed's trigger/level alignment — not the edge.

Reads only committed logs; commits nothing.

  python cam_rev_reconcile.py
"""
import argparse, bisect, json, os
from collections import defaultdict
import datetime as D

_HERE = os.path.dirname(os.path.abspath(__file__))
BAR = 900                      # m15 seconds
SANE_R = 6.0                   # drop broken fills (gbreak -59.97R style)


def _utc(ts):
    return D.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _day(ts):
    return D.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")


def load_live(path):
    """{signal_id: fill} — join 'placed' (entry/bracket/placed_ts) with 'closed' (realized_r/reason)
    by position_id. One row per live cam_rev position."""
    ex = json.load(open(os.path.join(_HERE, path))).get("executions", [])
    placed, closed = {}, {}
    for r in ex:
        sid = r.get("signal_id") or ""
        if not sid.startswith("cam_rev:"):
            continue
        pid = r.get("position_id")
        if r.get("event") == "placed":
            placed[pid] = r
        elif r.get("event") == "closed":
            closed[pid] = r
    out = {}
    for pid, p in placed.items():
        c = closed.get(pid)
        sid = p["signal_id"]
        _, pair, ts = sid.split(":")
        out[pid] = {
            "sid": sid, "pair": pair, "trigger_ts": int(ts), "dir": p.get("dir"),
            "entry": p.get("entry_filled"), "stop": p.get("stop"), "target": p.get("target"),
            "placed_ts": p.get("ts", 0) / 1000.0,
            "realized_r": (c.get("realized_r") if c else None),
            "reason": (c.get("reason") if c else "open"),
        }
    return out


def load_model(path):
    """{pair: sorted[(entry_ts, sig)]} and {sid: sig} for cam_rev model signals."""
    sigs = json.load(open(os.path.join(_HERE, path))).get("signals", {})
    by_pair = defaultdict(list)
    by_id = {}
    for k, v in sigs.items():
        if v.get("strategy") != "cam_rev":
            continue
        by_pair[v["pair"]].append((v["entry_ts"], v))
        by_id[k] = v
    for p in by_pair:
        by_pair[p].sort()
    return by_pair, by_id


def nearest(by_pair, pair, ts):
    arr = by_pair.get(pair)
    if not arr:
        return None, None
    times = [t for t, _ in arr]
    j = bisect.bisect_left(times, ts)
    best = None
    for jj in (j - 1, j, j + 1):
        if 0 <= jj < len(times):
            if best is None or abs(times[jj] - ts) < abs(best[0] - ts):
                best = (times[jj], arr[jj][1])
    return best if best else (None, None)


def agg(rs):
    rs = [r for r in rs if r is not None]
    n = len(rs)
    if not n:
        return (0, 0.0, 0.0, 0.0)
    return (n, 100.0 * sum(1 for r in rs if r > 0) / n, sum(rs) / n, sum(rs))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", default="swing-executions.json")
    ap.add_argument("--model", default="swing-shadow-log.json")
    args = ap.parse_args()

    live = load_live(args.live)
    by_pair, by_id = load_model(args.model)

    buckets = {"MATCHED": [], "DRIFTED": [], "ORPHAN": []}
    matched_detail = []
    hour_live, hour_model = defaultdict(int), defaultdict(int)
    dropped = 0
    for pid, f in live.items():
        rr = f["realized_r"]
        if rr is not None and abs(rr) > SANE_R:
            dropped += 1
            continue
        hour_live[int(D.datetime.utcfromtimestamp(f["trigger_ts"]).strftime("%H"))] += 1
        mt, ms = nearest(by_pair, f["pair"], f["trigger_ts"])
        if ms is not None:
            hour_model[int(D.datetime.utcfromtimestamp(mt).strftime("%H"))] += 1
        # classify
        if ms is not None and abs(mt - f["trigger_ts"]) <= BAR and ms.get("dir") == f["dir"]:
            kind = "MATCHED"
        elif ms is not None and _day(mt) == _day(f["trigger_ts"]):
            kind = "DRIFTED"
        else:
            kind = "ORPHAN"
        buckets[kind].append(rr)
        if kind == "MATCHED" and rr is not None and ms.get("r") is not None:
            mR = abs(f["entry"] - f["stop"]) or 1e-9
            matched_detail.append({
                "pair": f["pair"], "live_r": rr, "model_r": ms["r"],
                "d_entry_R": abs(f["entry"] - ms["entry"]) / mR,
                "lat_min": (f["placed_ts"] - f["trigger_ts"]) / 60.0 if f["placed_ts"] else None,
            })

    print("=" * 74)
    print("cam_rev LIVE-vs-MODEL reconciliation")
    print("=" * 74)
    tot = sum(len(v) for v in buckets.values())
    print(f"live cam_rev positions: {tot}" + (f"  (+{dropped} broken fills dropped)" if dropped else ""))
    print(f"model cam_rev signals:  {sum(len(v) for v in by_pair.values())}\n")
    print(f"{'bucket':<9}{'n':>4} {'share':>6} {'liveWR':>7} {'liveExp':>9} {'liveTot':>9}")
    for k in ("MATCHED", "DRIFTED", "ORPHAN"):
        n, wr, exp, t = agg(buckets[k])
        share = 100.0 * (len(buckets[k])) / max(tot, 1)
        print(f"{k:<9}{len(buckets[k]):>4} {share:>5.0f}% {wr:>6.0f}% {exp:>+8.3f}R {t:>+8.1f}R")
    print()
    # apples-to-apples on MATCHED
    if matched_detail:
        lr = [m["live_r"] for m in matched_detail]
        mr = [m["model_r"] for m in matched_detail]
        n, lwr, lexp, _ = agg(lr)
        _, mwr, mexp, _ = agg(mr)
        de = sum(m["d_entry_R"] for m in matched_detail) / len(matched_detail)
        lats = [m["lat_min"] for m in matched_detail if m["lat_min"] is not None]
        print("ON THE SAME TRADES (MATCHED) — isolates pure execution:")
        print(f"  live  n={n}  WR={lwr:.0f}%  exp={lexp:+.3f}R")
        print(f"  model n={n}  WR={mwr:.0f}%  exp={mexp:+.3f}R")
        print(f"  mean |entry - model entry| = {de:.2f}R of risk   "
              f"median fill latency = {sorted(lats)[len(lats)//2]:.0f} min" if lats else "")
        flips = sum(1 for m in matched_detail if m["model_r"] > 0 and m["live_r"] is not None and m["live_r"] < 0)
        print(f"  model-win -> live-loss flips: {flips}/{n}")
    print("\nTRIGGER HOUR (UTC) — live fills vs the model signals they map to:")
    for h in sorted(set(hour_live) | set(hour_model)):
        lv = hour_live.get(h, 0); mo = hour_model.get(h, 0)
        print(f"  {h:02d}:00  live {'#'*lv:<22} {lv:>2}   model {'#'*mo:<22} {mo:>2}")
    print("\nReading it: a large ORPHAN/DRIFTED share with low WR = the cBot is trading DIFFERENT bars")
    print("than the model (selection gap — fix the feed's trigger/level alignment). A MATCHED live WR")
    print("far below model WR = execution cost on the right trades (slippage/commission/late fills).")


if __name__ == "__main__":
    main()
