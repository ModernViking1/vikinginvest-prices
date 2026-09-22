"""cam_rev LIVE-fill tracker — watches the live-vs-observer gap as real fills accumulate.

Reads the committed swing-executions.json (written by the swing cBot), summarises every RESOLVED
cam_rev fill, and compares live realised R against the 3-yr backtest benchmark (+0.525R, 76.3% WR,
FX/index/comm gated). Also quantifies STOP-SLIPPAGE — losses printed beyond -1R, the real-world
"not best fill" cost. Breaks down by account mode, class and pair. Writes cam-rev-live-tracker.json
and prints a readable summary. No external fetch; runs fast in CI on a schedule.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone

from detect_triggers import PAIR_CLASS
try:
    from unified_shadow_harness import CAM_CRYPTO_PILOT
except Exception:
    CAM_CRYPTO_PILOT = {"btcusd", "ethusd", "xrpusd", "solusd"}

EXEC = "swing-executions.json"
OUT = "cam-rev-live-tracker.json"
BT_EXP, BT_WR = 0.525, 76.3         # 3-yr backtest benchmark (FX/index/comm, gated)
# Per-regime 3-yr benchmarks (cam_gap_session_backtest.py, C1) — what regime-tiered sizing bets on.
# The live split should track these once enough tagged fills accrue; if it doesn't, the tilt is wrong.
BT_REGIME = {"strong": {"exp": 0.682, "wr": 84.0}, "range": {"exp": 0.486, "wr": 74.0}}


def _ts(e):
    t = e.get("ts")
    if isinstance(t, (int, float)) and t > 1e11:
        t /= 1000.0
    return t if isinstance(t, (int, float)) else 0


def _cls(pair):
    return PAIR_CLASS.get(pair) or ("crypto" if pair in CAM_CRYPTO_PILOT else "other")


def _agg(rows):
    r = [e["realized_r"] for e in rows]
    n = len(r)
    if not n:
        return None
    w = sum(1 for x in r if x > 0)
    losses = [x for x in r if x <= 0]
    slip = [x for x in r if x < -1.0]          # printed worse than -1R => stop slippage
    return {
        "n": n, "wr": round(100.0 * w / n, 1),
        "avg_r": round(sum(r) / n, 3), "total_r": round(sum(r), 2),
        "avg_win": round(sum(x for x in r if x > 0) / w, 3) if w else 0.0,
        "avg_loss": round(sum(losses) / len(losses), 3) if losses else 0.0,
        "slip_beyond_1R": len(slip),
        "avg_slip_excess": round(sum(x + 1 for x in slip) / len(slip), 3) if slip else 0.0,
        "worst_r": round(min(r), 3),
    }


def main():
    try:
        d = json.load(open(EXEC))
    except Exception as e:
        print(f"cannot read {EXEC}: {e}", flush=True)
        return 1
    ex = d.get("executions") or d.get("rows") or (d if isinstance(d, list) else [])
    closed = sorted(
        [e for e in ex if e.get("strategy") == "cam_rev" and e.get("event") == "closed"
         and isinstance(e.get("realized_r"), (int, float))],
        key=_ts)

    overall = _agg(closed)
    by_mode = defaultdict(list); by_class = defaultdict(list); by_pair = defaultdict(list)
    by_regime = defaultdict(list)
    for e in closed:
        by_mode[e.get("account_mode") or "?"].append(e)
        by_class[_cls(e.get("pair"))].append(e)
        by_pair[e.get("pair")].append(e)
        by_regime[e.get("regime") or "untagged"].append(e)   # regime tag added 2026-09-22; old fills = untagged

    def _regime_block(name, rows):
        a = _agg(rows)
        if a and name in BT_REGIME:
            a = dict(a, bt_exp=BT_REGIME[name]["exp"], bt_wr=BT_REGIME[name]["wr"],
                     gap_exp=round(a["avg_r"] - BT_REGIME[name]["exp"], 3),
                     gap_wr=round(a["wr"] - BT_REGIME[name]["wr"], 1))
        return a
    regime_out = {r: _regime_block(r, v) for r, v in sorted(by_regime.items())}

    now = datetime.now(timezone.utc)
    out = {
        "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "strategy": "cam_rev",
        "backtest_benchmark": {"exp_r": BT_EXP, "wr_pct": BT_WR,
                               "note": "3-yr FX/index/comm gated, frictionless"},
        "overall_live": overall,
        "gap_vs_backtest": ({"exp_r": round(overall["avg_r"] - BT_EXP, 3),
                             "wr_pct": round(overall["wr"] - BT_WR, 1)} if overall else None),
        "by_account_mode": {m: _agg(v) for m, v in sorted(by_mode.items())},
        "by_class": {c: _agg(v) for c, v in sorted(by_class.items())},
        "by_regime": regime_out,
        "regime_benchmark": BT_REGIME,
        "by_pair": dict(sorted(({p: _agg(v) for p, v in by_pair.items()}).items(),
                               key=lambda kv: -(kv[1]["total_r"] if kv[1] else 0))),
        "recent": [{
            "when": datetime.utcfromtimestamp(_ts(e)).strftime("%Y-%m-%d %H:%M"),
            "pair": e.get("pair"), "class": _cls(e.get("pair")),
            "mode": e.get("account_mode"), "r": round(e["realized_r"], 3),
            "reason": e.get("reason"),
        } for e in closed[-25:]],
    }
    with open(OUT, "w") as f:
        json.dump(out, f, indent=1)

    # readable summary
    if not overall:
        print("no resolved cam_rev fills yet — tracker seeded empty.", flush=True)
    else:
        g = out["gap_vs_backtest"]
        print(f"cam_rev LIVE — {overall['n']} fills | WR {overall['wr']}% (bt {BT_WR}%, "
              f"gap {g['wr_pct']:+}) | avg {overall['avg_r']:+}R (bt {BT_EXP:+}, gap {g['exp_r']:+}R) "
              f"| total {overall['total_r']:+}R", flush=True)
        print(f"  stop-slippage: {overall['slip_beyond_1R']} loss(es) beyond -1R, "
              f"avg excess {overall['avg_slip_excess']:+}R, worst {overall['worst_r']}R", flush=True)
        for c, v in out["by_class"].items():
            if v:
                print(f"  {c:7} n={v['n']:<4} WR={v['wr']:>4}%  avg={v['avg_r']:+.3f}R  tot={v['total_r']:+.1f}", flush=True)
        print("  regime-tiered sizing (R+ strong x1.5 / R- range x0.75) — live vs 3-yr benchmark:", flush=True)
        for r in ("strong", "range", "untagged"):
            v = out["by_regime"].get(r)
            if v:
                extra = (f"  bt {v['bt_exp']:+.3f}R/{v['bt_wr']:.0f}% (gap {v['gap_exp']:+.3f}R)"
                         if "bt_exp" in v else "  (pre-tag fills)")
                print(f"    {('R+ '+r) if r=='strong' else ('R- '+r) if r=='range' else r:12} "
                      f"n={v['n']:<4} WR={v['wr']:>4}%  avg={v['avg_r']:+.3f}R  tot={v['total_r']:+.1f}{extra}", flush=True)
    return 0


if __name__ == "__main__":
    main()
