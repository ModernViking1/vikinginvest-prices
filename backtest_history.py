"""Server-side deep backtest → compact summary the dashboard reads (option 1).

Runs the FULL shadow-harness scoring over a given OHLC file (12mo today, 24mo when CI
fetches deeper), then aggregates a small per-strategy summary — expectancy, WR, OOS halves,
total R, max drawdown, plus a sampled book equity curve. Applies the live trend-quality gate
so the numbers reflect what the cBot actually trades, and also reports the ungated figures so
the gate's effect is visible on the longer window.

The heavy part (scoring dozens of strategies over years of bars) runs in CI; only the small
`backtest-summary.json` is published, so the dashboard shows a 2-year track record without
loading 2 years of raw bars. Never commits the deep OHLC and never touches swing-shadow-log.json
(it points the harness at a throwaway log).

Run:  python backtest_history.py [--hist historical-ohlc.json] [--out backtest-summary.json]
"""
import argparse, json, os, tempfile, math, bisect
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))


def _agg(rs):
    n = len(rs)
    if not n:
        return 0, 0.0, 0.0
    return n, 100 * sum(1 for r in rs if r > 0) / n, sum(rs) / n


def _max_dd(seq):
    """Max drawdown (in R) on the cumulative-R equity curve of a time-ordered R sequence."""
    cum = 0.0; peak = 0.0; dd = 0.0
    for r in seq:
        cum += r; peak = max(peak, cum); dd = min(dd, cum - peak)
    return dd


def _equity_curve(pairs_r, max_pts=400):
    """(ts-sorted [(ts,r)]) -> down-sampled [[ts, cumR], ...] for the dashboard."""
    pairs_r = sorted(pairs_r)
    cum = 0.0; pts = []
    for ts, r in pairs_r:
        cum += r; pts.append((ts, cum))
    if len(pts) <= max_pts:
        return [[int(t), round(c, 3)] for t, c in pts]
    step = len(pts) / max_pts
    return [[int(pts[int(i*step)][0]), round(pts[int(i*step)][1], 3)] for i in range(max_pts)]


MIN_MONTHS_FOR_PERFECT = 12   # need >=1yr of trading months before "positive every month" means anything


def _month_key(ts):
    dt = datetime.fromtimestamp(ts, timezone.utc)
    return "%04d-%02d" % (dt.year, dt.month)


def _monthly(rows, series_for=None):
    """Per-strategy calendar-month P&L. A month counts only if the strategy traded in it
       (a quiet month is 'no signal', not 'a loss'). 'perfect' = net-positive in EVERY
       trading month over at least MIN_MONTHS_FOR_PERFECT months — the real test behind a
       'profitable every month' claim. series (the YYYY-MM -> {n,r} detail) is emitted only
       for strategies in series_for, to keep the summary compact."""
    by = {}
    for s in rows:
        by.setdefault(s["strategy"], {}).setdefault(_month_key(s["entry_ts"]), []).append(s["r"])
    out = {}
    for st, months in by.items():
        ser = {mk: {"n": len(v), "r": round(sum(v), 2)} for mk, v in sorted(months.items())}
        rs = [m["r"] for m in ser.values()]
        traded = len(rs)
        pos = sum(1 for r in rs if r > 0)
        neg = sum(1 for r in rs if r < 0)
        row = {"months_traded": traded, "months_positive": pos, "months_negative": neg,
               "pct_positive": round(100 * pos / traded, 1) if traded else 0.0,
               "worst_month_r": round(min(rs), 2) if rs else 0.0,
               "best_month_r": round(max(rs), 2) if rs else 0.0,
               "perfect": bool(traded >= MIN_MONTHS_FOR_PERFECT and neg == 0 and pos == traded)}
        if series_for is None or st in series_for:
            row["series"] = ser
        out[st] = row
    return out


def _monthly_book(book_seq):
    """Aggregate book P&L per calendar month — 'is the portfolio positive every month?'."""
    months = {}
    for ts, r in book_seq:
        months.setdefault(_month_key(ts), []).append(r)
    ser = {mk: {"n": len(v), "r": round(sum(v), 2)} for mk, v in sorted(months.items())}
    rs = [m["r"] for m in ser.values()]
    return {"months_traded": len(rs),
            "months_positive": sum(1 for r in rs if r > 0),
            "months_negative": sum(1 for r in rs if r < 0),
            "worst_month_r": round(min(rs), 2) if rs else 0.0,
            "best_month_r": round(max(rs), 2) if rs else 0.0,
            "series": ser}


def run(hist_path, out_path):
    import unified_shadow_harness as H
    from trend_regime import build_regime, passes_gate

    # Point the harness at the deep file + a throwaway log so we never touch the live forward log.
    tmp_log = tempfile.NamedTemporaryFile(prefix="bt_log_", suffix=".json", delete=False).name
    os.remove(tmp_log)                       # start the harness from an empty log
    H.HIST = hist_path
    H.LOG = tmp_log
    print(f"[backtest] scoring harness over {hist_path} ...", flush=True)
    H.main()                                 # scores every detector over the file -> tmp_log
    log = json.load(open(tmp_log))
    try:
        os.remove(tmp_log)
    except OSError:
        pass

    sigs = [s for s in log.get("signals", {}).values() if s.get("status") == "resolved" and "r" in s]
    print(f"[backtest] resolved signals: {len(sigs)}", flush=True)

    # Regime series per (pair, tf) from the same deep file, for the live trend gate.
    doc = json.load(open(hist_path)); pairs = doc.get("pairs", {})
    regime = {}
    for pk, layers in pairs.items():
        for tf in ("h1", "m15"):
            bars = H._bars_norm(layers.get(tf) or [])
            if len(bars) >= 40:
                regime[(pk, tf)] = build_regime(bars)

    def gated(s):
        reg = regime.get((s["pair"], s.get("tf", "h1"))) or regime.get((s["pair"], "h1"))
        if reg is None:
            return True
        return passes_gate(s["strategy"], reg, s["entry_ts"])

    # Data window from the OHLC itself (robust) — a few detectors can stamp an outlier entry_ts
    # off an old daily pivot, so don't derive the window from signal min/max.
    h1_ts = [H._bars_norm(l.get("h1") or [])[0]["_ts"] for l in pairs.values() if len(l.get("h1") or []) > 1]
    h1_end = [H._bars_norm(l.get("h1") or [])[-1]["_ts"] for l in pairs.values() if len(l.get("h1") or []) > 1]
    data_start = int(min(h1_ts)) if h1_ts else None
    data_end = int(max(h1_end)) if h1_end else None
    window_days = round((data_end - data_start) / 86400) if data_start and data_end else doc.get("window_days", 0)

    ts_all = sorted(s["entry_ts"] for s in sigs)
    med = ts_all[len(ts_all)//2] if ts_all else 0

    def summarize(rows):
        by = {}
        for s in rows:
            by.setdefault(s["strategy"], []).append((s["entry_ts"], s["r"]))
        out = {}
        for st, tr in by.items():
            tr.sort()
            seq = [r for _, r in tr]
            n, wr, exp = _agg(seq)
            m = len(seq)//2
            _, _, eh = _agg(seq[:m]); _, _, es = _agg(seq[m:])
            out[st] = {"n": n, "wr": round(wr, 1), "exp": round(exp, 4),
                       "oos_1st": round(eh, 4), "oos_2nd": round(es, 4),
                       "total_r": round(sum(seq), 1), "max_dd_r": round(_max_dd(seq), 1)}
        return out

    live = [s for s in sigs if gated(s)]
    book_seq = sorted((s["entry_ts"], s["r"]) for s in live)
    nb, wb, eb = _agg([r for _, r in book_seq])
    nu, wu, eu = _agg([s["r"] for s in sigs])

    # ---- per-pair breakdown for the LIVE cBot strategies (to spot pairs to demote per class) ----
    try:
        from observer_review import LIVE as LIVE_STRATS
    except Exception:
        LIVE_STRATS = set()
    try:
        from detect_triggers import PAIR_CLASS as _PC
    except Exception:
        _PC = {}
    _by_sp = {}
    for s in live:
        if s["strategy"] in LIVE_STRATS:
            _by_sp.setdefault(s["strategy"], {}).setdefault(s.get("pair", "?"), []).append(s["r"])
    live_by_pair = {}
    for st, pairs_d in _by_sp.items():
        row = {}
        for pk, seq in pairs_d.items():
            if len(seq) < 15:        # skip thin pairs — not enough to judge
                continue
            n, wr, exp = _agg(seq); m = len(seq) // 2
            _, _, e1 = _agg(seq[:m]); _, _, e2 = _agg(seq[m:])
            row[pk] = {"cls": _PC.get(pk, "?"), "n": n, "wr": round(wr, 1), "exp": round(exp, 4),
                       "oos_1st": round(e1, 4), "oos_2nd": round(e2, 4), "total_r": round(sum(seq), 1)}
        if row:
            live_by_pair[st] = row

    summary = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_start": data_start,
        "data_end": data_end,
        "window_days": window_days,
        "book_gated":   {"n": nb, "wr": round(wb, 1), "exp": round(eb, 4),
                         "total_r": round(sum(r for _, r in book_seq), 1),
                         "max_dd_r": round(_max_dd([r for _, r in book_seq]), 1)},
        "book_ungated": {"n": nu, "wr": round(wu, 1), "exp": round(eu, 4)},
        "equity_curve_gated": _equity_curve(book_seq),
        "strategies_gated":   summarize(live),
        "strategies_ungated": summarize(sigs),
        "live_by_pair":       live_by_pair,
        # Per-month P&L (gated / live-emulated). Full YYYY-MM series only for the LIVE roster;
        # every gated strategy still gets the months_positive / worst_month / perfect summary so
        # we can answer "does any strategy stay positive every month?" across the whole book.
        "monthly_gated":      _monthly(live, series_for=LIVE_STRATS),
        "book_monthly_gated": _monthly_book(book_seq),
        "perfect_months":     sorted(st for st, v in _monthly(live).items() if v["perfect"]),
        "min_months_for_perfect": MIN_MONTHS_FOR_PERFECT,
    }
    with open(out_path, "w") as f:
        json.dump(summary, f, separators=(",", ":"))
    print(f"[backtest] wrote {out_path}: book gated exp={eb:+.4f}R (n={nb}) vs ungated {eu:+.4f}R (n={nu}), "
          f"~{summary['window_days']}d", flush=True)
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hist", default=os.path.join(_HERE, "historical-ohlc.json"))
    ap.add_argument("--out", default=os.path.join(_HERE, "backtest-summary.json"))
    args = ap.parse_args()
    run(args.hist, args.out)


if __name__ == "__main__":
    main()
