"""Investor-grade 3yr monthly net-R series for LIVE cam_rev (FX / index / commodity only).

Reproduces the EXACT methodology behind the dashboard's cam_rev headline (gated, target-bracket
scored) but restricts the universe to the classes cam_rev actually trades LIVE — major, minor,
index, commodity — and EXCLUDES the crypto pilot (btc/eth/xrp/sol), which is demo-only and
net-negative over 3yr (see cam_limit_backtest.py). The committed backtest-summary series can't be
used for this because backtest_history scores cam_rev over every pair in the deep file, so its
monthly series silently includes crypto.

For each eligible pair: detect_cam_rev -> trend-quality gate (passes_gate, the same live filter)
-> score_sess (explicit target bracket, bracket-honest) -> bucket the resolved R by entry month.
Emits a clean JSON block (between BEGIN/END markers) with per-month {n, w, r} plus totals, so the
chart is built from an auditable, scope-explicit series. Commits nothing.

Run in CI after fetching deep m15 (see cam-monthly-investor.yml):
    OANDA_TOKEN=... python cam_monthly_investor.py --hist deep-ohlc.json
"""
import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from trend_regime import build_regime, passes_gate

CLASSES = {'major', 'minor', 'index', 'comm'}     # LIVE cam_rev universe — crypto excluded
BLACKLIST = {'xptusd'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})

    monthly = defaultdict(lambda: {'n': 0, 'w': 0, 'r': 0.0})
    n_sig = n_res = 0
    pairs_used = []
    perpair = defaultdict(list)   # pk -> [(entry_ts, R), ...]
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        if cls not in CLASSES or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or [])
        daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        reg = build_regime(m15) if len(m15) >= 40 else None
        used = False
        for s in H.detect_cam_rev(pk, m15, daily):
            n_sig += 1
            # Same trend-quality gate the live feed applies (reversal strategies fire only
            # outside a strong trend). Fail-open when no regime, exactly like backtest_history.
            if reg is not None and not passes_gate('cam_rev', reg, s['entry_ts']):
                continue
            st, o = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'],
                                 s['dir'], H.CAM_HOLD)
            if st != 'resolved' or o is None:
                continue
            mkey = datetime.fromtimestamp(s['entry_ts'], timezone.utc).strftime('%Y-%m')
            b = monthly[mkey]
            b['n'] += 1
            b['w'] += 1 if o > 0 else 0
            b['r'] += o
            perpair[pk].append((s['entry_ts'], o))
            n_res += 1
            used = True
        if used:
            pairs_used.append(pk)

    # Per-pair 3yr breakdown (n / win% / expectancy / total R + OOS halves by median entry).
    per_pair = {}
    for pk, rows in perpair.items():
        rows.sort()
        rv = [o for _, o in rows]
        n = len(rv); m = n // 2
        h1, h2 = rv[:m], rv[m:]
        per_pair[pk] = {
            'n': n, 'w': sum(1 for o in rv if o > 0),
            'wr': round(100.0 * sum(1 for o in rv if o > 0) / n, 1),
            'exp': round(sum(rv) / n, 4), 'r': round(sum(rv), 1),
            'oos1': round(sum(h1) / len(h1), 4) if h1 else 0.0,
            'oos2': round(sum(h2) / len(h2), 4) if h2 else 0.0,
            'both_halves_pos': (len(h1) > 0 and len(h2) > 0 and sum(h1) > 0 and sum(h2) > 0),
        }
    per_pair = dict(sorted(per_pair.items(), key=lambda kv: -kv[1]['r']))

    series = {k: {'n': v['n'], 'w': v['w'], 'r': round(v['r'], 2)} for k, v in sorted(monthly.items())}
    rs = [v['r'] for v in series.values()]
    tot_n = sum(v['n'] for v in series.values())
    tot_w = sum(v['w'] for v in series.values())
    out = {
        'scope': 'cam_rev LIVE — FX/index/commodity (major,minor,index,comm); crypto excluded',
        'model': 'gated (passes_gate) + score_sess target bracket — same as dashboard headline',
        'months': len(series),
        'months_positive': sum(1 for r in rs if r > 0),
        'months_negative': sum(1 for r in rs if r < 0),
        'total_trades': tot_n,
        'overall_wr_pct': round(100.0 * tot_w / tot_n, 1) if tot_n else 0.0,
        'total_r': round(sum(rs), 1),
        'exp_per_trade_r': round(sum(rs) / tot_n, 4) if tot_n else 0.0,
        'best_month_r': round(max(rs), 1) if rs else 0.0,
        'worst_month_r': round(min(rs), 1) if rs else 0.0,
        'pairs_used': sorted(pairs_used),
        'pairs_net_positive': sum(1 for v in per_pair.values() if v['r'] > 0),
        'pairs_both_halves_pos': sum(1 for v in per_pair.values() if v['both_halves_pos']),
        'series': series,
        'per_pair': per_pair,
    }
    print(f"raw signals={n_sig}  gated+resolved={n_res}  pairs={len(pairs_used)}")
    print("BEGIN_CAM_MONTHLY_JSON")
    print(json.dumps(out, indent=1))
    print("END_CAM_MONTHLY_JSON")


if __name__ == '__main__':
    main()
