"""cam_rev per-trade + per-pair export (gross AND net-of-cost) over the deep 3-yr cache.

Faithful to the canonical backtest: production detect_cam_rev + score_sess (fixed 1:1 bracket,
CAM_HOLD), the same universe gate the harness uses, and the harness cost model
(cost_r = (0.0045% win / 0.0105% loss) x |entry| / R). Emits every RESOLVED fill with gross r,
cost_r and net r, plus per-pair aggregates. Self-validates the per-pair GROSS against
backtest-summary.json's live_by_pair.cam_rev so we know it reproduces the chart data. Commits nothing.

  Fast:  python cam_rev_pair_export.py                          # ~90d local
  3-yr:  python cam_rev_pair_export.py --hist deep-ohlc.json    # in CI (restores deep cache)
"""
import argparse
import json
from collections import defaultdict

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
try:
    from trend_regime import build_regime, _at, CHOP_LO
    HAVE_REGIME = True
except Exception:
    HAVE_REGIME = False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='historical-ohlc.json')
    ap.add_argument('--out', default='cam-rev-pair-export.json')
    ap.add_argument('--summary', default='backtest-summary.json')   # for reconciliation
    args = ap.parse_args()

    doc = json.load(open(args.hist))
    pairs = doc.get('pairs', {})
    trades = []
    for pk, layers in pairs.items():
        if not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or [])
        daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        reg = build_regime(H._bars_norm(layers.get('h1') or [])) if HAVE_REGIME and len(layers.get('h1') or []) >= 40 else None
        for s in H.detect_cam_rev(pk, m15, daily):          # detector self-gates the universe
            st, o = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], H.CAM_HOLD)
            if st != 'resolved':
                continue                                     # bracket-honest: unresolved excluded
            R_px = abs(s['entry'] - s['stop'])
            cost_r = H.cost(o, s['entry'], R_px)             # harness cost model (R units, >=0)
            regime = ''
            if reg is not None:
                try:
                    chop = _at(reg, s['entry_ts'])[1]
                    regime = 'strong' if (chop is not None and chop < CHOP_LO) else 'range'
                except Exception:
                    regime = ''
            trades.append({'pair': pk, 'cls': PAIR_CLASS.get(pk, '?'), 'entry_ts': int(s['entry_ts']),
                           'dir': s['dir'], 'entry': round(s['entry'], 6), 'stop': round(s['stop'], 6),
                           'target': round(s['target'], 6), 'R_px': R_px, 'r': o,
                           'cost_r': cost_r, 'r_net': o - cost_r, 'regime': regime})

    # per-pair aggregates (n>=15, matching backtest_history's thin-pair filter)
    by = defaultdict(list)
    for t in trades:
        by[t['pair']].append(t)
    by_pair = {}
    for pk, ts in by.items():
        if len(ts) < 15:
            continue
        n = len(ts); wins = sum(1 for t in ts if t['r'] > 0)
        by_pair[pk] = {
            'cls': PAIR_CLASS.get(pk, '?'), 'n': n, 'wins': wins,
            'total_r_gross': sum(t['r'] for t in ts),
            'total_r_net': sum(t['r_net'] for t in ts),
            'cost_r_avg': sum(t['cost_r'] for t in ts) / n,
            'wr': round(100.0 * wins / n, 1),
        }

    out = {'n_trades': len(trades), 'by_pair': by_pair, 'trades': trades,
           'cost_model': {'win_pct': H.WIN_COST_PCT, 'loss_pct': H.LOSS_COST_PCT}}
    json.dump(out, open(args.out, 'w'))
    print(f"[export] resolved cam_rev fills: {len(trades)}  pairs(>=15): {len(by_pair)} -> {args.out}", flush=True)

    # ---- reconciliation vs the committed canonical summary (per-pair GROSS) ----
    try:
        cr = json.load(open(args.summary))['live_by_pair']['cam_rev']
        dn = dr = dwr = 0; worst = []
        for pk, ref in cr.items():
            got = by_pair.get(pk)
            if not got:
                worst.append(f"{pk}: MISSING"); continue
            dn = max(dn, abs(got['n'] - ref['n']))
            dr = max(dr, abs(got['total_r_gross'] - ref['total_r']))
            dwr = max(dwr, abs(got['wr'] - ref['wr']))
            if abs(got['n'] - ref['n']) > 3 or abs(got['total_r_gross'] - ref['total_r']) > 5:
                worst.append(f"{pk}: n {got['n']} vs {ref['n']}, R {got['total_r_gross']:.1f} vs {ref['total_r']}")
        tot_g = sum(v['total_r_gross'] for v in by_pair.values())
        tot_n = sum(v['total_r_net'] for v in by_pair.values())
        N = sum(v['n'] for v in by_pair.values())
        print(f"[reconcile] max|dn|={dn} max|dR|={dr:.1f} max|dWR|={dwr:.1f}pp  (small = faithful)", flush=True)
        print(f"[totals] n={N}  gross_totR={tot_g:.1f} (exp {tot_g/N:+.4f})  net_totR={tot_n:.1f} (exp {tot_n/N:+.4f})", flush=True)
        if worst:
            print("[reconcile] notable diffs:\n  " + "\n  ".join(worst[:20]), flush=True)
    except Exception as e:
        print(f"[reconcile] skipped: {e}", flush=True)


if __name__ == '__main__':
    main()
