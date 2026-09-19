"""Research-only: compare cam_rev's London-only (07-16 UTC) vs London+US (07-22 UTC) session
window over the full deep-m15 history, for FX / index / commodity classes.

Prints per-class n / win-rate / expectancy / total-R and both OOS halves (count-split, the same
convention as backtest_history) for each window, side by side. It PATCHES the session window
in-process only for the comparison — it does NOT change production behaviour and commits nothing.
Run in CI after fetching a deep m15 file (see cam-session-research.yml).
"""
import argparse
import json
from collections import defaultdict

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS

CLASSES = {'major', 'minor', 'index', 'comm'}
BLACKLIST = {'xptusd'}


def _score(pairs, oh, ch):
    H.CAM_SESS_OPEN, H.CAM_SESS_CLOSE = oh, ch
    trades = []  # (entry_ts, cls, r)
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        if cls not in CLASSES or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or [])
        daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        for s in H.detect_cam_rev(pk, m15, daily):
            st, o = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], H.CAM_HOLD)
            if st == 'resolved' and o is not None:
                trades.append((s['entry_ts'], cls, o))
    return trades


def _agg(trades):
    byc = defaultdict(list)
    for et, cls, r in trades:
        byc[cls].append((et, r))
        byc['ALL'].append((et, r))
    out = {}
    for cls, rows in byc.items():
        rows.sort()
        rs = [r for _, r in rows]
        n = len(rs)
        if not n:
            continue
        m = n // 2
        h1, h2 = rs[:m], rs[m:]
        out[cls] = {'n': n, 'wr': 100 * sum(1 for r in rs if r > 0) / n, 'exp': sum(rs) / n,
                    'totR': sum(rs),
                    'oos1': (sum(h1) / len(h1) if h1 else 0.0),
                    'oos2': (sum(h2) / len(h2) if h2 else 0.0)}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    windows = [("London 07-16", 7, 16), ("London+US 07-22", 7, 22)]
    res = {label: _agg(_score(pairs, oh, ch)) for label, oh, ch in windows}
    print("=== cam_rev session-window research · FX/index/comm · full deep-m15 history ===")
    print("(count-split OOS halves; break-even at RR 1:1 is 50% WR)\n")
    for cls in ['ALL', 'major', 'minor', 'index', 'comm']:
        print(f"-- {cls} --")
        for label, _, _ in windows:
            v = res[label].get(cls)
            if v:
                print(f"  {label:16} n={v['n']:<6} WR={v['wr']:>3.0f}%  exp=+{v['exp']:.3f}R  "
                      f"totR=+{v['totR']:.0f}  OOS1=+{v['oos1']:.3f}  OOS2=+{v['oos2']:.3f}")
        # delta line
        a = res["London 07-16"].get(cls); b = res["London+US 07-22"].get(cls)
        if a and b:
            print(f"  {'Δ (US ext.)':16} n={b['n']-a['n']:+<6} WR={b['wr']-a['wr']:+.0f}pp  "
                  f"exp={b['exp']-a['exp']:+.3f}R  totR={b['totR']-a['totR']:+.0f}")
        print()


if __name__ == '__main__':
    main()
