"""cam_rev — stop-width variants backtest (is the fade stop too tight?).

The cam_rev stop is pinned to the Camarilla R4/S4 level + CAM_BUF*(level-spacing). On low-vol
pairs that can be only a few pips — tighter than intraday noise. This tests whether WIDENING the
stop improves risk-adjusted results, WITHOUT eyeballing it. Two mechanisms, each keeping the RR at
1:1 (target moves with the stop) so it stays a like-for-like cam_rev:

  • BUFFER widen  — stop = level ± buf*(spacing), buf swept 0.10 (current) .. 1.0.
  • ATR floor     — base stop (buf 0.10), then widen so R >= k*ATR(14) on m15, k swept 0.5 .. 1.5.

Faithful to live: production cam_rev trigger logic, resting-limit fill at ref_entry (reuses
sim_limit), bracket-honest WR on RESOLVED fills, OOS halves. R-expectancy is size-independent, so a
wider stop (smaller position) is compared fairly. Commits nothing.

  Fast:  python cam_stop_backtest.py                        # ~90d (historical-ohlc.json)
  3-yr:  python cam_stop_backtest.py --hist deep-ohlc.json  # CI restores the deep cache
"""
import argparse
import bisect
import json
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from cam_gap_session_backtest import sim_limit, agg   # reuse the faithful limit-fill + aggregator

CLASSES = {'major', 'minor', 'index', 'comm'}
CRYPTO = getattr(H, 'CAM_CRYPTO_PILOT', {'btcusd', 'ethusd', 'xrpusd', 'solusd'})
BLACKLIST = {'xptusd'}
BUFFERS = [0.10, 0.25, 0.50, 1.00]     # 0.10 == current live
ATR_FLOORS = [0.5, 1.0, 1.5]           # widen base(0.10) stop so R >= k*ATR


def cam_signals(pk, m15, daily):
    """Production cam_rev trigger, but also emit the Camarilla level/spacing + m15 ATR at the
    trigger so stop variants can be recomputed. Mirrors detect_cam_rev exactly otherwise."""
    if (PAIR_CLASS.get(pk) not in CLASSES and pk not in CRYPTO) or len(m15) < 500 or len(daily) < 30:
        return []
    lv = H._cam_levels(daily); out = []; done = set()
    for i in range(len(m15) - 1):
        b = m15[i]; bt = datetime.fromtimestamp(b['_ts'], timezone.utc)
        day = bt.strftime('%Y-%m-%d'); L = lv.get(day)
        if not L or not (H.CAM_SESS_OPEN <= bt.hour < H.CAM_SESS_CLOSE):
            continue
        a = H.atr(m15, 14, i) or 0.0
        if (day, 'S') not in done and b['h'] >= L['R3'] and b['c'] < L['R3'] and b['c'] < b['o']:
            out.append({'dir': 'bear', 'entry': b['c'], 'level': L['R4'], 'spacing': L['R4'] - L['R3'],
                        'atr': a, 'entry_ts': m15[i + 1]['_ts']})
            done.add((day, 'S'))
        if (day, 'L') not in done and b['l'] <= L['S3'] and b['c'] > L['S3'] and b['c'] > b['o']:
            out.append({'dir': 'bull', 'entry': b['c'], 'level': L['S4'], 'spacing': L['S3'] - L['S4'],
                        'atr': a, 'entry_ts': m15[i + 1]['_ts']})
            done.add((day, 'L'))
    return out


def make_bracket(sig, buf, atr_k):
    """Return (stop, target) for a variant. Sell stop is above entry, buy below; RR always 1:1."""
    e, d = sig['entry'], sig['dir']
    stop = (sig['level'] + buf * sig['spacing']) if d == 'bear' else (sig['level'] - buf * sig['spacing'])
    R = abs(e - stop)
    if atr_k and sig['atr'] > 0:
        R = max(R, atr_k * sig['atr'])
    if R <= 0:
        return None, None, 0.0
    stop = e + R if d == 'bear' else e - R
    target = e - R if d == 'bear' else e + R
    return stop, target, R


def score_variant(sigs, buf, atr_k, hold):
    """Score every signal under a stop variant; returns per-class agg + median stop-in-ATR."""
    by = defaultdict(list); atr_mult = []
    for cls, m15, ts, s in sigs:
        stop, target, R = make_bracket(s, buf, atr_k)
        if R <= 0:
            continue
        if s['atr'] > 0:
            atr_mult.append(R / s['atr'])
        sig = {'entry': s['entry'], 'stop': stop, 'target': target, 'dir': s['dir'], 'entry_ts': s['entry_ts']}
        st, r, _ = sim_limit(m15, ts, sig, hold)
        row = (s['entry_ts'], r if st == 'resolved' else None, st != 'nofill')
        by[cls].append(row); by['ALL'].append(row)
    atr_mult.sort()
    med = atr_mult[len(atr_mult) // 2] if atr_mult else 0.0
    return {c: agg(v) for c, v in by.items()}, med


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='historical-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    hold = H.CAM_HOLD
    sigs = []
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk); crypto = pk in CRYPTO
        if (cls not in CLASSES and not crypto) or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or []); daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]
        for s in cam_signals(pk, m15, daily):
            sigs.append(('crypto' if crypto else cls, m15, ts, s))
    live = [x for x in sigs if x[0] in CLASSES]

    order = ['ALL', 'major', 'minor', 'index', 'comm']
    print(f"=== cam_rev · STOP-WIDTH variants · {len(live)} live signals · {args.hist} ===")
    print("(RR held at 1:1; bracket-honest WR on resolved limit fills; R-expectancy is size-independent)\n")

    def show(tag, res, med):
        v = res.get('ALL')
        if v:
            print(f"  {tag:22} med-stop={med:.2f}xATR  n={v['n']:<5} res={v['nres']:<5} "
                  f"WR={v['wr']:>3.0f}%  exp={v['exp']:+.3f}R  totR={v['totR']:+.0f}  "
                  f"OOS1={v['oos1']:+.3f} OOS2={v['oos2']:+.3f}")

    print(">> BUFFER widen (0.10 = current live):")
    base_res, base_med = score_variant(live, 0.10, None, hold)
    show('buf 0.10 (BASELINE)', base_res, base_med)
    for buf in BUFFERS[1:]:
        r, m = score_variant(live, buf, None, hold); show(f'buf {buf:.2f}', r, m)

    print("\n>> ATR floor on the base(0.10) stop (R >= k*ATR):")
    for k in ATR_FLOORS:
        r, m = score_variant(live, 0.10, k, hold); show(f'floor {k:.1f}xATR', r, m)

    print("\n>> per-class — BASELINE vs the widest useful floor (1.0xATR):")
    fr, _ = score_variant(live, 0.10, 1.0, hold)
    for c in order[1:]:
        b = base_res.get(c); f = fr.get(c)
        if b and f:
            print(f"  {c:7} base exp={b['exp']:+.3f}R (WR {b['wr']:.0f}%, n{b['nres']})  ->  "
                  f"floor1.0 exp={f['exp']:+.3f}R (WR {f['wr']:.0f}%, n{f['nres']})")
    print("\nReading it: a wider stop that LIFTS exp with stable OOS and keeps trade count is a real")
    print("improvement; if exp is flat/down it just trades fewer-but-bigger for no gain — keep current.")


if __name__ == '__main__':
    main()
