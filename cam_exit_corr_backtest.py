"""cam_rev — 24h time-exit + correlation-cap backtest.

Two drawdown-oriented ideas (not expected to lift the +0.52R edge — that already exists — but to
smooth the equity path and align live to the backtest):

  A) 24h TIME-EXIT. The backtest holds 24h and excludes unresolved trades; live holds a fade
     indefinitely until stop/TP, so losers can ride for days (the drawdown positions we saw).
     Compare "hold to resolve (up to 5d)" vs "exit at 24h at market if unresolved".

  B) CORRELATION CAP. Today's losers were ~3 macro bets in 7 pairs (indices together, NZD crosses
     together). Event-driven portfolio sim: cap net same-side exposure per currency / equity / comm
     bloc; skip a fade that would breach it. Report total R AND max drawdown (R) vs no cap.

Faithful entry: production detect_cam_rev + resting-limit fill at ref_entry. Live classes only
(crypto is demo). Commits nothing.

  Fast:  python cam_exit_corr_backtest.py
  3-yr:  python cam_exit_corr_backtest.py --hist deep-ohlc.json
"""
import argparse
import bisect
import json
from collections import defaultdict

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS

CLASSES = {'major', 'minor', 'index', 'comm'}
CRYPTO = getattr(H, 'CAM_CRYPTO_PILOT', set())
BLACKLIST = {'xptusd'}
EXPIRY_BARS = 48        # limit rest window for the fill
HOLD_24 = 96            # 24h at m15
HOLD_LONG = 480         # ~5 trading days ("hold to resolve" proxy)
CAPS = [3, 4, 5, 6, 8]        # correlation-cap sweep (max net same-side exposure per bloc)


def fill_and_scan(bars, ts, sig):
    refpx, stop, target, d = sig['entry'], sig['stop'], sig['target'], sig['dir']
    R = abs(refpx - stop)
    if R <= 0:
        return None
    i0 = bisect.bisect_left(ts, sig['entry_ts'])
    fill = None
    for j in range(i0, min(i0 + EXPIRY_BARS, len(bars))):
        b = bars[j]
        if (d == 'bull' and b['l'] <= refpx) or (d == 'bear' and b['h'] >= refpx):
            fill = j; break
    if fill is None:
        return None

    def scan(horizon):
        end = min(fill + horizon, len(bars))
        for j in range(fill, end):
            b = bars[j]
            if d == 'bull':
                if b['l'] <= stop:   return -1.0, j
                if b['h'] >= target: return (target - refpx) / R, j
            else:
                if b['h'] >= stop:   return -1.0, j
                if b['l'] <= target: return (refpx - target) / R, j
        return None, end - 1

    def mkt(j):
        c = bars[j]['c']
        return (c - refpx) / R if d == 'bull' else (refpx - c) / R

    r24, j24 = scan(HOLD_24)
    lingered = r24 is None
    if lingered:
        j24 = min(fill + HOLD_24, len(bars) - 1); r24 = mkt(j24)
    rL, jL = scan(HOLD_LONG)
    if rL is None:
        jL = min(fill + HOLD_LONG, len(bars) - 1); rL = mkt(jL)
    return {'fill_ts': bars[fill]['_ts'], 'r24': r24, 'exit24_ts': bars[j24]['_ts'],
            'rhold': rL, 'lingered': lingered}


def exposure(pair, d):
    s = 1 if d == 'bull' else -1
    cls = PAIR_CLASS.get(pair)
    if cls == 'index':
        return {'EQ': s}
    if cls == 'comm':
        cm = {'xauusd': 'GOLD', 'xagusd': 'SILVER', 'wtiusd': 'OIL', 'usoil': 'OIL',
              'natgas': 'GAS', 'xptusd': 'PLAT'}
        return {cm.get(pair, pair.upper()): s}
    if len(pair) == 6:
        return {pair[:3].upper(): s, pair[3:].upper(): -s}
    return {pair.upper(): s}


def maxdd(rs):
    """Max peak-to-trough drawdown (in R) of the cumulative curve of a time-ordered R list."""
    peak = cum = dd = 0.0
    for r in rs:
        cum += r; peak = max(peak, cum); dd = min(dd, cum - peak)
    return dd


def agg(rs):
    n = len(rs)
    if not n:
        return None
    w = sum(1 for r in rs if r > 0)
    return {'n': n, 'wr': 100.0 * w / n, 'exp': sum(rs) / n, 'totR': sum(rs)}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='historical-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    recs = []
    for pk, layers in pairs.items():
        cls = PAIR_CLASS.get(pk)
        if cls not in CLASSES or pk in CRYPTO or pk in BLACKLIST or not isinstance(layers, dict):
            continue
        m15 = H._bars_norm(layers.get('m15') or []); daily = H._bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]
        for s in H.detect_cam_rev(pk, m15, daily):
            r = fill_and_scan(m15, ts, s)
            if r:
                r['pair'] = pk; r['exp_keys'] = exposure(pk, s['dir']); recs.append(r)
    recs.sort(key=lambda r: r['fill_ts'])
    print(f"=== cam_rev · 24h-exit + correlation-cap · {len(recs)} filled signals · {args.hist} ===\n")

    # A) 24h time-exit vs hold-to-resolve
    hold = [r['rhold'] for r in recs]; ex24 = [r['r24'] for r in recs]
    ling = sum(1 for r in recs if r['lingered'])
    h, e = agg(hold), agg(ex24)
    print(">> A. 24h TIME-EXIT vs HOLD-TO-RESOLVE (bracket, by fill order):")
    print(f"   lingered past 24h: {ling}/{len(recs)} ({100.0*ling/len(recs):.0f}%)")
    print(f"   HOLD-to-resolve : WR {h['wr']:.0f}%  exp {h['exp']:+.3f}R  totR {h['totR']:+.0f}  maxDD {maxdd(hold):+.0f}R")
    print(f"   24h TIME-EXIT   : WR {e['wr']:.0f}%  exp {e['exp']:+.3f}R  totR {e['totR']:+.0f}  maxDD {maxdd(ex24):+.0f}R")

    # B) correlation cap — event-driven, measuring CONCURRENT exposure (the real target)
    def sim(cap):
        ev = []
        for r in recs:
            ev.append((r['fill_ts'], 1, 'open', r)); ev.append((r['exit24_ts'], 0, 'close', r))
        net = defaultdict(int); openc = 0; max_open = 0; max_bloc = 0; taken = []; skipped = 0
        for _ts, _typ, kind, r in sorted(ev, key=lambda e: (e[0], e[1])):
            if kind == 'close':
                if r.get('_t'):
                    for k, s in r['exp_keys'].items():
                        net[k] -= s
                    openc -= 1
            else:
                ok = all(abs(net[k] + s) <= cap for k, s in r['exp_keys'].items())
                if ok:
                    r['_t'] = True
                    for k, s in r['exp_keys'].items():
                        net[k] += s
                    taken.append(r); openc += 1
                    max_open = max(max_open, openc)
                    max_bloc = max(max_bloc, max(abs(v) for v in net.values()) if net else 0)
                else:
                    skipped += 1
        for r in recs:
            r.pop('_t', None)
        return taken, skipped, max_open, max_bloc

    print("\n>> B. CORRELATION CAP — peak CONCURRENT exposure is the target (uses 24h-exit):")
    tk0, _, mo0, mb0 = sim(10**9)   # no cap
    a0 = agg([r['r24'] for r in tk0])
    print(f"   NO CAP : totR {a0['totR']:+.0f}  peak-concurrent-open {mo0}  peak-net-bloc {mb0}")
    for CAP in CAPS:
        tk, sk, mo, mb = sim(CAP)
        a = agg([r['r24'] for r in tk])
        print(f"   cap={CAP}  : totR {a['totR']:+.0f} (skip {sk:<4})  peak-open {mo:<3} peak-net-bloc {mb}"
              f"  exp {a['exp']:+.3f}R")
    print("\nReading it: A — 24h-exit ~= hold => adopt for hygiene. B — the cap trades some total R for a")
    print("lower PEAK CONCURRENT correlated exposure (fewer positions red together / less margin+variance).")


if __name__ == '__main__':
    main()
