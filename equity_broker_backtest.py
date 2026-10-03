"""Backtest the .EQ strategies on BROKER-exported OHLC (MQL5 VikingHistoryExport.mq5 output) —
the DE/UK/FR (and any) instruments TwelveData won't serve. Reads the exported JSON locally (no
network), replays the same causal .EQ detectors (holygrail_eq / twob_eq / volbreak_eq on h1,
holygrail_eq_m15 on m15) on the exact instruments you'd trade, scored on the shared trailing-runner
exit net of costs. Causal by construction (single-TF detectors). Resolved-only, both-OOS.

  python equity_broker_backtest.py --hist viking_intl_ohlc.json
"""
import argparse, json
from collections import defaultdict

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

STRATS = [('holygrail_eq','h1',H._holygrail_sig), ('twob_eq','h1',H._twob_sig),
          ('volbreak_eq','h1',H._volbreak_sig), ('holygrail_eq_m15','m15',H._holygrail_sig)]
MIN_BARS = 400


def market(sym):
    s = (sym or '').upper()
    if '.ETR' in s: return 'DE'
    if '.LSE' in s: return 'UK'
    if '.PAR' in s or '.XPAR' in s: return 'FR'
    if '.NYSE' in s or '.NAS' in s or '.US' in s: return 'US'
    return '?'


def _score(bars, s):
    st, o = H.score_trail_open(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                               H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
    if st != 'resolved':
        return None
    try:
        return o - H.cost(o, s['entry'], abs(s['entry'] - s['stop']))
    except Exception:
        return o


def _agg(d):
    d = sorted(d); rs = [r for _, r in d]; n = len(rs)
    if not n:
        return None
    m = n // 2
    return dict(n=n, wr=100.0*sum(1 for r in rs if r > 0)/n, exp=sum(rs)/n, tot=sum(rs),
                oos1=(sum(rs[:m])/m if m else 0.0), oos2=(sum(rs[m:])/(n-m) if n-m else 0.0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    if not pairs:
        print('no pairs in file'); return 1

    by_sym = defaultdict(lambda: defaultdict(list))  # sym -> strat -> [(ts,r)]
    by_mkt = defaultdict(list); by_ms = defaultdict(list); by_strat = defaultdict(list)
    meta = {}
    for key, layers in pairs.items():
        sym = layers.get('sym', key); mkt = market(sym)
        got = False
        for tag, tf, gen in STRATS:
            bars = _bars_norm(layers.get(tf) or [])
            if len(bars) < MIN_BARS:
                continue
            got = True
            for s in H._mw_signals(bars, key, tag, tf, gen):
                r = _score(bars, s)
                if r is None:
                    continue
                by_sym[sym][tag].append((s['entry_ts'], r))
                by_mkt[mkt].append((s['entry_ts'], r))
                by_ms[(mkt, tag)].append((s['entry_ts'], r))
                by_strat[tag].append((s['entry_ts'], r))
        meta[sym] = (mkt, got, {tf: len(_bars_norm(layers.get(tf) or [])) for tf in ('h1', 'm15')})

    print('=== .EQ on broker-exported data ·', args.hist, '===\n')
    print('bars per symbol:')
    for sym in sorted(meta):
        mkt, got, bc = meta[sym]
        print(f"  {mkt:>3} {sym:<14} h1={bc.get('h1',0):<5} m15={bc.get('m15',0):<5}" + ("" if got else "  (too few bars)"))

    print('\nBY MARKET (all .EQ pooled, net of costs):')
    print(f"  {'mkt':<5}{'n':>7}{'WR':>6}{'exp':>9}{'OOS1':>9}{'OOS2':>9}{'totR':>8}  edge?")
    for mkt in sorted(by_mkt):
        a = _agg(by_mkt[mkt])
        if not a:
            continue
        edge = 'REAL' if (a['oos1'] > 0 and a['oos2'] > 0 and a['exp'] > 0.02) else ('thin' if a['exp'] > 0 else 'none')
        print(f"  {mkt:<5}{a['n']:>7}{a['wr']:>5.0f}%{a['exp']:>+8.3f}R{a['oos1']:>+8.3f}{a['oos2']:>+8.3f}{a['tot']:>+7.0f}  {edge}")

    print('\nBY MARKET x STRATEGY (exp R / n):')
    mkts = sorted({m for m, _ in by_ms})
    print(f"  {'strategy':<18}" + ''.join(f"{m:>13}" for m in mkts))
    for tag, _, _ in STRATS:
        cells = []
        for mkt in mkts:
            a = _agg(by_ms[(mkt, tag)])
            cells.append(f"{a['exp']:+.3f}/{a['n']}" if a else '—')
        print(f"  {tag:<18}" + ''.join(f"{c:>13}" for c in cells))

    print('\nPER SYMBOL (all .EQ pooled):')
    for sym in sorted(by_sym):
        allr = []
        for tag in by_sym[sym]:
            allr += by_sym[sym][tag]
        a = _agg(allr)
        if a:
            print(f"  {market(sym):>3} {sym:<14} n={a['n']:<5} WR={a['wr']:>3.0f}%  exp={a['exp']:+.3f}R  totR={a['tot']:+.0f}")
    print('\nedge? REAL = exp>+0.02R AND both OOS halves + (net of costs). Compare DE/UK/FR to the US pilot.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
