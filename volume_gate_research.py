"""Does a relative-volume gate rescue the price strategies that just missed?

The industry-15m study left three strategies below breakeven on price alone —
ORB (closest, -0.026R universe / -0.010R crypto), EMA9/20 pullback, Bollinger
mean-reversion. Now that we carry volume, re-run each with a RELATIVE-VOLUME
gate at the signal bar: only take the trade if that bar's volume >= REL x its
own 20-bar average. The idea (classic): breakouts/reclaims on real participation
hold; low-volume ones are noise.

Same detectors (imported from intraday_industry_research, unchanged), same
market fills + cost + OOS. REL swept; baseline REL=0 (no gate) shown for the
delta. Crypto (real volume) reported separately — the gate is only as honest as
the volume behind it.

Run: python volume_gate_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost
from intraday_industry_research import sig_orb, sig_ema920, sig_bbmr, walk_to, HOLD

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
TF = 'm15'
RELS = [0.0, 1.2, 1.5, 2.0]        # 0 = no gate (baseline)
VOL_LB = 20
CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd', 'suiusd', 'taousd', 'nearusd'}

STRATS = {
    'ORB': (sig_orb, 'range'),
    'EMA9/20': (sig_ema920, 1.5),
    'Bollinger': (sig_bbmr, 'mid'),
}


def relvol(bars, i):
    """Signal-bar volume / its trailing VOL_LB average."""
    if i < VOL_LB:
        return None
    seg = bars[i - VOL_LB:i]
    avg = sum(b.get('v', 0) or 0 for b in seg) / VOL_LB
    if avg <= 0:
        return None
    return (bars[i].get('v', 0) or 0) / avg


def run(d, fn, mode, rel):
    allrows, crypto = [], []
    for pk in d:
        bars = _bars_norm(d.get(pk, {}).get(TF, []))
        if len(bars) < 400:
            continue
        rows = []
        for sg in fn(bars):
            ei, entry, stop, dr, extra = sg
            if ei >= len(bars):
                continue
            if rel > 0:                        # gate on the SIGNAL bar (ei-1)
                rv = relvol(bars, ei - 1)
                if rv is None or rv < rel:
                    continue
            R = abs(entry - stop)
            if R <= 0:
                continue
            if mode == 'range':
                target = entry + extra if dr == 'bull' else entry - extra
            elif mode == 'mid':
                target = extra
            else:
                target = entry + mode * R if dr == 'bull' else entry - mode * R
            o = walk_to(bars, ei, entry, stop, dr, target, HOLD)
            if o is not None:
                rows.append((bars[ei]['_ts'], o - cost(o, entry, R)))
        allrows.extend(rows)
        if pk in CRYPTO:
            crypto.extend(rows)
    return allrows, crypto


def stat(rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    return n, wr, e, eh, es, v


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 100)
    print('Relative-volume gate rescue — REL=0 is the no-gate baseline; higher = only high-participation bars')
    print('=' * 100)
    for name, (fn, mode) in STRATS.items():
        print(f"\n===== {name} =====")
        for scope in ('ALL', 'crypto'):
            print(f"  -- {scope} --")
            for rel in RELS:
                allrows, crypto = run(d, fn, mode, rel)
                rows = crypto if scope == 'crypto' else allrows
                n, wr, e, eh, es, v = stat(rows)
                gate = 'no gate' if rel == 0 else f'relvol>={rel}'
                print(f"      {gate:<12} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


if __name__ == '__main__':
    main()
