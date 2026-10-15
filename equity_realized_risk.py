"""Realized risk % per trade on the live equity book — is the notional cap
starving the tight-stop strategies?

For each closed fill: realized_risk$ = volume x |entry - init_sl|  (the ACTUAL
dollars at risk to the original stop, after any notional-cap throttle), expressed
as a % of account equity. The 0.5% target is only hit when the notional cap did
NOT bind; a tight-stop strategy capped at 20% notional shows up here at a tiny
fraction (e.g. ~0.03%). Run before/after raising InpMaxNotionalPctUS to confirm
US trades now size to ~0.5% instead of being capped.

    python equity_realized_risk.py                 # uses $1,000,000 equity base
    python equity_realized_risk.py --equity 990000
"""
import json, os, argparse
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))


def mkt(s):
    s = (s or '').upper()
    for suf, m in (('.NAS', 'US'), ('.NYSE', 'US'), ('.ETR', 'DE'), ('.LSE', 'UK'),
                   ('.PAR', 'FR'), ('.TSE', 'JP'), ('.HK', 'HK'), ('.MAD', 'ES')):
        if suf in s:
            return m
    return '?'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--equity', type=float, default=1_000_000.0, help='account equity base for the %% (default 1,000,000)')
    ap.add_argument('--file', default=os.path.join(HERE, 'equity-executions.json'))
    args = ap.parse_args()
    ex = json.load(open(args.file)).get('executions', [])

    rows = []
    for e in ex:
        v = e.get('volume'); en = e.get('entry'); sl = e.get('init_sl')
        if not v or not en or sl is None:
            continue
        risk = v * abs(en - sl)
        rows.append({'strat': e.get('strategy', '?'), 'mkt': mkt(e.get('sym')),
                     'risk_pct': 100 * risk / args.equity, 'tf': 'm15' if 'm15' in (e.get('strategy') or '') else 'h1/4h'})
    if not rows:
        print('no fills with init_sl — nothing to measure'); return

    def line(label, rs):
        if not rs:
            return
        rp = sorted(r['risk_pct'] for r in rs)
        med = rp[len(rp)//2]; mean = sum(rp)/len(rp)
        hit = 100 * sum(1 for x in rp if x >= 0.45) / len(rp)   # within ~10% of the 0.5% target
        print(f"  {label:<26} n={len(rs):>4}  median={med:>5.3f}%  mean={mean:>5.3f}%  >=0.45%: {hit:>3.0f}%")

    print(f"Realized risk % per trade (target 0.5%); equity base ${args.equity:,.0f}\n")
    print("BY STRATEGY:")
    bs = defaultdict(list)
    for r in rows:
        bs[r['strat']].append(r)
    for s in sorted(bs):
        line(s, bs[s])
    print("\nBY MARKET:")
    bm = defaultdict(list)
    for r in rows:
        bm[r['mkt']].append(r)
    for m in sorted(bm):
        line(m, bm[m])
    print("\nBY MARKET x TF (m15 = tight stops, most cap-throttled):")
    bmt = defaultdict(list)
    for r in rows:
        bmt[(r['mkt'], r['tf'])].append(r)
    for k in sorted(bmt):
        line(f"{k[0]} {k[1]}", bmt[k])
    print("\n>=0.45% = fraction of trades sized to ~full 0.5% risk (cap did NOT bind).")
    print("A low median on US m15 means the notional cap is still throttling — raise InpMaxNotionalPctUS.")


if __name__ == '__main__':
    main()
