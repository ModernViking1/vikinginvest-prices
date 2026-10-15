"""Fib-0.5 entry as a POSITION-SIZING tweak, not an edge tweak.

In R-normalised fixed-fractional sizing, cash P&L = risk$ x realized_R, so the
entry PRICE cancels out — the equity curve is fully determined by the R-multiple
sequence (already measured in hs_fib_rsi_research.py, and the fib R-sequence was
worse). The tighter stop is NOT free leverage there.

The ONE place the tighter stop does something real: a FIXED PRICE target
(the measured move is a fixed price distance = pattern height). Entering closer
to the stop converts that same price move into a larger R-multiple. This sim
holds the TARGET PRICE and STOP PRICE identical for both entries and only moves
the entry (break vs 0.5 retrace), sizing each to the same cash risk. It then
measures what actually matters for capital efficiency:

  - total return & max drawdown of a compounding 0.5%-risk book
  - win rate (closer entry => more adverse-excursion stop-outs before target)
  - average notional % and how often the 20% notional cap throttles the size
    (the tighter stop demands MORE units => hits the cap more => forced smaller
    real risk, which claws back the supposed benefit)

    python hs_fib_sizing_sim.py
"""
import json
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
import hs_swing_research as HS
from hs_swing_research import scan, neck_val, WIN_COST_PCT, LOSS_COST_PCT, MAX_HOLD
from hs_fib_rsi_research import fib_retrace_entry

RISK_PCT = 0.005          # 0.5% of equity per trade (the live equity book's risk)
NOTIONAL_CAP = 0.20       # 20% of equity max notional (live cap)
START_EQ = 100_000.0


def resolve_abs(h1, kind, entry_idx, stop, target):
    """First-touch to an ABSOLUTE target price or stop. Returns 'win'/'loss'/None."""
    for j in range(entry_idx, min(entry_idx + MAX_HOLD, len(h1))):
        b = h1[j]
        if kind == 'bear':
            if b['h'] >= stop: return 'loss'
            if b['l'] <= target: return 'win'
        else:
            if b['l'] <= stop: return 'loss'
            if b['h'] >= target: return 'win'
    return None


def collect():
    d = json.load(open(HS.HIST)); pairs = d.get('pairs', {})
    out = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 300:
            continue
        for kind in ('bear', 'bull'):
            for tr in scan(h1, kind):
                target = tr['mm_target']               # FIXED price target (measured move)
                stop = tr['stop']                       # FIXED stop (right shoulder)
                be = tr['entry']                        # baseline neckline-break entry
                fib = fib_retrace_entry(h1, tr, kind)   # (entry, R, idx) or None
                rec = {'pair': pk, 'cls': PAIR_CLASS.get(pk), 'ts': tr['ts'],
                       'kind': kind, 'target': target, 'stop': stop,
                       'be': be, 'be_R': tr['R'], 'be_idx': tr['entry_idx']}
                if fib:
                    fe, fR, fidx = fib
                    rec.update(fe=fe, fe_R=fR, fe_idx=fidx, filled=True)
                else:
                    rec['filled'] = False
                out.append(rec)
    out.sort(key=lambda r: r['ts'])
    return out


def run_book(rows, use_fib):
    """Compounding book. use_fib=False -> break entry; True -> 0.5 retrace entry.
    Only trades that FILLED under the chosen entry are taken; the target & stop
    PRICE are identical either way so the comparison is pure entry/size."""
    eq = START_EQ; peak = eq; maxdd = 0.0
    wins = n = capped = 0; notional_sum = 0.0; h1cache = {}
    d = json.load(open(HS.HIST)); pairs = d.get('pairs', {})
    for r in rows:
        if use_fib and not r['filled']:
            continue
        pk = r['pair']
        if pk not in h1cache:
            h1cache[pk] = _bars_norm(pairs[pk].get('h1', []))
        h1 = h1cache[pk]
        if use_fib:
            entry, Rp, eidx = r['fe'], r['fe_R'], r['fe_idx']
        else:
            entry, Rp, eidx = r['be'], r['be_R'], r['be_idx']
        if Rp <= 0:
            continue
        res = resolve_abs(h1, r['kind'], eidx, r['stop'], r['target'])
        if res is None:
            continue
        n += 1
        risk_cash = RISK_PCT * eq
        units = risk_cash / Rp
        notional = units * entry
        cap_notional = NOTIONAL_CAP * eq
        if notional > cap_notional:                    # broker/account notional throttle
            units = cap_notional / entry
            capped += 1
        notional_sum += (units * entry) / eq
        real_risk = units * Rp                         # cash at risk after any cap
        if res == 'win':
            wins += 1
            rr = abs(r['target'] - entry) / Rp
            gross = real_risk * rr
            pnl = gross - real_risk * (WIN_COST_PCT / (Rp / abs(entry)) if entry else 0)
        else:
            pnl = -real_risk - real_risk * (LOSS_COST_PCT / (Rp / abs(entry)) if entry else 0)
        eq += pnl
        peak = max(peak, eq)
        maxdd = max(maxdd, (peak - eq) / peak)
    ret = (eq / START_EQ - 1) * 100
    return {'n': n, 'wr': 100*wins/max(1, n), 'ret': ret, 'maxdd': 100*maxdd,
            'end_eq': eq, 'avg_notional': 100*notional_sum/max(1, n),
            'capped_pct': 100*capped/max(1, n)}


def main():
    rows = collect()
    filled = [r for r in rows if r['filled']]
    print(f"Detected H&S patterns: {len(rows)}   fib-0.5 filled: {len(filled)} "
          f"({100*len(filled)/max(1,len(rows)):.0f}%)\n")
    print("Same measured-move TARGET price and same STOP for both — only the entry")
    print("and the resulting size differ. Compounding 0.5%-risk book, 20% notional cap.\n")

    # apples-to-apples: restrict BOTH books to the fib-fillable set
    def show(tag, rows_, use_fib):
        s = run_book(rows_, use_fib)
        print(f"  {tag:<40} n={s['n']:>4}  WR={s['wr']:>5.1f}%  "
              f"ret={s['ret']:>+8.1f}%  maxDD={s['maxdd']:>5.1f}%  "
              f"avgNot={s['avg_notional']:>5.1f}%  capped={s['capped_pct']:>4.1f}%")

    print("ALL detected patterns:")
    show("break entry (baseline, all)", rows, False)
    print("\nFILLABLE subset (patterns that DID retrace to 0.5) — apples-to-apples:")
    show("break entry, same subset", filled, False)
    show("fib-0.5 entry (tighter stop, bigger size)", filled, True)

    print("\nPer-class (fillable subset): break vs fib — ret% / maxDD% / capped%")
    from collections import defaultdict
    byc = defaultdict(list)
    for r in filled:
        byc[r['cls']].append(r)
    print(f"  {'class':<8} {'break ret':>9} {'break DD':>9} | {'fib ret':>9} {'fib DD':>8} {'fib cap%':>8}")
    for c in sorted(byc):
        b = run_book(byc[c], False); f = run_book(byc[c], True)
        print(f"  {c:<8} {b['ret']:>+8.1f}% {b['maxdd']:>8.1f}% | "
              f"{f['ret']:>+8.1f}% {f['maxdd']:>7.1f}% {f['capped_pct']:>7.1f}%")


if __name__ == '__main__':
    main()
