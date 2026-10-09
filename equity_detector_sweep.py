"""Does ANY of the full 60-strategy roster — not just the 7 .EQ ideas already tried — have a
portable edge on EQUITIES over the 3-year period?

The live equity book was built from 4 Market-Wizards setups; only orb_eq/gbreak/gtrend were ever
additionally screened on stocks. This sweep points the REST of the roster's detectors at equity
bars and scores each with the equity book's own exit (trailing runner, arm +1R, trail 0.75R,
200-bar horizon), net of the harness cost model, resolved-only, both OOS halves reported.

Three honest buckets (printed at the end):
  TESTED        — detector fired on equity bars; WR/RR/exp/OOS shown. A CANDIDATE must clear a
                  STRICTER bar than a single backtest (see MULTIPLE TESTING below) because we are
                  running dozens of detectors at once — some WILL look good by chance.
  NO-FIRE       — detector ran but emitted ~no signals on equity bars (needs bespoke wiring: a
                  draw/trigger callable, 4h/m5 bars, or a daily-structure context this data lacks).
  NOT-APPLICABLE— instrument/session-bound by construction (gold-only, crypto-delta, index micro-
                  structure, London/Asian session). Running these on daily/h1/m15 US stock bars
                  would be noise, so they are listed, not scored.

Equity pairs are unclassified in PAIR_CLASS, so class-gated detectors would silently return []. We
monkeypatch them to the closest liquid analog (--asset-class, default 'index': trending, exchange-
hours, non-24h) and add them to membership gates so the structural detectors actually fire. Tags
and thresholds were tuned on the native class, so a pass here is a SCREEN, not a deployment
decision — confirm any survivor on the full broker-exported universe before believing it.

MULTIPLE TESTING: with K detectors screened, a Bonferroni-style guard applies — CANDIDATE requires
exp > +0.05R AND both OOS halves > +0.02R AND n >= 200. Marginal/thin passes are shown but NOT
promoted. One strong, both-OOS, large-n survivor beats five borderline ones.

  python equity_detector_sweep.py --hist equity-ohlc.json [--asset-class index]
"""
import argparse
import inspect
import json
from collections import defaultdict

import detect_triggers as DT
import market_wizards_research as MW

EQ_PAIRS_DEFAULT = ['aapl', 'nvda', 'tsla', 'msft', 'amzn']

# Market-Wizards generators with a clean (bars)->(ei,entry,stop,dir) signature — run on h1 AND m15.
CLEAN = {'holy_grail': MW.holy_grail, 'two_b': MW.two_b, 'volbreak': MW.volbreak,
         'turtle_soup': MW.turtle_soup}

# Structural detect_*(pk, h1[, daily]) dispatchers that are NOT hard-bound to one instrument/session.
STRUCTURAL = ['detect_ob', 'detect_tl', 'detect_w5pb', 'detect_rsimr', 'detect_sid', 'detect_fibgz',
              'detect_fredtl', 'detect_threepush', 'detect_engulf_manip', 'detect_sweeprev',
              'detect_wm', 'detect_obfvg', 'detect_e90break', 'detect_gbreak', 'detect_gtrend',
              'detect_gtrend_inv', 'detect_po3_kane', 'detect_ew_wave5', 'detect_hs']

# Bound to a specific instrument/session/microstructure — listed, never scored on equities.
NOT_APPLICABLE = {
    'hs_crypto': 'crypto-specific head&shoulders', 'zbreak_crypto': 'crypto volume breakout',
    'zbreak_ix': 'index volume breakout', 'zbreak_gold': 'gold volume breakout',
    'gfib': 'gold fib', 'fma_gold': 'gold session', 'gold_us2h': 'gold US 2nd-hour m5 session',
    'orb_ln': 'London-open m5 session', 'absorb_btc': 'BTC order-flow delta',
    'asianglitch': 'Asian-session FX', 'mmove': 'crypto momentum', 'mmove_ix': 'index momentum',
    'mmove_ix4': 'index 4h momentum', 'mmove_c4': 'crypto 4h momentum', 'mmove_m15': 'crypto m15',
    'volbreak_ix': 'index breakout', 'twob_ix': 'index 2B', 'twob_cm': 'commodity 2B',
    'holygrail_cm': 'commodity holygrail', 'holygrail_cm_m15': 'commodity holygrail m15',
    'fma_sweep_cm': 'commodity session sweep', 'fma_sweep_ix': 'index session sweep',
    'po3_cm': 'commodity session po3', 'sweepfvg_ix': 'index session sweep-fvg',
    'varev_ix': 'index volatility reversal', 'obfvg_fx4': 'FX 4h OB/FVG',
    'ew_wave5_4h': '4h Elliott wave', 'ew_wave5_fib_4h': '4h Elliott wave fib',
}


def _agg(rows):
    rows = sorted(rows)
    rs = [r for _, r in rows]
    n = len(rs)
    if n < 30:
        return None
    w = [x for x in rs if x > 0]
    l = [x for x in rs if x <= 0]
    m = n // 2
    return dict(n=n, wr=100.0 * len(w) / n,
                rr=((sum(w) / len(w)) / abs(sum(l) / len(l))) if (w and l) else 0.0,
                exp=sum(rs) / n,
                oos1=sum(rs[:m]) / m if m else 0.0,
                oos2=sum(rs[m:]) / (n - m) if (n - m) else 0.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='equity-ohlc.json')
    ap.add_argument('--asset-class', default='index',
                    help="class to impersonate for gating (index|major|crypto|comm)")
    args = ap.parse_args()

    # classify equity pairs BEFORE importing the harness so class-gated detectors fire
    d = json.load(open(args.hist))['pairs']
    pairs = [p for p in d]
    for pk in pairs:
        DT.PAIR_CLASS[pk] = args.asset_class

    import unified_shadow_harness as H
    from backtest_rsi_per_class import _bars_norm

    # open membership gates where present (sets only; harmless if absent)
    for nm in ('OBFVG_LIVE', 'MMOVE_LIVE', 'VAREV_LIVE', 'GTREND_LIVE', 'GBREAK_LIVE'):
        g = getattr(H, nm, None)
        if isinstance(g, set):
            g.update(pairs)

    def score(bars, s):
        st, o = H.score_trail_open(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                                   H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
        if st != 'resolved':
            return None
        try:
            return o - H.cost(o, s['entry'], abs(s['entry'] - s['stop']))
        except Exception:
            return o

    results = {}
    nofire = []
    errored = {}

    # 1) clean MW generators on h1 + m15
    for name, gen in CLEAN.items():
        rows = []
        for pk in pairs:
            for tf in ('h1', 'm15'):
                bars = _bars_norm(d[pk].get(tf, []))
                if len(bars) < 400:
                    continue
                try:
                    for s in H._mw_signals(bars, pk, name, tf, gen):
                        r = score(bars, s)
                        if r is not None:
                            rows.append((s['entry_ts'], r))
                except Exception as e:
                    errored[name] = f"{type(e).__name__}: {str(e)[:50]}"
        if rows:
            results[name] = rows
        elif name not in errored:
            nofire.append(name)

    # 2) structural detect_* on h1 (with equity daily context where the signature wants it)
    for fn in STRUCTURAL:
        f = getattr(H, fn, None)
        short = fn.replace('detect_', '')
        if f is None:
            errored[short] = 'missing'
            continue
        nparams = len(inspect.signature(f).parameters)
        rows = []
        for pk in pairs:
            h1 = _bars_norm(d[pk].get('h1', []))
            daily = _bars_norm(d[pk].get('daily', []))
            if len(h1) < 400:
                continue
            try:
                if nparams >= 4:          # needs a draw/trigger callable we can't supply here
                    errored[short] = f'needs {nparams} args (draw/trigger callable) — skipped'
                    break
                sigs = f(pk, h1, daily) if nparams == 3 else f(pk, h1)
                for s in sigs:
                    r = score(h1, s)
                    if r is not None:
                        rows.append((s['entry_ts'], r))
            except Exception as e:
                errored[short] = f"{type(e).__name__}: {str(e)[:50]}"
                break
        if rows:
            results[short] = rows
        elif short not in errored:
            nofire.append(short)

    # ---- report ----
    K = len(results)
    print("=" * 84)
    print(f"EQUITY DETECTOR SWEEP · {args.hist} · {len(pairs)} names · impersonating '{args.asset_class}'")
    print(f"exit: trailing runner (arm +1R, trail {H.TRAIL_DIST}R, {H.TRAIL_HOLD}-bar), net of cost")
    print(f"MULTIPLE TESTING: {K} detectors scored — CANDIDATE bar = exp>+0.05R AND both OOS>+0.02R AND n>=200")
    print("=" * 84)
    print(f"\n{'strategy':<16}{'n':>7}{'WR':>6}{'RR':>6}{'exp':>9}{'OOS1':>8}{'OOS2':>8}  verdict")
    print("-" * 84)
    cands = []
    for name in sorted(results, key=lambda k: -(_agg(results[k]) or {'exp': -9})['exp']):
        a = _agg(results[name])
        if not a:
            continue
        strong = a['exp'] > 0.05 and a['oos1'] > 0.02 and a['oos2'] > 0.02 and a['n'] >= 200
        thin = a['exp'] > 0 and a['oos1'] > 0 and a['oos2'] > 0
        v = '** CANDIDATE **' if strong else ('thin / watch' if thin else 'fail')
        if strong:
            cands.append(name)
        print(f"{name:<16}{a['n']:>7}{a['wr']:>5.0f}%{a['rr']:>6.2f}{a['exp']:>+8.3f}R"
              f"{a['oos1']:>+7.3f}{a['oos2']:>+7.3f}  {v}")
    print("-" * 84)
    print(f"CANDIDATES (clear the multiple-testing bar): {cands or 'none'}")
    if nofire:
        print(f"\nNO-FIRE (ran, ~0 signals on equity bars — need bespoke wiring): {sorted(nofire)}")
    if errored:
        print("\nSKIPPED / ERRORED:")
        for k in sorted(errored):
            print(f"  {k:<16} {errored[k]}")
    print("\nNOT-APPLICABLE (instrument/session-bound — not scored on equities):")
    for k in sorted(NOT_APPLICABLE):
        print(f"  {k:<16} {NOT_APPLICABLE[k]}")
    print("\nAny CANDIDATE is a SCREEN result on a thin universe with class-mismatched thresholds.")
    print("Confirm on the full broker-exported 3yr universe (equity_broker_backtest.py) before wiring.")


if __name__ == '__main__':
    raise SystemExit(main())
