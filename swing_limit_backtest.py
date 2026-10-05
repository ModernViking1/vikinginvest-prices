"""Would LIMIT-entry fix the swing book's execution drift without killing fills?

The exec reconciliation showed market-entry fills drift ~0.5R from the model entry (late market
chase). The cBot has a limit-entry mode that fills AT the entry (no drift, aligns live with the
backtest) — but cam_rev proved a limit can also just NOT FILL. So before switching the swing
strategies to limit-entry we test, per strategy: does a resting limit at the entry fill, and does
its R match the immediate-fill baseline?

  baseline  = immediate fill at entry_ts (what the backtest assumes today)  — score_sess
  limit     = rest a limit at entry for the 12h window; fill if price returns to it; then score
Reads the deep cache in CI (3yr) or historical-ohlc.json locally. Commits nothing.

  python swing_limit_backtest.py --hist deep-ohlc.json
"""
import argparse, bisect, json
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

RR = 2.0
EXPIRY_H = 12          # swing_signals EXPIRY_HOURS (market strategies' resting window)

# live swing roster -> (detector callable(pk,h1,daily,m15,draw), timeframe, strategy filter)
def _roster():
    return {
        'hs':               (lambda pk,h1,dl,m,dr: H.detect_hs(pk,h1,dl,dr),                 'h1'),
        'twob_ix':          (lambda pk,h1,dl,m,dr: [s for s in H.detect_twob(pk,h1) if s['strategy']=='twob_ix'], 'h1'),
        'gbreak':           (lambda pk,h1,dl,m,dr: H.detect_gbreak(pk,h1,dl),                 'h1'),
        'mmove':            (lambda pk,h1,dl,m,dr: [s for s in H.detect_mmove(pk,h1,dl) if s['strategy']=='mmove'], 'h1'),
        'engulf_manip':     (lambda pk,h1,dl,m,dr: H.detect_engulf_manip(pk,h1,dl),           'h1'),
        'fma_sweep_cm':     (lambda pk,h1,dl,m,dr: [s for s in H.detect_fma(pk,m) if s['strategy']=='fma_sweep_cm'], 'm15'),
        'holygrail_cm_m15': (lambda pk,h1,dl,m,dr: [s for s in H.detect_holygrail_m15(pk,m) if s['strategy']=='holygrail_cm_m15'], 'm15'),
    }


def sim_limit(bars, ts, s, hold, expiry_bars):
    """Rest a limit at entry; fill if price returns within expiry_bars, then score the bracket."""
    entry, stop, d = s['entry'], s['stop'], s['dir']
    R = abs(entry - stop)
    if R <= 0:
        return ('nofill', None)
    tgt = entry + RR*R if d == 'bull' else entry - RR*R
    i0 = bisect.bisect_left(ts, s['entry_ts'])
    fill = None
    for j in range(i0, min(i0 + expiry_bars, len(bars))):
        b = bars[j]
        if d == 'bull' and b['l'] <= entry: fill = j; break
        if d == 'bear' and b['h'] >= entry: fill = j; break
    if fill is None:
        return ('nofill', None)
    for j in range(fill, min(fill + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= tgt:  return ('resolved', RR)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= tgt:  return ('resolved', RR)
    return ('expired', None)


def score_imm(bars, s, hold):
    st, o = H.score_sess(bars, s['entry_ts'], s['entry'], s['stop'],
                         (s['entry'] + RR*abs(s['entry']-s['stop'])) if s['dir']=='bull'
                         else (s['entry'] - RR*abs(s['entry']-s['stop'])), s['dir'], hold)
    return o if st == 'resolved' else None


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='historical-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    roster = _roster()
    HOLD = 96
    print(f"=== swing limit-fill vs immediate-fill · {args.hist} (RR{RR:.0f}, {EXPIRY_H}h window) ===\n")
    print(f"{'strategy':<17}{'signals':>8}{'fill%':>7} | {'immExp':>8}{'limExp':>8}{'Δ':>8}  (imm n / lim n)")
    for st, (fn, tf) in roster.items():
        imm, lim = [], []; nsig = 0; nfill = 0
        exp_bars = EXPIRY_H * (4 if tf == 'm15' else 1)
        for pk, layers in pairs.items():
            if not isinstance(layers, dict): continue
            h1 = _bars_norm(layers.get('h1') or []); daily = _bars_norm(layers.get('daily') or [])
            m15 = _bars_norm(layers.get('m15') or []); draw = layers.get('daily') or []
            if len(daily) < 30: continue
            bars = m15 if tf == 'm15' else h1
            if len(bars) < 400: continue
            ts = [b['_ts'] for b in bars]
            try:
                sigs = fn(pk, h1, daily, m15, draw)
            except Exception:
                continue
            for s in sigs:
                nsig += 1
                r = score_imm(bars, s, HOLD)
                if r is not None: imm.append(r)
                fst, fo = sim_limit(bars, ts, s, HOLD, exp_bars)
                if fst != 'nofill': nfill += 1
                if fst == 'resolved': lim.append(fo)
        if not nsig:
            print(f"{st:<17}{'(none)':>8}"); continue
        ie = sum(imm)/len(imm) if imm else 0
        le = sum(lim)/len(lim) if lim else 0
        fillpct = 100.0*nfill/nsig
        print(f"{st:<17}{nsig:>8}{fillpct:>6.0f}% | {ie:>+7.3f}R{le:>+7.3f}R{(le-ie):>+7.3f}R  ({len(imm)}/{len(lim)})")
    print("\nReading it: if fill% is HIGH and limExp ~= immExp, limit-entry fills reliably AND matches")
    print("the backtest -> switch that strategy to entry_mode='limit' to kill the market-chase drift.")
    print("If fill% is LOW, a limit would miss too many (the cam_rev problem) -> keep market + tighten")
    print("the cBot's MaxEntryDeviationPctOfR drift cap instead.")


if __name__ == '__main__':
    main()
