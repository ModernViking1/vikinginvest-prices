"""Build equity signal paths for the 13 structural detectors that NO-FIRE'd in the roster sweep,
and test them on equity bars with their OWN native exit (not the book's trail-runner), 3yr,
net of cost, both OOS halves, multiple-testing bar.

Each detector is hard-gated to a native asset class / instrument / allowlist and uses a class-tuned
timeframe + exit. We unblock each the minimal honest way and score it with the exit it was designed
for, so this tests whether the SIGNAL has predictive value on stocks — not whether it fits the book:
  rsimr        class->major   4h  RSI-50 mean-reversion exit (score_meanrev)
  fib_gz       class->comm    h1  fixed RR2            (score)
  fredtl       pk='xauusd'    4h  fixed RR2            (score)   [runs the gold logic on eq bars]
  threepush    class->comm    4h  fixed RR2            (score)
  engulf_manip class->crypto  4h  fixed RR2            (score)
  sweeprev     class->minor   4h  sweep-reversal exit  (score_sweeprev)
  wm           class->crypto  h1  RR1 asian-style      (score_asianglitch)
  e90break     class->crypto  h1  RR1.5 asian-style    (score_asianglitch)
  ew_wave5     class->comm    4h  session RR2          (score_sess)
  po3_kane     add to POCKETS h1  session RR2 (US open)(score_sess)

gbreak / gtrend were ALREADY tested on equities by equity_swing_research.py (both FAIL: h1 gbreak
-0.040R, gtrend -0.014R, and thin/fail on the swing TF); gtrend_inv is gtrend inverted (a ~0R edge
inverted stays ~0R and loses to double costs). They are reported from that existing evidence, not
re-run here.

Impersonating a class applies that class's tuned thresholds to equity price data — mostly ATR/price-
relative (scale-invariant), but still a SCREEN. CANDIDATE bar (Bonferroni-ish for ~10 tests):
exp>+0.05R AND both OOS>+0.02R AND n>=150.

  python equity_structural_sweep.py --hist equity-ohlc.json
"""
import argparse
import json

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm


def _cost_net(o, entry, stop):
    try:
        return o - H.cost(o, entry, abs(entry - stop))
    except Exception:
        return o


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='equity-ohlc.json')
    args = ap.parse_args()
    d = json.load(open(args.hist))['pairs']
    pairs = list(d)

    H4HOLD = (H.HOLD['4h'] if isinstance(getattr(H, 'HOLD', None), dict) and '4h' in H.HOLD else 60)
    H1HOLD = getattr(H, 'HS_HOLD', 120)

    # (label, detector, fire, scorer, tf) ; fire = ('class', X) | ('pkalias', X) | ('pocket',)
    CONFIG = [
        ('rsimr',        H.detect_rsimr,        ('class', 'major'),  'meanrev',  '4h'),
        ('fib_gz',       H.detect_fibgz,        ('class', 'comm'),   'rr2',      'h1'),
        ('fredtl',       H.detect_fredtl,       ('pkalias', 'xauusd'),'rr2',     '4h'),
        ('threepush',    H.detect_threepush,    ('class', 'comm'),   'rr2',      '4h'),
        ('engulf_manip', H.detect_engulf_manip, ('class', 'crypto'), 'rr2',      '4h'),
        ('sweeprev',     H.detect_sweeprev,     ('class', 'minor'),  'sweeprev', '4h'),
        ('wm',           H.detect_wm,           ('class', 'crypto'), 'asian',    'h1'),
        ('e90break',     H.detect_e90break,     ('class', 'crypto'), 'asian',    'h1'),
        ('ew_wave5',     H.detect_ew_wave5,     ('class', 'comm'),   'sess',     '4h'),
        ('po3_kane',     H.detect_po3_kane,     ('pocket',),         'sess',     'h1'),
    ]

    def call(det, fire, pk, h1, daily):
        import inspect
        n = len(inspect.signature(det).parameters)
        if fire[0] == 'pkalias':
            return det('xauusd', h1, daily) if n >= 3 else det('xauusd', h1)
        if fire[0] == 'class':
            H.PAIR_CLASS[pk] = fire[1]
        elif fire[0] == 'pocket':
            try: H.PO3K_POCKETS.add(pk)
            except Exception: pass
        return det(pk, h1, daily) if n >= 3 else det(pk, h1)

    def score_one(label, scorer, tf, h1, b4, rsi4, s):
        ts, e, st, dr = s['entry_ts'], s['entry'], s['stop'], s['dir']
        if scorer == 'meanrev':
            r = H.score_meanrev(b4, rsi4, ts, e, st, dr, H.RSIMR_HOLD)
        elif scorer == 'sweeprev':
            r = H.score_sweeprev(b4, ts, e, st, s.get('target'), dr, H.SWEEPREV_HOLD)
        elif scorer == 'asian':
            hold = H.WM_HOLD if label == 'wm' else H.E90_HOLD
            rr = s.get('rr', H.WM_RR if label == 'wm' else H.E90_RR)
            r = H.score_asianglitch(h1, ts, e, st, dr, hold, rr)
        elif scorer == 'sess':
            bars = b4 if tf == '4h' else h1
            hold = H.EW_WAVE5_HOLD if label == 'ew_wave5' else H.PO3K_HOLD
            r = H.score_sess(bars, ts, e, st, s.get('target'), dr, hold)
        else:  # rr2
            bars = b4 if tf == '4h' else h1
            r = H.score(bars, ts, e, st, dr, H4HOLD if tf == '4h' else H1HOLD)
        stt, o = r
        if stt != 'resolved':
            return None
        return _cost_net(o, e, st)

    results = {}
    errors = {}
    for label, det, fire, scorer, tf in CONFIG:
        rows = []
        for pk in pairs:
            h1 = _bars_norm(d[pk].get('h1', []))
            daily = _bars_norm(d[pk].get('daily', []))
            if len(h1) < 400:
                continue
            b4 = H.agg4h(h1)
            rsi4 = H.precompute_rsi([b['c'] for b in b4], 14) if scorer == 'meanrev' else None
            try:
                sigs = call(det, fire, pk, h1, daily)
            except Exception as e:
                errors[label] = f"{type(e).__name__}: {str(e)[:60]}"
                sigs = []
            for s in sigs or []:
                try:
                    r = score_one(label, scorer, tf, h1, b4, rsi4, s)
                except Exception as e:
                    errors[label] = f"score {type(e).__name__}: {str(e)[:50]}"
                    r = None
                if r is not None:
                    rows.append((s['entry_ts'], r))
        results[label] = rows

    def agg(rows):
        rows = sorted(rows)
        rs = [r for _, r in rows]
        n = len(rs)
        if n == 0:
            return None
        w = [x for x in rs if x > 0]
        l = [x for x in rs if x <= 0]
        m = n // 2
        return dict(n=n, wr=100.0 * len(w) / n,
                    rr=((sum(w) / len(w)) / abs(sum(l) / len(l))) if (w and l) else 0.0,
                    exp=sum(rs) / n,
                    oos1=sum(rs[:m]) / m if m else 0.0,
                    oos2=sum(rs[m:]) / (n - m) if (n - m) else 0.0)

    print("=" * 84)
    print(f"EQUITY STRUCTURAL SWEEP · {args.hist} · {len(pairs)} names · native exits · net of cost")
    print("CANDIDATE bar: exp>+0.05R AND both OOS>+0.02R AND n>=150")
    print("=" * 84)
    print(f"\n{'detector':<14}{'n':>6}{'WR':>6}{'RR':>6}{'exp':>9}{'OOS1':>8}{'OOS2':>8}  verdict")
    print("-" * 84)
    cands = []
    for label, *_ in CONFIG:
        a = agg(results.get(label, []))
        if a is None:
            note = errors.get(label, 'no signals (setup never occurs on equity bars)')
            print(f"{label:<14}{'0':>6}{'—':>6}{'—':>6}{'—':>9}{'—':>8}{'—':>8}  NO-FIRE: {note}")
            continue
        strong = a['exp'] > 0.05 and a['oos1'] > 0.02 and a['oos2'] > 0.02 and a['n'] >= 150
        thin = a['exp'] > 0 and a['oos1'] > 0 and a['oos2'] > 0
        if strong:
            cands.append(label)
        v = '** CANDIDATE **' if strong else ('thin / watch' if thin else 'fail')
        print(f"{label:<14}{a['n']:>6}{a['wr']:>5.0f}%{a['rr']:>6.2f}{a['exp']:>+8.3f}R"
              f"{a['oos1']:>+7.3f}{a['oos2']:>+7.3f}  {v}")
    print("-" * 84)
    print(f"CANDIDATES: {cands or 'none'}")
    print("\nPRIOR EVIDENCE (equity_swing_research.py, not re-run):")
    print("  gbreak      h1 ALL -0.040R (both-OOS unstable) — FAIL on equities")
    print("  gtrend      h1 ALL -0.014R (negative)          — FAIL on equities")
    print("  gtrend_inv  inverse of a ~0R edge, loses to 2x cost — FAIL by construction")
    print("\nSCREEN on 5 US names with class-impersonated thresholds — confirm any candidate on the")
    print("full broker-exported universe before trusting it.")


if __name__ == '__main__':
    raise SystemExit(main())
