"""cam_rev PIVOT SCALE-IN backtest.

Current live = one entry at the rejection CLOSE. This tests ADDING a second leg: a resting limit
at the PIVOT (R3/S3), filled on a retest while the close position is open (two legs when both fill).
Each leg is its own 1:1 bracket off the shared structural stop.

The decision is NOT 'does the pivot leg make money' (it will — same fade) but 'does it beat simply
sizing the close entry up to the same risk'. So:
  CLOSE-ONLY  = 1 unit risk/signal (baseline, current live)
  SCALE-IN    = close leg + pivot leg (up to 2 units risk when both fill)
  CLOSE @ 2x  = 2 x the close leg (2 units risk/signal) — the fair equal-risk benchmark
The pivot leg adds real edge only if its per-R expectancy > the close leg's; if ~equal, it's just
correlated leverage and sizing the close up is simpler/better.

Faithful: close leg = H.score_sess from entry_ts (immediate, ~always fills live); pivot leg = resting
limit, touch-fill over the hold window. detect_cam_rev emits 'pivot'. Bracket-honest, resolved-only.
Commits nothing.

  python cam_pivot_scalein_backtest.py --hist deep-ohlc.json
"""
import argparse, bisect, json
import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'index', 'comm'}
RR = H.CAM_RR


def sim_pivot(bars, ts, entry_ts, pivot, stop, d, hold):
    """Resting limit at the pivot, touch-fill over the hold window, own 1:1 bracket. ->(status,R)."""
    R = abs(pivot - stop)
    if R <= 0:
        return ('nofill', None)
    target = pivot - RR * R if d == 'bear' else pivot + RR * R
    i0 = bisect.bisect_left(ts, entry_ts)
    end = min(i0 + hold, len(bars))
    fill = None
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull' and b['l'] <= pivot:
            fill = j; break
        if d == 'bear' and b['h'] >= pivot:
            fill = j; break
    if fill is None:
        return ('nofill', None)
    end2 = min(fill + hold, len(bars))
    for j in range(fill, end2):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:   return ('resolved', -1.0)
            if b['h'] >= target: return ('resolved', (target - pivot) / R)
        else:
            if b['h'] >= stop:   return ('resolved', -1.0)
            if b['l'] <= target: return ('resolved', (pivot - target) / R)
    return ('expired', None)


def agg(rs):
    n = len(rs)
    return dict(n=n, wr=100.0*sum(1 for r in rs if r > 0)/n, exp=sum(rs)/n, tot=sum(rs)) if n else None


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='deep-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    close_r, pivot_r = [], []
    n_sig = 0; pivot_fills = 0; both_resolved = 0
    for pk, layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or not isinstance(layers, dict):
            continue
        m15 = _bars_norm(layers.get('m15') or []); daily = _bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        ts = [b['_ts'] for b in m15]
        for s in H.detect_cam_rev(pk, m15, daily):
            if s.get('pivot') is None:
                continue
            n_sig += 1
            st, oc = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'], s['dir'], H.CAM_HOLD)
            if st == 'resolved':
                close_r.append(oc)
            ps, op = sim_pivot(m15, ts, s['entry_ts'], s['pivot'], s['stop'], s['dir'], H.CAM_HOLD)
            if ps != 'nofill':
                pivot_fills += 1
            if ps == 'resolved':
                pivot_r.append(op)
            if st == 'resolved' and ps == 'resolved':
                both_resolved += 1

    c = agg(close_r); p = agg(pivot_r)
    print(f"=== cam_rev · PIVOT SCALE-IN · {n_sig} signals · {args.hist} ===")
    print("(close leg = immediate fill; pivot leg = resting limit, touch-fill; each its own 1:1)\n")
    print(f">> CLOSE-ONLY leg (baseline, 1 unit risk/signal):")
    print(f"   n={c['n']}  WR={c['wr']:.0f}%  exp={c['exp']:+.3f}R  totR={c['tot']:+.0f}")
    print(f"\n>> PIVOT leg (the add-on):")
    print(f"   pivot fill rate: {100.0*pivot_fills/max(n_sig,1):.0f}% of signals ({pivot_fills}/{n_sig})")
    print(f"   resolved n={p['n']}  WR={p['wr']:.0f}%  exp={p['exp']:+.3f}R  totR={p['tot']:+.0f}")
    print(f"\n>> HEAD-TO-HEAD (the decision):")
    print(f"   close-leg exp = {c['exp']:+.3f}R   |   pivot-leg exp = {p['exp']:+.3f}R   "
          f"(pivot better by {p['exp']-c['exp']:+.3f}R)" )
    scalein_tot = c['tot'] + p['tot']
    close2x_tot = 2 * c['tot']
    avg_risk = 1 + pivot_fills / max(n_sig, 1)
    print(f"\n   SCALE-IN total R = close {c['tot']:+.0f} + pivot {p['tot']:+.0f} = {scalein_tot:+.0f}  "
          f"(avg ~{avg_risk:.2f} units risk/signal; both-open on {100.0*both_resolved/max(n_sig,1):.0f}% of signals)")
    print(f"   CLOSE @ 2x       = {close2x_tot:+.0f}  (2.00 units risk/signal) — the equal-risk benchmark")
    print(f"\n   R captured per unit of risk deployed:")
    print(f"     scale-in : {scalein_tot/ (avg_risk*n_sig):+.4f} R/signal/unit  (tot {scalein_tot:+.0f} over {avg_risk:.2f}u)")
    print(f"     close@2x : {close2x_tot/(2*n_sig):+.4f} R/signal/unit")
    print(f"     close@1x : {c['tot']/n_sig:+.4f} R/signal/unit")
    print("\nReading it: the scale-in is worth it ONLY if pivot-leg exp clearly beats close-leg exp AND")
    print("R-per-unit-risk beats close@2x. If pivot exp ~= close exp, it's correlated leverage — just")
    print("size the close entry up instead (simpler, same risk-adjusted return).")


if __name__ == '__main__':
    main()
