"""Does moving the stop to break-even early help the trailing-runner book, or eat the winners?

Tests exit-rule variants on the Market-Wizards continuation detectors (holygrail / volbreak / twob)
— the exact exit mechanic the .EQ book uses (arm +1R, trail 1R, no fixed target). For each signal we
replay the SAME bars under each rule and compare expectancy / WR / avg-win / avg-loss, so we can see
whether a +0.5R-to-BE floor protects more than it costs.

  baseline   arm 1R, trail 1R                (current — BE effectively arrives at +1R)
  be_0.5     + move stop to BE once +0.5R     (the proposed rule)
  be_0.75    + move stop to BE once +0.75R
  arm_0.5    arm the trail at +0.5R instead   (earlier protection, keeps riding)
  trail_0.75 arm 1R, trail 0.75R              (tighter runner)
  trail_1.5  arm 1R, trail 1.5R               (wider runner — let it breathe)

Baseline reproduces unified_shadow_harness.score_trail_open exactly (same intrabar ordering, no
look-ahead). Reads the deep cache (3yr, CI) or any *-ohlc.json locally. Commits nothing.

  python exit_rule_backtest.py --hist equity-ohlc.json
  python exit_rule_backtest.py --hist deep-ohlc.json
"""
import argparse, bisect, json
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

HOLD = H.TRAIL_HOLD   # 200 bars

DETECTORS = [('holygrail', H._holygrail_sig), ('volbreak', H._volbreak_sig), ('twob', H._twob_sig)]
VARIANTS = {
    'baseline':   dict(arm=1.0,  trail=1.0,  be_at=None),
    'be_0.5':     dict(arm=1.0,  trail=1.0,  be_at=0.5),
    'be_0.75':    dict(arm=1.0,  trail=1.0,  be_at=0.75),
    'arm_0.5':    dict(arm=0.5,  trail=1.0,  be_at=None),
    'trail_0.75': dict(arm=1.0,  trail=0.75, be_at=None),
    'trail_1.5':  dict(arm=1.0,  trail=1.5,  be_at=None),
}
ORDER = ['baseline', 'be_0.5', 'be_0.75', 'arm_0.5', 'trail_0.75', 'trail_1.5']


def score(bars, ts, entry_ts, entry, stop, d, hold, arm, trail, be_at):
    """Trailing-runner with optional early break-even floor. Returns R, or None if it never resolves
    (not enough bars past entry to reach the horizon). Mirrors score_trail_open's intrabar ordering:
    test the stop against this bar first (stop set from prior bars), THEN ratchet from this bar's extreme."""
    i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return None
    es = stop; armed = False; peak = entry; end = i0 + hold
    for j in range(i0, min(end, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= es:
                return (es - entry) / R
            peak = max(peak, b['h'])
            if be_at is not None and peak >= entry + be_at * R:
                es = max(es, entry)
            if not armed and b['h'] >= entry + arm * R:
                armed = True
            if armed:
                es = max(es, peak - trail * R)
        else:
            if b['h'] >= es:
                return (entry - es) / R
            peak = min(peak, b['l'])
            if be_at is not None and peak <= entry - be_at * R:
                es = min(es, entry)
            if not armed and b['l'] <= entry - arm * R:
                armed = True
            if armed:
                es = min(es, peak + trail * R)
    if end <= len(bars):
        lc = bars[end - 1]['c']
        return ((lc - entry) if d == 'bull' else (entry - lc)) / R
    return None


def stats(rs):
    n = len(rs)
    if not n:
        return None
    w = [x for x in rs if x > 0.02]; l = [x for x in rs if x <= 0.02]
    return dict(n=n, wr=100.0 * len(w) / n, exp=sum(rs) / n, tot=sum(rs),
                aw=(sum(w) / len(w) if w else 0.0), al=(sum(l) / len(l) if l else 0.0),
                be=sum(1 for x in rs if -0.02 <= x <= 0.02))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='equity-ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    out = defaultdict(list)
    nsig = 0
    for pk, layers in pairs.items():
        if not isinstance(layers, dict):
            continue
        h1 = _bars_norm(layers.get('h1') or [])
        if len(h1) < 400:
            continue
        ts = [b['_ts'] for b in h1]
        for tag, gen in DETECTORS:
            for s in H._mw_signals(h1, pk, tag, 'h1', gen):
                base = score(h1, ts, s['entry_ts'], s['entry'], s['stop'], s['dir'], HOLD, 1.0, 1.0, None)
                if base is None:
                    continue                      # unresolved -> exclude from every variant equally
                nsig += 1
                for v, p in VARIANTS.items():
                    r = score(h1, ts, s['entry_ts'], s['entry'], s['stop'], s['dir'], HOLD, **p)
                    if r is not None:
                        out[v].append(r)

    print(f"=== exit-rule backtest · {args.hist} · MW continuation (holygrail/volbreak/twob) h1 · "
          f"{nsig} resolved signals ===\n")
    print(f"{'variant':<12}{'n':>6}{'WR':>6}{'exp':>9}{'totR':>8}{'avgWin':>8}{'avgLoss':>9}{'BE/scr':>8}{'vs base':>9}")
    bexp = stats(out['baseline'])['exp']
    for v in ORDER:
        s = stats(out[v])
        if not s:
            continue
        delta = s['exp'] - bexp
        print(f"{v:<12}{s['n']:>6}{s['wr']:>5.0f}%{s['exp']:>+8.3f}R{s['tot']:>+7.0f}{s['aw']:>+8.2f}"
              f"{s['al']:>+9.2f}{s['be']:>8}{delta:>+8.3f}R")
    print("\nReading it: a rule BEATS baseline only if 'vs base' is clearly > 0. A BE floor should LIFT WR")
    print("and avgLoss (fewer -1R) but CUT avgWin (runners scratched on the pullback) — net is what matters.")


if __name__ == '__main__':
    main()
