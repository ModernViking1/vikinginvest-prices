"""Intraday shadow forward-test for the NY-open FX 'flow model' observe candidate.

Analogue of unified_shadow_harness.py but on m15: detects the NY-open FX flow-model
signals (4H no-wick bias -> 1H liquidity sweep -> m15 MSS+FVG pullback, market entry,
fixed 2:1), logs each NEW one with the data-end at first sight, re-scores all logged
signals on the latest m15 bars, and reports IN-SAMPLE backfill vs GENUINE FORWARD
(entry after this harness's first run). MODEL-ONLY — writes flow-model-shadow-log.json,
which nothing on the platform trades. Evidence-gathering only.

Scoped to FX majors + minors (the only classes that survived walk-forward). RR2 is
the deploy target; the DOL/opposite-liquidity target is NOT used (it had no edge).

Run: python flow_model_shadow.py
"""
import json, os, bisect
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from flow_model_research import detect_flow_model, FM_RR, HOLD as FM_HOLD

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
LOG = os.path.join(_HERE, 'flow-model-shadow-log.json')
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
FX_CLASSES = {'major', 'minor'}


def _cost(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def score(m15, entry_ts, entry, stop, target, d, hold):
    """Fixed-target bracket on m15: ('resolved', r) on target/stop, ('expired', None)
    on time-out with data available, ('pending', None) if data runs out first."""
    ts = [b['_ts'] for b in m15]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(m15):
        return ('pending', None)
    rr = abs(target - entry)/R
    end = min(i0 + hold, len(m15))
    for j in range(i0, end):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= target: return ('resolved', rr)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= target: return ('resolved', rr)
    return ('pending', None) if end >= len(m15) else ('expired', None)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        if PAIR_CLASS.get(pk) not in FX_CLASSES:
            continue
        m15 = _bars_norm(pairs[pk].get('m15', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(m15) < 1000 or len(h1) < 200:
            continue
        data_end = max(data_end, m15[-1]['_ts'])
        for s in detect_flow_model(m15, h1, ny_only=True):
            detected += 1
            k = f"flowmodel:{pk}:{int(s['entry_ts'])}"
            if k not in sigs:
                s = dict(s); s['strategy'] = 'flowmodel'; s['pair'] = pk
                s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
            rec = sigs[k]
            st, o = score(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['target'], rec['dir'], FM_HOLD)
            rec['status'] = st
            if st == 'resolved':
                rec['r'] = o - _cost(o, rec['entry'], abs(rec['entry']-rec['stop']))
            else:
                rec.pop('r', None)
    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())

    def rep(title, rows):
        sub = [s for s in rows if s['status'] == 'resolved' and 'r' in s]
        pend = sum(1 for s in rows if s['status'] == 'pending')
        if sub:
            w = sum(1 for s in sub if s['r'] > 0)
            print(f"  {title:<28} resolved={len(sub):>3} pending={pend:>3} WR={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R")
        else:
            print(f"  {title:<28} resolved=0 pending={pend}")

    print(f"flow-model shadow · data_end {int(data_end)} · detected {detected} (RR{FM_RR}, NY-open FX)")
    rep("ALL (incl. in-sample)", allv)
    rep("GENUINE FORWARD", [s for s in allv if s['entry_ts'] > base])
    for c in ('major', 'minor'):
        rep(f"  {c}", [s for s in allv if PAIR_CLASS.get(s.get('pair')) == c])
    if all(s['entry_ts'] <= base for s in allv):
        print("  (baseline just set — re-run as new m15 bars publish to accumulate forward signals)")


if __name__ == '__main__':
    main()
