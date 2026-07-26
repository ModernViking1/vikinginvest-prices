"""Intraday (m15) shadow forward-test for the OB+FVG retrace cells on indices.

Analogue of flow_model_shadow.py but for the 15m OB+FVG cells found in the per-pair
breakdown:
  SPX500-15m  — parameter-robust (13/20 grid combos PASS, +0.29R median). Flagged for
                the intraday pipeline; tracked here forward.
  DJ30-15m    — WATCH: base looked positive but the fixed-RR grid was weak (2/26); the
                day-range "edge" was a fat-tail artifact (one +341R trade). Logged to
                decide edge vs noise.

MODEL-ONLY — writes obfvg-intraday-shadow-log.json (nothing trades it). Same OB+FVG
retrace + market entry + fixed 2:1 as the swing obfvg candidate, on m15 bars.

Run: python obfvg_intraday_shadow.py
"""
import json, os, bisect
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import _obfvg_signals, OBFVG_HOLD

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
LOG = os.path.join(_HERE, 'obfvg-intraday-shadow-log.json')
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RR = 2.0
M15_HOLD = 96
LIVE_15M = {'spx500': 'obfvg15'}       # confirmed intraday cell
WATCH_15M = {'dj30': 'obfvg15_w'}      # watch cell


def _cost(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def score(m15, entry_ts, entry, stop, d, hold):
    ts = [b['_ts'] for b in m15]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(m15):
        return ('pending', None)
    tgt = entry + RR*R if d == 'bull' else entry - RR*R
    end = min(i0 + hold, len(m15))
    for j in range(i0, end):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= tgt: return ('resolved', RR)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= tgt: return ('resolved', RR)
    return ('pending', None) if end >= len(m15) else ('expired', None)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    cells = dict(LIVE_15M); cells.update(WATCH_15M)
    for pk, tag in cells.items():
        if pk not in pairs:
            continue
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(m15) < 1000:
            continue
        data_end = max(data_end, m15[-1]['_ts'])
        for s in _obfvg_signals(pk, m15, tag, tf='15m'):
            detected += 1
            k = f"{tag}:{pk}:{int(s['entry_ts'])}"
            if k not in sigs:
                s = dict(s); s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
            rec = sigs[k]
            st, o = score(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], M15_HOLD)
            rec['status'] = st
            if st == 'resolved':
                rec['r'] = o - _cost(o, rec['entry'], abs(rec['entry']-rec['stop']))
            else:
                rec.pop('r', None)
    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    tracking = log.setdefault('tracking', {})
    fs_by_strat = {}
    for s in sigs.values():
        st = s.get('strategy'); fs = s.get('first_seen')
        if st and fs is not None:
            fs_by_strat[st] = fs if st not in fs_by_strat else min(fs_by_strat[st], fs)
    for st, fs in fs_by_strat.items():
        if st not in tracking:
            tracking[st] = int(fs)
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())

    def rep(title, rows):
        sub = [s for s in rows if s['status'] == 'resolved' and 'r' in s]
        pend = sum(1 for s in rows if s['status'] == 'pending')
        if sub:
            w = sum(1 for s in sub if s['r'] > 0)
            print(f"  {title:<22} resolved={len(sub):>3} pending={pend:>3} WR={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R")
        else:
            print(f"  {title:<22} resolved=0 pending={pend}")

    print(f"obfvg intraday shadow · data_end {int(data_end)} · detected {detected} (m15, RR{RR})")
    for pk, tag in {**LIVE_15M, **WATCH_15M}.items():
        _t = tracking.get(tag); _td = f" · tracked {int((data_end - _t)/86400)}d" if _t else ""
        rep(f"{pk} ({tag}){_td}", [s for s in allv if s.get('strategy') == tag])
    rep("GENUINE FORWARD", [s for s in allv if s['entry_ts'] > base])
    if all(s['entry_ts'] <= base for s in allv):
        print("  (baseline just set — re-run as new m15 bars publish)")


if __name__ == '__main__':
    main()
