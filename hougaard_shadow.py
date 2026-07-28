"""Shadow forward-test for the Tom Hougaard 'situational' index setup (MONITOR ONLY).

Setup (see hougaard_situational_research.py): Friday fails to exceed Thursday's high
-> short the following Monday's open, target = Friday's low, stop = Thursday's high,
INDICES ONLY. In back-test the pooled-index cell was borderline positive (n=98,
+0.07R, both OOS halves +) BUT no single index was robust and half were negative — a
multiple-testing-flavoured aggregate. Too weak for the live cBot; logged here so a
genuine out-of-sample forward record decides it.

MODEL-ONLY — writes hougaard-shadow-log.json (nothing on the platform trades it).
h1-resolved (TP-first vs SL-first) over ~2 sessions.

Run: python hougaard_shadow.py
"""
import json, os, bisect
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from hougaard_situational_research import triples, HOLD_H1

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
LOG = os.path.join(_HERE, 'hougaard-shadow-log.json')
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
EXCLUDE = {'dxy'}                 # macro anchor, not a tradeable index


def _cost(o, entry, R):
    frac = R / abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def score(h1, entry_ts, entry, tp, sl):
    """Short: SL above (Thursday high), TP below (Friday low). Status + R."""
    R = sl - entry
    if R <= 0 or entry <= tp:
        return ('skip', None)
    rr = (entry - tp) / R
    ts = [b['_ts'] for b in h1]; i0 = bisect.bisect_left(ts, entry_ts)
    if i0 >= len(h1):
        return ('pending', None)
    end = min(i0 + HOLD_H1, len(h1))
    for j in range(i0, end):
        b = h1[j]
        if b['h'] >= sl:
            return ('resolved', -1.0)
        if b['l'] <= tp:
            return ('resolved', rr)
    return ('pending', None) if end >= len(h1) else ('expired', None)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    for pk in [x for x in PAIR_CLASS if x in pairs and PAIR_CLASS[x] == 'index' and x not in EXCLUDE]:
        daily = _bars_norm(pairs[pk].get('daily', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(daily) < 60 or len(h1) < 400:
            continue
        tri = triples(daily)
        if len(tri) < 10:
            continue
        data_end = max(data_end, h1[-1]['_ts'])
        for thu, fri, mon in tri:
            if not (fri['h'] < thu['h']):          # Friday failed Thursday's high
                continue
            detected += 1
            entry = mon['o']; tp = fri['l']; sl = thu['h']
            k = f"hougaard:{pk}:{int(mon['_ts'])}"
            if k not in sigs:
                sigs[k] = {'strategy': 'hougaard', 'pair': pk, 'dir': 'bear', 'entry_ts': int(mon['_ts']),
                           'entry': entry, 'target': tp, 'stop': sl, 'first_seen': data_end, 'status': 'pending'}
            rec = sigs[k]
            st, o = score(h1, rec['entry_ts'], rec['entry'], rec['target'], rec['stop'])
            rec['status'] = st
            if st == 'resolved':
                rec['r'] = o - _cost(o, rec['entry'], abs(rec['entry'] - rec['stop']))
            else:
                rec.pop('r', None)
    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    tracking = log.setdefault('tracking', {})
    fs = [s.get('first_seen') for s in sigs.values() if s.get('first_seen') is not None]
    if fs and 'hougaard' not in tracking:
        tracking['hougaard'] = int(min(fs))
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())

    def rep(title, rows):
        sub = [s for s in rows if s['status'] == 'resolved' and 'r' in s]
        pend = sum(1 for s in rows if s['status'] == 'pending')
        if sub:
            w = sum(1 for s in sub if s['r'] > 0)
            print(f"  {title:<24} resolved={len(sub):>3} pending={pend:>3} TPrate={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R")
        else:
            print(f"  {title:<24} resolved=0 pending={pend}")

    _t = tracking.get('hougaard'); _td = f" · tracked {int((data_end - _t)/86400)}d" if _t and data_end else ""
    print(f"hougaard index shadow · data_end {int(data_end)} · detected {detected} (short Mon open, TP=Fri low, SL=Thu high){_td}")
    rep("ALL (incl. in-sample)", allv)
    rep("GENUINE FORWARD", [s for s in allv if s['entry_ts'] > base])
    for pk in sorted({s['pair'] for s in allv}):
        rep(f"  {pk}", [s for s in allv if s['pair'] == pk])
    if all(s['entry_ts'] <= base for s in allv):
        print("  (baseline just set — re-run as new weeks publish)")


if __name__ == '__main__':
    main()
