"""Shadow forward-test harness for the H&S · macro-OPPOSES edge.

Run periodically (manually or from CI). Each run:
  1. Detects current H&S setups that fire AGAINST a high-confidence macro-EW read
     (the validated cohort) on the latest published H1/daily data.
  2. Logs any NEW ones (dedup by pair+entry_ts) to hs-shadow-log.json, stamped
     with the data-end at first sight.
  3. Re-scores every logged signal against the latest data (resolved / pending).
  4. Reports running stats, separating GENUINE FORWARD signals (entry after the
     harness's first-ever run) from the in-sample backfill.

Touches NO platform file — hs-shadow-log.json is inert research data the cBot and
dashboard never read. This is how we earn confidence in the edge with real
out-of-sample trades before proposing any deployment.
"""
import json, os, bisect
from detect_triggers import (
    PAIR_CLASS, auto_detect_ew, AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm
from hs_swing_research import scan, MAX_HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
LOG = '/home/user/vikinginvest-prices/hs-shadow-log.json'
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
TARGETS = {'1:1': 1.0, '1:2': 2.0}


def resolve(h1, i0, entry, stop, kind, target):
    d = 'bear' if kind == 'bear' else 'bull'
    R = abs(entry - stop); rr = abs(target - entry) / R if R else 0
    for j in range(i0, min(i0 + MAX_HOLD, len(h1))):
        b = h1[j]
        if d == 'bear':
            if b['h'] >= stop: return -1.0
            if b['l'] <= target: return rr
        else:
            if b['l'] <= stop: return -1.0
            if b['h'] >= target: return rr
    return None


def cost(o, entry, R):
    frac = R / abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']

    data_end = 0
    detected = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', []))
        daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 300 or len(daily) < 35: continue
        data_end = max(data_end, h1[-1]['_ts'])
        d_ts = [b['_ts'] for b in daily]; cache = {}
        def aew(dd):
            if dd not in cache:
                try:
                    r = auto_detect_ew(draw[:dd + 1]); e = r.get('ew') if r.get('ok') else None
                    cache[dd] = e['dir'] if (e and e.get('dir') in ('bull', 'bear') and e.get('confidence', 0) >= AUTO_EW_MIN_CONFIDENCE and e.get('pattern') in AUTO_EW_VALID_PATTERNS) else None
                except Exception:
                    cache[dd] = None
            return cache[dd]
        for kind in ('bear', 'bull'):
            for tr in scan(h1, kind):
                dd = bisect.bisect_right(d_ts, tr['ts']) - 2
                macro = aew(dd); tdir = 'bear' if kind == 'bear' else 'bull'
                if not (macro is not None and macro != tdir):
                    continue
                detected += 1
                key = f"{pk}:{int(tr['ts'])}:{kind}"
                if key not in sigs:
                    sigs[key] = {
                        'pair': pk, 'cls': PAIR_CLASS.get(pk), 'kind': kind,
                        'entry_ts': tr['ts'], 'entry': tr['entry'], 'stop': tr['stop'],
                        'R': tr['R'], 'mm_target': tr['mm_target'],
                        'first_seen_data_end': data_end, 'outcome': {}, 'status': 'pending',
                    }
                # (re)score against current data
                s = sigs[key]
                s['outcome'] = {}; s['status'] = 'resolved'
                i0 = bisect.bisect_left([b['_ts'] for b in h1], s['entry_ts'])  # entry bar, inclusive
                for tn, mult in TARGETS.items():
                    tgt = s['entry'] - mult * s['R'] if kind == 'bear' else s['entry'] + mult * s['R']
                    o = resolve(h1, i0, s['entry'], s['stop'], kind, tgt)
                    if o is None:
                        s['status'] = 'pending'
                    else:
                        s['outcome'][tn] = o - cost(o, s['entry'], s['R'])

    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']
    allv = list(sigs.values())
    fwd = [s for s in allv if s['entry_ts'] > base]

    def rep(name, rows):
        res = [s for s in rows if s['status'] == 'resolved']
        pend = len(rows) - len(res)
        print(f"\n{name}: {len(rows)} logged  ·  {len(res)} resolved  ·  {pend} pending")
        for tn in TARGETS:
            seq = [s['outcome'][tn] for s in res if tn in s['outcome']]
            if seq:
                w = sum(1 for x in seq if x > 0)
                print(f"    {tn}: n={len(seq)} WR={100*w/len(seq):.1f}% exp={sum(seq)/len(seq):+.3f}R")

    print(f"harness run · data_end stamp {int(data_end)} · detected {detected} setups this pass")
    rep("ALL logged (incl. in-sample backfill)", allv)
    rep("GENUINE FORWARD (entry after first run)", fwd)
    if not fwd:
        print("\n  (no forward signals yet — baseline just set; re-run as new bars publish to accumulate them)")


if __name__ == '__main__':
    main()
