#!/usr/bin/env python3
"""Generate the per-pair EW hit-rate table embedded in the Auto-EW panel.

Reuses the accuracy harness's replay. For each FX pair, counts target-hit
(win) vs stop (loss) when the blended EW read AGREED vs DISAGREED with the
trade direction. Emits a compact JS const for dashboard-backtest-ui.js.

Run:  python3 gen_ew_hitrate.py > /tmp/ew_hitrate.js
Then paste the EW_HITRATE const into dashboard-backtest-ui.js.
"""
import json
from backtest_ew_accuracy import run_pair
import detect_triggers as dt

ASOF = "2026-07-01"   # stamp by hand — Date.now() is unavailable in harness

def main():
    hist = json.load(open('historical-ohlc.json'))
    fx = [p for p, c in dt.PAIR_CLASS.items() if c in ('major', 'minor')
          and p in hist['pairs']]
    rows = []
    for pair in fx:
        run_pair(pair, hist, rows)

    per = {}
    allc = {'aW': 0, 'aL': 0, 'dW': 0, 'dL': 0}
    for r in rows:
        eb, d = r['ew_blend'], r['dir']
        if eb not in ('bull', 'bear'):
            continue  # neutral — not agree/disagree
        agree = (eb == d)
        win = (r['outcome'] == 'win')
        b = per.setdefault(r['pair'], {'aW': 0, 'aL': 0, 'dW': 0, 'dL': 0})
        key = ('aW' if win else 'aL') if agree else ('dW' if win else 'dL')
        b[key] += 1
        allc[key] += 1

    out = {'_asof': ASOF, '_all': allc}
    for k in sorted(per):
        out[k] = per[k]

    # Emit as a JS const (compact one-line objects per pair).
    print("// EW hit-rate track record — target-hit rate when the blended EW")
    print("// read AGREED (a) vs DISAGREED (d) with the trade direction.")
    print("// Frictionless macd-primary replay over the ~2mo m15 window.")
    print("// Regenerate: python3 gen_ew_hitrate.py  (as-of " + ASOF + ")")
    print("var EW_HITRATE = {")
    print('  "_asof": "%s",' % out['_asof'])
    a = out['_all']
    print('  "_all": {"aW":%d,"aL":%d,"dW":%d,"dL":%d},' % (a['aW'], a['aL'], a['dW'], a['dL']))
    for k in sorted(per):
        b = out[k]
        print('  "%s": {"aW":%d,"aL":%d,"dW":%d,"dL":%d},' % (k, b['aW'], b['aL'], b['dW'], b['dL']))
    print("};")

if __name__ == '__main__':
    main()
