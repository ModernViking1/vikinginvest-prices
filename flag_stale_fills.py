"""Classify closed swing (demo) fills that the 2026-09-09 stale-fill guards would now
block, and report per-strategy win rate RAW vs GUARDED (ex-stale-fill).

Two guard tests, applied causally to each closed trade by reconstructing the signal's
intended ref_entry from the detector on the published history:
  • drift  — |entry_filled - stop| > (1+MAX_DRIFT_R)*|ref_entry - stop|  (risk inflated by a
             late fill far from ref_entry; the USDCAD obfvg case)
  • stopinv — price already traded through the stop between the trigger bar and the fill
             (setup invalidated before the late fill; the USDCHF hs case)

We do NOT rewrite the execution log — the raw live record stays intact and honest. This
writes a DERIVED file (swing-stale-fills.json: signal_id -> reason) and prints the impact,
so reported edge can be shown both ways: the real demo result, and the forward expectation
now the execution bug is fixed. A trade whose trigger bar has rolled out of the retained
history is marked 'unknown' (cannot reconstruct) and is NEVER flagged.
"""
import json, os, bisect
from collections import defaultdict
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import (detect_hs, detect_s5, detect_obfvg, detect_ob, detect_tl,
    detect_w5pb, detect_gbreak, detect_gtrend, detect_fibgz, detect_fredtl, detect_asianglitch,
    detect_wm, detect_fma, detect_s5_rsi_wide, detect_twob)

_HERE = os.path.dirname(os.path.abspath(__file__))
MAX_DRIFT_R = 0.5          # matches VikingSwingBridge MaxEntryDriftR (1.5x intended stop)
KNOWN = {'hs','s5_rsi','obfvg','ob','tl','w5pb','twob','twob_cm','twob_ix','gbreak','gtrend',
         'fibgz','fredtl','asianglitch','wm','fma_gold','s5_rsi_wide'}

def strat_of(r):
    sid = r.get('signal_id') or ''
    p = sid.split(':')
    if p and p[0] in KNOWN: return p[0], (p[1] if len(p) > 1 else ''), (int(p[2]) if len(p) > 2 and p[2].isdigit() else None)
    if len(p) >= 3 and p[-1] in KNOWN: return p[-1], p[0], (int(p[1]) if p[1].isdigit() else None)
    return (r.get('strategy') or (p[0] if p else '?')), (p[1] if len(p) > 1 else ''), None

def dets(pk, h1, daily, draw):
    """All swing detector outputs for a pair, indexed by (strategy, int(entry_ts))."""
    out = {}
    def add(sigs):
        for s in sigs:
            out[(s['strategy'], int(s['entry_ts']))] = s
    try: add(detect_hs(pk, h1, daily, draw))
    except Exception: pass
    for fn, args in ((detect_s5,(pk,h1,daily,'rsi')),):
        try: add(fn(*args))
        except Exception: pass
    for fn in (detect_obfvg, detect_ob, detect_tl, detect_w5pb, detect_fibgz):
        try: add(fn(pk, h1, daily))
        except Exception: pass
    for fn in (detect_gbreak, detect_gtrend, detect_fredtl, detect_asianglitch, detect_wm):
        try: add(fn(pk, h1, daily))
        except Exception: pass
    try: add(detect_s5_rsi_wide(pk, h1, daily))
    except Exception: pass
    try: add(detect_fma(pk, m15_of(pk)))
    except Exception: pass
    try: add(detect_twob(pk, h1))
    except Exception: pass
    return out

_PAIRS = json.load(open(os.path.join(_HERE, 'historical-ohlc.json'))).get('pairs', {})
def m15_of(pk): return _bars_norm(_PAIRS.get(pk, {}).get('m15', []))

def classify():
    ex = json.load(open(os.path.join(_HERE, 'swing-executions.json'))).get('executions', [])
    cl = [r for r in ex if r.get('event') == 'closed' and r.get('realized_r') is not None]
    cache = {}
    flagged = {}           # signal_id -> reason
    rows = defaultdict(lambda: {'all': [], 'kept': []})
    n_unknown = 0
    for r in cl:
        st, pk, trig = strat_of(r)
        rr = r['realized_r']
        rows[st]['all'].append(rr)
        if trig is None or pk not in _PAIRS:
            n_unknown += 1; rows[st]['kept'].append(rr); continue
        if pk not in cache:
            h1 = _bars_norm(_PAIRS[pk].get('h1', [])); daily = _bars_norm(_PAIRS[pk].get('daily', []))
            cache[pk] = (h1, daily, _PAIRS[pk].get('daily', []), [b['_ts'] for b in h1])
        h1, daily, draw, ts = cache[pk]
        if len(h1) < 100:
            n_unknown += 1; rows[st]['kept'].append(rr); continue
        idx = dets(pk, h1, daily, draw)
        ref = idx.get((st, trig))
        entry_f = r.get('entry_filled'); stop = r.get('stop'); d = r.get('dir')
        reason = None
        if ref is not None and entry_f and stop:
            Rref = abs(ref['entry'] - ref['stop'])
            Ract = abs(entry_f - stop)
            if Rref > 0 and Ract > (1 + MAX_DRIFT_R) * Rref:
                reason = 'drift'
            else:
                # stop-invalidation: breach in (trigger, estimated fill bar]
                fill_i = None
                for j in range(bisect.bisect_right(ts, trig), len(h1)):
                    b = h1[j]
                    if b['l'] <= entry_f <= b['h']: fill_i = j; break
                if fill_i is not None:
                    for j in range(bisect.bisect_right(ts, trig), fill_i):
                        b = h1[j]
                        if (d == 'bear' and b['h'] >= ref['stop']) or (d == 'bull' and b['l'] <= ref['stop']):
                            reason = 'stopinv'; break
        else:
            n_unknown += 1
        if reason:
            flagged[r['signal_id']] = reason
        else:
            rows[st]['kept'].append(rr)
    return rows, flagged, n_unknown

def agg(rs):
    n = len(rs)
    if not n: return (0, 0.0, 0.0)
    return (n, 100*sum(1 for x in rs if x > 0)/n, sum(rs)/n)

def main():
    rows, flagged, n_unknown = classify()
    print("STALE-FILL classification — RAW live vs GUARDED (ex-stale-fill). Records untouched.")
    print("flagged: %d closed fills  |  unreconstructable(kept): %d\n" % (len(flagged), n_unknown))
    print("  %-14s %-22s %-22s" % ("strategy", "RAW (all fills)", "GUARDED (ex-stale)"))
    tot_all, tot_kept = [], []
    for st in sorted(rows, key=lambda s: -len(rows[s]['all'])):
        na, wa, ea = agg(rows[st]['all']); nk, wk, ek = agg(rows[st]['kept'])
        tot_all += rows[st]['all']; tot_kept += rows[st]['kept']
        d = len(rows[st]['all']) - len(rows[st]['kept'])
        star = "   <== " + ("%d removed" % d) if d else ""
        print("  %-14s n=%3d WR=%2.0f%% %+0.3fR  |  n=%3d WR=%2.0f%% %+0.3fR%s" % (
            st or '(unattr)', na, wa, ea, nk, wk, ek, star))
    na, wa, ea = agg(tot_all); nk, wk, ek = agg(tot_kept)
    print("  " + "-"*60)
    print("  %-14s n=%3d WR=%2.0f%% %+0.3fR  |  n=%3d WR=%2.0f%% %+0.3fR" % ("ALL", na, wa, ea, nk, wk, ek))
    with open(os.path.join(_HERE, 'swing-stale-fills.json'), 'w') as f:
        json.dump({'note': 'derived: closed swing fills the 2026-09-09 stale-fill guards would block; '
                   'raw execution log is unmodified', 'max_drift_r': MAX_DRIFT_R,
                   'flagged': flagged}, f, indent=1)
    print("\nwrote swing-stale-fills.json (%d flagged)" % len(flagged))

if __name__ == '__main__':
    main()
