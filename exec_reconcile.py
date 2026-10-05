"""Per-strategy LIVE-vs-MODEL execution reconciliation across the whole live book.

Generalises cam_rev_reconcile to every live strategy: joins each real cBot fill (swing-executions.json
+ executions.json) to the model signal it should correspond to (swing-shadow-log.json), classifies
MATCHED (same bar+dir) / DRIFTED (model fired that day, different bar) / ORPHAN (no model that
pair+day), and on MATCHED trades isolates the execution drag — live R vs model R, entry drift (in R),
and fill latency. Tells us, per strategy, whether the backtest-vs-live gap is SELECTION (wrong bars)
or EXECUTION (slippage/cost/late fills on the right bars). Reads committed logs only; commits nothing.

  python exec_reconcile.py
"""
import bisect, json, os
import datetime as dt
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
SANE_R = 6.0
try:
    from observer_review import LIVE as LIVE_ROSTER
except Exception:
    LIVE_ROSTER = set()


def _parse_id(sid, swing):
    """-> (strategy, pair, trigger_ts_seconds) or None. swing ids = strat:pair:ts(sec);
    intraday ids = (viking-)pair:ts(ms):method."""
    p = (sid or '').split(':')
    if len(p) < 3:
        return None
    try:
        if swing:
            return p[0], p[1], float(p[2])
        return p[-1], p[0].replace('viking-', ''), float(p[1]) / 1000.0
    except (ValueError, IndexError):
        return None


def load_live():
    """{(strategy,pair): [ {trigger_ts, entry, stop, realized_r, placed_ts, reason} ]}"""
    out = defaultdict(list)
    for fn in ('swing-executions.json', 'executions.json'):
        swing = 'swing' in fn
        try:
            ex = json.load(open(os.path.join(_HERE, fn))).get('executions', [])
        except Exception:
            continue
        placed, closed = {}, {}
        for r in ex:
            pid = r.get('position_id')
            if r.get('event') == 'placed':
                placed[pid] = r
            elif r.get('event') == 'closed':
                closed[pid] = r
        for pid, c in closed.items():
            rr = c.get('realized_r')
            if rr is None or abs(rr) > SANE_R:
                continue
            meta = _parse_id(c.get('signal_id'), swing)
            if not meta:
                continue
            strat, pair, trig = meta
            p = placed.get(pid)
            out[(strat, pair)].append({
                'trigger_ts': trig, 'entry': c.get('entry_filled'), 'stop': c.get('stop'),
                'realized_r': rr, 'reason': c.get('reason'),
                'placed_ts': (p.get('ts', 0) / 1000.0 if p else None),
            })
    return out


def load_model(path='swing-shadow-log.json'):
    """{(strategy,pair): sorted[(entry_ts, sig)]} from the shadow log."""
    sigs = json.load(open(os.path.join(_HERE, path))).get('signals', {})
    by = defaultdict(list)
    for v in sigs.values():
        st = v.get('strategy'); pk = v.get('pair')
        if st and pk and 'entry_ts' in v:
            by[(st, pk)].append((v['entry_ts'], v))
    for k in by:
        by[k].sort()
    return by


def _tf_bar(sig):
    return {'m15': 900, 'h1': 3600, '4h': 14400, 'daily': 86400}.get(sig.get('tf'), 3600)


def nearest(arr, ts):
    if not arr:
        return None
    times = [t for t, _ in arr]
    j = bisect.bisect_left(times, ts)
    best = None
    for jj in (j - 1, j, j + 1):
        if 0 <= jj < len(arr):
            if best is None or abs(arr[jj][0] - ts) < abs(best[0] - ts):
                best = arr[jj]
    return best


def main():
    live = load_live()
    model = load_model()
    print("=" * 92)
    print("PER-STRATEGY EXECUTION RECONCILIATION — live fills vs model signals")
    print("=" * 92)
    print(f"{'strategy':<17}{'fills':>6}{'match%':>7}{'drift%':>7}{'orph%':>6} | "
          f"{'liveR(m)':>9}{'modelR(m)':>10}{'drag':>8} | {'entdrift':>9}{'lat(med)':>9}")

    roster = sorted(s for s in LIVE_ROSTER if any(k[0] == s for k in live))
    for st in roster:
        fills = [(pk, f) for (s, pk), fl in live.items() if s == st for f in fl]
        if not fills:
            continue
        buckets = {'M': [], 'D': [], 'O': []}
        matched_live, matched_model, drifts, lats = [], [], [], []
        for pk, f in fills:
            arr = model.get((st, pk), [])
            m = nearest(arr, f['trigger_ts'])
            if m is None:
                buckets['O'].append(f['realized_r']); continue
            mt, ms = m
            bar = _tf_bar(ms)
            if abs(mt - f['trigger_ts']) <= bar:
                buckets['M'].append(f['realized_r'])
                if ms.get('status') == 'resolved' and 'r' in ms:
                    matched_live.append(f['realized_r']); matched_model.append(ms['r'])
                    if f['entry'] and ms.get('entry') and ms.get('stop'):
                        R = abs(ms['entry'] - ms['stop']) or 1e-9
                        drifts.append(abs(f['entry'] - ms['entry']) / R)
                    if f['placed_ts']:
                        d = (f['placed_ts'] - f['trigger_ts']) / 60.0
                        if 0 <= d <= 48 * 60:
                            lats.append(d)
            elif dt.datetime.utcfromtimestamp(mt).date() == dt.datetime.utcfromtimestamp(f['trigger_ts']).date():
                buckets['D'].append(f['realized_r'])
            else:
                buckets['O'].append(f['realized_r'])
        n = sum(len(v) for v in buckets.values())
        sh = lambda k: 100.0 * len(buckets[k]) / n if n else 0
        lr = (sum(matched_live) / len(matched_live)) if matched_live else None
        mr = (sum(matched_model) / len(matched_model)) if matched_model else None
        drag = (lr - mr) if (lr is not None and mr is not None) else None
        ed = (sum(drifts) / len(drifts)) if drifts else None
        lat = (sorted(lats)[len(lats) // 2]) if lats else None
        def f3(x, suf='R'):
            return (f"{x:+.3f}{suf}" if x is not None else "   —  ")
        print(f"{st:<17}{n:>6}{sh('M'):>6.0f}%{sh('D'):>6.0f}%{sh('O'):>5.0f}% | "
              f"{f3(lr):>9}{f3(mr):>10}{f3(drag):>8} | "
              f"{(f'{ed:.2f}R' if ed is not None else '  — '):>9}{(f'{lat:.0f}m' if lat is not None else '  —'):>9}")

    print("\nReading it:")
    print("  high ORPHAN/DRIFT % = SELECTION gap (cBot trading different bars than the model).")
    print("  MATCHED but drag < 0 (live R below model R) = EXECUTION gap on the right trades")
    print("    — look at entry-drift (slippage vs the model entry) and latency (late fills).")
    print("  liveR(m)/modelR(m) = mean R on MATCHED resolved trades only (apples-to-apples).")


if __name__ == '__main__':
    main()
