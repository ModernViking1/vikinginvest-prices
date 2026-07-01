#!/usr/bin/env python3
"""Execution-cost report v2 — did this week's fixes close the live↔sim gap?

Reads executions.json (broker fills). For every CLOSED trade computes the
entry-drift (how far the fill landed from the modelled signal entry, as a
fraction of R) alongside the realized R, then splits PRE vs POST the fix
cutoff so we can see whether the pip-floor + entry-deviation gate + EW-veto
actually tightened live execution. Read-only. Run each Friday:

    python3 build_exec_cost_report.py
"""
import ast, json, datetime, statistics

# Fixes landed 30 Jun → 01 Jul (pip-floor, entry-deviation gate, EW-veto).
# Trades on/after this cutoff are "post-fix". Adjust if the cBot rebuild
# that activates the deviation gate happened later.
CUTOFF_ISO = "2026-06-30T00:00:00Z"
DEV_GATE = 0.30   # the cBot's MaxEntryDeviationPctOfR — post-fix fills should sit under this

def cutoff_ms():
    return int(datetime.datetime(2026,6,30,0,0,0,tzinfo=datetime.timezone.utc).timestamp()*1000)

def pair_class_map():
    tree = ast.parse(open('detect_triggers.py').read())
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t,'id',None)=='PAIR_CLASS' for t in n.targets):
            return ast.literal_eval(n.value)
    return {}

def drift_of_R(r):
    # Entry drift must be read from the PLACED event — CLOSED events carry
    # entry_attempt=0 (not backfilled), which would compute a bogus ~100%R.
    # `r` here is expected to already be the matched placed event.
    ea, ef, st = r.get('entry_attempt'), r.get('entry_filled'), r.get('stop')
    if None in (ea, ef, st) or ea == 0: return None
    rdist = abs(ea - st)
    if rdist <= 0: return None
    return abs(ef - ea) / rdist

def summarize(rows, label):
    if not rows:
        print(f"  {label:9} — no trades"); return
    wins = [r for r in rows if (r.get('realized_r') or 0) > 0]
    drifts = [r['_drift'] for r in rows if r.get('_drift') is not None]
    netR = sum(r.get('realized_r') or 0 for r in rows)
    wr = 100*len(wins)/len(rows)
    md = statistics.median(drifts) if drifts else None
    mn = statistics.mean(drifts) if drifts else None
    over = sum(1 for d in drifts if d > DEV_GATE)
    print(f"  {label:9} n={len(rows):>3}  WR={wr:5.1f}%  netR={netR:+7.1f}  "
          f"drift md={md*100:4.0f}%R mean={mn*100:4.0f}%R  >{int(DEV_GATE*100)}%R:{over}"
          if drifts else
          f"  {label:9} n={len(rows):>3}  WR={wr:5.1f}%  netR={netR:+7.1f}  drift n/a")

def main():
    PC = pair_class_map()
    ex = json.load(open('executions.json')).get('executions', [])
    closed = [r for r in ex if r.get('event') == 'closed']
    placed = {r['signal_id']: r for r in ex
              if r.get('event') == 'placed' and r.get('signal_id')}
    cut = cutoff_ms()
    for r in closed:
        r['_cls'] = PC.get((r.get('pair') or '').lower(), '?')
        p = placed.get(r.get('signal_id'))
        # Drift + period keyed off the PLACED event (real entry fields +
        # entry time — the deviation gate acts at entry, not exit).
        r['_drift'] = drift_of_R(p) if p else None
        entry_ts = (p.get('ts') if p else None) or r.get('ts') or 0
        r['_period'] = 'post' if entry_ts >= cut else 'pre'

    print(f"EXECUTION-COST REPORT v2 — closed trades: {len(closed)}  (cutoff {CUTOFF_ISO})\n")

    for per in ('pre', 'post'):
        sub = [r for r in closed if r['_period'] == per]
        print(f"── {per.upper()}-FIX ──")
        summarize(sub, 'ALL')
        for cls in ('major','minor','comm','index','crypto'):
            summarize([r for r in sub if r['_cls'] == cls], cls)
        print()

    # Gate-effectiveness: post-fix fills exceeding the deviation cap should
    # be ~0 (the cBot skips them before placing).
    post = [r for r in closed if r['_period'] == 'post']
    post_over = [(r.get('pair','?').upper(), r['_drift']) for r in post
                 if (r.get('_drift') or 0) > DEV_GATE]
    print(f"Deviation-gate check — post-fix fills over {int(DEV_GATE*100)}%R "
          f"(should be ~0 if the gate is live): {len(post_over)}")
    for p, d in sorted(post_over, key=lambda x:-x[1])[:8]:
        print(f"    {p:8} drift {d*100:.0f}%R")

    # Worst live-vs-sim offenders overall (highest median drift by pair).
    bypair = {}
    for r in closed:
        d = r.get('_drift')
        if d is not None:
            bypair.setdefault((r.get('pair','?').upper(), r['_cls']), []).append(d)
    ranked = sorted(bypair.items(), key=lambda kv: -statistics.median(kv[1]))
    print("\nHighest entry-drift pairs (median %R, all-time):")
    for (p, cls), ds in ranked[:10]:
        print(f"    {p:8} {cls:6} median {statistics.median(ds)*100:4.0f}%R  n={len(ds)}")

if __name__ == '__main__':
    main()
