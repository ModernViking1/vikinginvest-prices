#!/usr/bin/env python3
"""Post-veto minor-class watch.

Tallies live macd-primary minor trades placed AFTER the EW-veto deploy
(2026-07-01) and their closed outcomes, so we can judge whether the veto
lifted live minor WR. Run daily during the watch week.

    python3 watch_postveto_minors.py

Reads executions.json (leave it alone — read-only) and the PAIR_CLASS map
in detect_triggers.py.
"""
import ast, json, datetime

# EW-veto went live ~2026-07-01T12:00Z (commit 38d3b96c). Trades PLACED at
# or after this cutoff are the ones the veto could have gated.
CUTOFF_ISO = "2026-07-01T12:00:00Z"
DECISION_N = 12   # closed minor trades before the sample is worth judging

def cutoff_ms():
    dt = datetime.datetime(2026, 7, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)

def load_minor_pairs():
    tree = ast.parse(open('detect_triggers.py').read())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                getattr(t, 'id', None) == 'PAIR_CLASS' for t in node.targets):
            pc = ast.literal_eval(node.value)
            return {p for p, c in pc.items() if c == 'minor'}
    return set()

def main():
    minors = load_minor_pairs()
    cut = cutoff_ms()
    ex = json.load(open('executions.json')).get('executions', [])

    # Group by signal_id; a trade is post-veto if its PLACED ts >= cutoff.
    placed, closed = {}, {}
    for r in ex:
        sid = r.get('signal_id')
        if not sid:
            continue
        if r.get('event') == 'placed':
            placed[sid] = r
        elif r.get('event') == 'closed':
            closed[sid] = r

    rows = []
    for sid, p in placed.items():
        pair = (p.get('pair') or '').lower()
        if pair not in minors:
            continue
        if (p.get('ts') or 0) < cut:
            continue
        c = closed.get(sid)
        rows.append({
            'pair': pair.upper(), 'dir': p.get('dir'),
            'placed_ts': p.get('ts'),
            'closed': c is not None,
            'rr': (c.get('realized_r') if c else None),
            'reason': (c.get('reason') if c else None),
        })

    rows.sort(key=lambda r: r['placed_ts'] or 0)
    decided = [r for r in rows if r['closed'] and r['rr'] is not None]
    wins = [r for r in decided if r['rr'] > 0]
    losses = [r for r in decided if r['rr'] <= 0]
    net_r = sum(r['rr'] for r in decided)
    wr = (100.0 * len(wins) / len(decided)) if decided else None

    print(f"POST-VETO MINOR WATCH — cutoff {CUTOFF_ISO}")
    print(f"  placed (minors, post-veto): {len(rows)}")
    print(f"  closed/decided:             {len(decided)}")
    if wr is not None:
        print(f"  WR: {wr:.1f}%  ({len(wins)}W / {len(losses)}L)  netR {net_r:+.2f}")
    else:
        print("  WR: — (no closed minor trades yet)")
    print(f"  decision-ready at n>={DECISION_N}: "
          f"{'YES' if len(decided) >= DECISION_N else 'not yet'}")
    if rows:
        print("  trades:")
        for r in rows:
            t = datetime.datetime.utcfromtimestamp((r['placed_ts'] or 0)/1000).strftime('%m-%d %H:%MZ')
            out = (f"{r['rr']:+.2f}R {r['reason']}" if r['closed'] else "OPEN")
            print(f"    {t}  {r['pair']:7} {r['dir'] or '?':4}  {out}")

if __name__ == '__main__':
    main()
