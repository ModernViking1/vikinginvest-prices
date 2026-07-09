"""Quantify the EXECUTION GAP: compare the demo swing cBot's real fills against
the shadow harness's MODELLED outcomes, matched by signal id.

  swing-shadow-log.json  -> modelled outcome per signal (r at 1:2, clean fills)
  swing-executions.json  -> demo cBot's REAL closed trades (realized_r net of
                            spread/slippage/swap), keyed by signal_id

The gap between the two is the execution cost — the exact thing that turned the
intraday backtest 82% into live 41%. This is how we measure it for the swing
edges on a demo account BEFORE any real capital.

Both keys share the feed's id format `strategy:pair:entry_ts`, so they match
directly. Run any time; it reports what has resolved in BOTH so far.
"""
import json, os
from collections import defaultdict

SHADOW = 'swing-shadow-log.json'
EXEC = 'swing-executions.json'


def load_shadow():
    if not os.path.exists(SHADOW):
        return {}
    d = json.load(open(SHADOW))
    out = {}
    for k, s in d.get('signals', {}).items():
        if s.get('status') == 'resolved' and 'r' in s:
            out[k] = s
    return out


def load_exec():
    if not os.path.exists(EXEC):
        return {}
    d = json.load(open(EXEC))
    closed = {}
    for e in d.get('executions', []):
        if e.get('event') != 'closed':
            continue
        sid = e.get('signal_id')
        if not sid:
            continue
        # keep the latest close per signal_id
        if sid not in closed or (e.get('ts', 0) > closed[sid].get('ts', 0)):
            closed[sid] = e
    return closed


def main():
    shadow = load_shadow()
    demo = load_exec()
    print(f"shadow resolved signals: {len(shadow)}   demo closed trades: {len(demo)}")

    matched = [(k, shadow[k], demo[k]) for k in shadow if k in demo]
    if not matched:
        print("\nNo signals resolved in BOTH yet.")
        print(" - shadow log fills automatically (daily CI).")
        print(" - demo trades appear once the swing cBot is deployed and positions close,")
        print("   and their rows reach swing-executions.json (auto-publish or manual import).")
        print("Re-run this once demo trades have closed to see the execution gap.")
        return

    n = len(matched)
    shadow_r = [s['r'] for _, s, _ in matched]
    demo_r = [float(e.get('realized_r') or 0) for _, _, e in matched]
    shadow_wr = 100 * sum(1 for x in shadow_r if x > 0) / n
    demo_wr = 100 * sum(1 for x in demo_r if x > 0) / n
    shadow_exp = sum(shadow_r) / n
    demo_exp = sum(demo_r) / n

    # agreement
    agree = sum(1 for i in range(n) if (shadow_r[i] > 0) == (demo_r[i] > 0))
    quad = defaultdict(int)
    for i in range(n):
        sw, dw = shadow_r[i] > 0, demo_r[i] > 0
        quad['ww' if sw and dw else 'll' if not sw and not dw else 'wl' if sw else 'lw'] += 1

    print(f"\nMATCHED (resolved in both): {n}")
    print(f"  {'':<10} {'WR':>7} {'meanR':>8}")
    print(f"  {'shadow':<10} {shadow_wr:>6.1f}% {shadow_exp:>+8.3f}")
    print(f"  {'demo':<10} {demo_wr:>6.1f}% {demo_exp:>+8.3f}")
    print(f"  EXECUTION GAP (shadow - demo): {shadow_exp - demo_exp:+.3f}R per trade")
    print(f"\n  trade-for-trade agreement: {100*agree/n:.0f}%")
    print(f"    shadow win  & demo win : {quad['ww']}")
    print(f"    shadow loss & demo loss: {quad['ll']}")
    print(f"    shadow WIN  & demo LOSS: {quad['wl']}   (execution turned a modelled win into a loss)")
    print(f"    shadow loss & demo WIN : {quad['lw']}")

    # cost drag on demo winners: how much below the clean +2R do target-hits land?
    wins = [float(e.get('realized_r') or 0) for _, _, e in matched if (e.get('reason') == 'target-hit')]
    losses = [float(e.get('realized_r') or 0) for _, _, e in matched if (e.get('reason') == 'stop-hit')]
    if wins:
        print(f"\n  demo target-hits: n={len(wins)} mean realized {sum(wins)/len(wins):+.3f}R (clean would be +2.000R) "
              f"-> entry+exit slippage ~{2.0 - sum(wins)/len(wins):.3f}R")
    if losses:
        print(f"  demo stop-hits:   n={len(losses)} mean realized {sum(losses)/len(losses):+.3f}R (clean would be -1.000R) "
              f"-> stop slippage ~{-1.0 - sum(losses)/len(losses):.3f}R")

    # per-strategy
    print("\n  per-strategy execution gap:")
    by = defaultdict(lambda: [[], []])
    for k, s, e in matched:
        strat = k.split(':')[0]
        by[strat][0].append(s['r']); by[strat][1].append(float(e.get('realized_r') or 0))
    for strat, (sr, dr) in sorted(by.items()):
        print(f"    {strat:<10} n={len(sr):>3}  shadow {sum(sr)/len(sr):+.3f}R  demo {sum(dr)/len(dr):+.3f}R  gap {sum(sr)/len(sr)-sum(dr)/len(dr):+.3f}R")


if __name__ == '__main__':
    main()
