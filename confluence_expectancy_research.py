"""Prime vs Action, quantified — does confluence predict expectancy? (book Ch4/Ch6).

The book's central operational claim: 'Prime' trades (all criteria aligned) carry positive
expectancy; 'Action' trades (criteria only partially met) should be resisted. We can test a
mechanical version on our OWN data. Every logged shadow signal carries two independent
confluence flags computed causally at entry:
  bos_aligned    — the break-of-structure at entry agrees with the trade direction
  nowick_aligned — the pre-entry bar is an in-trend no-wick momentum candle

Confluence score = bos_aligned + nowick_aligned (0, 1, or 2). If the Prime/Action thesis
holds, expectancy should rise monotonically with the score. Reads swing-shadow-log.json.

Run: python confluence_expectancy_research.py
"""
import json, os
from collections import defaultdict

LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'swing-shadow-log.json')


def stats(rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n = len(seq)
    if not n:
        return 0, 0, 0, 0, 0
    e = sum(seq) / n; w = 100 * sum(1 for x in seq if x > 0) / n
    mid = n // 2
    eh = sum(x for _, x in rows[:mid]) / max(1, mid); es = sum(x for _, x in rows[mid:]) / max(1, n - mid)
    return n, w, e, eh, es


def show(title, buckets):
    print(f"\n{title}")
    print(f"    {'confluence':<16} {'n':>5} {'WR%':>6} {'expR':>8}   {'OOS h1/h2':>16}")
    for k in sorted(buckets):
        n, w, e, eh, es = stats(buckets[k])
        if n:
            print(f"    {k:<16} {n:>5} {w:>5.1f}% {e:>+7.3f}   [{eh:>+6.3f}/{es:>+6.3f}]")


def main():
    log = json.load(open(LOG))
    sigs = list(log['signals'].values()) if isinstance(log.get('signals'), dict) else log.get('signals', [])
    res = [s for s in sigs if s.get('status') == 'resolved' and 'r' in s
           and 'bos_aligned' in s and 'nowick_aligned' in s]
    print("=" * 84)
    print("Confluence -> expectancy (Prime vs Action) — our own shadow log")
    print("=" * 84)
    print(f"resolved signals with both confluence flags: {len(res)}")

    overall = defaultdict(list)
    for s in res:
        c = int(bool(s['bos_aligned'])) + int(bool(s['nowick_aligned']))
        overall[f"{c}/2"].append((s['entry_ts'], s['r']))
    show("ALL strategies pooled:", overall)

    # each flag on its own
    byflag = {'bos_aligned=1': [], 'bos_aligned=0': [], 'nowick=1': [], 'nowick=0': []}
    for s in res:
        byflag['bos_aligned=1' if s['bos_aligned'] else 'bos_aligned=0'].append((s['entry_ts'], s['r']))
        byflag['nowick=1' if s['nowick_aligned'] else 'nowick=0'].append((s['entry_ts'], s['r']))
    print("\nEach confluence layer in isolation:")
    print(f"    {'layer':<16} {'n':>5} {'WR%':>6} {'expR':>8}   {'OOS h1/h2':>16}")
    for k in ['bos_aligned=1', 'bos_aligned=0', 'nowick=1', 'nowick=0']:
        n, w, e, eh, es = stats(byflag[k])
        print(f"    {k:<16} {n:>5} {w:>5.1f}% {e:>+7.3f}   [{eh:>+6.3f}/{es:>+6.3f}]")

    # per big strategy: 2/2 (Prime) vs 0/2 (Action)
    bystrat = defaultdict(lambda: defaultdict(list))
    for s in res:
        c = int(bool(s['bos_aligned'])) + int(bool(s['nowick_aligned']))
        bystrat[s['strategy']][c].append((s['entry_ts'], s['r']))
    print("\nPrime (2/2) vs Action (0/2), per strategy with n>=25 in both:")
    print(f"    {'strategy':<12} {'Prime n':>7} {'Prime exp':>10} {'Action n':>8} {'Action exp':>11}  edge")
    for st in sorted(bystrat):
        pn, _, pe, _, _ = stats(bystrat[st][2]); an, _, ae, _, _ = stats(bystrat[st][0])
        if pn >= 25 and an >= 25:
            print(f"    {st:<12} {pn:>7} {pe:>+9.3f}R {an:>8} {ae:>+10.3f}R  {'+' if pe > ae else '-'}{abs(pe-ae):.3f}")


if __name__ == '__main__':
    main()
