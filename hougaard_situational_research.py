"""Tom Hougaard 'situational analysis' — the Friday->Monday revisit setup.

Rule (from the screenshots): Thursday makes a high; Friday trades BELOW Thursday's
high and fails to break it; the following Monday is then expected to revisit (trade
down to) Friday's low. Claimed edge ~21/24 or "~95%" on the Dow. Framed as a
directional bias, target = Friday's low.

This is a PROBABILITY / situational claim, so the test has two halves:
  1. LIFT — P(Monday revisits Friday's low | setup) vs the base rate P(revisit | NO
     setup) and unconditional. A high hit-rate is only an edge if the condition adds
     lift; Monday often dips below Friday's low anyway.
  2. TRADEABLE — short at Monday's open, target = Friday's low, stop = Thursday's high
     (the invalidation), resolved intraday on h1 over the next 2 sessions. Reports the
     R expectancy so a near target's high hit-rate (low-RR mirage) is exposed.

Sessions are identified by the weekend GAP (last bar before the gap = 'Friday',
bar after = 'Monday', bar before = 'Thursday') — timezone-robust and it naturally
absorbs holiday weeks (the first session back becomes the 'Monday'). Crypto trades
weekends, so it has no gap and is reported as N/A.

Run: python hougaard_situational_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
DAY = 86400
WEEKEND_MIN = 1.8 * DAY      # gap to the next bar that marks a weekend
NORMAL_MAX = 1.8 * DAY       # gap between two consecutive weekday sessions
HOLD_H1 = 48                 # h1 bars (~2 sessions) to resolve the Monday trade
BUF = 0.0                    # stop buffer beyond Thursday's high (fraction of the entry->stop dist added); 0 = exact


def triples(daily):
    """Yield (thu, fri, mon) daily bars around each weekend gap."""
    out = []
    for i in range(1, len(daily) - 1):
        gap_next = daily[i + 1]['_ts'] - daily[i]['_ts']
        gap_prev = daily[i]['_ts'] - daily[i - 1]['_ts']
        if gap_next >= WEEKEND_MIN and gap_prev <= NORMAL_MAX:
            out.append((daily[i - 1], daily[i], daily[i + 1]))
    return out


def resolve_trade(h1, entry_ts, entry, tp, sl):
    """Short: TP below (Friday low), SL above (Thursday high). Return R or None."""
    R_risk = sl - entry
    if R_risk <= 0 or entry <= tp:
        return None
    rr = (entry - tp) / R_risk
    ts = [b['_ts'] for b in h1]; i0 = bisect.bisect_left(ts, entry_ts)
    if i0 >= len(h1):
        return None
    for j in range(i0, min(i0 + HOLD_H1, len(h1))):
        b = h1[j]
        if b['h'] >= sl:
            return -1.0
        if b['l'] <= tp:
            return rr
    return None   # unresolved within the window — excluded


def run():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    # per pair: [n_setup, hit_setup, n_nosetup, hit_nosetup, trades(list of R)]
    by_pair = {}; by_class = defaultdict(lambda: [0, 0, 0, 0, []])
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        daily = _bars_norm(pairs[pk].get('daily', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(daily) < 60:
            continue
        tri = triples(daily)
        if len(tri) < 10:          # crypto / gapless -> N/A
            by_pair[pk] = None; continue
        ns = nh = nns = nnh = 0; trades = []
        for thu, fri, mon in tri:
            setup = fri['h'] < thu['h']          # Friday failed to exceed Thursday's high
            revisit = mon['l'] <= fri['l']       # Monday revisited Friday's low
            if setup:
                ns += 1; nh += int(revisit)
                # tradeable: short at Monday open, TP=Friday low, SL=Thursday high
                entry = mon['o']; tp = fri['l']; sl = thu['h']
                r = resolve_trade(h1, mon['_ts'], entry, tp, sl)
                if r is not None:
                    trades.append(r)
            else:
                nns += 1; nnh += int(revisit)
        by_pair[pk] = (cls, ns, nh, nns, nnh, trades)
        c = by_class[cls]; c[0] += ns; c[1] += nh; c[2] += nns; c[3] += nnh; c[4].extend(trades)
    return by_pair, by_class


def pct(h, n):
    return f"{100*h/n:5.1f}%" if n else "  n/a"


def exp_line(trades):
    if not trades:
        return "n=0"
    n = len(trades); wr = 100 * sum(1 for r in trades if r > 0) / n
    e = sum(trades) / n
    return f"n={n:>3} TPrate={wr:4.1f}% exp={e:+.3f}R"


def main():
    by_pair, by_class = run()
    print("=" * 96)
    print("TOM HOUGAARD situational setup — Friday fails Thursday's high -> Monday revisits Friday's low")
    print("=" * 96)
    print("\nLIFT TEST — does the setup beat the base rate of a Friday-low revisit?")
    print(f"  {'class':<8} {'setup n':>8} {'P(revisit|setup)':>17} {'P(revisit|NO setup)':>20} {'lift':>8}")
    order = ['index', 'major', 'minor', 'comm', 'crypto']
    for cls in order:
        c = by_class.get(cls)
        if not c or (c[0] + c[2]) == 0:
            print(f"  {cls:<8} {'—':>8}  (no weekend structure / no data)")
            continue
        ps = 100 * c[1] / c[0] if c[0] else 0
        pn = 100 * c[3] / c[2] if c[2] else 0
        print(f"  {cls:<8} {c[0]:>8} {pct(c[1],c[0]):>17} {pct(c[3],c[2]):>20} {ps-pn:>+7.1f}pp")
    # overall
    tot = [sum(by_class[k][i] for k in by_class) for i in range(4)]
    ps = 100*tot[1]/tot[0] if tot[0] else 0; pn = 100*tot[3]/tot[2] if tot[2] else 0
    print(f"  {'ALL':<8} {tot[0]:>8} {pct(tot[1],tot[0]):>17} {pct(tot[3],tot[2]):>20} {ps-pn:>+7.1f}pp")

    print("\nTRADEABLE — short Monday open, TP=Friday low, SL=Thursday high (h1-resolved, R):")
    for cls in order:
        c = by_class.get(cls)
        if not c or not c[4]:
            print(f"  {cls:<8} {exp_line([])}"); continue
        print(f"  {cls:<8} {exp_line(c[4])}")
    alltr = [r for k in by_class for r in by_class[k][4]]
    print(f"  {'ALL':<8} {exp_line(alltr)}")

    print("\nPER-PAIR (index + selected) — setup revisit rate vs base, and trade expectancy:")
    print(f"  {'pair':<9}{'cls':<7}{'setup':>6}{'revisit':>9}{'base':>8}{'lift':>8}   trade")
    for pk, v in by_pair.items():
        if v is None:
            continue
        cls, ns, nh, nns, nnh, trades = v
        if cls not in ('index', 'comm') and pk not in ('eurusd', 'gbpusd', 'usdjpy'):
            continue
        ps = 100*nh/ns if ns else 0; pn = 100*nnh/nns if nns else 0
        print(f"  {pk:<9}{cls:<7}{ns:>6}{pct(nh,ns):>9}{pct(nnh,nns):>8}{ps-pn:>+7.1f}pp   {exp_line(trades)}")

    print("\nNotes: 'revisit' = Monday's low <= Friday's low. 'base' = same probability when")
    print("Friday DID exceed Thursday's high (no setup). lift = setup - base, in percentage points.")
    print("Crypto trades weekends (no Fri->Mon gap) so the setup is undefined there.")


if __name__ == '__main__':
    main()
