"""Calibrate the backtest simulator against REAL cTrader fills.

We have 227 real intraday demo trades (executions.json) with real entry fills,
stops, targets, and REAL outcomes (stop-hit / target-hit / manual). We replay
each one through the SAME OANDA-M15-bar walk() the swing backtest uses — real
entry fill + real stop + real target — and ask: does the simulator's verdict
(which level got hit first) match what actually happened on the live account?

The gap between simulated expectancy and REAL expectancy on the same trades is
an empirical measurement of how much our backtests overstate reality. That is
the haircut to mentally apply to the swing backtest's +0.26R etc.

Also measures the real frictions we CAN'T see in a backtest: entry slippage,
commissions, swap — straight from the execution records.
"""
import json, os, bisect, collections
from backtest_rsi_per_class import _bars_norm

_HERE = os.path.dirname(os.path.abspath(__file__))
EXEC = os.path.join(_HERE, 'executions.json')
HIST = os.path.join(_HERE, 'historical-ohlc.json')
MAX_BARS = 500          # ~5 trading days of M15 — generous hold for resolution


def sim_exit(bars, entry_ts, entry, stop, target, d):
    """Replay on M15 bars from entry. Return ('win'|'loss'|'both'|'none')."""
    ts = [b['_ts'] for b in bars]
    i0 = bisect.bisect_left(ts, entry_ts)
    for j in range(i0, min(i0 + MAX_BARS, len(bars))):
        b = bars[j]
        if d == 'bull':
            hit_stop = b['l'] <= stop
            hit_tgt = b['h'] >= target
        else:
            hit_stop = b['h'] >= stop
            hit_tgt = b['l'] <= target
        if hit_stop and hit_tgt:
            return 'both'          # intrabar ambiguity — order unknown
        if hit_tgt:
            return 'win'
        if hit_stop:
            return 'loss'
    return 'none'


def main():
    ex = json.load(open(EXEC)); rows = ex.get('executions', ex)
    hist = json.load(open(HIST))['pairs']
    m15 = {}
    for pk, v in hist.items():
        b = _bars_norm(v.get('m15', []))
        if b:
            m15[pk] = b

    byid = collections.defaultdict(dict)
    for r in rows:
        pid = r.get('position_id'); ev = r.get('event')
        if pid and ev in ('placed', 'closed'):
            byid[pid][ev] = r
    paired = [v for v in byid.values() if 'placed' in v and 'closed' in v]

    mech = []          # mechanical exits (stop/target) — the calibration set
    manual = 0; no_data = 0
    slip = []; swaps = []; comms = []
    conf = collections.Counter()   # (sim, real) verdict pairs
    sim_R = []; real_R = []

    for v in paired:
        p, c = v['placed'], v['closed']
        pk = p.get('pair'); d = p.get('dir')
        entry = c.get('entry_filled') or p.get('entry_filled')
        stop = c.get('stop') or p.get('stop'); target = c.get('target') or p.get('target')
        ts = p.get('ts', 0) / 1000.0
        rr_real = abs(target - entry) / abs(entry - stop) if entry not in (None,) and stop != entry else None
        # frictions (available on every trade)
        if p.get('entry_attempt') and entry:
            slip.append(abs(entry - p['entry_attempt']))
        if c.get('swap') is not None: swaps.append(c['swap'])
        if c.get('commissions') is not None: comms.append(c['commissions'])

        reason = c.get('reason'); realized = c.get('realized_r', 0.0)
        real_verdict = 'win' if realized > 0 else 'loss'

        if reason == 'manual-or-broker':
            manual += 1
            continue
        if pk not in m15:
            no_data += 1
            continue
        sv = sim_exit(m15[pk], ts, entry, stop, target, d)
        if sv == 'none':
            no_data += 1
            continue
        # resolve 'both' pessimistically (assume stop first) for the headline,
        # but track it so we can bound it
        sim_v = 'loss' if sv == 'both' else sv
        conf[(sim_v, real_verdict)] += 1
        mech.append((sim_v, real_verdict, sv, rr_real, realized))
        sim_R.append(rr_real if sim_v == 'win' else -1.0)
        real_R.append(realized)

    n = len(mech)
    agree = sum(1 for s, r, _, _, _ in mech if s == r)
    both_n = sum(1 for _, _, sv, _, _ in mech if sv == 'both')
    print("=== SIMULATOR vs REALITY — calibration on real cTrader intraday trades ===\n")
    print(f"paired real trades: {len(paired)}   mechanical (stop/target): {n}   manual-closed: {manual}   unresolved/no-data: {no_data}\n")

    print(f"Agreement (sim verdict == real verdict): {agree}/{n} = {100*agree/n:.1f}%")
    print(f"  (of which {both_n} bars touched stop AND target intrabar — resolved pessimistically as loss)\n")

    print("Confusion  [sim \\ real]:")
    print(f"            real WIN   real LOSS")
    print(f"  sim WIN     {conf[('win','win')]:>5}     {conf[('win','loss')]:>5}")
    print(f"  sim LOSS    {conf[('loss','win')]:>5}     {conf[('loss','loss')]:>5}\n")

    sim_wr = 100*sum(1 for s,_,_,_,_ in mech if s=='win')/n
    real_wr = 100*sum(1 for _,r,_,_,_ in mech if r=='win')/n
    sim_exp = sum(sim_R)/n; real_exp = sum(real_R)/n
    print(f"On the SAME {n} trades:")
    print(f"  SIMULATED : WR={sim_wr:.1f}%   expectancy={sim_exp:+.3f}R")
    print(f"  REAL      : WR={real_wr:.1f}%   expectancy={real_exp:+.3f}R")
    print(f"  --> simulator OVERSTATES expectancy by {sim_exp-real_exp:+.3f}R per trade\n")

    print("Real frictions (invisible to a backtest):")
    if slip:
        print(f"  entry slippage |attempt-fill|: mean={sum(slip)/len(slip):.6f} price units (n={len(slip)})")
    if comms: print(f"  commissions: mean={sum(comms)/len(comms):+.4f} acct-ccy/trade")
    if swaps: print(f"  swap:        mean={sum(swaps)/len(swaps):+.4f} acct-ccy/trade  (scales with hold — matters MORE for swing)")


if __name__ == '__main__':
    main()
