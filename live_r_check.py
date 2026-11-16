#!/usr/bin/env python3
"""Live-vs-model R check for the widened equity strategies (2026-11-16 changes).

Reads equity-executions.json (closed broker fills from the MT5 bridge) and, for each
live-config strategy, reports realised R on trades CLOSED SINCE a cutoff — comparing it
to the gap-aware MODEL target the widening/overnight decisions were made on.

Why a cutoff: the stretch/near-MA gates + full-universe widen + overnight-hold only took
effect when the bridge/EA were redeployed. Trades closed before that aren't on the new
config, so they'd contaminate the comparison. Default cutoff = the deploy date; override
with --since for the real restart date.

realized_r in the feed is GROSS price R ((exit-entry)/|entry-init_sl|), directly comparable
to the model (which is also price-only). We also show NET R (profit_ccy/risk_ccy) so the
commission drag is visible — the model ignores commission, live doesn't.

Usage: python live_r_check.py [--since YYYY-MM-DD] [--strats ma_pull_eq,turtle_soup_eq]
"""
import argparse, json, datetime as dt

# Gap-aware MODEL targets for the CURRENT live config (full universe + entry gate + hold).
# These are optimistic by construction (clean-ish model, best-of-N history, no live slippage
# beyond the 4% backstop). A persistent live gap worse than ~0.10R is the signal to pull back.
MODEL = {
    'ma_pull_eq':     {'target': 0.112, 'note': 'full-universe, stretch>=0.5, held overnight (model n=80973, OOS [+0.115/+0.109])'},
    'turtle_soup_eq': {'target': 0.391, 'note': 'full-universe, near-MA & mid-RSI, held h1 (model n=3730, OOS [+0.392/+0.389])'},
}
PULLBACK_GAP = 0.10   # live below (target - this) over a decent sample => reconsider

def half_split(rows):
    """both-OOS style: avgR of the chronological first vs second half."""
    sv = sorted(rows, key=lambda r: r['closed_ms']); h = len(sv) // 2
    def avg(x): return sum(r['realized_r'] for r in x) / len(x) if x else 0.0
    return avg(sv[:h]), avg(sv[h:])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', default='2026-11-16', help='only trades closed on/after this UTC date')
    ap.add_argument('--strats', default='ma_pull_eq,turtle_soup_eq')
    ap.add_argument('--file', default='equity-executions.json')
    a = ap.parse_args()
    cutoff_ms = int(dt.datetime.strptime(a.since, '%Y-%m-%d').replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
    strats = [s.strip() for s in a.strats.split(',') if s.strip()]
    ex = json.load(open(a.file)).get('executions', [])

    print('LIVE-vs-MODEL R CHECK  —  closed since %s UTC  (%d total fills in feed)' % (a.since, len(ex)))
    print('=' * 78)
    for strat in strats:
        rows = [r for r in ex if r.get('strategy') == strat and (r.get('closed_ms') or 0) >= cutoff_ms
                and r.get('realized_r') is not None]
        tgt = MODEL.get(strat, {}); target = tgt.get('target')
        print('\n%s   (model %s)' % (strat, tgt.get('note', '?')))
        if not rows:
            print('  no closed trades yet since cutoff — nothing to judge.')
            continue
        n = len(rows)
        gross = sum(r['realized_r'] for r in rows) / n
        wr = 100.0 * sum(1 for r in rows if r['realized_r'] > 0) / n
        sumr = sum(r['realized_r'] for r in rows)
        # net R uses ccy: risk per trade = |entry-init_sl| * volume
        nets = []
        for r in rows:
            risk = abs((r.get('entry') or 0) - (r.get('init_sl') or 0)) * (r.get('volume') or 0)
            if risk > 0 and r.get('profit_ccy') is not None:
                nets.append(r['profit_ccy'] / risk)
        net = sum(nets) / len(nets) if nets else float('nan')
        comm = sum(r.get('commission') or 0 for r in rows)
        pnl = sum(r.get('profit_ccy') or 0 for r in rows)
        h1, h2 = half_split(rows)
        print('  n={:<4d}  grossR={:+.3f}  netR={:+.3f}  WR={:4.1f}%  sumR={:+.1f}  P&L={:+,.0f}  comm={:+,.0f}'
              .format(n, gross, net, wr, sumr, pnl, comm))
        print('  halves (gross): [%+.3f / %+.3f]%s' % (h1, h2, '  OOS+' if h1 > 0 and h2 > 0 else ''))
        if target is not None:
            gap = gross - target
            verdict = ('ON TRACK' if gap >= -0.03 else
                       'SOFT (watch)' if gap >= -PULLBACK_GAP else
                       'BELOW MODEL — reconsider')
            flag = '' if n >= 25 else '   [n<25: too few to judge yet]'
            print('  vs model %+.3f  ->  gap %+.3f   %s%s' % (target, gap, verdict, flag))
    print('\n' + '=' * 78)
    print('Model R is GROSS and optimistic by construction. Judge on grossR vs target once')
    print('n>=25; a persistent gap worse than %.2fR is the pull-back signal. netR shows the' % PULLBACK_GAP)
    print('commission reality the model omits.')

if __name__ == '__main__':
    main()
