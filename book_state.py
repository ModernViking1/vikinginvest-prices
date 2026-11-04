#!/usr/bin/env python3
"""Reconcile the LIVE swing/intraday (cTrader/OANDA) book state — correctly.

WHY THIS EXISTS (2026-11-04): a naive "open = every filled order in the exec log without a matching
close" over-counts badly. The execution ledgers (swing-executions.json / executions.json) are a ROLLING
log that drops close events over time, and pending limit orders that expire leave a dangling 'placed'
forever — so set-differencing placed-vs-closed reported 8 "open" when the broker had 1 open + 4 pending.

CORRECT SOURCES:
  • PENDING orders  -> the live SIGNAL FEED (swing-signals.json / signals.json), state=='triggered',
                       filtered to the live roster. Authoritative (matches the cTrader "Orders" tab).
  • OPEN positions  -> ledger, but by LATEST EVENT per (strategy,symbol,dir): open iff the most recent
                       event is a 'placed' (a fill) AND within RECENCY_DAYS. Data-justified: across 169
                       live closed trades the MAX hold was 5.8 days (p95 0.7d), so a 'placed' with no
                       later close older than ~7d is a lossy dropped-close phantom, not a live position.
  • REALIZED P&L    -> 'closed' events only (each carries its own realized_r/net_profit — reliable).

BEST-EFFORT caveat: open-position detection is still inferred from a lossy log. The durable fix is a
live positions export from the cBot (an equity-open.json equivalent for this book); until then, treat
the OPEN list as "verify against the broker app." Realized P&L and pending orders are reliable.

Usage: python book_state.py [week_start=YYYY-MM-DD] [recency_days=7]
"""
import json, sys, datetime as dt
from collections import defaultdict

LIVE = {'twob_ix', 'engulf_manip',                 # swing emitters
        'mmove_m15', 'absorb_btc', 'crt_ix'}       # intraday emitters
BOOK = {'twob_ix': 'swing', 'engulf_manip': 'swing',
        'mmove_m15': 'intraday', 'absorb_btc': 'intraday', 'crt_ix': 'intraday'}
EXEC_FILES = ['swing-executions.json', 'executions.json']
SIG_FILES = ['swing-signals.json', 'signals.json']

def _ts(ms): return dt.datetime.utcfromtimestamp((ms or 0) / 1000)

def load_execs():
    rows = []
    for f in EXEC_FILES:
        try:
            for r in json.load(open(f))['executions']:
                if r.get('strategy') in LIVE:
                    rows.append(r)
        except (FileNotFoundError, KeyError):
            pass
    return rows

def open_positions(rows, recency_days=7, now_ms=None):
    """OPEN iff latest event per (strategy,symbol,dir) is a fill within the recency window."""
    if now_ms is None:
        now_ms = dt.datetime.now(dt.timezone.utc).timestamp() * 1000
    latest = defaultdict(lambda: (0, None, None))
    for r in rows:
        ev = r.get('event')
        if ev not in ('placed', 'closed'):
            continue
        k = (r.get('strategy'), r.get('symbol'), r.get('dir'))
        ts = r.get('ts') or 0
        if ts > latest[k][0]:
            latest[k] = (ts, ev, r)
    out = []
    for k, (ts, ev, r) in latest.items():
        if ev == 'placed' and (now_ms - ts) <= recency_days * 86400000:
            out.append(dict(strategy=k[0], symbol=k[1], dir=k[2], filled=_ts(ts),
                            entry=r.get('entry_filled'), book=BOOK.get(k[0])))
    return sorted(out, key=lambda x: x['filled'])

def pending_orders():
    """PENDING = live-roster signals currently in a resting/triggered state (the broker 'Orders' tab)."""
    out = []
    for f in SIG_FILES:
        try:
            d = json.load(open(f))
        except FileNotFoundError:
            continue
        for s in d.get('signals', []):
            if s.get('state') == 'triggered' and s.get('strategy') in LIVE:
                out.append(dict(strategy=s.get('strategy'), book=BOOK.get(s.get('strategy')),
                                symbol=s.get('symbol') or s.get('sym') or s.get('pair'),
                                dir=s.get('dir')))
    return out

def realized(rows, week_start_ms):
    closed = [r for r in rows if r.get('event') == 'closed' and (r.get('ts') or 0) >= week_start_ms]
    by = defaultdict(list)
    for r in closed:
        by[r.get('strategy')].append(r)
    def agg(rs):
        n = len(rs); w = sum(1 for r in rs if (r.get('realized_r') or 0) > 0)
        return n, w, sum(r.get('net_profit') or 0 for r in rs), sum(r.get('realized_r') or 0 for r in rs)
    return closed, by, agg

def main():
    ws = sys.argv[1] if len(sys.argv) > 1 else '2026-11-02'
    recency = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    week_start_ms = dt.datetime.fromisoformat(ws).replace(tzinfo=dt.timezone.utc).timestamp() * 1000
    rows = load_execs()

    print('=== LIVE BOOK STATE (reconciled) ===')
    opos = open_positions(rows, recency)
    print('OPEN positions (best-effort — verify vs broker): %d' % len(opos))
    for p in opos:
        print('  %-9s %-14s %-5s %-8s entry=%s filled %s'
              % (p['book'], p['strategy'], p['dir'], p['symbol'], p['entry'], p['filled']))
    pend = pending_orders()
    print('PENDING orders (signal feed, authoritative): %d' % len(pend))
    for p in pend:
        print('  %-9s %-14s %-5s %-8s' % (p['book'], p['strategy'], p['dir'], p['symbol']))

    print('\n=== REALIZED since %s (closed events — reliable) ===' % ws)
    closed, by, agg = realized(rows, week_start_ms)
    print('%-14s %-9s %4s %5s %10s %8s' % ('strategy', 'book', 'n', 'WR', 'net$', 'sumR'))
    for s in sorted(by, key=lambda k: -agg(by[k])[2]):
        n, w, net, r_ = agg(by[s])
        print('%-14s %-9s %4d %4.0f%% %+10.0f %+8.2f' % (s, BOOK.get(s), n, 100*w/n if n else 0, net, r_))
    n, w, net, r_ = agg(closed)
    print('%-14s %-9s %4d %4.0f%% %+10.0f %+8.2f' % ('TOTAL', '', n, 100*w/n if n else 0, net, r_))

if __name__ == '__main__':
    main()
