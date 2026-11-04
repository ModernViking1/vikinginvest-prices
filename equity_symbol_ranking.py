#!/usr/bin/env python3
"""Per-symbol x per-strategy ma_pull/holygrail/turtle/threepush/fib profitability matrix, each scored
in its LIVE regime — so we can build a ma_pull symbol allow-list without cutting names the OTHER
strategies need. Gap-aware (fills at the bar open on a gap), 4% backstop floor, resolved-only.

Regimes:
  ma_pull_eq         -> INTRADAY-ONLY (flat at the entry-day session close; matches the live EA)
  holygrail_eq_m15   -> full-hold (m15)      | these HOLD overnight by design, so full-hold is correct
  turtle_soup_eq     -> full-hold (h1)
  threepush_eq       -> full-hold (4h)
  fib_eq             -> full-hold (h1 + 4h)

Output: a matrix of avgR (with OOS flag) per symbol x strategy, plus, per symbol, which live strategies
are OOS-robust-positive. Names positive for NO strategy are universe-drop candidates; names bad for
ma_pull but good elsewhere stay in the universe and only leave ma_pull's allow-list.

Usage: python equity_symbol_ranking.py <export.json>   (works on the 20-name or a full 50-name export)
"""
import sys, json, bisect, statistics as st, datetime as dt
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

TA, TD, TH = H.TRAIL_ARM, H.TRAIL_DIST, H.TRAIL_HOLD
FLOOR = -8.0
STRATS = ['ma_pull_eq', 'holygrail_eq_m15', 'turtle_soup_eq', 'threepush_eq', 'fib_eq']
d = json.load(open(sys.argv[1])).get('pairs', {})

def _score(bars, ets, entry, stop, dr, intraday):
    """gap-aware trailing runner; intraday=True forces flat at the entry-day session close."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, ets); R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars): return None
    eday = dt.datetime.utcfromtimestamp(bars[i0]['_ts']).date()
    es = stop; armed = False; peak = entry; end = i0 + TH
    for j in range(i0, min(end, len(bars))):
        b = bars[j]
        if intraday and dt.datetime.utcfromtimestamp(b['_ts']).date() != eday:
            lc = bars[j-1]['c']; return ((lc-entry) if dr == 'bull' else (entry-lc))/R
        if dr == 'bull':
            if b['o'] <= es: return (b['o']-entry)/R
            if b['l'] <= es: return (es-entry)/R
            peak = max(peak, b['h'])
            if not armed and b['h'] >= entry+TA*R: armed = True
            if armed: es = max(es, peak-TD*R)
        else:
            if b['o'] >= es: return (entry-b['o'])/R
            if b['h'] >= es: return (entry-es)/R
            peak = min(peak, b['l'])
            if not armed and b['l'] <= entry-TA*R: armed = True
            if armed: es = min(es, peak+TD*R)
    if end <= len(bars):
        lc = bars[end-1]['c']; return ((lc-entry) if dr == 'bull' else (entry-lc))/R
    return None

# cell[(symbol, strategy)] -> list of (entry_ts, R)
cell = defaultdict(list)
for pk in d:
    if not (pk.endswith('_nas') or pk.endswith('_nyse')): continue
    eh1 = _bars_norm(d[pk].get('h1', [])); em15 = _bars_norm(d[pk].get('m15', []))
    e4h = H.agg4h(eh1) if len(eh1) >= 600 else []
    jobs = []   # (strategy, signals, book, intraday)
    if len(eh1) >= 400:
        jobs.append(('ma_pull_eq', H._ma_pull_signals(pk, eh1, 'h1'), eh1, True))
        jobs.append(('turtle_soup_eq', H.trend_filter_signals(eh1, H._mw_signals(eh1, pk, 'turtle_soup_eq', 'h1', H._turtlesoup_sig)), eh1, False))
        jobs.append(('fib_eq', H._fib_eq_signals(pk, eh1, 'h1'), eh1, False))
    if len(em15) >= 400:
        jobs.append(('ma_pull_eq', H._ma_pull_signals(pk, em15, 'm15'), em15, True))
        jobs.append(('holygrail_eq_m15', H.trend_filter_signals(em15, H._mw_signals(em15, pk, 'holygrail_eq_m15', 'm15', H._holygrail_sig)), em15, False))
    if len(e4h) >= 150:
        jobs.append(('threepush_eq', H._mw_signals(e4h, pk, 'threepush_eq', '4h', H.threepush_core), e4h, False))
    if len(e4h) >= 250:
        jobs.append(('fib_eq', H._fib_eq_signals(pk, e4h, '4h'), e4h, False))
    seen = set()
    for strat, sigs, book, intraday in jobs:
        for s in sigs:
            k = (strat, s['tf'], int(s['entry_ts']))
            if k in seen: continue
            seen.add(k)
            r = _score(book, s['entry_ts'], s['entry'], s['stop'], s['dir'], intraday)
            if r is None: continue
            r = max(r - H.cost(r, s['entry'], abs(s['entry']-s['stop'])), FLOOR)
            cell[(pk, strat)].append((s['entry_ts'], r))

def stat(rows):
    if not rows: return None
    R = [x[1] for x in rows]; n = len(R)
    srt = [x[1] for x in sorted(rows)]; mid = n//2
    o1 = st.mean(srt[:mid]) if srt[:mid] else 0; o2 = st.mean(srt[mid:]) if srt[mid:] else 0
    return dict(n=n, avg=st.mean(R), oos=(o1 > 0 and o2 > 0))

def cf(pk, strat):
    s = stat(cell.get((pk, strat)))
    if not s: return '    -   '
    return '%+.3f%s' % (s['avg'], '*' if s['oos'] else ' ')

syms = sorted({pk for (pk, _) in cell})
print('ma_pull=INTRADAY-ONLY; others=full-hold. gap-aware, 4%% backstop. avgR shown, * = both-OOS positive.\n')
print('%-11s %-9s %-9s %-9s %-9s %-9s' % ('symbol', 'ma_pull', 'holygr_m15', 'turtle', '3push', 'fib'))
for pk in sorted(syms, key=lambda p: -(stat(cell.get((p, 'ma_pull_eq'))) or {'avg': -9})['avg']):
    print('%-11s %-9s %-9s %-9s %-9s %-9s' % (pk, cf(pk, 'ma_pull_eq'), cf(pk, 'holygrail_eq_m15'),
          cf(pk, 'turtle_soup_eq'), cf(pk, 'threepush_eq'), cf(pk, 'fib_eq')))

print('\n=== ma_pull ALLOW-LIST (OOS-robust positive for ma_pull) ===')
allow = [pk for pk in syms if (stat(cell.get((pk, 'ma_pull_eq'))) or {}).get('oos') and (stat(cell.get((pk, 'ma_pull_eq'))) or {}).get('avg', 0) > 0]
print('  ', ', '.join(allow) if allow else '(none)')

print('\n=== per-symbol: which live strategies are OOS-robust positive ===')
drop_cand = []
for pk in syms:
    robust = [s for s in STRATS if (stat(cell.get((pk, s))) or {}).get('oos') and (stat(cell.get((pk, s))) or {}).get('avg', 0) > 0]
    tag = '  <-- bad for ma_pull, KEPT by: ' + ','.join(x.replace('_eq', '') for x in robust) if (robust and pk not in allow) else ''
    if not robust:
        drop_cand.append(pk); tag = '  <-- UNIVERSE-DROP candidate (positive for NO strategy)'
    print('  %-11s %s%s' % (pk, ','.join(x.replace('_eq', '') for x in robust) or '(none)', tag))
print('\nUNIVERSE-DROP candidates (help no live strategy):', ', '.join(drop_cand) if drop_cand else '(none)')
