"""FX STATE sweep-reversal — SESSION-TIMED cut (Asian range swept at the London open).

Julien's actual context: 'the Asian low's been swept, price looks ready for a bounce.'
So instead of fading any swing-pivot sweep (which lost universe-wide), restrict to the
classic London-open liquidity grab: mark the Asian-session range (00:00-07:00 UTC); during
the London window (07:00-11:00 UTC) fade the FIRST sweep of that range — a wick below the
Asian low that closes back above -> BUY; a wick above the Asian high that closes back
below -> SELL. One trade per pair per day. SL beyond the sweep extreme; the 3 scaled TPs.

This is the same session logic that gave 'asianglitch' its (gold-only) edge, applied to
the Asian-range/London-open structure Julien trades. m15 / h1 (no m5 in the feed), all
pairs, per class, OOS. FX majors/minors are the relevant classes (he trades GBP/EUR).

Run: python fxstate_session_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost
from fxstate_sweep_research import _fixed, _scaled, TPS, BUF, line

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
ASIAN_HOURS = {0, 1, 2, 3, 4, 5, 6}
LONDON_HOURS = {7, 8, 9, 10}
MIN_ASIAN = {'m15': 12, 'h1': 4, '4h': 1}


def hour(b):
    return int((b['_ts'] // 3600) % 24)


def _emit(bars, ei, entry, stop, d, store, cls):
    if ei >= len(bars):
        return
    R = abs(entry - stop); ts = bars[ei]['_ts']
    for rr in TPS:
        o = _fixed(bars, ei, entry, stop, d, rr)
        if o is not None:
            store[cls][('rr', rr)].append((ts, o - cost(o, entry, R)))
    for tag, be in (('blend', False), ('blendBE', True)):
        o = _scaled(bars, ei, entry, stop, d, be)
        store[cls][(tag, 0)].append((ts, o - cost(1 if o > 0 else -1, entry, R)))


def scan(bars, tf, store, cls):
    n = len(bars); min_asian = MIN_ASIAN[tf]
    # group bar indices by UTC day
    by_day = defaultdict(list)
    for i, b in enumerate(bars):
        by_day[int(b['_ts'] // 86400)].append(i)
    for day, idxs in by_day.items():
        asian = [i for i in idxs if hour(bars[i]) in ASIAN_HOURS]
        london = [i for i in idxs if hour(bars[i]) in LONDON_HOURS]
        if len(asian) < min_asian or not london:
            continue
        a_hi = max(bars[i]['h'] for i in asian); a_lo = min(bars[i]['l'] for i in asian)
        for j in sorted(london):
            b = bars[j]
            if b['l'] < a_lo and b['c'] > a_lo:          # swept Asian low, reclaimed -> BUY
                a = atr(bars, 14, j) or 0.0
                entry = b['c']; stop = b['l'] - BUF * a
                if stop < entry:
                    _emit(bars, j + 1, entry, stop, 'bull', store, cls)
                break
            if b['h'] > a_hi and b['c'] < a_hi:          # swept Asian high, reclaimed -> SELL
                a = atr(bars, 14, j) or 0.0
                entry = b['c']; stop = b['h'] + BUF * a
                if stop > entry:
                    _emit(bars, j + 1, entry, stop, 'bear', store, cls)
                break


def scan_break(bars, tf, store, cls):
    """CONTINUATION mirror: fade fails, so also test riding the London-open breakout of
    the Asian range (close beyond the range -> go with it). 3TP-blend exit."""
    mn = MIN_ASIAN[tf]; by = defaultdict(list)
    for i, b in enumerate(bars):
        by[int(b['_ts'] // 86400)].append(i)
    for day, idxs in by.items():
        asian = [i for i in idxs if hour(bars[i]) in ASIAN_HOURS]; lon = [i for i in idxs if hour(bars[i]) in LONDON_HOURS]
        if len(asian) < mn or not lon:
            continue
        a_hi = max(bars[i]['h'] for i in asian); a_lo = min(bars[i]['l'] for i in asian)
        for j in sorted(lon):
            b = bars[j]; a = atr(bars, 14, j) or 0.0
            d = 'bull' if b['c'] > a_hi else ('bear' if b['c'] < a_lo else None)
            if not d:
                continue
            entry = b['c']; stop = (a_lo - BUF * a) if d == 'bull' else (a_hi + BUF * a)
            if ((d == 'bull' and stop < entry) or (d == 'bear' and stop > entry)) and j + 1 < len(bars):
                R = abs(entry - stop); o = _scaled(bars, j + 1, entry, stop, d, False)
                store[cls][('blend', 0)].append((bars[j + 1]['_ts'], o - cost(1 if o > 0 else -1, entry, R)))
            break


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def run(tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(lambda: defaultdict(list)); npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        if tf == '4h':
            h1 = _bars_norm(pairs[pk].get('h1', []))
            if len(h1) < 300: continue
            bars = make_4h(h1)
        else:
            bars = _bars_norm(pairs[pk].get(tf, []))
        if len(bars) < 300: continue
        npr += 1
        scan(bars, tf, store, cls)
    print(f"\n===== SESSION SWEEP (Asian range / London open) · {tf} — {npr} pairs =====")
    keys = [('rr', 0.75), ('rr', 1.5), ('rr', 2.25), ('blend', 0), ('blendBE', 0)]
    lab = {('rr', 0.75): 'TP1 .75R', ('rr', 1.5): 'TP2 1.5R', ('rr', 2.25): 'TP3 2.25R',
           ('blend', 0): '3TP blend', ('blendBE', 0): '3TP +BE'}
    for c in ['major', 'minor', 'comm', 'crypto', 'index']:
        print(f"  {c}:")
        for k in keys:
            line(lab[k], store[c][k])
    print("  FX (major+minor):")
    for k in keys:
        line(lab[k], store['major'][k] + store['minor'][k])
    print("  ALL pooled:")
    for k in keys:
        line(lab[k], [r for c in store for r in store[c][k]])

    # continuation mirror (breakout) — 3TP blend only
    bs = defaultdict(lambda: defaultdict(list))
    for pk in [x for x in pairs if x in PAIR_CLASS]:
        cls = PAIR_CLASS.get(pk)
        bars = make_4h(_bars_norm(pairs[pk].get('h1', []))) if tf == '4h' else _bars_norm(pairs[pk].get(tf, []))
        if len(bars) < 300:
            continue
        scan_break(bars, tf, bs, cls)
    print("  --- CONTINUATION (ride the breakout) · 3TP blend ---")
    line('FX(maj+min)', bs['major'][('blend', 0)] + bs['minor'][('blend', 0)])
    line('ALL', [r for c in bs for r in bs[c][('blend', 0)]])


def main():
    print("=" * 90)
    print("FX STATE session sweep — fade AND ride the London-open Asian-range break + 3 scaled TPs")
    print("=" * 90)
    for tf in ('m15', 'h1'):
        run(tf)


if __name__ == '__main__':
    main()
