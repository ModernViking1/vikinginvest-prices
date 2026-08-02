"""Alex Morris / Trading Cafe supply-demand — systematic replication on FX.

Motivated by a student's manual BarReplay results (Pascal S: 300 trades, 76.7% WR,
+1.90R avg, on FX minors/majors). Those are DISCRETIONARY, hand-placed limit entries at
zone edges. This script asks the only honest question: does the SAME idea survive when it is
(a) detected mechanically, (b) filled at MARKET on the retest (not a favorable limit at the
zone edge), and (c) split chronologically OOS — on the exact class the student traded?

The Trading Cafe method, faithfully:
  1. Fresh supply/demand zone = a tight BASE (consolidation) then an EXPLOSIVE departure that
     leaves the base (imbalance). Base range is the zone (proximal/distal lines).
  2. TOP-DOWN BIAS ("bullish sentiment on the weekly/daily"): take demand (long) only when the
     HTF bias is up; supply (short) only when HTF bias is down. This is the discretionary
     filter the student's notes lean on — encoded here explicitly.
  3. "Do not trade a ranging market": require trend separation on the zone TF (not-ranging).
  4. Retest entry: price returns into the zone and holds -> enter MARKET at the retest bar
     close (a full fill THROUGH the zone invalidates). Stop beyond the zone extreme. Target
     2:1 / 3:1.

Matrix of filters so we can see WHERE any edge lives:
  base      = zone + market retest, no context filter
  +bias     = + top-down HTF bias alignment
  +bias+trd = + not-ranging (trend separation) on the zone TF

Zones on 4h and daily; entry resolved on h1. Realistic fixed cost. Both-OOS-halves-positive
and n>=40 => PASS. Headline is minor/major (the student's class); all classes reported for
context.

Run: python alex_morris_sd_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost, ema, agg4h

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')

BASE_RNG = 0.80      # base (consolidation) candle range <= BASE_RNG * ATR  (tight)
EXPL_ATR = 1.20      # explosive departure body >= EXPL_ATR * ATR
BUF = 0.20           # stop buffer beyond the zone extreme (ATR)
RETR_H1 = 400        # h1 bars to wait for the retest (~16d)
HOLD = 160           # h1 bars to reach target (~6.7d)
TREND_SEP = 0.30     # not-ranging: |ema20-ema50| >= TREND_SEP * ATR on the zone TF
RRS = [2.0, 3.0]


def make_4h(h1):
    return [{'o': h1[k]['o'], 'c': h1[k + 3]['c'], 'h': max(x['h'] for x in h1[k:k + 4]),
             'l': min(x['l'] for x in h1[k:k + 4]), '_ts': h1[k]['_ts']} for k in range(0, len(h1) - 3, 4)]


def zones(htf, bar_secs):
    """Yield (ready_ts, dir, zlo, zhi, bias_up, not_ranging) for each tight-base -> explosive
    departure zone on the HTF. bias_up / not_ranging are HTF-context flags computed causally at
    the departure bar (no look-ahead)."""
    c = [b['c'] for b in htf]; e20 = ema(c, 20); e50 = ema(c, 50)
    out = []
    for i in range(50, len(htf) - 1):
        a = atr(htf, 14, i) or 0.0
        if a <= 0:
            continue
        base = htf[i]; nxt = htf[i + 1]
        base_rng = base['h'] - base['l']
        if base_rng <= 0 or base_rng > BASE_RNG * a:        # base must be a tight consolidation
            continue
        dep_body = nxt['c'] - nxt['o']
        if abs(dep_body) < EXPL_ATR * a:                    # departure must be explosive
            continue
        d = 'bull' if dep_body > 0 else 'bear'
        # departure must actually leave the base (imbalance), not just wiggle inside it
        if d == 'bull' and nxt['c'] <= base['h']:
            continue
        if d == 'bear' and nxt['c'] >= base['l']:
            continue
        if e20[i] is None or e50[i] is None:
            continue
        htf_up = e20[i] > e50[i]
        bias_ok = htf_up if d == 'bull' else (not htf_up)
        not_ranging = abs(e20[i] - e50[i]) >= TREND_SEP * a
        out.append((nxt['_ts'] + bar_secs, d, base['l'], base['h'], bias_ok, not_ranging))
    return out


def score(h1, ready_ts, d, zlo, zhi, rr):
    ts = [b['_ts'] for b in h1]; s = bisect.bisect_left(ts, ready_ts)
    for r in range(s, min(s + RETR_H1, len(h1) - 1)):
        b = h1[r]
        if d == 'bull':
            touched = b['l'] <= zhi and b['c'] > zlo           # dipped into zone, held above its low
        else:
            touched = b['h'] >= zlo and b['c'] < zhi
        if not touched:
            continue
        a = atr(h1, 14, r) or 0.0
        entry = b['c']; stop = (zlo - BUF * a) if d == 'bull' else (zhi + BUF * a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            return None
        R = abs(entry - stop); tgt = entry + rr * R if d == 'bull' else entry - rr * R
        for j in range(r + 1, min(r + 1 + HOLD, len(h1))):
            bb = h1[j]
            if d == 'bull':
                if bb['l'] <= stop: return (-1.0, entry, R)
                if bb['h'] >= tgt: return (rr, entry, R)
            else:
                if bb['h'] >= stop: return (-1.0, entry, R)
                if bb['l'] <= tgt: return (rr, entry, R)
        return None
    return None


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<7} RR{rr:g} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


FILTERS = ['base', '+bias', '+bias+trd']


def passes(flt, bias_ok, not_ranging):
    if flt == 'base':
        return True
    if flt == '+bias':
        return bias_ok
    return bias_ok and not_ranging


def run(zone_tf):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    # store[filter][class][rr] -> list[(ts, net_R)]
    store = {f: defaultdict(lambda: defaultdict(list)) for f in FILTERS}
    ZSECS = {'4h': 4 * 3600, 'daily': 86400}
    npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 500:
            continue
        zbars = make_4h(h1) if zone_tf == '4h' else _bars_norm(pairs[pk].get('daily', []))
        if len(zbars) < 80:
            continue
        npr += 1
        for ready, dr, zlo, zhi, bias_ok, not_ranging in zones(zbars, ZSECS[zone_tf]):
            for rr in RRS:
                res = score(h1, ready, dr, zlo, zhi, rr)
                if res is None:
                    continue
                o, entry, R = res
                net = o - cost(o, entry, R)
                for f in FILTERS:
                    if passes(f, bias_ok, not_ranging):
                        store[f][cls][rr].append((ready, net))
    print(f"\n===== {zone_tf}-zone -> h1 retest (Trading Cafe S&D, MARKET fills) — {npr} pairs =====")
    for f in FILTERS:
        print(f"  [{f}]")
        for c in ['minor', 'major', 'comm', 'crypto', 'index']:
            for rr in RRS:
                line(c, store[f][c][rr], rr)
        # FX = minor+major combined (the student's universe)
        for rr in RRS:
            fx = store[f]['minor'][rr] + store[f]['major'][rr]
            line('FX(m+M)', fx, rr)


def main():
    print("=" * 92)
    print("Alex Morris / Trading Cafe supply-demand — systematic, market fills, FX focus")
    print("=" * 92)
    run('4h')
    run('daily')


if __name__ == '__main__':
    main()
