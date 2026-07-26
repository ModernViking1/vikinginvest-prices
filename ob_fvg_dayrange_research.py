"""TJR OB+FVG day-range strategy (user screenshots).

A strong impulse leaves a stacked ORDER BLOCK + FAIR VALUE GAP zone. Price retraces
into the zone and continues toward the opposite daily-range extreme:
  SHORT: down-impulse -> OB (last bullish candle) + bearish FVG (gap down) = supply.
    Price retraces UP into the zone -> SELL. SL above the OB. TARGET = day low.
  LONG (mirror): up-impulse -> OB + bullish FVG = demand -> BUY on retrace down.
    SL below the OB. TARGET = day high.

Day high/low = the running high/low of the current UTC day up to entry (no lookahead).
Two entries (retrace = a limit): LIMIT at the zone proximal edge (optimistic) and
MARKET at the tag bar's close (realistic). Day-range target + fixed-RR2 comparison.
Fixed cost, both-OOS gate, per class, tested 15m/h1/4h.

Run: python ob_fvg_dayrange_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
IMP_ATR = 1.0        # impulse candle range >= this * ATR
OB_LOOK = 4          # bars back for the order block (last opposite-colour candle)
RETR_WIN = 20        # bars to retrace into the zone
BUF = 0.10
COOLDOWN = 5
HOLD = {'15m': 96, 'h1': 72, '4h': 72}


def day_extremes(bars):
    """Running (dayLow, dayHigh) of the current UTC day up to each bar."""
    n = len(bars); dl = [None]*n; dh = [None]*n; cur = None; lo = hi = None
    for i in range(n):
        day = bars[i]['_ts'] // 86400
        if day != cur:
            cur = day; lo = bars[i]['l']; hi = bars[i]['h']
        else:
            lo = min(lo, bars[i]['l']); hi = max(hi, bars[i]['h'])
        dl[i] = lo; dh[i] = hi
    return dl, dh


def walk(bars, i0, entry, stop, target, d, hold):
    R = abs(entry - stop)
    if R <= 0: return None
    rr = abs(target - entry)/R
    for j in range(i0, min(i0+hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return (-1.0, rr)
            if b['h'] >= target: return (rr, rr)
        else:
            if b['h'] >= stop: return (-1.0, rr)
            if b['l'] <= target: return (rr, rr)
    return None


def scan(bars, tf, S, cls, SC):
    n = len(bars); dl, dh = day_extremes(bars); last = -1
    for j in range(OB_LOOK+2, n-2):
        if j <= last: continue
        a = atr(bars, 14, j) or 0.0
        if a <= 0: continue
        rng = bars[j]['h'] - bars[j]['l']; body = bars[j]['c'] - bars[j]['o']
        # bearish impulse -> supply zone (OB + bearish FVG)
        if -body >= IMP_ATR*a and rng >= IMP_ATR*a:
            ob = None
            for k in range(j-1, max(-1, j-1-OB_LOOK), -1):
                if bars[k]['c'] > bars[k]['o']: ob = k; break
            if ob is None: continue
            if not (j >= 2 and bars[j-2]['l'] > bars[j]['h']): continue   # bearish FVG
            zone_lo = bars[j]['h']; ob_top = bars[ob]['h']               # sell zone: FVG top..OB top
            if ob_top <= zone_lo: continue
            for r in range(j+1, min(j+1+RETR_WIN, n-1)):
                if bars[r]['h'] >= zone_lo:                              # retraced into zone
                    tgt_day = dl[r] if dl[r] is not None and dl[r] < zone_lo else None
                    stop = ob_top + BUF*a
                    _emit(bars, r, zone_lo, stop, tgt_day, 'bear', tf, S, cls, SC)
                    last = r + COOLDOWN; break
        # bullish impulse -> demand zone
        elif body >= IMP_ATR*a and rng >= IMP_ATR*a:
            ob = None
            for k in range(j-1, max(-1, j-1-OB_LOOK), -1):
                if bars[k]['c'] < bars[k]['o']: ob = k; break
            if ob is None: continue
            if not (j >= 2 and bars[j-2]['h'] < bars[j]['l']): continue   # bullish FVG
            zone_hi = bars[j]['l']; ob_bot = bars[ob]['l']
            if ob_bot >= zone_hi: continue
            for r in range(j+1, min(j+1+RETR_WIN, n-1)):
                if bars[r]['l'] <= zone_hi:
                    tgt_day = dh[r] if dh[r] is not None and dh[r] > zone_hi else None
                    stop = ob_bot - BUF*a
                    _emit(bars, r, zone_hi, stop, tgt_day, 'bull', tf, S, cls, SC)
                    last = r + COOLDOWN; break


def _emit(bars, r, zone_edge, stop, tgt_day, d, tf, S, cls, SC):
    ts = bars[r]['_ts']
    for mode, entry in (('limit', zone_edge), ('mkt', bars[r]['c'])):
        if (d == 'bear' and stop <= entry) or (d == 'bull' and stop >= entry): continue
        R = abs(entry-stop); ei = r if mode == 'limit' else r+1
        if ei >= len(bars): continue
        if tgt_day is not None and ((d == 'bear' and tgt_day < entry) or (d == 'bull' and tgt_day > entry)):
            o = walk(bars, ei, entry, stop, tgt_day, d, HOLD[tf])
            if o is not None:
                S[(tf,mode,'day')].append((ts, o[0]-cost(o[0],entry,R))); SC[cls][(tf,mode,'day')].append((ts, o[0]-cost(o[0],entry,R)))
        ft = entry+2*R if d == 'bull' else entry-2*R
        o = walk(bars, ei, entry, stop, ft, d, HOLD[tf])
        if o is not None:
            S[(tf,mode,'rr2')].append((ts, o[0]-cost(o[0],entry,R))); SC[cls][(tf,mode,'rr2')].append((ts, o[0]-cost(o[0],entry,R)))


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<18} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    S = defaultdict(list); SC = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        m15 = _bars_norm(pairs[pk].get('m15', [])); h1 = _bars_norm(pairs[pk].get('h1', []))
        if len(h1) < 400: continue
        npairs += 1
        tfs = {'h1': h1, '4h': agg4h(h1)}
        if len(m15) >= 1000: tfs['15m'] = m15
        for tf, bars in tfs.items():
            if len(bars) < 150: continue
            scan(bars, tf, S, PAIR_CLASS.get(pk), SC)

    print(f"TJR OB+FVG day-range (retrace into OB/FVG -> day extreme / RR2) — {npairs} pairs\n")
    for tf in ('15m', 'h1', '4h'):
        print(f"=== {tf} ===")
        line("limit / day-target", S[(tf,'limit','day')]); line("limit / RR2", S[(tf,'limit','rr2')])
        line("market / day-target", S[(tf,'mkt','day')]); line("market / RR2", S[(tf,'mkt','rr2')])
    print("\n=== per class (4H, market RR2) ===")
    for c in ['comm','crypto','index','major','minor']:
        line(c, SC[c][('4h','mkt','rr2')])


if __name__ == '__main__':
    main()
