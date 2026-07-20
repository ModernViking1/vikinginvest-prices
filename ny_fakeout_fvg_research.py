"""NY-open fakeout + FVG model, filtered by daily bias (user screenshots).

Chain (all mechanical, generic price-action / clean-room):
  1. DAILY BIAS: bull if the daily trend is up (close > rising EMA10 = HH/HL proxy),
     bear if down (LH/LL). Only trade in the bias direction.
  2. NY-OPEN FAKEOUT (m15, our finest tf; m5 unavailable): within the NY window a
     bar sweeps liquidity AGAINST bias (bull: low breaks a recent swing low) then
     CLOSES back in-bias (close back above it) — the fakeout / stop-hunt reversal.
  3. FVG: a 3-candle fair-value gap forms in the reversal impulse (bull: gap up).
  4. ENTRY: price retraces into the FVG -> enter in-bias (next-bar open, market fill).
     Stop beyond the gap / fakeout extreme; target RR2 (proxy for 'previous day level').

All pairs, m15, realistic fills, OOS split, per class. NY window is UTC hours.

Run: python ny_fakeout_fvg_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import walk, cost, atr, agg, ema

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
NY_START, NY_END = 12, 17     # UTC hours ~ NY open window
SWEEP_LB = 12                 # m15 bars for the liquidity level to sweep
FVG_WIN = 8                   # bars after the fakeout to find an FVG
RETRACE_WIN = 12              # bars to wait for the retrace into the FVG
BUF = 0.25
RR = 2.0
HOLD = 48
COOLDOWN = 8


def daily_bias(daily):
    dc = [b['c'] for b in daily]; e = ema(dc, 10)
    out = []
    for i in range(len(daily)):
        if i < 3 or e[i] is None or e[i-3] is None:
            b = None
        elif dc[i] > e[i] and e[i] > e[i-3]:
            b = 'bull'
        elif dc[i] < e[i] and e[i] < e[i-3]:
            b = 'bear'
        else:
            b = None
        out.append((int(daily[i]['_ts']) // 86400, b))
    return out


def scan(m15, daily, store, cls, store_cls):
    bmap = daily_bias(daily); dates = [x[0] for x in bmap]
    n = len(m15); last = -1
    for i in range(SWEEP_LB, n - 1):
        if i <= last:
            continue
        b = m15[i]; hour = (int(b['_ts']) // 3600) % 24
        if not (NY_START <= hour < NY_END):
            continue
        daykey = int(b['_ts']) // 86400
        idx = bisect.bisect_left(dates, daykey) - 1
        bias = bmap[idx][1] if idx >= 0 else None
        if bias is None:
            continue
        win = m15[i-SWEEP_LB:i]
        if bias == 'bull':
            lvl = min(x['l'] for x in win)
            if not (b['l'] < lvl and b['c'] > lvl):
                continue
            fvg = None
            for j in range(i+2, min(i+2+FVG_WIN, n)):
                if m15[j-2]['h'] < m15[j]['l']:
                    fvg = (m15[j-2]['h'], m15[j]['l'], j); break
            if fvg is None:
                continue
            g_bot, g_top, jf = fvg
            ei = None
            for r in range(jf+1, min(jf+1+RETRACE_WIN, n-1)):
                if m15[r]['l'] <= g_top:
                    ei = r+1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = m15[ei]['o']; a = atr(m15, 14, ei-1) or 0.0
            stop = min(g_bot, b['l']) - BUF*a; d = 'bull'
        else:
            lvl = max(x['h'] for x in win)
            if not (b['h'] > lvl and b['c'] < lvl):
                continue
            fvg = None
            for j in range(i+2, min(i+2+FVG_WIN, n)):
                if m15[j-2]['l'] > m15[j]['h']:
                    fvg = (m15[j]['h'], m15[j-2]['l'], j); break
            if fvg is None:
                continue
            g_bot, g_top, jf = fvg
            ei = None
            for r in range(jf+1, min(jf+1+RETRACE_WIN, n-1)):
                if m15[r]['h'] >= g_bot:
                    ei = r+1; break
            if ei is None or ei <= last or ei >= n:
                continue
            entry = m15[ei]['o']; a = atr(m15, 14, ei-1) or 0.0
            stop = max(g_top, b['h']) + BUF*a; d = 'bear'
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        R = abs(entry-stop); ts = m15[ei]['_ts']
        o = walk(m15, ei, entry, stop, d, RR, HOLD)
        if o is not None:
            net = o - cost(o, entry, R)
            store.append((ts, net)); store_cls[cls].append((ts, net))
        last = ei + COOLDOWN


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<12} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = []; store_cls = defaultdict(list); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        m15 = _bars_norm(pairs[pk].get('m15', []))
        if len(h1) < 400 or len(daily) < 80 or len(m15) < 300:
            continue
        npairs += 1
        scan(m15, daily, store, cls, store_cls)

    print(f"NY-open fakeout + FVG (daily-bias filtered) — {npairs} pairs, m15, RR2, realistic fills, OOS\n")
    line("ALL PAIRS", store)
    print("\nPer class:")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(c, store_cls[c])


if __name__ == '__main__':
    main()
