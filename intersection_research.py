"""Does requiring MULTIPLE indicators to agree (confluence intersection) beat
RSI-cross alone on QUALITY (expectancy), or just cut volume?

At each S5 context bar (weekly trend + daily-50EMA pullback + ADX>22), in the
trade direction, we measure:
  - fresh TRIGGERS this bar: macd cross / rsi-50 cross / golden(10/50) cross /
    wyckoff spring-UTAD  (a trigger provides the entry TIMING)
  - agreeing STATES: macd>sig / rsi>50 / sma10>sma50 / recent wyckoff  (0-4)

Ladder: enter when >=1 indicator fires this bar AND agree_count >= K, for K=1..4.
Then, directly: RSI-cross entries filtered by >=J of the OTHER 3 agreeing (J=0..3).
Realistic fill, 1:2, OOS split. Baseline reference: RSI-cross alone +0.585R.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS, macd_series
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import ema, atr, adx, agg4h, weekly, walk, cost, is_engulf, HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
RR = 2.0
WYCK_LB = 20


def sma(v, n, i): return None if i+1 < n else sum(v[i-n+1:i+1])/n


def states_and_triggers(b4, i, d, pre):
    """Return (agree_count, fired_set) for direction d at bar i."""
    m, s, r, c = pre
    st = {}; fired = set()
    # macd
    if m[i] is not None and s[i] is not None:
        st['macd'] = (m[i] > s[i]) if d == 'bull' else (m[i] < s[i])
        if None not in (m[i-1], s[i-1]):
            if (d == 'bull' and m[i-1] <= s[i-1] and m[i] > s[i]) or (d == 'bear' and m[i-1] >= s[i-1] and m[i] < s[i]):
                fired.add('macd')
    # rsi
    if r[i] is not None:
        st['rsi'] = (r[i] > 50) if d == 'bull' else (r[i] < 50)
        if r[i-1] is not None:
            if (d == 'bull' and r[i-1] <= 50 < r[i]) or (d == 'bear' and r[i-1] >= 50 > r[i]):
                fired.add('rsi')
    # golden 10/50
    a1, b1 = sma(c, 10, i), sma(c, 50, i); a0, b0 = sma(c, 10, i-1), sma(c, 50, i-1)
    if a1 is not None and b1 is not None:
        st['golden'] = (a1 > b1) if d == 'bull' else (a1 < b1)
        if None not in (a0, b0):
            if (d == 'bull' and a0 <= b0 and a1 > b1) or (d == 'bear' and a0 >= b0 and a1 < b1):
                fired.add('golden')
    # wyckoff (recent event within 3 bars = state; this-bar event = trigger)
    if i >= WYCK_LB:
        wy = False
        for j in range(max(WYCK_LB, i-2), i+1):
            sup = min(x['l'] for x in b4[j-WYCK_LB:j]); res = max(x['h'] for x in b4[j-WYCK_LB:j])
            hit = (d == 'bull' and b4[j]['l'] < sup and b4[j]['c'] > sup) or (d == 'bear' and b4[j]['h'] > res and b4[j]['c'] < res)
            if hit:
                wy = True
                if j == i: fired.add('wyckoff')
        st['wyckoff'] = wy
    agree = sum(1 for v in st.values() if v)
    return agree, fired


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0)


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else 'fail'
    print(f"  {label:<34} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}  OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    ladder = defaultdict(list)     # K -> [(ts,r)]
    rsi_conf = defaultdict(list)   # J (others agreeing) -> [(ts,r)]
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1); wk = weekly(daily)
        if len(wk) < 12 or len(b4) < 250: continue
        wc = [b['c'] for b in wk]; we20 = ema(wc, 10); dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
        d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
        c4 = [b['c'] for b in b4]; m4, s4 = macd_series(c4, 12, 26, 9); r4 = precompute_rsi(c4, 14); pre = (m4, s4, r4, c4)
        last_k = defaultdict(lambda: -1); last_j = defaultdict(lambda: -1)
        for i in range(2, len(b4) - 1):
            ts = b4[i]['_ts']; di = bisect.bisect_right(d_ts, ts) - 1; wi = bisect.bisect_right(w_ts, ts) - 1
            if di < 51 or wi < 11 or we20[wi] is None or de50[di] is None: continue
            up = wc[wi] > we20[wi] and we20[wi] > we20[wi-1]; dn = wc[wi] < we20[wi] and we20[wi] < we20[wi-1]
            if not (up or dn): continue
            a = atr(daily, 14, di)
            if a is None or abs(daily[di]['c'] - de50[di]) > 0.5*a: continue
            av = adx(b4, 14, i)
            if av is None or av < 22: continue
            d = 'bull' if up else 'bear'
            agree, fired = states_and_triggers(b4, i, d, pre)
            if not fired: continue
            stop = min(b4[i]['l'], b4[i-1]['l']) if d == 'bull' else max(b4[i]['h'], b4[i-1]['h'])
            entry = b4[i+1]['o']
            if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
            o = walk(b4, i+1, entry, stop, d, RR, HOLD['4h'])
            if o is None: continue
            r = o - cost(o, entry, abs(entry-stop))
            for K in (1, 2, 3, 4):
                if agree >= K and i > last_k[K]:
                    ladder[K].append((ts, r)); last_k[K] = i + 1
            # rsi-anchored: rsi must be among the fired triggers
            if 'rsi' in fired:
                others = agree - (1 if 'rsi' in fired else 0)   # approx others agreeing
                # count OTHER indicators agreeing by state
                oc, _ = states_and_triggers(b4, i, d, pre)
                others_agree = oc - 1 if r4[i] is not None and ((d == 'bull' and r4[i] > 50) or (d == 'bear' and r4[i] < 50)) else oc
                for J in (0, 1, 2, 3):
                    if others_agree >= J and i > last_j[J]:
                        rsi_conf[J].append((ts, r)); last_j[J] = i + 1

    print("CONFLUENCE LADDER — enter on any fresh trigger + >=K indicators agreeing (RR2):")
    for K in (1, 2, 3, 4):
        line(f"K>={K} agree", ladder[K])
    print("\nRSI-CROSS entries + >=J of the OTHER indicators agreeing (RR2):")
    print("  (J=0 is RSI-cross alone — reference +0.585R)")
    for J in (0, 1, 2, 3):
        line(f"rsi-cross + {J} others agree", rsi_conf[J])


if __name__ == '__main__':
    main()
