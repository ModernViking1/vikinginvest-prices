"""Overlap / complementarity of the validated edges:
   HS  = H&S fired against a high-confidence macro-EW read
   S5  = Multi-TF Confluence (weekly trend + daily-50EMA pullback + 4H trigger)
Do they trade the SAME setups (redundant) or DIFFERENT ones (diversifying)? And
does combining them smooth the equity curve? All on the historical (simulated)
sample. HS + S5 both scored at 1:2 (RR2) for a fair comparison; S5 uses the RSI
trigger (the strongest variant) plus the engulfing baseline.
"""
import json, bisect
from collections import defaultdict
from detect_triggers import (
    PAIR_CLASS, macd_series, auto_detect_ew, AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from hs_swing_research import scan as hs_scan, MAX_HOLD as HS_HOLD
from five_strategies_research import ema, atr, adx, agg4h, weekly, walk, cost, is_engulf, HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RR = 2.0
COOCCUR_H = 48   # hours window to call two signals "the same setup"


def costr(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def collect_hs(pk, h1, daily, draw):
    d_ts = [b['_ts'] for b in daily]; cache = {}
    def aew(dd):
        if dd not in cache:
            try:
                r = auto_detect_ew(draw[:dd+1]); e = r.get('ew') if r.get('ok') else None
                cache[dd] = e['dir'] if (e and e.get('dir') in ('bull','bear') and e.get('confidence',0) >= AUTO_EW_MIN_CONFIDENCE and e.get('pattern') in AUTO_EW_VALID_PATTERNS) else None
            except Exception:
                cache[dd] = None
        return cache[dd]
    out = []
    for kind in ('bear', 'bull'):
        for tr in hs_scan(h1, kind):
            dd = bisect.bisect_right(d_ts, tr['ts']) - 2
            macro = aew(dd); tdir = 'bear' if kind == 'bear' else 'bull'
            if not (macro is not None and macro != tdir): continue
            o = walk(h1, tr['entry_idx'], tr['entry'], tr['stop'], tdir, RR, HS_HOLD)
            if o is None: continue
            out.append({'ts': tr['ts'], 'pair': pk, 'dir': tdir, 'r': o - costr(o, tr['entry'], tr['R'])})
    return out


def collect_s5(pk, h1, daily, trigger):
    b4 = agg4h(h1); wk = weekly(daily)
    if len(wk) < 12 or len(b4) < 250: return []
    wc = [b['c'] for b in wk]; we20 = ema(wc, 10)
    dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
    d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
    closes4 = [b['c'] for b in b4]; m4, s4 = macd_series(closes4, 12, 26, 9); r4 = precompute_rsi(closes4, 14)
    out = []; last = -1
    for i in range(2, len(b4) - 1):
        if i <= last: continue
        ts = b4[i]['_ts']
        di = bisect.bisect_right(d_ts, ts) - 1; wi = bisect.bisect_right(w_ts, ts) - 1
        if di < 51 or wi < 11 or we20[wi] is None or de50[di] is None: continue
        wk_up = wc[wi] > we20[wi] and we20[wi] > we20[wi-1]; wk_dn = wc[wi] < we20[wi] and we20[wi] < we20[wi-1]
        if not (wk_up or wk_dn): continue
        a = atr(daily, 14, di)
        if a is None or abs(daily[di]['c'] - de50[di]) > 0.5*a: continue
        av = adx(b4, 14, i)
        if av is None or av < 22: continue
        d = 'bull' if wk_up else 'bear'
        fire = is_engulf(b4, i, d) if trigger == 'engulf' else (
            (r4[i-1] is not None and r4[i] is not None) and ((d == 'bull' and r4[i-1] <= 50 < r4[i]) or (d == 'bear' and r4[i-1] >= 50 > r4[i])))
        if not fire: continue
        stop = min(b4[i]['l'], b4[i-1]['l']) if d == 'bull' else max(b4[i]['h'], b4[i-1]['h'])
        entry = b4[i+1]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        R = abs(entry - stop)
        o = walk(b4, i+1, entry, stop, d, RR, HOLD['4h'])
        if o is not None:
            out.append({'ts': ts, 'pair': pk, 'dir': d, 'r': o - costr(o, entry, R)})
        last = i + 1
    return out


def stats(trades):
    r = [t['r'] for t in trades]; n = len(r); w = sum(1 for x in r if x > 0)
    cum = peak = mdd = 0.0
    for t in sorted(trades, key=lambda x: x['ts']):
        cum += t['r']; peak = max(peak, cum); mdd = min(mdd, cum - peak)
    return n, (100*w/n if n else 0), (sum(r)/n if n else 0), sum(r), mdd


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    HS, S5e, S5r = [], [], []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80: continue
        HS += collect_hs(pk, h1, daily, draw)
        S5e += collect_s5(pk, h1, daily, 'engulf')
        S5r += collect_s5(pk, h1, daily, 'rsi')

    for nm, tr in (('HS (H&S+macro)', HS), ('S5 engulf', S5e), ('S5 rsi', S5r)):
        n, w, e, tot, mdd = stats(tr)
        print(f"{nm:<16} n={n:>4} WR={w:>4.1f}% exp={e:>+.3f} totalR={tot:>+7.1f} maxDD={mdd:>+6.1f}")

    # co-occurrence: HS vs S5(rsi) on same pair within window, same dir
    def key(t): return (t['pair'], t['dir'])
    s5_by = defaultdict(list)
    for t in S5r: s5_by[key(t)].append(t['ts'])
    co = 0
    for t in HS:
        near = any(abs(t['ts'] - x) <= COOCCUR_H*3600 for x in s5_by.get(key(t), []))
        if near: co += 1
    print(f"\nCo-occurrence HS∩S5rsi (same pair+dir within {COOCCUR_H}h): {co} of {len(HS)} HS trades "
          f"({100*co/max(1,len(HS)):.0f}%) -> they mostly trade DIFFERENT setups" if co < len(HS)*0.3 else "")

    # combined portfolios
    print("\nCOMBINED PORTFOLIOS (union of trade streams):")
    for nm, combo in (('HS + S5rsi', HS + S5r), ('HS + S5engulf', HS + S5e), ('HS + S5rsi + S5engulf', HS + S5r + S5e)):
        n, w, e, tot, mdd = stats(combo)
        span = (max(t['ts'] for t in combo) - min(t['ts'] for t in combo)) / (7*86400)
        print(f"  {nm:<22} n={n:>4} ({n/span:>4.1f}/wk) exp={e:>+.3f} totalR={tot:>+7.1f} maxDD={mdd:>+6.1f} "
              f"return/DD={abs(tot/mdd):>4.1f}x" if mdd else "")

    # fold-level correlation (do they win/lose in the same periods?)
    import statistics
    def fold_returns(trades, K=8):
        trades = sorted(trades, key=lambda x: x['ts']); n = len(trades); f = max(1, n//K)
        return [sum(t['r'] for t in trades[i*f:(i+1)*f]) / max(1, len(trades[i*f:(i+1)*f])) for i in range(K)]
    fh, fs = fold_returns(HS), fold_returns(S5r)
    try:
        c = statistics.correlation(fh, fs)
        print(f"\nFold-return correlation HS vs S5rsi: {c:+.2f}  ({'diversifying' if c < 0.4 else 'correlated'})")
    except Exception:
        pass


if __name__ == '__main__':
    main()
