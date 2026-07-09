"""Unified shadow forward-test harness for the validated swing edges:
   hs       = H&S fired against a high-confidence macro-EW read (H1, 1:2)
   s5_engulf= Multi-TF Confluence, 4H engulfing trigger (4H, 1:2)
   s5_rsi   = Multi-TF Confluence, 4H RSI-50-cross trigger (4H, 1:2)  [strongest]

Run periodically against the latest published data. Each run detects current
signals, logs any NEW ones (dedup) with the data-end at first sight, re-scores
all logged signals on the latest bars, and reports IN-SAMPLE backfill vs GENUINE
FORWARD (entry after the harness's first run). Writes only swing-shadow-log.json,
which nothing on the platform reads. NO deployment — evidence-gathering only.
"""
import json, os, bisect
from detect_triggers import (
    PAIR_CLASS, macd_series, auto_detect_ew, AUTO_EW_MIN_CONFIDENCE, AUTO_EW_VALID_PATTERNS,
)
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from hs_swing_research import scan as hs_scan, MAX_HOLD as HS_HOLD
from five_strategies_research import ema, atr, adx, agg4h, weekly, is_engulf, HOLD

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
LOG = '/home/user/vikinginvest-prices/swing-shadow-log.json'
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
RR = 2.0


def detect_hs(pk, h1, daily, draw):
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
            out.append({'strategy': 'hs', 'tf': 'h1', 'pair': pk, 'dir': tdir,
                        'entry_ts': tr['ts'], 'entry': tr['entry'], 'stop': tr['stop']})
    return out


def detect_s5(pk, h1, daily, trigger):
    b4 = agg4h(h1); wk = weekly(daily)
    if len(wk) < 12 or len(b4) < 250: return []
    wc = [b['c'] for b in wk]; we20 = ema(wc, 10)
    dc = [b['c'] for b in daily]; de50 = ema(dc, 50)
    d_ts = [b['_ts'] for b in daily]; w_ts = [b['_ts'] for b in wk]
    c4 = [b['c'] for b in b4]; m4, s4 = macd_series(c4, 12, 26, 9); r4 = precompute_rsi(c4, 14)
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
            r4[i-1] is not None and r4[i] is not None and ((d == 'bull' and r4[i-1] <= 50 < r4[i]) or (d == 'bear' and r4[i-1] >= 50 > r4[i])))
        if not fire: continue
        stop = min(b4[i]['l'], b4[i-1]['l']) if d == 'bull' else max(b4[i]['h'], b4[i-1]['h'])
        entry = b4[i+1]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        out.append({'strategy': 's5_' + trigger, 'tf': '4h', 'pair': pk, 'dir': d,
                    'entry_ts': b4[i+1]['_ts'], 'entry': entry, 'stop': stop})
        last = i + 1
    return out


def score(bars, entry_ts, entry, stop, d, hold):
    """Return ('resolved', r) | ('pending', None) | ('expired', None).
    Matches the research walk(): unresolved within the hold is EXCLUDED (not a
    loss). 'pending' = ran out of data (will resolve later); 'expired' = hold
    elapsed with data available (time-stop, excluded from WR like the backtest)."""
    ts = [b['_ts'] for b in bars]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars): return ('pending', None)
    tgt = entry + RR*R if d == 'bull' else entry - RR*R
    end = min(i0 + hold, len(bars))
    for j in range(i0, end):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= tgt: return ('resolved', RR)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= tgt: return ('resolved', RR)
    return ('pending', None) if end >= len(bars) else ('expired', None)


def cost(o, entry, R):
    frac = R/abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT)/frac


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80: continue
        b4 = agg4h(h1); data_end = max(data_end, h1[-1]['_ts'])
        found = detect_hs(pk, h1, daily, draw) + detect_s5(pk, h1, daily, 'engulf') + detect_s5(pk, h1, daily, 'rsi')
        for s in found:
            detected += 1
            k = f"{s['strategy']}:{s['pair']}:{int(s['entry_ts'])}"
            if k not in sigs:
                s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
            rec = sigs[k]
            bars = h1 if rec['tf'] == 'h1' else b4
            hold = HS_HOLD if rec['tf'] == 'h1' else HOLD['4h']
            st, o = score(bars, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], hold)
            rec['status'] = st
            if st == 'resolved':
                rec['r'] = o - cost(o, rec['entry'], abs(rec['entry']-rec['stop']))
            else:
                rec.pop('r', None)
    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())
    def rep(title, rows):
        print(f"\n{title}")
        for strat in ('hs', 's5_engulf', 's5_rsi'):
            sub = [s for s in rows if s['strategy'] == strat and s['status'] == 'resolved' and 'r' in s]
            pend = sum(1 for s in rows if s['strategy'] == strat and s['status'] == 'pending')
            if sub:
                w = sum(1 for s in sub if s['r'] > 0)
                print(f"  {strat:<10} resolved={len(sub):>3} pending={pend:>3} WR={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R")
            else:
                print(f"  {strat:<10} resolved=0 pending={pend}")
    print(f"harness run · data_end {int(data_end)} · detected {detected} signals this pass")
    rep("ALL logged (incl. in-sample backfill):", allv)
    rep("GENUINE FORWARD (entry after first run):", [s for s in allv if s['entry_ts'] > base])
    if all(s['entry_ts'] <= base for s in allv):
        print("\n  (baseline just set — re-run as new bars publish to accumulate forward signals)")


if __name__ == '__main__':
    main()
