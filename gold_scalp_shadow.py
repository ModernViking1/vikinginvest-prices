"""Intraday (m15) shadow forward-test for the gold session-scalp (#9, MONITOR ONLY).

The Audacity 'intraday scalp' idea: trade WITH the H1 trend, enter on an m15 pullback
to a short-term MA, fixed R multiple — but ONLY inside the London-NY overlap (13-17
GMT). In testing that session gate is the whole edge: unfiltered the m15 scalp loses
(~-0.08R), session-gated it printed +0.21R @ RR2 on gold with both OOS halves +.
XAUUSD-only; silver did not reproduce it, so this is watch-forward before any wiring.

MODEL-ONLY — writes gold-scalp-shadow-log.json (nothing on the platform trades it).
Market entry (m15 close), stop 0.25 ATR beyond the pullback extreme, fixed RR.

Run: python gold_scalp_shadow.py
"""
import json, os, bisect
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import _ema, atr

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
LOG = os.path.join(_HERE, 'gold-scalp-shadow-log.json')
WIN_COST_PCT, LOSS_COST_PCT = 0.0045 / 100, 0.0105 / 100
GOLD = 'xauusd'
OVERLAP_HOURS = {13, 14, 15, 16}   # London-NY overlap, ~13:00-17:00 GMT
H1_FAST, H1_SLOW, M_EMA = 20, 50, 20
GBUF = 0.25
M15_HOLD = 48
RRS = [1.5, 2.0]


def _cost(o, entry, R):
    frac = R / abs(entry) if entry else 0
    return 0 if frac <= 0 else (WIN_COST_PCT if o > 0 else LOSS_COST_PCT) / frac


def hour_utc(b):
    return int((b['_ts'] // 3600) % 24)


def detect(m15, h1):
    ef = _ema([b['c'] for b in h1], H1_FAST); es = _ema([b['c'] for b in h1], H1_SLOW)
    h1_ts = [b['_ts'] for b in h1]; me = _ema([b['c'] for b in m15], M_EMA)
    out = []
    for i in range(M_EMA + 2, len(m15) - 1):
        b = m15[i]
        hi = bisect.bisect_right(h1_ts, b['_ts']) - 1
        if hi < H1_SLOW or ef[hi] is None or es[hi] is None or me[i] is None:
            continue
        bull = ef[hi] > es[hi]; bear = ef[hi] < es[hi]
        d = None
        if bull and b['l'] <= me[i] and b['c'] > me[i]:
            d = 'bull'; stop = b['l'] - GBUF * (atr(m15, 14, i) or 0.0)
        elif bear and b['h'] >= me[i] and b['c'] < me[i]:
            d = 'bear'; stop = b['h'] + GBUF * (atr(m15, 14, i) or 0.0)
        if not d:
            continue
        ei = i + 1
        if hour_utc(m15[ei]) not in OVERLAP_HOURS:   # session gate = the edge
            continue
        entry = b['c']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            continue
        out.append({'strategy': 'gscalp', 'pair': GOLD, 'dir': d,
                    'entry_ts': m15[ei]['_ts'], 'entry': entry, 'stop': stop})
    return out


def score(m15, entry_ts, entry, stop, d, rr, hold):
    ts = [b['_ts'] for b in m15]; i0 = bisect.bisect_left(ts, entry_ts)
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(m15):
        return ('pending', None)
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    end = min(i0 + hold, len(m15))
    for j in range(i0, end):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return ('resolved', -1.0)
            if b['h'] >= tgt: return ('resolved', rr)
        else:
            if b['h'] >= stop: return ('resolved', -1.0)
            if b['l'] <= tgt: return ('resolved', rr)
    return ('pending', None) if end >= len(m15) else ('expired', None)


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'baseline_data_end': None, 'signals': {}}
    sigs = log['signals']; data_end = 0; detected = 0
    if GOLD in pairs:
        m15 = _bars_norm(pairs[GOLD].get('m15', [])); h1 = _bars_norm(pairs[GOLD].get('h1', []))
        if len(m15) >= 1000 and len(h1) >= 200:
            data_end = m15[-1]['_ts']
            for s in detect(m15, h1):
                detected += 1
                k = f"gscalp:{GOLD}:{int(s['entry_ts'])}"
                if k not in sigs:
                    s = dict(s); s['first_seen'] = data_end; s['status'] = 'pending'; sigs[k] = s
                rec = sigs[k]
                # score at RR2 (the reported cell); keep the record's status on that
                st, o = score(m15, rec['entry_ts'], rec['entry'], rec['stop'], rec['dir'], 2.0, M15_HOLD)
                rec['status'] = st
                if st == 'resolved':
                    rec['r'] = o - _cost(o, rec['entry'], abs(rec['entry'] - rec['stop']))
                else:
                    rec.pop('r', None)
    if log['baseline_data_end'] is None:
        log['baseline_data_end'] = data_end
    log['last_run_data_end'] = data_end
    tracking = log.setdefault('tracking', {})
    fs = [s.get('first_seen') for s in sigs.values() if s.get('first_seen') is not None]
    if fs and 'gscalp' not in tracking:
        tracking['gscalp'] = int(min(fs))
    with open(LOG, 'w') as f:
        json.dump(log, f, indent=1)

    base = log['baseline_data_end']; allv = list(sigs.values())

    def rep(title, rows):
        sub = [s for s in rows if s['status'] == 'resolved' and 'r' in s]
        pend = sum(1 for s in rows if s['status'] == 'pending')
        if sub:
            w = sum(1 for s in sub if s['r'] > 0)
            print(f"  {title:<24} resolved={len(sub):>3} pending={pend:>3} WR={100*w/len(sub):>4.1f}% exp={sum(s['r'] for s in sub)/len(sub):+.3f}R")
        else:
            print(f"  {title:<24} resolved=0 pending={pend}")

    _t = tracking.get('gscalp'); _td = f" · tracked {int((data_end - _t)/86400)}d" if _t and data_end else ""
    print(f"gold-scalp shadow · data_end {int(data_end)} · detected {detected} (m15, session 13-17 GMT, RR2){_td}")
    rep("ALL (incl. in-sample)", allv)
    rep("GENUINE FORWARD", [s for s in allv if s['entry_ts'] > base])
    if all(s['entry_ts'] <= base for s in allv):
        print("  (baseline just set — re-run as new m15 bars publish)")


if __name__ == '__main__':
    main()
