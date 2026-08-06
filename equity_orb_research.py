"""#4 — Opening-Range Breakout on US equities (session-aware).

ORB's documented edge is EQUITY-cash-open specific (Crabel; the 2023 Zarattini-
Aziz stock study). Our 24h-instrument ORB proxy failed because a "UTC-day range"
isn't a real opening range. This does it properly on equity intraday data: the
opening range is the first OR_BARS m15 bars of each RTH session (Twelve Data
returns RTH-only bars, so the first bar of each calendar day IS the 9:30 ET open —
DST-proof), trade the first breakout for the rest of the session, exit at the
close (ORB is a day-trade).

Extras that matter for single stocks: an earnings blackout (equity-earnings.json)
and an optional relative-volume gate on the breakout bar (equities have REAL
volume). Realistic market fills, dealing cost, chronological OOS.

BLOCKED until equity-ohlc.json exists — run fetch_equity_ohlc.py first (needs
TWELVEDATA_API_KEY, in CI or locally; the m15 layer is now fetched). This script
is the ready-to-run scaffold; it exits cleanly with a message if data is absent.

Run: python fetch_equity_ohlc.py --output equity-ohlc.json && python equity_orb_research.py
"""
import json
import os
from collections import defaultdict
from datetime import datetime

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg, cost

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'equity-ohlc.json')
EARN = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'equity-earnings.json')
OR_BARS = 2            # opening range = first 30 min (2 x 15m)
BLACKOUT_DAYS = 1      # skip the session on / adjacent to an earnings date
VOL_LB = 20
RELS = [0.0, 1.5]      # 0 = no gate; 1.5 = high-participation breakouts only


def _day(ts):
    return int(ts // 86400) if ts else 0


def load_earnings():
    if not os.path.exists(EARN):
        return {}
    try:
        raw = json.load(open(EARN))
    except Exception:
        return {}
    out = {}
    for pk, dates in raw.items():
        ds = []
        for d in dates:
            try:
                ds.append(datetime.strptime(d[:10], "%Y-%m-%d"))
            except ValueError:
                pass
        out[pk] = ds
    return out


def in_blackout(ts, earn):
    if not earn:
        return False
    day = datetime.utcfromtimestamp(ts) if ts else None
    return bool(day) and any(abs((day - e).days) <= BLACKOUT_DAYS for e in earn)


def relvol(bars, i):
    if i < VOL_LB:
        return None
    avg = sum(b.get('v', 0) or 0 for b in bars[i - VOL_LB:i]) / VOL_LB
    return ((bars[i].get('v', 0) or 0) / avg) if avg > 0 else None


def orb_trades(bars, earn, rel):
    by_day = defaultdict(list)
    for i, b in enumerate(bars):
        by_day[_day(b['_ts'])].append(i)
    rows = []
    for day, idxs in sorted(by_day.items()):
        if len(idxs) < OR_BARS + 3:
            continue
        seg = idxs[:OR_BARS]
        hi = max(bars[j]['h'] for j in seg); lo = min(bars[j]['l'] for j in seg)
        if hi <= lo:
            continue
        h = hi - lo
        for j in idxs[OR_BARS:]:
            b = bars[j]
            d = 'bull' if b['c'] > hi else ('bear' if b['c'] < lo else None)
            if not d:
                continue
            if in_blackout(bars[j]['_ts'], earn):
                break
            if rel > 0:
                rv = relvol(bars, j)
                if rv is None or rv < rel:
                    break
            entry = b['c']; stop = lo if d == 'bull' else hi
            R = abs(entry - stop)
            if R <= 0:
                break
            tgt = entry + h if d == 'bull' else entry - h    # target = 1x opening range
            last_idx = idxs[-1]
            out = None
            for k in range(j + 1, last_idx + 1):
                bb = bars[k]
                if d == 'bull':
                    if bb['l'] <= stop:
                        out = -1.0; break
                    if bb['h'] >= tgt:
                        out = (tgt - entry) / R; break
                else:
                    if bb['h'] >= stop:
                        out = -1.0; break
                    if bb['l'] <= tgt:
                        out = (entry - tgt) / R; break
            if out is None:                                   # day-trade: exit at session close (MTM)
                cl = bars[last_idx]['c']
                out = ((cl - entry) if d == 'bull' else (entry - cl)) / R
            rows.append((bars[j]['_ts'], out - cost(out, entry, R)))
            break                                             # one ORB trade per session
    return rows


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<18} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    if not os.path.exists(DATA):
        print("No equity-ohlc.json — run `python fetch_equity_ohlc.py` first (needs")
        print("TWELVEDATA_API_KEY; the m15 layer is now fetched). Scaffold is ready.")
        return
    d = json.load(open(DATA))['pairs']
    earn = load_earnings()
    print("=" * 92)
    print("Equity Opening-Range Breakout (m15, RTH session) — market fills+cost, OOS")
    print("  earnings calendar:", "loaded" if earn else "absent")
    print("=" * 92)
    for rel in RELS:
        gate = 'no gate' if rel == 0 else f'relvol>={rel}'
        print(f"\n===== {gate} =====")
        allrows = []
        for pk in d:
            bars = _bars_norm(d.get(pk, {}).get('m15', []))
            if len(bars) < 200:
                continue
            r = orb_trades(bars, earn.get(pk, []), rel)
            if r:
                line(pk, r); allrows += r
        line('ALL', allrows)


if __name__ == '__main__':
    main()
