"""Intraday LIMIT-vs-MARKET entry A/B shadow tracker.

Runs periodically. Reads the LIVE signals.json feed (does NOT touch signal
generation) and, the first time it sees each intraday signal, records:
  - limit_entry : the signal's pullback limit level (what the live cBot places)
  - market_entry: the latest m15 close at capture (what a market order would fill)
  - stop, dir, method
Then scores BOTH arms on the latest m15 OHLC, signal-for-signal, at the live 1:1
target — a true forward A/B on identical signals:

  LIMIT arm  : fills only if price pulls back to limit_entry within FILL_WIN bars
               (the ~45-min cBot expiry). Unfilled = a MISSED trade (the runner) —
               this is the adverse-selection cost the backtest can't see.
  MARKET arm : enters immediately at market_entry (every signal fills), 1R target
               from the market fill, same stop.

Writes intraday-ab-log.json (inert — nothing on the platform reads it). Tracking
begins the first run; the comparison sharpens as signals resolve over hours/days.

Run: python intraday_ab_tracker.py
"""
import json, os, bisect
from backtest_rsi_per_class import _bars_norm

_HERE = os.path.dirname(os.path.abspath(__file__))
FEED = os.path.join(_HERE, 'signals.json')
HIST = os.path.join(_HERE, 'historical-ohlc.json')
LOG = os.path.join(_HERE, 'intraday-ab-log.json')

FILL_WIN = 3      # m15 bars (~45 min) for the limit to fill — matches cBot expiry
HOLD = 40         # m15 bars to resolve the trade (~10h)
RR = 1.0          # live intraday target (kept 1:1 — the sweep showed higher hurts)


TOL = 0.10
BODY_MIN = 0.5


def _nowick_side(b):
    """Decisive candle with ~no wick on one side (generic candle shape): bull = no
    lower wick + bullish body, bear = no upper wick. Tags each signal nowick_aligned
    so the Omar-style no-wick FILTER win rate accumulates live."""
    rng = b['h'] - b['l']
    if rng <= 0 or abs(b['c'] - b['o']) < BODY_MIN * rng:
        return None
    if b['c'] > b['o'] and (min(b['o'], b['c']) - b['l']) <= TOL * rng:
        return 'bull'
    if b['c'] < b['o'] and (b['h'] - max(b['o'], b['c'])) <= TOL * rng:
        return 'bear'
    return None


def walk(bars, i0, entry, stop, d, hold):
    """('resolved', r) | ('pending', None) | ('expired', None)."""
    R = abs(entry - stop)
    if R <= 0 or i0 >= len(bars):
        return ('pending', None)
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


def limit_fill(bars, ci, limit, d, win):
    """Return fill-bar index if price reaches the limit within `win` bars after
    capture, the sentinel 'nofill' if the window elapsed with data, else None
    (still pending)."""
    end = min(ci + 1 + win, len(bars))
    for j in range(ci + 1, end):
        b = bars[j]
        if b['l'] <= limit <= b['h']:      # order at `limit` fills when price touches it
            return j
    if ci + 1 + win <= len(bars):
        return 'nofill'
    return None


def main():
    feed = json.load(open(FEED)) if os.path.exists(FEED) else {}
    sigs_feed = feed.get('signals', []) if isinstance(feed, dict) else []
    hist = json.load(open(HIST)).get('pairs', {})
    m15 = {pk: _bars_norm(v.get('m15', [])) for pk, v in hist.items() if v.get('m15')}
    log = json.load(open(LOG)) if os.path.exists(LOG) else {'signals': {}}
    L = log['signals']

    # ---- capture: record each new intraday signal with its market price ----
    captured = 0
    for s in sigs_feed:
        sid = s.get('id')
        if not sid or sid in L:
            continue
        pk = s.get('pair'); bars = m15.get(pk)
        if not bars or s.get('entry') is None or s.get('stop') is None:
            continue
        ci = len(bars) - 1                    # latest closed m15 bar = capture point
        L[sid] = {
            'id': sid, 'pair': pk, 'dir': s.get('dir'), 'method': s.get('method'),
            'cls': s.get('cls'), 'limit_entry': s['entry'], 'stop': s['stop'],
            'market_entry': bars[ci]['c'], 'cap_ts': bars[ci]['_ts'],
            'state_at_capture': s.get('state'),
        }
        captured += 1

    # ---- score both arms on the latest m15 ----
    for rec in L.values():
        bars = m15.get(rec['pair'])
        if not bars:
            continue
        ts = [b['_ts'] for b in bars]
        ci = bisect.bisect_right(ts, rec['cap_ts']) - 1
        if ci < 0:
            continue
        d = rec['dir']
        # tag: was the signal (capture) bar a no-wick momentum candle in-trend?
        # computed once per signal; backfills records captured before this tag.
        if 'nowick_aligned' not in rec:
            rec['nowick_aligned'] = bool(0 <= ci < len(bars) and _nowick_side(bars[ci]) == d)
        # MARKET arm — enter next bar after capture
        st, o = walk(bars, ci + 1, rec['market_entry'], rec['stop'], d, HOLD)
        rec['market_status'] = st
        rec['market_r'] = o if st == 'resolved' else None
        # LIMIT arm — fill within window, then resolve
        f = limit_fill(bars, ci, rec['limit_entry'], d, FILL_WIN)
        if f == 'nofill':
            rec['limit_status'] = 'nofill'; rec['limit_r'] = None
        elif f is None:
            rec['limit_status'] = 'pending'; rec['limit_r'] = None
        else:
            st, o = walk(bars, f, rec['limit_entry'], rec['stop'], d, HOLD)
            rec['limit_status'] = st
            rec['limit_r'] = o if st == 'resolved' else None

    with open(LOG, 'w') as fp:
        json.dump(log, fp, indent=1)

    # ---- report ----
    allv = list(L.values())
    def agg(rows):
        n = len(rows); w = sum(1 for r in rows if r > 0)
        return n, (100*w/n if n else 0), (sum(rows)/n if n else 0)
    mkt = [r['market_r'] for r in allv if r.get('market_status') == 'resolved' and r.get('market_r') is not None]
    lim = [r['limit_r'] for r in allv if r.get('limit_status') == 'resolved' and r.get('limit_r') is not None]
    nofill = sum(1 for r in allv if r.get('limit_status') == 'nofill')
    filled = sum(1 for r in allv if r.get('limit_status') in ('resolved', 'expired'))
    print(f"intraday A/B tracker — captured {captured} new · {len(allv)} tracked (target 1:1)")
    mn, mw, me = agg(mkt); ln, lw, le = agg(lim)
    print(f"  MARKET arm : resolved={mn:>3}  WR={mw:>5.1f}%  exp={me:+.3f}R")
    print(f"  LIMIT  arm : resolved={ln:>3}  WR={lw:>5.1f}%  exp={le:+.3f}R  (fill rate {100*filled/max(1,filled+nofill):.0f}%, {nofill} no-fill/missed)")
    if mn and ln:
        print(f"  -> market minus limit: {me-le:+.3f}R/trade  (positive = market entry wins)")
    # Omar no-wick FILTER split — on the market arm (the realistic-fill one).
    def res_mkt(cond):
        return [r['market_r'] for r in allv
                if r.get('market_status') == 'resolved' and r.get('market_r') is not None and cond(r)]
    al = res_mkt(lambda r: r.get('nowick_aligned') is True)
    na = res_mkt(lambda r: r.get('nowick_aligned') is False)
    an, aw, ae = agg(al); nn2, nw2, ne = agg(na)
    print(f"  no-wick FILTER (market arm): ALIGNED n={an:>3} WR={aw:>5.1f}% exp={ae:+.3f}R  |  not-aligned n={nn2:>3} WR={nw2:>5.1f}% exp={ne:+.3f}R")
    print("  (forward A/B; sample sharpens as signals resolve)")


if __name__ == '__main__':
    main()
