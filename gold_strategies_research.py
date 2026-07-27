"""'10 Gold Strategies' (Audacity Capital screenshots) — standalone + combined tests.

Clean-room encodings of the marketed gold playbook, all on our XAUUSD data (and
XAGUSD as a correlated second gold-complex sample for robustness). Method matches
the rest of the research suite: realistic fills (market/stop, next-bar open, NO
favourable limits), fixed dealing cost, fixed-RR brackets, chronological OOS split
(both halves must be positive), n>=40 for a PASS. Gold-only universe.

Coverage vs the 10-item list:
  1  4H trend-following (50/200 EMA, pullback-to-50EMA, choppiness filter)   TESTED
  2  Range rejection off the daily band (wick / engulf, stop beyond extreme) TESTED
  3  Breakout + volatility (close beyond level + expanding ATR)              TESTED
  4  London-NY overlap session (13-17 GMT)  -> OVERLAY, tested as a combiner
  5  (missing from screenshots)
  6  Real-yield / dollar overlay             -> NOT TESTABLE (no DXY / yields in data)
  7  Fibonacci pullback (38.2-61.8 zone, invalidate past 78.6)               TESTED
  8  Gold:silver ratio mean-reversion tilt                                   TESTED (tilt)
  9  Intraday m15 scalp (H1 trend + m15 pullback-to-MA)                      TESTED
  10 ATR position sizing                     -> RISK MGMT (R-neutral, see note)

Combined: the session overlay (#4) layered on 1/2/3/9; trend-regime (#1) x fib (#7).

Run: python gold_strategies_research.py
"""
import json, os, bisect
from collections import defaultdict
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
GOLD = 'xauusd'
SILVER = 'xagusd'
RRS = [1.5, 2.0, 3.0]
BUF = 0.25                       # stop buffer beyond the structural level, in ATR
OVERLAP_HOURS = {13, 14, 15, 16} # London-NY overlap, ~13:00-17:00 GMT (#4)


# ── shared helpers ────────────────────────────────────────────────
def hour_utc(b):
    return int((b['_ts'] // 3600) % 24)


def ema(bars, period, key='c'):
    k = 2.0 / (period + 1); out = [None] * len(bars); e = None
    for i, b in enumerate(bars):
        e = b[key] if e is None else b[key] * k + e * (1 - k)
        out[i] = e
    return out


def resample_h4(h1):
    """Group hourly bars into UTC-aligned 4h buckets (00-03,04-07,...)."""
    buckets = {}
    order = []
    for b in h1:
        bk = (b['_ts'] // (4 * 3600))
        if bk not in buckets:
            buckets[bk] = {'_ts': bk * 4 * 3600, 'o': b['o'], 'h': b['h'], 'l': b['l'], 'c': b['c']}
            order.append(bk)
        else:
            g = buckets[bk]; g['h'] = max(g['h'], b['h']); g['l'] = min(g['l'], b['l']); g['c'] = b['c']
    return [buckets[bk] for bk in order]


def walk(bars, i0, entry, stop, d, rr, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == 'bull' else entry - rr * R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop: return -1.0
            if b['h'] >= tgt: return rr
        else:
            if b['h'] >= stop: return -1.0
            if b['l'] <= tgt: return rr
    return None


def emit(bars, ei, entry, stop, d, ts, store, rrset=RRS, hold=None):
    """Score an entry across the RR set; append (ts, net_R) per RR."""
    R = abs(entry - stop)
    if R <= 0:
        return
    for rr in rrset:
        h = hold if hold is not None else int(40 * rr)
        o = walk(bars, ei, entry, stop, d, rr, h)
        if o is not None:
            store[rr].append((ts, o - cost(o, entry, R)))


def line(label, rows, rr):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    be = 100 / (1 + rr)
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"    {label:<20} n={n:>4} WR={w:>5.1f}% (be {be:.0f}%) exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def report(title, store):
    print(f"  {title}")
    for rr in RRS:
        line(f"RR{rr}", store[rr], rr)


def donchian(bars, i, look):
    """[lo,hi] over the `look` bars ending just before i (the recent range/balance).
    A 20-DAY daily envelope is useless in trending gold (~$840 wide, price never
    leaves it), so 'the current range' is a recent Donchian channel on the entry
    timeframe — the actual consolidation price is trading within."""
    if i < look:
        return None
    seg = bars[i - look:i]
    return (min(x['l'] for x in seg), max(x['h'] for x in seg))


# ── #1 · 4H trend-following (50/200 EMA, pullback-to-50, choppiness filter) ──
def s1_trend(h4, session=False, choppy_filter=True):
    e50 = ema(h4, 50); e200 = ema(h4, 200)
    store = defaultdict(list); store_all = defaultdict(list)
    for i in range(201, len(h4) - 1):
        if e50[i] is None or e200[i] is None:
            continue
        b = h4[i]
        bull = e50[i] > e200[i] and b['c'] > e50[i] and b['c'] > e200[i]
        bear = e50[i] < e200[i] and b['c'] < e50[i] and b['c'] < e200[i]
        if not (bull or bear):
            continue
        # choppiness: crosses of close vs 50EMA over last 20 bars
        if choppy_filter:
            crosses = 0
            for j in range(i - 19, i + 1):
                if e50[j] is None or e50[j - 1] is None: continue
                if (h4[j]['c'] - e50[j]) * (h4[j - 1]['c'] - e50[j - 1]) < 0:
                    crosses += 1
            if crosses > 4:
                continue
        d = 'bull' if bull else 'bear'
        # pullback INTO the 50EMA on this bar, holding in-trend on close
        touched = (b['l'] <= e50[i] <= b['h'])
        held = (b['c'] > e50[i]) if bull else (b['c'] < e50[i])
        if not (touched and held):
            continue
        ei = i + 1
        entry = h4[ei]['o']; a = atr(h4, 14, i) or 0.0
        stop = (b['l'] - BUF * a) if bull else (b['h'] + BUF * a)
        if (bull and stop >= entry) or (bear and stop <= entry):
            continue
        ts = h4[ei]['_ts']
        if not session or hour_utc(h4[ei]) in OVERLAP_HOURS:
            emit(h4, ei, entry, stop, d, ts, store)
        emit(h4, ei, entry, stop, d, ts, store_all)
    return store, store_all


# ── #2 · Range rejection off the daily band ──
def _rej(bars, i, side):
    b = bars[i]; rng = b['h'] - b['l']
    if rng <= 0: return False
    body = abs(b['c'] - b['o']); p = bars[i - 1]
    if side == 'top':
        uw = b['h'] - max(b['o'], b['c'])
        wick = uw >= 0.5 * rng
        engulf = b['c'] < b['o'] and p['c'] > p['o'] and b['c'] < p['o'] and b['o'] > p['c']
        return wick or engulf
    else:
        lw = min(b['o'], b['c']) - b['l']
        wick = lw >= 0.5 * rng
        engulf = b['c'] > b['o'] and p['c'] < p['o'] and b['c'] > p['o'] and b['o'] < p['c']
        return wick or engulf


def s2_range(bars, look=48, tol=0.12, session=False):
    store = defaultdict(list)
    for i in range(look + 2, len(bars) - 1):
        band = donchian(bars, i, look)
        if not band: continue
        lo, hi = band; width = hi - lo
        if width <= 0: continue
        b = bars[i]; a = atr(bars, 14, i) or 0.0
        near_top = b['h'] >= hi - tol * width and b['h'] <= hi + tol * width
        near_bot = b['l'] <= lo + tol * width and b['l'] >= lo - tol * width
        d = None
        if near_top and _rej(bars, i, 'top'):
            d = 'bear'; stop = hi + BUF * a
        elif near_bot and _rej(bars, i, 'bot'):
            d = 'bull'; stop = lo - BUF * a
        if not d: continue
        ei = i + 1; entry = bars[ei]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        if session and hour_utc(bars[ei]) not in OVERLAP_HOURS: continue
        emit(bars, ei, entry, stop, d, bars[ei]['_ts'], store)
    return store


# ── #3 · Breakout + volatility confirmation (close beyond band + expanding ATR) ──
def s3_breakout(bars, look=48, atr_lb=10, atr_filter=True, tight=False, session=False):
    store = defaultdict(list)
    for i in range(look + 2, len(bars) - 1):
        band = donchian(bars, i, look)
        if not band: continue
        lo, hi = band; width = hi - lo
        b = bars[i]; a = atr(bars, 14, i); a_prev = atr(bars, 14, i - atr_lb)
        if a is None or a_prev is None or a <= 0: continue
        if atr_filter and not (a > a_prev):   # require expanding volatility
            continue
        # tight prior consolidation: the pre-break range is narrow vs ATR (the
        # article's "long, tight consolidation" that runs further)
        if tight and width > 6.0 * a:
            continue
        d = None
        # stop = 1 ATR back inside the broken level (breakout invalidation)
        if b['c'] > hi:      # CLOSE (not wick) beyond the level
            d = 'bull'; stop = hi - 1.0 * a
        elif b['c'] < lo:
            d = 'bear'; stop = lo + 1.0 * a
        if not d: continue
        ei = i + 1; entry = bars[ei]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        if session and hour_utc(bars[ei]) not in OVERLAP_HOURS: continue
        emit(bars, ei, entry, stop, d, bars[ei]['_ts'], store)
    return store


# ── #7 · Fibonacci pullback (38.2-61.8 zone, invalidate past 78.6) ──
def _pivots(bars, w=5):
    hi, lo = [], []
    for i in range(w, len(bars) - w):
        seg = bars[i - w:i + w + 1]
        if bars[i]['h'] == max(x['h'] for x in seg): hi.append(i)
        if bars[i]['l'] == min(x['l'] for x in seg): lo.append(i)
    return set(hi), set(lo)


def s7_fib(bars, w=5, trend_ema=None, round_conf=False, session=False):
    piv_hi, piv_lo = _pivots(bars, w)
    store = defaultdict(list)
    e = ema(bars, trend_ema) if trend_ema else None
    for i in range(2 * w + 5, len(bars) - 1):
        b = bars[i]; a = atr(bars, 14, i) or 0.0
        # last completed up-leg: pivot low then a later pivot high, both before i
        pl = max((j for j in piv_lo if j < i - w), default=None)
        ph = max((j for j in piv_hi if j < i - w), default=None)
        # BULL continuation: low(L) earlier than high(H), price now retracing down
        if pl is not None and ph is not None and pl < ph:
            L = bars[pl]['l']; H = bars[ph]['h']; rng = H - L
            if rng > 0:
                z_far = H - 0.618 * rng; z_near = H - 0.382 * rng
                in_zone = z_far <= b['l'] <= z_near
                invalid = b['l'] < H - 0.786 * rng
                react = b['c'] > b['o']
                trend_ok = (e is None) or (e[i] is not None and b['c'] > e[i])
                rconf = (not round_conf) or any(z_far <= r <= z_near for r in _rounds(z_far, z_near))
                if in_zone and not invalid and react and trend_ok and rconf:
                    ei = i + 1; entry = bars[ei]['o']; stop = L - BUF * a
                    if stop < entry and (not session or hour_utc(bars[ei]) in OVERLAP_HOURS):
                        emit(bars, ei, entry, stop, 'bull', bars[ei]['_ts'], store)
        # BEAR continuation: high earlier than low, price retracing up
        if pl is not None and ph is not None and ph < pl:
            H = bars[ph]['h']; L = bars[pl]['l']; rng = H - L
            if rng > 0:
                z_far = L + 0.618 * rng; z_near = L + 0.382 * rng
                in_zone = z_near <= b['h'] <= z_far
                invalid = b['h'] > L + 0.786 * rng
                react = b['c'] < b['o']
                trend_ok = (e is None) or (e[i] is not None and b['c'] < e[i])
                rconf = (not round_conf) or any(z_near <= r <= z_far for r in _rounds(z_near, z_far))
                if in_zone and not invalid and react and trend_ok and rconf:
                    ei = i + 1; entry = bars[ei]['o']; stop = H + BUF * a
                    if stop > entry and (not session or hour_utc(bars[ei]) in OVERLAP_HOURS):
                        emit(bars, ei, entry, stop, 'bear', bars[ei]['_ts'], store)
    return store


def _rounds(a, b):
    lo, hi = min(a, b), max(a, b)
    k = int(lo // 50) * 50; out = []
    while k <= hi:
        if k >= lo: out.append(k)
        k += 50
    return out


# ── #8 · Gold:silver ratio mean-reversion tilt ──
def s8_ratio(gold_d, silver_d, look=60, z=1.5, fwd=20):
    # align by timestamp
    gm = {b['_ts']: b['c'] for b in gold_d}; sm = {b['_ts']: b['c'] for b in silver_d}
    ts = sorted(t for t in gm if t in sm)
    ratio = [gm[t] / sm[t] for t in ts]
    gc = [gm[t] for t in ts]; sc = [sm[t] for t in ts]
    tilt = []   # (favoured_fwd_ret, relative_ret, reverted)
    for i in range(look, len(ts) - fwd):
        win = ratio[i - look:i]; mu = sum(win) / look
        sd = (sum((x - mu) ** 2 for x in win) / look) ** 0.5
        if sd <= 0: continue
        zz = (ratio[i] - mu) / sd
        gret = gc[i + fwd] / gc[i] - 1; sret = sc[i + fwd] / sc[i] - 1
        if zz >= z:      # ratio high -> favour silver
            fav = sret; rel = sret - gret; rev = ratio[i + fwd] < ratio[i]
            tilt.append((fav, rel, rev))
        elif zz <= -z:   # ratio low -> favour gold
            fav = gret; rel = gret - sret; rev = ratio[i + fwd] > ratio[i]
            tilt.append((fav, rel, rev))
    return tilt


# ── #9 · Intraday m15 scalp (H1 trend + m15 pullback-to-MA) ──
def s9_scalp(m15, h1, fast=20, slow=50, m_ema=20, session=True):
    ef = ema(h1, fast); es = ema(h1, slow)
    h1_ts = [b['_ts'] for b in h1]
    me = ema(m15, m_ema)
    store = defaultdict(list)
    for i in range(m_ema + 2, len(m15) - 1):
        b = m15[i]
        # H1 trend at/just before this m15 bar
        hi = bisect.bisect_right(h1_ts, b['_ts']) - 1
        if hi < slow or ef[hi] is None or es[hi] is None: continue
        bull = ef[hi] > es[hi]; bear = ef[hi] < es[hi]
        if me[i] is None: continue
        d = None
        if bull and b['l'] <= me[i] and b['c'] > me[i]:
            d = 'bull'; stop = b['l'] - BUF * (atr(m15, 14, i) or 0.0)
        elif bear and b['h'] >= me[i] and b['c'] < me[i]:
            d = 'bear'; stop = b['h'] + BUF * (atr(m15, 14, i) or 0.0)
        if not d: continue
        ei = i + 1; entry = m15[ei]['o']
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        if session and hour_utc(m15[ei]) not in OVERLAP_HOURS: continue
        emit(m15, ei, entry, stop, d, m15[ei]['_ts'], store, rrset=[1.5, 2.0], hold=48)
    return store


# ── driver ────────────────────────────────────────────────────────
def load():
    d = json.load(open(HIST)); p = d.get('pairs', {})
    out = {}
    for pk in (GOLD, SILVER):
        out[pk] = {
            'm15': _bars_norm(p[pk].get('m15', [])),
            'h1': _bars_norm(p[pk].get('h1', [])),
            'daily': _bars_norm(p[pk].get('daily', [])),
        }
        out[pk]['h4'] = resample_h4(out[pk]['h1'])
    return out


def tilt_report(name, tilt):
    if not tilt:
        print(f"    {name:<20} n=0"); return
    n = len(tilt)
    favwr = 100 * sum(1 for f, _, _ in tilt if f > 0) / n
    favmu = sum(f for f, _, _ in tilt) / n
    relwr = 100 * sum(1 for _, r, _ in tilt if r > 0) / n
    relmu = sum(r for _, r, _ in tilt) / n
    revwr = 100 * sum(1 for _, _, rv in tilt if rv) / n
    print(f"    {name:<20} n={n:>4} favoured-metal fwd: WR={favwr:.0f}% mean={favmu:+.2%} | "
          f"relative(fav-other): WR={relwr:.0f}% mean={relmu:+.2%} | ratio-reverted {revwr:.0f}%")


def main():
    D = load()
    g = D[GOLD]; s = D[SILVER]
    print(f"data: XAUUSD h1={len(g['h1'])} h4={len(g['h4'])} daily={len(g['daily'])} m15={len(g['m15'])}")

    for pk in (GOLD, SILVER):
        b = D[pk]
        tag = 'GOLD (XAUUSD)' if pk == GOLD else 'SILVER (XAGUSD · robustness)'
        print(f"\n================ {tag} ================")

        print("\n#1 Trend-following (H4, 50/200 EMA, pullback-to-50):")
        st, st_all = s1_trend(b['h4'], session=False, choppy_filter=True)
        report("standalone (choppiness filter ON):", st)
        st_nf, _ = s1_trend(b['h4'], session=False, choppy_filter=False)
        report("no choppiness filter:", st_nf)
        st_s, _ = s1_trend(b['h4'], session=True, choppy_filter=True)
        report("+ #4 session overlay (13-17 GMT):", st_s)

        print("\n#2 Range rejection (recent Donchian band, H1 entry, look=48):")
        report("standalone:", s2_range(b['h1'], look=48))
        report("+ #4 session overlay:", s2_range(b['h1'], look=48, session=True))
        print("  (H4 entry, look=24:)")
        report("standalone H4:", s2_range(b['h4'], look=24))

        print("\n#3 Breakout + volatility (Donchian look=48, H1, close-beyond + expanding ATR):")
        report("ATR filter ON:", s3_breakout(b['h1'], atr_filter=True))
        report("ATR filter OFF:", s3_breakout(b['h1'], atr_filter=False))
        report("+ tight-consolidation only:", s3_breakout(b['h1'], atr_filter=True, tight=True))
        report("+ #4 session overlay:", s3_breakout(b['h1'], atr_filter=True, session=True))

        print("\n#7 Fibonacci pullback (H4, 38.2-61.8 zone):")
        report("standalone:", s7_fib(b['h4']))
        report("+ round-number confluence:", s7_fib(b['h4'], round_conf=True))
        report("+ #1 trend-regime (200EMA):", s7_fib(b['h4'], trend_ema=200))
        print("  (H1:)")
        report("standalone H1:", s7_fib(b['h1']))

        print("\n#9 Intraday scalp (m15 pullback-to-EMA20 + H1 trend):")
        report("+ #4 session (13-17 GMT):", s9_scalp(b['m15'], b['h1'], session=True))
        report("no session filter:", s9_scalp(b['m15'], b['h1'], session=False))

    print("\n================ #8 Gold:silver ratio tilt (daily) ================")
    tilt_report("z>=1.5, fwd=20d", s8_ratio(g['daily'], s['daily'], z=1.5, fwd=20))
    tilt_report("z>=2.0, fwd=20d", s8_ratio(g['daily'], s['daily'], z=2.0, fwd=20))
    tilt_report("z>=1.5, fwd=10d", s8_ratio(g['daily'], s['daily'], z=1.5, fwd=10))

    print("\nNOTES:")
    print("  #4  session overlay is reported inline on 1/2/3/9 (not a standalone signal).")
    print("  #6  real-yield/dollar overlay: NOT TESTABLE — no DXY or 10y-yield series in data.")
    print("  #10 ATR position sizing: risk-management, R-neutral by construction — it scales")
    print("      volume so a fixed % of account = 1R; it changes drawdown/breach odds, not")
    print("      per-trade expectancy (which is what these R-based tests measure).")


if __name__ == '__main__':
    main()
