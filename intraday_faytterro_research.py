"""Does requiring the faytterro (Wyckoff spring/UTAD) event at entry turn the intraday
macd-primary cohort profitable?

The live H11 tracker shows macdp trades WITH a fresh spring/UTAD event winning ~88% vs ~37%
for no-event — but that's a small, recent live sample. This backtests the same split over the
full m15 history with REALISTIC fills.

macdp core trigger (faithfully reproduced from detect_macd_primary):
  - 15m MACD(12,26,9) / signal cross gives direction (bull = cross up, bear = cross down)
  - H1 RSI(14) centerline filter (bull: h1_rsi<50, bear: h1_rsi>50)
  - MARKET entry at the cross-bar CLOSE (the honest fill — the old 'fill at the bar's low'
    limit was the entry-fill artifact that inflated the headline WR), structural stop over the
    last 8 m15 bars, fixed 1:1 target (intraday targets 1R).
Omitted vs live macdp: the 4/4 EW/TL/NW/CL confluence gate and the 4H-cloud / FX time-of-day
filters — this isolates the faytterro FILTER's incremental value on the MACD-cross population.

Faytterro split uses the shipped h11_event_aligned() (H1 RSI spring→bull / UTAD→bear in the
last 5 h1 bars) — byte-for-byte the live filter. Realistic cost, chronological OOS split,
per class. Answers: does the event-aligned cohort clear cost where the base does not?

Run: python intraday_faytterro_research.py
"""
import json, os, bisect
from collections import defaultdict
from detect_triggers import PAIR_CLASS, macd_series, h11_event_aligned
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
STRUCT = 8         # structural stop lookback (m15 bars)
HOLD = 96          # bars to resolve 1:1 (m15 -> 24h)
H1WIN = 60         # h1 closes window passed to the faytterro filter


def _ts(b):
    t = b.get('t') or ''
    # ISO 'YYYY-MM-DDTHH:MM:SS...Z' -> a sortable epoch-ish integer (UTC, no DST in-window)
    return t


def scan(pk):
    d = json.load(open(HIST))['pairs'].get(pk, {})
    m15 = _bars_norm(d.get('m15', [])); h1 = _bars_norm(d.get('h1', []))
    if len(m15) < 200 or len(h1) < 80:
        return []
    mc = [b['c'] for b in m15]
    macd_line, sig_line = macd_series(mc, 12, 26, 9)
    h1c = [b['c'] for b in h1]; h1rsi = precompute_rsi(h1c, 14)
    h1ts = [b['_ts'] for b in h1]
    out = []; last = -1
    for i in range(35, len(m15) - 1):
        if i <= last:
            continue
        m0, m1, s0, s1 = macd_line[i-1], macd_line[i], sig_line[i-1], sig_line[i]
        if None in (m0, m1, s0, s1):
            continue
        if m0 <= s0 and m1 > s1:
            d_ = 'bull'
        elif m0 >= s0 and m1 < s1:
            d_ = 'bear'
        else:
            continue
        # H1 state at the cross time
        hi = bisect.bisect_right(h1ts, m15[i]['_ts']) - 1
        if hi < 20 or h1rsi[hi] is None:
            continue
        rv = h1rsi[hi]
        if (d_ == 'bull' and rv >= 50) or (d_ == 'bear' and rv <= 50):     # RSI centerline filter
            continue
        # structural stop + 1:1 target, MARKET entry at cross close
        seg = m15[max(0, i-STRUCT):i+1]
        entry = m15[i]['c']
        if d_ == 'bull':
            stop = min(x['l'] for x in seg)
            if stop >= entry: continue
            R = entry - stop; tgt = entry + R
        else:
            stop = max(x['h'] for x in seg)
            if stop <= entry: continue
            R = stop - entry; tgt = entry - R
        # faytterro alignment (byte-for-byte live filter) on the h1 window up to the cross
        aligned = h11_event_aligned(h1c[max(0, hi-H1WIN):hi+1], d_)
        # walk forward to 1:1
        o = None
        for j in range(i+1, min(i+1+HOLD, len(m15))):
            b = m15[j]
            if d_ == 'bull':
                if b['l'] <= stop: o = -1.0; break
                if b['h'] >= tgt: o = 1.0; break
            else:
                if b['h'] >= stop: o = -1.0; break
                if b['l'] <= tgt: o = 1.0; break
        if o is None:
            continue
        last = i + 2
        out.append((m15[i]['_ts'], o - cost(o, entry, R), bool(aligned)))
    return out


def agg(seq):
    n = len(seq); w = sum(1 for x in seq if x > 0)
    return n, (100*w/n if n else 0), (sum(seq)/n if n else 0)


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]; n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<20} n={n:>4} WR={w:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    print("=" * 96)
    print("Intraday macdp core (15m MACD cross + H1 RSI) — does the faytterro event gate turn it profitable?")
    print("Realistic MARKET fills, 1:1 target. base = all signals; aligned = faytterro event; none = no event.")
    print("=" * 96)
    pairs = json.load(open(HIST))['pairs']
    byclass = defaultdict(lambda: {'base': [], 'aligned': [], 'none': []})
    allb = {'base': [], 'aligned': [], 'none': []}
    npr = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        rows = scan(pk)
        if not rows:
            continue
        npr += 1
        for ts, r, al in rows:
            byclass[cls]['base'].append((ts, r)); allb['base'].append((ts, r))
            k = 'aligned' if al else 'none'
            byclass[cls][k].append((ts, r)); allb[k].append((ts, r))
    print(f"\n{npr} pairs · cohorts: base (all) / aligned (faytterro event) / none (no event)\n")
    for c in ['crypto', 'comm', 'index', 'major', 'minor']:
        b = byclass[c]
        if not b['base']:
            continue
        print(f"  ── {c} ──")
        line('base (all)', b['base'])
        line('aligned (event)', b['aligned'])
        line('none (no event)', b['none'])
    print("\n  ── ALL classes ──")
    line('base (all)', allb['base'])
    line('aligned (event)', allb['aligned'])
    line('none (no event)', allb['none'])


if __name__ == '__main__':
    main()
