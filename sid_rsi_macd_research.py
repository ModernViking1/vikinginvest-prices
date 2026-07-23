"""'Sid' RSI + MACD mean-reversion (user screenshots — trades US stocks/ETFs/indices).

Step #1 RSI(14): long only if RSI < 30 (oversold); short only if RSI > 70.
Step #2 MACD(12,26,9): wait for the MACD line to cross its signal in the RSI's
  direction (bull cross for longs, bear cross for shorts) — confirmation.
Step #3 reversal pattern (H&S / double top-bottom): recommended, not required —
  tested as an OPTIONAL extra filter (a local W/M pivot near entry).
Step #4 stop: below the low made while RSI<=30 (long) / above the high while
  RSI>=70 (short). EXIT: close when RSI reaches 50 (both directions) — a DYNAMIC
  exit (not a fixed RR), so scored bar-by-bar like rsimr.

Entry next-bar open after the confirming cross, causal RSI/MACD (no lookahead),
fixed cost, chronological OOS split (both halves +), per class (INDEX highlighted
— the book's instrument set), 4h/h1/daily.

Run: python sid_rsi_macd_research.py
"""
import json, os
from collections import defaultdict
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm, precompute_rsi
from five_strategies_research import agg4h, ema, atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
CONF_WIN = 10        # bars after the RSI extreme to wait for the MACD cross
BUF = 0.10           # stop buffer beyond the oversold low / overbought high, in ATR
COOLDOWN = 3
HOLD = {'h1': 120, '4h': 90, 'daily': 45}


def macd(closes):
    e12 = ema(closes, 12); e26 = ema(closes, 26); n = len(closes)
    ml = [(e12[i] - e26[i]) if (e12[i] is not None and e26[i] is not None) else None for i in range(n)]
    sig = [None]*n; k = 2/(9+1); e = None
    for i in range(n):
        if ml[i] is None:
            continue
        e = ml[i] if e is None else ml[i]*k + e*(1-k)
        sig[i] = e
    return ml, sig


def scan(bars, tf, store, cls, store_cls, use_rev):
    n = len(bars); closes = [b['c'] for b in bars]
    rsi = precompute_rsi(closes, 14); ml, sig = macd(closes)
    last = -1; i = 30
    while i < n - 1:
        if i <= last or rsi[i] is None:
            i += 1; continue
        d = None
        if rsi[i] < 30:
            d = 'bull'
        elif rsi[i] > 70:
            d = 'bear'
        if d is None:
            i += 1; continue
        # track the extreme (low while oversold / high while overbought) and wait for the cross
        ext = bars[i]['l'] if d == 'bull' else bars[i]['h']
        ei = None
        for j in range(i, min(i + CONF_WIN, n - 1)):
            ext = min(ext, bars[j]['l']) if d == 'bull' else max(ext, bars[j]['h'])
            if ml[j] is None or sig[j] is None or ml[j-1] is None or sig[j-1] is None:
                continue
            if d == 'bull' and ml[j-1] <= sig[j-1] and ml[j] > sig[j] and rsi[j] is not None and rsi[j] < 50:
                ei = j + 1; break
            if d == 'bear' and ml[j-1] >= sig[j-1] and ml[j] < sig[j] and rsi[j] is not None and rsi[j] > 50:
                ei = j + 1; break
        if ei is None:
            i += 1; continue
        # optional reversal-pattern confirmation: a local swing turn in trade dir near entry
        if use_rev:
            w = bars[max(0, ei-4):ei]
            if d == 'bull' and not (len(w) >= 3 and w[-1]['c'] > w[-1]['o']):
                i += 1; continue
            if d == 'bear' and not (len(w) >= 3 and w[-1]['c'] < w[-1]['o']):
                i += 1; continue
        a = atr(bars, 14, ei-1) or 0.0
        entry = bars[ei]['o']
        stop = (ext - BUF*a) if d == 'bull' else (ext + BUF*a)
        R = abs(entry - stop)
        if R <= 0 or (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry):
            i += 1; continue
        o = None
        for k in range(ei, min(ei + HOLD[tf], n)):
            if d == 'bull':
                if bars[k]['l'] <= stop:
                    o = (stop - entry)/R; break
                if rsi[k] is not None and rsi[k] >= 50:
                    o = (bars[k]['c'] - entry)/R; break
            else:
                if bars[k]['h'] >= stop:
                    o = (entry - stop)/R; break
                if rsi[k] is not None and rsi[k] <= 50:
                    o = (entry - bars[k]['c'])/R; break
        if o is None:
            i += 1; continue
        ts = bars[ei]['_ts']; net = o - cost(o, entry, R)
        store[tf].append((ts, net)); store_cls[cls][tf].append((ts, net))
        last = ei + COOLDOWN; i = last + 1
    return


def line(label, rows):
    rows = sorted(rows); seq = [r for _, r in rows]
    n, w, e = agg(seq); mid = len(rows)//2
    _, _, eh = agg([r for _, r in rows[:mid]]); _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"  {label:<16} n={n:>4} WR={w:>5.1f}% exp={e:>+7.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(use_rev, tag):
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    store = defaultdict(list); store_cls = defaultdict(lambda: defaultdict(list)); npairs = 0
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        cls = PAIR_CLASS.get(pk)
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        if len(h1) < 400 or len(daily) < 80:
            continue
        npairs += 1
        for tf, bars in {'h1': h1, '4h': agg4h(h1), 'daily': daily}.items():
            if len(bars) < 150:
                continue
            scan(bars, tf, store, cls, store_cls, use_rev)
    print(f"\n===== {tag} — {npairs} pairs, RSI-50 dynamic exit, OOS =====")
    for tf in ('daily', '4h', 'h1'):
        line(f"{tf}", store[tf])
    print("  -- per class (daily) --")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"  {c}", store_cls[c]['daily'])
    print("  -- per class (4h) --")
    for c in ['comm', 'crypto', 'index', 'major', 'minor']:
        line(f"  {c}", store_cls[c]['4h'])


def main():
    run(False, "CORE (RSI<30/>70 + MACD cross)")
    run(True, "CORE + reversal-pattern filter")


if __name__ == '__main__':
    main()
