"""VWAP mean-reversion + VWAP trend-pullback on m15 — now that we carry volume.

Session-anchored VWAP (resets each UTC day). Two classic desk strategies:

  MR   Mean-reversion: a close beyond VWAP +/- K*sigma is faded back to VWAP.
       Stop 1 ATR beyond entry, target = VWAP-at-entry, intraday hold.

  TP   Trend-pullback: while price holds one side of a rising/falling VWAP,
       buy the pullback that tags VWAP and closes back through it (mirror for
       shorts). Stop 1 ATR past VWAP, RR2, continuation hold.

sigma is the volume-weighted dispersion of typical price around VWAP, from
running sums (reset per session). Discipline as everywhere: market fills at the
signal-bar close, dealing cost, chronological OOS (BOTH halves + and n>=40 =
PASS). Split crypto (REAL Coinbase volume) vs non-crypto (OANDA TICK volume),
because VWAP is only as good as the volume behind it.

Run: python vwap_research.py
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
TF = 'm15'
K = 2.0            # sigma multiple for the MR band
WARMUP = 6         # bars into the session before VWAP is trusted
MR_HOLD = 16       # ~4h — mean-reversion should resolve intraday
TP_HOLD = 40       # continuation
RR_TP = 2.0
COOLDOWN = 2
CRYPTO = {'btcusd', 'ethusd', 'solusd', 'xrpusd', 'suiusd', 'taousd', 'nearusd'}


def _day(ts):
    return int(ts // 86400) if ts else 0   # _ts is in seconds


def session_vwap(bars):
    """Per-bar (vwap, sigma, bar-index-within-session). Resets each UTC day."""
    out = []
    cur = None
    pv = v = ptv = 0.0
    k = 0
    for b in bars:
        day = _day(b['_ts'])
        if day != cur:
            cur = day
            pv = v = ptv = 0.0
            k = 0
        tp = (b['h'] + b['l'] + b['c']) / 3.0
        vol = b.get('v', 0) or 0.0
        if vol <= 0:
            vol = 1.0                      # degenerate guard (shouldn't happen post-volume)
        pv += tp * vol
        v += vol
        ptv += tp * tp * vol
        vwap = pv / v
        var = max(ptv / v - vwap * vwap, 0.0)
        out.append((vwap, var ** 0.5, k))
        k += 1
    return out


def walk(bars, i0, entry, stop, d, target, hold):
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == 'bull':
            if b['l'] <= stop:
                return -1.0
            if b['h'] >= target:
                return (target - entry) / R
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= target:
                return (entry - target) / R
    lastc = bars[min(i0 + hold, len(bars) - 1)]['c']
    return ((lastc - entry) if d == 'bull' else (entry - lastc)) / R


def sig_mr(bars, vw):
    out = []
    last = -1
    for i in range(len(bars) - 1):
        if i <= last:
            continue
        vwap, sigma, k = vw[i]
        if k < WARMUP or sigma <= 0:
            continue
        a = atr(bars, 14, i)
        if not a or a <= 0:
            continue
        b = bars[i]
        if b['c'] > vwap + K * sigma:                 # stretched high → fade short to VWAP
            out.append((i + 1, b['c'], b['c'] + a, 'bear', vwap)); last = i + COOLDOWN
        elif b['c'] < vwap - K * sigma:               # stretched low → fade long to VWAP
            out.append((i + 1, b['c'], b['c'] - a, 'bull', vwap)); last = i + COOLDOWN
    return out


def sig_tp(bars, vw):
    out = []
    last = -1
    for i in range(1, len(bars) - 1):
        if i <= last:
            continue
        vwap, sigma, k = vw[i]
        pvwap = vw[i - 1][0]
        if k < WARMUP:
            continue
        a = atr(bars, 14, i)
        if not a or a <= 0:
            continue
        b = bars[i]
        # bullish: rising VWAP, price above it, this bar tagged VWAP and closed back above
        if vwap > pvwap and b['l'] <= vwap and b['c'] > vwap:
            entry = b['c']; stop = vwap - a
            if stop < entry:
                out.append((i + 1, entry, stop, 'bull', None)); last = i + COOLDOWN
        elif vwap < pvwap and b['h'] >= vwap and b['c'] < vwap:
            entry = b['c']; stop = vwap + a
            if stop > entry:
                out.append((i + 1, entry, stop, 'bear', None)); last = i + COOLDOWN
    return out


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = 'PASS' if (e > 0 and eh > 0 and es > 0 and n >= 40) else ('thin' if n < 40 else 'fail')
    print(f"      {label:<12} n={n:>5} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def run(d, kind):
    allrows, crypto, other = [], [], []
    for pk in d:
        bars = _bars_norm(d.get(pk, {}).get(TF, []))
        if len(bars) < 400:
            continue
        vw = session_vwap(bars)
        rows = []
        if kind == 'mr':
            for (ei, entry, stop, dr, tgt) in sig_mr(bars, vw):
                if ei >= len(bars):
                    continue
                o = walk(bars, ei, entry, stop, dr, tgt, MR_HOLD)
                if o is not None:
                    rows.append((bars[ei]['_ts'], o - cost(o, entry, abs(entry - stop))))
        else:
            for (ei, entry, stop, dr, _) in sig_tp(bars, vw):
                if ei >= len(bars):
                    continue
                R = abs(entry - stop)
                tgt = entry + RR_TP * R if dr == 'bull' else entry - RR_TP * R
                o = walk(bars, ei, entry, stop, dr, tgt, TP_HOLD)
                if o is not None:
                    rows.append((bars[ei]['_ts'], o - cost(o, entry, R)))
        (crypto if pk in CRYPTO else other).extend(rows)
        allrows.extend(rows)
    return allrows, crypto, other


def main():
    d = json.load(open(HIST))['pairs']
    print('=' * 96)
    print('VWAP strategies on m15 (session=UTC day) — market fills+cost, OOS')
    print('=' * 96)
    for kind, label in (('mr', 'VWAP mean-reversion (fade K*sigma back to VWAP)'),
                        ('tp', 'VWAP trend-pullback (buy the VWAP tag in-trend, RR2)')):
        print(f"\n===== {label} =====")
        allrows, crypto, other = run(d, kind)
        line('ALL', allrows)
        line('crypto(real)', crypto)
        line('other(tick)', other)


if __name__ == '__main__':
    main()
