"""Re-run the existing gold strategies on TRUE 5-minute data — does the finer
timeframe reveal an edge the higher timeframes missed (especially the FAILURE)?

Reuses the validated gold detectors from gold_strategies_research.py — the ones
defined purely in bars, so the pattern is identical, just at 5-minute resolution:
  #2 Range rejection  — the one that FAILED on h1/h4 (headline: does m5 rescue it?)
  #3 Breakout (gbreak) — passes on h1; does the edge survive at m5?
  #7 Fibonacci pullback — passes on h4/h1; at m5?
  #9 Intraday scalp    — m15+h1 in the original; here m5 entries + m15 trend filter.

Fed the real XAU_USD m5 series (gold-m5-ohlc.json). #1 trend / gtrend are swing
(H4) strategies — resampling m5 up to H4 just reproduces data we already hold, so
they're not intraday-m5 candidates and are omitted. Market fills, dealing cost,
fixed-RR brackets, chronological OOS (both halves + and n>=40 = PASS).

Run: python gold_m5_strategies_research.py
"""
import json
import os
from backtest_rsi_per_class import _bars_norm
from gold_strategies_research import s2_range, s3_breakout, s7_fib, s9_scalp, report

M5F = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gold-m5-ohlc.json')


def resample(bars, factor):
    """Aggregate `factor` consecutive m5 bars into one higher-TF bar (m5->m15=3, ->h1=12)."""
    out = []
    for i in range(0, len(bars) - factor + 1, factor):
        g = bars[i:i + factor]
        out.append({'_ts': g[0]['_ts'], 'o': g[0]['o'], 'h': max(x['h'] for x in g),
                    'l': min(x['l'] for x in g), 'c': g[-1]['c'],
                    'v': sum((x.get('v', 0) or 0) for x in g)})
    return out


def main():
    if not os.path.exists(M5F):
        print("no gold-m5-ohlc.json — run the gold-m5-fetch workflow first"); return
    m5 = _bars_norm(json.load(open(M5F))['pairs']['xauusd']['m5'])
    m15 = resample(m5, 3)
    print("=" * 92)
    print(f"GOLD strategies re-run on TRUE 5-minute · m5={len(m5)} bars (resampled m15={len(m15)})")
    print("market fills · dealing cost · fixed-RR brackets · chronological OOS (both halves + = PASS)")
    print("=" * 92)

    print("\n##2 RANGE REJECTION  — the h1/h4 FAILURE — does m5 rescue it?")
    report("standalone m5:", s2_range(m5, look=48))
    report("+ US/London session m5:", s2_range(m5, look=48, session=True))

    print("\n##3 BREAKOUT (gbreak)  — passes on h1; at m5?")
    report("ATR-filter m5:", s3_breakout(m5, atr_filter=True))
    report("+ session m5:", s3_breakout(m5, atr_filter=True, session=True))

    print("\n##7 FIBONACCI pullback  — passes on h4/h1; at m5?")
    report("standalone m5:", s7_fib(m5))
    report("+ session m5:", s7_fib(m5, session=True))

    print("\n##9 INTRADAY SCALP  — m5 entries + m15 trend filter")
    report("session m5:", s9_scalp(m5, m15, session=True))
    report("no session m5:", s9_scalp(m5, m15, session=False))


if __name__ == '__main__':
    main()
