"""Live swing-signal feed for the ISOLATED demo swing cBot.

Runs the two validated swing edges on the latest published data and emits the
FRESH, actionable ones to swing-signals.json for a separate cBot to market-execute
on a demo account:
  s5_rsi   = Multi-TF Confluence, 4H RSI-50-cross trigger   (best edge)
  hs_macro = H&S fired against a high-confidence macro-EW read

A signal is 'fresh' when its entry bar is within FRESH_HOURS of the data edge —
i.e., it just triggered, so the demo cBot enters at market now. The cBot dedups
on `id` and ignores anything past `expiry_ts`.

Schema is MARKET-ENTRY oriented: the feed supplies the STOP LEVEL + direction +
reward:risk; the cBot computes entry (market fill), R = |fill-stop|, target =
fill ± rr*R, and volume from r_pct. This keeps swing execution correct without
inheriting the intraday cBot's pullback-limit / 45-min-expiry logic.

Writes swing-signals.json ONLY (nothing the intraday cBot or dashboard reads).
"""
import json, os
from datetime import datetime, timezone
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm
from unified_shadow_harness import detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb, detect_s5_rsi_wide

_HERE = os.path.dirname(os.path.abspath(__file__))   # repo root — works in CI and locally
HIST = os.path.join(_HERE, 'historical-ohlc.json')
OUT = os.path.join(_HERE, 'swing-signals.json')
FRESH_HOURS = 24          # look-back window for emitting signals (>= data latency + feed interval)
EXPIRY_HOURS = 12         # a signal is valid to fill for this long after its bar (tolerates data-publish lag)
RR = 2.0
R_PCT = 1.0               # % of demo balance risked per swing trade
# Validated class scope: S5-rsi positive on all classes; H&S weak on minor.
HS_CLASSES = {'comm', 'crypto', 'index', 'major'}
S5_CLASSES = {'comm', 'crypto', 'index', 'major', 'minor'}
# Order Blocks (2026-07-11) — added as a 4th OBSERVED candidate on the demo cBot.
# Marginal edge (daily 1:2 +0.09R, 4/6 walk-forward folds); universe-wide, daily.
OB_CLASSES = {'comm', 'crypto', 'index', 'major', 'minor'}
# Trendline break-and-retest w/ no-wick confirmation (2026-07-14) — 5th OBSERVED
# candidate. 4H only; +0.26R, 5/6 walk-forward folds, robust to parameter sweeps.
# Positive on comm/crypto/index/minor; majors negative (thin) — excluded.
TL_CLASSES = {'comm', 'crypto', 'index', 'minor'}
# Elliott wave-5 pullback (Bratby Trade-the-Fifth) (2026-07-15) — 6th OBSERVED
# candidate, WEAKEST. Aggregate 4H is only breakeven and it failed walk-forward
# universe-wide; kept ONLY because comm/crypto at 4H were positive on both OOS
# halves. Scoped to those two classes; observe on demo, don't trust the backtest.
W5PB_CLASSES = {'comm', 'crypto'}
# s5_rsi + WIDE Bollinger-bandwidth gate (2026-07-15) — 7th OBSERVED candidate.
# A SUBSET of s5_rsi (same class scope): backtest ~doubles s5_rsi expectancy
# (+0.48R -> +0.94R), 6/6 walk-forward folds, robust to BB params. Runs in
# parallel to plain s5_rsi; wide setups therefore emit under BOTH tags (demo
# double-places those overlaps — acceptable observation artifact).
S5W_CLASSES = {'comm', 'crypto', 'index', 'major', 'minor'}


def main():
    d = json.load(open(HIST)); pairs = d.get('pairs', {})
    data_end = 0
    for pk in pairs:
        h1 = pairs[pk].get('h1', [])
        if h1:
            b = _bars_norm(h1)
            if b:
                data_end = max(data_end, b[-1]['_ts'])
    fresh_after = data_end - FRESH_HOURS * 3600

    rows = []
    for pk in [x for x in PAIR_CLASS if x in pairs]:
        h1 = _bars_norm(pairs[pk].get('h1', [])); daily = _bars_norm(pairs[pk].get('daily', []))
        draw = pairs[pk].get('daily', [])
        if len(h1) < 400 or len(daily) < 80:
            continue
        cls = PAIR_CLASS.get(pk)
        found = []
        if cls in S5_CLASSES:
            found += detect_s5(pk, h1, daily, 'rsi')
        if cls in HS_CLASSES:
            found += detect_hs(pk, h1, daily, draw)
        if cls in OB_CLASSES:
            found += detect_ob(pk, h1, daily)
        if cls in TL_CLASSES:
            found += detect_tl(pk, h1, daily)
        if cls in W5PB_CLASSES:
            found += detect_w5pb(pk, h1, daily)
        if cls in S5W_CLASSES:
            found += detect_s5_rsi_wide(pk, h1, daily)
        for s in found:
            if s['entry_ts'] < fresh_after:
                continue
            sid = f"{s['strategy']}:{pk}:{int(s['entry_ts'])}"
            rows.append({
                'id': sid,
                'strategy': s['strategy'],
                'pair': pk,
                'symbol': pk.upper(),
                'class': cls,
                'dir': s['dir'],
                'stop': round(s['stop'], 8),
                'ref_entry': round(s['entry'], 8),   # reference only; cBot enters at market
                'rr': RR,
                'r_pct': R_PCT,
                'entry_mode': 'market',
                'trigger_ts': int(s['entry_ts']),
                'created_ts': int(data_end),
                'expiry_ts': int(s['entry_ts'] + EXPIRY_HOURS * 3600),
                'state': 'triggered',
            })

    # newest first, stable
    rows.sort(key=lambda r: (-r['trigger_ts'], r['id']))
    out = {
        'schema_version': 1,
        'generated': datetime.now(timezone.utc).isoformat() if False else None,  # stamped by CI env below
        'data_end_ts': int(data_end),
        'count': len(rows),
        'signals': rows,
    }
    # deterministic 'generated' from data_end so re-runs on identical data don't churn
    out['generated'] = datetime.fromtimestamp(data_end, timezone.utc).isoformat()
    with open(OUT, 'w') as f:
        json.dump(out, f, indent=1)
    print(f"swing-signals.json: {len(rows)} fresh signal(s) (data_end {int(data_end)}, fresh window {FRESH_HOURS}h)")
    for r in rows[:20]:
        print(f"  {r['id']:<40} {r['dir']:<4} stop={r['stop']} rr={r['rr']} exp={r['expiry_ts']}")


if __name__ == '__main__':
    main()
