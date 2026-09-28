"""Equity live-signal feed — holygrail_eq_m15 (demo pilot) → equity-signals.json for the cTrader cBot.

Promotion-ready per the weekly observer review (n=191, +0.384R, both OOS halves +; survives its top
pair — ex-amzn +0.235R on n=160). Equities aren't on the swing/OANDA cBot, so this is a SEPARATE
feed + cBot path. DEMO-ONLY to start (forward-confirm real fills before any live capital), mirroring
how every strategy was piloted.

Reuses the production detector verbatim — unified_shadow_harness._mw_signals(..., _holygrail_sig) on
each symbol's m15 — so the live signal is identical to the backtest. holygrail_eq_m15 exits on a
TRAILING stop (arm at +1R, ride 1R behind the peak, 200-bar horizon), NOT a fixed target, so the feed
carries the trail params and the cBot trails.

Emits only a FRESH signal (entry on the latest closed bar, within FRESH_MIN) during US cash hours.

  python build_equity_signals.py --hist equity-ohlc.json --out equity-signals.json
"""
import argparse, json, datetime as dt
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

EQUITY_SYMBOLS = ['aapl', 'nvda', 'tsla', 'msft', 'amzn']
# cTrader broker symbol map — FILL IN THE EXACT NAMES from your broker's symbol list before going live.
# (Placeholders are the bare tickers; many cTrader brokers use e.g. "AAPL.US" / "US_AAPL".)
BROKER_SYMBOL = {'aapl': 'AAPL', 'nvda': 'NVDA', 'tsla': 'TSLA', 'msft': 'MSFT', 'amzn': 'AMZN'}
FRESH_MIN = 45                 # only emit a signal whose entry bar closed within this many minutes
US_OPEN_H, US_CLOSE_H = 14, 21  # US cash session ~14:30–21:00 UTC (we gate 14–21 for the m15 grid)
TRAIL_ARM, TRAIL_DIST, TRAIL_HOLD = H.TRAIL_ARM, H.TRAIL_DIST, H.TRAIL_HOLD


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='equity-ohlc.json')
    ap.add_argument('--out', default='equity-signals.json')
    ap.add_argument('--demo', default='1')      # 1 = demo_only (pilot); set 0 only after live-confirm
    args = ap.parse_args()
    demo = args.demo != '0'
    now = dt.datetime.now(dt.timezone.utc); now_ms = int(now.timestamp() * 1000)

    try:
        pairs = json.load(open(args.hist)).get('pairs', {})
    except Exception as e:
        json.dump({'schema_version': 1, 'generated': now.isoformat(), 'error': f'no equity data: {e}',
                   'signals': []}, open(args.out, 'w'), indent=1)
        print(f'no equity data ({e}) — wrote empty feed'); return

    out_sigs = []
    for pk in EQUITY_SYMBOLS:
        m15 = _bars_norm(pairs.get(pk, {}).get('m15') or [])
        if len(m15) < 400:
            continue
        last_ts = m15[-1]['_ts']
        raw = H._mw_signals(m15, pk, 'holygrail_eq_m15', 'm15', H._holygrail_sig)
        if not raw:
            continue
        s = max(raw, key=lambda x: x['entry_ts'])          # most recent signal for this symbol
        age_min = (last_ts - s['entry_ts']) / 60.0
        if age_min > FRESH_MIN:                            # not fresh → nothing actionable now
            continue
        hh = dt.datetime.utcfromtimestamp(s['entry_ts']).hour
        if not (US_OPEN_H <= hh < US_CLOSE_H):             # only US cash session
            continue
        entry, stop, d = s['entry'], s['stop'], s['dir']
        R = abs(entry - stop)
        if R <= 0:
            continue
        out_sigs.append({
            'id': f"holygrail_eq_m15:{pk}:{int(s['entry_ts'])}",
            'pair': pk, 'sym': BROKER_SYMBOL.get(pk, pk.upper()), 'cls': 'equity',
            'strategy': 'holygrail_eq_m15', 'dir': d, 'state': 'armed',
            'entry': round(entry, 4), 'stop': round(stop, 4),
            'exit_mode': 'trail', 'trail_arm_r': TRAIL_ARM, 'trail_dist_r': TRAIL_DIST,
            'trail_hold_bars': TRAIL_HOLD, 'tf': 'm15',
            'entry_ts_ms': int(s['entry_ts'] * 1000), 'age_min': round(age_min, 1),
            'source': 'server-detector', 'demo_only': demo,
        })

    doc = {'schema_version': 1, 'generated': now.isoformat(), 'generated_ms': now_ms,
           'session': f'{US_OPEN_H}-{US_CLOSE_H}UTC', 'demo_only_default': demo,
           'note': 'holygrail_eq_m15 equity pilot — trailing exit (arm +1R, trail 1R, 200-bar horizon). '
                   'Fill BROKER_SYMBOL with exact cTrader names before live.',
           'counts': {'emitted': len(out_sigs)}, 'signals': out_sigs}
    json.dump(doc, open(args.out, 'w'), indent=1)
    print(f"wrote {args.out} — {len(out_sigs)} fresh signal(s) [demo_only={demo}]")
    for s in out_sigs:
        print(f"  {s['sym']:6} {s['dir']:4} entry {s['entry']} stop {s['stop']} (age {s['age_min']}m)")


if __name__ == '__main__':
    main()
