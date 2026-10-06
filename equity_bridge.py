"""Viking equity bridge — generate .EQ signals LOCALLY from the broker's own MT5 bars.

Runs on the Windows machine where MT5 is logged in. Pulls h1+m15 bars for the configured symbols
straight from the running terminal via the MetaTrader5 package (NO external data feed), runs the
EXACT validated .EQ detectors (reused verbatim from unified_shadow_harness — zero reimplementation,
so the live signal is identical to the backtest), and writes equity-signals.json into the terminal's
MQL5/Files, where VikingEquityEA reads it (set the EA's InpLocalFile to this filename).

This is the EUROPEAN path: TwelveData can't serve LSE/XETRA, so the broker's own bars are the
source. Demo-first. It loops forever, rebuilding the feed on a timer.

SETUP (on the Windows PC where MT5 runs):
  1. Install Python 3.11+ from python.org (tick "Add python.exe to PATH").
  2. pip install MetaTrader5 requests
  3. Put this repo on the PC (git clone, or copy the folder) — the bridge imports the detectors.
  4. With MT5 OPEN and logged in:  python equity_bridge.py
     (Keep MT5 running; the bridge talks to the live terminal.)

  python equity_bridge.py [--poll 300] [--bars 1500] [--demo 1] [--out equity-signals.json]
"""
import argparse, json, os, sys, time
import datetime as dt

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm

# local_key -> MT5 Market Watch symbol. The whole equity book — US + DE + UK — driven off the broker's
# own bars through this bridge. The EA reads this bridge's LOCAL file, so every tradeable name lives here.
SYMBOLS = {
    # US (.NAS) — driven off the broker's own bars via this bridge, same as EU/UK. The EA reads the
    # LOCAL bridge file (InpLocalFile), which takes precedence over the TwelveData CDN feed, so the US
    # names MUST be here to trade — otherwise only the EU/UK book reaches the terminal.
    'aapl': 'AAPL.NAS', 'amzn': 'AMZN.NAS', 'msft': 'MSFT.NAS', 'nvda': 'NVDA.NAS', 'tsla': 'TSLA.NAS',
    # DE (.ETR) / UK (.LSE)
    'dbk': 'DBK.ETR', 'boss': 'BOSS.ETR', 'pah3': 'PAH3.ETR', 'vowg': 'VOWG.ETR',
    'barc': 'BARC.LSE', 'ba': 'BA.LSE', 'lse': 'LSE.LSE', 'rr': 'RR.LSE', 'tsco': 'TSCO.LSE',
}
# (tag, timeframe, generator) — identical to the live equity feed's .EQ book.
STRATS = [('holygrail_eq', 'h1', H._holygrail_sig), ('twob_eq', 'h1', H._twob_sig),
          ('volbreak_eq', 'h1', H._volbreak_sig), ('holygrail_eq_m15', 'm15', H._holygrail_sig)]
FRESH_MIN = {'h1': 90, 'm15': 45}          # emit only a signal whose entry bar closed within this
TA, TD, TH = H.TRAIL_ARM, H.TRAIL_DIST, H.TRAIL_HOLD
MAGIC = 5204940                            # must match the EA's InpMagic
LEDGER = 'equity_trades_open.csv'          # the EA's open-trade ledger (ticket -> entry/SL)


def _load_ledger(path):
    """{position_ticket: {id,sym,dir,entry,sl,open_ms}} from the EA's ledger CSV."""
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path, encoding='utf-8', errors='ignore'):
        p = line.strip().split(',')
        if len(p) < 8:
            continue
        try:
            out[int(p[0])] = {'id': p[1], 'sym': p[2], 'dir': int(p[3]),
                              'entry': float(p[4]), 'sl': float(p[5]), 'lots': float(p[6]),
                              'open_ms': int(p[7]) * 1000}
        except ValueError:
            continue
    return out


def write_executions(mt5, out_dir, days=120):
    """Match the EA's open ledger to MT5's CLOSED deals (our magic) and write equity-executions.json
    with realised R-multiples — the basis for win-rate / RR. R = (exit-entry)/|entry-initSL|*dir."""
    ledger = _load_ledger(os.path.join(out_dir, LEDGER))
    now = dt.datetime.now(dt.timezone.utc)
    deals = mt5.history_deals_get(now - dt.timedelta(days=days), now) or []
    bypos = {}
    for d in deals:
        if getattr(d, 'magic', 0) != MAGIC:
            continue
        bypos.setdefault(d.position_id, []).append(d)
    execs = []
    for pid, dl in bypos.items():
        outs = [d for d in dl if d.entry == 1]          # DEAL_ENTRY_OUT -> position closed
        if not outs:
            continue
        info = ledger.get(pid)
        ins = [d for d in dl if d.entry == 0]
        entry = info['entry'] if info else (ins[0].price if ins else None)
        sl = info['sl'] if info else None
        dirn = info['dir'] if info else (1 if (ins and ins[0].type == 0) else -1)
        exit_px = outs[-1].price
        profit = sum((d.profit + d.commission + d.swap) for d in dl)
        rr = None
        if entry is not None and sl is not None and abs(entry - sl) > 0:
            rr = ((exit_px - entry) if dirn > 0 else (entry - exit_px)) / abs(entry - sl)
        sid = info['id'] if info else str(pid)
        execs.append({'id': sid, 'strategy': sid.split(':')[0],
                      'sym': (info['sym'] if info else dl[0].symbol),
                      'dir': ('bull' if dirn > 0 else 'bear'),
                      'entry': entry, 'exit': exit_px, 'init_sl': sl,
                      'realized_r': (round(rr, 4) if rr is not None else None),
                      'profit_ccy': round(profit, 2), 'closed_ms': int(outs[-1].time) * 1000})
    execs.sort(key=lambda x: x['closed_ms'])
    doc = {'generated': now.isoformat(), 'source': 'mt5-bridge', 'magic': MAGIC,
           'count': len(execs), 'executions': execs}
    with open(os.path.join(out_dir, 'equity-executions.json'), 'w', encoding='utf-8') as f:
        json.dump(doc, f, separators=(',', ':'))
    return len(execs)


def _rates_to_bars(rates):
    out = []
    for r in rates:
        t = dt.datetime.fromtimestamp(int(r['time']), dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        try:
            out.append({'t': t, 'o': float(r['open']), 'h': float(r['high']),
                        'l': float(r['low']), 'c': float(r['close']), 'v': float(r['tick_volume'])})
        except (KeyError, ValueError, TypeError):
            continue
    return out


def build_doc(mt5, bars_n, demo):
    now = dt.datetime.now(dt.timezone.utc)
    tf_map = {'h1': mt5.TIMEFRAME_H1, 'm15': mt5.TIMEFRAME_M15}
    sigs = []
    for pk, sym in SYMBOLS.items():
        if not mt5.symbol_select(sym, True):
            print(f"  {sym}: not in Market Watch — skip", flush=True); continue
        layers = {}
        for tf, mtf in tf_map.items():
            rates = mt5.copy_rates_from_pos(sym, mtf, 0, bars_n)
            if rates is None or len(rates) == 0:
                continue
            layers[tf] = _bars_norm(_rates_to_bars(rates))
        for tag, tf, gen in STRATS:
            bars = layers.get(tf) or []
            if len(bars) < 400:
                continue
            raw = H._mw_signals(bars, pk, tag, tf, gen)
            if not raw:
                continue
            s = max(raw, key=lambda x: x['entry_ts'])     # most recent signal for this symbol+strategy
            age = (bars[-1]['_ts'] - s['entry_ts']) / 60.0
            if age > FRESH_MIN.get(tf, 60):
                continue
            entry, stop, d = s['entry'], s['stop'], s['dir']
            R = abs(entry - stop)
            if R <= 0:
                continue
            sigs.append({
                'id': f"{tag}:{pk}:{int(s['entry_ts'])}", 'pair': pk, 'sym': sym, 'cls': 'equity',
                'strategy': tag, 'dir': d, 'state': 'armed',
                'entry': round(entry, 4), 'stop': round(stop, 4),
                'exit_mode': 'trail', 'trail_arm_r': TA, 'trail_dist_r': TD, 'trail_hold_bars': TH,
                'tf': tf, 'entry_ts_ms': int(s['entry_ts'] * 1000), 'age_min': round(age, 1),
                'source': 'mt5-bridge', 'demo_only': demo,
            })
    return {
        'schema_version': 2, 'generated': now.isoformat(), 'generated_ms': int(now.timestamp() * 1000),
        'broker': 'mt5', 'demo_only_default': demo, 'source': 'mt5-bridge',
        'note': 'Equity .EQ signals generated LOCALLY from broker MT5 bars via the validated '
                'detectors (holygrail_eq/_m15, twob_eq, volbreak_eq). Trailing exit (arm +1R, '
                'trail 1R, 200-bar). Demo-only pilot; read by VikingEquityEA (InpLocalFile).',
        'counts': {'emitted': len(sigs)}, 'signals': sigs,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--poll', type=int, default=600, help='seconds between rebuilds (600 = 10 min; '
                    'raised from 300 to lighten the MT5 pull load when cTrader + MT5 + this bridge '
                    'share one machine — equity setups are h1/m15 so 10-min cadence is ample)')
    ap.add_argument('--bars', type=int, default=800, help='bars per timeframe to pull (was 1500; the '
                    'detectors need >=400, so 800 keeps a safe margin while nearly halving the MT5 query '
                    'and memory load each cycle — the biggest single workload cut on a shared machine)')
    ap.add_argument('--demo', default='1', help='1 = demo_only (keep 1 for the pilot)')
    ap.add_argument('--out', default='equity-signals.json', help='filename written into MQL5/Files')
    ap.add_argument('--out-dir', default='', help='override output dir (default: terminal MQL5/Files)')
    args = ap.parse_args()
    demo = args.demo != '0'

    try:
        import MetaTrader5 as mt5
    except Exception:
        sys.exit("ERROR: MetaTrader5 package not installed. On the MT5 Windows PC run:\n"
                 "    pip install MetaTrader5 requests")

    if not mt5.initialize():
        sys.exit(f"ERROR: mt5.initialize() failed ({mt5.last_error()}). Is MT5 open and logged in?")
    ti = mt5.terminal_info()
    acct = mt5.account_info()
    out_dir = args.out_dir or (os.path.join(ti.data_path, 'MQL5', 'Files') if ti else '.')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, args.out)
    print(f"bridge up — account {getattr(acct,'login','?')} ({'DEMO' if getattr(acct,'trade_mode',0)==0 else 'LIVE/other'}), "
          f"writing {out_path} every {args.poll}s, demo_only={demo}", flush=True)
    if getattr(acct, 'trade_mode', 0) != 0 and demo:
        print("  NOTE: account is not demo — the EA's demo guard will still block live fills.", flush=True)

    try:
        while True:
            try:
                doc = build_doc(mt5, args.bars, demo)
                tmp = out_path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(doc, f, separators=(',', ':'))
                os.replace(tmp, out_path)
                n = doc['counts']['emitted']
                print(f"{dt.datetime.now():%H:%M:%S} wrote {n} signal(s)"
                      + (": " + ", ".join(f"{s['sym']}/{s['strategy']} {s['dir']}" for s in doc['signals']) if n else ""),
                      flush=True)
                try:
                    nx = write_executions(mt5, out_dir)
                    print(f"           executions log: {nx} closed trade(s)", flush=True)
                except Exception as e:
                    print(f"::warning:: executions log failed ({e})", flush=True)
            except Exception as e:
                print(f"::warning:: build cycle failed ({e}) — retrying next poll", flush=True)
            time.sleep(args.poll)
    except KeyboardInterrupt:
        print("stopping", flush=True)
    finally:
        mt5.shutdown()


if __name__ == '__main__':
    main()
