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


def _tg(msg, token, chat):
    """Send a Telegram message. Best-effort, fail-open."""
    if not token or not chat:
        return
    if token.startswith("bot"):
        token = token[3:]                       # tolerate a stray 'bot' prefix (the URL adds it) -> avoids 404
    import urllib.parse, urllib.request
    data = urllib.parse.urlencode({"chat_id": chat, "text": msg, "parse_mode": "HTML",
                                   "disable_web_page_preview": "true"}).encode()
    try:
        urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=10)
    except Exception as e:
        print(f"  telegram send failed (non-fatal): {e}", flush=True)


ALERTED = 'equity-alerted.json'            # local: tickets already alerted on entry (dedup across cycles)


def _alert_new_entries(out_dir, ledger, tg_token, tg_chat):
    """Telegram one alert per NEW open in the EA ledger (mirrors the swing/intraday entry alerts).
    Seeds silently on first run so existing positions don't spam. Fail-open."""
    path = os.path.join(out_dir, ALERTED)
    try:
        seen = set(json.load(open(path)).get('tickets', []))
        seeded = True
    except Exception:
        seen, seeded = set(), False
    new = [(tk, p) for tk, p in ledger.items() if tk not in seen]
    if seeded and tg_token:
        for tk, p in sorted(new, key=lambda kv: kv[1].get('open_ms', 0)):
            strat = (p.get('id', '') or '').split(':')[0] or '?'
            side = 'SELL' if p.get('dir', 1) < 0 else 'BUY'
            _tg(f"🟢 <b>Equity ENTRY · {strat}</b>\n{p.get('sym','?')} <b>{side} {p.get('lots','?')}</b> "
                f"@ {p.get('entry','?')}\nSL {p.get('sl','?')} · trailing-runner (arm +1R, no fixed TP)",
                tg_token, tg_chat)
    try:
        json.dump({'tickets': sorted(ledger.keys())}, open(path, 'w'))
    except Exception:
        pass


CLOSED_ALERTED = 'equity-closed-alerted.json'   # local: closed trades already alerted (dedup)


def _alert_new_closes(out_dir, tg_token, tg_chat):
    """Telegram one alert per NEW closed trade (strategy, side, realised R, P&L). Reads the executions
    file write_executions just wrote. Seeds silently on first run; deduped across cycles. Fail-open."""
    path = os.path.join(out_dir, CLOSED_ALERTED)
    try:
        seen = set(json.load(open(path)).get('keys', [])); seeded = True
    except Exception:
        seen, seeded = set(), False
    try:
        execs = json.load(open(os.path.join(out_dir, 'equity-executions.json'))).get('executions', [])
    except Exception:
        execs = []
    keys, new = [], []
    for e in execs:
        cms = e.get('closed_ms')
        if cms is None:
            continue
        k = f"{e.get('id','?')}@{cms}"
        keys.append(k)
        if k not in seen:
            new.append(e)
    if seeded and tg_token:
        for e in sorted(new, key=lambda x: x.get('closed_ms', 0)):
            r = e.get('realized_r'); pnl = e.get('profit_ccy')
            side = 'SELL' if e.get('dir') == 'bear' else 'BUY'
            emoji = '✅' if (isinstance(r, (int, float)) and r > 0) else ('❌' if isinstance(r, (int, float)) else '➖')
            rtxt = f"{r:+.2f}R" if isinstance(r, (int, float)) else "—"
            pnltxt = f" · {pnl:+.2f}" if isinstance(pnl, (int, float)) else ""
            _tg(f"{emoji} <b>Equity EXIT · {e.get('strategy','?')}</b>\n{e.get('sym','?')} {side} closed "
                f"<b>{rtxt}</b>{pnltxt}\nentry {e.get('entry','?')} → exit {e.get('exit','?')}", tg_token, tg_chat)
    try:
        json.dump({'keys': sorted(set(keys))}, open(path, 'w'))
    except Exception:
        pass


def _push_executions(repo, token, out_dir):
    """Push equity-executions.json to the repo (GitHub contents API) when it CHANGES, so the
    per-strategy win/loss record is visible server-side and auto-refreshes the dashboard. Fail-open."""
    if not token or not repo:
        return
    import base64, hashlib, urllib.request
    src = os.path.join(out_dir, 'equity-executions.json')
    if not os.path.exists(src):
        return
    content = open(src, 'rb').read()
    stamp = os.path.join(out_dir, '.eqexec-pushed')
    h = hashlib.sha256(content).hexdigest()
    try:
        if open(stamp).read().strip() == h:
            return                                  # unchanged since last push — skip
    except Exception:
        pass
    api = f"https://api.github.com/repos/{repo}/contents/equity-executions.json"
    hdr = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
           "User-Agent": "viking-equity-bridge"}
    sha = None
    try:
        req = urllib.request.Request(api + "?ref=main", headers=hdr)
        with urllib.request.urlopen(req, timeout=10) as r:
            sha = json.load(r).get("sha")
    except Exception:
        sha = None                                  # file may not exist yet — first push creates it
    body = {"message": "data: equity executions (bridge) [skip ci]",
            "content": base64.b64encode(content).decode(), "branch": "main"}
    if sha:
        body["sha"] = sha
    try:
        req = urllib.request.Request(api, data=json.dumps(body).encode(), method="PUT", headers=hdr)
        urllib.request.urlopen(req, timeout=15)
        open(stamp, 'w').write(h)
        print("           pushed equity-executions.json", flush=True)
    except Exception as e:
        print(f"::warning:: equity-executions push failed ({e})", flush=True)


def _heartbeat(token, repo, acct, emitted):
    """Fire an `equity-bridge-heartbeat` repository_dispatch so the server-side watchdog can see the
    bridge/MT5 is alive (the bridge otherwise writes only to local MQL5\\Files). Best-effort, fail-open."""
    if not token or not repo:
        return
    import urllib.request
    body = json.dumps({"event_type": "equity-bridge-heartbeat",
                       "client_payload": {"acct": str(acct), "emitted": int(emitted)}}).encode()
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}/dispatches", data=body, method="POST",
                                 headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                                          "User-Agent": "viking-equity-bridge", "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  heartbeat post failed (non-fatal): {e}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--poll', type=int, default=900, help='seconds between rebuilds (900 = 15 min; '
                    'raised from 300->600->900 to lighten the MT5 pull load when cTrader + MT5 + this '
                    'bridge share one machine — equity setups are h1/m15 so a 15-min cadence is ample)')
    ap.add_argument('--bars', type=int, default=800, help='bars per timeframe to pull (was 1500; the '
                    'detectors need >=400, so 800 keeps a safe margin while nearly halving the MT5 query '
                    'and memory load each cycle — the biggest single workload cut on a shared machine)')
    ap.add_argument('--demo', default='1', help='1 = demo_only (keep 1 for the pilot)')
    ap.add_argument('--out', default='equity-signals.json', help='filename written into MQL5/Files')
    ap.add_argument('--out-dir', default='', help='override output dir (default: terminal MQL5/Files)')
    ap.add_argument('--gh-token', default=os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN', ''),
                    help='GitHub PAT (contents/repo scope) to fire a liveness heartbeat so the watchdog '
                         'can see MT5 is up. Falls back to the GH_TOKEN / GITHUB_TOKEN env var. Optional.')
    ap.add_argument('--gh-repo', default='ModernViking1/vikinginvest-prices',
                    help='owner/repo the heartbeat dispatch + executions push target')
    ap.add_argument('--tg-token', default=os.environ.get('TELEGRAM_BOT_TOKEN', ''),
                    help='Telegram bot token for entry alerts (falls back to TELEGRAM_BOT_TOKEN env). Optional.')
    ap.add_argument('--tg-chat', default=os.environ.get('TELEGRAM_CHAT_ID', ''),
                    help='Telegram chat id for entry alerts (falls back to TELEGRAM_CHAT_ID env). Optional.')
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
    print(f"  watchdog heartbeat: {'ON -> ' + args.gh_repo if args.gh_token else 'OFF (set --gh-token or GH_TOKEN to enable)'}", flush=True)
    print(f"  executions push:    {'ON -> ' + args.gh_repo if args.gh_token else 'OFF (needs --gh-token/GH_TOKEN)'}", flush=True)
    print(f"  entry+exit alerts:  {'ON (Telegram)' if (args.tg_token and args.tg_chat) else 'OFF (set --tg-token/--tg-chat or TELEGRAM_* env)'}", flush=True)

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
                # Telegram one alert per NEW entry the EA opened this cycle (mirrors swing/intraday).
                try:
                    _alert_new_entries(out_dir, _load_ledger(os.path.join(out_dir, LEDGER)),
                                       args.tg_token, args.tg_chat)
                except Exception as e:
                    print(f"::warning:: entry alert failed ({e})", flush=True)
                try:
                    nx = write_executions(mt5, out_dir)
                    print(f"           executions log: {nx} closed trade(s)", flush=True)
                    # Telegram one alert per NEW close (strategy, side, realised R, P&L).
                    _alert_new_closes(out_dir, args.tg_token, args.tg_chat)
                    # Publish the per-strategy win/loss record to the repo (on change) — server-visible
                    # + auto-refreshes the dashboard via live-book.yml.
                    _push_executions(args.gh_repo, args.gh_token, out_dir)
                except Exception as e:
                    print(f"::warning:: executions log/push failed ({e})", flush=True)
                # Liveness heartbeat so the watchdog can see MT5/the bridge is up (server-side).
                _heartbeat(args.gh_token, args.gh_repo, getattr(acct, 'login', '?'), n)
            except Exception as e:
                print(f"::warning:: build cycle failed ({e}) — retrying next poll", flush=True)
            time.sleep(args.poll)
    except KeyboardInterrupt:
        print("stopping", flush=True)
    finally:
        mt5.shutdown()


if __name__ == '__main__':
    main()
