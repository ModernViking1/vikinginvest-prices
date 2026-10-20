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

# Telegram alerts show UK local time (auto-handles BST/GMT) so the timestamp matches the user's
# phone/MT5 reconciliation. Prefer the IANA zone; fall back to the machine's local tz (which is UK
# on the trading PC) if tzdata isn't installed, so this never crashes an alert.
try:
    from zoneinfo import ZoneInfo
    _UK = ZoneInfo("Europe/London")
except Exception:
    _UK = None


def _uklocal(ms):
    """Format an epoch-ms as UK local HH:MM:SS."""
    t = dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)
    t = t.astimezone(_UK) if _UK is not None else dt.datetime.fromtimestamp(ms / 1000)
    return t.strftime('%H:%M:%S')

# local_key -> MT5 Market Watch symbol. The whole equity book — US + DE + UK — driven off the broker's
# own bars through this bridge. The EA reads this bridge's LOCAL file, so every tradeable name lives here.
SYMBOLS = {
    # US (.NAS) — driven off the broker's own bars via this bridge, same as EU/UK. The EA reads the
    # LOCAL bridge file (InpLocalFile), which takes precedence over the TwelveData CDN feed, so the US
    # names MUST be here to trade — otherwise only the EU/UK book reaches the terminal.
    'aapl': 'AAPL.NAS', 'amzn': 'AMZN.NAS', 'msft': 'MSFT.NAS', 'nvda': 'NVDA.NAS', 'tsla': 'TSLA.NAS',
    # US expansion (2026-10-15) — concentrate on US where the edge + cheap commission hold. Added live
    # on demo; validate on the 3yr broker export (VikingHistoryExport InpSymUS2) before trusting.
    'pep': 'PEP.NAS', 'pypl': 'PYPL.NAS', 'aal': 'AAL.NAS', 'abnb': 'ABNB.NAS', 'wday': 'WDAY.NAS',
    'vktx': 'VKTX.NAS', 'mat': 'MAT.NAS', 'pzza': 'PZZA.NAS', 'ryaay': 'RYAAY.NAS',
    'hood': 'HOOD.NAS', 'coin': 'COIN.NAS', 'pltr': 'PLTR.NAS', 'tri': 'TRI.NAS',
    'abtc': 'ABTC.NAS',   # dropped 2026-10-15: dpz (no quote/0 bars), wmg (+0.13R weakest)
    # US NYSE expansion (2026-10-15). NB 'ban' = BA.NYSE (Boeing) — 'ba' is already BA.LSE (BAE);
    # 'brkb' = BRK-B.NYSE. .NYSE is classed US everywhere (market() + bridge _is_us). Live on demo.
    'axp': 'AXP.NYSE', 'ban': 'BA.NYSE', 'bac': 'BAC.NYSE', 'blk': 'BLK.NYSE', 'c': 'C.NYSE',
    'dis': 'DIS.NYSE', 'dow': 'DOW.NYSE', 'jpm': 'JPM.NYSE', 'ko': 'KO.NYSE', 'lly': 'LLY.NYSE',
    'ma': 'MA.NYSE', 'mcd': 'MCD.NYSE', 'ms': 'MS.NYSE', 'nke': 'NKE.NYSE', 'pfe': 'PFE.NYSE',
    'brkb': 'BRK-B.NYSE', 'uber': 'UBER.NYSE', 'cof': 'COF.NYSE', 'acn': 'ACN.NYSE', 'dal': 'DAL.NYSE',
    'fis': 'FIS.NYSE', 'gm': 'GM.NYSE', 'ice': 'ICE.NYSE', 'nclh': 'NCLH.NYSE', 'snow': 'SNOW.NYSE',
    'bx': 'BX.NYSE', 'kkr': 'KKR.NYSE', 'levi': 'LEVI.NYSE', 'khc': 'KHC.NYSE',
    # dropped 2026-10-15: gty (+0.13R weakest), vsxy (never exported — unvalidated)
    # ── Europe + HK CUT from LIVE 2026-10-19 (cross-region demo review, equity_report.py) ──────
    # The demo record made Europe the ENTIRE book-level loss: NET DE -12.5k, UK -12.6k, FR -5.5k
    # vs US +13.6k, JP +1.4k (book -15.8k = all EU). Driver is COMMISSION on throttled-tiny
    # positions: UK median risk 0.004% (~$40) but ~$68 commission/trade, so cost > the whole risk
    # budget. DE/UK are also negative on GROSS (no price edge, sizing up can't save them); FR is
    # price-breakeven (+60 gross) but -5.5k on commission alone. HK cut too (live -188, negligible,
    # despite a marginal +0.24R backtest edge — the live cost structure doesn't support it). Trade
    # live on US (the engine) + JP (+0.32R live, monitored). Removed names stay in the
    # export/backtest universe (VikingHistoryExport) for monitoring; re-enable a region only if its
    # NET (post-commission) turns positive. Open EU/HK positions are left to the EA's trailing exit
    # (we stop OPENING there, we don't force-close). To re-enable, uncomment the region.
    # DE (.ETR):  'dbk':'DBK.ETR','boss':'BOSS.ETR','pah3':'PAH3.ETR','vowg':'VOWG.ETR','bayn':'BAYN.ETR','cbk':'CBK.ETR','mbg':'MBGn.ETR',
    # UK (.LSE):  'barc':'BARC.LSE','ba':'BA.LSE','lse':'LSE.LSE','rr':'RR.LSE','tsco':'TSCO.LSE','aml':'AML.LSE','bkg':'BKG.LSE','ezj':'EZJ.LSE','mks':'MKS.LSE','rdsb':'RDSB.LSE','nwg':'NWG.LSE',
    # FR (.PAR):  'bnp':'BNP.PAR','cap':'CAP.PAR','ca':'CA.PAR','bn':'BN.PAR','cdi':'CDI.PAR',
    # HK (.HK):   'hk1288':'1288.HK','hk9898':'9898.HK','hk0003':'0003.HK',
    # JP (.TSE) — KEPT (live +0.32R net; 3yr export +0.53R, both OOS+).
    'jp7267': '7267.TSE', 'jp8306': '8306.TSE', 'jp7974': '7974.TSE', 'jp6758': '6758.TSE',
    # ES (.MAD) HELD OUT — failed the 3yr validation. Still in export/backtest only. Do NOT trade live.
    #   'cabk': 'CABK.MAD', 'san': 'SAN.MAD',
}
# (tag, timeframe, generator) — identical to the live equity feed's .EQ book.
STRATS = [('holygrail_eq', 'h1', H._holygrail_sig), ('twob_eq', 'h1', H._twob_sig),
          ('volbreak_eq', 'h1', H._volbreak_sig), ('holygrail_eq_m15', 'm15', H._holygrail_sig),
          ('turtle_soup_eq', 'h1', H._turtlesoup_sig), ('turtle_soup_eq_m15', 'm15', H._turtlesoup_sig),
          ('threepush_eq', '4h', H.threepush_core)]
FRESH_MIN = {'h1': 90, 'm15': 45, '4h': 270}   # emit only a signal whose entry bar closed within this
                                           #   (4h = one bar + buffer; its signals also carry a wider
                                           #   max_age_min so the EA's 120-min cap doesn't drop them)
TA, TD, TH = H.TRAIL_ARM, H.TRAIL_DIST, H.TRAIL_HOLD
MAGIC = 5204940                            # must match the EA's InpMagic
LEDGER = 'equity_trades_open.csv'          # the EA's open-trade ledger (ticket -> entry/SL)

# ── Earnings blackout (2026-10-16; DISABLED 2026-10-19) ────────────────────────
# DISABLED by the precise full-universe study (precise_earnings_study.py, 152k trades / 25 US
# names, real yfinance calendar, gap-aware exit). Verdict: the PRE=2 rule (a) COSTS the book
# ~+161R of mean — the entries it removes are marginally POSITIVE (+0.026R), and holding through
# a report is actually the highest-expectancy cohort (+0.202R) for this trend book — and (b) does
# NOT reduce the worst-case tail: the catastrophic gap-through (-541R in the backtest) is a
# NON-earnings event, untouched by the blackout. Gap-through risk is handled instead by the EA's
# InpMaxLossPct=4% backstop, which catches EVERY gap regardless of earnings. (The original -235R
# rationale came from a >=4% price-gap PROXY that mislabelled favourable continuation gaps.)
# The fetcher/CI stay live so earnings-dates.json keeps refreshing for study; flip ENABLED back
# on only if a later analysis changes the verdict.
_HERE = os.path.dirname(os.path.abspath(__file__))
EARNINGS_FILE = os.path.join(_HERE, 'earnings-dates.json')
EARNINGS_BLACKOUT_ENABLED = False     # see block comment — off on full-universe evidence
EARNINGS_PRE_DAYS = 2      # no new entry within this many days BEFORE earnings (unused while disabled)
EARNINGS_POST_DAYS = 0     # the gap is at the report; entering after it is fine (macro 'post' was OK)


def _load_earnings():
    try:
        d = json.load(open(EARNINGS_FILE)).get('symbols', {})
        out = {}
        for tk, dates in d.items():
            ts = []
            for s in dates:
                try:
                    dd = dt.datetime.fromisoformat(s)
                    if dd.tzinfo is None:
                        dd = dd.replace(tzinfo=dt.timezone.utc)
                    ts.append(dd.timestamp())
                except Exception:
                    continue
            out[tk] = sorted(ts)
        return out
    except Exception:
        return {}


_EARNINGS = _load_earnings()


def _ticker(sym):
    return sym.rsplit('.', 1)[0].replace('-', '.')      # AAPL.NAS->AAPL ; BRK-B.NYSE->BRK.B


def _in_earnings_blackout(sym, now_ts):
    if not EARNINGS_BLACKOUT_ENABLED:
        return False
    ds = _EARNINGS.get(_ticker(sym))
    if not ds:
        return False
    for e in ds:
        if e - EARNINGS_PRE_DAYS * 86400 <= now_ts <= e + EARNINGS_POST_DAYS * 86400:
            return True
    return False


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
        gross = sum(d.profit for d in dl)               # price-only P&L (account ccy)
        commission = sum(d.commission for d in dl)      # broker commission (account ccy)
        swap = sum(d.swap for d in dl)                  # overnight financing (account ccy)
        volume = sum(d.volume for d in outs)            # closed size (units)
        profit = gross + commission + swap              # net (unchanged)
        rr = None
        if entry is not None and sl is not None and abs(entry - sl) > 0:
            rr = ((exit_px - entry) if dirn > 0 else (entry - exit_px)) / abs(entry - sl)
        # Resolve the signal id + strategy. Prefer the EA ledger; if it's missing (position
        # opened before tracking, ledger lost, or a manual close on our magic), recover the id
        # from the MT5 deal COMMENT — the EA stamps "strategy:pair:ts" there (same string shown
        # on the position). Only if neither is a proper colon-delimited id do we fall back, and
        # we tag the strategy 'unknown' rather than the bare ticket number (which otherwise
        # pollutes per-strategy stats with a one-off numeric "strategy").
        sid = info['id'] if info else None
        if not sid or sid.count(':') < 2:
            for d in dl:
                c = (getattr(d, 'comment', '') or '').strip()
                if c.count(':') >= 2:
                    sid = c; break
        strat = sid.split(':')[0] if (sid and sid.count(':') >= 2) else 'unknown'
        if not sid:
            sid = str(pid)
        execs.append({'id': sid, 'strategy': strat,
                      'sym': (info['sym'] if info else dl[0].symbol),
                      'dir': ('bull' if dirn > 0 else 'bear'),
                      'entry': entry, 'exit': exit_px, 'init_sl': sl,
                      'realized_r': (round(rr, 4) if rr is not None else None),
                      'profit_ccy': round(profit, 2),
                      # cost breakdown (account ccy) — lets the review split spread/commission/swap by
                      # market. gross_ccy is price-only P&L, so R in ccy = gross_ccy/realized_r.
                      'gross_ccy': round(gross, 2), 'commission': round(commission, 2),
                      'swap': round(swap, 2), 'volume': round(volume, 2),
                      'closed_ms': int(outs[-1].time) * 1000})
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
        # earnings blackout — don't open a new position that would be held through the report's gap
        if _in_earnings_blackout(sym, now.timestamp()):
            if demo:
                print(f"  {sym}: earnings blackout ({EARNINGS_PRE_DAYS}d pre) — skip", flush=True)
            continue
        layers = {}
        for tf, mtf in tf_map.items():
            rates = mt5.copy_rates_from_pos(sym, mtf, 0, bars_n)
            if rates is None or len(rates) == 0:
                continue
            layers[tf] = _bars_norm(_rates_to_bars(rates))
        # 4h is aggregated from h1 (identical to the backtest's agg4h — never fetched separately,
        # so live 4h bar boundaries match the research exactly).
        if layers.get('h1'):
            layers['4h'] = H.agg4h(layers['h1'])
        for tag, tf, gen in STRATS:
            # threepush_eq is US-only: on the full 3yr broker export it held on US (+0.44R) but went
            # NEGATIVE on German history (-0.14R, the small-sample DE edge didn't survive). Trade it
            # on US names only; it stays in the backtest STRATS so we keep watching it elsewhere.
            if tag == 'threepush_eq' and '.NAS' not in sym and '.NYSE' not in sym:
                continue
            bars = layers.get(tf) or []
            min_bars = 150 if tf == '4h' else 400
            if len(bars) < min_bars:
                continue
            raw = H._mw_signals(bars, pk, tag, tf, gen)
            # SMA200 trend-alignment gate (validated +0.092R/trade over 3yr, both OOS, every market
            # + strategy). Applies to the MW setups; threepush_eq (4h) is outside that study.
            if tag != 'threepush_eq':
                raw = H.trend_filter_signals(bars, raw)
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
                # a 4h signal sits within a 240-min bar, so it needs a wider age cap than the EA's
                # default 120 min — otherwise it alerts but is rejected before it can fill.
                'max_age_min': 270 if tf == '4h' else 120,
                'source': 'mt5-bridge', 'demo_only': demo,
            })
        # fib_eq (2026-10-15) — trend-gated golden-zone fib (Denis/Dantev), trailing exit.
        # Validated on the 3yr broker export (equity_dantev_fib.py): trend-gated golden-zone with
        # our native trailing runner PASSES on US/UK/FR h1 (+0.226/+0.175/+0.115R, both OOS +) and
        # US 4h (+0.318R). DE marginal, JP/HK thin, ES held out — excluded. The SMA50>100>200 trend
        # gate is baked into the detector. Market-scoped here by symbol suffix.
        _is_us = ('.NAS' in sym or '.NYSE' in sym)
        _fib_tfs = (['h1', '4h'] if _is_us else (['h1'] if ('.LSE' in sym or '.PAR' in sym) else []))
        for tf in _fib_tfs:
            bars = layers.get(tf) or []
            if len(bars) < (250 if tf == '4h' else 400):
                continue
            raw = H._fib_eq_signals(pk, bars, tf)
            if not raw:
                continue
            s = max(raw, key=lambda x: x['entry_ts'])
            age = (bars[-1]['_ts'] - s['entry_ts']) / 60.0
            if age > FRESH_MIN.get(tf, 60):
                continue
            entry, stop, d = s['entry'], s['stop'], s['dir']
            R = abs(entry - stop)
            if R <= 0:
                continue
            sigs.append({
                'id': f"fib_eq:{pk}:{int(s['entry_ts'])}", 'pair': pk, 'sym': sym, 'cls': 'equity',
                'strategy': 'fib_eq', 'dir': d, 'state': 'armed',
                'entry': round(entry, 4), 'stop': round(stop, 4),
                'exit_mode': 'trail', 'trail_arm_r': TA, 'trail_dist_r': TD, 'trail_hold_bars': TH,
                'tf': tf, 'entry_ts_ms': int(s['entry_ts'] * 1000), 'age_min': round(age, 1),
                'max_age_min': 270 if tf == '4h' else 120,
                'source': 'mt5-bridge', 'demo_only': demo,
            })
        # ma_pull_eq (2026-10-19) — 9EMA/20SMA pullback-runner, trailing exit. US m15 + h1,
        # DEMO-ONLY pilot (demo_only=True regardless of --demo): additive vs the existing m15 book
        # (+0.262R on the 59% non-overlapping, both OOS halves +). Let live demo fills validate it.
        for tf in (['m15', 'h1'] if _is_us else []):
            bars = layers.get(tf) or []
            if len(bars) < 400:
                continue
            raw = H._ma_pull_signals(pk, bars, tf)
            if not raw:
                continue
            s = max(raw, key=lambda x: x['entry_ts'])
            age = (bars[-1]['_ts'] - s['entry_ts']) / 60.0
            if age > FRESH_MIN.get(tf, 60):
                continue
            entry, stop, d = s['entry'], s['stop'], s['dir']
            R = abs(entry - stop)
            if R <= 0:
                continue
            sigs.append({
                'id': f"ma_pull_eq:{pk}:{int(s['entry_ts'])}", 'pair': pk, 'sym': sym, 'cls': 'equity',
                'strategy': 'ma_pull_eq', 'dir': d, 'state': 'armed',
                'entry': round(entry, 4), 'stop': round(stop, 4),
                'exit_mode': 'trail', 'trail_arm_r': TA, 'trail_dist_r': TD, 'trail_hold_bars': TH,
                'tf': tf, 'entry_ts_ms': int(s['entry_ts'] * 1000), 'age_min': round(age, 1),
                'max_age_min': 120,
                'source': 'mt5-bridge', 'demo_only': True,
            })
    return {
        'schema_version': 2, 'generated': now.isoformat(), 'generated_ms': int(now.timestamp() * 1000),
        'broker': 'mt5', 'demo_only_default': demo, 'source': 'mt5-bridge',
        'note': 'Equity .EQ signals generated LOCALLY from broker MT5 bars via the validated '
                'detectors (holygrail_eq/_m15, twob_eq, volbreak_eq, turtle_soup_eq/_m15, fib_eq). Trailing '
                'exit (arm +1R, trail 0.75R, 200-bar). Demo-only pilot; read by VikingEquityEA (InpLocalFile).',
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
# Telegram trade alerts are SCOPED (user 2026-10-20): swing/intraday alerts are off; the equity bridge
# alerts only these strategies. None = all. Set in main() from --alert-strats (default 'ma_pull_eq').
ALERT_STRATS = {'ma_pull_eq'}


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
            if ALERT_STRATS is not None and strat not in ALERT_STRATS:
                continue                       # scoped out (e.g. only ma_pull_eq alerts) — still deduped below
            side = 'SELL' if p.get('dir', 1) < 0 else 'BUY'
            oms = p.get('open_ms')
            ttxt = (f"\n🕒 opened {_uklocal(oms)} UK" if isinstance(oms, (int, float)) else "")
            _tg(f"🟢 <b>Equity ENTRY · {strat}</b>\n{p.get('sym','?')} <b>{side} {p.get('lots','?')}</b> "
                f"@ {p.get('entry','?')}\nSL {p.get('sl','?')} · trailing-runner (arm +1R, no fixed TP){ttxt}",
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
            if ALERT_STRATS is not None and e.get('strategy') not in ALERT_STRATS:
                continue                       # scoped out — still deduped below
            r = e.get('realized_r'); pnl = e.get('profit_ccy')
            side = 'SELL' if e.get('dir') == 'bear' else 'BUY'
            emoji = '✅' if (isinstance(r, (int, float)) and r > 0) else ('❌' if isinstance(r, (int, float)) else '➖')
            rtxt = f"{r:+.2f}R" if isinstance(r, (int, float)) else "—"
            pnltxt = f" · {pnl:+.2f}" if isinstance(pnl, (int, float)) else ""
            # stamp the actual fill time (UTC) so it reads unambiguously vs the phone's local
            # Telegram delivery time — the two differ by the phone's UTC offset, not a real delay.
            cms = e.get('closed_ms')
            ttxt = (f"\n🕒 closed {_uklocal(cms)} UK" if isinstance(cms, (int, float)) else "")
            _tg(f"{emoji} <b>Equity EXIT · {e.get('strategy','?')}</b>\n{e.get('sym','?')} {side} closed "
                f"<b>{rtxt}</b>{pnltxt}\nentry {e.get('entry','?')} → exit {e.get('exit','?')}{ttxt}", tg_token, tg_chat)
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
    import urllib.error
    api = f"https://api.github.com/repos/{repo}/contents/equity-executions.json"
    hdr = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
           "User-Agent": "viking-equity-bridge"}
    b64 = base64.b64encode(content).decode()
    # Retry on 409: the contents API conflicts when the file's SHA goes stale between our GET and
    # PUT — a race with main's frequent data commits (and our own prior push). Re-fetch the fresh
    # SHA and PUT again a few times before giving up (next poll would otherwise re-push anyway).
    for attempt in range(4):
        sha = None
        try:
            req = urllib.request.Request(api + "?ref=main", headers=hdr)
            with urllib.request.urlopen(req, timeout=10) as r:
                sha = json.load(r).get("sha")
        except Exception:
            sha = None                              # file may not exist yet — first push creates it
        body = {"message": "data: equity executions (bridge) [skip ci]", "content": b64, "branch": "main"}
        if sha:
            body["sha"] = sha
        try:
            req = urllib.request.Request(api, data=json.dumps(body).encode(), method="PUT", headers=hdr)
            urllib.request.urlopen(req, timeout=15)
            open(stamp, 'w').write(h)
            print("           pushed equity-executions.json", flush=True)
            return
        except urllib.error.HTTPError as e:
            if e.code == 409 and attempt < 3:
                time.sleep(1.5 * (attempt + 1))     # stale SHA / concurrent write — refetch + retry
                continue
            print(f"::warning:: equity-executions push failed (HTTP Error {e.code}: {e.reason})", flush=True)
            return
        except Exception as e:
            print(f"::warning:: equity-executions push failed ({e})", flush=True)
            return


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
    ap.add_argument('--alert-strats', default=os.environ.get('EQ_ALERT_STRATS', 'ma_pull_eq'),
                    help='comma list of equity strategies to Telegram-alert on (entry+exit). Default '
                         '"ma_pull_eq". "all" alerts every strategy. Expand as strategies qualify.')
    args = ap.parse_args()
    demo = args.demo != '0'
    global ALERT_STRATS
    _as = (args.alert_strats or '').strip().lower()
    ALERT_STRATS = None if _as in ('', 'all', '*') else set(s.strip() for s in args.alert_strats.split(',') if s.strip())

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
    _alert_scope = ('ALL strategies' if ALERT_STRATS is None else ', '.join(sorted(ALERT_STRATS)))
    print(f"  entry+exit alerts:  {('ON (Telegram) — ' + _alert_scope) if (args.tg_token and args.tg_chat) else 'OFF (set --tg-token/--tg-chat or TELEGRAM_* env)'}", flush=True)
    print(f"  trend filter:       {'ON (SMA' + str(H.TREND_SMA) + ' alignment)' if hasattr(H, 'trend_filter_signals') else 'OFF (old code — git pull)'}", flush=True)
    # Active equity strategy set (also a code-version indicator: if fib_eq is listed you're on the
    # trend-gated golden-zone build; if it's absent the clone is stale — git pull + restart).
    _fib_ok = hasattr(H, '_fib_eq_signals')
    _eq_strats = [t for t, _, _ in STRATS] + (['fib_eq'] if _fib_ok else []) + (['ma_pull_eq (demo)'] if hasattr(H, '_ma_pull_signals') else [])
    print(f"  strategies ({len(_eq_strats)}):     {', '.join(_eq_strats)}", flush=True)
    print(f"  scope notes:        threepush_eq US-only"
          + ("; fib_eq US h1+4h / UK+FR h1" if _fib_ok else " (fib_eq MISSING — stale clone, git pull)"), flush=True)
    # Universe count by region — confirms the loaded symbol set at a glance (e.g. after adding names).
    from collections import Counter as _Counter
    _suf2reg = {'NAS': 'US', 'NYSE': 'US', 'ETR': 'DE', 'LSE': 'UK', 'PAR': 'FR', 'TSE': 'JP', 'HK': 'HK', 'MAD': 'ES'}
    _reg = _Counter(_suf2reg.get(v.rsplit('.', 1)[-1], v.rsplit('.', 1)[-1]) for v in SYMBOLS.values())
    print(f"  symbols ({len(SYMBOLS)}):      " + ", ".join(f"{k} {_reg[k]}" for k in sorted(_reg, key=lambda x: -_reg[x])), flush=True)
    if not EARNINGS_BLACKOUT_ENABLED:
        _eb = f"OFF (disabled by precise_earnings_study — costs mean, no tail benefit; 4% EA backstop handles gaps; {len(_EARNINGS)} tickers loaded for study)"
    elif _EARNINGS:
        _eb = f"ON ({len(_EARNINGS)} tickers, {EARNINGS_PRE_DAYS}d pre)"
    else:
        _eb = "ON but no earnings-dates.json loaded — run fetch_earnings.py in CI"
    print(f"  earnings blackout:  {_eb}", flush=True)

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
