"""Periodic review of the shadow observer strategies — promotion / drop / watch.

Reads swing-shadow-log.json and, for every tracked strategy, computes its GENUINE
FORWARD record (signals whose entry is AFTER the baseline was set — i.e. real
out-of-sample-in-real-time evidence, not in-sample backfill). Each is classified
against the promotion gate the desk agreed on:

  PROMOTE  n>=40, expectancy > 0, BOTH chronological OOS halves > 0
  DROP     n>=25 and expectancy < -0.05R (persistent forward loss)
  WATCH    everything else (still thin, or marginal — keep accruing)

Prints a ranked digest and, when TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID are set,
sends a short summary. Read-only: never writes the log. Run weekly by
observer-review.yml.

Run: python observer_review.py
"""
import json
import os
import bisect
import datetime
import urllib.request
import urllib.parse
from backtest_rsi_per_class import _bars_norm

_HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(_HERE, 'swing-shadow-log.json')
EXECS = os.path.join(_HERE, 'executions.json')

# Weekly pipeline sanity check — is every data pipe still flowing? Each entry is
# (label, file, timestamp-key, warn-if-older-than-minutes). The intraday-feed check
# is the one that would have caught the dead cron pinger (signals.json going stale)
# weeks earlier instead of only surfacing via missing fills.
_HEALTH_FILES = [
    ('intraday feed', 'signals.json',         'generated',         30),
    ('swing feed',    'swing-signals.json',    'generated',         360),
    ('shadow log',    'swing-shadow-log.json', 'last_run_data_end', 2880),
    ('price data',    'historical-ohlc.json',  'generated',         1440),
]
PROMOTE_N = 40
DROP_N = 25
DROP_EXP = -0.05
# Live-vs-model gap: a strategy that trades live (real demo fills) AND has an observer
# model record should track its model. If live expectancy runs materially BELOW the model
# once there are enough fills, that's the tell the live edge isn't matching the idealised
# backtest (slippage, feed lag/differences, or the edge decaying) — flag it.
GAP_N = 15            # min closed live fills before the gap is meaningful
GAP_TOL = 0.15       # live this far below model expectancy -> flag

# Concentration guard: a "PROMOTE" whose edge collapses without its single best pair isn't a
# portfolio edge, it's one instrument in disguise (and usually one that dies on cost or won't
# scale). But a strategy that legitimately LEANS on its best instrument (e.g. a gold-heavy
# comm edge) is still valid if the rest holds up — so we only HOLD a verdict when the edge
# genuinely doesn't survive removing the top pair, or when the "other" pairs barely traded
# (concentration that's really single-pair in disguise). A deliberately single-pair strategy
# (gbreak = gold only) never reaches the guard at all — it fires only for len(pairs) > 1.
CONC_MAX = 0.40          # top-pair share above this is "top-heavy" — annotate, and scrutinise
CONC_MIN_REMAINDER = 20  # min non-top-pair fills for the remainder edge to be trustworthy

# Strategies executing on the cBot (live/demo feed) — swing_signals PRIORITY minus DEMOTED,
# the demo pilot, plus the promoted intraday emitters (mmove_m15, absorb_btc) AND the intraday
# structural methods (fib/wick/macdp). Everything else in the log is an observer. Keep in sync
# when promoting/demoting, or a live strategy wrongly shows as "promotion-ready" (and a demoted
# one lingers as live). NOTE: this set is the complete NAMED ROSTER of what trades on the cBot;
# per-strategy live/demo/class gating is enforced in the feeds (swing_signals.DEMO_ONLY,
# build_signals_json.DEMO_ONLY_METHODS / LIVE_CLASSES), not here.
# RECONCILED 2026-11-01 (user) to the REAL live emitters — this set MUST match what the feeds emit,
# or the review mis-labels (stale 'live-vs-model' flags on long-stripped strategies, e.g.
# holygrail_cm_m15/mmove flagged for weeks after their 2026-10-20 strip). Re-add a name here ONLY when
# it is actually re-enabled in swing_signals.LIVE_STRATS or build_signals_json.
#   SWING feed   (swing_signals.LIVE_STRATS):            engulf_manip, twob_ix
#   INTRADAY feed (build_signals_json dedicated emits):  mmove_m15, absorb_btc, crt_ix
# Demoted off-live (now observers): hs, gbreak, fma_sweep_cm, mmove (swing, not in LIVE_STRATS),
# holygrail_cm_m15 (stripped 2026-10-20), plus the earlier demotes (twob/twob_cm/cam_rev/holygrail_cm/
# fred_tl/fma_gold/s5_rsi*/asianglitch/obfvg/sweepfvg_ix — all negative on the causal 3yr re-validation).
#   EQUITY book  (equity_bridge.STRATS + ma_pull/fib):   ma_pull_eq, holygrail_eq_m15,
#                                                         turtle_soup_eq, fib_eq, threepush_eq
# Added 2026-11-08 (user) — the MT5 equity book was missing here, so its live strategies wrongly
# showed as "promotion-ready observers". holygrail_eq (h1) stays OUT (dropped from live 2026-10-27).
LIVE = {'engulf_manip', 'twob_ix', 'gbreak', 'cam_rev',      # swing (OANDA cBot); gbreak 2026-11-22, cam_rev 2026-11-25 (demo-first, filtered)
        'mmove_m15', 'absorb_btc', 'crt_ix',                 # intraday cBot
        'ma_pull_eq', 'turtle_soup_eq'}  # MT5 equity book (holygrail_eq_m15 DROPPED from live 2026-11-15)
#   fib_eq / threepush_eq DROPPED from live 2026-11-12 (user) — observer-only now, so OUT of LIVE
#   (they revert to 'obs' role in the review; the shadow log keeps their forward record).


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100.0 * w / n if n else 0.0), (sum(r) / n if n else 0.0)


# ── Forward trend-filter (2026-11-25) — re-score every shadow-logged strategy under the SMA200
# directional filter now live on the swing/intraday book, so DEMOTED strategies accrue a FILTERED
# forward verdict and can re-earn promotion on the right basis (not on an unfiltered model number).
# cam_rev is the lead re-observation candidate: filtered forward flipped -0.012R -> +0.195R, both-OOS,
# n=485. OHLC from the committed historical-ohlc.json; FAIL-OPEN (keep the signal) when a pair/tf is
# not covered (e.g. the MT5 equity names), so the filter never silently drops an uncheckable strategy.
_FWD_OHLC = None
_FWD_NORM = {}

def _fwd_bars(pair, tf):
    global _FWD_OHLC
    if _FWD_OHLC is None:
        try:
            _FWD_OHLC = json.load(open(os.path.join(_HERE, 'historical-ohlc.json'))).get('pairs', {})
        except Exception:
            _FWD_OHLC = {}
    key = (pair, tf)
    if key not in _FWD_NORM:
        lay = _FWD_OHLC.get(pair) or {}
        _FWD_NORM[key] = _bars_norm(lay.get(tf) or lay.get('h1') or [])
    return _FWD_NORM[key]

def _trend_aligned_fwd(pair, tf, ets, price, direction, n=200):
    """SMA200 directional alignment for a shadow-logged signal. Fail-open (True) on no coverage."""
    b = _fwd_bars(pair, tf or 'h1')
    if len(b) < n + 1 or not price:
        return True
    i = bisect.bisect_left([x['_ts'] for x in b], ets)
    if i < n or i >= len(b):
        return True
    sma = sum(x['c'] for x in b[i - n:i]) / n
    up = b[i]['c'] > sma
    return up if direction in ('bull', 'buy', 'long') else (not up)


def classify(n, exp, eh, es):
    if n >= PROMOTE_N and exp > 0 and eh > 0 and es > 0:
        return 'PROMOTE'
    if n >= DROP_N and exp < DROP_EXP:
        return 'DROP'
    return 'WATCH'


# Both execution logs: the intraday cBot (executions.json) AND the swing cBot
# (swing-executions.json). Reading only the intraday log hid the SWING live-vs-model gap
# entirely (the swing book's real fills never reached the gap check) — fixed 2026-10-03.
EXEC_LOGS = ('executions.json', 'swing-executions.json', 'equity-executions.json')
# A single closed fill beyond +-SANE_R is a broken/garbage fill (every bracket is ~1:1..RR3),
# e.g. the gbreak -59.97R glitch on 2026-08-20. Drop these from the live stats so one corrupt
# row can't swamp a strategy's record — the raw row stays in the log for audit.
SANE_R = 6.0


def _method_of(sid, strategy, swing):
    """Strategy/method key from a signal id. Swing ids are strat:pair:ts (method = FIRST seg);
    intraday ids are (viking-)pair:ts:method (method = LAST seg)."""
    parts = (sid or '').split(':')
    if len(parts) >= 3:
        return parts[0] if swing else parts[-1]
    return strategy or ''


def live_records():
    """{method: [realized_r,...]} for closed live (demo) fills across BOTH execution logs,
    with broken fills (|R|>SANE_R) dropped."""
    out = {}
    dropped = 0
    for fn in EXEC_LOGS:
        swing = 'swing' in fn
        equity = 'equity' in fn           # MT5 book: no 'event' field (all rows closed), carries 'strategy'
        try:
            ex = json.load(open(os.path.join(_HERE, fn))).get('executions', [])
        except Exception:
            continue
        for r in ex:
            # cTrader logs tag each row with event (placed/closed/rejected); the equity log has no
            # event field — every row is a closed trade. Skip non-closed only where event exists.
            ev = r.get('event')
            if ev is not None and ev != 'closed':
                continue
            rr = r.get('realized_r')
            if rr is None:
                continue
            if abs(rr) > SANE_R:
                dropped += 1
                continue
            # equity rows carry 'strategy' directly (reliable); cTrader rows derive it from the signal id
            method = r.get('strategy') if equity else _method_of(r.get('signal_id'), r.get('strategy'), swing)
            if method:
                out.setdefault(method, []).append(rr)
    if dropped:
        print(f"(live_records: dropped {dropped} broken fill(s) with |R|>{SANE_R:.0f})")
    return out


def gap_rows(model_rows, live):
    """For each live-traded method with >=GAP_N fills, the live-vs-model expectancy gap."""
    by_st = {r['st']: r for r in model_rows}
    out = []
    for method, lr in live.items():
        ln, lwr, lexp = agg(lr)
        if ln < GAP_N:
            continue
        m = by_st.get(method)
        mexp = m['exp'] if m else None
        gap = (lexp - mexp) if mexp is not None else None
        flag = gap is not None and gap < -GAP_TOL       # live materially below model
        out.append({'st': method, 'live_n': ln, 'live_wr': lwr, 'live_exp': lexp,
                    'model_n': (m['n'] if m else 0), 'model_exp': mexp,
                    'gap': gap, 'flag': flag})
    out.sort(key=lambda x: (x['gap'] if x['gap'] is not None else 1e9))
    return out


def review():
    log = json.load(open(LOG))
    base = log.get('baseline_data_end') or 0
    end = log.get('last_run_data_end') or 0
    track = log.get('tracking', {})
    by = {}; by_pair = {}; by_f = {}
    for s in log.get('signals', {}).values():
        st = s.get('strategy')
        # Genuine forward = entry AFTER this strategy's OWN tracking start (when it was
        # added), not the global baseline. Otherwise a newly-wired strategy's entire
        # backtest history (all after the global baseline) masquerades as forward
        # evidence and would wrongly read as promotion-ready on day one.
        cutoff = track.get(st, base)
        if s.get('entry_ts', 0) <= cutoff:
            continue
        if s.get('status') != 'resolved' or 'r' not in s:
            continue
        by.setdefault(st, []).append((s['entry_ts'], s['r']))
        by_pair.setdefault(st, {}).setdefault(s.get('pair', '?'), []).append(s['r'])
        # FILTERED forward record — same signal, kept only if SMA200 trend-aligned (fail-open).
        if _trend_aligned_fwd(s.get('pair'), s.get('tf'), s['entry_ts'], s.get('entry'), s.get('dir')):
            by_f.setdefault(st, []).append((s['entry_ts'], s['r']))
    rows = []
    for st, rec in by.items():
        rec.sort(); seq = [r for _, r in rec]; n, wr, exp = agg(seq); m = len(rec) // 2
        _, _, eh = agg([r for _, r in rec[:m]]); _, _, es = agg([r for _, r in rec[m:]])
        days = int((end - track[st]) / 86400) if st in track and end else None
        verdict = classify(n, exp, eh, es)
        # CONCENTRATION GUARD (2026-09-13): a PROMOTE should be a BROAD edge. For a multi-pair
        # strategy, test whether the edge survives removing its single best pair. HOLD at WATCH
        # only when it doesn't (ex-top expectancy <= 0), or when the strategy is top-heavy AND
        # the remainder barely traded (single-pair in disguise). A strategy that legitimately
        # leans on its best instrument but still has a real, well-sampled edge on the rest KEEPS
        # PROMOTE — we just annotate the lean. Deliberately single-pair strategies never reach
        # here (len(pairs) > 1 gate), so a gold-only edge like gbreak is exempt by construction.
        conc = None
        pairs = by_pair.get(st, {})
        tot = sum(seq)
        if verdict == 'PROMOTE' and tot > 0 and len(pairs) > 1:
            ptot = {pk: sum(v) for pk, v in pairs.items()}
            toppk = max(ptot, key=ptot.get); share = ptot[toppk] / tot
            ex = [r for pk, v in pairs.items() if pk != toppk for r in v]
            ex_n, _, ex_exp = agg(ex)
            held = ex_exp <= 0 or (share > CONC_MAX and ex_n < CONC_MIN_REMAINDER)
            if held:
                verdict = 'WATCH'
                conc = {'pair': toppk, 'share': share, 'ex_exp': ex_exp, 'ex_n': ex_n, 'held': True}
            elif share > CONC_MAX:   # top-heavy but the remainder is a real edge — keep PROMOTE, flag it
                conc = {'pair': toppk, 'share': share, 'ex_exp': ex_exp, 'ex_n': ex_n, 'held': False}
        # FILTERED forward verdict (SMA200-aligned subset) — the basis for re-observing demoted strats.
        recf = sorted(by_f.get(st, [])); seqf = [r for _, r in recf]; mf = len(recf) // 2
        nf, wrf, expf = agg(seqf)
        _, _, ehf = agg([r for _, r in recf[:mf]]); _, _, esf = agg([r for _, r in recf[mf:]])
        verdictf = classify(nf, expf, ehf, esf)
        rows.append({'st': st, 'n': n, 'wr': wr, 'exp': exp, 'eh': eh, 'es': es,
                     'verdict': verdict, 'days': days, 'live': st in LIVE, 'conc': conc,
                     'nf': nf, 'expf': expf, 'ehf': ehf, 'esf': esf, 'verdictf': verdictf})
    rows.sort(key=lambda r: ({'PROMOTE': 0, 'DROP': 1, 'WATCH': 2}[r['verdict']], -r['exp']))
    return rows, base, end


def fmt_table(rows):
    out = []
    out.append(f"{'strategy':<16}{'role':<5}{'n':>4} {'WR':>5} {'exp':>8}  {'OOS halves':<18}{'verdict'}")
    for r in rows:
        role = 'live' if r['live'] else 'obs'
        td = f" {r['days']}d" if r['days'] is not None else ""
        out.append(f"{r['st']:<16}{role:<5}{r['n']:>4} {r['wr']:>4.0f}% {r['exp']:>+7.3f}R  "
                   f"[{r['eh']:>+6.3f}/{r['es']:>+6.3f}]  {r['verdict']}{td}")
    return "\n".join(out)


def fmt_filtered(rows):
    """Re-observation bench: every shadow strategy re-scored under the live SMA200 trend filter.
    Flags demoted/observer strategies the filter flips toward PROMOTE; cam_rev is the lead."""
    cand = [r for r in rows if r.get('nf', 0) >= 25]
    cand.sort(key=lambda r: -r.get('expf', 0))
    out = ["FILTERED-FORWARD (SMA200 trend filter) — re-observation bench (demoted strats re-scored):",
           f"  {'strategy':<15}{'n_f':>5}{'raw exp':>9}{'filt exp':>10}   raw->filt"]
    for r in cand:
        flip = r['verdict'] in ('DROP', 'WATCH') and r['verdictf'] == 'PROMOTE'
        lead = '   <= LEAD re-observe' if r['st'] == 'cam_rev' else ('   <= re-observe' if (flip and not r['live']) else '')
        out.append(f"  {r['st']:<15}{r['nf']:>5}{r['exp']:>+9.3f}{r['expf']:>+10.3f}   "
                   f"{r['verdict']:>7}->{r['verdictf']:<7}{lead}")
    return "\n".join(out)


def fmt_gap_table(gaps):
    if not gaps:
        return "LIVE-vs-MODEL — no live-traded method has >=%d closed fills yet." % GAP_N
    out = ["LIVE-vs-MODEL gap (live demo fills vs observer model):",
           f"{'strategy':<16}{'live n':>7} {'live exp':>9} {'model exp':>10} {'gap':>9}  flag"]
    for g in gaps:
        me = f"{g['model_exp']:>+9.3f}R" if g['model_exp'] is not None else f"{'n/a':>10}"
        gp = f"{g['gap']:>+8.3f}R" if g['gap'] is not None else f"{'n/a':>9}"
        out.append(f"{g['st']:<16}{g['live_n']:>7} {g['live_exp']:>+8.3f}R {me} {gp}  "
                   f"{'⚠ BELOW MODEL' if g['flag'] else ''}")
    return "\n".join(out)


def _ts_of(path, key):
    """Epoch seconds of a timestamp field (ISO string or epoch) in a repo JSON file."""
    try:
        v = json.load(open(os.path.join(_HERE, path))).get(key)
    except Exception:
        return None
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return datetime.datetime.fromisoformat(str(v).replace('Z', '+00:00')).timestamp()
    except Exception:
        return None


def _fmt_age(mins):
    if mins is None:
        return 'n/a'
    if mins < 90:
        return f'{mins:.0f}m'
    if mins < 60 * 48:
        return f'{mins / 60:.1f}h'
    return f'{mins / 1440:.1f}d'


def _activity_7d():
    """(fills, stale_rejects) across both execution logs over the last 7 days."""
    cut = (datetime.datetime.now(datetime.timezone.utc).timestamp() - 7 * 86400) * 1000
    fills = stale = 0
    for fn in ('executions.json', 'swing-executions.json'):
        try:
            ex = json.load(open(os.path.join(_HERE, fn))).get('executions', [])
        except Exception:
            continue
        for r in ex:
            if (r.get('ts') or 0) < cut:
                continue
            if r.get('event') == 'placed':
                fills += 1
            elif r.get('event') == 'rejected' and 'stale' in (r.get('reason') or ''):
                stale += 1
    return fills, stale


def _trigger_ts_of(sid):
    """Epoch-SECONDS trigger stamp embedded in a signal id, whichever segment it is:
    swing ids are strat:pair:ts with ts in epoch SECONDS (10 digits); intraday ids are
    (viking-)pair:ts:method with ts in epoch MILLISECONDS (13 digits). Normalise both to
    seconds — else the intraday fills are silently dropped and the latency reads false-stale."""
    for seg in (sid or '').split(':'):
        s = seg.split('.')[0]
        if not s.isdigit():
            continue
        v = int(s)
        if 1_000_000_000 <= v <= 9_999_999_999:
            return v                     # epoch seconds
        if 1_000_000_000_000 <= v <= 9_999_999_999_999:
            return v // 1000             # epoch milliseconds -> seconds
    return None


# timeframe -> bar duration (minutes). Used to measure fill latency from BAR CLOSE, not the
# trigger-bar open: a signal can't be acted on until its own bar closes, so counting from the
# trigger bar charged a 4h strategy ~4h of "latency" it never had.
_TF_MIN = {'m5': 5, 'm15': 15, 'm30': 30, 'h1': 60, 'h2': 120, 'h4': 240, '4h': 240,
           'daily': 1440, 'd1': 1440}


def fill_latency_7d():
    """(n, median_delay_min, pct_prompt) over 7d — REAL execution lag from each signal's BAR CLOSE
    to the cBot's fill. FIXED 2026-11-22: previously counted from the trigger-bar OPEN, which
    conflated the structural bar-wait (engulf_manip is 4h — it CANNOT fill before its 4h bar
    closes) with real lag and cried wolf (showed ~1.5h median when the live book's genuine
    post-close lag is ~1h). We add the signal's timeframe (from the shadow log) to get bar-close,
    then floor at 0 (an intrabar / at-close fill is 0 lag). A rising median or collapsing '<1h'
    share is still the dead-pinger / cron-throttle signal. Absurd deltas dropped."""
    cut = (datetime.datetime.now(datetime.timezone.utc).timestamp() - 7 * 86400) * 1000
    try:
        shadow = json.load(open(LOG)).get('signals', {})
    except Exception:
        shadow = {}
    delays = []
    for fn in ('executions.json', 'swing-executions.json'):
        try:
            ex = json.load(open(os.path.join(_HERE, fn))).get('executions', [])
        except Exception:
            continue
        for r in ex:
            if r.get('event') != 'placed' or (r.get('ts') or 0) < cut:
                continue
            sid = r.get('signal_id')
            sg = shadow.get(sid)
            if sg and sg.get('entry_ts') and sg.get('tf'):
                bar_close = sg['entry_ts'] + _TF_MIN.get(sg['tf'], 60) * 60   # add the bar's own duration
            else:
                trig = _trigger_ts_of(sid)                                    # no shadow match → assume h1
                if trig is None:
                    continue
                bar_close = trig + 60 * 60
            d = max(r['ts'] / 1000.0 - bar_close, 0.0)                         # floor: at/before close = 0 lag
            if d <= 48 * 3600:
                delays.append(d / 60.0)
    if not delays:
        return (0, None, None)
    delays.sort(); n = len(delays)
    return (n, delays[n // 2], 100.0 * sum(1 for d in delays if d <= 60) / n)


def pipeline_health():
    """[(label, ok, detail)], overall_ok — weekly 'are all pipes flowing' check."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    out = []
    for label, path, key, warn in _HEALTH_FILES:
        ts = _ts_of(path, key)
        age = (now - ts) / 60.0 if ts else None
        ok = age is not None and age <= warn
        out.append((label, ok, f'{_fmt_age(age)} old (cap {_fmt_age(warn)})'))
    fills, stale = _activity_7d()
    # more stale-rejects than fills over 7d ⇒ the exact latency/dead-pinger failure mode
    ok_act = fills > 0 and stale <= fills
    out.append(('fills 7d', ok_act, f'{fills} filled / {stale} stale-rejected'))
    # trigger→fill latency: with the pinger live, fills should land within an hour of the
    # trigger bar. A median above that means the pinger/cron is lagging again (the root
    # cause of the stale-fill losses) even if fills are still happening.
    ln, med, prompt = fill_latency_7d()
    if ln:
        ok_lat = med is not None and med <= 60
        out.append(('fill latency 7d', ok_lat, f'median {_fmt_age(med)} · {prompt:.0f}% under 1h (n={ln})'))
    return out, all(o for _, o, _ in out)


def fmt_pipeline(health, ok):
    lines = ["PIPELINE HEALTH — " + ("all pipes OK" if ok else "!! ISSUE(S) DETECTED")]
    for label, o, detail in health:
        lines.append(f"  {'OK  ' if o else 'WARN'} {label:<14} {detail}")
    return "\n".join(lines)


def telegram_digest(rows, gaps):
    promote = [r for r in rows if r['verdict'] == 'PROMOTE' and not r['live']]
    drop = [r for r in rows if r['verdict'] == 'DROP']
    watch = [r for r in rows if r['verdict'] == 'WATCH' and not r['live']]
    lines = ["\U0001F52D <b>Weekly observer review</b>"]
    if promote:
        lines.append("\n✅ <b>Promotion-ready</b> (n≥40, both OOS halves +):")
        for r in promote:
            c = r.get('conc')
            note = (f"  (top-heavy: {c['pair']} {c['share']*100:.0f}% of R, but ex-{c['pair']} "
                    f"{c['ex_exp']:+.3f}R on n={c['ex_n']})") if c else ""
            lines.append(f"  • {r['st']}  n={r['n']}  {r['exp']:+.3f}R{note}")
    held = [r for r in watch if r.get('conc') and r['conc'].get('held')]
    if held:
        lines.append("\n\U0001F50E <b>Held back (concentration)</b> — cleared the gate but the edge doesn't survive its top pair:")
        for r in held:
            c = r['conc']
            lines.append(f"  • {r['st']}  n={r['n']}  {r['exp']:+.3f}R  "
                         f"({c['pair']} = {c['share']*100:.0f}% of R, ex-{c['pair']} {c['ex_exp']:+.3f}R on n={c['ex_n']})")
    if drop:
        lines.append("\n⛔ <b>Drop candidates</b> (persistent forward loss):")
        for r in drop:
            lines.append(f"  • {r['st']}  n={r['n']}  {r['exp']:+.3f}R")
    flagged = [g for g in gaps if g['flag']]
    if flagged:
        lines.append("\n⚠️ <b>Live-vs-model gap</b> (live demo underperforming model — consider pulling):")
        for g in flagged:
            me = f"{g['model_exp']:+.3f}R" if g['model_exp'] is not None else "n/a"
            lines.append(f"  • {g['st']}  live {g['live_exp']:+.3f}R (n={g['live_n']}) "
                         f"vs model {me}  → gap {g['gap']:+.3f}R")
    reobs = [r for r in rows if not r['live'] and r.get('verdictf') == 'PROMOTE'
             and r['verdict'] in ('DROP', 'WATCH')]
    reobs.sort(key=lambda r: -r.get('expf', 0))
    if reobs:
        lines.append("\n\U0001F501 <b>Filtered re-observation</b> (raw DROP/WATCH → PROMOTE under the live SMA200 trend filter — a FILTERED-MODEL signal, not a live green light):")
        for r in reobs[:8]:
            lead = " ← LEAD" if r['st'] == 'cam_rev' else ""
            lines.append(f"  • {r['st']}  n_f={r['nf']}  {r['exp']:+.3f}R → {r['expf']:+.3f}R{lead}")
    if not promote and not drop:
        lines.append("\nNo promotions or drops this week — all observers still accruing.")
    lines.append(f"\n{len(watch)} observer(s) on watch. Full table in the job log.")
    health, hp_ok = pipeline_health()
    lines.append("\n\U0001F527 <b>Pipeline health</b> — " + ("✅ all pipes OK" if hp_ok else "⚠️ <b>ISSUE(S)</b>"))
    for label, ok, detail in health:
        lines.append(f"  {'✅' if ok else '⚠️'} {label}: {detail}")
    return "\n".join(lines)


def send_telegram(text):
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    if not token or not chat:
        print("(no TELEGRAM_BOT_TOKEN/CHAT_ID — skipping digest send)")
        return
    def _post(payload):
        data = urllib.parse.urlencode(payload).encode()
        req = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage', data=data)
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    try:
        st = _post({'chat_id': chat, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': 'true'})
        print("telegram digest sent" if st == 200 else f"telegram HTTP {st}")
    except Exception as e:
        # HTML parse errors (a stray < / > / & in a dynamic field) return HTTP 400 and would
        # silently drop the whole digest. Fall back to plain text so the list always gets through.
        print(f"telegram HTML send failed ({e}); retrying as plain text")
        try:
            import re as _re
            plain = _re.sub(r'</?b>', '', text)
            _post({'chat_id': chat, 'text': plain, 'disable_web_page_preview': 'true'})
            print("telegram digest sent (plain text)")
        except Exception as e2:
            print(f"telegram plain send also failed: {e2}")


def main():
    rows, base, end = review()
    print("=" * 84)
    print("OBSERVER REVIEW · genuine forward evidence (entry after each strategy's tracking start)")
    print("=" * 84)
    print(fmt_table(rows))
    promote = [r['st'] for r in rows if r['verdict'] == 'PROMOTE' and not r['live']]
    drop = [r['st'] for r in rows if r['verdict'] == 'DROP']
    print("\nPROMOTION-READY observers:", ', '.join(promote) or 'none')
    print("DROP candidates:", ', '.join(drop) or 'none')
    print("\n" + "=" * 84)
    print(fmt_filtered(rows))
    reobs = [r['st'] for r in rows if not r['live'] and r.get('verdictf') == 'PROMOTE'
             and r['verdict'] in ('DROP', 'WATCH')]
    print("\nFILTERED re-observation candidates (raw DROP/WATCH -> filtered PROMOTE):",
          ', '.join(reobs) or 'none', "| LEAD: cam_rev" if 'cam_rev' in reobs else "")
    gaps = gap_rows(rows, live_records())
    print("\n" + "=" * 84)
    print(fmt_gap_table(gaps))
    flagged = [g['st'] for g in gaps if g['flag']]
    print("\nLIVE-vs-MODEL flags (live below model):", ', '.join(flagged) or 'none')
    health, hp_ok = pipeline_health()
    print("\n" + "=" * 84)
    print(fmt_pipeline(health, hp_ok))
    send_telegram(telegram_digest(rows, gaps))


if __name__ == '__main__':
    main()
