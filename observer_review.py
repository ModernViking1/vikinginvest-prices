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
import datetime
import urllib.request
import urllib.parse

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

# Strategies executing on the cBot (live/demo feed) — swing_signals PRIORITY minus DEMOTED,
# the demo pilot, plus the promoted intraday emitters (mmove_m15, absorb_btc). Everything
# else in the log is an observer. Keep in sync when promoting/demoting, or a live strategy
# wrongly shows as "promotion-ready" (and a demoted one lingers as live).
LIVE = {'hs', 's5_rsi', 'engulf_manip', 'fred_tl', 'asianglitch', 'obfvg',
        'gbreak', 'fma_gold', 'fma_sweep_cm', 'twob', 'twob_cm', 'twob_ix', 'mmove_m15',
        'absorb_btc', 'sweepfvg_ix', 'crt_ix'}


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100.0 * w / n if n else 0.0), (sum(r) / n if n else 0.0)


def classify(n, exp, eh, es):
    if n >= PROMOTE_N and exp > 0 and eh > 0 and es > 0:
        return 'PROMOTE'
    if n >= DROP_N and exp < DROP_EXP:
        return 'DROP'
    return 'WATCH'


def live_records():
    """{method: [realized_r,...]} for closed live (demo) fills.

    Method comes from the signal_id's last colon-segment (e.g. btcusd:169..:absorb_btc),
    which the emitters always set — the flat `strategy` field is only sparsely populated.
    """
    out = {}
    try:
        ex = json.load(open(EXECS)).get('executions', [])
    except Exception:
        return out
    for r in ex:
        if r.get('event') != 'closed':
            continue
        sid = r.get('signal_id') or ''
        parts = sid.split(':')
        method = parts[-1] if len(parts) >= 3 else (r.get('strategy') or '')
        rr = r.get('realized_r')
        if method and rr is not None:
            out.setdefault(method, []).append(rr)
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
    by = {}
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
    rows = []
    for st, rec in by.items():
        rec.sort(); seq = [r for _, r in rec]; n, wr, exp = agg(seq); m = len(rec) // 2
        _, _, eh = agg([r for _, r in rec[:m]]); _, _, es = agg([r for _, r in rec[m:]])
        days = int((end - track[st]) / 86400) if st in track and end else None
        rows.append({'st': st, 'n': n, 'wr': wr, 'exp': exp, 'eh': eh, 'es': es,
                     'verdict': classify(n, exp, eh, es), 'days': days,
                     'live': st in LIVE})
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


def fill_latency_7d():
    """(n, median_delay_min, pct_prompt) over 7d — the REAL gap from a signal's trigger bar
    (parsed from its id) to when the cBot actually placed the fill. This is the direct
    read on whether the external pinger keeps swing fills prompt: a rising median or a
    collapsing '<1h' share is the dead-pinger / cron-throttle failure that caused the
    stale-fill losses. Absurd/negative deltas (bad clock, backfilled id) are dropped."""
    cut = (datetime.datetime.now(datetime.timezone.utc).timestamp() - 7 * 86400) * 1000
    delays = []
    for fn in ('executions.json', 'swing-executions.json'):
        try:
            ex = json.load(open(os.path.join(_HERE, fn))).get('executions', [])
        except Exception:
            continue
        for r in ex:
            if r.get('event') != 'placed' or (r.get('ts') or 0) < cut:
                continue
            trig = _trigger_ts_of(r.get('signal_id'))
            if trig is None:
                continue
            d = r['ts'] / 1000.0 - trig
            if 0 <= d <= 48 * 3600:
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
        out.append(('fill latency 7d', ok_lat, f'median {_fmt_age(med)} · {prompt:.0f}% <1h (n={ln})'))
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
            lines.append(f"  • {r['st']}  n={r['n']}  {r['exp']:+.3f}R")
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
    try:
        data = urllib.parse.urlencode({'chat_id': chat, 'text': text, 'parse_mode': 'HTML',
                                       'disable_web_page_preview': 'true'}).encode()
        req = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage', data=data)
        with urllib.request.urlopen(req, timeout=20) as r:
            print("telegram digest sent" if r.status == 200 else f"telegram HTTP {r.status}")
    except Exception as e:
        print(f"telegram send failed: {e}")


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
