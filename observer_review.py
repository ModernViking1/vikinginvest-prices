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
import urllib.request
import urllib.parse

LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'swing-shadow-log.json')
PROMOTE_N = 40
DROP_N = 25
DROP_EXP = -0.05

# Strategies executing on the cBot (live/demo feed) — mirrors swing_signals PRIORITY minus
# DEMOTED, plus the demo pilot. Everything else in the log is an observer.
LIVE = {'hs', 's5_rsi', 's5_rsi_wide', 'ob', 'fred_tl', 'threepush',
        'engulf_manip', 'asianglitch', 'obfvg', 'gbreak', 'gtrend', 'fma_gold'}


def agg(seq):
    r = [x for x in seq if x is not None]; n = len(r); w = sum(1 for x in r if x > 0)
    return n, (100.0 * w / n if n else 0.0), (sum(r) / n if n else 0.0)


def classify(n, exp, eh, es):
    if n >= PROMOTE_N and exp > 0 and eh > 0 and es > 0:
        return 'PROMOTE'
    if n >= DROP_N and exp < DROP_EXP:
        return 'DROP'
    return 'WATCH'


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


def telegram_digest(rows):
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
    if not promote and not drop:
        lines.append("\nNo promotions or drops this week — all observers still accruing.")
    lines.append(f"\n{len(watch)} observer(s) on watch. Full table in the job log.")
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
    send_telegram(telegram_digest(rows))


if __name__ == '__main__':
    main()
