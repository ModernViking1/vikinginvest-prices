"""PRECISE earnings-calendar study — the decision-grade version of equity_gap_study.

equity_gap_study labelled trades by a >=4% overnight PRICE gap (an earnings proxy that also
catches other news). Now that earnings-dates.json exists (yfinance, real report dates), label
every backtest trade by its ACTUAL relationship to the calendar and measure whether the current
PRE-earnings blackout helps or costs the book — on the mean AND on the tail.

Exit is GAP-AWARE (a bar that opens past the stop fills at the open — the real -Nd R tail that an
ordinary bar-backtest hides), reused from equity_gap_study.

Cohorts (US names only — that's where we have earnings), mutually exclusive, by precedence:
  HELD_THRU  earnings report falls inside the trade's hold [entry_date .. exit_date]  (the tail cohort)
  POST       entered within POST_DAYS trading of a report, not held through another     (blackout ALLOWS)
  CLEAR      no report near entry or during the hold
Plus a BLACKOUT-IMPACT view: the exact set the PRE=N rule removes (new entry within N days
BEFORE a report) vs what's kept, and the book's expR/worst with vs without that set.

  python precise_earnings_study.py --hist <broker ohlc> [--pre 2] [--post 2]
"""
import argparse, json, bisect
from collections import defaultdict
from datetime import datetime, timezone, date

import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import STRATS
from equity_gap_study import resolve_gap_aware

EARN = 'earnings-dates.json'


def _d(ts):
    return datetime.fromtimestamp(ts, timezone.utc).date()


def _ticker(sym):
    return (sym or '').rsplit('.', 1)[0].replace('-', '.')


def load_earnings():
    out = {}
    try:
        d = json.load(open(EARN)).get('symbols', {})
        for tk, ds in d.items():
            days = []
            for s in ds:
                try:
                    days.append(date.fromisoformat(s[:10]))
                except Exception:
                    continue
            out[tk] = sorted(days)
    except Exception as e:
        print(f"could not load {EARN}: {e}")
    return out


def classify(earn_days, e_date, x_date, pre, post):
    """Return (cohort, removed_by_pre_rule). cohort in held_thru/post/clear."""
    held = any(e_date <= D <= x_date for D in earn_days)
    # nearest report on/after entry, and most recent report on/before entry
    pre_hit = any(0 <= (D - e_date).days <= pre for D in earn_days)       # entry within `pre` days BEFORE a report
    post_hit = any(0 <= (e_date - D).days <= post for D in earn_days)     # entry within `post` days AFTER a report
    if held:
        cohort = 'held_thru'
    elif post_hit:
        cohort = 'post'
    else:
        cohort = 'clear'
    return cohort, pre_hit


def agg(rs):
    n = len(rs); w = sum(1 for r in rs if r > 0)
    worst = min(rs) if rs else 0.0
    blown = sum(1 for r in rs if r < -1.05)
    return n, (100*w/n if n else 0), (sum(rs)/n if n else 0), sum(rs), worst, (100*blown/n if n else 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='viking_intl_ohlc.json')
    ap.add_argument('--pre', type=int, default=2)
    ap.add_argument('--post', type=int, default=2)
    args = ap.parse_args()
    earn = load_earnings()
    pairs = json.load(open(args.hist)).get('pairs', {})

    by_cohort = defaultdict(list)            # cohort -> [r]
    removed, kept = [], []                   # PRE-rule split
    all_us = []
    nsym = 0; n_us_names_with_earn = 0

    for key, layers in pairs.items():
        sym = layers.get('sym') or key
        tk = _ticker(sym).upper()            # 'aapl'->AAPL, 'AAPL.NAS'->AAPL, 'BRK-B.NYSE'->BRK.B
        edays = earn.get(tk)                 # earnings file is US-only, so membership IS the US filter
        if not edays:
            continue
        h1 = _bars_norm(layers.get('h1') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(h1) < 400:
            continue
        nsym += 1; n_us_names_with_earn += 1
        b4 = H.agg4h(h1)

        def run(strat, bars, raw):
            for s in raw:
                res = resolve_gap_aware(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                                        H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
                if res is None:
                    continue
                rmult, exit_ts = res
                r = rmult - H.cost(rmult, s['entry'], abs(s['entry'] - s['stop']))
                cohort, pre_hit = classify(edays, _d(s['entry_ts']), _d(exit_ts), args.pre, args.post)
                by_cohort[cohort].append(r)
                all_us.append(r)
                (removed if pre_hit else kept).append(r)

        for tagn, tf, gen in STRATS:
            bars = b4 if tf == '4h' else (m15 if tf == 'm15' else h1)
            if len(bars) < (150 if tf == '4h' else 400):
                continue
            raw = H._mw_signals(bars, key, tagn, tf, gen)
            if tagn != 'threepush_eq':
                raw = H.trend_filter_signals(bars, raw)
            run(tagn, bars, raw)
        for tf in ('h1', '4h'):
            bars = b4 if tf == '4h' else h1
            if len(bars) >= (250 if tf == '4h' else 400):
                run('fib_eq', bars, H._fib_eq_signals(key, bars, tf))
        for tf in ('m15', 'h1'):
            bars = m15 if tf == 'm15' else h1
            if len(bars) >= 400:
                run('ma_pull_eq', bars, H._ma_pull_signals(key, bars, tf))

    print(f"PRECISE earnings study — {nsym} US names with earnings data. "
          f"pre={args.pre}d post={args.post}d. Gap-aware exit.\n")
    print(f"  {'cohort':<12}{'n':>6}{'WR':>5}{'expR':>9}{'totR':>8}{'worst':>8}{'>1R loss%':>11}")
    for c in ('held_thru', 'post', 'clear'):
        n, wr, e, t, worst, blown = agg(by_cohort[c])
        if n:
            print(f"  {c:<12}{n:>6}{wr:>4.0f}%{e:>+9.3f}{t:>+8.0f}{worst:>+8.2f}{blown:>10.1f}%")
    na, wa, ea, ta, worsta, blowna = agg(all_us)
    print(f"  {'ALL US':<12}{na:>6}{wa:>4.0f}%{ea:>+9.3f}{ta:>+8.0f}{worsta:>+8.2f}{blowna:>10.1f}%")

    print(f"\nBLACKOUT IMPACT — PRE={args.pre}d rule removes entries within {args.pre}d before a report:")
    nr, wr_, er, tr, worstr, blownr = agg(removed)
    nk, wk, ek, tk_, worstk, blownk = agg(kept)
    print(f"  {'REMOVED':<12}{nr:>6}{wr_:>4.0f}%{er:>+9.3f}{tr:>+8.0f}{worstr:>+8.2f}{blownr:>10.1f}%")
    print(f"  {'KEPT':<12}{nk:>6}{wk:>4.0f}%{ek:>+9.3f}{tk_:>+8.0f}{worstk:>+8.2f}{blownk:>10.1f}%")
    if na:
        print(f"\n  book total R: WITH blackout (kept only) {tk_:+.0f}  vs  WITHOUT (all) {ta:+.0f}  "
              f"(blackout {'COSTS' if tk_ < ta else 'ADDS'} {abs(tk_-ta):.0f}R of mean)")
        print(f"  book worst trade: WITH blackout {worstk:+.2f}R  vs  WITHOUT {worsta:+.2f}R  "
              f"(removed cohort's worst {worstr:+.2f}R, >1R-loss rate {blownr:.0f}%)")
        print(f"  removed = {nr} trades ({100*nr/na:.0f}% of the US book)")


if __name__ == '__main__':
    main()
