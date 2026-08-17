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
from unified_shadow_harness import detect_hs, detect_s5, detect_ob, detect_tl, detect_w5pb, detect_s5_rsi_wide, detect_fibgz, detect_fredtl, detect_threepush, detect_engulf_manip, detect_asianglitch, detect_wm, detect_obfvg, detect_gbreak, detect_gtrend, detect_fma, detect_twob

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
# Dantev Fibonacci golden-zone reversal (2026-07-16) — 9th OBSERVED candidate,
# COMMODITIES + H1 only, fixed RR2. Thin edge (+0.06R, PF ~1.1) but the only cell
# in the Dantev class breakdown positive on BOTH OOS halves. cBot-executable.
FIBGZ_CLASSES = {'comm'}
# 3-push + break-of-structure + retest reversal (2026-07-18) — 11th OBSERVED
# candidate, COMMODITIES + 4H only. Thin (n=40, +0.19R, both OOS halves +,
# parameter-robust) but only 3/6 walk-forward folds — observe, don't trust.
THREEPUSH_CLASSES = {'comm'}
# 4H manipulation engulfing reversal (2026-07-18) — 12th OBSERVED candidate, CRYPTO
# only. Engulf that sweeps recent lows then closes in-trend. +0.14R, WR 38%, n=315,
# parameter-robust, 4/6 folds, both OOS halves + — stronger than the other thin adds.
ENGULF_CLASSES = {'crypto'}
# Exposure control: collapse all fresh signals on a pair into ONE live position so
# correlated strategies don't stack (e.g. s5_rsi + s5_rsi_wide + tl_nowick all
# short the same pair on the same bar = 3x one directional bet). Highest-conviction
# strategy wins the tag; the rest are recorded as cofire for stats/visibility. The
# shadow harness still logs EVERY signal independently, so per-strategy VOLUME stats
# are unaffected — this caps only live demo exposure, mirroring the intraday cBot's
# one-position-per-symbol rule. Order = validated backtest expectancy, best first.
# 'Asian-session gold glitch' (2026-07-22) — 14th OBSERVED candidate, XAUUSD only,
# H1, session-timed sweep-reversal at RR3 (self-gates to xauusd in the detector).
# cBot-executable (market entry + 3R bracket); +0.18..+0.28R at 3:1 with both OOS
# halves positive, robust across the reference-hour/buffer/hold grid. GOLD-ONLY —
# every other pair/class is negative. Lowest live priority so it never suppresses
# an established edge on gold; still shadow-logged and recorded as cofire.
# W/M neckline-break reversal (2026-07-23) — 15th OBSERVED candidate, CRYPTO only,
# H1, fixed 1:1 (self-gates to crypto). Emits on the neckline break (market entry ~
# break price). Thin but the only class surviving a realistic fill (+0.09R, both OOS
# halves +, robust across pivot/tolerance). Lowest live priority.
# 2026-07-23: ob now REQUIRES a reversal candle (engulf/3-bar/pin bar) at the retrace
# bar — validated filter that lifted ob from fragile (+0.087R, fails walk-forward) to
# robust (+0.257R, WR 42%, both OOS halves +). Folded into detect_ob; no separate tag.
# OB+FVG retrace (2026-07-25) — 18th OBSERVED candidate, XRPUSD + USDCAD H1 only,
# fixed RR2 market entry. Parameter-robust per-pair cells (+0.23..+0.29R median).
# FTSE100/BTCUSD H1 run shadow-only (obfvg_w) to decide edge vs noise forward.
# Gold playbook (2026-07-27) — 19th/20th OBSERVED candidates, XAUUSD only:
# gbreak = #3 volatility-confirmed range breakout (H1, RR2) — the robust standout
#          (15/15 parameter cells pass both OOS halves); highest gold priority.
# gtrend = #1 50/200 EMA trend pullback (H4, RR2, choppiness filter removed —
#          it hurt in testing). Both self-gate to xauusd and are cBot-executable.
PRIORITY = {'s5_rsi_wide': 0, 's5_rsi': 1, 'hs': 2, 'ob': 3, 'w5_pullback': 6, 'fred_tl': 7, 'threepush': 8, 'engulf_manip': 9, 'asianglitch': 10, 'wm': 11, 'obfvg': 12, 'gbreak': 13, 'gtrend': 14, 'fma_gold': 15, 'twob': 16, 'twob_cm': 17}

# Demo-only pilots — emitted to the swing feed but flagged so the cBot executes them
# ONLY on a demo account (skips on live). Lets a candidate accrue REAL forward fills
# on demo before risking live capital. Remove a tag to promote it to live.
#   fma_gold (2026-08-07) — FMA liquidity-sweep + 50-EMA reclaim reversal, m15 gold, RR2.
#     Cross-validated in-sample (native m15 + 12-month m5->m15), but zero forward evidence
#     yet — demo-first per decision.
DEMO_ONLY = {'fma_gold'}

# Demoted to observer-only — genuine-forward decay on live data since tracking began
# (see swing-shadow-log.json GENUINE FORWARD). The harness still runs each detector and
# logs it as an observer, so it keeps accumulating forward evidence — it is simply held
# back from the cBot feed until it re-earns a slot (target: n>=40 fwd, BOTH OOS halves +).
# To re-promote, delete the tag from DEMOTED.
#   tl_nowick  (2026-08-03)  fwd n=38  WR 18.4%  -0.475R  — negative across every class
#   fib_gz     (2026-08-05)  fwd n=86  WR 29%    -0.136R  (OOS 2nd half -0.241, decaying;
#              full-history +0.061R was carried by the in-sample half) AND live demo 0/5
#              -5.16R (5 straight stop-outs). Marginal model edge that has rolled negative
#              forward and is live-confirmed losing → demote.
#   wm         (2026-08-05)  live demo WR 13%  -6.5R  — crypto-only 1:1 mean-reversion that
#              has decayed hard on the live account (mostly straight stop-outs). Held back
#              from the cBot; harness keeps logging it as an observer to see if it recovers.
#   w5_pullback(2026-08-07)  fwd n=52  WR 13%  -0.638R  BOTH OOS halves - (-0.576/-0.701).
#              Kept LIVE on 2026-08-03 to fatten the sample before a keep/kill call; the
#              sample has now climbed and the decay persisted / worsened (was -0.314R),
#              with the first OOS half now also negative. The weekly observer review
#              flagged it a DROP → demote. Harness keeps logging it as an observer.
#   gtrend     (2026-08-12)  fwd n=33  WR 3%  -0.924R  — 50/200 EMA trend-pullback on gold
#              H4. Strongly +ve in-sample (+0.341R) but has collapsed forward: the recent
#              gold regime doesn't suit pullback entries (it keeps fading the trend and
#              getting stopped). Meets the DROP gate (n>=25, exp<-0.05R). Held back from the
#              cBot; harness keeps logging it — and its INVERTED mirror (gtrend_inv) — as
#              observers to see whether the decay is regime-transient or structural.
DEMOTED = {'tl_nowick', 'fib_gz', 'wm', 'w5_pullback', 'gtrend'}

# Scaled exit for the 2:1 gold signals (2026-08-01) — bank profit progressively instead of a
# single far TP. gbreak/gtrend are emitted as THREE legs (1/3 risk each, SHARED stop) with
# targets at 1R / 2R / 3R. The cBot opens one market position per signal id, so 3 ids = 3
# partial take-profits: the first two bank on the way up and only the runner rides to 3R, so
# a winner can't fully round-trip to a loss. Backtest: ~RR2 expectancy (gbreak +0.256R) with
# progressive banking. asianglitch stays single-TP at 3:1 (scaling it costs more than it saves).
# Requires the swing cBot / account to allow multiple positions per symbol (cTrader hedging mode).
SCALED_GOLD = {'gbreak', 'gtrend'}
SCALE_LEGS = (1.0, 2.0, 3.0)


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
        m15 = _bars_norm(pairs[pk].get('m15', []))   # for the m15 fma_gold demo pilot only
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
            found += detect_ob(pk, h1, daily)   # ob now requires reversal-candle confirmation (2026-07-23)
        if cls in TL_CLASSES:
            found += detect_tl(pk, h1, daily)
        if cls in W5PB_CLASSES:
            found += detect_w5pb(pk, h1, daily)
        if cls in S5W_CLASSES:
            found += detect_s5_rsi_wide(pk, h1, daily)
        if cls in FIBGZ_CLASSES:
            found += detect_fibgz(pk, h1, daily)
        found += detect_fredtl(pk, h1, daily)   # self-gates to xauusd only
        found += detect_asianglitch(pk, h1, daily)   # self-gates to xauusd only; emits rr=3.0
        found += detect_wm(pk, h1, daily)            # self-gates to crypto only; H1 1:1, emits rr=1.0
        found += detect_obfvg(pk, h1, daily)         # self-gates to xrpusd/usdcad H1; OB+FVG retrace, RR2
        found += detect_gbreak(pk, h1, daily)        # self-gates to xauusd H1; range breakout + expanding ATR, RR2
        found += detect_gtrend(pk, h1, daily)        # self-gates to xauusd H4; 50/200 EMA trend pullback (no choppiness filter), RR2
        # twob (2026-08-11) — 2B failed-breakout reversal, crypto H1. PROMOTED to live: genuine
        # forward n=52 WR 52% +0.090R, BOTH OOS halves +. Validated at a FIXED RR2 (re-scored
        # forward +0.080R), so it emits at the default RR2.
        # twob_cm (2026-08-12) — commodities H1 sibling PROMOTED: fwd n=74 +0.449R both OOS halves
        # +, and the edge SURVIVES fixed RR2 (re-scored fwd n=61 +0.328R, and forward > in-sample
        # so not a decay artifact). twob_ix (indices) stays observer-only — still negative.
        found += [s for s in detect_twob(pk, h1) if s['strategy'] in ('twob', 'twob_cm')]
        # FMA ($100->$1M) liquidity-sweep + 50-EMA reclaim reversal — GOLD only, m15, RR2.
        # DEMO-FIRST PILOT: emitted to the (demo) swing feed with demo_only=True so the cBot
        # skips it on any live account until it earns forward evidence. detect_fma also emits
        # commodities/index tags — those stay observer-only, so filter to fma_gold here.
        found += [s for s in detect_fma(pk, m15) if s['strategy'] == 'fma_gold']
        if cls in THREEPUSH_CLASSES:
            found += detect_threepush(pk, h1, daily)
        if cls in ENGULF_CLASSES:
            found += detect_engulf_manip(pk, h1, daily)
        for s in found:
            if s['entry_ts'] < fresh_after:
                continue
            if s['strategy'] in DEMOTED:   # detected but held back from the cBot; harness still logs it
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
                'rr': s.get('rr', RR),   # per-signal RR (asianglitch=3.0); others default to RR (2.0)
                'r_pct': R_PCT,
                'entry_mode': 'market',
                'demo_only': s['strategy'] in DEMO_ONLY,   # cBot skips these on a live account
                'trigger_ts': int(s['entry_ts']),
                'created_ts': int(data_end),
                'expiry_ts': int(s['entry_ts'] + EXPIRY_HOURS * 3600),
                'state': 'triggered',
            })

    # ---- exposure dedup: one live position per pair (highest-conviction wins) ----
    # Demo-only pilots bypass the cap — they run on a separate (demo) account and must be
    # allowed to trade their pair independently to accrue forward evidence, rather than be
    # suppressed by the higher-priority live signals on the same pair (e.g. fma_gold vs the
    # live gold gbreak/gtrend).
    demo_rows = [r for r in rows if r.get('demo_only')]
    # Demo-only pilots skip the dedup loop below, so give them the same cofire/suppressed
    # shape the deduped rows carry (single-strategy, nothing suppressed) — otherwise the
    # summary print and any consumer that reads these keys hits a KeyError.
    for r in demo_rows:
        r.setdefault('cofire', [r['strategy']])
        r.setdefault('cofire_dirs', [f"{r['strategy']}:{r['dir']}"])
        r.setdefault('suppressed', 0)
    live_rows = [r for r in rows if not r.get('demo_only')]
    by_pair = {}
    for r in live_rows:
        by_pair.setdefault(r['pair'], []).append(r)
    total_raw = len(rows); deduped = []
    for pk, group in by_pair.items():
        group.sort(key=lambda r: (PRIORITY.get(r['strategy'], 9), -r['trigger_ts']))
        primary = dict(group[0])
        primary['cofire'] = sorted({g['strategy'] for g in group})
        primary['cofire_dirs'] = sorted({f"{g['strategy']}:{g['dir']}" for g in group})
        primary['suppressed'] = len(group) - 1
        deduped.append(primary)
    rows = deduped + demo_rows          # demo-only pilots bypass the per-pair cap

    # ---- scaled exit: split the 2:1 gold signals into 3 partial-TP legs (1R/2R/3R) ----
    scaled = []
    for r in rows:
        if r['strategy'] in SCALED_GOLD and r['pair'] == 'xauusd':
            n = len(SCALE_LEGS)
            for i, rr in enumerate(SCALE_LEGS, 1):
                leg = dict(r)
                leg['id'] = f"{r['id']}:t{i}"
                leg['rr'] = rr                       # per-leg target: 1R / 2R / 3R
                leg['r_pct'] = round(R_PCT / n, 6)   # 1/3 risk each -> total risk = R_PCT
                leg['scaled_leg'] = i; leg['scaled_legs'] = n
                scaled.append(leg)
        else:
            scaled.append(r)
    rows = scaled

    # newest first, stable
    rows.sort(key=lambda r: (-r['trigger_ts'], r['id']))
    out = {
        'schema_version': 1,
        'generated': datetime.now(timezone.utc).isoformat() if False else None,  # stamped by CI env below
        'data_end_ts': int(data_end),
        'raw_signal_count': total_raw,   # pre-dedup (shadow harness logs all of these)
        'count': len(rows),
        'signals': rows,
    }
    # deterministic 'generated' from data_end so re-runs on identical data don't churn
    out['generated'] = datetime.fromtimestamp(data_end, timezone.utc).isoformat()
    with open(OUT, 'w') as f:
        json.dump(out, f, indent=1)
    print(f"swing-signals.json: {len(rows)} live position(s) after dedup from {total_raw} raw signal(s) "
          f"(data_end {int(data_end)}, fresh window {FRESH_HOURS}h)")
    for r in rows[:20]:
        extra = f"  cofire={r['cofire']}" if r.get('suppressed') else ""
        print(f"  {r['id']:<40} {r['dir']:<4} stop={r['stop']} rr={r['rr']}{extra}")


if __name__ == '__main__':
    main()
