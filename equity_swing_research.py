"""US-equity SWING pilot — does any of our durable swing edge survive on
single stocks, once you charge realistic cost AND model the equity-specific
overnight-gap risk that FX/indices don't have?

Runs our two most transferable, trusted swing edges — both instrument-agnostic
and both ~2:1 (matching the swing book's reward:risk):

    gbreak  Donchian range breakout + expanding ATR, 1-ATR stop, RR2
            (the validated gold-H1 breakout logic, unchanged)
    gtrend  50/200-EMA trend + pullback-to-the-50 continuation, RR2
            (the "published gold playbook" trend-pullback, unchanged)

on h1 + daily equity OHLC, with the discipline we hold everything else to:
realistic MARKET fills, fixed dealing cost, chronological OOS split
(BOTH halves positive AND n>=40 = PASS).

WHAT MAKES THIS AN EQUITY test, not just "run FX code on stocks":

  1. Overnight-gap-through-stop. A stock can gap through your stop on
     earnings/news and fill far worse than -1R. walk_gap() checks each bar's
     OPEN first: an adverse gap past the stop realises at the open (often
     << -1R), a favourable gap past target still only books the limit (RR).
     This is THE risk single names add over diversified indices.

  2. Earnings blackout. If equity-earnings.json is present, entries within
     BLACKOUT_DAYS of a report are skipped (you don't want a fresh swing on
     the eve of earnings). Without the file, a data-driven fallback skips
     entries that fill straight into a large overnight gap.

Usage:
    python fetch_equity_ohlc.py --output equity-ohlc.json     # get data first
    python equity_swing_research.py                            # then this

If equity-ohlc.json is missing it says so and exits — nothing to test yet.
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr, agg, cost, ema

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "equity-ohlc.json")
EARN = os.path.join(HERE, "equity-earnings.json")

LOOK = 48          # Donchian lookback (same bar-count as live gbreak)
ATRLB = 10         # ATR-expansion comparison lookback
COOLDOWN = 4
GAP_ATR = 1.5      # an open >1.5 ATR from the prior close = a "gap" bar
BLACKOUT_DAYS = 3  # skip entries within N calendar days of an earnings report
RR = 2.0


# ---------- gap handling ----------
def gap_flags(bars):
    """True where the bar OPENED more than GAP_ATR beyond the prior close."""
    out = [False] * len(bars)
    for i in range(1, len(bars)):
        a = atr(bars, 14, i)
        if a and a > 0 and abs(bars[i]["o"] - bars[i - 1]["c"]) > GAP_ATR * a:
            out[i] = True
    return out


def load_earnings():
    if not os.path.exists(EARN):
        return {}
    try:
        raw = json.load(open(EARN))
    except Exception:
        return {}
    out = {}
    for pk, dates in raw.items():
        parsed = []
        for d in dates:
            try:
                parsed.append(datetime.strptime(d[:10], "%Y-%m-%d"))
            except ValueError:
                continue
        out[pk] = parsed
    return out


def in_blackout(ts_ms, earn_dates):
    if not earn_dates:
        return False
    day = datetime.utcfromtimestamp(ts_ms / 1000)
    return any(abs((day - e).days) <= BLACKOUT_DAYS for e in earn_dates)


def walk_gap(bars, i0, entry, stop, d, rr, hold):
    """Like the shared walk(), but the bar OPEN is checked first so an
    overnight gap through the stop fills at the open (worse than -1R), and a
    gap through the target still only books the limit (rr)."""
    R = abs(entry - stop)
    if R <= 0:
        return None
    tgt = entry + rr * R if d == "bull" else entry - rr * R
    for j in range(i0, min(i0 + hold, len(bars))):
        b = bars[j]
        if d == "bull":
            if b["o"] <= stop:            # gapped down through the stop
                return (b["o"] - entry) / R
            if b["o"] >= tgt:             # gapped up through the target
                return rr
            if b["l"] <= stop:
                return -1.0
            if b["h"] >= tgt:
                return rr
        else:
            if b["o"] >= stop:            # gapped up through the stop
                return (entry - b["o"]) / R
            if b["o"] <= tgt:             # gapped down through the target
                return rr
            if b["h"] >= stop:
                return -1.0
            if b["l"] <= tgt:
                return rr
    return None


# ---------- detectors (unchanged logic, instrument-agnostic) ----------
def donch(bars, i, look):
    if i < look:
        return None
    seg = bars[i - look:i]
    return (min(x["l"] for x in seg), max(x["h"] for x in seg))


def sig_gbreak(bars):
    out = []
    n = len(bars)
    last = -1
    for i in range(LOOK + 2, n - 1):
        if i <= last:
            continue
        band = donch(bars, i, LOOK)
        if not band:
            continue
        lo, hi = band
        a, ap = atr(bars, 14, i), atr(bars, 14, i - ATRLB)
        if a is None or ap is None or a <= 0 or a <= ap:   # expanding vol only
            continue
        b = bars[i]
        if b["c"] > hi:
            entry, stop = b["c"], hi - a
            if stop < entry:
                out.append((i + 1, entry, stop, "bull")); last = i + COOLDOWN
        elif b["c"] < lo:
            entry, stop = b["c"], lo + a
            if stop > entry:
                out.append((i + 1, entry, stop, "bear")); last = i + COOLDOWN
    return out


def sig_gtrend(bars):
    """50/200-EMA trend + pullback-to-the-50 continuation."""
    out = []
    n = len(bars)
    last = -1
    closes = [x["c"] for x in bars]
    e50 = ema(closes, 50)     # full EMA series, aligned to bars (None until warm)
    e200 = ema(closes, 200)
    for i in range(200, n - 1):
        if i <= last:
            continue
        f, s = e50[i], e200[i]
        if f is None or s is None:
            continue
        a = atr(bars, 14, i)
        if a is None or a <= 0:
            continue
        b = bars[i]
        if f > s and b["l"] <= f and b["c"] > f:            # uptrend, tagged & held the 50
            entry, stop = b["c"], f - a
            if stop < entry:
                out.append((i + 1, entry, stop, "bull")); last = i + COOLDOWN
        elif f < s and b["h"] >= f and b["c"] < f:          # downtrend, tagged & held the 50
            entry, stop = b["c"], f + a
            if stop > entry:
                out.append((i + 1, entry, stop, "bear")); last = i + COOLDOWN
    return out


DETECTORS = {"gbreak": sig_gbreak, "gtrend": sig_gtrend}


def run_detector(name, fn, bars, gaps, earn_dates, hold):
    rows = []
    skipped_gap = skipped_earn = 0
    for (ei, entry, stop, d) in fn(bars):
        if ei >= len(bars):
            continue
        # earnings blackout OR fills straight into a large gap → skip the entry
        if in_blackout(bars[ei]["_ts"], earn_dates):
            skipped_earn += 1
            continue
        if gaps[ei]:
            skipped_gap += 1
            continue
        o = walk_gap(bars, ei, entry, stop, d, RR, hold)
        if o is not None:
            rows.append((bars[ei]["_ts"], o - cost(o, entry, abs(entry - stop))))
    return rows, skipped_gap, skipped_earn


def line(label, rows):
    rows = sorted(rows)
    seq = [r for _, r in rows]
    n, wr, e = agg(seq)
    mid = len(rows) // 2
    _, _, eh = agg([r for _, r in rows[:mid]])
    _, _, es = agg([r for _, r in rows[mid:]])
    v = "PASS" if (e > 0 and eh > 0 and es > 0 and n >= 40) else ("thin" if n < 40 else "fail")
    print(f"      {label:<16} n={n:>4} WR={wr:>5.1f}% exp={e:>+6.3f}R OOS[{eh:>+6.3f}/{es:>+6.3f}] {v}")


def main():
    if not os.path.exists(DATA):
        print("No equity-ohlc.json found — run `python fetch_equity_ohlc.py` first")
        print("(needs TWELVEDATA_API_KEY; must run in CI or locally — the agent")
        print(" sandbox blocks outbound data hosts).")
        return
    d = json.load(open(DATA))["pairs"]
    earn = load_earnings()
    print("=" * 92)
    print("US-EQUITY SWING PILOT — validated 2:1 edges, market fills + cost + gap risk + OOS")
    print("  earnings calendar:", "loaded" if earn else "absent (data-driven gap guard only)")
    print("=" * 92)
    for tf, hold in (("daily", 40), ("h1", 80)):
        print(f"\n===== {tf} =====")
        for name, fn in DETECTORS.items():
            allrows, sg, se = [], 0, 0
            print(f"  -- {name} --")
            for pk in d:
                bars = _bars_norm(d.get(pk, {}).get(tf, []))
                if len(bars) < LOOK + 200:
                    continue
                gaps = gap_flags(bars)
                rows, s1, s2 = run_detector(name, fn, bars, gaps, earn.get(pk, []), hold)
                sg += s1; se += s2
                if rows:
                    line(pk, rows); allrows += rows
            if allrows:
                line("ALL", allrows)
            print(f"        (skipped {sg} gap-entry, {se} earnings-blackout signals)")


if __name__ == "__main__":
    main()
