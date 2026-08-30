"""crt_ix detector — Candle-Range-Theory reversal, FILTERED to a real edge.

Raw CRT (sweep the prior candle's extreme, close back inside, fade it) is a noise
pattern — negative on every class/timeframe we tested. The edge only appears when the
swept level is a CONFIRMED MULTI-DAY SWING EXTREME and the sweep candle is a DISPLACEMENT
candle. With both filters it is robustly positive on INDICES at fixed RR2:

    index (7 pockets)  n=44  52% WR  +0.568R  both OOS halves + (+0.500/+0.636)

and — critically — positive across the ENTIRE parameter grid we swept (PIV 2/3/5 ×
displacement 1.2/1.5/2.0 × level-age 15/25/40d, all 27 cells +0.108R..+0.579R), spread
across nas100/spx500/dj30/fra40 (not one instrument), with only ~9% entry overlap with
sweepfvg_ix. comm/crypto/fx are marginal-or-negative, so this fires on indices only.

The setup, per m15 index bar:
  • a DISPLACEMENT candle (body >= DISP_MULT × recent median m15 body), that
  • WICKS BEYOND a confirmed daily swing high/low (multi-day ±PIV pivot, usable only
    after its confirmation bars have printed — never repaints), but
  • CLOSES BACK INSIDE the level, in the reversal direction.
Entry = that close; stop = the swept extreme (+ small buffer); fade at RR2.
Each level is liquidity — taken ONCE — so only the FIRST sweep of a level yields.

Returns (ei, entry, stop, d) tuples (d in {'bull','bear'}), the same shape sweepfvg's
detector returns, so crt_live can reuse the mmove/absorb/sweepfvg emitter pattern.
"""

PIV = 3            # daily pivot span -> a ±3-day swing extreme is "multi-day"
DISP_MULT = 1.5    # sweep candle body must be >= 1.5× the recent median m15 body
BODY_LOOK = 20     # median-body lookback (recent m15 bars)
AGE_D = 25         # a swing level is live liquidity for ~25 trading days after it confirms
BUF = 0.05         # stop buffer as a fraction of R beyond the swept wick
_DAY = 86400.0


def _median(xs):
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0.0
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def _daily_pivots(daily):
    """Confirmed multi-day swing extremes as (confirm_ts, price) — usable only from the
    bar PIV steps later (so the pivot is settled and the level never repaints)."""
    hi, lo = [], []
    for i in range(PIV, len(daily) - PIV):
        win = daily[i - PIV:i + PIV + 1]
        if daily[i]['h'] == max(b['h'] for b in win):
            hi.append((daily[i + PIV]['_ts'], daily[i]['h']))
        if daily[i]['l'] == min(b['l'] for b in win):
            lo.append((daily[i + PIV]['_ts'], daily[i]['l']))
    return hi, lo


def crt_signals(m15, daily):
    """Yield (ei, entry, stop, d) for first-sweeps of confirmed daily swing extremes.

    Deterministic full-history replay: the same first-sweep of each level is always
    identified, so the live emitter's fresh-gate admits exactly the newly-armed ones.
    """
    out = []
    if len(m15) < BODY_LOOK + 5 or len(daily) < 2 * PIV + 5:
        return out
    hiL, loL = _daily_pivots(daily)
    used = set()          # (kind, price) levels already swept once -> liquidity taken
    for i in range(BODY_LOOK, len(m15)):
        b = m15[i]
        ts = b['_ts']
        body = abs(b['c'] - b['o'])
        med = _median([abs(m15[j]['c'] - m15[j]['o']) for j in range(i - BODY_LOOK, i)]) or 1e-9
        if body < DISP_MULT * med:
            continue
        # bearish sweep of a swing HIGH: wick above, close back below, red displacement
        for (cts, lvl) in hiL:
            if cts > ts or ts - cts > AGE_D * _DAY:
                continue
            key = ('H', round(lvl, 6))
            if key in used:
                continue
            if b['h'] > lvl and b['c'] < lvl and b['c'] < b['o']:
                entry = b['c']
                R = b['h'] - entry
                if R <= 0:
                    continue
                stop = b['h'] + BUF * R
                used.add(key)
                out.append((i, entry, stop, 'bear'))
                break
        else:
            # bullish sweep of a swing LOW (only if no high-sweep fired this bar)
            for (cts, lvl) in loL:
                if cts > ts or ts - cts > AGE_D * _DAY:
                    continue
                key = ('L', round(lvl, 6))
                if key in used:
                    continue
                if b['l'] < lvl and b['c'] > lvl and b['c'] > b['o']:
                    entry = b['c']
                    R = entry - b['l']
                    if R <= 0:
                        continue
                    stop = b['l'] - BUF * R
                    used.add(key)
                    out.append((i, entry, stop, 'bull'))
                    break
    return out
