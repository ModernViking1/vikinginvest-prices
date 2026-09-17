"""Trend-quality regime gate (2026-09-17).

Forward-record research (46k regime-matched signals, both OOS halves) showed the swing
strategies split cleanly by the market regime they fire into:

  • MOMENTUM / TREND strategies (holygrail*, mmove*, breakouts, wm) make money in TRENDS and
    LOSE in chop  (+0.09..+0.25R trending vs -0.06..-0.30R choppy).
  • REVERSAL / fade strategies (twob*, po3*) make money in CHOP and lose in trends
    (-0.02..-0.09R trending vs +0.29..+0.45R choppy).

So a blanket "trade less in chop" filter would gut the reversal edge. The gate is
REGIME-MATCHED instead: momentum entries require a trending regime, reversal entries require
a non-trending one. Net book expectancy +0.082R -> +0.106R, holding in both chronological
halves (+0.019 / +0.028R), cutting ~22% of trades. Regime is measured on the signal's own
timeframe, causally (only bars up to the entry).

Metrics:
  ADX (Wilder, 14)      — trend STRENGTH. Low = no trend.
  Choppiness Index (14) — high (>61.8) = choppy/range-bound, low (<38.2) = strongly trending.

Fail-open: if the regime can't be computed (too few bars), the signal passes ungated.
"""
import math

# ── thresholds (Fibonacci-standard CI bands; ADX floor from the sweep) ──
CHOP_HI = 61.8    # Choppiness above this = choppy/range-bound
CHOP_LO = 38.2    # Choppiness below this = strongly trending
ADX_MIN = 20.0    # ADX below this = too weak a trend for a momentum entry
REGIME_N = 14

# Strategies whose edge is TREND-dependent — require a trending regime (skip if choppy).
MOMENTUM_STRATS = {
    'holygrail', 'holygrail_cm', 'holygrail_cm_m15', 'holygrail_eq', 'holygrail_eq_m15',
    'mmove', 'mmove_m15', 'mmove_ix', 'mmove_ix4', 'mmove_c4',
    'volbreak', 'volbreak_ix', 'volbreak_eq', 'zbreak_crypto', 'zbreak_ix', 'zbreak_gold',
    'wm', 'e90break', 'gbreak', 'gtrend', 'fma_gold', 'fma_sweep_cm', 'fma_sweep_ix',
}
# Strategies whose edge is CHOP-dependent (fades) — require a NON-strong-trend regime.
REVERSAL_STRATS = {
    'twob', 'twob_ix', 'twob_cm', 'twob_eq', 'po3_conf', 'po3_cm', 'po3_kane',
}


def adx_series(bars, n=REGIME_N):
    """Wilder ADX in one pass. Returns a list aligned to `bars` (None until warm)."""
    m = len(bars); out = [None] * m
    if m < 2 * n + 1:
        return out
    tr = [0.0] * m; pdm = [0.0] * m; ndm = [0.0] * m
    for i in range(1, m):
        h, l = bars[i]['h'], bars[i]['l']; ph, pl, pc = bars[i-1]['h'], bars[i-1]['l'], bars[i-1]['c']
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))
        up = h - ph; dn = pl - l
        pdm[i] = up if (up > dn and up > 0) else 0.0
        ndm[i] = dn if (dn > up and dn > 0) else 0.0
    atr = sum(tr[1:n+1]); ap = sum(pdm[1:n+1]); an = sum(ndm[1:n+1])
    dx = [None] * m
    for i in range(n + 1, m):
        atr = atr - atr / n + tr[i]; ap = ap - ap / n + pdm[i]; an = an - an / n + ndm[i]
        if atr <= 0:
            continue
        pdi = 100 * ap / atr; ndi = 100 * an / atr; s = pdi + ndi
        dx[i] = 100 * abs(pdi - ndi) / s if s > 0 else 0.0
    first = n + 1
    vals = [dx[i] for i in range(first, min(first + n, m)) if dx[i] is not None]
    if len(vals) < n:
        return out
    cur = sum(vals) / n; out[first + n - 1] = cur
    for i in range(first + n, m):
        if dx[i] is None:
            out[i] = cur; continue
        cur = (cur * (n - 1) + dx[i]) / n; out[i] = cur
    return out


def chop_series(bars, n=REGIME_N):
    """Choppiness Index. High (>61.8)=choppy/range, low (<38.2)=trending."""
    m = len(bars); out = [None] * m
    if m < n + 1:
        return out
    tr = [0.0] * m
    for i in range(1, m):
        h, l = bars[i]['h'], bars[i]['l']; pc = bars[i-1]['c']
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))
    logn = math.log10(n)
    for i in range(n, m):
        sumtr = sum(tr[i-n+1:i+1])
        hh = max(b['h'] for b in bars[i-n+1:i+1]); ll = min(b['l'] for b in bars[i-n+1:i+1])
        rng = hh - ll
        if rng > 0 and sumtr > 0:
            out[i] = 100 * math.log10(sumtr / rng) / logn
    return out


def build_regime(bars, n=REGIME_N):
    """Precompute (ts_list, adx_list, chop_list) for a bar array (normed: _ts, h, l, c)."""
    return ([b['_ts'] for b in bars], adx_series(bars, n), chop_series(bars, n))


def _at(regime, entry_ts):
    import bisect
    ts, adx, chop = regime
    i = bisect.bisect_right(ts, entry_ts) - 1   # last bar at/before entry (causal, no lookahead)
    if i < 0 or i >= len(ts):
        return None, None
    return adx[i], chop[i]


def passes_gate(strategy, regime, entry_ts):
    """True if `strategy`'s signal at `entry_ts` fires in a regime that suits it.
    Momentum needs a trend (not choppy, ADX not dead); reversal needs a non-strong-trend.
    Strategies in neither set, or an uncomputable regime, pass ungated (fail-open)."""
    if strategy not in MOMENTUM_STRATS and strategy not in REVERSAL_STRATS:
        return True
    adx, chop = _at(regime, entry_ts)
    if adx is None or chop is None:
        return True
    if strategy in MOMENTUM_STRATS:
        return chop <= CHOP_HI and adx >= ADX_MIN
    return chop >= CHOP_LO        # reversal: skip only in a strong trend
