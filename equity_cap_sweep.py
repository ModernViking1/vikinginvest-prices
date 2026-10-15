"""Notional-cap sweep for the equity book (3yr broker OHLC), with a REALISTIC
per-market commission model estimated from live fills.

Question: does raising the 20% notional cap turn the tight-stop m15 strategies
net-positive? Analytically the cap should NOT change the net SIGN — commission =
rate x notional and gross = R_mult x units x R_price both scale linearly with
units, so net per trade = units x (R_mult*R_price - rate*price); the sign is
independent of units (hence of the cap). What flips the sign is rate x (price/R):
cheap venues (US 1.17bp) make even tight stops viable, expensive ones
(UK 26.5bp, DE/FR 22.4bp) need ~2R just to cover commission. This sim confirms
that and shows how the cap trades magnitude for drawdown.

Commission rate (round-trip bp of notional) from live equity-executions.json.
Sizing mirrors the live book: risk 0.5%/trade, units = min(risk/R, cap*eq/price).

    python equity_cap_sweep.py --hist <broker ohlc>
"""
import json, argparse
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from equity_broker_backtest import market, STRATS

RISK_PCT = 0.005
START_EQ = 1_000_000.0
COMM_BP = {'US': 1.17, 'UK': 26.48, 'DE': 22.41, 'FR': 22.39, 'JP': 0.13, 'HK': 3.82, 'ES': 22.40}
CAPS = [0.10, 0.20, 0.40, 1.00, 10.0]          # 10.0 ≈ uncapped (risk-only)
CAP_LABEL = {0.10: '10%', 0.20: '20%(now)', 0.40: '40%', 1.00: '100%', 10.0: 'risk-only'}


def collect(pairs):
    """All signals across symbols/strategies -> list of (ts, strat, mkt, R_mult, R_price, entry)."""
    sigs = []
    for key, layers in pairs.items():
        mkt = market(layers.get('sym', key))
        h1 = _bars_norm(layers.get('h1') or [])
        m15 = _bars_norm(layers.get('m15') or [])
        if len(h1) < 400:
            continue
        b4 = H.agg4h(h1)
        def emit(tag, bars, raw):
            for s in raw:
                st, o = H.score_trail_open(bars, s['entry_ts'], s['entry'], s['stop'], s['dir'],
                                           H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
                if st != 'resolved':
                    continue
                Rp = abs(s['entry'] - s['stop'])
                if Rp <= 0:
                    continue
                sigs.append((s['entry_ts'], tag, mkt, o, Rp, s['entry']))
        for tag, tf, gen in STRATS:
            bars = b4 if tf == '4h' else (m15 if tf == 'm15' else h1)
            if len(bars) < (150 if tf == '4h' else 400):
                continue
            raw = H._mw_signals(bars, key, tag, tf, gen)
            if tag != 'threepush_eq':
                raw = H.trend_filter_signals(bars, raw)
            emit(tag, bars, raw)
        # fib_eq (US h1+4h, UK/FR h1)
        for tf in (['h1', '4h'] if mkt == 'US' else (['h1'] if mkt in ('UK', 'FR') else [])):
            bars = b4 if tf == '4h' else h1
            if len(bars) < (250 if tf == '4h' else 400):
                continue
            emit('fib_eq', bars, H._fib_eq_signals(key, bars, tf))
    sigs.sort(key=lambda x: x[0])
    return sigs


def sim(sigs, cap, per_strat=None):
    """Book at a given notional cap, FIXED-BASE sizing (risk/notional off START_EQ, not
    running equity) so trades are additive and comparable across caps — the shadow signal
    set is far larger than the live one-position-per-symbol book, so compounding it would
    explode. eq tracks the cumulative curve only for drawdown. Returns net ret %, maxDD %."""
    base = START_EQ
    eq = base; peak = eq; maxdd = 0.0
    risk_cash = RISK_PCT * base
    for ts, strat, mkt, rmult, Rp, entry in sigs:
        rate = COMM_BP.get(mkt, 22.4) / 1e4
        units_risk = risk_cash / Rp
        units_cap = cap * base / entry
        units = min(units_risk, units_cap)
        notional = units * entry
        gross = rmult * units * Rp
        comm = rate * notional                      # round-trip commission on notional
        net = gross - comm
        eq += net
        peak = max(peak, eq)
        maxdd = max(maxdd, (peak - eq) / peak)
        if per_strat is not None:
            per_strat[strat] += net
    return (eq / START_EQ - 1) * 100, 100 * maxdd, eq


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    sigs = collect(pairs)
    n = len(sigs)
    span_d = (sigs[-1][0] - sigs[0][0]) / 86400 if n else 0
    print(f"Equity notional-cap sweep — {n} resolved signals over ~{span_d:.0f}d, "
          f"risk {RISK_PCT*100:.1f}%/trade, live per-market commission.\n")

    print(f"{'cap':<11}{'book ret%':>11}{'maxDD%':>9}")
    for cap in CAPS:
        ret, dd, _ = sim(sigs, cap)
        print(f"{CAP_LABEL[cap]:<11}{ret:>+10.1f}%{dd:>8.1f}%")

    # per-strategy net $ at each cap
    print(f"\nPer-strategy NET $ by cap:")
    print(f"{'strategy':<20}" + "".join(f"{CAP_LABEL[c]:>12}" for c in CAPS))
    ps = {c: defaultdict(float) for c in CAPS}
    for c in CAPS:
        sim(sigs, c, ps[c])
    str128 = sorted({s for c in CAPS for s in ps[c]})
    for strat in str128:
        print(f"{strat:<20}" + "".join(f"{ps[c][strat]:>+12.0f}" for c in CAPS))

    # US vs EU split (the real driver): run the sim on each subset
    print(f"\nUS-only vs EU-only (net ret% / maxDD%) — shows commission rate, not cap, is the lever:")
    us = [s for s in sigs if s[2] == 'US']; eu = [s for s in sigs if s[2] in ('UK', 'DE', 'FR')]
    print(f"{'cap':<11}{'US ret%':>10}{'US DD%':>8}{'EU ret%':>10}{'EU DD%':>8}")
    for cap in CAPS:
        ur, ud, _ = sim(us, cap); er, ed, _ = sim(eu, cap)
        print(f"{CAP_LABEL[cap]:<11}{ur:>+9.1f}%{ud:>7.1f}%{er:>+9.1f}%{ed:>7.1f}%")

    # per-strategy US-only (does every strategy work when commission is cheap?)
    print(f"\nUS-ONLY per-strategy NET $ (cap 100%, risk-sized): is the m15 fine when commission is 1.2bp?")
    psu = defaultdict(float); sim(us, 1.00, psu)
    for strat in sorted(psu):
        print(f"  {strat:<20}{psu[strat]:>+12.0f}")


if __name__ == '__main__':
    main()
