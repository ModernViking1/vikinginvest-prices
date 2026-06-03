# Instrument additions — log

## v7 batch (2026-06-03) — DEPLOYED

All 11 new pairs wired across MKTS, fetch-prices.js,
fetch_historical_ohlc.py, publish_intraday_ohlc.py, and (for the new
indices) detect_triggers.py's FIB_ENTRY_PAIRS. RULES_VERSION bumped to
2026-06-03e so the backtest cache auto-invalidates and recomputes
under the expanded fib-pair set on next page load.

### FX Minors

| Key       | Symbol  | OANDA pair |
|-----------|---------|------------|
| `nzdcad`  | NZD/CAD | NZD_CAD    |
| `eurnok`  | EUR/NOK | EUR_NOK    |
| `nzdchf`  | NZD/CHF | NZD_CHF    |
| `gbpchf`  | GBP/CHF | GBP_CHF    |
| `usdzar`  | USD/ZAR | USD_ZAR    |
| `usdcnh`  | USD/CNH | USD_CNH    |
| `eursgd`  | EUR/SGD | EUR_SGD    |

`eursek` was already in the codebase (existing placeholder) so it
wasn't re-added.

### Crypto

| Key       | Symbol  | Coinbase   |
|-----------|---------|------------|
| `ltcusd`  | LTC/USD | LTC-USD    |

### Indices (Fib half-size entry)

| Key       | Symbol     | OANDA pair  | Note |
|-----------|------------|-------------|------|
| `jp225`   | Nikkei 225 | JP225_USD   | User originally wrote "Nikkei 250" — actual index is 225 |
| `fra40`   | CAC 40     | FR40_EUR    | French large-cap |
| `esp35`   | IBEX 35    | ES35_EUR    | Spanish large-cap — confirm OANDA ticker if /candles returns 404 |

Also patched: `spx500` was missing from `publish_intraday_ohlc.py`'s
PAIRS dict (present everywhere else). Added during this batch so the
intraday workflow now actually fetches m15 OHLC for the S&P.

## Rollback

If the dashboard becomes too slow with these added, revert the v7
batch by reverting commits between this entry and the previous
"v6 additions" comments — none of the existing code paths depend
on the new pairs.
