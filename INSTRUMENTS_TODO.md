# Pending instrument additions

Not yet deployed — placeholder list captured 2026-06-03 so we can wire
them up in one batch once the data feeds (OANDA mapping +
fetch-prices.js + fetch_historical_ohlc.py + intraday OHLC publisher +
detect_triggers.py) are ready.

## FX Minors

| Key       | Symbol  | OANDA pair | Notes |
|-----------|---------|------------|-------|
| `nzdcad`  | NZD/CAD | NZD_CAD    | NZ Dollar vs Canadian Dollar |
| `eursek`  | EUR/SEK | EUR_SEK    | already a placeholder MKTS entry — verify feed mapping |
| `eurnok`  | EUR/NOK | EUR_NOK    | Euro vs Norwegian Krone |
| `nzdchf`  | NZD/CHF | NZD_CHF    | NZ Dollar vs Swiss Franc |
| `gbpchf`  | GBP/CHF | GBP_CHF    | GBP vs Swiss Franc |
| `usdzar`  | USD/ZAR | USD_ZAR    | USD vs South African Rand — high spread, watch sizing |
| `usdcnh`  | USD/CNH | USD_CNH    | USD vs China Offshore (CNH, not CNY) |
| `eursgd`  | EUR/SGD | EUR_SGD    | Euro vs Singapore Dollar |

## Crypto

| Key       | Symbol  | OANDA pair  | Notes |
|-----------|---------|-------------|-------|
| `ltcusd`  | LTC/USD | LTC_USD     | Litecoin |

## Indices

| Key       | Symbol     | OANDA pair  | Notes |
|-----------|------------|-------------|-------|
| `jp225`   | Nikkei 225 | JP225_USD   | User wrote "Nikkei 250" — actual index is 225. Confirm before deploy. |
| `fra40`   | CAC 40     | FR40_EUR    | French large-cap |
| `esp35`   | IBEX 35    | ES35_EUR    | Spanish large-cap |

## Deploy checklist (per instrument)

1. Add MKTS entry (Viking_Invest_Trading_v69.html) with `t:` set to the
   right category — the dashboard's grouped overview + the backtest
   tab will pick it up automatically.
2. Add WICKATOR_EW seed entry (or rely on auto-EW).
3. Add to `_btProfileFor` if it needs Fib half-size routing (indices
   default to Fib; minors and crypto default to AUTO-EW + wick).
4. Add to `FIB_ENTRY_PAIRS` in `detect_triggers.py` if it's an
   index/commodity.
5. Add the price-feed mapping in `fetch-prices.js` and
   `fetch_historical_ohlc.py`.
6. Add to the intraday-OHLC publisher's pair list.
7. Force-rerun backtests so the deep cache picks it up.
