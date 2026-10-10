# VikingEquity MT5 EA — `.EQ` book executor (demo pilot)

Executes the equity (`.EQ`) book — **holygrail_eq, holygrail_eq_m15, twob_eq, volbreak_eq** — on an
MT5 account by polling the published `equity-signals.json` feed. Stocks are MT5-only (not cTrader),
so this is a separate execution path from the swing cTrader cBot.

## How it connects (important)
The signal server **never connects to your account.** This EA runs inside *your* MT5 terminal,
logged into *your* account by *you*, and pulls a **public, read-only JSON** over HTTPS. No
credentials ever leave MT5. There is nothing to "connect" on the server side.

```
server (GitHub Actions)                     your MT5 terminal
  build_equity_signals.py  ──►  equity-signals.json (CDN)  ──►  VikingEquityEA.mq5  ──►  orders
```

## One-time setup
1. Copy `VikingEquityEA.mq5` into `MQL5/Experts/` (MetaEditor → compile, or drop and refresh).
2. **Allow the feed URL:** MT5 → *Tools → Options → Expert Advisors* → tick **“Allow WebRequest for
   listed URL”** and add:  `https://cdn.jsdelivr.net`
3. Confirm the traded symbols exist in *Market Watch*. US pilot:
   `AAPL.NAS, AMZN.NAS, MSFT.NAS, NVDA.NAS, TSLA.NAS`. Expanded book (validate on 3yr first — see
   `VikingHistoryExport.mq5`): DE `DBK/BOSS/PAH3/VOWG/BAYN/CBK/MBGn.ETR`,
   UK `BARC/BA/LSE/RR/TSCO/AML/BKG/EZJ/MKS/RDSB/NWG.LSE`, FR `BNP/CAP/CA/BN/CDI.PAR`,
   JP `7267/8306/7974/6758.TSE`, HK `1288/9898/0003.HK`, ES `CABK/SAN.MAD`.
   (If your broker names them differently, the feed's `sym` field must match — tell me and I'll update
   the server map; don't rename on the EA side.)
4. Attach the EA to **one** chart (any symbol/timeframe — it manages all `.EQ` symbols itself) with
   **AutoTrading enabled**.

## Inputs (defaults are demo-safe)
| Input | Default | Meaning |
|---|---|---|
| `InpFeedURL` | …`@main/equity-signals.json` | the feed to poll |
| `InpPollSeconds` | 30 | poll cadence (feed refreshes ~15 min) |
| `InpRiskPct` | 0.5 | risk per trade, % of equity (sizes the lot off the signal's stop) |
| `InpMaxAgeMin` | 120 | ignore a signal whose entry bar is older than this |
| **`InpAllowLive`** | **false** | **hard guard — `false` trades DEMO accounts only.** Keep false for the pilot. |
| `InpMaxOpenPerSym` | 1 | max concurrent EA positions per symbol |
| `InpMaxLot` | 50 | absolute lot safety cap |

## What it does
- Entry: market order on a fresh `armed` signal, **SL from the feed, no fixed TP**.
- Exit: **trailing runner** — arms at **+1R**, then trails the stop **1R behind the best price**, with a
  hard time-horizon from `trail_hold_bars × timeframe` (200 bars → ~8 days on h1, ~2 days on m15).
- Sizing: `InpRiskPct` of equity ÷ the stop distance (via the symbol's tick value).
- Dedup: a signal id is acted once (open-position comment + a persisted `viking_equity_acted.csv`).
- Respects `demo_only` and the feed's `kill_switch`.

## Discipline / known limitations (read before trusting it)
- **This is a scaffold — test it on the demo account first.** Watch a few signals fill and confirm the
  entry/stop/size/trail match the feed before you consider real capital. Real-capital promotion is
  gated on the live demo record corroborating the backtest (the whole reason we're here).
- The JSON reader is tailored to *this* feed's flat schema, not a general parser. If the feed schema
  changes, the EA must change with it.
- After an EA **restart**, trailing state for already-open positions is rebuilt best-effort from the
  current SL and given a generous default horizon — it won't perfectly reconstruct the original R.
  Flat/low activity between restarts makes this a non-issue in practice; just be aware.
- Fills are market; MT5 slippage/stock spreads apply and will run the live result a touch below the
  frictionless backtest (expected).

## The strategies (why these four)
All four cleared the **causal** 3-year backtest both-OOS-positive (after the look-ahead fix):
holygrail_eq +0.31R, twob_eq +0.31R, holygrail_eq_m15 +0.30R, volbreak_eq +0.08R. `orb_eq` was left
out (thin, fails the 2nd OOS half, different exit). They share the same trailing-runner exit, which is
why one EA handles all of them uniformly.
