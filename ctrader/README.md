# Viking Invest cTrader Signal Bridge

Phase 1 prototype of the broker auto-execution roadmap. A **cTrader cBot**
(C#) that polls the public `signals.json` feed every 30 seconds and
places **demo-account** market orders for newly-triggered intraday
signals from the Viking Invest 4/4 confluence detector.

cTrader was picked over MT5 because:
- **IC Markets** offers cTrader natively as one of their three primary
  platforms (alongside MT4 and MT5).
- The .NET / C# stack has first-class HTTP and JSON support, so the
  bot stays small and auditable.
- cTrader's order API is cleaner than MQL5 — volume is in units (not
  lots), SL/TP are specified in pips relative to entry, and the
  `Robot` base class handles timer / lifecycle plumbing for us.

> **Demo-only by default.** The cBot refuses to start on a live
> account unless `AllowLive = true` is set explicitly in the parameters.
> This is deliberate — going live is a separate review step, not a
> tickbox.

## Architecture

```
GitHub Action (every ~10 min)
  └─ detect_triggers.py            (server-side 4/4 detector)
     └─ alerts-state.json          (raw per-pair detector state)
        └─ build_signals_json.py   (Phase 1 transformer)
           └─ signals.json         (the broker contract)
              └─ CDN (jsDelivr)
                 └─ cTrader cBot polls every 30s
                    └─ IC Markets broker (demo account)
```

Idempotency key on each signal:

```
id = "{pair}:{creator_ts_epoch_ms}:{wick|fib}"
```

The bot persists every id it has placed an order for in
`%LocalAppData%\VikingInvest\seen_ids.txt` (Windows) so the dedup list
survives cBot restarts, cTrader updates, and even VPS reboots.

## Install

1. Open **cTrader** → click the **Automate** tab on the left rail.
2. Right-click **Robots** in the Navigator pane → `New Robot`.
3. Name it `VikingInvestSignalBridge` and replace the template code
   with the contents of `VikingInvestSignalBridge.cs`.
4. Click the **Build** icon (or `F5`). The bot should compile with
   zero errors.
5. Drag the bot from `Robots` onto any chart (the chart symbol is
   irrelevant — the bot is multi-symbol).
6. In the parameters dialog, leave the defaults, click **Play**.

The bot logs to cTrader's `Log` tab (bottom of the Automate workspace).
You should see `✅ [VikingInvest] Bridge initialised.` within a second.

### Required cTrader setting

The bot uses `System.Net.Http.HttpClient` which is part of the .NET
framework cTrader bundles. **No URL whitelist is needed** — cTrader
allows all outbound HTTP/HTTPS by default for `FullAccess` cBots.
(Compare to MT5 where every URL has to be manually whitelisted under
Tools → Options → Expert Advisors.)

## Parameters

| Group | Parameter | Default | Notes |
|---|---|---|---|
| Feed | `SignalsUrl` | jsDelivr `/gh/` path | Override only if pointing at a fork |
| Feed | `PollSeconds` | `30` | Lower than ~15s is wasted bandwidth |
| Risk | `RiskPctPerTrade` | `0.5` | Multiplied by the signal's `r_size` (1.0 wick, 0.5 fib) |
| Risk | `MaxOpenPositions` | `5` | Counts only cBot-placed positions (by label) |
| Risk | `MaxSpreadPips` | `3.0` | Skip entry if spread is blown out |
| Risk | `MaxSignalAgeMin` | `60` | Skip triggered signals older than N minutes |
| Identity | `OrderLabel` | `VikingInvest` | Separates cBot orders from manual ones |
| Safety | `AllowLive` | `false` | **Safety gate.** Must be `true` to run on a non-demo account |
| Safety | `DryRun` | `false` | If true, logs what it would have placed without sending orders |
| Debug | `VerboseLog` | `true` | Print per-signal diagnostic info |

## Risk math

For every triggered signal:

```
risk_amount   = Account.Equity × RiskPctPerTrade / 100 × r_size
stop_distance = |entry − stop|
stop_pips     = stop_distance / symbol.PipSize
volume_units  = risk_amount / (stop_pips × symbol.PipValue)
volume_units  = NormalizeVolumeInUnits(volume_units, RoundingMode.Down)
```

A signal with `r_size = 0.5` (Fib half-size methodology used for
commodities + indices) places **half** the volume that the same stop
distance would on a `r_size = 1.0` (wick) pair. This matches the
weighting the dashboard's net-R numbers use, so the bot's P&L
distribution should track the backtest within slippage tolerance.

## What runs on the server, what runs on the broker

| Concern | Lives on |
|---|---|
| 4/4 confluence detection | GitHub Action (Python: `detect_triggers.py`) |
| Signal lifecycle (armed → triggered → invalidated) | GitHub Action |
| Public `signals.json` contract | GitHub Action → jsDelivr |
| Dedup by idempotency key | cBot (persisted to disk) |
| Volume sizing | cBot (account-equity-aware) |
| Spread filter, max-positions cap, demo-only gate | cBot |
| SL/TP attached to position | cBot (survives broker restart) |
| Order placement | cBot → IC Markets cTrader broker |

The split keeps the cBot small (~350 lines) and stateless beyond the
seen-ids file — which means a VPS restart, a network blip, or even a
fresh cTrader install never causes a re-fire on the same setup.

## VPS deployment notes

1. **Cheapest reliable option:** Contabo VPS S (~€5/mo) on Windows
   Server 2022. RDP into it once, install cTrader, attach the cBot,
   log off. RDP back in once a week to check the log.
2. **Why VPS, not your laptop:** cTrader only places new orders while
   the bot is running. If your laptop sleeps with positions open, the
   SL/TP still execute at the broker — that's why we attach them when
   the order is placed — but if a *new* signal arrives while you're
   offline, the cBot can't see it.
3. **Latency:** doesn't really matter for this strategy. Signals fire
   on closed 15m bars, the cBot polls every 30s, and entries are at
   structural levels that don't need millisecond execution. Any
   commodity VPS will do.

## Phase 2 — Kill-switch

The cBot polls `kill-switch.json` from the CDN on every cycle. When
the `killed` flag is `true` the cBot **stops placing new orders** but
keeps polling so the dashboard can still see the bot is alive. Existing
positions continue to be managed by the broker (the SL/TP were attached
at order time so they survive without the bot running).

**Flipping the switch:**

1. Repo → Actions tab → **"Set Kill Switch"** workflow → **Run workflow**
2. Set `killed = true` and fill in a `reason` (mandatory — goes into the
   audit trail AND surfaces on the dashboard's Performance tab so
   investors see why the bot is paused).
3. Click **Run workflow**. CDN purges automatically. The cBot picks up
   the new state within ~30 seconds of the next poll.
4. To resume, run the workflow again with `killed = false`.

**Failure mode:** if the kill-switch fetch itself fails (CDN hiccup,
network blip), the cBot **keeps the last-known state** rather than
flipping to a default. This is "fail-open": a transient network issue
shouldn't pause a healthy bot. To make the bot fail closed on fetch
error, edit `FetchKillSwitch()` in the .cs file.

## Phase 3 — Executions journal

Every action the cBot takes is appended to
`%LocalAppData%\VikingInvest\executions.jsonl` (Windows) on the VPS.
Line-delimited JSON, one event per line, append-only, never rewritten.

**Three event types:**

| `event` | When | Key fields |
|---|---|---|
| `placed` | Order accepted by broker | `signal_id`, `position_id`, `entry_attempt`, `entry_filled`, `slippage_pips`, `volume_units` |
| `rejected` | Order rejected | `signal_id`, `reason` |
| `closed` | Position closed (TP / SL / manual) | `signal_id`, `position_id`, `exit_price`, `net_profit`, `realized_r`, `reason` |

Plus shared fields on every row: `ts`, `pair`, `symbol`, `dir`,
`stop`, `target`, `r_size`, `account_mode` (`demo` / `live`),
`account` (the broker account number).

**Reconciliation into the dashboard:**

1. Copy `executions.jsonl` off the VPS (RDP file transfer, shared
   folder, or just open it in Notepad and copy/paste the content).
2. On the dashboard's Performance tab, click **📥 Import Executions**
   in the signal-log strip.
3. Pick the file. The dashboard parses every line, merges each event
   into the in-browser signal log (matched by `signal_id`), and the
   Performance tab's Trade Log immediately shows broker fills alongside
   detector predictions.

After import you'll see per-trade fields you can't get from the
backtest alone:

- **Slippage**: difference between detector entry and broker fill
- **Realized R**: actual P&L divided by risk amount, vs the predicted
  ±1R from the backtest
- **Stop-hit vs target-hit attribution**: ground-truth resolution
  reason, not the detector's inference

## Phase 3.5 — Auto-publish executions to the repo

The manual JSONL import is the fallback. The primary path is the cBot
firing a `repository_dispatch` event on every execution — the dashboard
then sees broker fills in near-real-time without copy/paste.

**Setup (one-time, ~5 minutes):**

1. **Create a fine-scoped GitHub PAT**:
   - GitHub → Settings → Developer settings → Personal access tokens →
     Fine-grained tokens → **Generate new token**
   - Repository access: **Only select repositories** →
     `ModernViking1/vikinginvest-prices`
   - Permissions → Repository → **Contents: Read and Write**
   - Expiration: 90 days (rotate before it lapses)
   - Copy the token (starts with `github_pat_…`) — you won't see it again

2. **Configure the cBot parameters** (cTrader → Robot instance → Edit):
   - `AutoPublishToRepo` → **true**
   - `GhPersonalAccessToken` → paste the PAT
   - Leave `GhRepoOwner` / `GhRepoName` at defaults unless using a fork
   - Click **OK** and restart the cBot

3. **Verify**: place a dry-run order (set `DryRun = true` temporarily).
   Within ~30 seconds you should see a new commit on the repo titled
   `cbot: placed — btcusd:…:wick`. The dashboard's Performance tab
   should also auto-populate the new fill within a minute.

**Architecture under the hood:**

```
cBot execution event
  └─ WriteExecution() — appends to executions.jsonl (always)
     └─ DispatchExecutionAsync() — POSTs to GitHub API (if PAT set)
        └─ repository_dispatch:cbot-execution
           └─ .github/workflows/ingest-cbot-execution.yml
              └─ ingest_execution.py — validates + dedups + appends
                 └─ git commit + push + CDN purge → executions.json
                    └─ Dashboard polls via _fetchExecutionsFromCDN()
                       └─ Merges into in-browser signal log
                          └─ Performance Trade Log shows the row
```

**Security model:**

- PAT scope: **only** `contents:write` on this one repo. Cannot read
  any other repo, cannot administer the org, cannot create new repos.
  Worst case if exfiltrated: someone can spam commits to this repo
  until you revoke the token. No financial data is at risk; the PAT
  is not the broker credential.
- PAT lives in **cTrader's local config** on the VPS, not in source
  control. The cBot reads it from a `[Parameter]` field that cTrader
  stores in the user's local `cbot-instances.config`.
- The workflow validates every payload: required fields, allowed
  events, plausible epoch-ms range, dedup by `(signal_id, event, ts)`.
  A malformed dispatch (or even an adversarial one) is rejected at
  the workflow without ever touching `executions.json`.

**Fallback semantics:**

- Network failure → dispatch fails → the local JSONL still has the
  row. Run a manual JSONL import once connectivity returns; dedup
  on the workflow side means re-publishing an already-ingested row
  is a no-op.
- PAT expires → 401 on dispatch → row stays local → rotate the PAT,
  manual-import the gap, autosync resumes.
- Rate limit on GitHub's dispatch API → 60 dispatches/hour at the
  fine-scoped tier. The cBot's 5-positions cap means realistic
  steady-state is well under this even on a busy day.

## Going live (eventually)

When you're ready to move from demo → live:

1. Run on the demo account for at least **2 weeks** to validate fill
   distribution vs the backtest predictions.
2. Reconcile demo-account P&L against the dashboard's recorded WR.
   Expect 3–6 pp slippage haircut — that's normal for retail fills.
3. Flip `AllowLive = true`, lower `RiskPctPerTrade` to `0.25` for the
   first week, and set `MaxOpenPositions = 2`.
4. Step risk + concurrency back up over the following month if the
   live distribution tracks the demo one.
5. Add a kill-switch file the cBot checks every poll
   (`%LocalAppData%\VikingInvest\kill.txt`). Future Phase 3 work.

## Reconciliation back into the dashboard

Phase 3 (not built yet) will write each cBot-placed order outcome to
a small `executions.json` on the repo. The dashboard's Performance
tab already merges multiple sources for the Trade Log — adding a third
source (live cBot fills) lets investors see paper-trade and live-trade
results side-by-side, computed from the same signal universe.

## File index

- `VikingInvestSignalBridge.cs` — the cBot source
- `README.md` — this file

## Support

Issues, feature requests: open an issue on the repo or
`engineering@vikinginvest.ai`.
