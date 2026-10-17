# PC / Second-Machine Setup — full local trading stack

How to run the **entire local stack** (MT5 equity bridge + EA, and the cTrader swing/intraday
cBots) on another Windows machine — a second PC or a VPS. Combines the MT5 equity-bridge steps
with the cTrader always-on guidance in [`VPS_SETUP.md`](VPS_SETUP.md), plus a failover runbook.

---

## ⚠️ Rule 0 — ONE machine at a time (per account)

Everything here acts on the **same demo accounts** (MT5 `53081625` IC Markets Global; cTrader
`9563532` Raw Trading Ltd). Running the bridge/EA/cBots on **two machines at once = duplicate
orders** and diverging in-memory trail/ledger state. So a second machine is for exactly one of:

- **Migration** — run on the new machine *instead of* the old one (stop the old one first), or
- **Failover** — set it up fully but leave it **stopped**; only start it if the primary goes down.

Never run both live simultaneously. (See the failover runbook at the bottom.)

---

## What runs locally vs in the cloud

Only three things run on the machine. **Everything else runs in GitHub Actions** (data feeds,
swing/intraday signal generation, backtests, the fetchers) and is unaffected by which machine you use.

| Component | Platform | Role |
|-----------|----------|------|
| `VikingEquityEA` | MT5 terminal | executes the `.EQ` equity book |
| `equity_bridge.py` | Python | generates `equity-signals.json` from MT5 bars for the EA |
| `VikingSwingBridge` + `VikingInvestSignalBridge` | cTrader | swing + intraday books |

---

## 1. Prerequisites

1. **Windows** PC or VPS (2 vCPU / 4 GB RAM min; 4 vCPU / 8 GB comfortable).
2. **Python 3.11+** from python.org — tick *"Add python.exe to PATH"* during install.
3. **MetaTrader 5** (IC Markets Global build), logged into demo **`53081625`**.
4. **cTrader Desktop**, logged into demo **`9563532`** (Raw Trading Ltd).
5. **git** for Windows.

## 2. Repo + Python deps

```powershell
git clone https://github.com/ModernViking1/vikinginvest-prices.git
cd vikinginvest-prices
pip install MetaTrader5 requests
```

## 3. Tokens (persist across reboots)

```powershell
setx GH_TOKEN          "github_pat_...  (contents:write scope)"
setx TELEGRAM_BOT_TOKEN "<bot token>"
setx TELEGRAM_CHAT_ID   "<chat id>"
```
Open a **new** PowerShell afterwards so they load. (`GH_TOKEN` powers the executions push +
watchdog heartbeat; the Telegram vars power entry/exit alerts. All optional — the bridge still
trades without them, just no push/alerts.)

## 4. MT5 equity book

1. MetaEditor → open `mt5/VikingEquityEA.mq5` → **Compile (F7)** (should be 0 errors).
2. Attach the EA to any chart. In **Inputs**:
   - **`InpLocalFile = equity-signals.json`**  ← so it reads the live bridge file, not the stale CDN.
   - `InpMaxNotionalPctUS = 100` (US cap raise), `InpMaxLossPct = 4` (catastrophic backstop) — defaults.
3. Enable the **Algo Trading** button (must be green) and allow DLL/WebRequest if prompted.
4. Start the bridge (MT5 must be open + logged in):
   ```powershell
   cd vikinginvest-prices
   python equity_bridge.py --poll 900
   ```
5. Confirm the startup banner shows:
   - `symbols (78): US 48, UK 11, DE 7, FR 5, JP 4, HK 3`
   - `strategies (8): holygrail_eq, twob_eq, volbreak_eq, holygrail_eq_m15, turtle_soup_eq, turtle_soup_eq_m15, threepush_eq, fib_eq`
   - `earnings blackout: ON/OFF` (ON once `earnings-dates.json` exists).

## 5. cTrader swing + intraday

Follow [`VPS_SETUP.md`](VPS_SETUP.md) §3 — in short:
1. cTrader → **Automate**, paste/build `ctrader/VikingSwingBridge.cs` and `ctrader/VikingInvestSignalBridge.cs`.
2. **Re-enter every parameter** (they reset on a rebuild) — especially **Auto-publish executions = true**,
   the **GitHub PAT**, owner `ModernViking1`, repo `vikinginvest-prices`, and risk settings.
3. Attach to charts and **Start**. Restart-recovery picks up any open positions on connect.

## 6. Keep the clone current

Use the helper (sidesteps the data-file merge conflict that stalls `git pull`):
```powershell
.\update-bridge.ps1          # stop the bridge first; fetch + hard-reset + verify universe
.\update-bridge.ps1 -Run     # ...and relaunch the bridge
```

## 7. Harden the OS (so it never silently stalls)

From [`VPS_SETUP.md`](VPS_SETUP.md) §2:
- Power plan **High performance**; disable **sleep / hibernate / display-off**.
- **Windows Update:** set active hours / defer auto-restart (an unattended reboot kills everything).
- Disable background-app throttling / efficiency mode for MT5 and cTrader.
- **NTP time sync ON** — signal age/expiry gates depend on an accurate clock.
- Set MT5, cTrader (and `equity_bridge.py` via Task Scheduler "run at logon") to **auto-start on boot**.
- On a VPS: always **disconnect** RDP, never *Sign out* (sign-out stops the apps).

---

## Failover runbook — clean handover without double-trading

**Primary → standby (planned migration):**
1. On the standby machine, complete steps 1–5 above but **leave the bridge + cBots STOPPED**.
2. On the **primary**: Ctrl+C the `equity_bridge.py`, and **Stop** both cTrader cBots.
   (Optionally close the EA / disable Algo Trading on the primary MT5.)
3. On the **standby**: `.\update-bridge.ps1`, start `equity_bridge.py`, Start the cBots.
4. Verify: bridge banner OK; a fresh execution row publishes; Telegram alerts fire.
   Restart-recovery on both the EA and the cBots re-adopts open positions automatically.

**Emergency (primary died unexpectedly):**
- Open positions keep their broker-side stops regardless of which machine is up (the stop lives
  on the broker). So there's no rush — bring the standby up with step 3, and it adopts the open
  positions via restart-recovery. Just make sure the dead primary is truly down (not merely RDP-
  disconnected but still running) before starting the standby, or you'll double-trade.

---

## A note on a VPS vs a second PC

A broker-adjacent **Windows VPS is more reliable than either laptop or PC** for 24/5 running
(no sleep, no Wi-Fi drops, broker-proximity latency) — it's what ends the cBot "zombie stall".
See [`VPS_SETUP.md`](VPS_SETUP.md) for provider/region guidance. The same one-machine-at-a-time
rule applies: migrate to it, don't run it in parallel.

**IC Markets' own free VPS:** offered via a sponsored host (New York City Servers), but note two
catches for *this* setup: (1) the free tier requires a monthly **live** trading volume (~15 lots
Standard) counted on **Forex & metals only** — a **demo** account generates no qualifying volume,
and **stock CFDs don't count**; (2) it's MT4/MT5-oriented, so cTrader would need to be installed
separately (it's a Windows box, so possible, but confirm). For a demo-first stack, a paid general
Windows VPS is usually the simpler route until the book is live with enough FX/metals volume.
