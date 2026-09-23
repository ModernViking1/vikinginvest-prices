# cTrader Always-On / VPS Setup — Viking cBots

Purpose: run `VikingSwingBridge` + `VikingInvestSignalBridge` 24/5 without the silent
stalls we saw on the laptop. cBots only execute while the cTrader desktop app is **open,
connected, and not throttled** — a laptop that stays awake can still drop the bot via a
network blip, an app/OS throttle, or an auto-update. A VPS removes all of these.

## The failure mode this fixes
A cBot can enter a **zombie state** after a brief broker/data disconnect: cTrader still shows
it "running", but its polling thread stopped — no orders, no logs, no `stopped by user` line.
Only a manual stop→start revives it. Root cause is an unstable connection, not the machine
being asleep. A wired, broker-adjacent VPS makes disconnects rare; the watchdog (below)
catches the rest within minutes.

## 1. Pick a VPS
- **Windows** VPS (cTrader Desktop is Windows/macOS). Most Forex VPS are Windows Server 2019/2022.
- **Location near the broker.** Raw Trading Ltd / IC Markets match in Equinix **LD4 (London)** and
  **NY4 (New York)**. Pick a VPS region close to that for low latency / fewer drops.
- **Specs:** 2 vCPU, 4 GB RAM, 40 GB SSD minimum (cTrader + a couple of cBots). 4 vCPU / 8 GB is comfortable.
- **Providers:** Forex-specialist (BeeksFX, CNS, FXVM, Cloudzy) give broker proximity + uptime SLAs;
  or general cloud (AWS Lightsail Windows, Azure, Vultr, Contabo). Aim for **≥99.9% uptime**.

## 2. Harden the OS
- Power plan **High performance**; **disable sleep, hibernate, display-off** entirely.
- **Windows Update:** set active hours / defer auto-restart so a reboot never lands mid-session
  (or reboot only on weekends). An unattended restart kills the bot.
- Disable background-app throttling / "efficiency mode" for cTrader.
- **NTP time sync on** — signal expiry/age gates depend on an accurate clock.
- Lock down **RDP**: strong password, non-default port, IP allow-list or VPN.

## 3. Install & configure cTrader
- Install cTrader Desktop, log into the demo account **9563532** (Raw Trading Ltd).
- Add both cBots (paste the raw `.cs`, Build), attach to their charts, **Start**.
- **Re-enter every parameter and write them down** (they reset on a rebuild):
  - Risk: `Risk % per trade`, `Max position size (lots)`, `Daily loss limit (%)`, `Max concurrent swing positions`.
  - Publish: **`Auto-publish executions to repo` = true**, **`GitHub PAT (contents:write)`**, repo owner `ModernViking1`, repo name `vikinginvest-prices`.
- Note: once set on a stable VPS, these **persist across restarts** (only a *rebuild* wipes them) —
  which ends the re-enter-PAT churn we kept hitting.

## 4. Keep it running when you disconnect RDP
This is the #1 gotcha: **logging OFF an RDP session stops the apps; merely DISCONNECTING keeps them running.**
- Always **disconnect** (close the RDP window), never "Sign out".
- Set cTrader (and the cBots' Start state) to **auto-start on boot / logon** so a VPS reboot self-heals.
- If the session still locks/pauses, use the console-session keep-alive (`tscon`) or a Task-Scheduler
  "run at logon, whether or not user is logged on" task.

## 5. One instance only
**Run the cBots on exactly one machine at a time.** Two live instances on the same account =
duplicate orders. When migrating: **stop the laptop bot first**, then start on the VPS. The VPS cBot
picks up existing open positions automatically via its restart-recovery on connect.

## 6. Monitoring — never be blind for hours again
- **Staleness watchdog** (recommended, in-repo): a scheduled job that alerts on Telegram if the swing
  bot goes silent during session hours — catches the zombie state in ~30 min instead of hours.
- Optional external uptime monitor (UptimeRobot / the VPS provider's) on the VPS itself.

## 7. Migration checklist (laptop → VPS)
1. Write down all cBot parameters (esp. Publish + Risk).
2. Provision + harden the VPS (steps 1–2), install cTrader, log in.
3. Build both cBots, set params, attach charts — but **don't Start yet**.
4. **Stop the laptop cBots.**
5. Start the cBots on the VPS; confirm restart-recovery picks up open positions and `publish=True`.
6. Verify a fresh row publishes and Telegram (R+) alerts fire.
7. Disconnect RDP (don't sign out). Done.
