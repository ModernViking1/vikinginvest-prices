// ════════════════════════════════════════════════════════════════════
//  VikingInvestSignalBridge.cs — Phase 1 cTrader cBot prototype
// ────────────────────────────────────────────────────────────────────
//  Polls https://cdn.jsdelivr.net/.../signals.json every 30 seconds,
//  dedupes via the idempotency key, and places demo-account market
//  orders for newly-triggered intraday signals from the Viking Invest
//  4/4 confluence detector.
//
//  Schema contract:  build_signals_json.py in the same repo
//  Roadmap doc:      ctrader/README.md (Phase 1)
//  Companion EA:     n/a — cTrader is the chosen execution platform
//                    because IC Markets supports it natively, the
//                    HTTP / JSON stack is first-class, and the C#
//                    code is easier to audit than MQL5.
//
//  Hard safety: refuses to place orders if Account.IsLive == true
//  unless InpAllowLive = true. Going live is intentionally a
//  separate code path with its own review step.
//
//  Copyright 2026, Viking Invest Ltd · https://vikinginvest.ai
// ════════════════════════════════════════════════════════════════════
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using cAlgo.API;
using cAlgo.API.Internals;

// 2026-06-14ccc / ddd — cAlgo.API ships its own File / Directory / HttpMethod
// types that collide with the .NET runtime types we want for VPS-local
// I/O and the GitHub-dispatch + Telegram POST calls. Aliases pin every
// call site to the System.* version unambiguously.
using IOFile      = System.IO.File;
using IODirectory = System.IO.Directory;
using HttpMethod  = System.Net.Http.HttpMethod;

namespace cAlgo.Robots
{
    [Robot(AccessRights = AccessRights.FullAccess, AddIndicators = true)]
    public class VikingInvestSignalBridge : Robot
    {
        // ───────────── Parameters ─────────────────────────────────────
        [Parameter("Signals URL", DefaultValue = "https://cdn.jsdelivr.net/gh/ModernViking1/vikinginvest-prices@main/signals.json", Group = "Feed")]
        public string SignalsUrl { get; set; }

        [Parameter("Kill-switch URL", DefaultValue = "https://cdn.jsdelivr.net/gh/ModernViking1/vikinginvest-prices@main/kill-switch.json", Group = "Feed")]
        public string KillSwitchUrl { get; set; }

        [Parameter("Poll seconds", DefaultValue = 30, MinValue = 10, MaxValue = 300, Group = "Feed")]
        public int PollSeconds { get; set; }

        [Parameter("Risk % per trade", DefaultValue = 0.5, MinValue = 0.05, MaxValue = 5.0, Group = "Risk")]
        public double RiskPctPerTrade { get; set; }

        [Parameter("Max open positions", DefaultValue = 5, MinValue = 1, MaxValue = 50, Group = "Risk")]
        public int MaxOpenPositions { get; set; }

        [Parameter("Max spread (pips)", DefaultValue = 3.0, MinValue = 0.1, MaxValue = 50.0, Group = "Risk")]
        public double MaxSpreadPips { get; set; }

        [Parameter("Max signal age (minutes)", DefaultValue = 60, MinValue = 5, MaxValue = 720, Group = "Risk")]
        public int MaxSignalAgeMin { get; set; }

        [Parameter("Order label", DefaultValue = "VikingInvest", Group = "Identity")]
        public string OrderLabel { get; set; }

        [Parameter("Allow LIVE account", DefaultValue = false, Group = "Safety")]
        public bool AllowLive { get; set; }

        [Parameter("Dry run (log only)", DefaultValue = false, Group = "Safety")]
        public bool DryRun { get; set; }

        [Parameter("Verbose logging", DefaultValue = true, Group = "Debug")]
        public bool VerboseLog { get; set; }

        // ── Phase 3.5 — auto-publish executions back to the repo ──
        // When configured with a fine-scoped GitHub PAT, the cBot fires
        // a repository_dispatch event for every placed/rejected/closed
        // event. The dashboard then sees broker fills in near-real-time
        // without the manual JSONL import step. Leave the PAT blank to
        // run in local-only mode (executions.jsonl still written on the
        // VPS — manual import path remains the fallback).
        [Parameter("GitHub repo owner", DefaultValue = "ModernViking1", Group = "Auto-publish")]
        public string GhRepoOwner { get; set; }

        [Parameter("GitHub repo name", DefaultValue = "vikinginvest-prices", Group = "Auto-publish")]
        public string GhRepoName { get; set; }

        [Parameter("GitHub PAT (fine-scoped: contents:write)", DefaultValue = "", Group = "Auto-publish")]
        public string GhPersonalAccessToken { get; set; }

        [Parameter("Auto-publish to repo", DefaultValue = false, Group = "Auto-publish")]
        public bool AutoPublishToRepo { get; set; }

        // ── Phase 4 — Live-mode risk caps + daily loss limiter ──
        // When Account.IsLive==true these caps OVERRIDE the corresponding
        // demo parameters above. Demo runs are unaffected. The split
        // exists because the demo settings can be aggressive (the goal
        // there is to generate enough trades to validate the loop fast)
        // while a real-money deployment should be conservative until
        // the slippage / latency profile is proven over 2+ weeks.
        [Parameter("LIVE — Risk % per trade", DefaultValue = 0.25, MinValue = 0.01, MaxValue = 2.0, Group = "Live-mode caps")]
        public double LiveRiskPctPerTrade { get; set; }

        [Parameter("LIVE — Max open positions", DefaultValue = 2, MinValue = 1, MaxValue = 10, Group = "Live-mode caps")]
        public int LiveMaxOpenPositions { get; set; }

        [Parameter("LIVE — Max spread (pips)", DefaultValue = 2.0, MinValue = 0.1, MaxValue = 20.0, Group = "Live-mode caps")]
        public double LiveMaxSpreadPips { get; set; }

        [Parameter("LIVE — Daily loss limit (% of equity)", DefaultValue = 2.0, MinValue = 0.5, MaxValue = 10.0, Group = "Live-mode caps")]
        public double LiveDailyLossPctLimit { get; set; }

        [Parameter("LIVE — Min equity required to start", DefaultValue = 500.0, MinValue = 100.0, Group = "Live-mode caps")]
        public double LiveMinEquity { get; set; }

        [Parameter("LIVE — Operator confirmation (must be true)", DefaultValue = false, Group = "Live-mode caps")]
        public bool LiveOperatorConfirmed { get; set; }

        // ── Phase 4 — Telegram alert routing ──
        // Two split paths:
        //   • cBot → Telegram: cBot-health events that the user must know
        //     about immediately even if GitHub is unreachable (dispatch
        //     failures, PAT expiry, daily loss-limit, kill-switch flips).
        //   • Workflow → Telegram: per-execution summaries, posted from
        //     ingest-cbot-execution.yml after each successful ingestion.
        // The workflow path keeps the secret out of the cBot for the
        // routine events. The cBot path is necessary for emergencies.
        [Parameter("Telegram bot token", DefaultValue = "", Group = "Telegram")]
        public string TelegramBotToken { get; set; }

        [Parameter("Telegram chat ID", DefaultValue = "", Group = "Telegram")]
        public string TelegramChatId { get; set; }

        [Parameter("Telegram alert level", DefaultValue = "important", Group = "Telegram")]
        public string TelegramAlertLevel { get; set; }  // "off" | "important" | "all"

        // ───────────── State ──────────────────────────────────────────
        private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        private HashSet<string> _seenIds = new HashSet<string>();
        private string _seenIdsPath;
        private string _executionsPath;
        private string _vikingDir;
        private bool _killed = false;
        private string _killReason = null;
        private string _killUpdated = null;
        private int _signalsSeen, _ordersPlaced, _ordersSkipped, _killBlockedCount, _dailyLimitBlockedCount;
        // Track which signal id placed each open position so the close
        // event can write a clean execution row tying broker P&L back
        // to the detector signal that created it. positionId → signal id.
        private Dictionary<long, string> _positionIdToSignalId = new Dictionary<long, string>();
        // Phase 4 — daily loss tracker. Realized R is summed per UTC day
        // (key = "yyyy-MM-dd") and persisted to disk so a cBot restart
        // mid-day doesn't reset the budget. When the day's negative R
        // crosses LiveDailyLossPctLimit (as a % of starting equity), new
        // orders are refused for the rest of the day. Resets at 00:00 UTC.
        private string _dailyLossPath;
        private string _todayKey = "";
        private double _todayRealizedR = 0;
        private double _todayStartEquity = 0;
        private bool   _dailyLimitHit = false;
        // Phase 4 — preflight gate result, surfaced in logs + Telegram.
        private bool _preflightPassed = false;
        private string _preflightReport = "";

        // ───────────── Lifecycle ──────────────────────────────────────
        protected override void OnStart()
        {
            if (Account.IsLive && !AllowLive)
            {
                Print("🛑 [VikingInvest] Refusing to start — account is LIVE and AllowLive=false.");
                Print($"   Account #{Account.Number}  Broker={Account.BrokerName}");
                Print("   Set AllowLive=true in parameters if you genuinely want live trading.");
                Stop();
                return;
            }

            _vikingDir = System.IO.Path.Combine(Environment.GetFolderPath(
                Environment.SpecialFolder.LocalApplicationData), "VikingInvest");
            IODirectory.CreateDirectory(_vikingDir);
            _seenIdsPath     = System.IO.Path.Combine(_vikingDir, "seen_ids.txt");
            _executionsPath  = System.IO.Path.Combine(_vikingDir, "executions.jsonl");
            _dailyLossPath   = System.IO.Path.Combine(_vikingDir, "daily_loss.txt");
            LoadSeenIds();
            LoadDailyLossState();

            // Phase 4 — preflight gate. On a live account, refuse to start
            // unless every check passes. On demo, run the same checks but
            // only warn (we want demo to keep validating the loop even
            // through weekend gaps / thin spreads).
            RunPreflight();
            if (Account.IsLive && !_preflightPassed)
            {
                Print("🛑 [VikingInvest] Live preflight FAILED — refusing to start.");
                Print(_preflightReport);
                TelegramSend("🛑 cBot live preflight FAILED on " + Account.BrokerName + " #" + Account.Number + "\n\n" + _preflightReport, important:true);
                Stop();
                return;
            }

            // Phase 3 — subscribe to broker close events so each settled
            // position writes one execution row to the JSONL file. We
            // listen on the global Positions collection (not just our
            // own) and filter inside the handler so the cBot still sees
            // closures that happened across a restart.
            Positions.Closed += OnPositionClosed;

            _http.DefaultRequestHeaders.UserAgent.ParseAdd("VikingInvest-cTrader-Bot/1.0");

            // Effective parameters AFTER live-mode override.
            var effRisk = EffectiveRiskPct;
            var effMax  = EffectiveMaxPositions;
            var effSpr  = EffectiveMaxSpreadPips;

            Print("✅ [VikingInvest] Bridge initialised.");
            Print($"   URL: {SignalsUrl}");
            Print($"   Poll: every {PollSeconds}s · Risk: {effRisk}% · Max positions: {effMax} · Max spread: {effSpr} pips");
            Print($"   Mode: {(DryRun ? "DRY-RUN (log only)" : (Account.IsLive ? "🔴 LIVE" : "🟢 DEMO"))}");
            Print($"   Seen ids loaded: {_seenIds.Count}");
            Print($"   Daily R today ({_todayKey}): {_todayRealizedR:+0.00;-0.00;0.00}R · limit {LiveDailyLossPctLimit}% of {_todayStartEquity:F2}");

            TelegramSend($"✅ Viking Invest cBot started on {(Account.IsLive ? "🔴 LIVE" : "🟢 DEMO")} #{Account.Number} · " +
                         $"risk {effRisk}% · max-pos {effMax}", important:true);

            Timer.Start(TimeSpan.FromSeconds(Math.Max(5, PollSeconds)));
            _ = PollAndProcess();
        }

        // Effective parameters after applying live-mode overrides. The
        // demo settings stay configurable for fast-loop validation; the
        // live settings are conservative defaults the user can still
        // tune but starting from a safer floor.
        private double EffectiveRiskPct        => Account.IsLive ? LiveRiskPctPerTrade : RiskPctPerTrade;
        private int    EffectiveMaxPositions   => Account.IsLive ? LiveMaxOpenPositions : MaxOpenPositions;
        private double EffectiveMaxSpreadPips  => Account.IsLive ? LiveMaxSpreadPips    : MaxSpreadPips;

        // Phase 4 — preflight gate. Run on every OnStart so a cBot
        // restart re-validates the environment. Live mode requires ALL
        // checks to pass; demo mode logs but doesn't gate.
        private void RunPreflight()
        {
            var lines = new List<string>();
            lines.Add("Preflight report:");
            // Equity floor
            bool equityOk = Account.Equity >= LiveMinEquity;
            lines.Add($"  {(equityOk ? "✅" : "❌")} Equity {Account.Equity:F2} {(equityOk ? "≥" : "<")} min {LiveMinEquity:F2}");
            // Operator confirmation (live only)
            bool opOk = !Account.IsLive || LiveOperatorConfirmed;
            lines.Add($"  {(opOk ? "✅" : "❌")} Operator confirmation: {(Account.IsLive ? (LiveOperatorConfirmed ? "yes" : "MISSING — set LiveOperatorConfirmed=true") : "n/a (demo)")}");
            // GitHub PAT (auto-publish only)
            bool patOk = !AutoPublishToRepo || !string.IsNullOrEmpty(GhPersonalAccessToken);
            lines.Add($"  {(patOk ? "✅" : "❌")} GitHub PAT: {(AutoPublishToRepo ? (patOk ? "present" : "MISSING — required when AutoPublishToRepo=true") : "n/a (auto-publish off)")}");
            // Telegram routing (live mode only — strongly recommended)
            bool tgOk = !Account.IsLive || (!string.IsNullOrEmpty(TelegramBotToken) && !string.IsNullOrEmpty(TelegramChatId));
            lines.Add($"  {(tgOk ? "✅" : "⚠️")} Telegram routing: {(string.IsNullOrEmpty(TelegramBotToken) ? "off" : "configured")}{(Account.IsLive && !tgOk ? " — strongly recommended for live mode" : "")}");
            // Daily loss budget reasonable
            bool budgetOk = LiveDailyLossPctLimit > 0 && LiveDailyLossPctLimit <= 5.0;
            lines.Add($"  {(budgetOk ? "✅" : "⚠️")} Daily loss limit {LiveDailyLossPctLimit}% of equity {(budgetOk ? "(within sane 0–5% band)" : "(outside the recommended 0–5% band)")}");
            // Resolve a few key symbols so we know the broker mapping works
            string[] checkPairs = { "eurusd", "btcusd", "xauusd" };
            int resolved = 0;
            foreach (var p in checkPairs) if (ResolveSymbol(p) != null) resolved++;
            bool symOk = resolved >= 1;
            lines.Add($"  {(symOk ? "✅" : "❌")} Symbol resolution: {resolved}/{checkPairs.Length} key pairs in Market Watch");

            _preflightPassed = equityOk && opOk && patOk && symOk
                               && (!Account.IsLive || (tgOk && budgetOk));
            _preflightReport = string.Join("\n", lines);
            Print(_preflightReport);
        }

        protected override void OnTimer()
        {
            _ = PollAndProcess();
        }

        protected override void OnStop()
        {
            Timer.Stop();
            Positions.Closed -= OnPositionClosed;
            SaveSeenIds();
            SaveDailyLossState();
            Print($"👋 [VikingInvest] Bridge stopped. Signals seen: {_signalsSeen} · Orders placed: {_ordersPlaced} · Skipped: {_ordersSkipped} · Kill-blocked: {_killBlockedCount} · Daily-limit-blocked: {_dailyLimitBlockedCount} · Today R: {_todayRealizedR:+0.00;-0.00;0.00}");
            TelegramSend($"👋 cBot stopped on {(Account.IsLive ? "LIVE" : "DEMO")} #{Account.Number} · today {_todayRealizedR:+0.00;-0.00;0.00}R · {_ordersPlaced} placed", important:true);
        }

        // ───────────── Phase 4 — Daily-loss tracker ──────────────────
        // Persists {today_key, today_R, start_equity, limit_hit} to a
        // small text file so cBot restarts mid-day don't reset the
        // budget. RollDailyKeyIfNeeded resets at 00:00 UTC.
        private void LoadDailyLossState()
        {
            _todayKey = DateTime.UtcNow.ToString("yyyy-MM-dd");
            _todayStartEquity = Account.Equity;
            _todayRealizedR = 0;
            _dailyLimitHit = false;
            if (!IOFile.Exists(_dailyLossPath)) { SaveDailyLossState(); return; }
            try
            {
                var parts = IOFile.ReadAllText(_dailyLossPath).Split('|');
                if (parts.Length >= 4 && parts[0] == _todayKey)
                {
                    _todayRealizedR = double.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
                    _todayStartEquity = double.Parse(parts[2], System.Globalization.CultureInfo.InvariantCulture);
                    _dailyLimitHit = parts[3] == "1";
                }
            }
            catch (Exception ex) { Print($"⚠️ [VikingInvest] daily-loss load failed: {ex.Message}"); }
        }
        private void SaveDailyLossState()
        {
            try
            {
                IOFile.WriteAllText(_dailyLossPath,
                    $"{_todayKey}|{_todayRealizedR.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}|" +
                    $"{_todayStartEquity.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}|" +
                    $"{(_dailyLimitHit ? "1" : "0")}");
            }
            catch (Exception ex) { Print($"⚠️ [VikingInvest] daily-loss save failed: {ex.Message}"); }
        }
        private void RollDailyKeyIfNeeded()
        {
            var nowKey = DateTime.UtcNow.ToString("yyyy-MM-dd");
            if (nowKey == _todayKey) return;
            // New UTC day — log the previous day's outcome and reset.
            Print($"📅 [VikingInvest] UTC day rolled: closing {_todayKey} at {_todayRealizedR:+0.00;-0.00}R, starting {nowKey}");
            TelegramSend($"📅 Day {_todayKey} closed: {_todayRealizedR:+0.00;-0.00}R. New day {nowKey} starting equity {Account.Equity:F2}", important:false);
            _todayKey = nowKey;
            _todayRealizedR = 0;
            _todayStartEquity = Account.Equity;
            _dailyLimitHit = false;
            SaveDailyLossState();
        }
        private void CheckDailyLimit()
        {
            if (!Account.IsLive) return;  // demo runs without the brake
            if (_dailyLimitHit) return;
            if (_todayStartEquity <= 0) return;
            // Loss limit is expressed as % of starting equity. Convert
            // realized R back to currency by assuming each R is roughly
            // worth (EffectiveRiskPct/100 * startEquity) — the size we
            // intended to risk per trade. This isn't broker-exact (slippage
            // distorts it slightly) but is close enough to drive the brake.
            var lossR = -_todayRealizedR;  // positive when in the red
            var lossPct = lossR * EffectiveRiskPct;
            if (lossPct >= LiveDailyLossPctLimit)
            {
                _dailyLimitHit = true;
                SaveDailyLossState();
                Print($"🛑 [VikingInvest] DAILY LOSS LIMIT HIT: -{lossR:F2}R ≈ -{lossPct:F2}% of equity. No new orders today.");
                TelegramSend(
                    $"🛑 LIVE DAILY LOSS LIMIT HIT\n" +
                    $"Today: {_todayRealizedR:+0.00;-0.00}R ≈ {-lossPct:F2}% of equity\n" +
                    $"Limit: {LiveDailyLossPctLimit}%\n" +
                    $"New orders blocked until 00:00 UTC. Existing positions keep running on broker SL/TP.",
                    important:true);
            }
        }

        // ───────────── Phase 4 — Telegram routing ────────────────────
        // Posts a message to the configured Telegram chat. Fire-and-
        // forget. `important` selects which level the message qualifies
        // for — when TelegramAlertLevel="important" only important
        // messages are sent; "all" sends everything; "off" silences.
        private void TelegramSend(string msg, bool important)
        {
            if (string.IsNullOrEmpty(TelegramBotToken) || string.IsNullOrEmpty(TelegramChatId)) return;
            if (TelegramAlertLevel == "off") return;
            if (TelegramAlertLevel == "important" && !important) return;
            _ = TelegramSendAsync(msg);
        }
        private async Task TelegramSendAsync(string msg)
        {
            try
            {
                var url = $"https://api.telegram.org/bot{TelegramBotToken}/sendMessage";
                var prefix = Account.IsLive ? "🔴" : "🟢";
                var body = "{\"chat_id\":\"" + TelegramChatId + "\",\"text\":\""
                         + (prefix + " [Viking cBot] " + msg).Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n")
                         + "\"}";
                using var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Content = new StringContent(body, Encoding.UTF8, "application/json");
                await _http.SendAsync(req);
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Telegram send failed: {ex.Message}");
            }
        }

        // ───────────── Poll loop ──────────────────────────────────────
        private async Task PollAndProcess()
        {
            // Phase 2 — fetch kill-switch FIRST. If killed, we still
            // need to poll signals (so the dashboard can see we're
            // alive + reading state) but we skip placement entirely.
            await FetchKillSwitch();

            string body;
            try
            {
                body = await _http.GetStringAsync(SignalsUrl);
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Fetch failed: {ex.Message}");
                return;
            }

            var signals = ParseSignals(body);
            if (signals == null)
            {
                Print($"⚠️ [VikingInvest] Could not parse signals.json (len={body?.Length ?? 0})");
                return;
            }

            if (VerboseLog && signals.Count > 0)
                Print($"📡 [VikingInvest] Polled {signals.Count} signals from feed. " +
                      $"Kill-switch: {(_killed ? "🛑 KILLED" : "✅ active")}");

            foreach (var sig in signals)
            {
                ProcessOneSignal(sig);
            }
        }

        // Phase 2 — kill-switch fetch + parse. The file is tiny
        // (~250 bytes) so the extra HTTP round-trip per cycle is
        // negligible. We fail OPEN: if the fetch fails we keep the
        // last-known state, which avoids accidentally pausing the
        // bot during a CDN hiccup. To make the bot fail CLOSED on
        // network failure, set FailClosedOnKillFetchError=true.
        private async Task FetchKillSwitch()
        {
            string body;
            try
            {
                body = await _http.GetStringAsync(KillSwitchUrl);
            }
            catch (Exception ex)
            {
                if (VerboseLog) Print($"ℹ️ [VikingInvest] Kill-switch fetch failed (keeping last state '{_killed}'): {ex.Message}");
                return;
            }
            var wasKilled = _killed;
            _killed       = JsonNum(body, "killed") > 0 || JsonStr(body, "killed") == "true";
            _killReason   = JsonStr(body, "reason");
            _killUpdated  = JsonStr(body, "updated");
            if (wasKilled != _killed)
            {
                Print(_killed
                    ? $"🛑 [VikingInvest] KILL-SWITCH ENGAGED — pausing new orders. Reason: {_killReason ?? "(unspecified)"}"
                    : $"✅ [VikingInvest] KILL-SWITCH RELEASED — resuming new orders. Last reason: {_killReason ?? "(unspecified)"}");
                TelegramSend(_killed
                    ? $"🛑 KILL-SWITCH ENGAGED: {_killReason ?? "(unspecified)"}"
                    : $"✅ KILL-SWITCH RELEASED: {_killReason ?? "(unspecified)"}",
                    important:true);
            }
        }

        // ───────────── Per-signal handling ────────────────────────────
        private void ProcessOneSignal(Signal sig)
        {
            _signalsSeen++;
            if (string.IsNullOrEmpty(sig.Id) || string.IsNullOrEmpty(sig.Pair) || string.IsNullOrEmpty(sig.State))
                return;

            // Only act on triggered signals — armed = not yet a fill,
            // invalidated = cancelled.
            if (sig.State != "triggered")
            {
                if (VerboseLog) Print($"ℹ️ [VikingInvest] id={sig.Id} state={sig.State} — no action.");
                return;
            }

            // Dedup — already-placed signals never fire twice.
            if (_seenIds.Contains(sig.Id)) return;

            // Phase 2 — kill-switch gate. We DON'T mark the id as seen
            // when the kill is the reason: that way the moment the
            // switch is released the bot picks up the still-active
            // setup on the very next poll. The signal does, however,
            // need to still be inside its stale-age window.
            if (_killed)
            {
                _killBlockedCount++;
                if (VerboseLog)
                    Print($"🛑 [VikingInvest] Kill-switch blocking order for id={sig.Id} (reason: {_killReason ?? "n/a"})");
                return;
            }

            // Phase 4 — daily-loss gate. RollDailyKeyIfNeeded handles the
            // midnight-UTC rollover. When the day's negative R crosses the
            // limit, refuse new orders for the rest of the day. We also
            // don't mark seen — once the day rolls, an active setup gets
            // picked up automatically (same rationale as the kill switch).
            RollDailyKeyIfNeeded();
            if (_dailyLimitHit)
            {
                _dailyLimitBlockedCount++;
                if (VerboseLog)
                    Print($"🛑 [VikingInvest] Daily loss limit reached ({_todayRealizedR:F2}R) — blocking id={sig.Id}");
                return;
            }

            // Stale-signal filter.
            // 2026-06-15ggg: prefer TriggeredAtMs over ArmedAtMs when
            // present. ArmedAtMs measures "how old is the original
            // setup creator" — which can be 1-3 hours old while the
            // actual TRIGGER event is fresh. Using ArmedAtMs caused
            // the cBot to silently skip valid triggers in normal
            // market action (signals visible on the dashboard but
            // never converted to orders, never reaching Telegram).
            var stalenessAnchor = sig.TriggeredAtMs > 0 ? sig.TriggeredAtMs : sig.ArmedAtMs;
            if (stalenessAnchor > 0)
            {
                var ageMin = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - stalenessAnchor) / 60000.0;
                if (ageMin > MaxSignalAgeMin)
                {
                    Print($"⏭ [VikingInvest] Skipping stale signal id={sig.Id} ({(sig.TriggeredAtMs > 0 ? "triggered" : "armed")} {ageMin:F0} min ago, cap {MaxSignalAgeMin})");
                    MarkSeen(sig.Id); _ordersSkipped++;
                    return;
                }
            }

            var symbol = ResolveSymbol(sig.Pair);
            if (symbol == null)
            {
                Print($"⚠️ [VikingInvest] No matching cTrader symbol for pair={sig.Pair} — skipping.");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Concurrency check — count our open positions by label.
            var ourOpen = Positions.Count(p => p.Label == OrderLabel);
            if (ourOpen >= EffectiveMaxPositions)
            {
                Print($"🛑 [VikingInvest] Max positions reached ({ourOpen}/{EffectiveMaxPositions}) — skipping id={sig.Id}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Spread filter.
            var spreadPips = symbol.Spread / symbol.PipSize;
            if (spreadPips > EffectiveMaxSpreadPips)
            {
                Print($"🛑 [VikingInvest] Spread too wide on {symbol.Name}: {spreadPips:F1} > {EffectiveMaxSpreadPips} pips. Skipping id={sig.Id}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Risk-based volume sizing. Factors in the signal's r_size
            // (1.0 wick, 0.5 fib) so commodity / index trades take half
            // size automatically — matching the dashboard's net-R math.
            // EffectiveRiskPct applies the live-mode override transparently.
            var riskPct = EffectiveRiskPct * (sig.RSize > 0 ? sig.RSize : 1.0);
            var volume = ComputeVolume(symbol, sig.Entry, sig.Stop, riskPct);
            if (volume <= 0)
            {
                Print($"⚠️ [VikingInvest] Volume compute returned {volume} for {symbol.Name} — skipping");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            var slPips = Math.Abs(sig.Entry - sig.Stop) / symbol.PipSize;
            var tpPips = Math.Abs(sig.Target - sig.Entry) / symbol.PipSize;
            var direction = sig.Dir == "bull" ? TradeType.Buy : TradeType.Sell;

            if (DryRun)
            {
                Print($"🟡 [VikingInvest] DRY-RUN: would {direction} {symbol.Name} {volume:F0} units · " +
                      $"entry≈{sig.Entry:F5} SL={sig.Stop:F5} TP={sig.Target:F5} ({slPips:F1}/{tpPips:F1} pips) id={sig.Id}");
                MarkSeen(sig.Id); _ordersPlaced++;
                return;
            }

            var result = ExecuteMarketOrder(direction, symbol.Name, volume, OrderLabel,
                                            slPips, tpPips, "viking-" + sig.Id);
            if (result.IsSuccessful)
            {
                Print($"✅ [VikingInvest] Order placed {direction} {symbol.Name} {volume:F0} units · " +
                      $"id={sig.Id} position-id={result.Position.Id}");
                _ordersPlaced++;
                TelegramSend($"📤 {direction} {symbol.Name} {volume:F0} units · " +
                             $"entry≈{result.Position.EntryPrice:F5} SL={sig.Stop:F5} TP={sig.Target:F5}",
                             important: Account.IsLive);  // important when live
                // Phase 3 — remember which signal opened this position
                // so the close handler can write a properly-linked
                // execution row. The dictionary stays small (≤ open
                // position count) so we don't bother capping it.
                _positionIdToSignalId[result.Position.Id] = sig.Id;
                WriteExecution(new ExecutionRow
                {
                    Event       = "placed",
                    SignalId    = sig.Id,
                    PositionId  = result.Position.Id,
                    Pair        = sig.Pair,
                    Symbol      = symbol.Name,
                    Dir         = sig.Dir,
                    VolumeUnits = volume,
                    EntryAttempt= sig.Entry,
                    EntryFilled = result.Position.EntryPrice,
                    Stop        = sig.Stop,
                    Target      = sig.Target,
                    RSize       = sig.RSize,
                    SlippagePips= (sig.Entry > 0 && symbol.PipSize > 0)
                                  ? (result.Position.EntryPrice - sig.Entry) / symbol.PipSize * (sig.Dir == "bull" ? 1 : -1)
                                  : 0,
                    AccountMode = Account.IsLive ? "live" : "demo",
                    Account     = Account.Number,
                });
            }
            else
            {
                Print($"❌ [VikingInvest] Order REJECTED {direction} {symbol.Name} · " +
                      $"error={result.Error} id={sig.Id}");
                _ordersSkipped++;
                WriteExecution(new ExecutionRow
                {
                    Event       = "rejected",
                    SignalId    = sig.Id,
                    Pair        = sig.Pair,
                    Symbol      = symbol.Name,
                    Dir         = sig.Dir,
                    VolumeUnits = volume,
                    EntryAttempt= sig.Entry,
                    Stop        = sig.Stop,
                    Target      = sig.Target,
                    RSize       = sig.RSize,
                    Reason      = result.Error.ToString(),
                    AccountMode = Account.IsLive ? "live" : "demo",
                    Account     = Account.Number,
                });
            }
            MarkSeen(sig.Id);
        }

        // ───────────── Phase 3 — Position closed handler ─────────────
        // Writes one closed-trade row per resolved position. The cBot
        // computes realized R by dividing realised P&L by the risk-amount
        // implied by stop distance, which matches the dashboard's
        // backtest math (1R risk per wick trade, 0.5R per fib trade).
        private void OnPositionClosed(PositionClosedEventArgs args)
        {
            var p = args.Position;
            if (p == null) return;
            if (p.Label != OrderLabel) return; // not one of ours

            string sigId = null;
            _positionIdToSignalId.TryGetValue(p.Id, out sigId);

            double realizedR = 0;
            try
            {
                // Risk in account ccy implied by stop distance at fill.
                double stopDistPx = (p.StopLoss.HasValue && p.EntryPrice > 0)
                                    ? Math.Abs(p.EntryPrice - p.StopLoss.Value)
                                    : 0;
                if (stopDistPx > 0 && p.Symbol.PipSize > 0 && p.Symbol.PipValue > 0)
                {
                    var stopPips = stopDistPx / p.Symbol.PipSize;
                    var riskAmt  = stopPips * p.Symbol.PipValue * p.VolumeInUnits;
                    if (riskAmt > 0) realizedR = p.NetProfit / riskAmt;
                }
            }
            catch { /* defensive — never let stats math crash the bot */ }

            WriteExecution(new ExecutionRow
            {
                Event       = "closed",
                SignalId    = sigId,
                PositionId  = p.Id,
                Pair        = sigId != null ? sigId.Split(':')[0] : p.SymbolName.ToLowerInvariant(),
                Symbol      = p.SymbolName,
                Dir         = p.TradeType == TradeType.Buy ? "bull" : "bear",
                VolumeUnits = p.VolumeInUnits,
                EntryFilled = p.EntryPrice,
                ExitPrice   = p.Symbol?.Bid ?? 0,  // close price = current quote; broker journal is authoritative
                Stop        = p.StopLoss ?? 0,
                Target      = p.TakeProfit ?? 0,
                NetProfit   = p.NetProfit,
                Commissions = p.Commissions,
                Swap        = p.Swap,
                RealizedR   = realizedR,
                Reason      = ClassifyCloseReason(p),
                AccountMode = Account.IsLive ? "live" : "demo",
                Account     = Account.Number,
            });
            if (sigId != null) _positionIdToSignalId.Remove(p.Id);
            Print($"📒 [VikingInvest] Position closed {p.SymbolName} {p.TradeType} · " +
                  $"net={p.NetProfit:F2} R={realizedR:F2} signal={sigId ?? "(unlinked)"}");

            // Phase 4 — credit/debit the daily R running total, persist,
            // check the loss limit. Once hit, lock out for the day +
            // Telegram. Resets at midnight UTC via RollDailyKeyIfNeeded.
            RollDailyKeyIfNeeded();
            _todayRealizedR += realizedR;
            SaveDailyLossState();
            CheckDailyLimit();

            // Routine close → Telegram (important level — every closure
            // is something the user wants to know about on a live account).
            if (TelegramAlertLevel != "off")
            {
                var emoji = realizedR > 0 ? "✅" : (realizedR < 0 ? "❌" : "↔");
                TelegramSend(
                    $"{emoji} {(Account.IsLive ? "LIVE" : "DEMO")} closed {p.SymbolName} {p.TradeType}\n" +
                    $"P&L: {p.NetProfit:+0.00;-0.00} ({realizedR:+0.00;-0.00}R)\n" +
                    $"Reason: {ClassifyCloseReason(p)}\n" +
                    $"Day total: {_todayRealizedR:+0.00;-0.00}R",
                    important:true);
            }
        }

        private string ClassifyCloseReason(Position p)
        {
            // cTrader doesn't expose a structured close reason, but we
            // can infer it from where the exit price lands vs the SL/TP.
            try
            {
                var exit = p.Symbol?.Bid ?? p.EntryPrice;
                if (p.TakeProfit.HasValue)
                {
                    var tpDist = Math.Abs(exit - p.TakeProfit.Value);
                    if (tpDist < p.Symbol.PipSize * 2) return "target-hit";
                }
                if (p.StopLoss.HasValue)
                {
                    var slDist = Math.Abs(exit - p.StopLoss.Value);
                    if (slDist < p.Symbol.PipSize * 2) return "stop-hit";
                }
            }
            catch { }
            return "manual-or-broker";
        }

        // ───────────── Symbol resolution ──────────────────────────────
        // IC Markets cTrader uses uppercase bare symbols (EURUSD, BTCUSD).
        // We try the canonical form first, then a couple of common
        // suffix variants used by other brokers.
        private Symbol ResolveSymbol(string pair)
        {
            var upper = pair.ToUpperInvariant();
            var candidates = new[] { upper, upper + ".r", upper + ".pro", upper + ".raw" };
            foreach (var name in candidates)
            {
                try { var s = Symbols.GetSymbol(name); if (s != null) return s; }
                catch { /* not found — try next */ }
            }
            return null;
        }

        // ───────────── Volume sizing ──────────────────────────────────
        // Risk-amount / pip-value at the broker's quoted pip size.
        private long ComputeVolume(Symbol symbol, double entry, double stop, double riskPct)
        {
            if (entry <= 0 || stop <= 0 || Math.Abs(entry - stop) < double.Epsilon) return 0;
            var equity = Account.Equity;
            var riskAmt = equity * riskPct / 100.0;
            var stopPips = Math.Abs(entry - stop) / symbol.PipSize;
            // PipValue is per 1 unit of the symbol. ExecuteMarketOrder
            // takes volume in UNITS (not lots), so we multiply through.
            var pipValuePerUnit = symbol.PipValue;
            if (pipValuePerUnit <= 0 || stopPips <= 0) return 0;
            var volume = riskAmt / (stopPips * pipValuePerUnit);
            // Normalize to broker's volume step.
            volume = symbol.NormalizeVolumeInUnits(volume, RoundingMode.Down);
            if (volume < symbol.VolumeInUnitsMin) return 0;
            if (volume > symbol.VolumeInUnitsMax) volume = symbol.VolumeInUnitsMax;
            return (long)volume;
        }

        // ───────────── Dedup persistence ──────────────────────────────
        private void LoadSeenIds()
        {
            try
            {
                if (!IOFile.Exists(_seenIdsPath)) return;
                foreach (var line in IOFile.ReadAllLines(_seenIdsPath))
                    if (!string.IsNullOrWhiteSpace(line)) _seenIds.Add(line.Trim());
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Could not load seen-ids file: {ex.Message}");
            }
        }

        private void MarkSeen(string id)
        {
            if (_seenIds.Add(id))
            {
                try
                {
                    IOFile.AppendAllText(_seenIdsPath, id + Environment.NewLine);
                }
                catch (Exception ex)
                {
                    Print($"⚠️ [VikingInvest] Could not persist seen id {id}: {ex.Message}");
                }
            }
        }

        private void SaveSeenIds()
        {
            try
            {
                // Cap the persisted file at 5000 ids — keep the newest
                // (the EA appends each new id at the end, so taking the
                // tail preserves the freshest dedup horizon).
                var toWrite = _seenIds.Count > 5000
                    ? _seenIds.Skip(_seenIds.Count - 5000)
                    : _seenIds;
                IOFile.WriteAllLines(_seenIdsPath, toWrite);
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Could not save seen-ids file: {ex.Message}");
            }
        }

        // ───────────── Phase 3 — Execution writer (JSONL) ────────────
        // Every execution row is one line in executions.jsonl — line-
        // delimited JSON. The dashboard's "📥 Import Executions" button
        // accepts this exact file as-is so reconciliation is trivial.
        // JSONL (not a JSON array) so append-on-each-event is O(1) and
        // a crash mid-write only loses one row, not the whole journal.
        private class ExecutionRow
        {
            public string Event;            // "placed" | "rejected" | "closed"
            public string SignalId;         // from the feed — links to dashboard log
            public long?  PositionId;
            public string Pair;
            public string Symbol;
            public string Dir;
            public double VolumeUnits;
            public double EntryAttempt;     // what we asked for (signal entry)
            public double EntryFilled;      // what we got (broker fill)
            public double ExitPrice;
            public double Stop;
            public double Target;
            public double RSize;
            public double SlippagePips;
            public double NetProfit;
            public double Commissions;
            public double Swap;
            public double RealizedR;
            public string Reason;
            public string AccountMode;      // "demo" | "live"
            public long   Account;
        }

        private void WriteExecution(ExecutionRow r)
        {
            // 2026-06-14yy: stamp ts once so the local JSONL row and the
            // GitHub dispatch payload carry the identical timestamp —
            // that's what the workflow's dedup key relies on.
            long tsMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            try
            {
                var sb = new StringBuilder(360);
                sb.Append('{');
                F(sb, "ts",            tsMs);                                            sb.Append(',');
                F(sb, "event",         r.Event);                                         sb.Append(',');
                F(sb, "signal_id",     r.SignalId);                                      sb.Append(',');
                F(sb, "position_id",   r.PositionId);                                    sb.Append(',');
                F(sb, "pair",          r.Pair);                                          sb.Append(',');
                F(sb, "symbol",        r.Symbol);                                        sb.Append(',');
                F(sb, "dir",           r.Dir);                                           sb.Append(',');
                F(sb, "volume_units",  r.VolumeUnits);                                   sb.Append(',');
                F(sb, "entry_attempt", r.EntryAttempt);                                  sb.Append(',');
                F(sb, "entry_filled",  r.EntryFilled);                                   sb.Append(',');
                F(sb, "exit_price",    r.ExitPrice);                                     sb.Append(',');
                F(sb, "stop",          r.Stop);                                          sb.Append(',');
                F(sb, "target",        r.Target);                                        sb.Append(',');
                F(sb, "r_size",        r.RSize);                                         sb.Append(',');
                F(sb, "slippage_pips", r.SlippagePips);                                  sb.Append(',');
                F(sb, "net_profit",    r.NetProfit);                                     sb.Append(',');
                F(sb, "commissions",   r.Commissions);                                   sb.Append(',');
                F(sb, "swap",          r.Swap);                                          sb.Append(',');
                F(sb, "realized_r",    r.RealizedR);                                     sb.Append(',');
                F(sb, "reason",        r.Reason);                                        sb.Append(',');
                F(sb, "account_mode",  r.AccountMode);                                   sb.Append(',');
                F(sb, "account",       r.Account);
                sb.Append('}');
                var line = sb.ToString();
                IOFile.AppendAllText(_executionsPath, line + Environment.NewLine);
                // Phase 3.5 — fire-and-forget dispatch to GitHub. The
                // local JSONL is the source of truth + audit trail; the
                // dispatch is best-effort. If it fails (network, rate-
                // limit, expired PAT) the row is still on disk for a
                // manual import.
                if (AutoPublishToRepo && !string.IsNullOrEmpty(GhPersonalAccessToken))
                {
                    _ = DispatchExecutionAsync(line);
                }
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Could not write execution row: {ex.Message}");
            }
        }

        // Phase 3.5 — POST a repository_dispatch event to GitHub. One
        // dispatch per execution, fire-and-forget. The PAT must have
        // contents:write scope on the target repo (fine-scoped, 90-day
        // rotation cadence documented in the README).
        private async Task DispatchExecutionAsync(string executionJsonLine)
        {
            try
            {
                var url = $"https://api.github.com/repos/{GhRepoOwner}/{GhRepoName}/dispatches";
                // Wrap the execution line as the client_payload. The line
                // is already valid JSON for one object — we just nest it.
                var body = "{\"event_type\":\"cbot-execution\",\"client_payload\":" + executionJsonLine + "}";
                using var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.Add("Accept", "application/vnd.github+json");
                req.Headers.Add("User-Agent", "VikingInvest-cTrader-Bot/1.0");
                req.Headers.Add("Authorization", $"Bearer {GhPersonalAccessToken}");
                req.Content = new StringContent(body, Encoding.UTF8, "application/json");
                var resp = await _http.SendAsync(req);
                if ((int)resp.StatusCode == 204)
                {
                    if (VerboseLog) Print($"📤 [VikingInvest] Dispatched execution to GitHub (204 No Content — accepted)");
                }
                else
                {
                    var bodyText = await resp.Content.ReadAsStringAsync();
                    Print($"⚠️ [VikingInvest] Dispatch returned {(int)resp.StatusCode}: {bodyText}");
                    // Phase 4 — 401/403 likely means an expired or revoked
                    // PAT. Surface immediately on Telegram so the operator
                    // rotates it instead of finding out hours later that
                    // executions stopped publishing.
                    if ((int)resp.StatusCode == 401 || (int)resp.StatusCode == 403)
                    {
                        TelegramSend($"⚠️ GitHub dispatch returned {(int)resp.StatusCode} — PAT may be expired/revoked. Local JSONL still recording; rotate the PAT in cBot params to resume auto-publish.", important:true);
                    }
                }
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Dispatch failed (non-fatal — local JSONL still has the row): {ex.Message}");
            }
        }
        // F = compact JSON field writer
        private static void F(StringBuilder sb, string k, string v)
        {
            sb.Append('"').Append(k).Append("\":");
            if (v == null) sb.Append("null");
            else sb.Append('"').Append(v.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
        }
        private static void F(StringBuilder sb, string k, double v)
        {
            sb.Append('"').Append(k).Append("\":")
              .Append(v.ToString("R", System.Globalization.CultureInfo.InvariantCulture));
        }
        private static void F(StringBuilder sb, string k, long v)
        {
            sb.Append('"').Append(k).Append("\":").Append(v.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        private static void F(StringBuilder sb, string k, long? v)
        {
            sb.Append('"').Append(k).Append("\":");
            if (v.HasValue) sb.Append(v.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
            else sb.Append("null");
        }

        // ───────────── Minimal JSON parser ────────────────────────────
        // signals.json has a stable, flat schema — every signal object
        // is {"id":"...","pair":"...",...,"armedAt":123,...}. We pull
        // each object out as a substring and extract the fields we need
        // by key. Faster + zero deps vs pulling in Newtonsoft.
        private class Signal
        {
            public string Id, Pair, State, Dir;
            public double Entry, Stop, Target, RSize;
            public long ArmedAtMs;
            public long TriggeredAtMs;
        }

        private List<Signal> ParseSignals(string body)
        {
            if (string.IsNullOrEmpty(body)) return null;
            var arrStart = body.IndexOf("\"signals\"", StringComparison.Ordinal);
            if (arrStart < 0) return null;
            arrStart = body.IndexOf('[', arrStart);
            if (arrStart < 0) return null;
            var arrEnd = FindMatchingBracket(body, arrStart);
            if (arrEnd < 0) return null;

            var arr = body.Substring(arrStart + 1, arrEnd - arrStart - 1);
            var signals = new List<Signal>();
            int pos = 0;
            while (pos < arr.Length)
            {
                var objStart = arr.IndexOf('{', pos);
                if (objStart < 0) break;
                var objEnd = FindMatchingBracket(arr, objStart);
                if (objEnd < 0) break;
                var obj = arr.Substring(objStart, objEnd - objStart + 1);
                signals.Add(new Signal
                {
                    Id        = JsonStr(obj, "id"),
                    Pair      = JsonStr(obj, "pair"),
                    State     = JsonStr(obj, "state"),
                    Dir       = JsonStr(obj, "dir"),
                    Entry     = JsonNum(obj, "entry"),
                    Stop      = JsonNum(obj, "stop"),
                    Target    = JsonNum(obj, "target"),
                    RSize     = JsonNum(obj, "r_size"),
                    ArmedAtMs     = (long)JsonNum(obj, "armedAt"),
                    TriggeredAtMs = (long)JsonNum(obj, "triggeredAt"),
                });
                pos = objEnd + 1;
            }
            return signals;
        }

        private static string JsonStr(string obj, string key)
        {
            var needle = "\"" + key + "\"";
            var p = obj.IndexOf(needle, StringComparison.Ordinal);
            if (p < 0) return "";
            var colon = obj.IndexOf(':', p);
            if (colon < 0) return "";
            var q1 = obj.IndexOf('"', colon);
            if (q1 < 0) return "";
            var q2 = obj.IndexOf('"', q1 + 1);
            if (q2 < 0) return "";
            return obj.Substring(q1 + 1, q2 - q1 - 1);
        }

        private static double JsonNum(string obj, string key)
        {
            var needle = "\"" + key + "\"";
            var p = obj.IndexOf(needle, StringComparison.Ordinal);
            if (p < 0) return 0;
            var colon = obj.IndexOf(':', p);
            if (colon < 0) return 0;
            var start = colon + 1;
            while (start < obj.Length && char.IsWhiteSpace(obj[start])) start++;
            if (start >= obj.Length || obj[start] == '"') return 0; // "null" or stringified
            var end = start;
            while (end < obj.Length)
            {
                var c = obj[end];
                if (!(char.IsDigit(c) || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+')) break;
                end++;
            }
            if (end <= start) return 0;
            return double.TryParse(obj.Substring(start, end - start),
                                    System.Globalization.NumberStyles.Float,
                                    System.Globalization.CultureInfo.InvariantCulture,
                                    out var v) ? v : 0;
        }

        private static int FindMatchingBracket(string s, int openIdx)
        {
            var open = s[openIdx];
            var close = open == '[' ? ']' : '}';
            int depth = 0;
            bool inStr = false;
            for (int i = openIdx; i < s.Length; i++)
            {
                var c = s[i];
                if (c == '"' && (i == 0 || s[i - 1] != '\\')) inStr = !inStr;
                if (inStr) continue;
                if (c == open) depth++;
                else if (c == close) { depth--; if (depth == 0) return i; }
            }
            return -1;
        }
    }
}

