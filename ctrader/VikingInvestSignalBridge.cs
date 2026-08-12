// ════════════════════════════════════════════════════════════════════
//  VikingInvestSignalBridge.cs — Phase 1 cTrader cBot prototype
// ────────────────────────────────────────────────────────────────────
//  Polls https://cdn.jsdelivr.net/.../signals.json every 30 seconds,
//  dedupes via the idempotency key, and places demo-account market
//  orders for newly-triggered intraday signals from the Viking Invest
//  Viking Edge signal engine.
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

        // 2026-07-03 — FACTOR-RISK BUDGET. Caps aggregate risk per
        // correlated factor-theme (long-USD, short-JPY, ANTIP, METAL, …)
        // so the book can't quietly become one leveraged bet spread across
        // many pairs. Backtest tail (90d macdp replay): the book reached
        // 13× single-trade risk all on one USD direction — the shape of
        // the Jul-2 drawdown. New legs on a saturated factor are SCALED
        // DOWN (not dropped — clustered trades are +EV). Budget is this
        // multiple × one full-risk trade, per factor. Sizing analysis:
        // C=4 keeps ~85% of edge while capping the tail 13→4 units. Set
        // to 0 to disable.
        [Parameter("Factor-risk budget (× 1 trade)", DefaultValue = 4.0, MinValue = 0.0, MaxValue = 50.0, Group = "Risk")]
        public double FactorRiskBudgetR { get; set; }

        // 2026-07-07 — tighter cluster budget for the METAL factor
        // (gold/silver/platinum). They're ~one bet (XAU-XAG ≈ 0.8 correlation),
        // so the global 4× budget lets a 2nd metal in at full size and the
        // correlated drawdown compounds — the H10 theme on XAU/XAG losses. At
        // 1.5 the 2nd correlated metal sizes to ~50% (aggregate metal risk
        // capped at 1.5×), bounding the tail WITHOUT blocking (metals are
        // ~50% WR — a hard block would kill winners too). 0 = use global.
        [Parameter("METAL cluster budget (× 1 trade)", DefaultValue = 1.5, MinValue = 0.0, MaxValue = 10.0, Group = "Risk")]
        public double MetalClusterBudgetR { get; set; }

        // 2026-07-08 — same tight cluster budget for the CRYPTO factor
        // (BTC/ETH/XRP/SOL/… move as one). The correlated-cluster diagnosis
        // fires H10 on 3/3–4/4 of crypto losses, and live clustered crypto ran
        // 25% WR vs 46% solo — the strongest cluster gap found. At 1.5 the 2nd
        // correlated crypto sizes to ~50%, bounding the tail. SIZING not
        // blocking (crypto backtest WR 76–80% — clusters win together too).
        [Parameter("CRYPTO cluster budget (× 1 trade)", DefaultValue = 1.5, MinValue = 0.0, MaxValue = 10.0, Group = "Risk")]
        public double CryptoClusterBudgetR { get; set; }

        // 2026-06-25 — widened demo from 3.0 → 5.0 so cross/exotic pairs
        // (EURNOK, XPTUSD, NZD crosses) aren't auto-skipped during normal
        // sessions. Their bid-ask spreads sit 3-5 pips on IC Markets even
        // outside news windows, so the old 3.0 gate was silently dropping
        // a chunk of signals the engine generated. Live cap stays at 2.0
        // (LiveMaxSpreadPips) — real money rewards conservatism, and the
        // demo run will tell us per-pair how much extra spread the wider
        // cap actually buys us before we lift the live cap.
        [Parameter("Max spread (pips)", DefaultValue = 5.0, MinValue = 0.1, MaxValue = 50.0, Group = "Risk")]
        public double MaxSpreadPips { get; set; }

        // 2026-07-03 — relative spread gate. Replaces the absolute pip cap
        // (which was meaningless across classes — PipSize differs by orders
        // of magnitude). Reject only when the live spread eats more than this
        // fraction of the signal's stop distance; anything below just sizes
        // the lot down (see spread-aware sizing in ComputeVolume). 0.5 = the
        // spread may not exceed half the stop. Set high (e.g. 2.0) to disable.
        [Parameter("Max spread (% of stop)", DefaultValue = 0.5, MinValue = 0.05, MaxValue = 2.0, Group = "Risk")]
        public double MaxSpreadPctOfStop { get; set; }

        [Parameter("Max signal age (minutes)", DefaultValue = 60, MinValue = 5, MaxValue = 720, Group = "Risk")]
        public int MaxSignalAgeMin { get; set; }

        // 2026-07-04 — feed-only trade blocklist. Illiquid micro-cap alts
        // (spread runs 68-180% of the structural stop → -EV on a 1:1 target no
        // matter how the lot is sized) stay in the signal feed for the
        // dashboard / backtest but are never traded live. Comma-separated feed
        // pairs; editable in the cBot UI without a rebuild.
        [Parameter("Trade blocklist (comma-sep pairs)", DefaultValue = "taousd,suiusd,nearusd,wtiusd,natgas", Group = "Risk")]
        public string TradeBlocklist { get; set; }

        // 2026-06-25 — CATASTROPHE GUARDS (added after a USDCHF macdp
        // signal with a 0.7-pip stop produced a 680-lot naked position).
        //
        // MinStopPips: reject any signal whose entry→stop distance is
        // below this floor. A near-zero stop is ALWAYS a degenerate
        // signal (structural stop landed on the entry bar) and it does
        // two dangerous things at once: (1) risk-based sizing divides by
        // a tiny denominator and explodes the position, (2) the stop is
        // below the broker's minimum distance so it gets silently dropped
        // and the position opens naked. Floor it.
        [Parameter("Min stop distance (pips)", DefaultValue = 3.0, MinValue = 0.0, MaxValue = 100.0, Group = "Risk")]
        public double MinStopPips { get; set; }

        // 2026-07-04 — FX-only COST floor (distinct from MinStopPips, which is
        // a near-zero degenerate guard). Live data: FX trades with a structural
        // stop under ~8 pips are cost-dominated — on the 0-8 pip buckets entry
        // slippage alone ran 45-85% of the stop, wins realised only ~+0.85R
        // while losses ran ~-1.4R, and expectancy was -0.5 to -0.7R vs ~-0.3R
        // wider. Skip FX signals below this. FX only (fiat/fiat) — indices /
        // metals / crypto have different pip scales and their own floors TBD.
        [Parameter("Min stop distance FX (pips)", DefaultValue = 8.0, MinValue = 0.0, MaxValue = 50.0, Group = "Risk")]
        public double MinStopPipsFX { get; set; }

        // 2026-07-04 — H11 faytterro size-weighting. macdp trades whose entry
        // does NOT align with a fresh spring/UTAD event (signal event_aligned
        // == false) size to this fraction. Forward test: aligned 86% live vs
        // no-event 40% — so we keep collecting on the no-event cohort at
        // reduced risk rather than cutting it. 1.0 disables (equal size).
        [Parameter("No-event size factor", DefaultValue = 0.5, MinValue = 0.1, MaxValue = 1.0, Group = "Risk")]
        public double NoEventSizeFactor { get; set; }

        // MaxPositionLots: absolute hard ceiling on position size, in
        // LOTS, independent of the risk-based calc and the broker's own
        // (effectively unlimited) max. Last line of defence — even if the
        // sizing math goes wrong again, we never place more than this.
        [Parameter("Max position size (lots)", DefaultValue = 100.0, MinValue = 0.01, MaxValue = 10000.0, Group = "Risk")]
        public double MaxPositionLots { get; set; }

        // 2026-06-30 — MAX ENTRY DEVIATION. The cBot market-fills a
        // triggered signal up to 30s-60min after its trigger price. On
        // fast instruments the market has drifted far by fill time
        // (SPX500 entries landed ~189% of R from the signal price, oil
        // 58-76%), which decouples the live trade from the modelled one
        // — the dominant live-vs-sim gap. This gate skips a signal when
        // the current market is more than MaxEntryDeviationPctOfR of the
        // signal's R distance away from sig.Entry (either direction —
        // adverse drift = worse fill; favourable drift = the structural
        // stop/target levels no longer apply). Default 0.30 (30% of R).
        // The proper fix is limit orders at sig.Entry; this is the
        // pragmatic market-order version. Set 0 to disable the gate.
        [Parameter("Max entry deviation (% of R, 0=off)", DefaultValue = 0.30, MinValue = 0.0, MaxValue = 5.0, Group = "Risk")]
        public double MaxEntryDeviationPctOfR { get; set; }

        // 2026-07-07 — LIMIT-ENTRY execution. Entry-drift was 60% of all live
        // rejections (market moved 30-397% of R past the signal entry before a
        // market order could fill). A limit order AT sig.Entry fills only at
        // the modelled price or better — or waits/expires — so it eliminates
        // drift by construction and aligns live fills with the backtest's
        // modelled entries. When on, the entry-deviation gate above is skipped
        // (the limit handles drift). Set false to revert to market orders.
        [Parameter("Use limit-order entry", DefaultValue = true, Group = "Execution")]
        public bool UseLimitEntry { get; set; }

        [Parameter("Limit order expiry (minutes)", DefaultValue = 45, MinValue = 5, MaxValue = 720, Group = "Execution")]
        public int LimitExpiryMin { get; set; }

        [Parameter("Order label", DefaultValue = "VikingInvest", Group = "Identity")]
        public string OrderLabel { get; set; }

        // 2026-08-07 — trailing stop tuned for the intraday 1:1 population. intraday_trail_research.py
        // + OOS: once a position is +0.5R in profit, trail the stop 0.25R behind the best price
        // (first move locks +0.25R). On crypto (the live intraday class) this lifts +0.028R -> +0.102R,
        // both OOS halves positive. NOT 75% — an earlier/tighter trail wins at 1:1. Take-profit is
        // never touched. Toggle here.
        [Parameter("Trailing stop (0.5R arm / 0.25R trail)", DefaultValue = true, Group = "Safety")]
        public bool TrailingStop { get; set; }

        [Parameter("Allow LIVE account", DefaultValue = false, Group = "Safety")]
        public bool AllowLive { get; set; }

        [Parameter("Dry run (log only)", DefaultValue = false, Group = "Safety")]
        public bool DryRun { get; set; }

        // 2026-06-24 — default OFF. The invalidated-signal branch sits ahead
        // of the dedup gate (so a position opened mid-poll still gets closed),
        // which means every dead signal in the feed re-logs on every 30s poll.
        // With a couple dozen stale invalidations that's a wall of noise that
        // buries the lines that matter (placed / closed / kill-switch). Flip
        // to true per-instance when actively debugging.
        [Parameter("Verbose logging", DefaultValue = false, Group = "Debug")]
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
        // 2026-06-22 — fires once after the first poll returns signals,
        // emits a single audit log line + Telegram listing every pair in
        // the engine universe that the broker doesn't have in Market
        // Watch. Saves grep-ing per-signal "No matching symbol" lines.
        private bool _universeAuditDone = false;
        // Track which signal id placed each open position so the close
        // event can write a clean execution row tying broker P&L back
        // to the detector signal that created it. positionId → signal id.
        private Dictionary<long, string> _positionIdToSignalId = new Dictionary<long, string>();

        // 2026-07-07 — LIMIT-ENTRY. Signals for which we placed a pending limit
        // (keyed by signal id) with the detail needed to write the 'placed'
        // execution row when/if the limit FILLS (async, via OnPositionOpened).
        // Only limit placements populate this — market orders write their row
        // synchronously, so OnPositionOpened ignores anything not in here.
        private class PendingLimit { public string Pair, Dir; public double Entry, Stop, Target, RSize, Volume; }
        private Dictionary<string, PendingLimit> _pendingLimitSignals = new Dictionary<string, PendingLimit>();
        private static string SignalIdFromComment(string comment)
        {
            const string pfx = "viking-";
            if (string.IsNullOrEmpty(comment) || !comment.StartsWith(pfx, StringComparison.Ordinal)) return null;
            return comment.Substring(pfx.Length);
        }
        // The method is the 3rd segment of the intraday signal id (pair:armed_ms:method).
        // Returns null when the id is missing or malformed so the row stays honestly blank
        // rather than mislabelled.
        private static string StrategyFromSignalId(string sigId)
        {
            if (string.IsNullOrEmpty(sigId)) return null;
            var seg = sigId.Split(':');
            return seg.Length >= 3 && seg[2].Length > 0 ? seg[2] : null;
        }
        // 2026-08-06 — durable position→signal_id map. The broker does not reliably preserve a
        // position Comment across a cBot restart on this venue, so a close firing after a restart
        // lost BOTH the in-memory map and the Comment fallback → the row logged a null signal_id
        // and the dashboard rendered it as an "unlabelled" strategy. Persisting the map to disk
        // (rewritten on every open/close, pruned to live positions on load) closes that gap: the
        // close handler resolves the method even when Comment is gone.
        private string _pidMapPath;
        private void LoadPositionSignalMap()
        {
            try
            {
                if (string.IsNullOrEmpty(_pidMapPath) || !IOFile.Exists(_pidMapPath)) return;
                foreach (var raw in IOFile.ReadAllLines(_pidMapPath))
                {
                    var t = raw.Split('\t');
                    if (t.Length >= 2 && long.TryParse(t[0], out var pid) && !string.IsNullOrEmpty(t[1]))
                        _positionIdToSignalId[pid] = t[1];
                }
                // Drop entries whose position is no longer open (closes we missed while down),
                // so the file can't grow without bound.
                var open = new HashSet<long>(Positions.Where(x => x.Label == OrderLabel).Select(x => (long)x.Id));
                foreach (var k in _positionIdToSignalId.Keys.ToList())
                    if (!open.Contains(k)) _positionIdToSignalId.Remove(k);
                PersistPositionSignalMap();
                Print($"🔁 [VikingInvest] restored {_positionIdToSignalId.Count} position→signal mapping(s) from disk");
            }
            catch (Exception ex) { Print($"⚠️ [VikingInvest] pid-map load failed: {ex.Message}"); }
        }
        private void PersistPositionSignalMap()
        {
            try
            {
                if (string.IsNullOrEmpty(_pidMapPath)) return;
                var sb = new StringBuilder(256);
                foreach (var kv in _positionIdToSignalId)
                    sb.Append(kv.Key).Append('\t').Append(kv.Value).Append('\n');
                IOFile.WriteAllText(_pidMapPath, sb.ToString());
            }
            catch (Exception ex) { Print($"⚠️ [VikingInvest] pid-map persist failed: {ex.Message}"); }
        }
        // 2026-06-24 — Phase 1 failed-trade inspector hooks.
        // _positionMaxFavR     : running MFE in R per open position, sampled
        //                        once per poll tick (≥5s). Coarse but ample
        //                        for multi-hour intraday trades — answers
        //                        "did this loss ever run into profit first?".
        // _positionCloseSource : "cbot_invalidation" / "manual" / "broker".
        //                        Stamped by whichever code path triggers the
        //                        close; OnPositionClosed reads it and falls
        //                        back to "broker" if nothing stamped it.
        private Dictionary<long, double> _positionMaxFavR     = new Dictionary<long, double>();
        private Dictionary<long, string> _positionCloseSource = new Dictionary<long, string>();
        // Trailing state: original risk unit R (from the take-profit, restart-safe at 1:1) and the
        // best favourable price seen, per open position.
        private Dictionary<long, double> _posOrigR   = new Dictionary<long, double>();
        private Dictionary<long, double> _posPeakPx  = new Dictionary<long, double>();
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
            _pidMapPath      = System.IO.Path.Combine(_vikingDir, "position_signal_map.tsv");
            LoadSeenIds();
            LoadDailyLossState();
            LoadPositionSignalMap();   // restore method linkage for positions opened before a restart

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
            Positions.Opened += OnPositionOpened;   // records LIMIT fills (async)

            _http.DefaultRequestHeaders.UserAgent.ParseAdd("VikingInvest-cTrader-Bot/1.0");
            // 2026-06-23 — force CDN revalidation on every poll. The
            // CacheBust(?t=minute) query trick does NOTHING on the two
            // feed endpoints we use: raw.githubusercontent.com (Fastly,
            // max-age=300) and jsDelivr @main (caches branch refs for
            // HOURS) both key their cache on PATH ONLY and ignore the
            // query string. Result: the bot was acting on signals.json
            // up to 5 min (raw) / hours (jsDelivr) stale, so fast
            // trigger→invalidate transitions around the London/NY opens
            // were missed — a 'triggered' alert fired on Telegram while
            // the bot's cached feed still showed armed/invalidated, or
            // didn't contain the signal at all. no-cache forces Fastly /
            // jsDelivr to revalidate against origin (GitHub) each fetch,
            // bringing staleness down to seconds. Kept the query bust as
            // belt-and-braces for any intermediary that DOES vary on it.
            _http.DefaultRequestHeaders.CacheControl =
                new System.Net.Http.Headers.CacheControlHeaderValue
                {
                    NoCache = true,
                    NoStore = true,
                    MustRevalidate = true
                };
            _http.DefaultRequestHeaders.Pragma.ParseAdd("no-cache");

            // Effective parameters AFTER live-mode override.
            var effRisk = EffectiveRiskPct;
            var effMax  = EffectiveMaxPositions;
            var effSpr  = MaxSpreadPctOfStop;

            Print("✅ [VikingInvest] Bridge initialised.");
            Print($"   URL: {SignalsUrl}");
            Print($"   Poll: every {PollSeconds}s · Risk: {effRisk}% · Max positions: {effMax} · Max spread: {effSpr:P0} of stop (spread-aware sizing on)");
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
            // Sample max-favourable-excursion for every open position first
            // (cheap, synchronous, main-thread) so even if PollAndProcess
            // does nothing this tick we keep the MFE trace fresh.
            SampleOpenPositionMfe();
            _ = PollAndProcess();
        }

        // 2026-06-24 — Phase 1 inspector. Walk open positions and record the
        // furthest each has run into profit, expressed in R (favourable price
        // move ÷ stop distance). Stored per position-id; OnPositionClosed
        // reads the final value into the execution row. Sampling on the poll
        // tick (not OnTick) keeps this off the hot path — a few-second
        // resolution is plenty to distinguish "never went green" from "ran to
        // +1.5R then reversed", which is the only thing the inspector needs.
        private void SampleOpenPositionMfe()
        {
            try
            {
                foreach (var pos in Positions)
                {
                    if (pos.Label != OrderLabel || pos.Symbol == null || pos.EntryPrice <= 0) continue;
                    // Original risk unit R: from the take-profit (intraday is 1:1, so |entry-TP| == R),
                    // which is restart-safe and unaffected by trailing the stop. Fall back to the
                    // current stop distance only if there's no take-profit.
                    double origR;
                    if (!_posOrigR.TryGetValue(pos.Id, out origR) || origR <= 0)
                    {
                        origR = (pos.TakeProfit.HasValue && pos.TakeProfit.Value > 0)
                                ? Math.Abs(pos.EntryPrice - pos.TakeProfit.Value)
                                : (pos.StopLoss.HasValue ? Math.Abs(pos.EntryPrice - pos.StopLoss.Value) : 0);
                        if (origR <= 0) continue;
                        _posOrigR[pos.Id] = origR;
                    }
                    bool isBuy = pos.TradeType == TradeType.Buy;
                    var nowPx = isBuy ? pos.Symbol.Bid : pos.Symbol.Ask;
                    var favPx = isBuy ? (nowPx - pos.EntryPrice) : (pos.EntryPrice - nowPx);
                    var favR = favPx / origR;
                    double prev = 0;
                    _positionMaxFavR.TryGetValue(pos.Id, out prev);
                    if (favR > prev) _positionMaxFavR[pos.Id] = favR;

                    // ── 0.5R-arm / 0.25R-trail (intraday 1:1) ──
                    if (TrailingStop)
                    {
                        double peak;
                        if (!_posPeakPx.TryGetValue(pos.Id, out peak) || peak == 0) peak = pos.EntryPrice;
                        peak = isBuy ? Math.Max(peak, nowPx) : Math.Min(peak, nowPx);
                        _posPeakPx[pos.Id] = peak;
                        bool armed = isBuy ? (peak >= pos.EntryPrice + 0.5 * origR)
                                           : (peak <= pos.EntryPrice - 0.5 * origR);
                        if (armed)
                        {
                            double desired = isBuy ? peak - 0.25 * origR : peak + 0.25 * origR;
                            RatchetStop(pos, desired, isBuy);           // tick-guarded; skips no-op modifies
                        }
                    }
                }
            }
            catch { /* defensive — never let the MFE trace crash the poll */ }
        }

        // Ratchet a position's stop toward `desired`, but ONLY when the normalised move
        // clears one full tick in the favourable direction. cTrader pops "Order execution
        // error / Nothing to change" whenever a modify resolves to the stop already set —
        // a sub-tick move that survived a raw-price comparison would do exactly that on
        // every MFE sample. Normalising to the symbol's precision and requiring a >= 1-tick
        // move removes the no-op calls (and therefore the notifications) while still
        // ratcheting on real moves.
        private void RatchetStop(Position p, double desired, bool isBuy)
        {
            if (p == null || p.Symbol == null) return;
            int digits = p.Symbol.Digits;
            double tick = p.Symbol.TickSize > 0 ? p.Symbol.TickSize : Math.Pow(10, -digits);
            desired = Math.Round(desired, digits);
            double? cur = p.StopLoss;
            bool ok = isBuy ? (!cur.HasValue || desired >= Math.Round(cur.Value, digits) + tick)
                            : (!cur.HasValue || desired <= Math.Round(cur.Value, digits) - tick);
            if (!ok) return;                                            // sub-tick / backward → skip
            try { p.ModifyStopLossPrice(desired); }
            catch (Exception ex) { Print($"[VikingInvest] trail pid={p.Id} failed: {ex.Message}"); }
        }

        protected override void OnStop()
        {
            Timer.Stop();
            Positions.Closed -= OnPositionClosed;
            Positions.Opened -= OnPositionOpened;
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
        // Cache-bust helper. Appends ?t=<unix-minute> (or &t= if the URL
        // already has a query string) so every poll fetches a unique URL
        // that no CDN, proxy, or HttpClient handler can serve from cache.
        // Resolution is one minute — fine-grained enough that any edge
        // cache TTL ≤ 60s is bypassed, while still letting two requests
        // within the same minute hit a warm cache if anything in between
        // chooses to cache. raw.githubusercontent.com sends max-age=300
        // and jsDelivr can hold @main refs for hours; this neutralises
        // both. URL parsing is intentionally string-level (no Uri()) so
        // a malformed user-supplied URL fails cleanly downstream rather
        // than throwing during the bust.
        private static string CacheBust(string url)
        {
            if (string.IsNullOrEmpty(url)) return url;
            var t = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60;
            var sep = url.Contains("?") ? "&" : "?";
            return $"{url}{sep}t={t}";
        }

        private async Task PollAndProcess()
        {
            // Phase 2 — fetch kill-switch FIRST. If killed, we still
            // need to poll signals (so the dashboard can see we're
            // alive + reading state) but we skip placement entirely.
            await FetchKillSwitch();

            string body;
            try
            {
                body = await _http.GetStringAsync(CacheBust(SignalsUrl));
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

            // 2026-06-22 — CRITICAL THREADING FIX. PollAndProcess is async:
            // after `await _http.GetStringAsync(...)` the continuation runs
            // on a thread-pool thread, NOT the cAlgo main thread. Any cAlgo
            // API touched from here (Symbols.GetSymbol in ResolveSymbol,
            // ExecuteMarketOrder, Account.*, Positions) throws "Unable to
            // invoke target method in current thread" — the exact crash
            // seen ~5×/day in the production log, which restarts the bot and
            // means a triggered signal arriving during the dead window is
            // never converted to an order. It also intermittently killed
            // the bot on plain poll cycles whenever the HTTP call was slow
            // enough to force a true async resume. Marshal the entire
            // signal-processing pass onto the main thread so every cAlgo
            // call inside ProcessOneSignal is on the right thread.
            BeginInvokeOnMainThread(() =>
            {
                // 2026-06-22 — one-shot universe audit on the first
                // populated poll. Lists every pair in the live feed and
                // whether the broker has it in Market Watch. The cBot
                // depends on Symbols.GetSymbol returning non-null, which
                // in cAlgo requires the symbol to be in cTrader's Market
                // Watch panel — order-screen visibility alone isn't
                // enough. This audit makes that requirement explicit at
                // startup so missing symbols don't silently drop signals
                // for hours before anyone notices.
                if (!_universeAuditDone && signals.Count > 0)
                {
                    var distinctPairs = new List<string>();
                    var seenPairs = new HashSet<string>();
                    foreach (var sig in signals)
                    {
                        if (string.IsNullOrEmpty(sig.Pair)) continue;
                        if (seenPairs.Add(sig.Pair)) distinctPairs.Add(sig.Pair);
                    }
                    var missing = new List<string>();
                    var resolved = new List<string>();
                    foreach (var p in distinctPairs)
                    {
                        if (ResolveSymbol(p) == null) missing.Add(p.ToUpperInvariant());
                        else resolved.Add(p.ToUpperInvariant());
                    }
                    var auditLine = $"📋 [VikingInvest] Universe audit: {resolved.Count}/{distinctPairs.Count} pairs resolved in Market Watch.";
                    Print(auditLine);
                    if (missing.Count > 0)
                    {
                        var msg = $"❌ Missing from Market Watch (add via cTrader → Market Watch → +): {string.Join(", ", missing)}\n" +
                                  $"   Signals on these pairs will be skipped until added.";
                        Print(msg);
                        TelegramSend($"📋 cBot universe audit on {(Account.IsLive ? "🔴 LIVE" : "🟢 DEMO")} #{Account.Number}\n" +
                                     $"{resolved.Count}/{distinctPairs.Count} pairs resolved.\n" +
                                     $"Missing: {string.Join(", ", missing)}\n" +
                                     $"Add via cTrader → Market Watch → + to enable trading on these.",
                                     important:true);
                    }
                    else if (Account.IsLive)
                    {
                        TelegramSend($"📋 cBot universe audit: all {distinctPairs.Count} feed pairs resolved on {Account.BrokerName} #{Account.Number}.",
                                     important:false);
                    }
                    _universeAuditDone = true;
                }

                foreach (var sig in signals)
                {
                    try { ProcessOneSignal(sig); }
                    catch (Exception ex)
                    {
                        Print($"⚠️ [VikingInvest] ProcessOneSignal failed for id={sig?.Id}: {ex.Message}");
                    }
                }
            });
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
                body = await _http.GetStringAsync(CacheBust(KillSwitchUrl));
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
                // 2026-06-22 — same threading rule as PollAndProcess: this
                // runs in the continuation after `await GetStringAsync`, i.e.
                // on a thread-pool thread. Marshal the Print/Telegram onto
                // the main thread to avoid the cross-thread crash.
                var killedNow = _killed;
                var reason = _killReason ?? "(unspecified)";
                BeginInvokeOnMainThread(() =>
                {
                    Print(killedNow
                        ? $"🛑 [VikingInvest] KILL-SWITCH ENGAGED — pausing new orders. Reason: {reason}"
                        : $"✅ [VikingInvest] KILL-SWITCH RELEASED — resuming new orders. Last reason: {reason}");
                    TelegramSend(killedNow
                        ? $"🛑 KILL-SWITCH ENGAGED: {reason}"
                        : $"✅ KILL-SWITCH RELEASED: {reason}",
                        important:true);
                });
            }
        }

        // ───────────── Per-signal handling ────────────────────────────
        private void ProcessOneSignal(Signal sig)
        {
            _signalsSeen++;
            if (string.IsNullOrEmpty(sig.Id) || string.IsNullOrEmpty(sig.Pair) || string.IsNullOrEmpty(sig.State))
                return;

            // 2026-06-22 — invalidated → close any matching open position.
            // The backtest WR figures assume invalidations exit the trade at
            // the invalidation bar (typically -0.2 to -0.5R), NOT at the full
            // structural stop. If we ignore the invalidated state the live
            // trade keeps running on its own SL/TP and the live edge ends up
            // strictly worse than the backtest. The Telegram alert template
            // literally says "Suggested: exit at market to limit loss before
            // stop fills." — this branch honours that.
            if (sig.State == "invalidated")
            {
                // Cancel any still-pending limit for this signal — the setup
                // flipped, so we must not let the limit fill on a dead level.
                if (UseLimitEntry)
                {
                    foreach (var po in PendingOrders)
                    {
                        if (po.Label == OrderLabel && SignalIdFromComment(po.Comment) == sig.Id)
                        {
                            try { CancelPendingOrder(po); Print($"⚠️ [VikingInvest] INVALIDATED — cancelled pending limit {po.SymbolName} id={sig.Id}"); }
                            catch (Exception ex) { Print($"   CancelPendingOrder threw: {ex.Message}"); }
                        }
                    }
                    _pendingLimitSignals.Remove(sig.Id);
                }
                // Find the open position opened by THIS signal id, if any.
                // _positionIdToSignalId is the placement-time mapping; we
                // walk it because there's no reverse index (open positions
                // are typically ≤5 so the scan is cheap).
                long matchedPosId = 0;
                foreach (var kv in _positionIdToSignalId)
                {
                    if (kv.Value == sig.Id) { matchedPosId = kv.Key; break; }
                }
                if (matchedPosId != 0)
                {
                    var pos = Positions.FirstOrDefault(p => p.Id == matchedPosId);
                    if (pos != null)
                    {
                        // Stamp the source BEFORE closing so the synchronous
                        // Positions.Closed callback (OnPositionClosed) reads
                        // "cbot_invalidation" rather than the "broker" default.
                        _positionCloseSource[pos.Id] = "cbot_invalidation";
                        var close = ClosePosition(pos);
                        if (close.IsSuccessful)
                        {
                            Print($"⚠️ [VikingInvest] INVALIDATED — closed {pos.SymbolName} {pos.TradeType} " +
                                  $"PID={pos.Id} · net≈{pos.NetProfit:F2} · pips={pos.Pips:F1} · id={sig.Id}");
                            TelegramSend($"⚠️ Closed {pos.SymbolName} on invalidation (signal flipped). " +
                                         $"Position {pos.Id} net {pos.NetProfit:F2}", important:true);
                        }
                        else
                        {
                            Print($"⚠️ [VikingInvest] ClosePosition failed for PID={pos.Id} id={sig.Id}: {close.Error}");
                        }
                    }
                    else if (VerboseLog)
                    {
                        // Position already closed (broker SL/TP beat us to it).
                        Print($"ℹ️ [VikingInvest] id={sig.Id} invalidated but position PID={matchedPosId} already closed.");
                    }
                }
                else if (VerboseLog)
                {
                    Print($"ℹ️ [VikingInvest] id={sig.Id} state=invalidated — no open position to close.");
                }
                MarkSeen(sig.Id);
                return;
            }

            // Only act on triggered signals — armed = not yet a fill.
            if (sig.State != "triggered")
            {
                if (VerboseLog) Print($"ℹ️ [VikingInvest] id={sig.Id} state={sig.State} — no action.");
                return;
            }

            // Dedup — already-placed signals never fire twice.
            if (_seenIds.Contains(sig.Id)) return;

            // 2026-07-04 — feed-only trade blocklist. Skip SILENTLY (Print
            // only, NO 'rejected' dispatch): the blocklist is deliberate config,
            // not a dynamic gate, so it must not spam the execution log /
            // Telegram every bar. These pairs stay in the feed for the
            // dashboard / backtest but are never traded live.
            if (IsBlocklisted(sig.Pair))
            {
                if (VerboseLog) Print($"⏭ [VikingInvest] {sig.Pair} on trade blocklist — feed-only, not traded live. id={sig.Id}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

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
                    EmitRejection(sig, null, "stale", $"{ageMin:F0} min old (cap {MaxSignalAgeMin})");
                    MarkSeen(sig.Id); _ordersSkipped++;
                    return;
                }
            }

            var symbol = ResolveSymbol(sig.Pair);
            if (symbol == null)
            {
                Print($"⚠️ [VikingInvest] No matching cTrader symbol for pair={sig.Pair} — skipping.");
                EmitRejection(sig, null, "no-symbol", $"no cTrader symbol for {sig.Pair}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // 2026-06-24 — one-position-per-pair guard. Two DIFFERENT signal
            // ids on the same instrument (a wick + a fib trigger, or two wick
            // triggers on different bars) would each open a position, stacking
            // correlated exposure on one symbol and burning the Max-positions
            // budget on a single bet. The backtest WR is computed one-trade-
            // per-signal with no such stacking, so we cap live to one open
            // position per resolved broker symbol to keep the live edge aligned
            // with the simulated edge. Check the resolved symbol.Name (not the
            // feed pair) so aliased instruments collapse correctly. Mark seen
            // so the duplicate trigger doesn't re-evaluate every poll.
            if (Positions.Any(p => p.Label == OrderLabel && p.SymbolName == symbol.Name)
                || PendingOrders.Any(o => o.Label == OrderLabel && o.SymbolName == symbol.Name))
            {
                Print($"⏭ [VikingInvest] Already holding {symbol.Name} — skipping id={sig.Id} (one-per-pair).");
                EmitRejection(sig, symbol.Name, "already-holding", $"already holding {symbol.Name}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Concurrency check — count our open positions by label.
            // Count open positions PLUS still-pending limit orders — otherwise
            // several limits filling at once could overshoot the position cap.
            var ourOpen = Positions.Count(p => p.Label == OrderLabel)
                        + PendingOrders.Count(o => o.Label == OrderLabel);
            if (ourOpen >= EffectiveMaxPositions)
            {
                Print($"🛑 [VikingInvest] Max positions reached ({ourOpen}/{EffectiveMaxPositions}) — skipping id={sig.Id}");
                EmitRejection(sig, symbol.Name, "max-positions", $"{ourOpen}/{EffectiveMaxPositions} open");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Spread filter — RELATIVE to the stop distance, not an absolute
            // pip cap. 2026-07-03 — the old absolute pip cap was nonsensical
            // across asset classes: PipSize differs by orders of magnitude, so
            // BTC reads ~1200 "pips", an index ~40, FX ~2, and a single cap
            // wrongly rejected normal BTC / index spreads. What matters is how
            // much of the trade's R the spread eats. Paired with spread-aware
            // sizing in ComputeVolume (which folds the spread into the stop so
            // the lot sizes DOWN and risk stays within budget), a wide-but-
            // tolerable spread just trades smaller; only a spread that eats
            // more than MaxSpreadPctOfStop of the stop is rejected as toxic.
            var spreadPips   = symbol.PipSize > 0 ? symbol.Spread / symbol.PipSize : 0;
            var gateStopPips = Math.Abs(sig.Entry - sig.Stop) / symbol.PipSize;
            if (MaxSpreadPctOfStop > 0 && gateStopPips > 0 &&
                spreadPips > MaxSpreadPctOfStop * gateStopPips)
            {
                Print($"🛑 [VikingInvest] Spread too wide on {symbol.Name}: {spreadPips:F1} pips is {(spreadPips / gateStopPips):P0} of the {gateStopPips:F1}-pip stop (cap {MaxSpreadPctOfStop:P0}). Skipping id={sig.Id}");
                EmitRejection(sig, symbol.Name, "spread", $"{spreadPips:F1} pips = {(spreadPips / gateStopPips):P0} of stop (cap {MaxSpreadPctOfStop:P0})");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // 2026-06-25 — CATASTROPHE GUARD 1: minimum stop distance.
            // A degenerate signal with a near-zero stop (structural stop
            // landed on the entry bar) both explodes the position size
            // and gets its protective stop silently dropped by the broker
            // (below min distance). Reject BEFORE sizing. Compute slPips
            // here (was below) so the gate runs first.
            var slPips = Math.Abs(sig.Entry - sig.Stop) / symbol.PipSize;
            var tpPips = Math.Abs(sig.Target - sig.Entry) / symbol.PipSize;
            if (slPips < MinStopPips)
            {
                // 2026-07-01 — log-only (Telegram removed). A gated non-trade;
                // Telegram now carries real executions only. Still in the log.
                Print($"🛑 [VikingInvest] Stop too tight on {symbol.Name}: {slPips:F2} < {MinStopPips} pips — DEGENERATE signal, skipping id={sig.Id}");
                EmitRejection(sig, symbol.Name, "stop-too-tight", $"{slPips:F2} < {MinStopPips} pips");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // 2026-07-04 — FX COST FLOOR (see MinStopPipsFX). slPips is already
            // computed above. Below this, FX trades don't survive spread +
            // slippage (the drag is a large fraction of a tiny R). Emits a
            // throttled 'rejected' so we can watch how many trades it filters.
            if (MinStopPipsFX > 0 && IsFxPair(sig.Pair) && slPips < MinStopPipsFX)
            {
                Print($"🛑 [VikingInvest] FX stop too tight on {symbol.Name}: {slPips:F1} < {MinStopPipsFX} pips (cost floor) — skipping id={sig.Id}");
                EmitRejection(sig, symbol.Name, "min-stop-fx", $"{slPips:F1} < {MinStopPipsFX} pips (FX cost floor)");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // 2026-06-30 — MAX ENTRY DEVIATION gate. If the market has
            // drifted too far from the signal's entry price by the time
            // we'd fill (market-fill latency), the live trade no longer
            // matches the modelled setup. Skip rather than chase. R is the
            // signal's stop distance; compare the price we'd actually fill
            // at (Ask for a buy, Bid for a sell) to sig.Entry. See the
            // MaxEntryDeviationPctOfR comment block. Skipped under limit-entry:
            // a limit at sig.Entry can't fill at a drifted price, so drift is
            // handled structurally, not by rejecting.
            if (!UseLimitEntry && MaxEntryDeviationPctOfR > 0)
            {
                var rDist = Math.Abs(sig.Entry - sig.Stop);
                var fillRef = sig.Dir == "bull" ? symbol.Ask : symbol.Bid;
                if (rDist > 0 && fillRef > 0)
                {
                    var devR = Math.Abs(fillRef - sig.Entry) / rDist;
                    if (devR > MaxEntryDeviationPctOfR)
                    {
                        // 2026-07-01 — log-only (Telegram removed). Drifters are
                        // gated non-trades; Telegram now carries real executions
                        // only. The skip is still recorded in the cBot log.
                        Print($"⏭ [VikingInvest] Entry drifted on {symbol.Name}: market {fillRef:F5} is {devR:P0} of R from signal entry {sig.Entry:F5} (cap {MaxEntryDeviationPctOfR:P0}) — skipping id={sig.Id}");
                        EmitRejection(sig, symbol.Name, "entry-drift", $"{devR:P0} of R (cap {MaxEntryDeviationPctOfR:P0})");
                        MarkSeen(sig.Id); _ordersSkipped++;
                        return;
                    }
                }
            }

            // Risk-based volume sizing. Factors in the signal's r_size
            // (1.0 wick, 0.5 fib) so commodity / index trades take half
            // size automatically — matching the dashboard's net-R math.
            // EffectiveRiskPct applies the live-mode override transparently.
            var riskPct = EffectiveRiskPct * (sig.RSize > 0 ? sig.RSize : 1.0);
            // 2026-07-04 — H11 faytterro size-weighting: half-size macdp trades
            // that don't align with a fresh spring/UTAD event. Only when the
            // flag is explicitly false (macdp no-event/fought); true and null
            // (aligned, or non-macdp signals) keep full size.
            if (sig.EventAligned == false)
            {
                riskPct *= NoEventSizeFactor;
                if (VerboseLog) Print($"⚖️ [VikingInvest] No faytterro event on {sig.Pair} {sig.Dir} — sizing to {NoEventSizeFactor:P0}. id={sig.Id}");
            }
            var volume = ComputeVolume(symbol, sig.Entry, sig.Stop, riskPct);
            if (volume <= 0)
            {
                Print($"⚠️ [VikingInvest] Volume compute returned {volume} for {symbol.Name} — skipping");
                EmitRejection(sig, symbol.Name, "zero-volume", $"sized to {volume}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // 2026-07-03 — FACTOR-RISK SIZING. Before placing, cap the
            // aggregate open risk sharing this trade's factor-theme
            // (long-USD, short-JPY, …). New legs on a saturated factor are
            // SCALED DOWN so the book keeps participating in +EV clusters
            // while the correlated tail stays bounded (see FactorRiskBudgetR
            // block). Existing open risk is recovered per position from its
            // signal_id (→ feed pair) and its live stop distance, the same
            // risk math OnPositionClosed uses.
            if (FactorRiskBudgetR > 0)
            {
                var myTags = FactorTags(sig.Pair, sig.Dir);
                if (myTags.Count > 0)
                {
                    var eq              = Account.Equity;
                    var singleFullRisk  = eq * EffectiveRiskPct / 100.0;      // one rSize=1 trade
                    var budgetPerFactor = FactorRiskBudgetR * singleFullRisk;
                    var newTradeRisk    = eq * riskPct / 100.0;               // this trade (incl. rSize)
                    var exposure = new Dictionary<string, double>();
                    foreach (var pos in Positions)
                    {
                        if (pos.Label != OrderLabel) continue;
                        string sid;
                        if (!_positionIdToSignalId.TryGetValue(pos.Id, out sid) || string.IsNullOrEmpty(sid))
                        {
                            var cmt = pos.Comment ?? "";                       // survive a cBot restart
                            if (cmt.StartsWith("viking-")) sid = cmt.Substring(7);
                        }
                        if (string.IsNullOrEmpty(sid)) continue;
                        var ppair = sid.Split(':')[0];
                        var pdir  = pos.TradeType == TradeType.Buy ? "bull" : "bear";
                        var ptags = FactorTags(ppair, pdir);
                        if (ptags.Count == 0) continue;
                        var prisk = PositionRiskAmount(pos);
                        if (prisk <= 0) continue;
                        foreach (var kv in ptags)
                        {
                            var key = kv.Key + ":" + kv.Value;
                            exposure[key] = (exposure.ContainsKey(key) ? exposure[key] : 0.0) + prisk;
                        }
                    }
                    double scale = 1.0; string binding = null;
                    foreach (var kv in myTags)
                    {
                        var key = kv.Key + ":" + kv.Value;
                        // METAL and CRYPTO are tight correlated clusters (~one
                        // bet each) — they get their own tighter budget so a 2nd
                        // correlated leg sizes down rather than entering full.
                        double factorBudget = budgetPerFactor;
                        if (kv.Key == "METAL" && MetalClusterBudgetR > 0)
                            factorBudget = MetalClusterBudgetR * singleFullRisk;
                        else if (kv.Key == "CRYPTO" && CryptoClusterBudgetR > 0)
                            factorBudget = CryptoClusterBudgetR * singleFullRisk;
                        var existing = exposure.ContainsKey(key) ? exposure[key] : 0.0;
                        var allowed  = Math.Max(0.0, factorBudget - existing);
                        var s = newTradeRisk > 0 ? allowed / newTradeRisk : 1.0;
                        if (s < scale) { scale = s; binding = key; }
                    }
                    if (scale < 0.999)
                    {
                        scale = Math.Max(0.0, scale);
                        var scaledVol = symbol.NormalizeVolumeInUnits(volume * scale, RoundingMode.Down);
                        if (scaledVol <= 0 || scaledVol < symbol.VolumeInUnitsMin)
                        {
                            Print($"🛑 [VikingInvest] Factor-risk budget saturated on {binding} — {symbol.Name} {sig.Dir} would size to ~0, skipping id={sig.Id}");
                            EmitRejection(sig, symbol.Name, "factor-saturated", $"budget hit on {binding}");
                            MarkSeen(sig.Id); _ordersSkipped++;
                            return;
                        }
                        Print($"⚖️ [VikingInvest] Factor-risk sizing: {symbol.Name} {sig.Dir} scaled to {scale:P0} (budget hit on {binding}) — {volume}→{(long)scaledVol} units. id={sig.Id}");
                        volume = (long)scaledVol;
                    }
                }
            }

            // CATASTROPHE GUARD 2: absolute lot ceiling. Independent of
            // the risk calc and the broker's (effectively unlimited) max.
            // Even if sizing math misfires, we never place more than this.
            var maxUnits = symbol.QuantityToVolumeInUnits(MaxPositionLots);
            if (maxUnits > 0 && volume > maxUnits)
            {
                Print($"🛑 [VikingInvest] Sized volume {volume} units exceeds cap {maxUnits} ({MaxPositionLots} lots) on {symbol.Name} — capping. id={sig.Id}");
                TelegramSend($"⚠️ {symbol.Name} {sig.Dir} sized to {volume} units (> {MaxPositionLots}-lot cap) — capped. Check stop distance on this signal.", important: Account.IsLive);
                volume = (long)symbol.NormalizeVolumeInUnits(maxUnits, RoundingMode.Down);
            }

            var direction = sig.Dir == "bull" ? TradeType.Buy : TradeType.Sell;

            if (DryRun)
            {
                Print($"🟡 [VikingInvest] DRY-RUN: would {direction} {symbol.Name} {volume:F0} units · " +
                      $"entry≈{sig.Entry:F5} SL={sig.Stop:F5} TP={sig.Target:F5} ({slPips:F1}/{tpPips:F1} pips) id={sig.Id}");
                MarkSeen(sig.Id); _ordersPlaced++;
                return;
            }

            // 2026-07-07 — LIMIT-ENTRY path. Place a pending limit AT sig.Entry
            // instead of a market order: fills only at the modelled price (or
            // better), otherwise waits and expires — eliminating entry-drift.
            // The 'placed' execution row is written asynchronously when the
            // limit fills (OnPositionOpened), not here.
            if (UseLimitEntry)
            {
                var expiry = Server.Time.AddMinutes(LimitExpiryMin);
                // CS0618: this pips-based PlaceLimitOrder overload is deprecated by cTrader but
                // still fully functional (slPips/tpPips are pip distances, exactly as intended).
                // The replacement overload's SL/TP parameters need demo verification before
                // migrating, so suppress the deprecation notice here rather than risk changing
                // live stop/target placement untested.
#pragma warning disable CS0618
                var lr = PlaceLimitOrder(direction, symbol.Name, volume, sig.Entry, OrderLabel,
                                         slPips, tpPips, expiry, "viking-" + sig.Id);
#pragma warning restore CS0618
                if (lr.IsSuccessful)
                {
                    _pendingLimitSignals[sig.Id] = new PendingLimit {
                        Pair = sig.Pair, Dir = sig.Dir, Entry = sig.Entry,
                        Stop = sig.Stop, Target = sig.Target, RSize = sig.RSize, Volume = volume };
                    Print($"⏳ [VikingInvest] LIMIT placed {direction} {symbol.Name} {volume:F0} @ {sig.Entry:F5} " +
                          $"(expiry {LimitExpiryMin}m) SL={sig.Stop:F5} TP={sig.Target:F5} id={sig.Id}");
                }
                else
                {
                    Print($"❌ [VikingInvest] LIMIT REJECTED {direction} {symbol.Name}: {lr.Error} id={sig.Id}");
                    EmitRejection(sig, symbol.Name, "limit-rejected", lr.Error.ToString());
                    _ordersSkipped++;
                }
                MarkSeen(sig.Id);
                return;
            }

            var result = ExecuteMarketOrder(direction, symbol.Name, volume, OrderLabel,
                                            slPips, tpPips, "viking-" + sig.Id);
            if (result.IsSuccessful)
            {
                // 2026-06-25 — CATASTROPHE GUARD 3: never hold a naked
                // position. If the broker accepted the order but dropped
                // the protective stop (SL below min distance, or any other
                // reason), the position has no downside cap. Try once to
                // set it explicitly; if it STILL has no SL, close the
                // position immediately rather than let it run unbounded.
                var pos = result.Position;
                if (!pos.StopLoss.HasValue)
                {
                    Print($"⚠️ [VikingInvest] {symbol.Name} opened with NO stop loss — attempting to set SL={sig.Stop:F5}");
                    try { pos.ModifyStopLossPrice(sig.Stop); } catch (Exception ex) { Print($"   ModifyStopLossPrice threw: {ex.Message}"); }
                    if (!pos.StopLoss.HasValue)
                    {
                        Print($"🛑 [VikingInvest] SL still not set on {symbol.Name} PID={pos.Id} — CLOSING to avoid a naked position. id={sig.Id}");
                        TelegramSend($"🛑 {symbol.Name} {sig.Dir} opened WITHOUT a stop and the stop couldn't be attached (likely below broker min distance). Position CLOSED immediately to avoid unbounded risk. id={sig.Id}", important: true);
                        try { ClosePosition(pos); } catch (Exception ex) { Print($"   Emergency ClosePosition threw: {ex.Message}"); }
                        EmitRejection(sig, symbol.Name, "no-stop-closed", "opened without a stop, closed immediately");
                        MarkSeen(sig.Id); _ordersSkipped++;
                        return;
                    }
                    Print($"✅ [VikingInvest] SL attached on retry: {symbol.Name} SL={pos.StopLoss}");
                }
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
                PersistPositionSignalMap();   // durable so a restart-orphaned close keeps its method
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
        // 2026-07-07 — LIMIT-ENTRY fill handler. When a pending limit we placed
        // FILLS, the broker opens a position asynchronously and fires this. We
        // write the 'placed' execution row here. Market orders write their row
        // synchronously, so any fill whose signal isn't in _pendingLimitSignals
        // is ignored. Also runs the naked-stop catastrophe guard.
        private void OnPositionOpened(PositionOpenedEventArgs args)
        {
            var p = args.Position;
            if (p == null || p.Label != OrderLabel) return;
            var sigId = SignalIdFromComment(p.Comment);
            if (sigId == null || !_pendingLimitSignals.TryGetValue(sigId, out var pl)) return;
            _pendingLimitSignals.Remove(sigId);

            if (!p.StopLoss.HasValue)
            {
                Print($"⚠️ [VikingInvest] {p.SymbolName} limit filled with NO stop — attaching SL={pl.Stop:F5}");
                try { p.ModifyStopLossPrice(pl.Stop); } catch (Exception ex) { Print($"   ModifyStopLossPrice threw: {ex.Message}"); }
                if (!p.StopLoss.HasValue)
                {
                    Print($"🛑 [VikingInvest] SL still not set on {p.SymbolName} PID={p.Id} — CLOSING to avoid a naked position. id={sigId}");
                    TelegramSend($"🛑 {p.SymbolName} limit filled WITHOUT a stop and it couldn't be attached — position CLOSED immediately. id={sigId}", important: true);
                    try { ClosePosition(p); } catch (Exception ex) { Print($"   Emergency ClosePosition threw: {ex.Message}"); }
                    return;
                }
            }

            _positionIdToSignalId[p.Id] = sigId;
            PersistPositionSignalMap();   // durable so a restart-orphaned close keeps its method
            _ordersPlaced++;
            Print($"✅ [VikingInvest] LIMIT FILLED {p.TradeType} {p.SymbolName} {p.VolumeInUnits:F0} @ {p.EntryPrice:F5} · id={sigId} PID={p.Id}");
            TelegramSend($"📤 {p.TradeType} {p.SymbolName} {p.VolumeInUnits:F0} units (limit) · " +
                         $"entry {p.EntryPrice:F5} SL={pl.Stop:F5} TP={pl.Target:F5}", important: Account.IsLive);
            WriteExecution(new ExecutionRow
            {
                Event       = "placed",
                SignalId    = sigId,
                PositionId  = p.Id,
                Pair        = pl.Pair,
                Symbol      = p.SymbolName,
                Dir         = pl.Dir,
                VolumeUnits = p.VolumeInUnits,
                EntryAttempt= pl.Entry,
                EntryFilled = p.EntryPrice,
                Stop        = pl.Stop,
                Target      = pl.Target,
                RSize       = pl.RSize,
                SlippagePips= (pl.Entry > 0 && p.Symbol.PipSize > 0)
                              ? (p.EntryPrice - pl.Entry) / p.Symbol.PipSize * (pl.Dir == "bull" ? 1 : -1)
                              : 0,
                AccountMode = Account.IsLive ? "live" : "demo",
                Account     = Account.Number,
            });
        }

        private void OnPositionClosed(PositionClosedEventArgs args)
        {
            var p = args.Position;
            if (p == null) return;
            if (p.Label != OrderLabel) return; // not one of ours

            string sigId = null;
            _positionIdToSignalId.TryGetValue(p.Id, out sigId);
            // Survive a cBot restart (in-memory map lost): the signal_id — which carries the
            // method as its 3rd segment (pair:ts:method) — is recoverable from the position
            // Comment. Without this fallback the closed row logs a null signal_id, which the
            // dashboard renders as an "unlabelled" winning strategy ("?").
            if (string.IsNullOrEmpty(sigId)) sigId = SignalIdFromComment(p.Comment);

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

            // Phase 1 inspector — pull the MFE trace + close-source stamp for
            // this position. Default close-source to "broker" (SL/TP fill or
            // manual close in cTrader) when nothing in our code stamped it.
            double mfeR = 0;
            _positionMaxFavR.TryGetValue(p.Id, out mfeR);
            string closeSrc;
            if (!_positionCloseSource.TryGetValue(p.Id, out closeSrc)) closeSrc = "broker";

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
                MfeR        = mfeR,
                CloseSource = closeSrc,
                AccountMode = Account.IsLive ? "live" : "demo",
                Account     = Account.Number,
            });
            if (sigId != null) _positionIdToSignalId.Remove(p.Id);
            PersistPositionSignalMap();   // keep the on-disk map in step with the live open set
            _positionMaxFavR.Remove(p.Id);
            _positionCloseSource.Remove(p.Id);
            _posOrigR.Remove(p.Id); _posPeakPx.Remove(p.Id);   // trailing-state cleanup
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
            // 2026-06-30 — direction-aware "reached the level" instead of
            // a fixed 2-pip band. The old band mislabelled index/metal TP
            // fills (SPX500, XAU, XAG) that overshoot the TP by more than
            // 2 of their large pips as "manual-or-broker", under-counting
            // genuine target-hits. Now: a long that exits at/above its TP
            // (or a short at/below) is a target-hit; at/beyond the SL is a
            // stop-hit. A small tolerance lets an exactly-AT-level fill
            // still count. Anything BETWEEN the levels (a real manual /
            // partial exit) falls through to "manual-or-broker".
            try
            {
                var exit = p.Symbol?.Bid ?? p.EntryPrice;
                bool isBuy = p.TradeType == TradeType.Buy;
                var tol = (p.Symbol != null ? p.Symbol.PipSize : 0) * 0.5;
                if (p.TakeProfit.HasValue)
                {
                    var tp = p.TakeProfit.Value;
                    bool tpReached = isBuy ? (exit >= tp - tol) : (exit <= tp + tol);
                    if (tpReached) return "target-hit";
                }
                if (p.StopLoss.HasValue)
                {
                    var sl = p.StopLoss.Value;
                    bool slReached = isBuy ? (exit <= sl + tol) : (exit >= sl - tol);
                    // A stop hit that closes IN PROFIT is the trailing stop banking a gain
                    // (stop ratcheted past break-even), not a stop-out — label it "trail-hit".
                    if (slReached) return p.NetProfit > 0 ? "trail-hit" : "stop-hit";
                }
            }
            catch { }
            return "manual-or-broker";
        }

        // ───────────── Symbol resolution ──────────────────────────────
        // IC Markets cTrader uses uppercase bare symbols (EURUSD, BTCUSD).
        // We try the canonical form first, then a couple of common
        // suffix variants used by other brokers.
        // Maps our internal pair keys → broker-specific symbol candidates.
        // IC Markets cTrader uses different naming for commodities + indices
        // than the dashboard's pair identifiers. Each entry lists the most
        // likely broker symbol first, then known aliases. Add more candidates
        // here as new broker quirks surface.
        private static readonly Dictionary<string, string[]> _symbolAliases = new Dictionary<string, string[]>
        {
            // Commodities
            { "usoil",  new[] { "XBRUSD", "BRENT",   "BRENTOIL", "UKOIL",   "USOIL" } },
            { "wtiusd", new[] { "XTIUSD", "WTI",     "USOIL",    "OIL",     "WTIUSD" } },
            { "natgas", new[] { "XNGUSD", "NATGAS",  "NGAS",     "NG" } },
            // Indices
            { "de40",   new[] { "DE30",   "GER40",   "GER30",    "DAX40",   "DAX30",   "DE40" } },
            { "dj30",   new[] { "US30",   "DJI",     "DJ30" } },
            { "nas100", new[] { "USTEC",  "NAS100",  "NASDAQ",   "NQ" } },
            { "spx500", new[] { "US500",  "SPX500",  "SP500",    "ES" } },
            { "ftse100",new[] { "UK100",  "FTSE100", "FTSE" } },
            { "jp225",  new[] { "JPN225", "JP225",   "NIKKEI",   "N225" } },
            // Crypto — IC Markets uses uppercase bare, fall back to suffixed
            // variants for the few brokers that append qualifiers.
        };

        private Symbol ResolveSymbol(string pair)
        {
            var upper = pair.ToUpperInvariant();
            // 2026-06-15mmm: try broker-specific aliases first (commodities
            // + indices have wildly different names across MT5/cTrader
            // brokers). If no alias entry, fall back to the generic
            // "uppercase ± common suffix" sweep.
            List<string> candidates = new List<string>();
            if (_symbolAliases.ContainsKey(pair))
            {
                foreach (var name in _symbolAliases[pair])
                {
                    candidates.Add(name);
                    candidates.Add(name + ".r");
                    candidates.Add(name + ".pro");
                    candidates.Add(name + ".raw");
                }
            }
            else
            {
                candidates.Add(upper);
                candidates.Add(upper + ".r");
                candidates.Add(upper + ".pro");
                candidates.Add(upper + ".raw");
            }
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
            // 2026-07-03 — SPREAD-AWARE SIZING. The bid/ask spread is a real
            // cost paid on entry, so fold it into the effective stop distance:
            // a losing trade pays (spread + stop), so sizing off (stop + spread)
            // keeps total risk within the budget. This is what lets wide-spread
            // instruments (indices, BTC) trade INSIDE the per-trade risk limit
            // by taking a smaller lot instead of being rejected outright. FX,
            // where spread is a tiny fraction of the stop, is barely affected.
            var sizeSpreadPips = symbol.PipSize > 0 ? symbol.Spread / symbol.PipSize : 0;
            var effStopPips = stopPips + Math.Max(0, sizeSpreadPips);
            // PipValue is per 1 unit of the symbol. ExecuteMarketOrder
            // takes volume in UNITS (not lots), so we multiply through.
            var pipValuePerUnit = symbol.PipValue;
            if (pipValuePerUnit <= 0 || effStopPips <= 0) return 0;
            var volume = riskAmt / (effStopPips * pipValuePerUnit);
            // Normalize to broker's volume step.
            volume = symbol.NormalizeVolumeInUnits(volume, RoundingMode.Down);
            if (volume < symbol.VolumeInUnitsMin) return 0;
            if (volume > symbol.VolumeInUnitsMax) volume = symbol.VolumeInUnitsMax;
            return (long)volume;
        }

        // Signed risk-factor tags for a (feed-pair, dir). Mirrors the
        // dashboard inspector's _viFactorTags. Base leg carries +sign
        // (bull = long base), quote leg -sign; blocs that net to zero
        // across the two legs (a pure intra-bloc cross, e.g. AUDNZD) are
        // dropped. Used by the factor-risk sizing budget.
        private Dictionary<string, int> FactorTags(string pair, string dir)
        {
            var tags = new Dictionary<string, int>();
            if (string.IsNullOrEmpty(pair)) return tags;
            var p = pair.ToLowerInvariant();
            int sign = dir == "bull" ? 1 : -1;
            Func<string, string> bloc = c =>
            {
                c = (c ?? "").ToLowerInvariant();
                switch (c)
                {
                    case "usd": return "USD";
                    case "jpy": return "JPY";
                    case "chf": return "CHF";
                    case "aud": case "nzd": return "ANTIP";
                    case "cad": return "CAD";
                    case "eur": return "EUR";
                    case "gbp": return "GBP";
                    case "xau": case "xag": case "xpt": return "METAL";
                    case "oil": return "OIL";
                    case "crypto": return "CRYPTO";
                    default: return string.IsNullOrEmpty(c) ? null : c.ToUpperInvariant();
                }
            };
            var special = new Dictionary<string, string[]>
            {
                { "usoil",  new[]{ "oil", "usd" } }, { "wtiusd", new[]{ "oil", "usd" } },
                { "natgas", new[]{ "oil", (string)null } },
                { "xauusd", new[]{ "xau", "usd" } }, { "xagusd", new[]{ "xag", "usd" } },
                { "xptusd", new[]{ "xpt", "usd" } },
                // 2026-07-08 — crypto share one CRYPTO factor (BTC/ETH/XRP/SOL/…
                // are ~one bet; H10 fires 3/3–4/4 on their losses). Without this
                // each was its own factor, so a basket of crypto longs was
                // completely uncapped for correlation.
                { "btcusd", new[]{ "crypto", "usd" } }, { "ethusd", new[]{ "crypto", "usd" } },
                { "xrpusd", new[]{ "crypto", "usd" } }, { "solusd", new[]{ "crypto", "usd" } },
                { "nearusd", new[]{ "crypto", "usd" } }, { "suiusd", new[]{ "crypto", "usd" } },
                { "taousd", new[]{ "crypto", "usd" } }, { "ondousd", new[]{ "crypto", "usd" } }
            };
            string baseC, quoteC;
            if (special.ContainsKey(p)) { baseC = special[p][0]; quoteC = special[p][1]; }
            else if (p.Length >= 6) { baseC = p.Substring(0, 3); quoteC = p.Substring(3, 3); }
            else return tags;
            var acc = new Dictionary<string, int>();
            Action<string, int> add = (c, s) =>
            {
                var b = bloc(c); if (b == null) return;
                acc[b] = (acc.ContainsKey(b) ? acc[b] : 0) + s;
            };
            add(baseC, sign); add(quoteC, -sign);
            foreach (var kv in acc) { if (kv.Value > 0) tags[kv.Key] = 1; else if (kv.Value < 0) tags[kv.Key] = -1; }
            return tags;
        }

        // Current downside risk of an open position, in account currency —
        // stop distance × pip value × units. Mirrors OnPositionClosed's
        // realized-R denominator. Returns 0 if the position has no stop or
        // symbol data (excluded from the factor budget rather than guessed).
        private double PositionRiskAmount(Position p)
        {
            if (p == null || !p.StopLoss.HasValue || p.Symbol == null) return 0;
            if (p.Symbol.PipSize <= 0 || p.Symbol.PipValue <= 0) return 0;
            var stopDistPx = Math.Abs(p.EntryPrice - p.StopLoss.Value);
            if (stopDistPx <= 0) return 0;
            var stopPips = stopDistPx / p.Symbol.PipSize;
            return stopPips * p.Symbol.PipValue * p.VolumeInUnits;
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
            public double MfeR;             // max favourable excursion in R (Phase 1 inspector)
            public string CloseSource;      // cbot_invalidation | manual | broker
            public string AccountMode;      // "demo" | "live"
            public long   Account;
        }

        // 2026-07-03 — emit a 'rejected' execution row for a pre-order skip
        // so the gating REASON reaches executions.json (dashboard + remote
        // audit), not just the local VPS log. Broker-level rejections already
        // write their own row in the ExecuteMarketOrder failure branch.
        //
        // Throttled per (pair, category): a chronically-gated instrument (an
        // index whose spread is wide all session, a fast index that keeps
        // drifting past its entry) triggers a FRESH signal id every bar, so an
        // unthrottled dispatch would fire hundreds of repository_dispatch calls
        // a day and spam the ingest workflow. One row per pair+reason per
        // cooldown window is enough to see the pattern; the full detail always
        // stays in the local cBot log via the Print at each gate.
        private const int RejectLogCooldownMin = 30;
        private readonly Dictionary<string, long> _lastRejectDispatchMs = new Dictionary<string, long>();

        // Feed-only trade blocklist (TradeBlocklist param). Parsed + cached;
        // re-parsed only when the param string changes (cTrader lets the user
        // edit it live without a rebuild).
        private HashSet<string> _blocklistCache;
        private string _blocklistRaw;
        private bool IsBlocklisted(string pair)
        {
            if (string.IsNullOrWhiteSpace(pair)) return false;
            if (_blocklistCache == null || !string.Equals(_blocklistRaw, TradeBlocklist, StringComparison.Ordinal))
            {
                _blocklistRaw = TradeBlocklist;
                _blocklistCache = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (!string.IsNullOrWhiteSpace(TradeBlocklist))
                    foreach (var p in TradeBlocklist.Split(','))
                    {
                        var t = p.Trim();
                        if (t.Length > 0) _blocklistCache.Add(t);
                    }
            }
            return _blocklistCache.Contains(pair.Trim());
        }

        // A feed pair is FX iff it is 6 chars and BOTH halves are fiat ISO
        // codes. This cleanly separates FX (euraud, cadjpy, usdcad) from gold /
        // platinum (xauusd, xptusd), crypto (btcusd, xrpusd) and indices
        // (jp225) — so the FX-only cost floor never touches a non-FX pair.
        private static readonly HashSet<string> _fiatCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        { "usd","eur","gbp","jpy","aud","nzd","cad","chf","sgd","nok","sek","dkk","zar","mxn","try","hkd","cnh","pln","huf","czk" };
        private static bool IsFxPair(string pair)
        {
            if (string.IsNullOrEmpty(pair) || pair.Length != 6) return false;
            return _fiatCodes.Contains(pair.Substring(0, 3)) && _fiatCodes.Contains(pair.Substring(3, 3));
        }

        private void EmitRejection(Signal sig, string symbolName, string category, string detail)
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var key = (sig.Pair ?? "?") + "|" + category;
            if (_lastRejectDispatchMs.TryGetValue(key, out var last) &&
                nowMs - last < (long)RejectLogCooldownMin * 60000L)
                return; // within cooldown — already in the local log; skip the dispatch
            _lastRejectDispatchMs[key] = nowMs;
            WriteExecution(new ExecutionRow
            {
                Event        = "rejected",
                SignalId     = sig.Id,
                Pair         = sig.Pair,
                Symbol       = symbolName,
                Dir          = sig.Dir,
                EntryAttempt = sig.Entry,
                Stop         = sig.Stop,
                Target       = sig.Target,
                RSize        = sig.RSize,
                Reason       = category + ": " + detail,
                AccountMode  = Account.IsLive ? "live" : "demo",
                Account      = Account.Number,
            });
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
                F(sb, "strategy",      StrategyFromSignalId(r.SignalId));                sb.Append(',');
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
                F(sb, "mfe_r",         r.MfeR);                                          sb.Append(',');
                F(sb, "close_source",  r.CloseSource);                                   sb.Append(',');
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
                // GitHub's repository_dispatch caps client_payload at 10 TOP-
                // LEVEL properties and returns 422 otherwise. The execution
                // row carries ~24 fields, so we nest the whole row under a
                // single "row" key — client_payload then has exactly one
                // property. The ingest workflow reads client_payload.row.
                // (executionJsonLine is already a valid JSON object string.)
                var body = "{\"event_type\":\"cbot-execution\",\"client_payload\":{\"row\":" + executionJsonLine + "}}";
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
            public bool? EventAligned;   // H11 faytterro: true=aligned, false=no-event/fought, null=n/a
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
                    EventAligned  = JsonBoolN(obj, "event_aligned"),
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

        // Nullable bool: true / false, or null when the key is absent or JSON null.
        private static bool? JsonBoolN(string obj, string key)
        {
            var needle = "\"" + key + "\"";
            var p = obj.IndexOf(needle, StringComparison.Ordinal);
            if (p < 0) return null;
            var colon = obj.IndexOf(':', p);
            if (colon < 0) return null;
            int i = colon + 1;
            while (i < obj.Length && char.IsWhiteSpace(obj[i])) i++;
            if (i + 4 <= obj.Length && obj.Substring(i, 4) == "true") return true;
            if (i + 5 <= obj.Length && obj.Substring(i, 5) == "false") return false;
            return null;
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

