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

namespace cAlgo.Robots
{
    [Robot(AccessRights = AccessRights.FullAccess, AddIndicators = true)]
    public class VikingInvestSignalBridge : Robot
    {
        // ───────────── Parameters ─────────────────────────────────────
        [Parameter("Signals URL", DefaultValue = "https://cdn.jsdelivr.net/gh/ModernViking1/vikinginvest-prices@main/signals.json", Group = "Feed")]
        public string SignalsUrl { get; set; }

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

        // ───────────── State ──────────────────────────────────────────
        private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        private HashSet<string> _seenIds = new HashSet<string>();
        private string _seenIdsPath;
        private int _signalsSeen, _ordersPlaced, _ordersSkipped;

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

            _seenIdsPath = Path.Combine(Environment.GetFolderPath(
                Environment.SpecialFolder.LocalApplicationData),
                "VikingInvest", "seen_ids.txt");
            Directory.CreateDirectory(Path.GetDirectoryName(_seenIdsPath));
            LoadSeenIds();

            _http.DefaultRequestHeaders.UserAgent.ParseAdd("VikingInvest-cTrader-Bot/1.0");

            Print("✅ [VikingInvest] Bridge initialised.");
            Print($"   URL: {SignalsUrl}");
            Print($"   Poll: every {PollSeconds}s · Risk: {RiskPctPerTrade}% · Max positions: {MaxOpenPositions}");
            Print($"   Mode: {(DryRun ? "DRY-RUN (log only)" : (Account.IsLive ? "LIVE" : "DEMO"))}");
            Print($"   Seen ids loaded: {_seenIds.Count}");

            Timer.Start(TimeSpan.FromSeconds(Math.Max(5, PollSeconds)));
            // Kick the first poll immediately so the user gets feedback.
            _ = PollAndProcess();
        }

        protected override void OnTimer()
        {
            _ = PollAndProcess();
        }

        protected override void OnStop()
        {
            Timer.Stop();
            SaveSeenIds();
            Print($"👋 [VikingInvest] Bridge stopped. Signals seen: {_signalsSeen} · Orders placed: {_ordersPlaced} · Skipped: {_ordersSkipped}");
        }

        // ───────────── Poll loop ──────────────────────────────────────
        private async Task PollAndProcess()
        {
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
                Print($"📡 [VikingInvest] Polled {signals.Count} signals from feed.");

            foreach (var sig in signals)
            {
                ProcessOneSignal(sig);
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

            // Stale-signal filter.
            if (sig.ArmedAtMs > 0)
            {
                var ageMin = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - sig.ArmedAtMs) / 60000.0;
                if (ageMin > MaxSignalAgeMin)
                {
                    Print($"⏭ [VikingInvest] Skipping stale signal id={sig.Id} (armed {ageMin:F0} min ago)");
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
            if (ourOpen >= MaxOpenPositions)
            {
                Print($"🛑 [VikingInvest] Max positions reached ({ourOpen}) — skipping id={sig.Id}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Spread filter.
            var spreadPips = symbol.Spread / symbol.PipSize;
            if (spreadPips > MaxSpreadPips)
            {
                Print($"🛑 [VikingInvest] Spread too wide on {symbol.Name}: {spreadPips:F1} > {MaxSpreadPips} pips. Skipping id={sig.Id}");
                MarkSeen(sig.Id); _ordersSkipped++;
                return;
            }

            // Risk-based volume sizing. Factors in the signal's r_size
            // (1.0 wick, 0.5 fib) so commodity / index trades take half
            // size automatically — matching the dashboard's net-R math.
            var riskPct = RiskPctPerTrade * (sig.RSize > 0 ? sig.RSize : 1.0);
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
            }
            else
            {
                Print($"❌ [VikingInvest] Order REJECTED {direction} {symbol.Name} · " +
                      $"error={result.Error} id={sig.Id}");
                _ordersSkipped++;
            }
            MarkSeen(sig.Id);
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
                if (!File.Exists(_seenIdsPath)) return;
                foreach (var line in File.ReadAllLines(_seenIdsPath))
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
                    File.AppendAllText(_seenIdsPath, id + Environment.NewLine);
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
                File.WriteAllLines(_seenIdsPath, toWrite);
            }
            catch (Exception ex)
            {
                Print($"⚠️ [VikingInvest] Could not save seen-ids file: {ex.Message}");
            }
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
                    ArmedAtMs = (long)JsonNum(obj, "armedAt"),
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

