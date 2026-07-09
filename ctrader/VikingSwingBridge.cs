// ============================================================================
// VikingSwingBridge — ISOLATED demo swing executor
// ----------------------------------------------------------------------------
// Trades ONLY swing-signals.json (the separate swing feed), independently of the
// intraday VikingInvestSignalBridge. Runs the two validated swing edges
// (S5+RSI, H&S+macro) as MARKET orders with swing-appropriate execution:
//   - market entry at the current price when a fresh signal arrives (NOT a
//     pullback limit — swing entries are momentum-confirmation, enter now)
//   - structural stop from the feed; target = entry ± rr*R computed at fill
//   - own risk budget, own OrderLabel, own seen-ids file -> cannot disturb the
//     intraday bot's positions or dedup state
//
// SAFETY: this is written to mirror the proven idioms in VikingInvestSignalBridge
// (JSON parse, ComputeVolume, ExecuteMarketOrder, naked-position guard, dedup).
// It has NOT been compiled in the authoring environment — build + run it on a
// DEMO account only, verify the first few fills against the feed, before trusting
// it. It never touches real capital unless deliberately attached to a live acct.
// ============================================================================
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using cAlgo.API;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    [Robot(AccessRights = AccessRights.FullAccess, AddIndicators = false)]
    public class VikingSwingBridge : Robot
    {
        [Parameter("Swing signals URL", DefaultValue = "https://cdn.jsdelivr.net/gh/ModernViking1/vikinginvest-prices@main/swing-signals.json", Group = "Feed")]
        public string SignalsUrl { get; set; }

        [Parameter("Poll seconds", DefaultValue = 60, MinValue = 15, MaxValue = 600, Group = "Feed")]
        public int PollSeconds { get; set; }

        [Parameter("Risk % per trade", DefaultValue = 1.0, MinValue = 0.1, MaxValue = 5.0, Group = "Risk")]
        public double RiskPct { get; set; }

        [Parameter("Max concurrent swing positions", DefaultValue = 12, MinValue = 1, MaxValue = 50, Group = "Risk")]
        public int MaxConcurrent { get; set; }

        [Parameter("Min stop (pips)", DefaultValue = 5.0, MinValue = 0.0, Group = "Risk")]
        public double MinStopPips { get; set; }

        [Parameter("Order label", DefaultValue = "VikingSwing", Group = "Execution")]
        public string OrderLabel { get; set; }

        // ---- execution auto-publish (optional; local JSONL always written) ----
        [Parameter("Auto-publish executions to repo", DefaultValue = false, Group = "Publish")]
        public bool AutoPublishToRepo { get; set; }

        [Parameter("GitHub PAT (contents:write)", DefaultValue = "", Group = "Publish")]
        public string GhPersonalAccessToken { get; set; }

        [Parameter("GitHub repo owner", DefaultValue = "ModernViking1", Group = "Publish")]
        public string GhRepoOwner { get; set; }

        [Parameter("GitHub repo name", DefaultValue = "vikinginvest-prices", Group = "Publish")]
        public string GhRepoName { get; set; }

        private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        private readonly HashSet<string> _seenIds = new HashSet<string>();
        private readonly Dictionary<long, string> _positionIdToSignalId = new Dictionary<long, string>();
        private string _seenIdsPath;
        private string _executionsPath;
        private bool _busy;

        protected override void OnStart()
        {
            var dir = System.IO.Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "VikingSwing");
            try { System.IO.Directory.CreateDirectory(dir); } catch { }
            _seenIdsPath = System.IO.Path.Combine(dir, "swing_seen_ids.txt");
            _executionsPath = System.IO.Path.Combine(dir, "swing-executions.jsonl");
            LoadSeenIds();
            Positions.Closed += OnPositionClosed;
            Print($"[VikingSwing] started. acct={Account.Number} live={Account.IsLive} seen={_seenIds.Count} risk={RiskPct}% publish={AutoPublishToRepo}");
            if (Account.IsLive)
                Print("⚠️ [VikingSwing] LIVE account detected — this bot is intended for DEMO forward-testing.");
            Timer.Start(PollSeconds);
        }

        protected override void OnStop() { Positions.Closed -= OnPositionClosed; SaveSeenIds(); }

        protected override void OnTimer()
        {
            if (_busy) return;
            _busy = true;
            PollAsync().ContinueWith(_ => _busy = false);
        }

        private async Task PollAsync()
        {
            string body;
            try { body = await _http.GetStringAsync(CacheBust(SignalsUrl)); }
            catch (Exception ex) { Print($"[VikingSwing] fetch failed: {ex.Message}"); return; }

            List<Sig> sigs;
            try { sigs = ParseSignals(body); }
            catch (Exception ex) { Print($"[VikingSwing] parse failed: {ex.Message}"); return; }
            if (sigs == null) return;

            // BeginInvokeOnMainThread: cAlgo trading calls must run on the bot thread,
            // not the await continuation thread.
            BeginInvokeOnMainThread(() =>
            {
                foreach (var s in sigs)
                {
                    try { Consider(s); }
                    catch (Exception ex) { Print($"[VikingSwing] consider {s.Id} threw: {ex.Message}"); }
                }
                SaveSeenIds();
            });
        }

        private void Consider(Sig s)
        {
            if (string.IsNullOrEmpty(s.Id) || _seenIds.Contains(s.Id)) return;
            if (s.State != "triggered") return;
            if (s.Dir != "bull" && s.Dir != "bear") return;

            // expiry (feed timestamps are epoch SECONDS)
            if (s.ExpiryTs > 0)
            {
                var exp = DateTimeOffset.FromUnixTimeSeconds(s.ExpiryTs).UtcDateTime;
                if (Server.Time > exp) { MarkSeen(s.Id); return; }
            }

            var concurrent = Positions.Count(p => p.Label == OrderLabel);
            if (concurrent >= MaxConcurrent)
            {
                Print($"[VikingSwing] max concurrent ({MaxConcurrent}) reached — skipping {s.Id}");
                return; // do NOT MarkSeen: retry next poll when a slot frees
            }

            var symbol = ResolveSymbol(s.Pair);
            if (symbol == null) { Print($"[VikingSwing] symbol not found for {s.Pair} id={s.Id}"); MarkSeen(s.Id); return; }

            var isBuy = s.Dir == "bull";
            var entry = isBuy ? symbol.Ask : symbol.Bid;   // MARKET entry now
            if (entry <= 0) return;

            // stop must be on the correct side of the current market
            if ((isBuy && s.Stop >= entry) || (!isBuy && s.Stop <= entry))
            {
                Print($"[VikingSwing] stop on wrong side / market moved past it — skipping {s.Id} (entry={entry:F5} stop={s.Stop:F5})");
                MarkSeen(s.Id); return;
            }

            var slPips = Math.Abs(entry - s.Stop) / symbol.PipSize;
            if (slPips < MinStopPips)
            {
                Print($"[VikingSwing] stop too tight {slPips:F1} < {MinStopPips} — skipping {s.Id}");
                MarkSeen(s.Id); return;
            }
            var rr = s.Rr > 0 ? s.Rr : 2.0;
            var tpPips = rr * slPips;

            var volume = ComputeVolume(symbol, entry, s.Stop, RiskPct);
            if (volume <= 0)
            {
                Print($"[VikingSwing] volume computed 0 for {symbol.Name} — skipping {s.Id}");
                MarkSeen(s.Id); return;
            }

            var direction = isBuy ? TradeType.Buy : TradeType.Sell;
            var result = ExecuteMarketOrder(direction, symbol.Name, volume, OrderLabel,
                                            slPips, tpPips, "swing-" + s.Id);
            if (result.IsSuccessful)
            {
                var pos = result.Position;
                // naked-position guard (mirrors intraday bot)
                if (!pos.StopLoss.HasValue)
                {
                    try { pos.ModifyStopLossPrice(s.Stop); } catch (Exception ex) { Print($"   ModifyStopLossPrice threw: {ex.Message}"); }
                    if (!pos.StopLoss.HasValue)
                    {
                        Print($"🛑 [VikingSwing] {symbol.Name} opened with NO stop — closing to avoid naked position. id={s.Id}");
                        try { ClosePosition(pos); } catch (Exception ex) { Print($"   Emergency ClosePosition threw: {ex.Message}"); }
                        MarkSeen(s.Id); return;
                    }
                }
                Print($"✅ [VikingSwing] {direction} {symbol.Name} {volume:F0}u @~{pos.EntryPrice:F5} " +
                      $"SL={pos.StopLoss:F5} TPpips={tpPips:F1} strat={s.Strategy} id={s.Id} pid={pos.Id}");
                _positionIdToSignalId[pos.Id] = s.Id;
                WriteExec("placed", s.Id, pos.Id, symbol.Name, s.Dir, pos.VolumeInUnits,
                          pos.EntryPrice, 0, pos.StopLoss ?? s.Stop, pos.TakeProfit ?? 0, 0, 0, 0, 0, "placed");
            }
            else
            {
                Print($"❌ [VikingSwing] order rejected {direction} {symbol.Name}: {result.Error} id={s.Id}");
            }
            MarkSeen(s.Id);
        }

        private Symbol ResolveSymbol(string pair)
        {
            if (string.IsNullOrEmpty(pair)) return null;
            foreach (var name in new[] { pair.ToUpperInvariant(), pair.ToLowerInvariant(), pair })
            {
                try { var sym = Symbols.GetSymbol(name); if (sym != null) return sym; } catch { }
            }
            return null;
        }

        private long ComputeVolume(Symbol symbol, double entry, double stop, double riskPct)
        {
            if (entry <= 0 || stop <= 0 || Math.Abs(entry - stop) < double.Epsilon) return 0;
            var riskAmt = Account.Equity * riskPct / 100.0;
            var stopPips = Math.Abs(entry - stop) / symbol.PipSize;
            var spreadPips = symbol.PipSize > 0 ? symbol.Spread / symbol.PipSize : 0;
            var effStopPips = stopPips + Math.Max(0, spreadPips);
            if (symbol.PipValue <= 0 || effStopPips <= 0) return 0;
            var volume = riskAmt / (effStopPips * symbol.PipValue);
            volume = symbol.NormalizeVolumeInUnits(volume, RoundingMode.Down);
            if (volume < symbol.VolumeInUnitsMin) return 0;
            if (volume > symbol.VolumeInUnitsMax) volume = symbol.VolumeInUnitsMax;
            return (long)volume;
        }

        // ---- execution logging (mirrors intraday bot: realizedR = NetProfit/riskAmt) ----
        private void OnPositionClosed(PositionClosedEventArgs args)
        {
            var p = args.Position;
            if (p == null || p.Label != OrderLabel) return;   // not one of ours
            string sigId = null;
            _positionIdToSignalId.TryGetValue(p.Id, out sigId);

            double realizedR = 0;
            try
            {
                double stopDistPx = (p.StopLoss.HasValue && p.EntryPrice > 0) ? Math.Abs(p.EntryPrice - p.StopLoss.Value) : 0;
                if (stopDistPx > 0 && p.Symbol.PipSize > 0 && p.Symbol.PipValue > 0)
                {
                    var stopPips = stopDistPx / p.Symbol.PipSize;
                    var riskAmt = stopPips * p.Symbol.PipValue * p.VolumeInUnits;
                    if (riskAmt > 0) realizedR = p.NetProfit / riskAmt;
                }
            }
            catch { }

            string reason;
            try
            {
                var exit = p.Symbol?.Bid ?? p.EntryPrice; var tol = (p.Symbol?.PipSize ?? 0) * 2;
                bool buy = p.TradeType == TradeType.Buy;
                if (p.TakeProfit.HasValue && ((buy && exit >= p.TakeProfit.Value - tol) || (!buy && exit <= p.TakeProfit.Value + tol))) reason = "target-hit";
                else if (p.StopLoss.HasValue && ((buy && exit <= p.StopLoss.Value + tol) || (!buy && exit >= p.StopLoss.Value - tol))) reason = "stop-hit";
                else reason = "manual-or-broker";
            }
            catch { reason = "manual-or-broker"; }

            WriteExec("closed", sigId, p.Id, p.SymbolName, p.TradeType == TradeType.Buy ? "bull" : "bear",
                      p.VolumeInUnits, p.EntryPrice, p.Symbol?.Bid ?? 0, p.StopLoss ?? 0, p.TakeProfit ?? 0,
                      p.NetProfit, p.Commissions, p.Swap, realizedR, reason);
            if (sigId != null) _positionIdToSignalId.Remove(p.Id);
            Print($"📒 [VikingSwing] closed {p.SymbolName} {p.TradeType} net={p.NetProfit:F2} R={realizedR:F2} reason={reason} id={sigId ?? "(unlinked)"}");
        }

        private void WriteExec(string ev, string sigId, long posId, string symbol, string dir, double vol,
                               double entry, double exit, double stop, double target,
                               double net, double comm, double swap, double realizedR, string reason)
        {
            long tsMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            try
            {
                var sb = new System.Text.StringBuilder(320);
                sb.Append('{');
                F(sb, "ts", tsMs); sb.Append(',');
                F(sb, "event", ev); sb.Append(',');
                F(sb, "signal_id", sigId); sb.Append(',');
                F(sb, "position_id", posId); sb.Append(',');
                F(sb, "pair", sigId != null && sigId.Contains(":") ? sigId.Split(':')[1] : symbol.ToLowerInvariant()); sb.Append(',');
                F(sb, "symbol", symbol); sb.Append(',');
                F(sb, "dir", dir); sb.Append(',');
                F(sb, "volume_units", vol); sb.Append(',');
                F(sb, "entry_filled", entry); sb.Append(',');
                F(sb, "exit_price", exit); sb.Append(',');
                F(sb, "stop", stop); sb.Append(',');
                F(sb, "target", target); sb.Append(',');
                F(sb, "net_profit", net); sb.Append(',');
                F(sb, "commissions", comm); sb.Append(',');
                F(sb, "swap", swap); sb.Append(',');
                F(sb, "realized_r", realizedR); sb.Append(',');
                F(sb, "reason", reason); sb.Append(',');
                F(sb, "account_mode", Account.IsLive ? "live" : "demo"); sb.Append(',');
                F(sb, "account", (long)Account.Number);
                sb.Append('}');
                var line = sb.ToString();
                File.AppendAllText(_executionsPath, line + Environment.NewLine);
                if (AutoPublishToRepo && !string.IsNullOrEmpty(GhPersonalAccessToken))
                    _ = DispatchAsync(line);
            }
            catch (Exception ex) { Print($"[VikingSwing] write execution failed: {ex.Message}"); }
        }

        private async Task DispatchAsync(string jsonLine)
        {
            try
            {
                var url = $"https://api.github.com/repos/{GhRepoOwner}/{GhRepoName}/dispatches";
                var body = "{\"event_type\":\"swing-cbot-execution\",\"client_payload\":{\"row\":" + jsonLine + "}}";
                using var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.Add("Accept", "application/vnd.github+json");
                req.Headers.Add("User-Agent", "VikingSwing-cTrader-Bot/1.0");
                req.Headers.Add("Authorization", $"Bearer {GhPersonalAccessToken}");
                req.Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
                var resp = await _http.SendAsync(req);
                if ((int)resp.StatusCode != 204)
                    Print($"[VikingSwing] dispatch returned {(int)resp.StatusCode} (local JSONL still recorded)");
            }
            catch (Exception ex) { Print($"[VikingSwing] dispatch failed (non-fatal): {ex.Message}"); }
        }

        private static void F(System.Text.StringBuilder sb, string k, string v)
        {
            sb.Append('"').Append(k).Append("\":");
            if (v == null) sb.Append("null");
            else sb.Append('"').Append(v.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
        }
        private static void F(System.Text.StringBuilder sb, string k, double v)
        {
            sb.Append('"').Append(k).Append("\":").Append(v.ToString("R", System.Globalization.CultureInfo.InvariantCulture));
        }
        private static void F(System.Text.StringBuilder sb, string k, long v)
        {
            sb.Append('"').Append(k).Append("\":").Append(v.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        private static string CacheBust(string url)
        {
            var sep = url.Contains("?") ? "&" : "?";
            return url + sep + "t=" + DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }

        // ---- dedup persistence (mirrors intraday bot) ----
        private void LoadSeenIds()
        {
            try { if (File.Exists(_seenIdsPath)) foreach (var l in File.ReadAllLines(_seenIdsPath)) if (!string.IsNullOrWhiteSpace(l)) _seenIds.Add(l.Trim()); }
            catch (Exception ex) { Print($"[VikingSwing] load seen-ids: {ex.Message}"); }
        }
        private void MarkSeen(string id)
        {
            if (_seenIds.Add(id))
                try { File.AppendAllText(_seenIdsPath, id + Environment.NewLine); } catch { }
        }
        private void SaveSeenIds()
        {
            try
            {
                var toWrite = _seenIds.Count > 5000 ? _seenIds.Skip(_seenIds.Count - 5000) : _seenIds;
                File.WriteAllLines(_seenIdsPath, toWrite);
            }
            catch { }
        }

        // ---- swing-signals.json parse (mirrors intraday bot's manual parser) ----
        private class Sig
        {
            public string Id, Pair, State, Dir, Strategy;
            public double Stop, Rr;
            public long ExpiryTs, TriggerTs;
        }

        private List<Sig> ParseSignals(string body)
        {
            if (string.IsNullOrEmpty(body)) return null;
            var a = body.IndexOf("\"signals\"", StringComparison.Ordinal);
            if (a < 0) return null;
            a = body.IndexOf('[', a); if (a < 0) return null;
            var e = FindMatchingBracket(body, a); if (e < 0) return null;
            var arr = body.Substring(a + 1, e - a - 1);
            var outp = new List<Sig>();
            int pos = 0;
            while (pos < arr.Length)
            {
                var os = arr.IndexOf('{', pos); if (os < 0) break;
                var oe = FindMatchingBracket(arr, os); if (oe < 0) break;
                var o = arr.Substring(os, oe - os + 1);
                outp.Add(new Sig
                {
                    Id = JsonStr(o, "id"), Pair = JsonStr(o, "pair"), State = JsonStr(o, "state"),
                    Dir = JsonStr(o, "dir"), Strategy = JsonStr(o, "strategy"),
                    Stop = JsonNum(o, "stop"), Rr = JsonNum(o, "rr"),
                    ExpiryTs = (long)JsonNum(o, "expiry_ts"), TriggerTs = (long)JsonNum(o, "trigger_ts"),
                });
                pos = oe + 1;
            }
            return outp;
        }

        private static string JsonStr(string obj, string key)
        {
            var p = obj.IndexOf("\"" + key + "\"", StringComparison.Ordinal); if (p < 0) return "";
            var c = obj.IndexOf(':', p); if (c < 0) return "";
            var q1 = obj.IndexOf('"', c); if (q1 < 0) return "";
            var q2 = obj.IndexOf('"', q1 + 1); if (q2 < 0) return "";
            return obj.Substring(q1 + 1, q2 - q1 - 1);
        }
        private static double JsonNum(string obj, string key)
        {
            var p = obj.IndexOf("\"" + key + "\"", StringComparison.Ordinal); if (p < 0) return 0;
            var c = obj.IndexOf(':', p); if (c < 0) return 0;
            var s = c + 1; while (s < obj.Length && char.IsWhiteSpace(obj[s])) s++;
            if (s >= obj.Length || obj[s] == '"') return 0;
            var en = s;
            while (en < obj.Length) { var ch = obj[en]; if (!(char.IsDigit(ch) || ch == '.' || ch == '-' || ch == 'e' || ch == 'E' || ch == '+')) break; en++; }
            if (en <= s) return 0;
            return double.TryParse(obj.Substring(s, en - s), System.Globalization.NumberStyles.Float,
                                   System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : 0;
        }
        private static int FindMatchingBracket(string s, int openIdx)
        {
            var open = s[openIdx]; var close = open == '[' ? ']' : '}';
            int depth = 0; bool inStr = false;
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
