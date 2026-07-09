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

        private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        private readonly HashSet<string> _seenIds = new HashSet<string>();
        private string _seenIdsPath;
        private bool _busy;

        protected override void OnStart()
        {
            var dir = System.IO.Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "VikingSwing");
            try { System.IO.Directory.CreateDirectory(dir); } catch { }
            _seenIdsPath = System.IO.Path.Combine(dir, "swing_seen_ids.txt");
            LoadSeenIds();
            Print($"[VikingSwing] started. acct={Account.Number} live={Account.IsLive} seen={_seenIds.Count} risk={RiskPct}%");
            if (Account.IsLive)
                Print("⚠️ [VikingSwing] LIVE account detected — this bot is intended for DEMO forward-testing.");
            Timer.Start(PollSeconds);
        }

        protected override void OnStop() { SaveSeenIds(); }

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
