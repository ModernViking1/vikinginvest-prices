// VikingHistoryExport.cs — cTrader (cAlgo) history exporter
//
// Dumps H1 + M15 history for a list of symbols to a JSON file in the SAME schema as the MT5
// exporter (mt5/VikingHistoryExport.mq5), so the .EQ strategies can be backtested on the exact
// instruments you'd trade from cTrader — including DE/UK/FR/JP/HK/ES names TwelveData won't serve.
// Feed the output to equity_broker_backtest.py:  python equity_broker_backtest.py --hist <file>
//
// USE IT:
//   1. cTrader → Automate → New cBot → paste this → Build.
//   2. Add it to any chart. In its parameters set "Symbols" to YOUR cTrader symbol names.
//      cTrader symbol names differ from MT5 (e.g. no .ETR/.LSE suffix). To keep the backtest's
//      market() classification working, use LABEL=BROKER entries, where LABEL is the MT5-style
//      name (carried into the JSON) and BROKER is the cTrader symbol actually fetched, e.g.:
//        BAYN.ETR=Bayer, BARC.LSE=Barclays, 7267.TSE=Honda, CABK.MAD=Caixabank
//      A plain entry (no '=') is used as both label and broker name.
//   3. Run it once. It writes OutFile (default: Documents\viking_intl_ohlc.json), logs the path,
//      and stops. Commit/send that file back.
//
// NOTE: history depth depends on what your cTrader broker serves. The bot pages back with
// LoadMoreHistory() until it has the requested bar count or the broker has no more.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using cAlgo.API;
using cAlgo.API.Internals;
using IOFile = System.IO.File;
using IOPath = System.IO.Path;

namespace cAlgo.Robots
{
    [Robot(AccessRights = AccessRights.FullAccess, AddIndicators = false)]
    public class VikingHistoryExport : Robot
    {
        [Parameter("Symbols (LABEL=BROKER, comma-sep)", DefaultValue =
            "AAPL.NAS=AAPL,MSFT.NAS=MSFT,NVDA.NAS=NVDA,AMZN.NAS=AMZN,TSLA.NAS=TSLA," +
            "DBK.ETR=Bayer,BAYN.ETR=Bayer,CBK.ETR=Commerzbank,MBGn.ETR=Mercedes,VOWG.ETR=Volkswagen," +
            "BARC.LSE=Barclays,RR.LSE=RollsRoyce,LSE.LSE=LSEG,TSCO.LSE=Tesco,AML.LSE=AstonMartin," +
            "BKG.LSE=Berkeley,EZJ.LSE=easyJet,MKS.LSE=MarksSpencer,NWG.LSE=NatWest," +
            "BNP.PAR=BNPParibas,CAP.PAR=Capgemini,CA.PAR=Carrefour,BN.PAR=Danone,CDI.PAR=Dior," +
            "7267.TSE=Honda,8306.TSE=MitsubishiUFJ,7974.TSE=Nintendo,6758.TSE=Sony," +
            "1288.HK=AgBankChina,9898.HK=Weibo,0003.HK=HKChinaGas,CABK.MAD=Caixabank,SAN.MAD=Santander")]
        public string Symbols { get; set; }

        [Parameter("H1 bars (~3yr)", DefaultValue = 8000, MinValue = 400)]
        public int BarsH1 { get; set; }

        [Parameter("M15 bars (~3yr)", DefaultValue = 28000, MinValue = 400)]
        public int BarsM15 { get; set; }

        [Parameter("Output file", DefaultValue = "viking_intl_ohlc.json")]
        public string OutFile { get; set; }

        protected override void OnStart()
        {
            var outPath = OutFile;
            if (!outPath.Contains("\\") && !outPath.Contains("/"))
                outPath = IOPath.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), OutFile);

            var sb = new StringBuilder();
            sb.Append("{\"granularities\":[\"h1\",\"m15\"],\"source\":\"ctrader-export\",\"pairs\":{");

            int served = 0;
            foreach (var raw in Symbols.Split(','))
            {
                var entry = raw.Trim();
                if (entry.Length == 0) continue;
                string label = entry, broker = entry;
                int eq = entry.IndexOf('=');
                if (eq > 0) { label = entry.Substring(0, eq).Trim(); broker = entry.Substring(eq + 1).Trim(); }

                var sym = Symbols_GetSymbol(broker);
                if (sym == null) { Print("symbol not found: {0} — skipped", broker); continue; }

                if (served > 0) sb.Append(",");
                sb.Append("\"").Append(Sanitize(label)).Append("\":{\"sym\":\"").Append(label).Append("\",\"h1\":");
                int nh = WriteSeries(sb, broker, TimeFrame.Hour, BarsH1);
                sb.Append(",\"m15\":");
                int nm = WriteSeries(sb, broker, TimeFrame.Minute15, BarsM15);
                sb.Append("}");
                served++;
                Print("{0} ({1}): h1={2} m15={3}", label, broker, nh, nm);
            }
            sb.Append("}}");

            try
            {
                IOFile.WriteAllText(outPath, sb.ToString());
                Print("wrote {0} — {1} symbols. Find it there and send it back.", outPath, served);
            }
            catch (Exception e) { Print("write failed: {0}", e.Message); }
            Stop();
        }

        // Resolve a symbol by name (wrapper keeps the call site tidy / null-safe).
        private Symbol Symbols_GetSymbol(string name)
        {
            try { return Symbols.GetSymbol(name); }
            catch { return null; }
        }

        // Page history back until we have `want` bars (or the broker has no more), then write the
        // oldest->newest array. Matches the MT5 exporter's row schema exactly.
        private int WriteSeries(StringBuilder sb, string symName, TimeFrame tf, int want)
        {
            var bars = MarketData.GetBars(tf, symName);
            int guard = 0;
            while (bars != null && bars.Count < want && guard++ < 200)
            {
                int got = bars.LoadMoreHistory();
                if (got <= 0) break;
            }
            sb.Append("[");
            if (bars != null && bars.Count > 0)
            {
                int start = Math.Max(0, bars.Count - want);
                for (int i = start; i < bars.Count; i++)
                {
                    var ts = bars.OpenTimes[i].ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    sb.Append("{\"t\":\"").Append(ts).Append("\",\"o\":").Append(Num(bars.OpenPrices[i]))
                      .Append(",\"h\":").Append(Num(bars.HighPrices[i]))
                      .Append(",\"l\":").Append(Num(bars.LowPrices[i]))
                      .Append(",\"c\":").Append(Num(bars.ClosePrices[i]))
                      .Append(",\"v\":").Append(((long)bars.TickVolumes[i]).ToString(CultureInfo.InvariantCulture))
                      .Append("}");
                    if (i < bars.Count - 1) sb.Append(",");
                }
            }
            sb.Append("]");
            return bars == null ? 0 : bars.Count;
        }

        private static string Num(double v)
        {
            return v.ToString("0.######", CultureInfo.InvariantCulture);
        }

        private static string Sanitize(string s)
        {
            var r = new StringBuilder();
            foreach (char c in s)
                r.Append((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ? c : '_');
            return r.ToString().ToLowerInvariant();
        }
    }
}
