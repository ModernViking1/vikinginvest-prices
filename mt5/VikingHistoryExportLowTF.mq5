//+------------------------------------------------------------------+
//| VikingHistoryExportLowTF — ONE-OFF M5 + M1 equity export          |
//|                                                                  |
//| Dumps ~1yr of M5 + M1 bars for a SMALL set of liquid US names to |
//| a SEPARATE file (equity-lowtf-ohlc.json), purely to backtest the |
//| 9EMA/20SMA pullback-runner (ma_pull) on finer timeframes.        |
//|                                                                  |
//| ONE-OFF — NOTHING in the live pipeline reads equity-lowtf-ohlc.  |
//| The bridge/engine use equity-signals.json / historical-ohlc.json |
//| only, so this cannot bloat the engine. Run once, send the JSON,  |
//| then delete it. Not committed as data.                           |
//|                                                                  |
//| Setup: "Max bars in chart" = Unlimited (or >= InpBarsM1), then    |
//| run this script on any chart. M1 history depth is broker-limited |
//| (often < 1yr for stock CFDs) — it grabs whatever is available.   |
//+------------------------------------------------------------------+
#property script_show_inputs

// ~10 liquid US names — enough for a statistically ample finer-TF test without a huge file.
input string InpSymbols = "AAPL.NAS,AMZN.NAS,MSFT.NAS,NVDA.NAS,TSLA.NAS,JPM.NYSE,BAC.NYSE,COIN.NAS,HOOD.NAS,UBER.NYSE";
input int    InpBarsM5  = 20000;                       // ~1yr of RTH M5 (6.5h x 12 x 252 ~ 19.7k)
input int    InpBarsM1  = 100000;                      // ~1yr of RTH M1 (grabs whatever the broker keeps)
input string InpOut     = "equity-lowtf-ohlc.json";    // SEPARATE one-off file in MQL5/Files/

string sanitize(const string s)
  {
   string r="";
   for(int i=0;i<StringLen(s);i++)
     {
      ushort c=StringGetCharacter(s,i);
      if((c>='A'&&c<='Z')||(c>='a'&&c<='z')||(c>='0'&&c<='9')) r+=ShortToString(c);
      else r+="_";
     }
   StringToLower(r);
   return r;
  }

int WriteSeries(const int h, const string sym, ENUM_TIMEFRAMES tf, const int want)
  {
   MqlRates rates[];
   ArraySetAsSeries(rates,false);                       // index 0 = oldest (ASC)
   int got=0;
   for(int t=0;t<40 && got<=0;t++)
     {
      got=CopyRates(sym,tf,0,want,rates);
      if(got<=0) Sleep(300);
     }
   int digits=(int)SymbolInfoInteger(sym,SYMBOL_DIGITS); if(digits<=0) digits=2;
   FileWriteString(h,"[");
   for(int i=0;i<got;i++)
     {
      MqlDateTime d; TimeToStruct(rates[i].time,d);
      string ts=StringFormat("%04d-%02d-%02d %02d:%02d:%02d",d.year,d.mon,d.day,d.hour,d.min,d.sec);
      string row="{\"t\":\""+ts+"\",\"o\":"+DoubleToString(rates[i].open,digits)
                 +",\"h\":"+DoubleToString(rates[i].high,digits)
                 +",\"l\":"+DoubleToString(rates[i].low,digits)
                 +",\"c\":"+DoubleToString(rates[i].close,digits)
                 +",\"v\":"+IntegerToString((long)rates[i].tick_volume)+"}";
      FileWriteString(h,row+(i<got-1?",":""));
     }
   FileWriteString(h,"]");
   return got;
  }

void OnStart()
  {
   string syms[]; int n=StringSplit(InpSymbols,',',syms);
   int h=FileOpen(InpOut,FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(h==INVALID_HANDLE){ PrintFormat("cannot open %s (err %d)",InpOut,GetLastError()); return; }
   FileWriteString(h,"{\"granularities\":[\"m5\",\"m1\"],\"source\":\"mt5-export-lowtf\",\"pairs\":{");
   int served=0;
   for(int i=0;i<n;i++)
     {
      string sym=syms[i]; StringTrimLeft(sym); StringTrimRight(sym);
      if(sym=="") continue;
      if(!SymbolSelect(sym,true)){ PrintFormat("symbol not found: %s — skipped",sym); continue; }
      string key=sanitize(sym);
      if(served>0) FileWriteString(h,",");
      FileWriteString(h,"\""+key+"\":{\"sym\":\""+sym+"\",\"m5\":");
      int n5=WriteSeries(h,sym,PERIOD_M5,InpBarsM5);
      FileWriteString(h,",\"m1\":");
      int n1=WriteSeries(h,sym,PERIOD_M1,InpBarsM1);
      FileWriteString(h,"}");
      served++;
      PrintFormat("%s (%s): m5=%d m1=%d",key,sym,n5,n1);
     }
   FileWriteString(h,"}}");
   FileClose(h);
   PrintFormat("wrote %s — %d symbols (ONE-OFF; send it over, then delete).",InpOut,served);
  }
//+------------------------------------------------------------------+
