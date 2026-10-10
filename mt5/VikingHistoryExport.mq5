//+------------------------------------------------------------------+
//|                                        VikingHistoryExport.mq5    |
//|   Dumps H1 + M15 history for a list of broker symbols to a JSON  |
//|   file, so the .EQ strategies can be backtested on the exact     |
//|   instruments you'd trade (US/DE/UK/FR/JP/HK/ES CFDs that        |
//|   TwelveData won't serve). SCRIPT, not an EA — runs once, exits. |
//|   Defaults to ~3yr H1 + M15; set "Max bars in chart"=Unlimited.  |
//|                                                                  |
//|  RUN IT: MetaEditor → compile (F7). In MT5 Navigator → Scripts → |
//|   drag VikingHistoryExport onto any chart. In the dialog set the |
//|   symbols (or keep the defaults) and click OK. It writes         |
//|   MQL5/Files/<InpOut>. Find it via File → Open Data Folder →     |
//|   MQL5 → Files, then send that JSON back.                        |
//|                                                                  |
//|  NOTE: history depth is capped by Tools → Options → Charts →     |
//|   "Max bars in chart" — set it to Unlimited (or ≥ InpBars) first.|
//|   Times are broker server time (fine for backtesting — the       |
//|   detectors use bar sequence, not wall-clock hour).              |
//+------------------------------------------------------------------+
#property copyright "Viking Invest"
#property version   "1.00"
#property script_show_inputs
#property strict

// Symbols are split across SEVERAL input fields on purpose: MT5 truncates a single input-string
// parameter at ~250 chars, which silently dropped the tail of a long one-line list (the ES/FR/HK
// names). Each field below stays short; OnStart concatenates them. Edit the one for your region.
input string InpSymUS  = "AAPL.NAS,AMZN.NAS,MSFT.NAS,NVDA.NAS,TSLA.NAS";
input string InpSymDE  = "DBK.ETR,BOSS.ETR,PAH3.ETR,VOWG.ETR,BAYN.ETR,CBK.ETR,MBGn.ETR";
input string InpSymUK  = "BARC.LSE,BA.LSE,LSE.LSE,RR.LSE,TSCO.LSE,AML.LSE,BKG.LSE,EZJ.LSE,MKS.LSE,RDSB.LSE,NWG.LSE";
input string InpSymJPHK= "7267.TSE,8306.TSE,7974.TSE,6758.TSE,1288.HK,9898.HK,0003.HK";
input string InpSymESFR= "CABK.MAD,SAN.MAD,BNP.PAR,CAP.PAR,CA.PAR,BN.PAR,CDI.PAR";
input int    InpBarsH1  = 8000;                    // H1 bars (~3yr of RTH equity sessions)
input int    InpBarsM15 = 28000;                   // M15 bars (~3yr of RTH equity sessions)
input string InpOut     = "viking_intl_ohlc.json"; // written to MQL5/Files/

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

// write one timeframe array for a symbol; returns bars written
int WriteSeries(const int h, const string sym, ENUM_TIMEFRAMES tf, const int want)
  {
   MqlRates rates[];
   ArraySetAsSeries(rates,false);                  // index 0 = oldest (ASC)
   int got=0;
   for(int t=0;t<40 && got<=0;t++)                 // history may need to load — retry
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
   string all=InpSymUS+","+InpSymDE+","+InpSymUK+","+InpSymJPHK+","+InpSymESFR;
   string syms[]; int n=StringSplit(all,',',syms);
   int h=FileOpen(InpOut,FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(h==INVALID_HANDLE){ PrintFormat("cannot open %s (err %d)",InpOut,GetLastError()); return; }
   FileWriteString(h,"{\"granularities\":[\"h1\",\"m15\"],\"source\":\"mt5-export\",\"pairs\":{");
   int served=0;
   for(int i=0;i<n;i++)
     {
      string sym=syms[i]; StringTrimLeft(sym); StringTrimRight(sym);
      if(sym=="") continue;
      if(!SymbolSelect(sym,true)){ PrintFormat("symbol not found: %s — skipped",sym); continue; }
      string key=sanitize(sym);
      if(served>0) FileWriteString(h,",");
      FileWriteString(h,"\""+key+"\":{\"sym\":\""+sym+"\",\"h1\":");
      int nh=WriteSeries(h,sym,PERIOD_H1,InpBarsH1);
      FileWriteString(h,",\"m15\":");
      int nm=WriteSeries(h,sym,PERIOD_M15,InpBarsM15);
      FileWriteString(h,"}");
      served++;
      PrintFormat("%s: h1=%d m15=%d",sym,nh,nm);
     }
   FileWriteString(h,"}}");
   FileClose(h);
   PrintFormat("wrote %s — %d/%d symbols. Find it in MQL5/Files and send it back.",InpOut,served,n);
  }
//+------------------------------------------------------------------+
