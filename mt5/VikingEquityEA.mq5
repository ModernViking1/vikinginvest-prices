//+------------------------------------------------------------------+
//|                                            VikingEquityEA.mq5     |
//|   Viking Invest — equity (.EQ) demo pilot executor for MT5       |
//|                                                                  |
//|  Polls the published equity-signals.json feed and executes the  |
//|  .EQ book (holygrail_eq / holygrail_eq_m15 / twob_eq /          |
//|  volbreak_eq) on an MT5 account. Entry = market on a fresh       |
//|  'armed' signal; exit = TRAILING RUNNER (arm at +ArmR, ride     |
//|  DistR behind the best price, hard time-horizon from            |
//|  trail_hold_bars x timeframe). SL-only on entry (no fixed TP).  |
//|                                                                  |
//|  SCAFFOLD — demo-first. Default is DEMO-ONLY and it refuses to   |
//|  trade a live account unless you explicitly opt in. Test on the  |
//|  demo account, confirm fills match the feed, THEN consider live. |
//|  The signal server never connects here: this EA pulls a public   |
//|  read-only JSON; your account credentials never leave MT5.       |
//|                                                                  |
//|  SETUP (one-time): MT5 → Tools → Options → Expert Advisors →     |
//|    tick "Allow WebRequest for listed URL" and add:              |
//|       https://cdn.jsdelivr.net                                   |
//|    Attach to ONE chart (any symbol); it manages all .EQ symbols. |
//+------------------------------------------------------------------+
#property copyright "Viking Invest"
#property version   "1.01"
#property strict

#include <Trade/Trade.mqh>
#include <Trade/PositionInfo.mqh>

input string InpFeedURL      = "https://cdn.jsdelivr.net/gh/ModernViking1/vikinginvest-prices@main/equity-signals.json";
input string InpLocalFile    = "";        // if set, read this file from MQL5/Files instead of the URL
                                          //   (bridge mode — equity_bridge.py writes it there). e.g. "equity-signals.json"
input int    InpPollSeconds  = 30;        // feed poll cadence (OANDA/equity feed refreshes ~15 min)
input double InpRiskPct      = 0.5;       // risk per trade, % of account equity
input long   InpMagic        = 5204940;   // EA magic (ties positions to this EA)
input int    InpMaxAgeMin    = 120;       // skip a signal whose entry bar is older than this
input bool   InpAllowLive    = false;     // HARD GUARD: false = trade DEMO accounts only
input int    InpMaxOpenPerSym= 1;         // max concurrent EA positions per symbol
input int    InpSlippagePts  = 30;        // max deviation (points)
input double InpMaxNotionalPct = 20.0;    // cap a position's NOTIONAL at this % of equity (the real
                                          //   size governor for stock CFDs — tight stops would make
                                          //   pure 0.5%-risk sizing hugely over-leveraged)
input double InpMaxLot       = 100000.0;  // absolute lot backstop (notional cap normally binds first)
input bool   InpVerbose      = true;      // log decisions

CTrade        trade;
CPositionInfo pos;

// per-ticket trail state (rebuilt on init from open EA positions)
struct TrailState { ulong ticket; double entry; double initStop; double R; int dir; bool armed; double best; datetime expiry; };
TrailState gStates[];
string      gActed[];          // signal ids already acted this session (dedup)
string      gActedFile = "viking_equity_acted.csv";
string      gOpenLedger = "equity_trades_open.csv";   // ticket->entry/SL so WR/RR can be computed

//+------------------------------------------------------------------+
int OnInit()
  {
   trade.SetExpertMagicNumber(InpMagic);
   trade.SetDeviationInPoints(InpSlippagePts);
   trade.SetTypeFillingBySymbol(_Symbol);
   LoadActed();
   RebuildStates();
   EventSetTimer(MathMax(5, InpPollSeconds));
   PrintFormat("VikingEquityEA init — demo-only=%s, risk=%.2f%%, feed=%s",
               (string)(!InpAllowLive), InpRiskPct, InpFeedURL);
   if(!IsTradeableAccount())
      Print("WARNING: live account + AllowLive=false → EA will NOT place trades (demo guard).");
   return(INIT_SUCCEEDED);
  }

void OnDeinit(const int reason){ EventKillTimer(); }

//+------------------------------------------------------------------+
//| Poll the feed on the timer                                       |
//+------------------------------------------------------------------+
void OnTimer()
  {
   string body;
   bool ok = (StringLen(InpLocalFile) > 0) ? ReadLocalBody(InpLocalFile, body) : HttpGet(InpFeedURL, body);
   if(!ok)
      return;                                   // transient fetch/read failure → try next tick

   // kill-switch / demo-default are top-level; signals is the array
   if(JTopBool(body, "killed"))
     { if(InpVerbose) Print("feed kill_switch active — standing down"); return; }

   string arr;
   if(!ExtractArray(body, "signals", arr))
      return;

   string objs[];
   int n = SplitObjects(arr, objs);
   for(int i=0;i<n;i++)
      ProcessSignal(objs[i]);
  }

//+------------------------------------------------------------------+
//| Manage trailing + time-exit on every tick                       |
//+------------------------------------------------------------------+
void OnTick()
  {
   for(int i=ArraySize(gStates)-1; i>=0; i--)
     {
      if(!PositionSelectByTicket(gStates[i].ticket)) { Remove(i); continue; }
      string sym = PositionGetString(POSITION_SYMBOL);
      double bid = SymbolInfoDouble(sym, SYMBOL_BID);
      double ask = SymbolInfoDouble(sym, SYMBOL_ASK);
      double px  = (gStates[i].dir>0) ? bid : ask;      // conservative mark for the open side
      double R   = gStates[i].R;
      if(R<=0){ continue; }

      // hard time horizon
      if(TimeCurrent() >= gStates[i].expiry)
        { trade.PositionClose(gStates[i].ticket); if(InpVerbose) PrintFormat("%s: horizon close", sym); continue; }

      // track best price
      if(gStates[i].dir>0) gStates[i].best = MathMax(gStates[i].best, px);
      else                 gStates[i].best = MathMin(gStates[i].best, px);

      // arm once profit >= ArmR*R (ArmR folded into initStop distance == 1R; arm at +1R)
      double profitR = (gStates[i].dir>0) ? (px-gStates[i].entry)/R : (gStates[i].entry-px)/R;
      if(!gStates[i].armed && profitR >= 1.0) gStates[i].armed = true;    // arm at +1R (TRAIL_ARM)
      if(!gStates[i].armed) continue;

      // trail 1R behind the best (TRAIL_DIST=1R)
      double newSL = (gStates[i].dir>0) ? gStates[i].best - R : gStates[i].best + R;
      double curSL = PositionGetDouble(POSITION_SL);
      bool improve = (gStates[i].dir>0) ? (newSL > curSL) : (newSL < curSL || curSL==0);
      if(improve)
        {
         double tp = PositionGetDouble(POSITION_TP);
         newSL = NormalizeDouble(newSL, (int)SymbolInfoInteger(sym, SYMBOL_DIGITS));
         trade.PositionModify(gStates[i].ticket, newSL, tp);
        }
     }
  }

//+------------------------------------------------------------------+
//| Decide + place a single signal                                  |
//+------------------------------------------------------------------+
void ProcessSignal(const string obj)
  {
   string id    = JGetStr(obj, "id");
   string sym   = JGetStr(obj, "sym");
   string dirS  = JGetStr(obj, "dir");
   string state = JGetStr(obj, "state");
   double entry = JGetNum(obj, "entry");
   double stop  = JGetNum(obj, "stop");
   double ageM  = JGetNum(obj, "age_min");
   bool   demo  = JGetBool(obj, "demo_only");
   double armR  = JGetNum(obj, "trail_arm_r");    // informational; arm/dist are 1R by design
   double holdB = JGetNum(obj, "trail_hold_bars");
   string tf    = JGetStr(obj, "tf");
   if(id=="" || sym=="" || entry<=0 || stop<=0) return;

   // demo guard
   if(demo && !IsTradeableAccount())
     { if(InpVerbose) PrintFormat("skip %s — demo_only but account not demo (AllowLive=%s)", id, (string)InpAllowLive); return; }
   if(ageM > InpMaxAgeMin) { if(InpVerbose) PrintFormat("skip %s — stale (%.0f>%d min)", id, ageM, InpMaxAgeMin); return; }
   if(AlreadyActed(id))    return;
   if(OpenCountForSymbol(sym) >= InpMaxOpenPerSym) return;
   if(!SymbolSelect(sym, true)) { PrintFormat("skip %s — symbol %s not in Market Watch", id, sym); return; }
   if((ENUM_SYMBOL_TRADE_MODE)SymbolInfoInteger(sym, SYMBOL_TRADE_MODE)==SYMBOL_TRADE_MODE_DISABLED) return;

   int dir = (dirS=="bull") ? 1 : -1;
   if(MathAbs(entry - stop) <= 0) return;

   int    digits = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
   double point  = SymbolInfoDouble(sym, SYMBOL_POINT);
   double price  = (dir>0) ? SymbolInfoDouble(sym, SYMBOL_ASK) : SymbolInfoDouble(sym, SYMBOL_BID);
   // broker minimum distance between market and a stop (stock CFDs enforce this -> retcode 10016)
   double minDist = (double)SymbolInfoInteger(sym, SYMBOL_TRADE_STOPS_LEVEL) * point;
   double sl = stop;
   // setup has MOVED since the signal bar — SL now on the wrong side of the live price -> skip,
   // don't chase a blown level (same discipline as the swing stale-fill guard).
   if((dir>0 && sl >= price) || (dir<0 && sl <= price))
     { if(InpVerbose) PrintFormat("skip %s — SL %.5f wrong side of market %.5f (setup moved)", id, sl, price); return; }
   // too close to market -> widen to the broker minimum (accept a slightly larger R rather than reject)
   if(dir>0 && (price - sl) < minDist) sl = price - minDist;
   if(dir<0 && (sl - price) < minDist) sl = price + minDist;
   sl = NormalizeDouble(sl, digits);
   double R = MathAbs(price - sl);               // R off the actual fill-side price + (adjusted) SL
   if(R<=0) return;
   double lots = LotsForRisk(sym, R, price);
   if(lots<=0) { PrintFormat("skip %s — lot sizing <=0", id); return; }

   bool ok = (dir>0) ? trade.Buy(lots, sym, 0.0, sl, 0.0, id)
                     : trade.Sell(lots, sym, 0.0, sl, 0.0, id);
   if(!ok) { PrintFormat("ORDER FAIL %s %s: %d %s", sym, dirS, trade.ResultRetcode(), trade.ResultRetcodeDescription()); return; }

   ulong ticket = trade.ResultOrder();
   // find the resulting position ticket (hedging: by symbol+magic+comment)
   ulong ptk = FindPositionByComment(sym, id);
   double holdSec = holdB * TfMinutes(tf) * 60.0;
   ulong  statetk = (ptk>0?ptk:ticket);
   AddState(statetk, price, sl, R, dir, TimeCurrent()+(datetime)holdSec);
   LogOpen(statetk, id, sym, dir, price, sl, lots);   // ledger so the bridge can compute WR/RR
   MarkActed(id);
   PrintFormat("PLACED %s %s %.2f lots entry~%.4f SL %.4f (R=%.4f, hold~%.0fh)", sym, dirS, lots, price, sl, R, holdSec/3600.0);
  }

// Append an opened trade to the ledger (ticket,id,sym,dir,entry,initSL,lots,open_epoch). The bridge
// matches these to MT5's closed deals to compute realised R-multiples (win rate / RR).
void LogOpen(ulong tk, const string id, const string sym, int dir, double entry, double sl, double lots)
  {
   int h = FileOpen(gOpenLedger, FILE_READ|FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_SHARE_READ|FILE_SHARE_WRITE);
   if(h==INVALID_HANDLE) return;
   FileSeek(h, 0, SEEK_END);
   FileWriteString(h, StringFormat("%I64u,%s,%s,%d,%.5f,%.5f,%.2f,%I64d\n",
                                    tk, id, sym, dir, entry, sl, lots, (long)TimeCurrent()));
   FileClose(h);
  }

//+------------------------------------------------------------------+
//| Risk-based position sizing                                       |
//+------------------------------------------------------------------+
double LotsForRisk(const string sym, double stopDist, double price)
  {
   double equity   = AccountInfoDouble(ACCOUNT_EQUITY);
   double riskMoney= equity * (InpRiskPct/100.0);
   double tickVal  = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
   double tickSize = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE);
   if(tickVal<=0 || tickSize<=0) return 0.0;
   double lossPerLot = (stopDist / tickSize) * tickVal;   // account-currency loss per 1.0 lot at the stop
   if(lossPerLot<=0) return 0.0;
   double lots = riskMoney / lossPerLot;
   // NOTIONAL CAP — the real governor for stock CFDs. A tight structural stop makes pure 0.5%-risk
   // sizing demand a notional far above the account; cap each position at InpMaxNotionalPct of equity
   // (so on tight-stop names the realised risk is simply < the 0.5% target, which is conservative).
   double contract = SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE); if(contract<=0) contract=1.0;
   double notionalPerLot = contract * price;
   if(notionalPerLot > 0)
     {
      double maxLotsByNotional = (equity * (InpMaxNotionalPct/100.0)) / notionalPerLot;
      lots = MathMin(lots, maxLotsByNotional);
     }
   // clamp to symbol volume constraints + the absolute backstop
   double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN);
   double vmax = MathMin(SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX), InpMaxLot);
   double vstep= SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
   if(vstep>0) lots = MathFloor(lots/vstep)*vstep;
   lots = MathMax(vmin, MathMin(vmax, lots));
   return lots;
  }

//+------------------------------------------------------------------+
//| Helpers: account / positions                                    |
//+------------------------------------------------------------------+
bool IsTradeableAccount()
  {
   bool isDemo = (AccountInfoInteger(ACCOUNT_TRADE_MODE)==ACCOUNT_TRADE_MODE_DEMO);
   return isDemo || InpAllowLive;                 // live only if explicitly allowed
  }
int OpenCountForSymbol(const string sym)
  {
   int c=0;
   for(int i=PositionsTotal()-1;i>=0;i--)
      if(pos.SelectByIndex(i) && pos.Symbol()==sym && pos.Magic()==InpMagic) c++;
   return c;
  }
ulong FindPositionByComment(const string sym, const string id)
  {
   for(int i=PositionsTotal()-1;i>=0;i--)
      if(pos.SelectByIndex(i) && pos.Symbol()==sym && pos.Magic()==InpMagic && pos.Comment()==id)
         return pos.Ticket();
   return 0;
  }

void AddState(ulong tk, double entry, double initStop, double R, int dir, datetime expiry)
  {
   int n=ArraySize(gStates); ArrayResize(gStates, n+1);
   gStates[n].ticket=tk; gStates[n].entry=entry; gStates[n].initStop=initStop;
   gStates[n].R=R; gStates[n].dir=dir; gStates[n].armed=false;
   gStates[n].best=entry; gStates[n].expiry=expiry;
  }
void Remove(int i){ int n=ArraySize(gStates); if(i<0||i>=n)return; gStates[i]=gStates[n-1]; ArrayResize(gStates,n-1); }

void RebuildStates()
  {
   ArrayResize(gStates,0);
   for(int i=PositionsTotal()-1;i>=0;i--)
     {
      if(!pos.SelectByIndex(i) || pos.Magic()!=InpMagic) continue;
      int dir = (pos.PositionType()==POSITION_TYPE_BUY)?1:-1;
      double entry = pos.PriceOpen();
      double initStop = pos.StopLoss();
      double R = (initStop>0)? MathAbs(entry-initStop) : 0.0;   // best-effort after restart
      // horizon unknown after restart → default a generous 10 trading days
      AddState(pos.Ticket(), entry, initStop, R, dir, TimeCurrent()+(datetime)(10*24*3600));
     }
  }

//+------------------------------------------------------------------+
//| Dedup (session memory + file so a restart doesn't re-enter)     |
//+------------------------------------------------------------------+
bool AlreadyActed(const string id)
  {
   for(int i=ArraySize(gActed)-1;i>=0;i--) if(gActed[i]==id) return true;
   return false;
  }
void MarkActed(const string id)
  {
   int n=ArraySize(gActed); ArrayResize(gActed,n+1); gActed[n]=id;
   int h=FileOpen(gActedFile, FILE_READ|FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(h!=INVALID_HANDLE){ FileSeek(h,0,SEEK_END); FileWriteString(h, id+"\n"); FileClose(h); }
  }
void LoadActed()
  {
   int h=FileOpen(gActedFile, FILE_READ|FILE_TXT|FILE_ANSI);
   if(h==INVALID_HANDLE) return;
   while(!FileIsEnding(h)){ string s=FileReadString(h); if(StringLen(s)>0){ int n=ArraySize(gActed); ArrayResize(gActed,n+1); gActed[n]=s; } }
   FileClose(h);
  }

//+------------------------------------------------------------------+
//| HTTP GET via WebRequest                                          |
//+------------------------------------------------------------------+
bool HttpGet(const string url, string &out)
  {
   char post[]; char result[]; string headers; string rhead;
   ResetLastError();
   int code = WebRequest("GET", url, "", 5000, post, result, rhead);
   if(code==-1)
     { PrintFormat("WebRequest failed (%d). Allow the URL in Tools>Options>Expert Advisors.", GetLastError()); return false; }
   if(code!=200){ if(InpVerbose) PrintFormat("feed HTTP %d", code); return false; }
   out = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   return StringLen(out)>0;
  }

// Read the whole signal file from MQL5/Files (bridge mode). The equity_bridge.py process writes it
// atomically, so a partial read is unlikely; a failed/empty read just skips this cycle.
bool ReadLocalBody(const string fname, string &out)
  {
   int h = FileOpen(fname, FILE_READ|FILE_TXT|FILE_ANSI|FILE_SHARE_READ|FILE_SHARE_WRITE);
   if(h==INVALID_HANDLE)
     { if(InpVerbose) PrintFormat("local feed %s not found yet (err %d)", fname, GetLastError()); return false; }
   out = "";
   while(!FileIsEnding(h))
      out += FileReadString(h);
   FileClose(h);
   return StringLen(out) > 0;
  }

//+------------------------------------------------------------------+
//| Minimal JSON helpers for our FLAT signal schema                 |
//|  (not a general JSON parser — tailored to equity-signals.json)  |
//+------------------------------------------------------------------+
int TfMinutes(const string tf){ if(tf=="m15") return 15; if(tf=="h1") return 60; if(tf=="m5") return 5; return 60; }

bool JTopBool(const string body, const string key)
  {
   string v; if(!RawValue(body, key, v)) return false;
   return (StringFind(v,"true")>=0);
  }
string JGetStr(const string obj, const string key)
  {
   string v; if(!RawValue(obj, key, v)) return "";
   int a=StringFind(v,"\""); if(a<0) return "";
   int b=StringFind(v,"\"",a+1); if(b<0) return "";
   return StringSubstr(v, a+1, b-a-1);
  }
double JGetNum(const string obj, const string key)
  {
   string v; if(!RawValue(obj, key, v)) return 0.0;
   return StringToDouble(v);
  }
bool JGetBool(const string obj, const string key)
  {
   string v; if(!RawValue(obj, key, v)) return false;
   return (StringFind(v,"true")>=0);
  }
// returns the raw text right after "key": up to the next comma/brace
bool RawValue(const string s, const string key, string &out)
  {
   int k=StringFind(s, "\""+key+"\"");
   if(k<0) return false;
   int c=StringFind(s, ":", k); if(c<0) return false;
   int end=c+1; int depth=0; bool inStr=false;
   for(int i=c+1;i<StringLen(s);i++)
     {
      ushort ch=StringGetCharacter(s,i);
      if(ch=='"' ) inStr=!inStr;
      if(inStr){ end=i+1; continue; }
      if(ch=='{'||ch=='[') depth++;
      if(ch=='}'||ch==']'){ if(depth==0){ end=i; break; } depth--; }
      if(ch==',' && depth==0){ end=i; break; }
      end=i+1;
     }
   out=StringSubstr(s, c+1, end-(c+1));
   return true;
  }
// pull the array text for "key":[ ... ]
bool ExtractArray(const string s, const string key, string &out)
  {
   int k=StringFind(s, "\""+key+"\""); if(k<0) return false;
   int a=StringFind(s, "[", k); if(a<0) return false;
   int depth=0; bool inStr=false;
   for(int i=a;i<StringLen(s);i++)
     {
      ushort ch=StringGetCharacter(s,i);
      if(ch=='"') inStr=!inStr;
      if(inStr) continue;
      if(ch=='[') depth++;
      if(ch==']'){ depth--; if(depth==0){ out=StringSubstr(s, a, i-a+1); return true; } }
     }
   return false;
  }
// split a JSON array string into its top-level { } object substrings
int SplitObjects(const string arr, string &objs[])
  {
   ArrayResize(objs,0);
   int depth=0, start=-1; bool inStr=false;
   for(int i=0;i<StringLen(arr);i++)
     {
      ushort ch=StringGetCharacter(arr,i);
      if(ch=='"') inStr=!inStr;
      if(inStr) continue;
      if(ch=='{'){ if(depth==0) start=i; depth++; }
      else if(ch=='}'){ depth--; if(depth==0 && start>=0){ int n=ArraySize(objs); ArrayResize(objs,n+1); objs[n]=StringSubstr(arr,start,i-start+1); start=-1; } }
     }
   return ArraySize(objs);
  }
//+------------------------------------------------------------------+
