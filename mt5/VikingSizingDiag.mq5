//+------------------------------------------------------------------+
//| VikingSizingDiag — READ-ONLY US position-sizing throttle report   |
//|                                                                  |
//| The equity book's US names realise ~0.035% risk/trade vs a 0.5%  |
//| target. LotsForRisk clamps size to SYMBOL_VOLUME_MAX (the broker |
//| per-ORDER volume cap, "brokerMax"=1000), which binds well below  |
//| the risk target on mid/low-priced names.                          |
//|                                                                  |
//| THE decision this script answers: can we lift it by SPLITTING    |
//| into multiple orders? That only works if the broker's per-SYMBOL |
//| aggregate cap (SYMBOL_VOLUME_LIMIT) is HIGHER than the per-order  |
//| cap. If VOLUME_LIMIT == VOLUME_MAX, the broker hard-caps total    |
//| exposure/symbol and no EA change can exceed it.                   |
//|                                                                  |
//| Places NO orders. Run it on any chart; read the Experts log.     |
//| For each US name it prints the broker caps + what 0.5% needs +    |
//| a verdict (OK / SPLIT:n / CAPPED).                                |
//+------------------------------------------------------------------+
#property script_show_inputs
#property strict

input string InpSymbols = "AAPL.NAS,MSFT.NAS,NVDA.NAS,TSLA.NAS,AMZN.NAS,PEP.NAS,PYPL.NAS,HOOD.NAS,COIN.NAS,PLTR.NAS,AXP.NYSE,BA.NYSE,BAC.NYSE,BLK.NYSE,JPM.NYSE,LLY.NYSE,MA.NYSE,MCD.NYSE,ACN.NYSE,UBER.NYSE,ICE.NYSE,BX.NYSE,KKR.NYSE";
input double InpRiskPct      = 0.5;     // the risk target we're trying to hit
input double InpNotionalPctUS= 100.0;   // US notional cap (matches EA InpMaxNotionalPctUS)
input double InpRefStopPct   = 1.0;     // reference stop as % of price (typical .EQ structural stop)

double CeilDiv(double a, double b){ return (b>0)? MathCeil(a/b) : 0; }

void OnStart()
  {
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   PrintFormat("=== Viking US sizing diagnostic === equity=%.0f %s, riskTarget=%.2f%% (=%.0f), refStop=%.1f%% of price",
               equity, AccountInfoString(ACCOUNT_CURRENCY), InpRiskPct, equity*InpRiskPct/100.0, InpRefStopPct);
   Print("sym         price     vMin   vStep   vMax(ord)  vLimit(sym)  need0.5%   1-order%   split->   verdict");

   string syms[]; int n = StringSplit(InpSymbols, ',', syms);
   int okSingle=0, okSplit=0, capped=0;
   for(int i=0;i<n;i++)
     {
      string sym=syms[i]; StringTrimLeft(sym); StringTrimRight(sym);
      if(sym=="") continue;
      if(!SymbolSelect(sym,true)){ PrintFormat("%-11s  not found — skipped", sym); continue; }
      double ask = SymbolInfoDouble(sym, SYMBOL_ASK);
      double bid = SymbolInfoDouble(sym, SYMBOL_BID);
      double px  = (ask>0? ask : bid);
      double tickVal = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
      double tickSize= SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE);
      double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN);
      double vstep= SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
      double vmax = SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX);     // per-ORDER cap
      double vlim = SymbolInfoDouble(sym, SYMBOL_VOLUME_LIMIT);   // per-SYMBOL aggregate cap (0 = none)
      if(px<=0 || tickVal<=0 || tickSize<=0){ PrintFormat("%-11s  no price/tick data — skipped", sym); continue; }

      double stopDist   = px * InpRefStopPct/100.0;
      double lossPerUnit= (stopDist/tickSize)*tickVal;            // acct-ccy loss per 1 unit at the ref stop
      double riskMoney  = equity * InpRiskPct/100.0;
      double needUnits  = (lossPerUnit>0)? riskMoney/lossPerUnit : 0;      // units wanted for 0.5%
      double notionalPerUnit = px*tickVal/tickSize;
      double notCapUnits= (notionalPerUnit>0)? equity*(InpNotionalPctUS/100.0)/notionalPerUnit : 0;
      double targetUnits= MathMin(needUnits, notCapUnits);        // what we'd size to pre-broker-cap

      double oneOrder   = MathMin(targetUnits, vmax);
      double oneRiskPct = oneOrder*lossPerUnit/equity*100.0;

      // max the broker allows in aggregate (split across orders)
      double aggCap     = (vlim>0)? MathMin(vlim, notCapUnits) : notCapUnits;
      double achievable = MathMin(targetUnits, aggCap);
      double splitRiskPct=achievable*lossPerUnit/equity*100.0;
      int    orders     = (int)CeilDiv(achievable, vmax);

      string verdict;
      if(oneOrder >= targetUnits-vstep)              { verdict="OK (1 order hits target)"; okSingle++; }
      else if(achievable >= targetUnits-vstep)       { verdict=StringFormat("SPLIT: %d orders -> target", orders); okSplit++; }
      else                                           { verdict=StringFormat("CAPPED @ %.2f%% (vLimit binds)", splitRiskPct); capped++; }

      PrintFormat("%-11s %8.2f %7.0f %7.2f %9.0f %11.0f %9.0f %9.2f%% %8d %s",
                  sym, px, vmin, vstep, vmax, vlim, needUnits, oneRiskPct, orders, verdict);
     }
   PrintFormat("--- summary: %d names hit target in 1 order, %d need SPLITTING, %d HARD-CAPPED by vLimit ---",
               okSingle, okSplit, capped);
   Print("If most rows show vLimit=0 (or >> vMax) and 'SPLIT: n', order-splitting in the EA lifts US to the 0.5% target.");
   Print("If rows show CAPPED (vLimit==vMax), the broker caps exposure/symbol — ask the broker to raise it, or accept it.");
  }
//+------------------------------------------------------------------+
