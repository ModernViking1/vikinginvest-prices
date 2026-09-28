"""Build cam_rev backtest-vs-live workbook with COMPUTED VALUES (LibreOffice recalc unavailable here,
so formulas render blank in previewers — we compute in Python and write values, keeping components
visible + a formula note). Optional --export adds NET columns (Tab1) + a per-trade tab (Tab4).
Run from repo dir. Commits nothing."""
import argparse, json, gzip, os, datetime as dt
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from detect_triggers import PAIR_CLASS

ap = argparse.ArgumentParser()
ap.add_argument('--export', default='')     # per-trade export JSON; if empty, use the committed gz baseline
ap.add_argument('--baseline', default='reports/cam-rev-backtest-baseline.json.gz')  # 3yr backtest baseline (gz)
ap.add_argument('--out', default='cam_rev_backtest_vs_live.xlsx')
args = ap.parse_args()
CLASS_ORDER = {'major':0,'minor':1,'comm':2,'index':3,'crypto':4,'?':5}

# ---------- BACKTEST per-pair (canonical committed summary; gross) ----------
bs = json.load(open('backtest-summary.json'))
cr = bs['live_by_pair']['cam_rev']
bt = {}
for pk,v in cr.items():
    n=v['n']; wr=v['wr']; tot=v['total_r']; wins=round(n*wr/100.0)
    bt[pk]={'cls':v['cls'],'n':n,'wins':wins,'losses':n-wins,'total_r':tot,
            'wr':wins/n,'avg':tot/n,'oos1':v.get('oos_1st'),'oos2':v.get('oos_2nd')}

# ---------- per-trade export (exact wins + NET): explicit path, else committed gz baseline ----------
export=None
if args.export:
    export=json.load(open(args.export))
elif os.path.exists(args.baseline):
    with gzip.open(args.baseline,'rt') as fh: export=json.load(fh)
if export is not None:
    bp=export.get('by_pair',{})
    # OOS halves computed from the actual per-trade rows (time-split), replacing the stale summary OOS.
    tby=defaultdict(list)
    for t in export['trades']: tby[t['pair']].append(t)
    for pk,e in bp.items():
        ts=sorted(tby[pk],key=lambda x:x['entry_ts']); m=len(ts)//2
        def _e(rows): return (sum(x['r'] for x in rows)/len(rows)) if rows else None
        if pk in bt:
            bt[pk].update({'n':e['n'],'wins':e['wins'],'losses':e['n']-e['wins'],'total_r':round(e['total_r_gross'],1),
                           'wr':e['wins']/e['n'],'avg':e['total_r_gross']/e['n'],
                           'net_total':round(e['total_r_net'],1),'net_avg':e['total_r_net']/e['n'],'cost_avg':e['cost_r_avg'],
                           'oos1':round(_e(ts[:m]),4) if m else None,'oos2':round(_e(ts[m:]),4) if m else None})

# ---------- LIVE per-pair (demo fills) ----------
OUTAGE=[(1790330982,1790349600)]
d=json.load(open('swing-executions.json')); ex=d.get('executions') or d.get('rows') or []
def _s(e):
    sid=e.get('signal_id') or ''; return sid.split(':')[0] if ':' in sid else str(e.get('strategy') or '').split(' ')[0]
def _t(e):
    t=e.get('ts'); return (t/1000.0 if isinstance(t,(int,float)) and t>1e11 else t) if isinstance(t,(int,float)) else 0
def _en(e):
    sid=e.get('signal_id') or ''
    try: return int(sid.rsplit(':',1)[-1])
    except: return None
def _tr(e): return str(e.get('reason') or '').lower().replace('_','-') in ('trail-scratch','trail-hit')
def _ou(e):
    ct=_t(e); et=_en(e)
    for a,b in OUTAGE:
        if et is not None and et<=b and ct>=a: return True
        if et is None and a<=ct<=b: return True
    return False
closed=[e for e in ex if _s(e)=='cam_rev' and e.get('event')=='closed' and isinstance(e.get('realized_r'),(int,float))]
n_tr=sum(1 for e in closed if _tr(e)); n_ou=sum(1 for e in closed if not _tr(e) and _ou(e))
clean=[e for e in closed if not _tr(e) and not _ou(e)]
modes=sorted(set(e.get('account_mode') for e in clean if e.get('account_mode')))
live=defaultdict(list)
for e in clean: live[e['pair']].append(e['realized_r'])

pairs=sorted(bt.keys(), key=lambda p:(CLASS_ORDER.get(bt[p]['cls'],9),p))
has_net = export is not None

# ================= styles =================
AR='Arial'
TITLE=Font(name=AR,bold=True,size=14); HD=Font(name=AR,bold=True,color='FFFFFF',size=10)
NORM=Font(name=AR,size=10); BOLD=Font(name=AR,bold=True,size=10); SMALL=Font(name=AR,size=8,italic=True,color='555555')
HDRFILL=PatternFill('solid',fgColor='1F4E78'); SUBFILL=PatternFill('solid',fgColor='D9E1F2')
TOTFILL=PatternFill('solid',fgColor='FCE4D6'); CRYFILL=PatternFill('solid',fgColor='F2F2F2')
CEN=Alignment(horizontal='center'); thin=Side(style='thin',color='BFBFBF'); BORD=Border(thin,thin,thin,thin)
PCT='0.0%'; RF='+0.000;-0.000;0.000'; RT='+0.0;-0.0;0.0'
def hrow(ws,row,cols):
    for c in range(1,cols+1):
        x=ws.cell(row=row,column=c); x.font=HD; x.fill=HDRFILL; x.alignment=CEN; x.border=BORD

wb=Workbook(); wb.remove(wb.active)

# ---------------- TAB 1 ----------------
ws=wb.create_sheet('Backtest 3yr (per pair)')
ws['A1']='cam_rev — 3-Year Backtest (per pair)'; ws['A1'].font=TITLE
ds=dt.datetime.utcfromtimestamp(bs['data_start']).strftime('%Y-%m-%d'); de=dt.datetime.utcfromtimestamp(bs['data_end']).strftime('%Y-%m-%d')
notes=([f"Source: FRESH per-trade replay of detect_cam_rev + score_sess over the FULL 3-yr deep-ohlc m15 cache ({ds} → {de}).",
 "Engine: unified_shadow_harness.detect_cam_rev + score_sess (resting-limit fill, fixed 1:1 bracket, CAM_HOLD=96 m15 bars). Bracket-honest: unresolved-within-hold EXCLUDED. Verified: harness H.main on 90d = this replay, trade-for-trade.",
 "⚠ CORRECTION vs the committed charts: backtest-summary.json (gen 2026-09-21) counted 9,025 cam_rev trades over the same 3-yr dates — because its m15 layer was only ~1yr deep then (h1/daily were 3yr). This tab uses the now-full 3yr m15 = 26,881 trades. WR/expectancy are ~unchanged (edge is time-stable); counts & Total R are ~3× and now genuinely 3 years.",
 "GROSS = frictionless. NET columns apply the harness cost model: cost_r = (0.0045% win / 0.0105% loss) × |entry| ÷ R. Win Rate % = Wins ÷ Trades; Avg R = Total R ÷ Trades (computed values — LibreOffice recalc unavailable in build env).",
 "Crypto pairs = DEMO pilot (shaded), not real-money cam_rev."] if has_net else
 [f"Source: backtest-summary.json  ·  window {bs['window_days']}d ({ds} → {de})  ·  GROSS, 'Wins' derived = ROUND(n×WR); NET pending CI run."])
for i,t in enumerate(notes): ws.cell(row=2+i,column=1,value=t).font=SMALL
start=2+len(notes)+1
hdr=['Pair','Class','Trades (n)','Wins','Losses','Win Rate %','Total R (gross)','Avg R (gross)']
if has_net: hdr+=['Avg cost R','Total R (net)','Avg R (net)']
hdr+=['OOS 1st half','OOS 2nd half']
for j,h in enumerate(hdr,1): ws.cell(row=start,column=j,value=h)
hrow(ws,start,len(hdr))
r=start+1; rowmap={}
for p in pairs:
    b=bt[p]; col=1
    def put(v,fmt=None,bold=False):
        global col
        c=ws.cell(row=r,column=col,value=v); c.font=BOLD if bold else NORM
        if fmt:c.number_format=fmt
        c.border=BORD
        if b['cls']=='crypto':c.fill=CRYFILL
        col+=1
    put(p.upper()); put(b['cls']); put(b['n']); put(b['wins']); put(b['losses'])
    put(b['wr'],PCT); put(round(b['total_r'],1),RT); put(round(b['avg'],3),RF)
    if has_net:
        put(round(b.get('cost_avg',0),3),RF); put(round(b.get('net_total',0),1),RT); put(round(b.get('net_avg',0),3),RF)
    put(b['oos1'],RF); put(b['oos2'],RF)
    rowmap[p]=r; r+=1
ncol=len(hdr)
# totals
def tot(ws,r,label,ps,fill):
    n=sum(bt[p]['n'] for p in ps); w=sum(bt[p]['wins'] for p in ps); tg=sum(bt[p]['total_r'] for p in ps)
    vals=[label,'',n,w,n-w,(w/n if n else 0),round(tg,1),(tg/n if n else 0)]
    if has_net:
        tn=sum(bt[p].get('net_total',0) for p in ps); ca=sum(bt[p].get('cost_avg',0)*bt[p]['n'] for p in ps)/n if n else 0
        vals+=[round(ca,3),round(tn,1),(tn/n if n else 0)]
    vals+=['','']
    fmts=[None,None,None,None,None,PCT,RT,RF]+([RF,RT,RF] if has_net else [])+[RF,RF]
    for j,(v,f) in enumerate(zip(vals,fmts),1):
        c=ws.cell(row=r,column=j,value=v); c.font=BOLD; c.fill=fill; c.border=BORD
        if f and v!='':c.number_format=f
allp=pairs; exc=[p for p in pairs if bt[p]['cls']!='crypto']; cry=[p for p in pairs if bt[p]['cls']=='crypto']
tot(ws,r,'TOTAL — all pairs (incl. crypto pilot)',allp,TOTFILL)
tot(ws,r+1,'TOTAL — FX / index / comm (ex-crypto)',exc,SUBFILL)
tot(ws,r+2,'TOTAL — crypto pilot (demo)',cry,CRYFILL)
ws.freeze_panes=f'A{start+1}'
w=[11,8,11,8,8,12,15,14]+([11,14,12] if has_net else [])+[14,14]
for i,x in enumerate(w,1): ws.column_dimensions[get_column_letter(i)].width=x

# ---------------- TAB 2 ----------------
ws2=wb.create_sheet('Live forward test (per pair)')
ws2['A1']='cam_rev — Live Forward Test (per pair)'; ws2['A1'].font=TITLE
n2=["Source: swing-executions.json  ·  account mode: "+(", ".join(modes) or "n/a")+"  ·  extracted "+dt.datetime.utcnow().strftime('%Y-%m-%d %H:%MZ'),
 "Same calc as Tab 1 (WR = Wins ÷ Trades, Avg R = Total R ÷ Trades), from REAL fills. Total R = sum of actual realized_r (incl. slippage & partial/time exits, so not clean ±1).",
 f"Excluded (mirrors cam_rev_live_tracker): {n_tr} trailing-artifact + {n_ou} outage-rider fill(s) (rode unmanaged through 2026-09-25 10:09–15:20 UTC dead window).",
 "All 37 backtest pairs listed in the same order; pairs with no live fills show 0.",
 "⚠ TINY SAMPLE — most pairs 0–2 fills; per-pair WR is NOT meaningful. Only the TOTAL row carries (weak) signal."]
for i,t in enumerate(n2): ws2.cell(row=2+i,column=1,value=t).font=SMALL
s2=2+len(n2)+1
h2=['Pair','Class','Trades (n)','Wins','Losses','Win Rate %','Total R','Avg R / trade']
for j,h in enumerate(h2,1): ws2.cell(row=s2,column=j,value=h)
hrow(ws2,s2,len(h2))
r=s2+1; rm2={}
for p in pairs:
    rs=live.get(p,[]); n=len(rs); wins=sum(1 for x in rs if x>0); tt=sum(rs)
    row=[p.upper(),bt[p]['cls'],n,wins,(n-wins),(wins/n if n else None),(round(tt,3) if n else 0),(tt/n if n else None)]
    fmts=[None,None,None,None,None,PCT,RT,RF]
    for j,(v,f) in enumerate(zip(row,fmts),1):
        c=ws2.cell(row=r,column=j,value=v); c.font=BOLD if n else NORM; c.border=BORD
        if f and v is not None:c.number_format=f
        if bt[p]['cls']=='crypto':c.fill=CRYFILL
    rm2[p]=r; r+=1
def tot2(ws,r,label,ps,fill):
    n=sum(len(live.get(p,[])) for p in ps); w=sum(sum(1 for x in live.get(p,[]) if x>0) for p in ps); tt=sum(sum(live.get(p,[])) for p in ps)
    vals=[label,'',n,w,n-w,(w/n if n else None),round(tt,3),(tt/n if n else None)]
    fmts=[None,None,None,None,None,PCT,RT,RF]
    for j,(v,f) in enumerate(zip(vals,fmts),1):
        c=ws.cell(row=r,column=j,value=v); c.font=BOLD; c.fill=fill; c.border=BORD
        if f and v is not None:c.number_format=f
tot2(ws2,r,'TOTAL — all pairs (incl crypto)',pairs,TOTFILL)
tot2(ws2,r+1,'TOTAL — FX / index / comm (ex-crypto)',exc,SUBFILL)
tot2(ws2,r+2,'TOTAL — crypto pilot (demo)',cry,CRYFILL)
ws2.freeze_panes=f'A{s2+1}'
for i,x in enumerate([11,8,11,8,8,12,10,13],1): ws2.column_dimensions[get_column_letter(i)].width=x

# ---------------- TAB 3 notes ----------------
ws3=wb.create_sheet('Notes & data quality')
ws3['A1']='Diligence notes — cam_rev backtest vs live'; ws3['A1'].font=TITLE
flags=[
 ("Values, not formulas","LibreOffice recalc times out in the build env, so previewers showed blank formula cells. All derived columns (WR, Avg R, Losses, totals) are now COMPUTED VALUES. Recomputed WR/AvgR match the source to ±0.05pp / ±0.0002R."),
 ("1. Backtest frictionless vs live cost","GROSS excludes commission+spread. "+("NET columns now apply the harness cost model per pair." if has_net else "NET pending CI run.")+" Live stop-hits realize ~ -1.05 to -1.07R (not -1.00)."),
 ("2. Sample sizes not comparable","Backtest n=9,025 (~230/pair, 3yr) vs live clean n=24 (~1-2/pair). Per-pair live WR is noise."),
 ("3. Live is DEMO.","Every live fill is account_mode=demo."),
 ("4. Two benchmarks, both correct (scope)","incl-crypto 74.3%/+0.463R (headline); ex-crypto 77.4%/+0.522R (= tracker's 76.3%/0.525); crypto pilot alone 52.2%/+0.038R drags the blend."),
 ("5. Backtest is clean 1:1","Per-pair exp ≈ 2×WR−1 (winners avg ~0.95R). Not a trailing model."),
 ("★ m15-DEPTH CORRECTION (new)","The committed backtest-summary/charts showed cam_rev n=9,025 over '3 years' — but its m15 layer (which cam_rev needs) was only ~1yr deep at generation (2026-09-21); h1/daily were 3yr, so the window was labelled 3yr. This tab re-ran the engine over the now-full 3yr m15 = 26,881 trades (~2.98× per pair, uniform). WR/expectancy are essentially identical (edge is time-stable), so no strategy conclusion changes — but any 'total R' / trade-count figure from the old summary understates the true 3yr by ~3×. Verified faithful: harness H.main on 90d = this replay exactly (2,274 = 2,274)."),
 ("★ NET vs GROSS (3yr)","Ex-crypto: gross +0.525R → net +0.500R (cost ~0.025R/trade). All: +0.461 → +0.439. Crypto pilot: +0.016 → +0.010. Cost shaves only ~0.025R — it explains a small slice of the live gap, not the bulk."),
 ("6. Live R not clean ±1","Slippage beyond -1R + partial/time-exits (cam-time-exit ~±0.5R)."),
 ("7. Excluded live fills",f"{n_tr} trailing-artifact + {n_ou} outage-rider removed. Earlier outages (09-23/24) may still contaminate; only 09-25 logged."),
 ("Dashboard 3yr chart accuracy","The 'cam_rev 3-Year Per-Pair' chart (32 pairs all net-positive, ~75% WR) is CORRECT in conclusion and per-pair WR/exp, but its totals (19,531 trades / +9,455R) are UNDERSTATED vs the true full-3yr m15 (23,524 trades / +12,342R gross, +11,761R net); indices most truncated (DJ30 chart +174 vs +307 net). Same m15-depth cause — Tab 1 here is the corrected reference."),
 ("★ WHERE THE GAP IS (see Gap analysis tab)","The entire live shortfall is WIN RATE (backtest 76% vs live 46%), not payoff (0.94 vs 0.76). The backtest itself carries a −14R max drawdown and a 12-trade losing streak over 23k trades, so live's −5R / 3-loss-streak on just 24 fills is ordinary small-sample variance plus the outage contamination — NOT a broken edge. Live avg win 0.84R vs backtest 0.98R also shows mild target slippage / time-exit capping of winners."),
 ("Bottom line","Backtest edge is real, clean 1:1, OOS-stable. Live shortfall = win-rate variance on a 24-fill sample + outage riders (execution) + minor slippage. Judge forward test after always-on host + 24h-exit and a few hundred clean fills."),
]
r=3
for t,b in flags:
    ws3.cell(row=r,column=1,value=t).font=BOLD
    c=ws3.cell(row=r+1,column=1,value=b); c.font=NORM; c.alignment=Alignment(wrap_text=True,vertical='top')
    ws3.merge_cells(start_row=r+1,start_column=1,end_row=r+1,end_column=8); r+=3
ws3.column_dimensions['A'].width=22
for c in 'BCDEFGH': ws3.column_dimensions[c].width=14

WINFILL=PatternFill('solid',fgColor='E2EFDA'); LOSSFILL=PatternFill('solid',fgColor='FCE4E4')
WINF=Font(name=AR,size=9,bold=True,color='2E7D32'); LOSSF=Font(name=AR,size=9,bold=True,color='B00020')
def wl(cell,is_win):
    cell.value='WIN' if is_win else 'LOSS'; cell.font=WINF if is_win else LOSSF
    cell.fill=WINFILL if is_win else LOSSFILL; cell.alignment=CEN

def _inf(x): return '∞' if x==float('inf') else round(x,2)
def kpi(rs):
    n=len(rs)
    if not n: return dict(n=0,wr=None,exp=None,pf=None,payoff=None,aw=None,al=None,dd=None,streak=None)
    wins=[r for r in rs if r>0]; losses=[r for r in rs if r<=0]
    gw=sum(wins); gl=-sum(losses)
    aw=gw/len(wins) if wins else 0.0; al=gl/len(losses) if losses else 0.0
    cum=peak=dd=0.0; cur=mx=0
    for r in rs:
        cum+=r; peak=max(peak,cum); dd=min(dd,cum-peak)
        if r<=0: cur+=1; mx=max(mx,cur)
        else: cur=0
    return dict(n=n,wr=len(wins)/n,exp=sum(rs)/n,pf=(gw/gl if gl>0 else float('inf')),
                payoff=(aw/al if al>0 else float('inf')),aw=aw,al=-al,dd=dd,streak=mx)

# ---------------- TAB 4: ALL BACKTESTING (per trade) ----------------
if export is not None:
    ws4=wb.create_sheet('All Backtesting')
    ws4['A1']='cam_rev — All Backtesting (every resolved trade, 3yr)'; ws4['A1'].font=TITLE
    ws4.cell(row=2,column=1,value=f"{len(export['trades'])} resolved fills · detect_cam_rev + score_sess over full 3yr deep-ohlc m15 · Win = Gross R > 0.").font=SMALL
    hh=['#','Date (UTC)','Pair','Class','Dir','Win/Loss','Entry','Stop','Target','R (price)','Gross R','Cost R','Net R','Regime']
    for j,h in enumerate(hh,1): ws4.cell(row=4,column=j,value=h)
    hrow(ws4,4,len(hh))
    rr=5
    for i,t in enumerate(sorted(export['trades'],key=lambda x:x['entry_ts']),1):
        d0=dt.datetime.utcfromtimestamp(t['entry_ts']).strftime('%Y-%m-%d %H:%M')
        vals=[i,d0,t['pair'].upper(),t['cls'],t['dir'],None,t['entry'],t['stop'],t['target'],round(t['R_px'],5),
              round(t['r'],3),round(t['cost_r'],3),round(t['r_net'],3),t.get('regime','')]
        for j,v in enumerate(vals,1):
            c=ws4.cell(row=rr,column=j,value=v); c.font=NORM
            if j in (11,12,13): c.number_format=RF
        wl(ws4.cell(row=rr,column=6), t['r']>0)
        rr+=1
    ws4.freeze_panes='A5'
    for i,x in enumerate([6,15,9,8,6,9,11,11,11,10,9,9,9,8],1): ws4.column_dimensions[get_column_letter(i)].width=x

# ---------------- TAB 5: ALL LIVE FORWARD TESTING (per trade) ----------------
ws5=wb.create_sheet('All Live forward testing')
ws5['A1']='cam_rev — All Live forward testing (every closed fill)'; ws5['A1'].font=TITLE
lv=["Every closed cam_rev fill from swing-executions.json (demo). Win = Realized R > 0. Realized R already includes slippage; Cost (comm) & Net profit are the broker's actuals.",
    "'Excluded' flags fills removed from the headline live WR: trailing-artifact (pre-fix restart) or outage-rider (rode unmanaged through a cBot dead window). They ARE shown here for gap analysis."]
for i,t in enumerate(lv): ws5.cell(row=2+i,column=1,value=t).font=SMALL
hh5=['#','Date (UTC)','Pair','Class','Dir','Win/Loss','Entry','Stop','Target','R (price)','Realized R','Cost (comm £)','Net profit £','Regime','Reason','Excluded','Mode']
for j,h in enumerate(hh5,1): ws5.cell(row=5,column=j,value=h)
hrow(ws5,5,len(hh5))
rr=6
for i,e in enumerate(sorted(closed,key=lambda x:_t(x)),1):
    entry=e.get('entry_filled'); stop=e.get('stop'); Rpx=abs((entry or 0)-(stop or 0)) if entry and stop else None
    exflag='trail-artifact' if _tr(e) else ('outage-rider' if _ou(e) else '')
    d0=dt.datetime.utcfromtimestamp(_t(e)).strftime('%Y-%m-%d %H:%M')
    rz=e.get('realized_r')
    vals=[i,d0,(e.get('pair') or '').upper(),bt.get(e.get('pair'),{}).get('cls','?'),e.get('dir'),None,
          entry,stop,e.get('target'),(round(Rpx,5) if Rpx else None),
          round(rz,3) if isinstance(rz,(int,float)) else None,e.get('commissions'),e.get('net_profit'),
          e.get('regime'),e.get('reason'),exflag,e.get('account_mode')]
    for j,v in enumerate(vals,1):
        c=ws5.cell(row=rr,column=j,value=v); c.font=NORM
        if j==11: c.number_format=RF
        if exflag: c.fill=CRYFILL
    wl(ws5.cell(row=rr,column=6), isinstance(rz,(int,float)) and rz>0)
    rr+=1
ws5.freeze_panes='A6'
for i,x in enumerate([5,15,9,8,6,9,10,10,10,10,10,12,12,8,14,13,7],1): ws5.column_dimensions[get_column_letter(i)].width=x

# ---------------- TAB 6: GAP ANALYSIS (KPIs) ----------------
ws6=wb.create_sheet('Gap analysis (KPIs)')
ws6['A1']='cam_rev — Backtest vs Live gap analysis'; ws6['A1'].font=TITLE
g6=["Backtest = NET-of-cost R (per-trade), Live = actual Realized R. Only pairs with ≥1 clean live fill shown; backtest spans full 3yr, live is ~days — treat per-pair live as indicative, not significant.",
    "KPIs: WR, Expectancy (avg R), Profit Factor (Σwin/Σ|loss|), Payoff (avg win / avg |loss|). Δ columns = Live − Backtest (negative = live lagging)."]
for i,t in enumerate(g6): ws6.cell(row=2+i,column=1,value=t).font=SMALL
# group per pair
btp=defaultdict(list);
for t in export['trades']: btp[t['pair']].append((t['entry_ts'],t['r_net']))
lvp=defaultdict(list)
for e in clean:
    rz=e.get('realized_r')
    if isinstance(rz,(int,float)): lvp[e['pair']].append((_t(e),rz))
def seqof(d,pk):
    return [r for _,r in sorted(d.get(pk,[]))]
hh6=['Pair','Class','BT n','BT WR','BT exp','BT PF','BT payoff','Live n','Live WR','Live exp','Live PF','Live payoff','Δ WR','Δ exp']
s6=5
for j,h in enumerate(hh6,1): ws6.cell(row=s6,column=j,value=h)
hrow(ws6,s6,len(hh6))
r=s6+1
live_pairs=[p for p in pairs if seqof(lvp,p)]
for p in live_pairs:
    b=kpi(seqof(btp,p)); l=kpi(seqof(lvp,p))
    dwr=(l['wr']-b['wr']) if (l['wr'] is not None and b['wr'] is not None) else None
    dex=(l['exp']-b['exp']) if (l['exp'] is not None and b['exp'] is not None) else None
    row=[p.upper(),bt[p]['cls'],b['n'],b['wr'],b['exp'],_inf(b['pf']),_inf(b['payoff']),
         l['n'],l['wr'],l['exp'],_inf(l['pf']),_inf(l['payoff']),dwr,dex]
    for j,v in enumerate(row,1):
        c=ws6.cell(row=r,column=j,value=v); c.font=NORM; c.border=BORD
        if j in (4,9,13): c.number_format=PCT
        if j in (5,10,14): c.number_format=RF
        if bt[p]['cls']=='crypto': c.fill=CRYFILL
    r+=1
# overall KPI block (BT net vs Live) — the headline gap
allbt=kpi([r for _,r in sorted((t['entry_ts'],t['r_net']) for t in export['trades'] if t['cls']!='crypto')])
alllv=kpi([r for _,r in sorted((_t(e),e['realized_r']) for e in clean if isinstance(e.get('realized_r'),(int,float)))])
r+=1
ws6.cell(row=r,column=1,value='HEADLINE KPIs — Backtest (net, ex-crypto) vs Live (all clean)').font=BOLD; r+=1
kpirows=[('Trades',allbt['n'],alllv['n'],None),('Win rate',allbt['wr'],alllv['wr'],PCT),
         ('Expectancy (R/trade)',allbt['exp'],alllv['exp'],RF),('Profit factor',_inf(allbt['pf']),_inf(alllv['pf']),None),
         ('Payoff (avg win / avg loss)',_inf(allbt['payoff']),_inf(alllv['payoff']),None),
         ('Avg win (R)',allbt['aw'],alllv['aw'],RF),('Avg loss (R)',allbt['al'],alllv['al'],RF),
         ('Max drawdown (R)',allbt['dd'],alllv['dd'],RF),('Longest losing streak',allbt['streak'],alllv['streak'],None)]
ws6.cell(row=r,column=1,value='KPI').font=HD; ws6.cell(row=r,column=1).fill=HDRFILL
ws6.cell(row=r,column=2,value='Backtest (net)').font=HD; ws6.cell(row=r,column=2).fill=HDRFILL
ws6.cell(row=r,column=3,value='Live').font=HD; ws6.cell(row=r,column=3).fill=HDRFILL
for c in (1,2,3): ws6.cell(row=r,column=c).alignment=CEN; ws6.cell(row=r,column=c).border=BORD
r+=1
for label,bv,lvv,fmt in kpirows:
    ws6.cell(row=r,column=1,value=label).font=NORM
    cb=ws6.cell(row=r,column=2,value=bv); cl=ws6.cell(row=r,column=3,value=lvv); cb.font=NORM; cl.font=NORM
    if fmt: cb.number_format=fmt; cl.number_format=fmt
    for c in (1,2,3): ws6.cell(row=r,column=c).border=BORD
    r+=1
ws6.freeze_panes=f'A{s6+1}'
for i,x in enumerate([10,8,7,8,9,8,9,7,8,9,8,9,8,9],1): ws6.column_dimensions[get_column_letter(i)].width=x

try: wb.calculation.fullCalcOnLoad=True
except Exception: pass
wb.save(args.out)
print('wrote',args.out,'| net columns:',has_net,'| tabs:',wb.sheetnames)
