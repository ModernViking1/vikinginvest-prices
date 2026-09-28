"""Two cam_rev visuals: (A) recent LIVE trades with entry/exit/SL/TP on real candles (cTrader-style),
(B) the idealised cam_rev setup schematic. Writes PNGs. Run from the repo dir."""
import json, datetime as dt
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from backtest_rsi_per_class import _bars_norm

import argparse
_ap=argparse.ArgumentParser(); _ap.add_argument('--outdir', default='.'); _args=_ap.parse_args()
import os as _os
OUT=_args.outdir.rstrip('/')+'/'
_os.makedirs(OUT, exist_ok=True)
BG='#0e1513'; GRID='#243029'; GRN='#2ec27e'; RED='#e0574a'; INK='#e8efec'; MUT='#9aa8a1'; BLU='#5ca8ff'; AMB='#d9a441'

def sty(ax):
    ax.set_facecolor(BG)
    for s in ax.spines.values(): s.set_color(GRID)
    ax.tick_params(colors=MUT, labelsize=7); ax.grid(True,color=GRID,lw=.5,alpha=.6)

def candles(ax, bars):
    for i,b in enumerate(bars):
        up=b['c']>=b['o']; col=GRN if up else RED
        ax.plot([i,i],[b['l'],b['h']],color=col,lw=.8,zorder=2)
        lo=min(b['o'],b['c']); h=abs(b['c']-b['o']) or (b['h']-b['l'])*0.02
        ax.add_patch(Rectangle((i-0.3,lo),0.6,h,facecolor=col,edgecolor=col,zorder=3))

# ---------- data ----------
d=json.load(open('swing-executions.json')); ex=d.get('executions') or d.get('rows') or []
def strat(e):
    s=e.get('signal_id') or ''; return s.split(':')[0] if ':' in s else str(e.get('strategy') or '').split(' ')[0]
def tsec(e):
    t=e.get('ts'); return (t/1000 if t>1e11 else t) if isinstance(t,(int,float)) else 0
def entts(e):
    s=e.get('signal_id') or ''
    try: return int(s.rsplit(':',1)[-1])
    except: return None
hist=json.load(open('historical-ohlc.json')).get('pairs',{})
def m15(pk): return _bars_norm(hist.get(pk,{}).get('m15') or [])

cl=[e for e in ex if strat(e)=='cam_rev' and e.get('event')=='closed' and isinstance(e.get('realized_r'),(int,float))]
cl.sort(key=tsec)
# pick recent trades that have candle coverage (entry & exit within the m15 range), non-crypto
picks=[]
for e in reversed(cl):
    pk=e['pair']; bars=m15(pk)
    if not bars: continue
    et=entts(e); xt=tsec(e)
    if et is None: continue
    if bars[0]['_ts']<=et and xt<=bars[-1]['_ts']:
        picks.append(e)
    if len(picks)>=6: break
picks=list(reversed(picks))

# ---------- A: per-trade entry/exit ----------
n=len(picks); rows=(n+2)//3
figA,axs=plt.subplots(rows,3,figsize=(14,4.4*rows),dpi=120); figA.patch.set_facecolor(BG)
axs=axs.flatten() if n>1 else [axs]
figA.suptitle('cam_rev — recent LIVE trades: entry → exit (real m15 candles)',color=INK,fontsize=15,fontweight='bold')
for k,e in enumerate(picks):
    ax=axs[k]; pk=e['pair']; bars=m15(pk); et=entts(e); xt=tsec(e)
    ei=min(range(len(bars)),key=lambda i:abs(bars[i]['_ts']-et))
    xi=min(range(len(bars)),key=lambda i:abs(bars[i]['_ts']-xt))
    a=max(0,ei-16); b=min(len(bars),xi+6); win=bars[a:b]
    candles(ax,win); sty(ax)
    entry=e.get('entry_filled'); stop=e.get('stop'); tgt=e.get('target'); xp=e.get('exit_price'); dR=e['realized_r']
    short=e['dir']=='bear'
    ax.axhline(entry,color=BLU,lw=1,ls='-',alpha=.8); ax.text(len(win)-1,entry,f' entry {entry:g}',color=BLU,fontsize=6.5,va='center')
    ax.axhline(stop,color=RED,lw=1,ls='--',alpha=.8);  ax.text(len(win)-1,stop,f' SL {stop:g}',color=RED,fontsize=6.5,va='center')
    ax.axhline(tgt,color=GRN,lw=1,ls='--',alpha=.8);   ax.text(len(win)-1,tgt,f' TP {tgt:g}',color=GRN,fontsize=6.5,va='center')
    # entry & exit markers
    ecol=RED if short else GRN
    ax.scatter([ei-a],[entry],marker='v' if short else '^',s=130,color=ecol,edgecolors=INK,zorder=6)
    ax.scatter([xi-a],[xp],marker='x',s=90,color=INK,zorder=6,lw=2)
    ax.plot([ei-a,xi-a],[entry,xp],color=MUT,lw=1,ls=':',zorder=5)
    res='WIN' if dR>0 else 'LOSS'; rc=GRN if dR>0 else RED
    ax.set_title(f"{pk.upper()}  {'SELL' if short else 'BUY'}   {res} {dR:+.2f}R",color=rc,fontsize=10,fontweight='bold')
    ax.set_xticks([])
for k in range(n,len(axs)): axs[k].axis('off')
figA.tight_layout(rect=[0,0,1,0.97]); figA.savefig(OUT+'trade_entries_exits.png',facecolor=BG,bbox_inches='tight'); plt.close(figA)
print('wrote trade_entries_exits.png with',n,'trades:',[p['pair'] for p in picks])

# ---------- B: idealised cam_rev schematic ----------
figB,ax=plt.subplots(figsize=(12,7),dpi=125); figB.patch.set_facecolor(BG); sty(ax)
# Camarilla-style levels (short setup at R3)
R4,R3,PIV,S3,S4=118,112,100,88,82
for y,lab,c in [(R4,'R4',RED),(R3,'R3  (resistance / fade zone)',AMB),(PIV,'daily pivot',MUT),(S3,'S3',AMB),(S4,'S4',GRN)]:
    ax.axhline(y,color=c,lw=1,ls='--',alpha=.55); ax.text(0.2,y,lab,color=c,fontsize=8,va='bottom')
# synthetic candles: rally into R3, rejection wick candle at R3, then fade down to TP
syn=[(96,98),(98,101),(101,104),(104,107),(107,110),   # rally up (o,c)
     (110,111.5),                                       # approach
     (111.5,109.5)]                                     # <- rejection candle (long upper wick to R3+)
highs=[c+1 for o,c in syn]; highs[-1]=R3+3.5           # rejection wick pierces R3
lows =[o-1 for o,c in syn]
# draw
for i,((o,c)) in enumerate(syn):
    up=c>=o; col=GRN if up else RED
    ax.plot([i,i],[lows[i],highs[i]],color=col,lw=1.1,zorder=2)
    ax.add_patch(Rectangle((i-0.32,min(o,c)),0.64,abs(c-o) or .3,facecolor=col,edgecolor=col,zorder=3))
# post-entry fade candles down to TP
entry=109.5; R=(R4+ (R4-R3)*0.1)-entry  # stop above R4+buffer
stop=R4+ (R4-R3)*0.1; risk=stop-entry; tp=entry-risk
fade=[(109.5,106),(106,103),(103,100.5),(100.5,tp+0.5),(tp+0.5,tp-0.3)]
for j,(o,c) in enumerate(fade):
    i=len(syn)+j; up=c>=o; col=GRN if up else RED
    ax.plot([i,i],[min(o,c)-1,max(o,c)+1],color=col,lw=1.1,zorder=2)
    ax.add_patch(Rectangle((i-0.32,min(o,c)),0.64,abs(c-o) or .3,facecolor=col,edgecolor=col,zorder=3))
xr=len(syn)+len(fade)
ri=len(syn)-1  # rejection candle index
ax.scatter([ri],[entry],marker='v',s=200,color=RED,edgecolors=INK,zorder=7)
ax.annotate('m15 rejection candle at R3\n(long upper wick) → SHORT at its close',
            xy=(ri,entry),xytext=(ri-4.2,entry-9),color=INK,fontsize=9,
            arrowprops=dict(arrowstyle='->',color=INK))
ax.axhline(entry,color=BLU,lw=1.4); ax.text(xr-0.5,entry,f'  ENTRY',color=BLU,fontsize=9,va='center',fontweight='bold')
ax.axhline(stop,color=RED,lw=1.4,ls='--'); ax.text(xr-0.5,stop,'  SL  (S4/R4 + buffer)',color=RED,fontsize=9,va='center',fontweight='bold')
ax.axhline(tp,color=GRN,lw=1.4,ls='--'); ax.text(xr-0.5,tp,'  TP  (1:1, = entry − 1R)',color=GRN,fontsize=9,va='center',fontweight='bold')
# R brackets
ax.annotate('',xy=(xr-1.5,entry),xytext=(xr-1.5,stop),arrowprops=dict(arrowstyle='<->',color=RED))
ax.text(xr-1.3,(entry+stop)/2,'1R risk',color=RED,fontsize=8,va='center')
ax.annotate('',xy=(xr-0.9,entry),xytext=(xr-0.9,tp),arrowprops=dict(arrowstyle='<->',color=GRN))
ax.text(xr-0.7,(entry+tp)/2,'1R target',color=GRN,fontsize=8,va='center')
ax.set_title('Idealised cam_rev — Camarilla reversal (short at R3). Long side mirrors at S3.',color=INK,fontsize=13,fontweight='bold',pad=12)
ax.set_xlim(-1,xr+3); ax.set_ylim(S4-6,R4+8); ax.set_xticks([]); ax.set_ylabel('price',color=MUT)
figB.text(0.5,0.01,'Entry at the rejection-candle close · stop just beyond S4/R4 (+0.1×spacing) · fixed 1:1 target · m15 trigger, 07–22 UTC, 24h max hold',color=MUT,fontsize=8,ha='center')
figB.tight_layout(rect=[0,0.03,1,1]); figB.savefig(OUT+'camrev_idealised.png',facecolor=BG,bbox_inches='tight'); plt.close(figB)
print('wrote camrev_idealised.png')
