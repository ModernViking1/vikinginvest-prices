"""Regenerate the investor 'Viking Live Book' dashboard (viking_live_book.html) from committed data.

Single source of truth for the backtest-vs-live reconciliation board the investors open. Reads:
  - backtest-summary.json        -> causal 3yr (regime-gated) exp / WR / both-OOS per strategy
  - executions.json              -> intraday cBot live fills   (placed/closed events)
  - swing-executions.json        -> swing cBot live fills      (placed/closed events)
  - equity-executions.json       -> equity MT5 demo fills      (flat closed-trade rows)  [optional]

Live exp/WR/n per strategy is computed like observer_review: closed trades only, sane-R filtered
(|R| <= 6), and only trades CLOSED at/after the forward-test inception (see FORWARD_START). The
equity column wires itself in automatically as the demo trades close.

RANKING: one flat leaderboard across ALL books (swing / intraday / equity mixed), most profitable
on top, ranked by a LIVE-WEIGHTED blended score — backtest acts as a small prior and the live mean
pulls the score as fills accumulate, so a strategy proven live rises as it earns it.

  python build_live_book.py                 # -> viking_live_book.html
  python build_live_book.py --out x.html     # custom path
Commits nothing; the published artifact is refreshed by re-publishing this file.
"""
import argparse, datetime as dt, json, os
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
SANE_R = 6.0

# Forward-test inception. The live book was corrected on 5 Oct (causal detectors, swing limit entry,
# equity-EA trail fix). Only trades CLOSED at/after this instant count toward live WR/RR — a clean
# forward test on the fully-fixed system. Bump it to re-baseline again later.
FORWARD_START_ISO = "2026-10-05T00:00:00"
FORWARD_START = dt.datetime.fromisoformat(FORWARD_START_ISO).replace(tzinfo=dt.timezone.utc).timestamp()

# Rank score = (n*liveMean + K*backtest) / (n+K). Backtest is a K-trade prior; live pulls the score
# as fills accrue, so live-filled trades carry progressively higher weight. Lower K => live dominates
# sooner. K=4 keeps a single good/bad live trade from catapulting a strategy on n=1 (the cam_rev
# lesson) while still letting a real live edge climb within a handful of fills.
RANK_PRIOR_K = 4.0

ROSTER = {
    "Swing · OANDA cBot":   ["hs", "gbreak", "twob_ix", "mmove", "engulf_manip",
                              "fma_sweep_cm", "holygrail_cm_m15"],
    "Intraday · cBot":      ["absorb_btc", "mmove_m15"],
    "Equity · MT5 demo":    ["holygrail_eq", "twob_eq", "holygrail_eq_m15", "volbreak_eq"],
}
BOOK_BADGE = {"Swing · OANDA cBot": "SWING", "Intraday · cBot": "INTRADAY", "Equity · MT5 demo": "EQUITY"}


def _load_json(name):
    try:
        return json.load(open(os.path.join(_HERE, name)))
    except Exception:
        return None


def backtest_rows():
    d = _load_json("backtest-summary.json") or {}
    sg = d.get("strategies_gated") or d.get("strategies") or {}
    return {st: {"exp": r.get("exp", 0.0), "wr": r.get("wr", 0.0),
                 "o1": r.get("oos_1st", 0.0), "o2": r.get("oos_2nd", 0.0)} for st, r in sg.items()}


def _method_from_id(sid, swing):
    p = (sid or "").split(":")
    if len(p) < 3:
        return None
    return p[0] if swing else p[-1]


def live_rows():
    """{strategy: [realized_r,...]} across all three logs (closed, sane-R, at/after inception)."""
    agg = defaultdict(list)
    for fn, swing in (("executions.json", False), ("swing-executions.json", True)):
        d = _load_json(fn)
        if not d:
            continue
        for r in d.get("executions", []):
            if r.get("event") != "closed":
                continue
            rr = r.get("realized_r")
            if rr is None or abs(rr) > SANE_R:
                continue
            ts = r.get("ts")
            if ts is not None and (ts / 1000.0) < FORWARD_START:
                continue
            m = _method_from_id(r.get("signal_id"), swing)
            if m:
                agg[m].append(rr)
    d = _load_json("equity-executions.json")
    if d:
        for r in d.get("executions", []):
            rr = r.get("realized_r")
            st = r.get("strategy")
            cms = r.get("closed_ms")
            if cms is not None and (cms / 1000.0) < FORWARD_START:
                continue
            if st and rr is not None and abs(rr) <= SANE_R:
                agg[st].append(rr)
    return agg


def build_data():
    bt = backtest_rows()
    lv = live_rows()
    rows = []
    for book, strategies in ROSTER.items():
        for st in strategies:
            b = bt.get(st)
            if not b:
                continue
            rs = lv.get(st, [])
            n = len(rs)
            lvmean = (sum(rs) / n) if n else None
            score = ((n * (lvmean if lvmean is not None else 0.0) + RANK_PRIOR_K * b["exp"])
                     / (n + RANK_PRIOR_K))
            rows.append({
                "bk": book, "badge": BOOK_BADGE[book], "s": st,
                "bt": round(b["exp"], 3), "btwr": round(b["wr"]),
                "o1": round(b["o1"], 2), "o2": round(b["o2"], 2),
                "lv": (round(lvmean, 3) if lvmean is not None else None),
                "lvwr": (round(100.0 * sum(1 for x in rs if x > 0) / n) if n else None),
                "n": n, "score": round(score, 3),
            })
    # ONE flat leaderboard — most profitable on top, by the live-weighted blended score.
    rows.sort(key=lambda r: -r["score"])
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    return rows


_TMPL = r"""<title>Viking Live Book</title>
<meta name="description" content="One live-weighted leaderboard of every Viking strategy — causal 3-year backtest beside the live forward test, most profitable on top across swing, intraday and equity.">
<style>
  /* One flat leaderboard, ranked by a live-weighted blend of backtest + live expectancy. Each row:
     rank, strategy + book badge, a zero-centred backtest-vs-live bar, and the figures. Dark-first. */
  :root{
    --bg:#0b100d; --panel:#111814; --rule:#20302a; --rule2:#2b3d35;
    --ink:#e9f2ed; --inkm:#a4b9af; --inkd:#6b7f76;
    --bt:#56b487; --bt-deep:#2f8f63; --live:#e0a83a; --neg:#cf5b4c; --warn:#d8b24a;
    --sw:#5aa0d6; --intra:#b584d8; --eq:#4fb59a;        /* book badges */
    color-scheme:dark;
  }
  @media (prefers-color-scheme:light){ :root:not([data-theme="dark"]){
    --bg:#f2f6f3; --panel:#ffffff; --rule:#dde7e1; --rule2:#cbd9d1;
    --ink:#122019; --inkm:#47584f; --inkd:#78897f;
    --bt:#2f8f5d; --bt-deep:#1e7849; --live:#9a7410; --neg:#b23e2d; --warn:#8a6b10;
    --sw:#2b6ca3; --intra:#7a4caa; --eq:#1f8a72;
    color-scheme:light;
  }}
  :root[data-theme="light"]{
    --bg:#f2f6f3; --panel:#ffffff; --rule:#dde7e1; --rule2:#cbd9d1;
    --ink:#122019; --inkm:#47584f; --inkd:#78897f;
    --bt:#2f8f5d; --bt-deep:#1e7849; --live:#9a7410; --neg:#b23e2d; --warn:#8a6b10;
    --sw:#2b6ca3; --intra:#7a4caa; --eq:#1f8a72;
    color-scheme:light;
  }
  *{box-sizing:border-box}
  body{background:var(--bg); color:var(--ink); font-family:"IBM Plex Sans",system-ui,-apple-system,sans-serif; line-height:1.5;}
  .mono{font-family:"JetBrains Mono",ui-monospace,"SFMono-Regular",monospace; font-variant-numeric:tabular-nums;}
  .wrap{max-width:960px; margin:0 auto; padding-block:30px; padding-left:16px; padding-right:16px;}
  .eyebrow{font-family:"JetBrains Mono",monospace; font-size:11px; letter-spacing:3px; color:var(--inkd); text-transform:uppercase;}
  h1{font-family:"JetBrains Mono",monospace; font-weight:700; font-size:clamp(23px,5vw,34px); margin:.3rem 0 .45rem; letter-spacing:-.5px; text-wrap:balance;}
  .lede{color:var(--inkm); font-size:14px; max-width:70ch; margin:0;}
  .lede b{color:var(--ink); font-weight:600;}
  .kpis{display:flex; flex-wrap:wrap; gap:10px; margin:18px 0 2px;}
  .kpi{flex:1 1 120px; min-width:0; background:var(--panel); border:1px solid var(--rule); border-radius:8px; padding:11px 13px;}
  .kpi .v{font-family:"JetBrains Mono",monospace; font-size:19px; font-weight:700; color:var(--ink); font-variant-numeric:tabular-nums;}
  .kpi .k{font-size:10.5px; color:var(--inkd); text-transform:uppercase; letter-spacing:1px; margin-top:2px;}
  .note{margin-top:16px; padding:12px 14px; border:1px solid var(--rule); border-left:3px solid var(--warn); border-radius:6px; background:var(--panel); color:var(--inkm); font-size:12.5px; line-height:1.65;}
  .note b{color:var(--ink);}
  .legend{display:flex; gap:16px; flex-wrap:wrap; font-size:11.5px; color:var(--inkm); margin:22px 0 6px; align-items:center;}
  .key{display:inline-flex; align-items:center; gap:6px;}
  .dot{width:11px; height:11px; border-radius:2px; display:inline-block;}
  .hdr{display:grid; grid-template-columns:30px 1fr 150px; gap:12px; padding:0 2px 6px; border-bottom:1px solid var(--rule2);
    font-family:"JetBrains Mono",monospace; font-size:10px; letter-spacing:1px; text-transform:uppercase; color:var(--inkd);}
  .hdr .r3{text-align:right;}
  .row{display:grid; grid-template-columns:30px 1fr 150px; gap:12px; align-items:center; padding:10px 2px; border-bottom:1px solid var(--rule);}
  @media (max-width:640px){ .row,.hdr{grid-template-columns:28px 1fr;} .chart{grid-column:1/-1; order:3;} .fig{grid-column:2; text-align:right;} .hdr .r2{display:none;} }
  .rk{font-family:"JetBrains Mono",monospace; font-size:15px; font-weight:700; color:var(--inkd); text-align:center;}
  .rk.top{color:var(--live);}
  .nm{min-width:0;}
  .nm .s{font-family:"JetBrains Mono",monospace; font-size:13px; color:var(--ink); font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;}
  .nm .meta{font-size:10px; color:var(--inkd); margin-top:2px; display:flex; gap:7px; align-items:center; flex-wrap:wrap;}
  .badge{font-family:"JetBrains Mono",monospace; font-size:8.5px; font-weight:700; letter-spacing:.5px; padding:1px 5px; border-radius:3px; color:#0b100d;}
  .badge.SWING{background:var(--sw);} .badge.INTRADAY{background:var(--intra);} .badge.EQUITY{background:var(--eq);}
  .chart{position:relative; height:34px; min-width:0;}
  .zero{position:absolute; top:3px; bottom:3px; width:1px; background:var(--inkd); opacity:.55;}
  .bar{position:absolute; height:11px; border-radius:2px; transition:filter .12s;}
  .bar.bt{top:4px;} .bar.lv{top:19px;} .bar:hover{filter:brightness(1.18);}
  .fig{text-align:right; font-family:"JetBrains Mono",monospace; line-height:1.4;}
  .fig .score{font-size:15px; font-weight:700;}
  .fig .score.p{color:var(--live);} .fig .score.n{color:var(--neg);}
  .fig .sub{font-size:10px; color:var(--inkd); display:block; margin-top:1px;}
  .fig .sub .b{color:var(--bt);} .fig .sub .l{color:var(--live);} .fig .sub .ln{color:var(--neg);}
  .fig .st{font-size:9px; color:var(--inkd); display:block; margin-top:1px;}
  .foot{margin-top:24px; display:flex; flex-direction:column; gap:9px;}
  .foot p{margin:0; font-size:11.5px; color:var(--inkd); line-height:1.6;} .foot b{color:var(--inkm);}
  .stamp{margin-top:4px; font-family:"JetBrains Mono",monospace; font-size:10.5px; color:var(--inkd);}
</style>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=JetBrains+Mono:wght@500;700&display=swap">
<div class="wrap">
  <div class="eyebrow">Viking Edge · Forward Test</div>
  <h1>Viking Live Book</h1>
  <p class="lede">One leaderboard of every live strategy — swing, intraday and equity mixed — ranked <b>most profitable on top</b>. The rank blends each strategy's <b>causal 3-year backtest</b> with its <b>live forward test</b>, and the live result carries <b>more weight as real fills accumulate</b>. Live counts only trades closed at/after inception below. A reconciliation, not a returns claim.</p>
  <div class="kpis" id="kpis"></div>
  <div class="note"><b>How the rank works.</b> Score = a blend of live and backtest expectancy (R/trade): the backtest sets the starting line, and each real closed trade pulls the score toward the live mean — so a strategy climbs as it proves itself live, and one lucky or unlucky fill can't catapult it on tiny n. The <b>equity book is MT5 demo</b>; its live fills join automatically as trades close. These figures are <b>not yet confirmed as achievable</b> — real capital waits until live tracks the backtest.</div>
  <div class="legend">
    <span class="key"><span class="dot" style="background:var(--bt)"></span>Backtest · 3yr causal</span>
    <span class="key"><span class="dot" style="background:var(--live)"></span>Live · positive</span>
    <span class="key"><span class="dot" style="background:var(--neg)"></span>Live · negative</span>
    <span class="key"><span class="badge SWING">SWING</span><span class="badge INTRADAY">INTRADAY</span><span class="badge EQUITY">EQUITY</span></span>
  </div>
  <div class="hdr"><span>#</span><span class="r2">strategy · book · backtest bar / live bar</span><span class="r3">rank score</span></div>
  <div id="ranked"></div>
  <div class="foot">
    <p><b>Score</b> = (n·liveMean + K·backtest) / (n+K), K=4 — backtest as a 4-trade prior, live-weighted thereafter. <b>Backtest</b> = look-ahead-free 3-year replay, regime-gated, frictionless (net is lower). <b>Live</b> = real cBot/MT5 fills since inception, |R| ≤ 6.</p>
    <p><b>Fresh start.</b> Live counting was re-baselined at inception (__FWD__); earlier fills (pre-fix regime) are excluded, so live columns start near zero and fill from here. Treat any row under ~n=20 as provisional.</p>
    <p class="stamp" id="stamp"></p>
  </div>
</div>
<script>
  var ASOF = "__ASOF__";
  var data = __DATA__;
  var LO=-0.45, HI=0.35, SPAN=HI-LO;
  function pos(v){ return (Math.max(LO,Math.min(HI,v))-LO)/SPAN*100; }
  var zeroPct = pos(0);
  function barHtml(v, cls){
    if(v===null||v===undefined) return '';
    var p=pos(v), neg=v<0, left=neg?p:zeroPct, w=Math.max(0.8, Math.abs(p-zeroPct));
    var color = cls==='bt' ? 'linear-gradient(90deg,color-mix(in srgb,var(--bt) 55%,transparent),var(--bt-deep))' : (neg?'var(--neg)':'var(--live)');
    var title=(cls==='bt'?'Backtest':'Live')+' expectancy '+(v>=0?'+':'')+v.toFixed(3)+'R'+((v<LO||v>HI)?' (clamped)':'');
    return '<div class="bar '+cls+'" style="left:'+left+'%;width:'+w+'%;background:'+color+'" title="'+title+'"></div>';
  }
  function statusOf(d){
    if(d.n===0) return d.badge==='EQUITY' ? 'demo · awaiting first close' : 'awaiting first fill';
    if(d.n<20)  return 'live thin · n='+d.n;
    var g=d.lv-d.bt;
    if(g < -0.04) return 'live below · '+g.toFixed(2)+'R';
    if(g >  0.04) return 'live ahead · +'+g.toFixed(2)+'R';
    return 'tracking';
  }
  var liveN=data.reduce(function(a,d){return a+(d.n||0);},0);
  var withLive=data.filter(function(d){return d.n>0;}).length;
  document.getElementById('kpis').innerHTML=[
    [data.length,'strategies ranked'],['~'+liveN,'live fills since inception'],
    [withLive,'with live trades'],['3 yr','backtest window']
  ].map(function(k){return '<div class="kpi"><div class="v mono">'+k[0]+'</div><div class="k">'+k[1]+'</div></div>';}).join('');
  document.getElementById('ranked').innerHTML=data.map(function(d){
    var sc=(d.score>=0?'+':'')+d.score.toFixed(3)+'R';
    var scls=d.score<0?'n':'p';
    var lvtxt = d.n===0 ? 'LV —' : '<span class="'+(d.lv<0?'ln':'l')+'">LV '+(d.lv>=0?'+':'')+d.lv.toFixed(3)+' ('+d.lvwr+'% n='+d.n+')</span>';
    return '<div class="row">'
      +'<div class="rk'+(d.rank<=3?' top':'')+'">'+d.rank+'</div>'
      +'<div class="nm"><div class="s">'+d.s+'</div><div class="meta"><span class="badge '+d.badge+'">'+d.badge+'</span> BT '+d.btwr+'% WR · OOS '+(d.o1>=0?'+':'')+d.o1.toFixed(2)+'/'+(d.o2>=0?'+':'')+d.o2.toFixed(2)+'</div></div>'
      +'<div class="chart"><div class="zero" style="left:'+zeroPct+'%"></div>'+barHtml(d.bt,'bt')+barHtml(d.lv,'lv')+'</div>'
      +'<div class="fig"><span class="score '+scls+'">'+sc+'</span>'
        +'<span class="sub"><span class="b">BT +'+d.bt.toFixed(3)+'</span> · '+lvtxt+'</span>'
        +'<span class="st">'+statusOf(d)+'</span></div>'
      +'</div>';
  }).join('');
  document.getElementById('stamp').textContent='Snapshot as of '+ASOF+' · forward-test inception __FWD__ · ranked by live-weighted score (K=4) · |R| ≤ 6';
</script>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(_HERE, "viking_live_book.html"))
    args = ap.parse_args()
    rows = build_data()
    asof = dt.datetime.now(dt.timezone.utc).strftime("%-d %b %Y %H:%M") + " UTC"
    fwd = dt.datetime.fromisoformat(FORWARD_START_ISO).strftime("%-d %b %Y %H:%M") + " UTC"
    html = (_TMPL.replace("__ASOF__", asof)
                 .replace("__FWD__", fwd)
                 .replace("__DATA__", json.dumps(rows)))
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"wrote {args.out}  ({len(rows)} strategies; inception {fwd})")
    for r in rows:
        live = f"{r['lv']:+.3f}R n={r['n']}" if r["n"] else "—"
        print(f"  #{r['rank']:<2} {r['s']:<18} {r['badge']:<9} score {r['score']:+.3f}R  (bt {r['bt']:+.3f} | live {live})")


if __name__ == "__main__":
    main()
