"""Regenerate the investor 'Viking Live Book' dashboard (viking_live_book.html) from committed data.

Single source of truth for the backtest-vs-live reconciliation board the investors open. Reads:
  - backtest-summary.json        -> causal 3yr (regime-gated) exp / WR / both-OOS per strategy
  - executions.json              -> intraday cBot live fills   (placed/closed events)
  - swing-executions.json        -> swing cBot live fills      (placed/closed events)
  - equity-executions.json       -> equity MT5 demo fills      (flat closed-trade rows)  [optional]

Live exp/WR/n per strategy is computed exactly like observer_review: closed trades only, sane-R
filtered (|R| <= 6). The EQUITY column wires itself in automatically — the four .EQ rows stay
'demo / 0 closed' until equity-executions.json appears, then show real WR/R on the next build.

Ordering: MOST PROFITABLE ON TOP. Books are ordered by their best backtest expectancy (equity,
then swing, then intraday); strategies within each book are ordered by backtest expectancy desc —
the proven 3yr number, so thin live samples can't jerk the board around every few trades.

  python build_live_book.py                 # -> viking_live_book.html
  python build_live_book.py --out x.html     # custom path
Commits nothing; the published artifact is refreshed by re-publishing this file.
"""
import argparse, datetime as dt, json, os
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
SANE_R = 6.0

# Forward-test inception. The live book was corrected over early October (causal detectors, swing
# limit entry, the equity-EA trail fix). Pre-inception fills came from the old/buggy regime and
# don't represent the system now live — so the live WR/RR counts ONLY trades CLOSED on/after this
# date, a clean forward test on the corrected system. Raise it to re-baseline again later.
FORWARD_START_ISO = "2026-10-05"
FORWARD_START = dt.datetime.fromisoformat(FORWARD_START_ISO).replace(tzinfo=dt.timezone.utc).timestamp()

# The live book, grouped by execution venue. Strategy tags match backtest-summary + the feeds.
ROSTER = {
    "Swing · OANDA cBot":   ["hs", "gbreak", "twob_ix", "mmove", "engulf_manip",
                              "fma_sweep_cm", "holygrail_cm_m15"],
    "Intraday · cBot":      ["absorb_btc", "mmove_m15"],
    "Equity · MT5 demo":    ["holygrail_eq", "twob_eq", "holygrail_eq_m15", "volbreak_eq"],
}
BOOK_SUB = {
    "Swing · OANDA cBot":  "swing structure · H1 / M15 · limit entry",
    "Intraday · cBot":     "BTC absorption + FVG continuation · M15",
    "Equity · MT5 demo":   "the .EQ edges · demo forward test",
}


def _load_json(name):
    try:
        return json.load(open(os.path.join(_HERE, name)))
    except Exception:
        return None


def backtest_rows():
    """{strategy: {exp, wr, o1, o2}} from the gated causal summary."""
    d = _load_json("backtest-summary.json") or {}
    sg = d.get("strategies_gated") or d.get("strategies") or {}
    out = {}
    for st, r in sg.items():
        out[st] = {"exp": r.get("exp", 0.0), "wr": r.get("wr", 0.0),
                   "o1": r.get("oos_1st", 0.0), "o2": r.get("oos_2nd", 0.0)}
    return out


def _method_from_id(sid, swing):
    """swing ids = strat:pair:ts ; intraday ids = (viking-)pair:ts_ms:method."""
    p = (sid or "").split(":")
    if len(p) < 3:
        return None
    return p[0] if swing else p[-1]


def live_rows():
    """{strategy: [realized_r,...]} across all three logs (closed trades, sane-R)."""
    agg = defaultdict(list)
    # Event-based logs (intraday + swing): join on closed events.
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
            ts = r.get("ts")                      # closed-event epoch (ms)
            if ts is not None and (ts / 1000.0) < FORWARD_START:
                continue                          # pre-inception fill — excluded from the forward test
            m = _method_from_id(r.get("signal_id"), swing)
            if m:
                agg[m].append(rr)
    # Equity bridge log: flat closed-trade rows, each carries its own strategy + realized_r.
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
                continue                         # not in the summary -> skip rather than fake it
            rs = lv.get(st, [])
            n = len(rs)
            rec = {"bk": book, "s": st, "bt": round(b["exp"], 3), "btwr": round(b["wr"]),
                   "o1": round(b["o1"], 2), "o2": round(b["o2"], 2)}
            if n:
                rec.update({"lv": round(sum(rs) / n, 3),
                            "lvwr": round(100.0 * sum(1 for x in rs if x > 0) / n), "n": n})
            else:
                rec.update({"lv": None, "lvwr": None, "n": 0})
            rows.append(rec)
    # MOST PROFITABLE ON TOP: order books by best backtest exp, strategies by backtest exp desc.
    book_rank = {}
    for r in rows:
        book_rank[r["bk"]] = max(book_rank.get(r["bk"], -9), r["bt"])
    rows.sort(key=lambda r: (-book_rank[r["bk"]], r["bk"], -r["bt"]))
    return rows


# ── HTML template (data injected as JSON; the view logic lives in the page) ──────────
_TMPL = r"""<title>Viking Live Book</title>
<meta name="description" content="Causal 3-year backtest set beside the live forward test to date — WR and expectancy across the equity, intraday and swing books, most profitable on top.">
<style>
  /* Reconciliation board: per strategy, the causal 3-yr backtest expectancy sits above the
     live-so-far expectancy on one zero-centred scale, grouped by book, most profitable on top. */
  :root{
    --bg:#0b100d; --panel:#111814; --rule:#20302a; --rule2:#2b3d35;
    --ink:#e9f2ed; --inkm:#a4b9af; --inkd:#6b7f76;
    --bt:#56b487; --bt-deep:#2f8f63; --live:#e0a83a; --neg:#cf5b4c; --warn:#d8b24a;
    color-scheme:dark;
  }
  @media (prefers-color-scheme:light){ :root:not([data-theme="dark"]){
    --bg:#f2f6f3; --panel:#ffffff; --rule:#dde7e1; --rule2:#cbd9d1;
    --ink:#122019; --inkm:#47584f; --inkd:#78897f;
    --bt:#2f8f5d; --bt-deep:#1e7849; --live:#9a7410; --neg:#b23e2d; --warn:#8a6b10;
    color-scheme:light;
  }}
  :root[data-theme="light"]{
    --bg:#f2f6f3; --panel:#ffffff; --rule:#dde7e1; --rule2:#cbd9d1;
    --ink:#122019; --inkm:#47584f; --inkd:#78897f;
    --bt:#2f8f5d; --bt-deep:#1e7849; --live:#9a7410; --neg:#b23e2d; --warn:#8a6b10;
    color-scheme:light;
  }
  *{box-sizing:border-box}
  body{background:var(--bg); color:var(--ink); font-family:"IBM Plex Sans",system-ui,-apple-system,sans-serif; line-height:1.5;}
  .mono{font-family:"JetBrains Mono",ui-monospace,"SFMono-Regular",monospace; font-variant-numeric:tabular-nums;}
  .wrap{max-width:940px; margin:0 auto; padding-block:30px; padding-left:16px; padding-right:16px;}
  .eyebrow{font-family:"JetBrains Mono",monospace; font-size:11px; letter-spacing:3px; color:var(--inkd); text-transform:uppercase;}
  h1{font-family:"JetBrains Mono",monospace; font-weight:700; font-size:clamp(23px,5vw,34px); margin:.3rem 0 .45rem; letter-spacing:-.5px; text-wrap:balance;}
  .lede{color:var(--inkm); font-size:14px; max-width:68ch; margin:0;}
  .lede b{color:var(--ink); font-weight:600;}
  .kpis{display:flex; flex-wrap:wrap; gap:10px; margin:18px 0 2px;}
  .kpi{flex:1 1 120px; min-width:0; background:var(--panel); border:1px solid var(--rule); border-radius:8px; padding:11px 13px;}
  .kpi .v{font-family:"JetBrains Mono",monospace; font-size:20px; font-weight:700; color:var(--ink); font-variant-numeric:tabular-nums;}
  .kpi .k{font-size:10.5px; color:var(--inkd); text-transform:uppercase; letter-spacing:1px; margin-top:2px;}
  .note{margin-top:16px; padding:12px 14px; border:1px solid var(--rule); border-left:3px solid var(--warn); border-radius:6px; background:var(--panel); color:var(--inkm); font-size:12.5px; line-height:1.65;}
  .note b{color:var(--ink);}
  .legend{display:flex; gap:16px; flex-wrap:wrap; font-size:11.5px; color:var(--inkm); margin:22px 0 2px; align-items:center;}
  .key{display:inline-flex; align-items:center; gap:6px;}
  .dot{width:11px; height:11px; border-radius:2px; display:inline-block;}
  .grp{display:flex; align-items:baseline; justify-content:space-between; gap:10px; font-family:"JetBrains Mono",monospace; font-size:11px; letter-spacing:1.5px; color:var(--inkd); text-transform:uppercase; margin:22px 0 2px; padding-bottom:6px; border-bottom:1px solid var(--rule2);}
  .grp .gn{color:var(--inkm);} .grp .gs{letter-spacing:.3px; text-transform:none; color:var(--inkd); font-size:10.5px;}
  .row{display:grid; grid-template-columns:154px 1fr 160px; gap:12px; align-items:center; padding:9px 2px; border-bottom:1px solid var(--rule);}
  @media (max-width:640px){ .row{grid-template-columns:1fr; gap:5px;} .fig{order:3; text-align:left;} .chart{order:2;} }
  .nm{min-width:0;}
  .nm .s{font-family:"JetBrains Mono",monospace; font-size:13px; color:var(--ink); font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;}
  .nm .meta{font-size:10px; color:var(--inkd); margin-top:1px;}
  .chart{position:relative; height:36px; min-width:0;}
  .zero{position:absolute; top:3px; bottom:3px; width:1px; background:var(--inkd); opacity:.55;}
  .bar{position:absolute; height:12px; border-radius:2px; transition:filter .12s;}
  .bar.bt{top:4px;} .bar.lv{top:20px;} .bar:hover{filter:brightness(1.18);}
  .fig{text-align:right; font-family:"JetBrains Mono",monospace; font-size:11.5px; line-height:1.45;}
  .fig .b{color:var(--bt);} .fig .l{color:var(--live);} .fig .n{color:var(--neg);} .fig .z{color:var(--inkd);}
  .fig .st{font-size:9px; color:var(--inkd); display:block; margin-top:2px; letter-spacing:.2px;}
  .foot{margin-top:24px; display:flex; flex-direction:column; gap:9px;}
  .foot p{margin:0; font-size:11.5px; color:var(--inkd); line-height:1.6;} .foot b{color:var(--inkm);}
  .stamp{margin-top:4px; font-family:"JetBrains Mono",monospace; font-size:10.5px; color:var(--inkd);}
</style>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=JetBrains+Mono:wght@500;700&display=swap">
<div class="wrap">
  <div class="eyebrow">Viking Edge · Forward Test</div>
  <h1>Viking Live Book</h1>
  <p class="lede">Every live strategy's <b>causal 3-year backtest</b> (look-ahead removed) set beside its <b>live forward test</b>, most profitable on top. Live counts only trades closed <b>on or after the inception date below</b> — a clean forward test on the corrected system. Bars are expectancy in R per trade on one zero-centred scale; a reconciliation, not a returns claim.</p>
  <div class="kpis" id="kpis"></div>
  <div class="note"><b>Read it straight.</b> The backtest is causal and positive in both out-of-sample halves. The live forward test is <b>early and thin</b> — most strategies have under ~40 fills — and on the names with enough trades it is running <b>below</b> backtest: the remaining gap is execution (slippage, cost, fill timing), now being closed by the switch to limit entry. The <b>equity book is on MT5 demo</b>; its live column fills in automatically as the first demo trades close. So these backtest figures are <b>not yet confirmed as achievable</b>; real capital waits until live tracks the backtest.</div>
  <div class="legend">
    <span class="key"><span class="dot" style="background:var(--bt)"></span>Backtest expectancy · 3yr causal</span>
    <span class="key"><span class="dot" style="background:var(--live)"></span>Live expectancy · positive</span>
    <span class="key"><span class="dot" style="background:var(--neg)"></span>Live expectancy · negative</span>
  </div>
  <div id="books"></div>
  <div class="foot">
    <p><b>Backtest</b> = look-ahead-free 3-year replay, regime-gated and frictionless (net of cost is lower). <b>Live</b> = real cBot / MT5 fills to date, broken fills filtered (|R| ≤ 6). The right column shows backtest expectancy, then live expectancy with win rate and fill count (n).</p>
    <p><b>Order.</b> Books are ranked by their best proven expectancy (equity, then swing, then intraday); within each book, strategies are ranked by 3-yr backtest expectancy — the proven number, so a thin live sample can't reorder the board every few trades.</p>
    <p><b>Fresh start.</b> Live counting was re-baselined at inception (__FWD__); earlier fills, taken under the pre-fix regime, are excluded. So the live columns start near zero and fill in from here — treat any row under ~n=20 as provisional, a single trade swings it.</p>
    <p class="stamp" id="stamp"></p>
  </div>
</div>
<script>
  var ASOF = "__ASOF__";
  var data = __DATA__;
  var BOOK_SUB = __BOOKSUB__;
  var LO=-0.45, HI=0.35, SPAN=HI-LO;
  function pos(v){ return (Math.max(LO,Math.min(HI,v))-LO)/SPAN*100; }
  var zeroPct = pos(0);
  function clampNote(v){ return (v<LO||v>HI) ? " (clamped)" : ""; }
  function barHtml(v, cls){
    if(v===null||v===undefined) return '';
    var p=pos(v), neg=v<0, left=neg?p:zeroPct, w=Math.max(0.8, Math.abs(p-zeroPct));
    var color = cls==='bt' ? 'linear-gradient(90deg,color-mix(in srgb,var(--bt) 55%,transparent),var(--bt-deep))' : (neg?'var(--neg)':'var(--live)');
    var title=(cls==='bt'?'Backtest':'Live')+' expectancy '+(v>=0?'+':'')+v.toFixed(3)+'R'+clampNote(v);
    return '<div class="bar '+cls+'" style="left:'+left+'%;width:'+w+'%;background:'+color+'" title="'+title+'"></div>';
  }
  function statusOf(d){
    if(d.n===0) return d.bk.indexOf('Equity')>=0 ? 'demo · awaiting first close' : 'awaiting first fill';
    if(d.n<20)  return 'live thin · n='+d.n;
    var g=d.lv-d.bt;
    if(g < -0.04) return 'live below · '+g.toFixed(2)+'R';
    if(g >  0.04) return 'live ahead · +'+g.toFixed(2)+'R';
    return 'tracking';
  }
  var liveN=data.reduce(function(a,d){return a+(d.n||0);},0);
  var books=data.reduce(function(a,d){if(a.indexOf(d.bk)<0)a.push(d.bk);return a;},[]);
  document.getElementById('kpis').innerHTML=[
    ['3 yr','backtest window'],[data.length,'live strategies'],
    [books.length,'books · equity / swing / intraday'],['~'+liveN,'live fills to date']
  ].map(function(k){return '<div class="kpi"><div class="v mono">'+k[0]+'</div><div class="k">'+k[1]+'</div></div>';}).join('');
  var grouped={}, order=[];
  data.forEach(function(d){ if(!grouped[d.bk]){grouped[d.bk]=[];order.push(d.bk);} grouped[d.bk].push(d); });
  document.getElementById('books').innerHTML=order.map(function(bk){
    var rows=grouped[bk].map(function(d){
      var lvtxt,lvcls;
      if(d.n===0){lvtxt='—';lvcls='z';} else {lvtxt=(d.lv>=0?'+':'')+d.lv.toFixed(3)+'R';lvcls=(d.lv<0?'n':'l');}
      var wrn=d.n===0?'':' · '+d.lvwr+'% · n='+d.n;
      return '<div class="row">'
        +'<div class="nm"><div class="s">'+d.s+'</div><div class="meta">BT '+d.btwr+'% WR · OOS '+(d.o1>=0?'+':'')+d.o1.toFixed(2)+'/'+(d.o2>=0?'+':'')+d.o2.toFixed(2)+'</div></div>'
        +'<div class="chart"><div class="zero" style="left:'+zeroPct+'%"></div>'+barHtml(d.bt,'bt')+barHtml(d.lv,'lv')+'</div>'
        +'<div class="fig"><span class="b">+'+d.bt.toFixed(3)+'R</span> &nbsp;<span class="'+lvcls+'">'+lvtxt+'</span><span class="st">'+statusOf(d)+wrn+'</span></div>'
        +'</div>';
    }).join('');
    return '<div class="grp"><span class="gn">'+bk+'</span><span class="gs">'+(BOOK_SUB[bk]||'')+'</span></div>'+rows;
  }).join('');
  document.getElementById('stamp').textContent='Snapshot as of '+ASOF+' · forward-test inception __FWD__ · backtest regime-gated, causal · live = real fills, |R| ≤ 6';
</script>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(_HERE, "viking_live_book.html"))
    args = ap.parse_args()
    rows = build_data()
    asof = dt.datetime.now(dt.timezone.utc).strftime("%-d %B %Y")
    fwd = dt.datetime.fromisoformat(FORWARD_START_ISO).strftime("%-d %b %Y")
    html = (_TMPL.replace("__ASOF__", asof)
                 .replace("__FWD__", fwd)
                 .replace("__DATA__", json.dumps(rows))
                 .replace("__BOOKSUB__", json.dumps(BOOK_SUB)))
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    neq = sum(1 for r in rows if r["bk"].startswith("Equity") and r["n"] > 0)
    print(f"wrote {args.out}  ({len(rows)} strategies, equity live rows: {neq})")
    for r in rows:
        live = f"{r['lv']:+.3f}R n={r['n']}" if r["n"] else "demo/0"
        print(f"  {r['bk'][:16]:<16} {r['s']:<18} bt {r['bt']:+.3f}R | live {live}")


if __name__ == "__main__":
    main()
