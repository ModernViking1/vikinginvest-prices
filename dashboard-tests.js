// dashboard-tests.js — lazy-loaded test harnesses for the Viking Invest
// Trading dashboard. Contents extracted from Viking_Invest_Trading_v*.html
// to slim the initial mobile load. Loaded on first invocation of any
// `runXxxTest()` button or any `compareXxx()` console call via the
// _ensureBacktestTestsLoaded() stub in the main HTML.
//
// All functions in this file assume the main dashboard globals are
// already defined: MKTS, HISTORY, DEEP_HIST, calcRecentBacktest,
// computeAllRecentBacktestsAsync, _loadRecentBtCache, _saveRecentBtCache,
// _btProfileFor, _btMethodFor, _classOf, RULES_FINGERPRINT, etc.
// They run in the page's main JS realm — same global scope as the HTML's
// inline scripts — so direct references resolve normally.

// ── COUNTER-BAR RULE DRY-RUN ───────────────────────────────────
// Run calcRecentBacktest twice per pair (current shipped rule vs a
// proposed variant) without touching the localStorage cache, then
// log a per-pair and per-class comparison. Used to validate a
// rule change against the production walker BEFORE shipping it —
// per the 2026-06-11dd revert lesson where the standalone Python
// script mispredicted production by ~5pp.
//
// Default scoping: NEW (wick-budget at 0.5) on FX (major + minor)
// and crypto only; comm + index stay on the shipped any-body
// rule. Pass {wickRatio, classes} to override the wick ratio or
// the classes that receive the new rule.
//
// Usage from devtools console after DEEP_HIST has loaded:
//   compareCounterBarRules()                          // default scoping
//   compareCounterBarRules({wickRatio:0.25})          // tighter
//   compareCounterBarRules({classes:['major','minor']}) // FX-only
//
// Output is a tidy table per pair, per-class aggregates, and the
// projected overall WR under the variant. No localStorage writes.
function compareCounterBarRules(opts){
  opts = opts || {};
  var wickRatio = (typeof opts.wickRatio === 'number') ? opts.wickRatio : 0.5;
  var classes = opts.classes || ['major','minor','crypto'];
  var mode = opts.mode || 'auto-ew';
  // variant: 'wick-budget' (default) → wick-budget at wickRatio
  //          'momentum-disabled'    → short-circuit the 2-bar momentum
  //                                   check entirely
  //          'buffer'               → skip momentum check while
  //                                   currentIdx - fromIdx < bufferBars
  //                                   (settling window after creator)
  //          'close-beyond'         → keep momentum check but require
  //                                   the second counter bar to close
  //                                   beyond the fromIdx bar's extreme
  var variant = opts.variant || 'wick-budget';
  var bufferBars = (typeof opts.bufferBars === 'number') ? opts.bufferBars : 3;
  // 'momentum-disabled' / 'buffer' / 'close-beyond' are conceptually
  // global rule changes (not class-scoped). Default `classes` to all
  // five so the per-class table tells the whole story. Pass a
  // narrower `classes` opt if you want to scope explicitly.
  var globalVariants = {'momentum-disabled': true, 'buffer': true, 'close-beyond': true};
  if(globalVariants[variant] && !opts.classes){
    classes = ['major','minor','crypto','comm','index'];
  }
  var classSet = {};
  classes.forEach(function(c){ classSet[c] = true; });
  // Build the override the engine will read on each NEW-rule pass.
  var newOverride;
  if(variant === 'momentum-disabled')      newOverride = {mode: 'disabled'};
  else if(variant === 'buffer')            newOverride = {mode: 'buffer', bufferBars: bufferBars};
  else if(variant === 'close-beyond')      newOverride = {mode: 'close-beyond'};
  else                                     newOverride = {wickRatio: wickRatio};

  console.log('%c[compareCounterBarRules] running…', 'color:#c8860a;font-weight:700;',
              {variant: variant, wickRatio: wickRatio, bufferBars: bufferBars,
               classes: classes, mode: mode});
  console.log('  This runs calcRecentBacktest twice per pair (~20-40s on');
  console.log('  mobile, 5-10s desktop). No cache writes.');

  function statsFor(rb, k){
    if(!rb) return null;
    var profile = (typeof _btProfileFor === 'function') ? _btProfileFor(k) : {method: 'wick'};
    var w, l;
    if(profile.method === 'fib' && rb.hybrid){
      w = rb.hybrid.midWins || 0;
      l = rb.hybrid.midLosses || 0;
    } else {
      w = rb.wins || 0;
      l = rb.losses || 0;
    }
    var n = w + l;
    return {w: w, l: l, n: n, wr: n > 0 ? (w / n) * 100 : null,
            method: profile.method};
  }

  var rows = [];
  var byClass = {};
  var tStart = Date.now();
  Object.keys(MKTS).sort().forEach(function(k){
    if(k === 'dxy') return;
    var m = MKTS[k];
    var cls = (m && m.t) || '?';
    // Skip pairs without enough deep history — same gate calcRecentBacktest applies
    if(!(typeof DEEP_HIST !== 'undefined' && DEEP_HIST[k]
        && DEEP_HIST[k].m15 && DEEP_HIST[k].m15.length >= 1000
        && DEEP_HIST[k].h1 && DEEP_HIST[k].h1.length >= 200)){
      return;
    }
    var oldRb = null, newRb = null;
    try {
      window.__counterBarOverride = null;
      oldRb = calcRecentBacktest(k, mode);
    } catch(e){ console.warn(k, 'OLD walker errored', e && e.message); }
    try {
      window.__counterBarOverride = classSet[cls] ? newOverride : null;
      newRb = calcRecentBacktest(k, mode);
    } catch(e){ console.warn(k, 'NEW walker errored', e && e.message); }
    window.__counterBarOverride = null;

    var oldS = statsFor(oldRb, k);
    var newS = statsFor(newRb, k);
    if(!oldS || !newS) return;
    var ruleApplied = classSet[cls] ? 'NEW' : 'OLD-kept';
    rows.push({pair: k, cls: cls, rule: ruleApplied,
               method: oldS.method,
               old_w: oldS.w, old_l: oldS.l, old_wr: oldS.wr,
               new_w: newS.w, new_l: newS.l, new_wr: newS.wr,
               delta: (oldS.wr != null && newS.wr != null)
                      ? (newS.wr - oldS.wr) : null});
    if(!byClass[cls]) byClass[cls] = {old_w:0,old_l:0,new_w:0,new_l:0,pairs:0};
    var b = byClass[cls];
    b.old_w += oldS.w; b.old_l += oldS.l;
    b.new_w += newS.w; b.new_l += newS.l;
    b.pairs += 1;
  });

  var tElapsed = ((Date.now() - tStart) / 1000).toFixed(1);
  console.log('%cDone in ' + tElapsed + 's. Pairs included: ' + rows.length,
              'color:#1a7a4a;font-weight:700;');

  // Per-pair table
  console.table(rows.map(function(r){
    return {
      pair: r.pair, class: r.cls, method: r.method, rule: r.rule,
      old: r.old_w + 'W/' + r.old_l + 'L',
      old_WR: (r.old_wr != null) ? r.old_wr.toFixed(1) + '%' : '—',
      new: r.new_w + 'W/' + r.new_l + 'L',
      new_WR: (r.new_wr != null) ? r.new_wr.toFixed(1) + '%' : '—',
      delta_pp: (r.delta != null) ? (r.delta >= 0 ? '+' : '') + r.delta.toFixed(1) : '—'
    };
  }));

  // Per-class aggregates
  console.log('%c── PER-CLASS AGGREGATES ──', 'color:#c8860a;font-weight:700;');
  var classRows = [];
  Object.keys(byClass).sort().forEach(function(c){
    var b = byClass[c];
    var oldD = b.old_w + b.old_l;
    var newD = b.new_w + b.new_l;
    var oldWr = oldD > 0 ? (b.old_w / oldD) * 100 : null;
    var newWr = newD > 0 ? (b.new_w / newD) * 100 : null;
    classRows.push({
      class: c, pairs: b.pairs, rule: classSet[c] ? 'NEW' : 'OLD-kept',
      old: b.old_w + 'W/' + b.old_l + 'L',
      old_WR: (oldWr != null) ? oldWr.toFixed(1) + '%' : '—',
      new: b.new_w + 'W/' + b.new_l + 'L',
      new_WR: (newWr != null) ? newWr.toFixed(1) + '%' : '—',
      delta_pp: (oldWr != null && newWr != null) ? ((newWr - oldWr) >= 0 ? '+' : '') + (newWr - oldWr).toFixed(1) : '—'
    });
  });
  console.table(classRows);

  // Overall projection
  var totals = Object.keys(byClass).reduce(function(acc, c){
    var b = byClass[c];
    acc.old_w += b.old_w; acc.old_l += b.old_l;
    acc.new_w += b.new_w; acc.new_l += b.new_l;
    return acc;
  }, {old_w:0, old_l:0, new_w:0, new_l:0});
  var oldOverall = totals.old_w / Math.max(1, totals.old_w + totals.old_l) * 100;
  var newOverall = totals.new_w / Math.max(1, totals.new_w + totals.new_l) * 100;
  var deltaOverall = newOverall - oldOverall;
  var color = (newOverall >= 70) ? '#1a7a4a' : '#c0281a';
  console.log('%c── PROJECTED OVERALL ──', 'color:#c8860a;font-weight:700;');
  console.log('  OLD (current shipped):  ' + totals.old_w + 'W/' + totals.old_l + 'L  WR = ' + oldOverall.toFixed(2) + '%');
  console.log('%c  SCOPED proposal:        ' + totals.new_w + 'W/' + totals.new_l + 'L  WR = ' + newOverall.toFixed(2) + '%  Δ = ' + (deltaOverall >= 0 ? '+' : '') + deltaOverall.toFixed(2) + 'pp',
              'color:' + color + ';font-weight:700;');
  if(newOverall < 70){
    console.log('%c  ⚠ Projected overall below 70% floor — do not deploy under this scoping.',
                'color:#c0281a;font-weight:700;');
  } else {
    console.log('%c  ✓ Projected overall above 70% floor — safe to deploy under this scoping (still monitor).',
                'color:#1a7a4a;font-weight:700;');
  }
  return {byClass: byClass, totals: totals, oldOverall: oldOverall,
          newOverall: newOverall, delta: deltaOverall, rows: rows,
          variant: variant, wickRatio: wickRatio, bufferBars: bufferBars,
          classes: classes};
}
// Expose to window so the user can call it from devtools console
if(typeof window !== 'undefined'){ window.compareCounterBarRules = compareCounterBarRules; }

// In-page wrapper for the "🧪 TEST COUNTER-BAR RULE" button on the
// Backtest tab. Mobile Safari has no devtools console, so the result
// is rendered into #btCounterTestResult instead of console.table.
// Uses the default SCOPED config (FX + crypto on wick-budget,
// comm + index keep current rule) — matches what compareCounterBarRules
// runs with no args.
function runCounterBarRuleTest(opts){
  var btn = document.getElementById('btCounterTestBtn');
  var statusEl = document.getElementById('btCounterTestStatus');
  var resultEl = document.getElementById('btCounterTestResult');
  var ratioSel = document.getElementById('btCounterTestRatio');
  if(!resultEl) return;
  if(typeof compareCounterBarRules !== 'function'){
    if(statusEl) statusEl.textContent = 'Engine not loaded — reload the page.';
    return;
  }
  // Read the variant dropdown so the in-page button stays useful when
  // we want to sweep ratios / disabled / buffer / close-beyond without
  // dropping into devtools. Caller-supplied opts (programmatic) override
  // the dropdown.
  if(!opts) opts = {};
  if(ratioSel && opts.variant == null && opts.wickRatio == null){
    var raw = ratioSel.value;
    if(raw === 'momentum-disabled'){
      opts.variant = 'momentum-disabled';
    } else if(raw === 'close-beyond'){
      opts.variant = 'close-beyond';
    } else if(raw && raw.indexOf('buffer-') === 0){
      opts.variant = 'buffer';
      var n = parseInt(raw.slice('buffer-'.length), 10);
      if(isFinite(n) && n > 0) opts.bufferBars = n;
    } else {
      var parsed = parseFloat(raw);
      if(isFinite(parsed) && parsed > 0) opts.wickRatio = parsed;
    }
  }
  // Deep-history gate — the function silently skips pairs without
  // enough deep data, but on a fresh session it's worth being explicit.
  if(typeof DEEP_HIST === 'undefined' || Object.keys(DEEP_HIST).length === 0){
    if(statusEl) statusEl.textContent = '⏳ Deep history still loading — try again in a few seconds.';
    return;
  }
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🧪 RUNNING…'; }
  var statusRatio = (opts && opts.wickRatio != null) ? ' (wick≤body×' + opts.wickRatio + ')' : '';
  if(statusEl) statusEl.textContent = 'Running both walkers per pair' + statusRatio + '…';
  resultEl.style.display = 'block';
  resultEl.innerHTML = '<div style="color:var(--inkd);font-style:italic;">⏳ Computing — '
                     + 'this runs calcRecentBacktest twice on every pair '
                     + 'and can take 10-40 seconds depending on device. '
                     + 'No production data is touched.</div>';
  // Defer to next tick so the browser repaints the running state.
  setTimeout(function(){
    var out;
    try { out = compareCounterBarRules(opts || {}); }
    catch(e){
      resultEl.innerHTML = '<div style="color:var(--bear);font-weight:700;">❌ Test errored: '
                         + (e && e.message ? e.message : String(e)) + '</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🧪 TEST COUNTER-BAR RULE'; }
      if(statusEl) statusEl.textContent = 'Errored — see panel.';
      return;
    }
    if(!out){
      resultEl.innerHTML = '<div style="color:var(--bear);">Test returned no data.</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🧪 TEST COUNTER-BAR RULE'; }
      if(statusEl) statusEl.textContent = 'No data.';
      return;
    }
    // Render the comparison
    var safe = (out.newOverall >= 70);
    var color = safe ? '#1a7a4a' : '#c0281a';
    var verdict = safe
      ? '✓ Projected overall ≥ 70% — safe to deploy under this scoping (still monitor after recompute).'
      : '⚠ Projected overall below 70% — do not deploy this scoping. Try a different wickRatio or class set.';
    var classRows = '';
    Object.keys(out.byClass).sort().forEach(function(c){
      var b = out.byClass[c];
      var oldD = b.old_w + b.old_l;
      var newD = b.new_w + b.new_l;
      var oldWr = oldD > 0 ? (b.old_w / oldD * 100) : null;
      var newWr = newD > 0 ? (b.new_w / newD * 100) : null;
      var delta = (oldWr != null && newWr != null) ? (newWr - oldWr) : null;
      var deltaStr = (delta == null) ? '—'
                   : (delta >= 0 ? '+' : '') + delta.toFixed(1) + 'pp';
      var deltaCol = (delta == null) ? 'var(--inkd)'
                   : (delta >= 0 ? 'var(--bull)' : 'var(--bear)');
      var ruleApplied = b.old_w === b.new_w && b.old_l === b.new_l
                       ? 'OLD-kept' : 'NEW';
      classRows += '<tr>'
        + '<td style="padding:3px 6px;">'+c+'</td>'
        + '<td style="padding:3px 6px;color:var(--inkd);">'+b.pairs+' pairs</td>'
        + '<td style="padding:3px 6px;color:'+(ruleApplied==='NEW'?'var(--purple,#6a5cb8)':'var(--inkd)')+';">'+ruleApplied+'</td>'
        + '<td style="padding:3px 6px;text-align:right;">'+b.old_w+'W/'+b.old_l+'L</td>'
        + '<td style="padding:3px 6px;text-align:right;">'+(oldWr!=null?oldWr.toFixed(1)+'%':'—')+'</td>'
        + '<td style="padding:3px 6px;text-align:right;">'+b.new_w+'W/'+b.new_l+'L</td>'
        + '<td style="padding:3px 6px;text-align:right;">'+(newWr!=null?newWr.toFixed(1)+'%':'—')+'</td>'
        + '<td style="padding:3px 6px;text-align:right;color:'+deltaCol+';font-weight:700;">'+deltaStr+'</td>'
        + '</tr>';
    });
    // Spot-check rows for the index pairs the user explicitly cares about
    var watchRows = '';
    ['de40','dj30','xagusd','xauusd'].forEach(function(k){
      var r = out.rows.filter(function(x){return x.pair===k;})[0];
      if(!r) return;
      var d = (r.delta!=null)
            ? (r.delta>=0?'+':'')+r.delta.toFixed(1)+'pp' : '—';
      var dCol = (r.delta==null) ? 'var(--inkd)'
               : (r.delta>=0 ? 'var(--bull)' : 'var(--bear)');
      watchRows += '<tr>'
        + '<td style="padding:3px 6px;">'+r.pair+'</td>'
        + '<td style="padding:3px 6px;color:'+(r.rule==='NEW'?'var(--purple,#6a5cb8)':'var(--inkd)')+';">'+r.rule+'</td>'
        + '<td style="padding:3px 6px;text-align:right;">'+(r.old_wr!=null?r.old_wr.toFixed(1)+'%':'—')+' ('+r.old_w+'W/'+r.old_l+'L)</td>'
        + '<td style="padding:3px 6px;text-align:right;">'+(r.new_wr!=null?r.new_wr.toFixed(1)+'%':'—')+' ('+r.new_w+'W/'+r.new_l+'L)</td>'
        + '<td style="padding:3px 6px;text-align:right;color:'+dCol+';font-weight:700;">'+d+'</td>'
        + '</tr>';
    });
    var outVariant = out && out.variant;
    var isGlobal = (outVariant === 'momentum-disabled' || outVariant === 'buffer' || outVariant === 'close-beyond');
    var variantLbl;
    if(outVariant === 'momentum-disabled'){
      variantLbl = 'momentum-disabled (rely on opposing CHoCH only)';
    } else if(outVariant === 'buffer'){
      var bb = (out && out.bufferBars) || 3;
      variantLbl = 'Opt 1 · ' + bb + '-bar settling buffer';
    } else if(outVariant === 'close-beyond'){
      variantLbl = 'Opt 2 · close beyond entry required';
    } else {
      variantLbl = 'wick≤body×' + ((opts && opts.wickRatio != null) ? opts.wickRatio : 0.5);
    }
    var scopeLbl = isGlobal
      ? 'all classes — the rule is either on or off, can\'t be class-scoped'
      : '(FX+crypto NEW, comm+index OLD-kept)';
    var sanityNote;
    if(outVariant === 'momentum-disabled'){
      sanityNote = 'Every class runs NEW (with the 2-bar momentum check switched off, leaving only the opposing-CHoCH structural close-through). Watch-list shows DE40 / DJ30 / XAG / XAU under both rules so the impact on the cohort that flagged this work is visible.';
    } else if(outVariant === 'buffer'){
      var bb2 = (out && out.bufferBars) || 3;
      sanityNote = 'Every class runs NEW. The 2-bar momentum check is suppressed while the current bar is fewer than ' + bb2 + ' bars past fromIdx (creator or trigger) — a "settling window" that ignores lingering breakdown volatility. After ' + bb2 + ' bars the momentum check resumes exactly as in production.';
    } else if(outVariant === 'close-beyond'){
      sanityNote = 'Every class runs NEW. The 2-bar momentum check still fires, but the SECOND counter bar must close past the fromIdx bar\'s wick extreme (entry for pre-trigger calls, trigger high/low post-trigger) — i.e. price must have actually retraced back into the trade zone before the rule invalidates.';
    } else {
      sanityNote = 'Compares the shipped any-body rule (OLD) against the ' + variantLbl + ' budget (NEW) on FX + crypto pairs only. Comm + index pairs run OLD on both passes — their WR is identical and acts as a sanity check.';
    }
    resultEl.innerHTML =
      '<div style="font-family:Orbitron,monospace;font-size:8.5px;color:var(--purple,#6a5cb8);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:6px;">🧪 Counter-bar rule dry-run · ' + variantLbl + ' · ' + scopeLbl + '</div>'
      + '<div style="margin-bottom:6px;color:var(--inkd);font-size:8.5px;">' + sanityNote + '</div>'
      + '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;margin-top:4px;">'
      + '<thead><tr style="border-bottom:1px solid rgba(0,0,0,0.1);font-size:8.5px;color:var(--inkd);">'
      + '<th style="padding:3px 6px;text-align:left;">class</th>'
      + '<th style="padding:3px 6px;text-align:left;"></th>'
      + '<th style="padding:3px 6px;text-align:left;">rule</th>'
      + '<th style="padding:3px 6px;text-align:right;">OLD W/L</th>'
      + '<th style="padding:3px 6px;text-align:right;">OLD WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">NEW W/L</th>'
      + '<th style="padding:3px 6px;text-align:right;">NEW WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">Δ</th>'
      + '</tr></thead><tbody>'
      + classRows
      + '</tbody></table></div>'
      + '<div style="font-family:Orbitron,monospace;font-size:8.5px;color:var(--purple,#6a5cb8);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-top:10px;margin-bottom:4px;">Watch-list pairs (DE40 / DJ30 / XAG / XAU)</div>'
      + '<div style="color:var(--inkd);font-size:8.5px;margin-bottom:4px;">' + (isGlobal
          ? 'Each pair runs OLD vs NEW directly. Big NEW WR drops on DE40 / DJ30 / XAG / XAU would replay the 11dd regression — read the deltas before deciding.'
          : 'Must be marked OLD-kept on this scoping — verifies the comm + index regression doesn\'t reappear.') + '</div>'
      + '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
      + '<thead><tr style="border-bottom:1px solid rgba(0,0,0,0.1);font-size:8.5px;color:var(--inkd);">'
      + '<th style="padding:3px 6px;text-align:left;">pair</th>'
      + '<th style="padding:3px 6px;text-align:left;">rule</th>'
      + '<th style="padding:3px 6px;text-align:right;">OLD WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">NEW WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">Δ</th>'
      + '</tr></thead><tbody>'
      + watchRows
      + '</tbody></table></div>'
      + '<div style="margin-top:12px;padding:8px 10px;border-radius:3px;background:'+(safe?'rgba(26,122,74,0.07)':'rgba(192,40,26,0.07)')+';border:1px solid '+(safe?'rgba(26,122,74,0.3)':'rgba(192,40,26,0.3)')+';">'
      + '  <div style="font-family:Orbitron,monospace;font-size:9px;font-weight:700;letter-spacing:0.4px;text-transform:uppercase;color:var(--inkd);margin-bottom:4px;">Projected overall</div>'
      + '  <div style="font-size:11px;">'
      + '    <span style="color:var(--inkd);">OLD shipped:</span> '
      + '    <strong>'+out.oldOverall.toFixed(2)+'%</strong>'
      + '    <span style="color:var(--inkd);margin-left:4px;">('+out.totals.old_w+'W/'+out.totals.old_l+'L)</span>'
      + '  </div>'
      + '  <div style="font-size:13px;color:'+color+';font-weight:700;margin-top:2px;">'
      + '    SCOPED proposal: '+out.newOverall.toFixed(2)+'%'
      + '    <span style="font-size:10px;margin-left:6px;">('+out.totals.new_w+'W/'+out.totals.new_l+'L &middot; Δ '+(out.delta>=0?'+':'')+out.delta.toFixed(2)+'pp)</span>'
      + '  </div>'
      + '  <div style="margin-top:6px;color:'+color+';font-weight:700;font-size:9.5px;">'+verdict+'</div>'
      + '</div>';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🧪 TEST COUNTER-BAR RULE'; }
    if(statusEl) statusEl.textContent = safe ? 'Done — projection is ≥70%.' : 'Done — projection BELOW 70%.';
  }, 50);
}
if(typeof window !== 'undefined'){ window.runCounterBarRuleTest = runCounterBarRuleTest; }

// ── SR GATING DRY-RUN (DE40 / DJ30) ───────────────────────────
// User asked to evaluate four variants for DE40 + DJ30 specifically,
// since those two pairs underperform on the dashboard's aggregate
// (DE40 50.0% → 41.7% on the 11ee recompute). The variants:
//
//   sr-window-only   keep only trades whose creator timestamp falls
//                    INSIDE the SR window (07:00–09:00 UTC DE40,
//                    13:30–15:30 UTC DJ30 — 8 bars × 15m each).
//                    Other trades dropped.
//
//   sr-broken-only   keep only trades whose creator timestamp had
//                    an SR break already in state (bull_broken /
//                    bear_broken). Pending and failed states drop.
//
//   sr-tier-only     keep only trades whose SR tier at creator time
//                    was 5/5 (4/4 confluence + SR break aligned) or
//                    4/5 (3/4 confluence + SR break aligned). Drop
//                    3/5 (2/4 + SR) and everything else.
//
//   sr-no-44         IGNORE 4/4 alignment; fire a fresh setup on
//                    every SR break (bull_broken or bear_broken)
//                    with entry at the break candle's close, stop
//                    at the opposite SR boundary, target at 1:1 R:R.
//                    Walk forward to determine outcome.
//
// Outputs per-variant W/L per pair plus aggregate. Decision rule:
// pick the variant that improves DE40 AND DJ30 WR materially while
// not tanking the overall 73-74% dashboard aggregate (we project
// the aggregate by replacing DE40/DJ30 totals with the filtered
// counts and recomputing).

function _computeHistoricalSRState(bars, pairKey){
  // Walk m15 bars and return, per bar, the active SR state from the
  // most recent ref candle (if within windowBars), or null when
  // outside any session. Mirrors _findRefCandle + _computeSRState
  // for one-pass historical evaluation.
  var spec = SR_REF_TIMES[pairKey];
  if(!spec || !bars || bars.length === 0) return null;
  var n = bars.length;
  var out = new Array(n);
  var ref = null;
  var state = 'pending';
  for(var i = 0; i < n; i++){
    var b = bars[i];
    var ts = b && b.t;
    if(!ts || ts.length < 16){ out[i] = null; continue; }
    var hhmm = ts.substr(11, 5);
    var date = ts.substr(0, 10);
    // Open of a new session?
    if(spec.openTimes.indexOf(hhmm) !== -1){
      ref = {
        idx: i,
        refHigh: b.h != null ? b.h : b.p,
        refLow:  b.l != null ? b.l : b.p,
        date: date,
        openTime: hhmm
      };
      state = 'pending';
    }
    // Within the active 2h SR window?
    var inWindow = ref && (i - ref.idx) <= spec.windowBars
                   && ref.date === date;
    if(!ref || !inWindow){ out[i] = null; continue; }
    var c = b.c != null ? b.c : b.p;
    if(c != null){
      var hi = Math.max(ref.refHigh, ref.refLow);
      var lo = Math.min(ref.refHigh, ref.refLow);
      if(state === 'pending'){
        if(c > hi) state = 'bull_broken';
        else if(c < lo) state = 'bear_broken';
      } else if(state === 'bull_broken'){
        if(c <= hi) state = 'bull_failed';
      } else if(state === 'bear_broken'){
        if(c >= lo) state = 'bear_failed';
      }
    }
    out[i] = {
      refIdx: ref.idx,
      refHigh: ref.refHigh,
      refLow: ref.refLow,
      state: state,
      barsSinceRef: i - ref.idx,
      sessionDate: ref.date
    };
  }
  return out;
}

function _walkSRNo44(bars, pairKey){
  // For variant 'sr-no-44': fire on every SR break regardless of
  // 4/4 alignment. Entry at break candle close, stop at opposite
  // SR boundary, target at 1:1 R:R. Walk forward up to 16 bars
  // (matches TRIGGERED_MAX_BARS) to resolve.
  var spec = SR_REF_TIMES[pairKey];
  if(!spec || !bars || bars.length === 0) return [];
  var n = bars.length;
  var trades = [];
  var ref = null;
  var brokenAlready = null;  // track to avoid double-firing the same session
  for(var i = 0; i < n; i++){
    var b = bars[i];
    var ts = b && b.t;
    if(!ts || ts.length < 16) continue;
    var hhmm = ts.substr(11, 5);
    var date = ts.substr(0, 10);
    if(spec.openTimes.indexOf(hhmm) !== -1){
      ref = {idx: i, refHigh: b.h, refLow: b.l, date: date};
      brokenAlready = null;
    }
    var inWindow = ref && (i - ref.idx) <= spec.windowBars
                   && ref.date === date;
    if(!ref || !inWindow) continue;
    var c = b.c;
    if(c == null) continue;
    var hi = Math.max(ref.refHigh, ref.refLow);
    var lo = Math.min(ref.refHigh, ref.refLow);
    var dir = null;
    if(brokenAlready == null){
      if(c > hi) dir = 'bull';
      else if(c < lo) dir = 'bear';
    }
    if(!dir) continue;
    brokenAlready = dir;
    var entry = c;
    var stop = (dir === 'bull') ? lo : hi;
    if(dir === 'bull' && entry <= stop) continue;
    if(dir === 'bear' && entry >= stop) continue;
    var R = (dir === 'bull') ? (entry - stop) : (stop - entry);
    var target = (dir === 'bull') ? (entry + R) : (entry - R);
    // Forward walk
    var outcome = 'expired';
    for(var j = i + 1; j < Math.min(n, i + 17); j++){
      var bb = bars[j];
      if(dir === 'bull'){
        if(bb.l != null && bb.l <= stop){ outcome = 'loss'; break; }
        if(bb.h != null && bb.h >= target){ outcome = 'win'; break; }
      } else {
        if(bb.h != null && bb.h >= stop){ outcome = 'loss'; break; }
        if(bb.l != null && bb.l <= target){ outcome = 'win'; break; }
      }
    }
    if(outcome === 'win' || outcome === 'loss'){
      trades.push({outcome: outcome, dir: dir, entry: entry, stop: stop,
                   target: target, R: R, creatorTs: ts});
    }
  }
  return trades;
}

// Variant 'sr-orb-failure' — research-paper "ORB failure" reversal.
// Most opening-range breakouts fail; the higher-edge play is to
// fade the failed breakout when price closes BACK INSIDE the SR
// range within N bars of the break. Bull-break that fails →
// fade short. Bear-break that fails → fade long.
// Entry at the close-back-inside bar's close.
// Stop at the failed-break extreme (highest high since break for
// bull-failed, lowest low for bear-failed).
// Target at 1:1 R:R for parity with other variants.
//
// Sources: tradertom.com Dax/Dow open strategy, daytradingtoolkit
// first-15-minutes pro guide — both flag the failure cohort as the
// main edge once you accept ~70% of breakouts roll over.
function _walkSRFailure(bars, pairKey){
  var spec = SR_REF_TIMES[pairKey];
  if(!spec || !bars || bars.length === 0) return [];
  var n = bars.length;
  var trades = [];
  var ref = null;
  var breakSt = null;  // {dir, breakIdx, extreme}
  for(var i = 0; i < n; i++){
    var b = bars[i];
    var ts = b && b.t;
    if(!ts || ts.length < 16) continue;
    var hhmm = ts.substr(11, 5);
    var date = ts.substr(0, 10);
    if(spec.openTimes.indexOf(hhmm) !== -1){
      ref = {idx: i, refHigh: b.h, refLow: b.l, date: date};
      breakSt = null;
    }
    var inWindow = ref && (i - ref.idx) <= spec.windowBars
                   && ref.date === date;
    if(!ref || !inWindow) continue;
    var c = b.c;
    if(c == null) continue;
    var hi = Math.max(ref.refHigh, ref.refLow);
    var lo = Math.min(ref.refHigh, ref.refLow);
    // First leg — detect the breakout
    if(!breakSt){
      if(c > hi) breakSt = {dir: 'bull', breakIdx: i, extreme: (b.h != null ? b.h : c)};
      else if(c < lo) breakSt = {dir: 'bear', breakIdx: i, extreme: (b.l != null ? b.l : c)};
      continue;
    }
    // Track the extreme since the break
    if(breakSt.dir === 'bull' && b.h != null && b.h > breakSt.extreme) breakSt.extreme = b.h;
    if(breakSt.dir === 'bear' && b.l != null && b.l < breakSt.extreme) breakSt.extreme = b.l;
    // Watch up to 8 bars for a failure (research suggests fast
    // failure is the strongest edge; slow drift back doesn't have
    // the trapped-trader liquidity).
    var failed = false;
    if(breakSt.dir === 'bull' && c <= hi) failed = true;
    else if(breakSt.dir === 'bear' && c >= lo) failed = true;
    if(!failed){
      if(i - breakSt.breakIdx > 8) breakSt = null;
      continue;
    }
    // Fade the failed break
    var dir = (breakSt.dir === 'bull') ? 'bear' : 'bull';
    var entry = c;
    var stop = breakSt.extreme;
    if(dir === 'bull' && stop >= entry){ breakSt = null; continue; }
    if(dir === 'bear' && stop <= entry){ breakSt = null; continue; }
    var R = Math.abs(stop - entry);
    if(R <= 0){ breakSt = null; continue; }
    var target = (dir === 'bull') ? (entry + R) : (entry - R);
    var outcome = 'expired';
    for(var j = i + 1; j < Math.min(n, i + 17); j++){
      var bb = bars[j];
      if(dir === 'bull'){
        if(bb.l != null && bb.l <= stop){ outcome = 'loss'; break; }
        if(bb.h != null && bb.h >= target){ outcome = 'win'; break; }
      } else {
        if(bb.h != null && bb.h >= stop){ outcome = 'loss'; break; }
        if(bb.l != null && bb.l <= target){ outcome = 'win'; break; }
      }
    }
    if(outcome === 'win' || outcome === 'loss'){
      trades.push({outcome: outcome, dir: dir, entry: entry, stop: stop,
                   target: target, R: R, creatorTs: ts});
    }
    breakSt = null;  // one failure-fade per session
  }
  return trades;
}

// Variant 'sr-orb-strong' — Quantified Strategies / tradersmastermind
// ORB with strict break-strength filter. Same as sr-no-44 but the
// breakout candle must be a CONVINCING break: body ≥ 60% of total
// range AND body magnitude ≥ 1.5 × ATR(20). This drops the wicky
// fake-breakout cohort that pulled sr-no-44 down to 41-54%.
function _walkSROrbStrong(bars, pairKey){
  var spec = SR_REF_TIMES[pairKey];
  if(!spec || !bars || bars.length === 0) return [];
  var BODY_RATIO_MIN = 0.6;
  var ATR_MULT_MIN = 1.5;
  var n = bars.length;
  var trades = [];
  var ref = null;
  var brokenAlready = null;
  for(var i = 0; i < n; i++){
    var b = bars[i];
    var ts = b && b.t;
    if(!ts || ts.length < 16) continue;
    var hhmm = ts.substr(11, 5);
    var date = ts.substr(0, 10);
    if(spec.openTimes.indexOf(hhmm) !== -1){
      ref = {idx: i, refHigh: b.h, refLow: b.l, date: date};
      brokenAlready = null;
    }
    var inWindow = ref && (i - ref.idx) <= spec.windowBars
                   && ref.date === date;
    if(!ref || !inWindow) continue;
    var c = b.c, o = b.o, h = b.h, l = b.l;
    if(c == null || o == null || h == null || l == null) continue;
    var hi = Math.max(ref.refHigh, ref.refLow);
    var lo = Math.min(ref.refHigh, ref.refLow);
    var dir = null;
    if(brokenAlready == null){
      if(c > hi) dir = 'bull';
      else if(c < lo) dir = 'bear';
    }
    if(!dir) continue;
    // STRENGTH FILTER #1 — body fills ≥ 60% of bar's range. Filters
    // out the wicky doji-style closes that count as a break under
    // sr-no-44 but rarely follow through.
    var body = Math.abs(c - o);
    var rng = h - l;
    if(rng <= 0) continue;
    if((body / rng) < BODY_RATIO_MIN) continue;
    // STRENGTH FILTER #2 — body magnitude ≥ 1.5 × ATR(20). Filters
    // out the small-body grind through the SR boundary; we want a
    // decisive expansion candle.
    var atrStart = Math.max(0, i - 20);
    var atrSum = 0, atrCount = 0;
    for(var ai = atrStart; ai < i; ai++){
      var ab = bars[ai];
      if(ab && ab.h != null && ab.l != null){
        atrSum += Math.max(0, ab.h - ab.l);
        atrCount++;
      }
    }
    var atr20 = atrCount > 0 ? atrSum / atrCount : 0;
    if(atr20 > 0 && body < ATR_MULT_MIN * atr20) continue;
    brokenAlready = dir;
    var entry = c;
    var stop = (dir === 'bull') ? lo : hi;
    if(dir === 'bull' && entry <= stop) continue;
    if(dir === 'bear' && entry >= stop) continue;
    var R = (dir === 'bull') ? (entry - stop) : (stop - entry);
    var target = (dir === 'bull') ? (entry + R) : (entry - R);
    var outcome = 'expired';
    for(var j = i + 1; j < Math.min(n, i + 17); j++){
      var bb = bars[j];
      if(dir === 'bull'){
        if(bb.l != null && bb.l <= stop){ outcome = 'loss'; break; }
        if(bb.h != null && bb.h >= target){ outcome = 'win'; break; }
      } else {
        if(bb.h != null && bb.h >= stop){ outcome = 'loss'; break; }
        if(bb.l != null && bb.l <= target){ outcome = 'win'; break; }
      }
    }
    if(outcome === 'win' || outcome === 'loss'){
      trades.push({outcome: outcome, dir: dir, entry: entry, stop: stop,
                   target: target, R: R, creatorTs: ts});
    }
  }
  return trades;
}

// Variant 'sr-vwap-pullback' — VWAP pullback in session.
// Compute session VWAP (cumulative typical price ÷ bar count —
// volume-approximation since we don't have volume per bar). Wait
// for the morning trend to displace at least 0.5×ATR away from
// VWAP, then look for a pullback within 0.15×ATR of VWAP with a
// confirmation bar (closes back in the trend direction). Entry at
// the confirmation bar's close, stop at the confirmation bar's
// opposite extreme, target 1:1 R:R.
//
// Sources: humbledtrader, asktraders, rupeezy, fyers. VWAP works
// because institutional traders use it as a benchmark — they buy
// dips to VWAP in uptrends and sell rallies to VWAP in downtrends.
// One trade per session (the first qualifying pullback).
function _walkSRVWAPPullback(bars, pairKey){
  var spec = SR_REF_TIMES[pairKey];
  if(!spec || !bars || bars.length === 0) return [];
  var n = bars.length;
  var trades = [];
  var sessionStartIdx = -1;
  var sessionDate = null;
  var vwapSum = 0, vwapBars = 0;
  var trendDir = null;       // 'bull' / 'bear' once displacement confirms
  var trendArmed = false;    // armed for pullback after displacement
  var firedThisSession = false;
  for(var i = 0; i < n; i++){
    var b = bars[i];
    if(!b || !b.t || b.t.length < 16) continue;
    var hhmm = b.t.substr(11, 5);
    var date = b.t.substr(0, 10);
    // New session detection — at any of the spec's open times we
    // reset VWAP and trend tracking for the day.
    if(spec.openTimes.indexOf(hhmm) !== -1 && date !== sessionDate){
      sessionStartIdx = i;
      sessionDate = date;
      vwapSum = 0; vwapBars = 0;
      trendDir = null; trendArmed = false;
      firedThisSession = false;
    }
    if(sessionStartIdx < 0 || date !== sessionDate) continue;
    if(b.h == null || b.l == null || b.c == null) continue;
    // Cumulative typical-price VWAP since session open
    var typ = (b.h + b.l + b.c) / 3;
    vwapSum += typ;
    vwapBars += 1;
    var vwap = vwapSum / vwapBars;
    if(firedThisSession) continue;
    // ATR(20) at this bar — same noise-floor proxy used elsewhere
    var atrStart = Math.max(0, i - 20);
    var atrSum = 0, atrCount = 0;
    for(var ai = atrStart; ai < i; ai++){
      var ab = bars[ai];
      if(ab && ab.h != null && ab.l != null){
        atrSum += Math.max(0, ab.h - ab.l); atrCount++;
      }
    }
    var atr20 = atrCount > 0 ? atrSum / atrCount : 0;
    if(atr20 <= 0) continue;
    // Trend establishment: price must displace >= 0.5×ATR from VWAP
    // in one direction before we'll trade pullbacks. Needs at least
    // 3 bars of session to avoid the noisy first candle.
    if(!trendArmed && vwapBars >= 3){
      if((b.c - vwap) >= 0.5 * atr20){ trendDir = 'bull'; trendArmed = true; }
      else if((vwap - b.c) >= 0.5 * atr20){ trendDir = 'bear'; trendArmed = true; }
    }
    if(!trendArmed) continue;
    // Pullback detection — current bar must touch VWAP (within 0.15×ATR)
    var dist = Math.abs(b.c - vwap);
    if(dist > 0.15 * atr20) continue;
    // Confirmation rule: bar closes in trend direction. Bull trend
    // wants a bullish candle (c > o); bear trend wants bearish.
    if(b.o == null) continue;
    if(trendDir === 'bull' && b.c <= b.o) continue;
    if(trendDir === 'bear' && b.c >= b.o) continue;
    // Fire
    var entry = b.c;
    var stop = (trendDir === 'bull') ? b.l : b.h;
    if(trendDir === 'bull' && stop >= entry) continue;
    if(trendDir === 'bear' && stop <= entry) continue;
    var R = Math.abs(stop - entry);
    if(R <= 0) continue;
    var target = (trendDir === 'bull') ? (entry + R) : (entry - R);
    var outcome = 'expired';
    for(var j = i + 1; j < Math.min(n, i + 17); j++){
      var bb = bars[j];
      if(trendDir === 'bull'){
        if(bb.l != null && bb.l <= stop){ outcome = 'loss'; break; }
        if(bb.h != null && bb.h >= target){ outcome = 'win'; break; }
      } else {
        if(bb.h != null && bb.h >= stop){ outcome = 'loss'; break; }
        if(bb.l != null && bb.l <= target){ outcome = 'win'; break; }
      }
    }
    if(outcome === 'win' || outcome === 'loss'){
      trades.push({outcome: outcome, dir: trendDir, entry: entry, stop: stop,
                   target: target, R: R, creatorTs: b.t});
    }
    firedThisSession = true;
  }
  return trades;
}

// Variant 'sr-silver-bullet' — ICT Silver Bullet on 15m.
// Kill zone is the second hour of cash open (DAX 08:00-09:00 UTC,
// DJ30 14:30-15:30 UTC). During the kill zone we look for a 3-bar
// Fair Value Gap aligned with the session's prevailing direction
// (bias derived from the close-vs-open of the first session bar).
// FVG = bull when bar[i-2].h < bar[i].l (gap stays open below);
// bear when bar[i-2].l > bar[i].h. We then watch up to 6 bars for
// a retest into the FVG midpoint and fire at the retest bar's close.
// Stop at the FVG creator's opposite extreme, target 1:1 R:R.
//
// Sources: fxopen, ictpdf, innercircletrader.net, phidiaspropfirm
// — all flag the Silver Bullet as one of the higher-conviction
// intraday plays on index futures and major FX.
var SR_SILVER_BULLET_TIMES = {
  // Second open time in each spec is the production kill-zone open
  // (DST-adjusted equivalent). Kill zone runs 4 bars (= 1h on 15m).
  // de40 removed 2026-06-13kk.
  dj30: { openTime: '14:30', windowBars: 4 }
};
function _walkSRSilverBullet(bars, pairKey){
  var spec = SR_SILVER_BULLET_TIMES[pairKey];
  if(!spec || !bars || bars.length === 0) return [];
  var n = bars.length;
  var trades = [];
  var killStart = -1;
  var killDate = null;
  var sessionBias = null;
  var firedThisSession = false;
  var pendingFvg = null;  // {dir, top, bottom, creatorIdx, expireIdx}
  for(var i = 0; i < n; i++){
    var b = bars[i];
    if(!b || !b.t || b.t.length < 16) continue;
    var hhmm = b.t.substr(11, 5);
    var date = b.t.substr(0, 10);
    if(hhmm === spec.openTime && date !== killDate){
      killStart = i;
      killDate = date;
      // Session bias from the kill-zone open bar — bullish body
      // means bias up, bearish means bias down. Doji skipped.
      sessionBias = null;
      if(b.o != null && b.c != null){
        if(b.c > b.o) sessionBias = 'bull';
        else if(b.c < b.o) sessionBias = 'bear';
      }
      firedThisSession = false;
      pendingFvg = null;
    }
    var inKill = killStart >= 0 && (i - killStart) <= spec.windowBars && date === killDate;
    if(!inKill || firedThisSession || !sessionBias) continue;
    // Detect FVG at this bar using the 3-bar pattern bars[i-2], i-1, i
    if(i >= 2 && !pendingFvg){
      var b0 = bars[i - 2], b2 = bars[i];
      if(b0 && b2 && b0.h != null && b0.l != null && b2.h != null && b2.l != null){
        if(sessionBias === 'bull' && b0.h < b2.l){
          // Bull FVG — gap between bar0.high and bar2.low. Entry on
          // retest into midpoint, stop just below bar0.low (FVG
          // creator), target 1:1 R:R.
          pendingFvg = {
            dir: 'bull',
            top: b2.l, bottom: b0.h,
            creatorLow: b0.l,
            creatorIdx: i, expireIdx: i + 6,
            creatorTs: b2.t
          };
        } else if(sessionBias === 'bear' && b0.l > b2.h){
          pendingFvg = {
            dir: 'bear',
            top: b0.l, bottom: b2.h,
            creatorHigh: b0.h,
            creatorIdx: i, expireIdx: i + 6,
            creatorTs: b2.t
          };
        }
      }
    }
    // Wait for retest into the FVG
    if(pendingFvg && i > pendingFvg.creatorIdx){
      var inRetest = false;
      var mid = (pendingFvg.top + pendingFvg.bottom) / 2;
      if(pendingFvg.dir === 'bull' && b.l != null && b.l <= mid) inRetest = true;
      if(pendingFvg.dir === 'bear' && b.h != null && b.h >= mid) inRetest = true;
      if(inRetest){
        var entry = b.c;
        var stop = (pendingFvg.dir === 'bull') ? pendingFvg.creatorLow : pendingFvg.creatorHigh;
        if(entry == null || stop == null){ pendingFvg = null; continue; }
        if(pendingFvg.dir === 'bull' && stop >= entry){ pendingFvg = null; continue; }
        if(pendingFvg.dir === 'bear' && stop <= entry){ pendingFvg = null; continue; }
        var R = Math.abs(stop - entry);
        if(R <= 0){ pendingFvg = null; continue; }
        var target = (pendingFvg.dir === 'bull') ? (entry + R) : (entry - R);
        var outcome = 'expired';
        for(var j = i + 1; j < Math.min(n, i + 17); j++){
          var bb = bars[j];
          if(pendingFvg.dir === 'bull'){
            if(bb.l != null && bb.l <= stop){ outcome = 'loss'; break; }
            if(bb.h != null && bb.h >= target){ outcome = 'win'; break; }
          } else {
            if(bb.h != null && bb.h >= stop){ outcome = 'loss'; break; }
            if(bb.l != null && bb.l <= target){ outcome = 'win'; break; }
          }
        }
        if(outcome === 'win' || outcome === 'loss'){
          trades.push({outcome: outcome, dir: pendingFvg.dir, entry: entry,
                       stop: stop, target: target, R: R, creatorTs: pendingFvg.creatorTs});
        }
        firedThisSession = true;
        pendingFvg = null;
      } else if(i >= pendingFvg.expireIdx){
        pendingFvg = null;  // FVG retest window expired
      }
    }
  }
  return trades;
}

function _binSearchTsIdx(bars, target){
  // Find the last bar index whose timestamp <= target.
  if(!bars || bars.length === 0 || !target) return -1;
  var lo = 0, hi = bars.length - 1, ans = -1;
  while(lo <= hi){
    var mid = (lo + hi) >> 1;
    var t = bars[mid] && bars[mid].t;
    if(!t){ hi = mid - 1; continue; }
    if(t <= target){ ans = mid; lo = mid + 1; }
    else hi = mid - 1;
  }
  return ans;
}

function compareSRGating(opts){
  opts = opts || {};
  var variant = opts.variant || 'sr-window-only';
  var pairs = opts.pairs || ['de40', 'dj30'];
  var mode = opts.mode || 'auto-ew';

  console.log('%c[compareSRGating] running…', 'color:#c8860a;font-weight:700;',
              {variant: variant, pairs: pairs, mode: mode});

  var perPair = {};
  pairs.forEach(function(k){
    if(!(typeof DEEP_HIST !== 'undefined' && DEEP_HIST[k]
         && DEEP_HIST[k].m15 && DEEP_HIST[k].m15.length >= 1000)){
      perPair[k] = {error: 'no deep history'};
      return;
    }
    var bars = DEEP_HIST[k].m15;
    var srStates = _computeHistoricalSRState(bars, k);
    if(variant === 'sr-no-44' || variant === 'sr-orb-failure'
       || variant === 'sr-orb-strong' || variant === 'sr-vwap-pullback'
       || variant === 'sr-silver-bullet'){
      // Standalone walker — ignores 4/4 entirely. Each variant has
      // its own walker so the strategy logic is isolated:
      //   sr-no-44          basic ORB — every SR break fires
      //   sr-orb-failure    reverse on the failed-break (close back
      //                     inside the SR range within 8 bars)
      //   sr-orb-strong     ORB with body≥60%-of-range AND body
      //                     ≥ 1.5×ATR(20) strength filter
      //   sr-vwap-pullback  VWAP pullback in session — fade move
      //                     back to VWAP after 0.5×ATR displacement
      //   sr-silver-bullet  ICT Silver Bullet — 3-bar FVG inside
      //                     the 1h kill zone after cash open
      var walker;
      if(variant === 'sr-orb-failure')          walker = _walkSRFailure;
      else if(variant === 'sr-orb-strong')      walker = _walkSROrbStrong;
      else if(variant === 'sr-vwap-pullback')   walker = _walkSRVWAPPullback;
      else if(variant === 'sr-silver-bullet')   walker = _walkSRSilverBullet;
      else                                      walker = _walkSRNo44;
      var trades = walker(bars, k);
      var w = trades.filter(function(t){return t.outcome === 'win';}).length;
      var l = trades.filter(function(t){return t.outcome === 'loss';}).length;
      var n = w + l;
      perPair[k] = {
        variant: variant, w: w, l: l, n: n,
        wr: n > 0 ? (w / n) * 100 : null,
        oldW: null, oldL: null, oldWR: null,  // no comparable baseline
        trades: trades
      };
      return;
    }
    // Filter variants — take production fib trades and drop those
    // whose creator timestamp doesn't match the SR criterion.
    var rb;
    try {
      window.__counterBarOverride = null;  // ensure shipped rule
      rb = calcRecentBacktest(k, mode);
    } catch(e){ perPair[k] = {error: 'calc failed: ' + (e&&e.message)}; return; }
    if(!rb || !rb.hybrid || !rb.hybrid.midTrades){
      perPair[k] = {error: 'no midTrades — older cache or wick pair'};
      return;
    }
    var midTrades = rb.hybrid.midTrades;
    var oldW = midTrades.filter(function(t){return t.outcome === 'win';}).length;
    var oldL = midTrades.filter(function(t){return t.outcome === 'loss';}).length;
    var newW = 0, newL = 0;
    midTrades.forEach(function(t){
      var idx = _binSearchTsIdx(bars, t.creatorTs);
      if(idx < 0){ return; }
      var sr = srStates ? srStates[idx] : null;
      var keep = false;
      if(variant === 'sr-window-only'){
        keep = !!sr;  // any bar with sr state means inside window
      } else if(variant === 'sr-broken-only'){
        keep = !!sr && (sr.state === 'bull_broken' || sr.state === 'bear_broken');
      } else if(variant === 'sr-tier-only'){
        // 5/5 or 4/5 = SR aligned with macro direction. We approximate
        // by requiring sr.state matches trade dir AND keeping all kept
        // trades (which already had 4/4 confluence — that's why they
        // existed in midTrades). The 4/4 part gives 5/5 vs 4/5 split
        // but for filter purposes both pass.
        var aligned = sr && (
          (sr.state === 'bull_broken' && t.dir === 'bull') ||
          (sr.state === 'bear_broken' && t.dir === 'bear')
        );
        keep = !!aligned;
      }
      if(!keep) return;
      if(t.outcome === 'win') newW++;
      else if(t.outcome === 'loss') newL++;
    });
    perPair[k] = {
      variant: variant,
      oldW: oldW, oldL: oldL, oldWR: (oldW + oldL) > 0 ? (oldW / (oldW + oldL)) * 100 : null,
      w: newW, l: newL, n: newW + newL,
      wr: (newW + newL) > 0 ? (newW / (newW + newL)) * 100 : null
    };
  });

  // Aggregate impact on dashboard 73.94% — replace DE40 + DJ30 totals
  // with the filtered counts, recompute. Need the other pairs' W/L
  // for the denominator.
  var globalW = 0, globalL = 0;
  var dashAdjustW = 0, dashAdjustL = 0;
  if(typeof MKTS !== 'undefined'){
    Object.keys(MKTS).forEach(function(k){
      if(k === 'dxy') return;
      var m = MKTS[k];
      var bw = m.bw || 0, bl = m.bl || 0;
      globalW += bw; globalL += bl;
      if(pairs.indexOf(k) !== -1){
        dashAdjustW += bw; dashAdjustL += bl;
      }
    });
  }
  var projW = globalW - dashAdjustW, projL = globalL - dashAdjustL;
  pairs.forEach(function(k){
    var p = perPair[k];
    if(p && p.n != null){ projW += p.w; projL += p.l; }
  });
  var projOverall = (projW + projL) > 0 ? (projW / (projW + projL)) * 100 : null;
  var origOverall = (globalW + globalL) > 0 ? (globalW / (globalW + globalL)) * 100 : null;

  console.log('%c── PER-PAIR FILTERED RESULTS ──', 'color:#c8860a;font-weight:700;');
  console.table(pairs.map(function(k){
    var p = perPair[k] || {};
    return {
      pair: k, variant: variant,
      old: (p.oldW != null) ? (p.oldW + 'W/' + p.oldL + 'L') : '—',
      old_WR: (p.oldWR != null) ? p.oldWR.toFixed(1) + '%' : '—',
      'new': (p.w != null) ? (p.w + 'W/' + p.l + 'L') : (p.error || '—'),
      new_WR: (p.wr != null) ? p.wr.toFixed(1) + '%' : '—',
      delta_pp: (p.oldWR != null && p.wr != null)
        ? ((p.wr - p.oldWR) >= 0 ? '+' : '') + (p.wr - p.oldWR).toFixed(1) : '—'
    };
  }));
  console.log('Original dashboard overall: ' + (origOverall != null ? origOverall.toFixed(2) + '%' : '—'));
  console.log('Projected after filter: ' + (projOverall != null ? projOverall.toFixed(2) + '%' : '—'));
  return {variant: variant, pairs: pairs, perPair: perPair,
          origOverall: origOverall, projOverall: projOverall};
}
if(typeof window !== 'undefined'){ window.compareSRGating = compareSRGating; }

// In-page handler for the SR gating test button. Reads the variant
// dropdown and renders results into the on-page panel below the
// button (same UX as runCounterBarRuleTest — no devtools required).
function runSRGatingTest(opts){
  var btn = document.getElementById('btSRTestBtn');
  var statusEl = document.getElementById('btSRTestStatus');
  var resultEl = document.getElementById('btSRTestResult');
  var variantSel = document.getElementById('btSRTestVariant');
  if(!resultEl) return;
  if(typeof compareSRGating !== 'function'){
    if(statusEl) statusEl.textContent = 'Engine not loaded — reload the page.';
    return;
  }
  if(typeof DEEP_HIST === 'undefined' || Object.keys(DEEP_HIST).length === 0){
    if(statusEl) statusEl.textContent = '⏳ Deep history still loading — try again in a few seconds.';
    return;
  }
  if(!opts) opts = {};
  if(variantSel && !opts.variant) opts.variant = variantSel.value || 'sr-window-only';
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🏫 RUNNING…'; }
  if(statusEl) statusEl.textContent = 'Filtering DE40 + DJ30 trades…';
  resultEl.style.display = 'block';
  resultEl.innerHTML = '<div style="color:var(--inkd);font-style:italic;">⏳ Computing — '
                     + 'walks DE40 + DJ30 fib trades and filters by the SR variant. '
                     + 'No production data is touched.</div>';
  setTimeout(function(){
    var out;
    try { out = compareSRGating(opts); }
    catch(e){
      resultEl.innerHTML = '<div style="color:var(--bear);font-weight:700;">❌ Test errored: '
                         + (e && e.message ? e.message : String(e)) + '</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🏫 TEST SR GATING'; }
      if(statusEl) statusEl.textContent = 'Errored — see panel.';
      return;
    }
    if(!out){
      resultEl.innerHTML = '<div style="color:var(--bear);">Test returned no data.</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🏫 TEST SR GATING'; }
      return;
    }
    var safe = (out.projOverall != null && out.projOverall >= 70);
    var color = safe ? '#1a7a4a' : '#c0281a';
    var verdict = (out.projOverall == null)
      ? '⚠ Could not project overall — variant may use a non-fib walker (sr-no-44 has no comparable baseline).'
      : (safe ? '✓ Projected overall ≥ 70% — safe to deploy if DE40/DJ30 per-pair WR improves enough.'
              : '⚠ Projected overall below 70% — do not deploy this scoping.');
    var pairRows = '';
    out.pairs.forEach(function(k){
      var p = out.perPair[k] || {};
      var oldStr = (p.oldW != null) ? (p.oldW + 'W/' + p.oldL + 'L') : '—';
      var oldWr = (p.oldWR != null) ? p.oldWR.toFixed(1) + '%' : '—';
      var newStr = (p.w != null) ? (p.w + 'W/' + p.l + 'L') : (p.error || '—');
      var newWr = (p.wr != null) ? p.wr.toFixed(1) + '%' : '—';
      var delta = (p.oldWR != null && p.wr != null) ? (p.wr - p.oldWR) : null;
      var deltaStr = (delta == null) ? '—' : ((delta >= 0 ? '+' : '') + delta.toFixed(1) + 'pp');
      var deltaCol = (delta == null) ? 'var(--inkd)' : (delta >= 0 ? 'var(--bull)' : 'var(--bear)');
      var sampleDrop = (p.oldW != null && p.w != null)
        ? Math.round(100 * (1 - (p.n / Math.max(1, p.oldW + p.oldL)))) + '%'
        : '—';
      pairRows += '<tr>'
        + '<td style="padding:3px 6px;">' + k + '</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + oldStr + '</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + oldWr + '</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + newStr + '</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + newWr + '</td>'
        + '<td style="padding:3px 6px;text-align:right;color:' + deltaCol + ';font-weight:700;">' + deltaStr + '</td>'
        + '<td style="padding:3px 6px;text-align:right;color:var(--inkd);">−' + sampleDrop + '</td>'
        + '</tr>';
    });
    var noBaseline = (out.variant === 'sr-no-44'
                      || out.variant === 'sr-orb-failure'
                      || out.variant === 'sr-orb-strong'
                      || out.variant === 'sr-vwap-pullback'
                      || out.variant === 'sr-silver-bullet');
    resultEl.innerHTML =
      '<div style="font-family:Orbitron,monospace;font-size:8.5px;color:var(--purple,#6a5cb8);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:6px;">🏫 SR gating dry-run · ' + out.variant + '</div>'
      + '<div style="margin-bottom:6px;color:var(--inkd);font-size:8.5px;">' + ({
          'sr-window-only': 'Keeps DE40 / DJ30 fib trades whose creator timestamp falls inside the 2-hour SR window (07:00–09:00 UTC DE40, 13:30–15:30 UTC DJ30). Anything outside the window is dropped.',
          'sr-broken-only': 'Keeps DE40 / DJ30 trades whose creator timestamp had an SR break already in state (bull_broken / bear_broken). Pending and failed states drop.',
          'sr-tier-only':   'Keeps DE40 / DJ30 trades where the SR state matches the trade direction (bull_broken with bull trade, bear_broken with bear trade) — proxy for the 5/5 + 4/5 SR cohorts.',
          'sr-no-44':       'IGNORES 4/4 alignment entirely. Fires a fresh setup on every SR break (bull_broken / bear_broken). Entry at break candle close, stop at opposite SR boundary, target at 1:1 R:R, 16-bar expiry. The OLD column is N/A since this isn\'t a filter — it\'s a different strategy.',
          'sr-orb-failure': 'ORB FAILURE (research: tradertom, daytradingtoolkit). Most opening-range breakouts fail; this variant FADES the failed break. After SR break, watches up to 8 bars; if price closes back INSIDE the SR range, fires a counter-trend trade at the close-back-inside bar. Stop at the failed-break extreme, target 1:1 R:R. The expected edge: trapped breakout traders provide momentum in the reversal direction.',
          'sr-orb-strong':  'ORB WITH STRENGTH FILTER (research: quantifiedstrategies, tradersmastermind). Same as sr-no-44 (any-SR-break) but the breakout candle must be a CONVINCING break: body ≥ 60% of total range AND body magnitude ≥ 1.5 × ATR(20). Drops the wicky fake-breakout cohort that pulled sr-no-44 down to 41-54% on DE40/DJ30. Expected to lift WR closer to the cited 65%.',
          'sr-vwap-pullback': 'VWAP PULLBACK in session (research: humbledtrader, asktraders, fyers). Computes session VWAP (cumulative typical price — no volume data so this is an approximation). Waits for the morning trend to displace ≥ 0.5×ATR away from VWAP, then arms. Fires on the first bar that pulls back within 0.15×ATR of VWAP AND closes in the trend direction. Entry at confirmation-bar close, stop at confirmation-bar opposite extreme, target 1:1 R:R. One trade per session.',
          'sr-silver-bullet': 'ICT SILVER BULLET (research: fxopen, ictpdf, innercircletrader.net, phidiaspropfirm). During the second cash hour kill zone (DAX 08:00-09:00 UTC, DJ30 14:30-15:30 UTC), scans for a 3-bar Fair Value Gap aligned with the session bias (kill-zone open bar direction). On a retest into the FVG midpoint within 6 bars, fires at the retest bar\'s close. Stop at the FVG creator\'s opposite extreme, target 1:1 R:R. The "kill zone" framing is core to ICT theory — these windows have the highest institutional participation.'
        }[out.variant] || '') + '</div>'
      + '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
      + '<thead><tr style="border-bottom:1px solid rgba(0,0,0,0.1);font-size:8.5px;color:var(--inkd);">'
      + '<th style="padding:3px 6px;text-align:left;">pair</th>'
      + '<th style="padding:3px 6px;text-align:right;">OLD W/L</th>'
      + '<th style="padding:3px 6px;text-align:right;">OLD WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">NEW W/L</th>'
      + '<th style="padding:3px 6px;text-align:right;">NEW WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">Δ</th>'
      + '<th style="padding:3px 6px;text-align:right;">sample drop</th>'
      + '</tr></thead><tbody>' + pairRows + '</tbody></table></div>'
      + '<div style="margin-top:12px;padding:8px 10px;border-radius:3px;background:' + (safe ? 'rgba(26,122,74,0.07)' : 'rgba(192,40,26,0.07)') + ';border:1px solid ' + (safe ? 'rgba(26,122,74,0.3)' : 'rgba(192,40,26,0.3)') + ';">'
      + '<div style="font-family:Orbitron,monospace;font-size:9px;font-weight:700;letter-spacing:0.4px;text-transform:uppercase;color:var(--inkd);margin-bottom:4px;">Projected dashboard impact</div>'
      + (noBaseline
          ? '<div style="font-size:9.5px;color:var(--inkd);">sr-no-44 is a strategy variant, not a filter — overall impact requires per-pair deployment to evaluate. Use the per-pair WR + sample size above to judge.</div>'
          : '<div style="font-size:11px;"><span style="color:var(--inkd);">Current dashboard:</span> <strong>' + (out.origOverall != null ? out.origOverall.toFixed(2) + '%' : '—') + '</strong></div>'
            + '<div style="font-size:13px;color:' + color + ';font-weight:700;margin-top:2px;">Projected after SR filter: ' + (out.projOverall != null ? out.projOverall.toFixed(2) + '%' : '—') + '</div>'
            + '<div style="margin-top:6px;color:' + color + ';font-weight:700;font-size:9.5px;">' + verdict + '</div>')
      + '</div>';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🏫 TEST SR GATING'; }
    if(statusEl) statusEl.textContent = noBaseline ? 'Done — see per-pair table.' : (safe ? 'Done — projection is ≥70%.' : 'Done — projection BELOW 70%.');
  }, 50);
}
if(typeof window !== 'undefined'){ window.runSRGatingTest = runSRGatingTest; }

// ── NEWS BLACKOUT FILTER (variant 9) ───────────────────────────
// Goal: drop trade setups whose creator timestamp falls inside a
// high-impact news window for the pair's currency. Targets the
// XAG/XAU/DAX/DJ-style false-invalidation cohort where the trade
// was structurally valid but lost because a scheduled release
// drove the next bar against the setup.
//
// Two filters compose the variant:
//
// 1. SCHEDULED RELEASE BLACKOUT — hardcoded recurring high-impact
//    windows per currency, derived from public econ calendars.
//    Doesn't cover one-off events (rate cuts off-cycle, EM crises)
//    but does cover the regular cadence that drives 70%+ of news
//    volatility: NFP, CPI, FOMC, ECB, BOE, BOJ. The live path
//    falls back to events.json (which is populated hourly by
//    publish-events.yml) so the LIVE filter has the actual
//    upcoming calendar; the historical dry-run uses the recurring
//    schedule because events.json doesn't carry historical data.
//
// 2. VOLATILITY SPIKE FILTER — catches UNSCHEDULED news the
//    calendar can't predict (Trump tweets, terror attacks,
//    surprise central bank moves). When the m15 bar at creator
//    time has a range > VOL_SPIKE_RATIO × ATR(20), treat as
//    event-driven and skip. Reactive — doesn't prevent the spike
//    but does prevent entering a setup that fired on the spike.
//    This is the canonical no-trade-the-noise rule for prop firms.

// Recurring high-impact news windows by currency, UTC.
// Each entry: {dow:0-6, hour, min, durationMin, label}.
// Day of week: 0=Sunday, 1=Monday, ..., 5=Friday.
// The blackout is widened by BLACKOUT_PAD_MIN on each side so a
// setup that fires just before/after the release also gets dropped.
//
// US covers: NFP (1st Fri 12:30/13:30), CPI/PPI/Retail (mid-month
// 12:30), FOMC (8x/yr 18:00 Wed), claims (Thu 12:30). EU covers
// CPI (mid-month 09:00), ECB (8x/yr 12:15 Thu), PMI (Mon-Wed 07:30-
// 08:30). GBP covers BOE (8x/yr 11:00 Thu), CPI (Wed 06:00),
// PMI/employment. JPY covers BOJ + Tankan (Tue/Wed 04:00-06:00).
// AUD/NZD/CAD/CHF approximate from their major calendars.
var NEWS_WINDOWS_BY_CCY = {
  USD: [
    // Wed FOMC press window (8x/yr — over a 12mo window most Weds
    // at 18:00 are post-FOMC pressers; treat conservatively).
    {dow:3, hour:18, min:0, durationMin:60, label:'FOMC'},
    // Tue-Fri 12:30-13:45 UTC catches NFP, CPI, PCE, PPI, retail,
    // claims (covers winter EST 13:30 too via the durationMin).
    {dow:2, hour:12, min:30, durationMin:75, label:'US data'},
    {dow:3, hour:12, min:30, durationMin:75, label:'US data'},
    {dow:4, hour:12, min:30, durationMin:75, label:'US data'},
    {dow:5, hour:12, min:30, durationMin:75, label:'US data'}
  ],
  EUR: [
    {dow:1, hour:7, min:30, durationMin:120, label:'EU AM data'},
    {dow:2, hour:7, min:30, durationMin:120, label:'EU AM data'},
    {dow:3, hour:7, min:30, durationMin:120, label:'EU AM data'},
    {dow:4, hour:7, min:30, durationMin:120, label:'EU AM data'},
    {dow:5, hour:7, min:30, durationMin:120, label:'EU AM data'},
    // ECB presser Thu 12:15 (~8x/yr)
    {dow:4, hour:12, min:15, durationMin:90, label:'ECB'}
  ],
  GBP: [
    {dow:1, hour:6, min:30, durationMin:90, label:'UK AM data'},
    {dow:2, hour:6, min:30, durationMin:90, label:'UK AM data'},
    {dow:3, hour:6, min:30, durationMin:90, label:'UK AM data'},
    {dow:4, hour:6, min:30, durationMin:90, label:'UK AM data'},
    {dow:5, hour:6, min:30, durationMin:90, label:'UK AM data'},
    // BOE Thu 11:00 (~8x/yr)
    {dow:4, hour:11, min:0, durationMin:60, label:'BOE'}
  ],
  JPY: [
    {dow:1, hour:0, min:30, durationMin:90, label:'JP data'},
    {dow:2, hour:0, min:30, durationMin:90, label:'JP data'},
    {dow:3, hour:0, min:30, durationMin:90, label:'JP data'}
  ],
  AUD: [
    {dow:1, hour:1, min:0, durationMin:60, label:'AU data'},
    {dow:2, hour:1, min:0, durationMin:60, label:'AU data'},
    {dow:3, hour:1, min:0, durationMin:60, label:'AU data'}
  ],
  NZD: [
    {dow:2, hour:21, min:45, durationMin:90, label:'NZ data'},
    {dow:3, hour:21, min:45, durationMin:90, label:'NZ data'}
  ],
  CAD: [
    {dow:2, hour:12, min:30, durationMin:75, label:'CA data'},
    {dow:5, hour:12, min:30, durationMin:75, label:'CA data'}
  ],
  CHF: [
    {dow:1, hour:6, min:30, durationMin:60, label:'CH data'},
    {dow:4, hour:6, min:30, durationMin:60, label:'CH data'}
  ]
};

var NEWS_BLACKOUT_PAD_MIN = 15;        // ± minutes around release
var NEWS_VOL_SPIKE_RATIO  = 2.5;       // range > 2.5 × ATR(20)
var NEWS_VOL_SPIKE_ATR_LOOKBACK = 20;

function _isInScheduledNewsWindow(tsIso, currencies, padMin){
  if(!tsIso) return null;
  var d = new Date(tsIso);
  if(isNaN(d.getTime())) return null;
  var dow = d.getUTCDay();
  var minOfDay = d.getUTCHours() * 60 + d.getUTCMinutes();
  var dayOfMonth = d.getUTCDate();
  for(var i = 0; i < currencies.length; i++){
    var ccy = currencies[i];
    var wins = NEWS_WINDOWS_BY_CCY[ccy] || [];
    for(var j = 0; j < wins.length; j++){
      var w = wins[j];
      if(w.dow !== dow) continue;
      var wStart = w.hour * 60 + w.min - padMin;
      var wEnd   = w.hour * 60 + w.min + w.durationMin + padMin;
      if(minOfDay >= wStart && minOfDay <= wEnd){
        return ccy + ' · ' + w.label;
      }
    }
    // Special: NFP first Friday of month for USD (release 12:30 or 13:30)
    if(ccy === 'USD' && dow === 5 && dayOfMonth <= 7){
      var nfpStart = 12 * 60 + 30 - padMin;
      var nfpEnd   = 13 * 60 + 45 + padMin;
      if(minOfDay >= nfpStart && minOfDay <= nfpEnd) return 'USD · NFP';
    }
  }
  return null;
}

function _isVolatilitySpike(bars, idx, threshold, lookback){
  if(idx < lookback || !bars[idx]) return false;
  var atrSum = 0, atrCount = 0;
  for(var i = idx - lookback; i < idx; i++){
    var b = bars[i];
    if(b && b.h != null && b.l != null){
      atrSum += Math.max(0, b.h - b.l);
      atrCount++;
    }
  }
  if(atrCount === 0) return false;
  var atr = atrSum / atrCount;
  var bar = bars[idx];
  if(bar.h == null || bar.l == null) return false;
  return (bar.h - bar.l) > threshold * atr;
}

function compareNewsBlackout(opts){
  opts = opts || {};
  var padMin       = (opts.padMin != null) ? opts.padMin : NEWS_BLACKOUT_PAD_MIN;
  var volThreshold = (opts.volSpikeRatio != null) ? opts.volSpikeRatio : NEWS_VOL_SPIKE_RATIO;
  var mode         = opts.mode || 'auto-ew';

  console.log('%c[compareNewsBlackout] running…', 'color:#c8860a;font-weight:700;',
              {padMin: padMin, volSpikeRatio: volThreshold, mode: mode});

  var perPair = {};
  var classAgg = {};
  Object.keys(MKTS).forEach(function(k){
    if(k === 'dxy') return;
    var m = MKTS[k];
    var cls = m && m.t;
    if(!cls) return;
    var currencies = PAIR_CURRENCIES[k] || [];
    if(currencies.length === 0){
      perPair[k] = {error: 'no currency mapping (crypto / no-coverage)'};
      return;
    }
    if(!(typeof DEEP_HIST !== 'undefined' && DEEP_HIST[k]
        && DEEP_HIST[k].m15 && DEEP_HIST[k].m15.length >= 1000)){
      perPair[k] = {error: 'no deep history'};
      return;
    }
    var bars = DEEP_HIST[k].m15;
    window.__counterBarOverride = null;
    var rb;
    try { rb = calcRecentBacktest(k, mode); }
    catch(e){ perPair[k] = {error: 'calc failed: ' + (e && e.message)}; return; }
    if(!rb){ perPair[k] = {error: 'no rb'}; return; }
    var method = (typeof _btMethodFor === 'function') ? _btMethodFor(k) : 'wick';
    var trades;
    if(method === 'fib' && rb.hybrid && rb.hybrid.midTrades) trades = rb.hybrid.midTrades;
    else trades = rb.trades;
    if(!trades){ perPair[k] = {error: 'no per-trade records'}; return; }
    var oldW = 0, oldL = 0, newW = 0, newL = 0;
    var droppedNews = 0, droppedVol = 0;
    trades.forEach(function(t){
      if(t.outcome === 'win') oldW++;
      else if(t.outcome === 'loss') oldL++;
      else return;
      var newsHit = _isInScheduledNewsWindow(t.creatorTs, currencies, padMin);
      if(newsHit){ droppedNews++; return; }
      var idx = _binSearchTsIdx(bars, t.creatorTs);
      var volHit = (idx >= 0) && _isVolatilitySpike(bars, idx, volThreshold, NEWS_VOL_SPIKE_ATR_LOOKBACK);
      if(volHit){ droppedVol++; return; }
      if(t.outcome === 'win') newW++;
      else newL++;
    });
    perPair[k] = {
      cls: cls, method: method,
      oldW: oldW, oldL: oldL,
      newW: newW, newL: newL,
      droppedNews: droppedNews, droppedVol: droppedVol,
      oldWR: (oldW + oldL) > 0 ? (oldW / (oldW + oldL)) * 100 : null,
      newWR: (newW + newL) > 0 ? (newW / (newW + newL)) * 100 : null
    };
    if(!classAgg[cls]) classAgg[cls] = {oldW:0,oldL:0,newW:0,newL:0,pairs:0,droppedNews:0,droppedVol:0};
    var ca = classAgg[cls];
    ca.oldW += oldW; ca.oldL += oldL;
    ca.newW += newW; ca.newL += newL;
    ca.droppedNews += droppedNews; ca.droppedVol += droppedVol;
    ca.pairs += 1;
  });

  // Overall projection
  var totOldW = 0, totOldL = 0, totNewW = 0, totNewL = 0;
  Object.keys(classAgg).forEach(function(c){
    var ca = classAgg[c];
    totOldW += ca.oldW; totOldL += ca.oldL;
    totNewW += ca.newW; totNewL += ca.newL;
  });
  var oldOverall = (totOldW + totOldL) > 0 ? (totOldW / (totOldW + totOldL)) * 100 : null;
  var newOverall = (totNewW + totNewL) > 0 ? (totNewW / (totNewW + totNewL)) * 100 : null;

  console.log('per-pair:', perPair);
  console.log('per-class:', classAgg);
  console.log('overall — OLD', oldOverall && oldOverall.toFixed(2)+'%',
              'NEW', newOverall && newOverall.toFixed(2)+'%');

  return {
    perPair: perPair, classAgg: classAgg,
    oldOverall: oldOverall, newOverall: newOverall,
    totOldW: totOldW, totOldL: totOldL,
    totNewW: totNewW, totNewL: totNewL,
    padMin: padMin, volSpikeRatio: volThreshold
  };
}
if(typeof window !== 'undefined'){ window.compareNewsBlackout = compareNewsBlackout; }

// In-page handler — same UX shape as runSRGatingTest.
function runNewsBlackoutTest(opts){
  var btn = document.getElementById('btNewsTestBtn');
  var statusEl = document.getElementById('btNewsTestStatus');
  var resultEl = document.getElementById('btNewsTestResult');
  if(!resultEl) return;
  if(typeof compareNewsBlackout !== 'function'){
    if(statusEl) statusEl.textContent = 'Engine not loaded — reload the page.';
    return;
  }
  if(typeof DEEP_HIST === 'undefined' || Object.keys(DEEP_HIST).length === 0){
    if(statusEl) statusEl.textContent = '⏳ Deep history still loading — try again in a few seconds.';
    return;
  }
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🗞 RUNNING…'; }
  if(statusEl) statusEl.textContent = 'Filtering all pairs by news + vol-spike…';
  resultEl.style.display = 'block';
  resultEl.innerHTML = '<div style="color:var(--inkd);font-style:italic;">⏳ Computing — '
                     + 'recomputes calcRecentBacktest across the universe and applies the news + '
                     + 'volatility-spike filter on each trade. ~20-60s.</div>';
  setTimeout(function(){
    var out;
    try { out = compareNewsBlackout(opts || {}); }
    catch(e){
      resultEl.innerHTML = '<div style="color:var(--bear);font-weight:700;">❌ ' + (e && e.message ? e.message : String(e)) + '</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🗞 TEST NEWS BLACKOUT'; }
      return;
    }
    if(!out){
      resultEl.innerHTML = '<div style="color:var(--bear);">No data.</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🗞 TEST NEWS BLACKOUT'; }
      return;
    }
    var safe = (out.newOverall != null && out.newOverall >= 70);
    var color = safe ? '#1a7a4a' : '#c0281a';
    var verdict = (out.newOverall == null)
      ? 'No samples passed the filter.'
      : (safe ? '✓ Projected overall ≥ 70% — safe to deploy if uplift is meaningful.'
              : '⚠ Projected overall below 70% — do not deploy this scoping.');
    // Per-class table
    var classRows = '';
    Object.keys(out.classAgg).sort().forEach(function(c){
      var ca = out.classAgg[c];
      var oldD = ca.oldW + ca.oldL, newD = ca.newW + ca.newL;
      var oldWr = oldD > 0 ? (ca.oldW / oldD) * 100 : null;
      var newWr = newD > 0 ? (ca.newW / newD) * 100 : null;
      var delta = (oldWr != null && newWr != null) ? (newWr - oldWr) : null;
      var deltaStr = (delta == null) ? '—' : ((delta >= 0 ? '+' : '') + delta.toFixed(1) + 'pp');
      var deltaCol = (delta == null) ? 'var(--inkd)' : (delta >= 0 ? 'var(--bull)' : 'var(--bear)');
      classRows += '<tr>'
        + '<td style="padding:3px 6px;">' + c + '</td>'
        + '<td style="padding:3px 6px;color:var(--inkd);">' + ca.pairs + ' pairs</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + ca.oldW + 'W/' + ca.oldL + 'L</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + (oldWr != null ? oldWr.toFixed(1) + '%' : '—') + '</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + ca.newW + 'W/' + ca.newL + 'L</td>'
        + '<td style="padding:3px 6px;text-align:right;">' + (newWr != null ? newWr.toFixed(1) + '%' : '—') + '</td>'
        + '<td style="padding:3px 6px;text-align:right;color:' + deltaCol + ';font-weight:700;">' + deltaStr + '</td>'
        + '<td style="padding:3px 6px;text-align:right;color:var(--inkd);">' + ca.droppedNews + ' news · ' + ca.droppedVol + ' vol</td>'
        + '</tr>';
    });
    resultEl.innerHTML =
      '<div style="font-family:Orbitron,monospace;font-size:8.5px;color:var(--purple,#6a5cb8);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:6px;">🗞 News blackout · ±' + out.padMin + ' min · vol-spike > ' + out.volSpikeRatio + '× ATR(20)</div>'
      + '<div style="margin-bottom:6px;color:var(--inkd);font-size:8.5px;">Drops any trade whose creator timestamp lands inside a recurring high-impact news window for the pair\'s currencies (NFP, CPI, FOMC, ECB, BOE, BOJ, etc.) OR on a bar whose range exceeds 2.5× ATR(20) — the volatility-spike heuristic that catches unscheduled events (Trump tweets, surprise statements, terror).</div>'
      + '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
      + '<thead><tr style="border-bottom:1px solid rgba(0,0,0,0.1);font-size:8.5px;color:var(--inkd);">'
      + '<th style="padding:3px 6px;text-align:left;">class</th><th></th>'
      + '<th style="padding:3px 6px;text-align:right;">OLD W/L</th><th style="padding:3px 6px;text-align:right;">OLD WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">NEW W/L</th><th style="padding:3px 6px;text-align:right;">NEW WR</th>'
      + '<th style="padding:3px 6px;text-align:right;">Δ</th><th style="padding:3px 6px;text-align:right;">dropped</th>'
      + '</tr></thead><tbody>' + classRows + '</tbody></table></div>'
      + '<div style="margin-top:12px;padding:8px 10px;border-radius:3px;background:' + (safe ? 'rgba(26,122,74,0.07)' : 'rgba(192,40,26,0.07)') + ';border:1px solid ' + (safe ? 'rgba(26,122,74,0.3)' : 'rgba(192,40,26,0.3)') + ';">'
      + '<div style="font-family:Orbitron,monospace;font-size:9px;font-weight:700;letter-spacing:0.4px;text-transform:uppercase;color:var(--inkd);margin-bottom:4px;">Projected overall</div>'
      + '<div style="font-size:11px;"><span style="color:var(--inkd);">Current:</span> <strong>' + (out.oldOverall != null ? out.oldOverall.toFixed(2) + '%' : '—') + '</strong> (' + out.totOldW + 'W/' + out.totOldL + 'L)</div>'
      + '<div style="font-size:13px;color:' + color + ';font-weight:700;margin-top:2px;">After news filter: ' + (out.newOverall != null ? out.newOverall.toFixed(2) + '%' : '—') + ' (' + out.totNewW + 'W/' + out.totNewL + 'L)</div>'
      + '<div style="margin-top:6px;color:' + color + ';font-weight:700;font-size:9.5px;">' + verdict + '</div>'
      + '</div>';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🗞 TEST NEWS BLACKOUT'; }
    if(statusEl) statusEl.textContent = safe ? 'Done — projection ≥70%.' : 'Done — projection below 70%.';
  }, 50);
}
if(typeof window !== 'undefined'){ window.runNewsBlackoutTest = runNewsBlackoutTest; }

// ── PER-CLASS R:R DRY-RUN ──────────────────────────────────────
// User hypothesis after 9 prior variants couldn't lift DAX/DJ30:
// DE40/DJ30 might respond better to a different reward multiplier
// than the production 1:1 R:R. Smaller targets are easier to hit
// (higher WR but smaller per-win), bigger targets vice versa.
//
// Test design: for each pair, take the per-trade records (wins
// + losses with their maxFavorable excursion) and recompute the
// outcome at each candidate ratio. Rule:
//   triggered + maxFav >= ratio  → WIN at this ratio
//   triggered + maxFav <  ratio  → LOSS (stop) at this ratio
// Skip invalidated/expired since they didn't reach a stop or
// target outcome. Aggregate per class to find the ratio that
// maximises EXPECTED R per trade (win × ratio − loss × 1.0).
//
// Same dry-run pattern as the other tests — no production
// behaviour changes, no cache writes.
function compareRRPerClass(opts){
  opts = opts || {};
  // 2026-06-13 — user explicitly added 2.0 (1:2 R:R) to the test set.
  var ratios = opts.ratios || [0.5, 0.75, 1.0, 1.5, 2.0];
  var mode = opts.mode || 'auto-ew';
  var perPair = {};
  var classAgg = {};
  Object.keys(MKTS).forEach(function(k){
    if(k === 'dxy') return;
    var m = MKTS[k];
    var cls = m && m.t;
    if(!cls) return;
    if(!(typeof DEEP_HIST !== 'undefined' && DEEP_HIST[k]
        && DEEP_HIST[k].m15 && DEEP_HIST[k].m15.length >= 1000)){
      perPair[k] = {error: 'no deep history'};
      return;
    }
    window.__counterBarOverride = null;
    var rb;
    try { rb = calcRecentBacktest(k, mode); }
    catch(e){ perPair[k] = {error: 'calc failed: ' + (e && e.message)}; return; }
    if(!rb){ perPair[k] = {error: 'no rb'}; return; }
    var method = (typeof _btMethodFor === 'function') ? _btMethodFor(k) : 'wick';
    var trades;
    if(method === 'fib' && rb.hybrid && rb.hybrid.midTrades) trades = rb.hybrid.midTrades;
    else trades = rb.trades;
    if(!trades){ perPair[k] = {error: 'no per-trade records'}; return; }
    if(!classAgg[cls]){
      classAgg[cls] = {pairs: 0, byRatio: {}};
      ratios.forEach(function(r){ classAgg[cls].byRatio[r] = {W: 0, L: 0}; });
    }
    classAgg[cls].pairs += 1;
    var pairByRatio = {};
    ratios.forEach(function(r){ pairByRatio[r] = {W: 0, L: 0}; });
    trades.forEach(function(t){
      if(t.triggered === false) return;
      // Only count trades that produced a structural outcome
      // (win or loss). Invalidated/expired didn't traverse from
      // entry to target/stop, so promoting them to a different
      // ratio outcome would over-estimate the sample.
      if(t.outcome !== 'win' && t.outcome !== 'loss') return;
      var maxFav;
      if(typeof t.maxFavorable === 'number') maxFav = t.maxFavorable;
      else maxFav = (t.outcome === 'win') ? 1.0 : 0;
      ratios.forEach(function(r){
        if(maxFav >= r){
          classAgg[cls].byRatio[r].W += 1;
          pairByRatio[r].W += 1;
        } else {
          classAgg[cls].byRatio[r].L += 1;
          pairByRatio[r].L += 1;
        }
      });
    });
    perPair[k] = {cls: cls, method: method, byRatio: pairByRatio};
  });
  // Compute WR + E[R] per class per ratio, pick the best
  Object.keys(classAgg).forEach(function(cls){
    var bestR = null, bestEV = -Infinity, bestWR = null;
    ratios.forEach(function(r){
      var c = classAgg[cls].byRatio[r];
      var n = c.W + c.L;
      c.n = n;
      c.wr = n > 0 ? (c.W / n) * 100 : null;
      c.evPerTrade = n > 0 ? (c.W * r - c.L * 1.0) / n : null;
      if(n > 0 && c.evPerTrade > bestEV){
        bestEV = c.evPerTrade;
        bestR = r;
        bestWR = c.wr;
      }
    });
    classAgg[cls].bestRatio = bestR;
    classAgg[cls].bestEV = bestEV;
    classAgg[cls].bestWR = bestWR;
  });
  return {classAgg: classAgg, perPair: perPair, ratios: ratios};
}
if(typeof window !== 'undefined'){ window.compareRRPerClass = compareRRPerClass; }

function runRRPerClassTest(opts){
  var btn = document.getElementById('btRRTestBtn');
  var statusEl = document.getElementById('btRRTestStatus');
  var resultEl = document.getElementById('btRRTestResult');
  if(!resultEl) return;
  if(typeof compareRRPerClass !== 'function'){
    if(statusEl) statusEl.textContent = 'Engine not loaded — reload the page.';
    return;
  }
  if(typeof DEEP_HIST === 'undefined' || Object.keys(DEEP_HIST).length === 0){
    if(statusEl) statusEl.textContent = '⏳ Deep history still loading — try again in a few seconds.';
    return;
  }
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🎯 RUNNING…'; }
  if(statusEl) statusEl.textContent = 'Sweeping R:R ratios across all classes…';
  resultEl.style.display = 'block';
  resultEl.innerHTML = '<div style="color:var(--inkd);font-style:italic;">⏳ Computing — '
                     + 'recomputes calcRecentBacktest across the universe and re-classifies each trade at each candidate ratio via maxFavorable. ~20-60s.</div>';
  setTimeout(function(){
    var out;
    try { out = compareRRPerClass(opts || {}); }
    catch(e){
      resultEl.innerHTML = '<div style="color:var(--bear);font-weight:700;">❌ ' + (e && e.message ? e.message : String(e)) + '</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🎯 TEST R:R PER CLASS'; }
      return;
    }
    if(!out){
      resultEl.innerHTML = '<div style="color:var(--bear);">No data.</div>';
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🎯 TEST R:R PER CLASS'; }
      return;
    }
    // Header row of ratios
    var hdrCells = '<th style="padding:3px 6px;text-align:left;">class</th><th></th>';
    out.ratios.forEach(function(r){
      hdrCells += '<th style="padding:3px 6px;text-align:right;" colspan="2">1:' + r + '</th>';
    });
    hdrCells += '<th style="padding:3px 6px;text-align:right;">best</th>';
    var sub = '<th></th><th></th>';
    out.ratios.forEach(function(){
      sub += '<th style="padding:1px 6px;text-align:right;font-size:7.5px;color:var(--inkd);font-weight:400;">WR</th>'
           + '<th style="padding:1px 6px;text-align:right;font-size:7.5px;color:var(--inkd);font-weight:400;">E[R]</th>';
    });
    sub += '<th></th>';
    var rows = '';
    Object.keys(out.classAgg).sort().forEach(function(cls){
      var ca = out.classAgg[cls];
      var row = '<tr>'
        + '<td style="padding:3px 6px;font-weight:700;">' + cls + '</td>'
        + '<td style="padding:3px 6px;color:var(--inkd);">' + ca.pairs + ' pairs</td>';
      out.ratios.forEach(function(r){
        var c = ca.byRatio[r];
        var wrStr = (c.wr != null) ? c.wr.toFixed(1) + '%' : '—';
        var evStr = (c.evPerTrade != null) ? (c.evPerTrade >= 0 ? '+' : '') + c.evPerTrade.toFixed(2) + 'R' : '—';
        var evCol = (c.evPerTrade != null) ? (c.evPerTrade > 0 ? 'var(--bull)' : 'var(--bear)') : 'var(--inkd)';
        var bgWR = (r === ca.bestRatio) ? 'background:rgba(26,122,74,0.07);font-weight:700;' : '';
        var bgEV = (r === ca.bestRatio) ? 'background:rgba(26,122,74,0.07);font-weight:700;' : '';
        row += '<td style="padding:3px 6px;text-align:right;' + bgWR + '">' + wrStr + '</td>'
             + '<td style="padding:3px 6px;text-align:right;color:' + evCol + ';' + bgEV + '">' + evStr + '</td>';
      });
      row += '<td style="padding:3px 6px;text-align:right;color:var(--bull);font-weight:700;">1:' + ca.bestRatio + '</td>';
      row += '</tr>';
      rows += row;
    });
    // Watch-list — DE40 / DJ30 / XAG / XAU per-pair so the user can
    // see whether smaller targets specifically help the cohort that
    // drags the INDEX aggregate down. The class aggregate masks
    // DAX/DJ30 because NAS / SPX / FTSE pull it up.
    var watchList = ['de40','dj30','xagusd','xauusd'];
    var watchRows = '';
    watchList.forEach(function(k){
      var pp = out.perPair[k];
      if(!pp || pp.error || !pp.byRatio) return;
      // Compute best per pair
      var pBestR = null, pBestEV = -Infinity;
      out.ratios.forEach(function(r){
        var c = pp.byRatio[r];
        var n = c.W + c.L;
        if(n === 0) return;
        var ev = (c.W * r - c.L * 1.0) / n;
        if(ev > pBestEV){ pBestEV = ev; pBestR = r; }
      });
      var wr = '<tr><td style="padding:3px 6px;font-weight:700;">' + k + '</td>'
             + '<td style="padding:3px 6px;color:var(--inkd);">' + (pp.method || '?') + '</td>';
      out.ratios.forEach(function(r){
        var c = pp.byRatio[r];
        var n = c.W + c.L;
        var wrV = n > 0 ? (c.W / n) * 100 : null;
        var evV = n > 0 ? (c.W * r - c.L * 1.0) / n : null;
        var wrStr = (wrV != null) ? wrV.toFixed(1) + '%' : '—';
        var evStr = (evV != null) ? (evV >= 0 ? '+' : '') + evV.toFixed(2) + 'R' : '—';
        var evCol = (evV != null) ? (evV > 0 ? 'var(--bull)' : 'var(--bear)') : 'var(--inkd)';
        var hl = (r === pBestR) ? 'background:rgba(26,122,74,0.07);font-weight:700;' : '';
        wr += '<td style="padding:3px 6px;text-align:right;' + hl + '">' + wrStr + '</td>'
            + '<td style="padding:3px 6px;text-align:right;color:' + evCol + ';' + hl + '">' + evStr + ' <span style="color:var(--inkd);font-size:7px;">(n=' + n + ')</span></td>';
      });
      wr += '<td style="padding:3px 6px;text-align:right;color:var(--bull);font-weight:700;">1:' + (pBestR != null ? pBestR : '—') + '</td>';
      wr += '</tr>';
      watchRows += wr;
    });
    var watchSection = '';
    if(watchRows){
      watchSection = '<div style="font-family:Orbitron,monospace;font-size:8.5px;color:var(--purple,#6a5cb8);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-top:14px;margin-bottom:4px;">Watch-list pairs (DE40 / DJ30 / XAG / XAU)</div>'
        + '<div style="color:var(--inkd);font-size:8.5px;margin-bottom:4px;">Per-pair view of the same ratio sweep — the index aggregate masks DAX/DJ30 because NAS / SPX / FTSE pull it up. The "best" column is the per-pair ratio that maximises E[R] for that single pair.</div>'
        + '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
        + '<thead>'
        + '<tr style="border-bottom:1px solid rgba(0,0,0,0.1);font-size:8.5px;color:var(--inkd);">' + hdrCells + '</tr>'
        + '<tr style="border-bottom:1px solid rgba(0,0,0,0.06);">' + sub + '</tr>'
        + '</thead><tbody>' + watchRows + '</tbody></table></div>';
    }
    resultEl.innerHTML =
      '<div style="font-family:Orbitron,monospace;font-size:8.5px;color:var(--purple,#6a5cb8);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:6px;">🎯 R:R sweep · per asset class · ratios ' + out.ratios.map(function(r){return '1:'+r;}).join(' / ') + '</div>'
      + '<div style="margin-bottom:6px;color:var(--inkd);font-size:8.5px;">For each existing trade, recomputes WIN if maxFavorable ≥ target ratio, else LOSS. Invalidated/expired trades excluded — they never reached a structural outcome. The "best" column is the ratio that maximises E[R] per trade for that class (highlighted green in the table).</div>'
      + '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
      + '<thead>'
      + '<tr style="border-bottom:1px solid rgba(0,0,0,0.1);font-size:8.5px;color:var(--inkd);">' + hdrCells + '</tr>'
      + '<tr style="border-bottom:1px solid rgba(0,0,0,0.06);">' + sub + '</tr>'
      + '</thead><tbody>' + rows + '</tbody></table></div>'
      + watchSection
      + '<div style="margin-top:10px;padding:7px 9px;border-radius:3px;background:rgba(200,134,10,0.08);border:1px solid rgba(200,134,10,0.25);color:var(--inkd);font-size:8.5px;line-height:1.4;">⚠ <strong>Methodology caveat for ratios > 1.0</strong>: the production walker exits the trade the moment the 1:1 target is hit, so maxFavorable is capped near 1.0R for winning trades. The 1:1.5 and 1:2 WR columns are therefore UNDER-stated — they only count trades whose favourable peak somehow exceeded the production exit. Only 1:0.5, 1:0.75 and 1:1 are reliable on this data. Larger ratios need a re-walker that lets winners run past 1:1 to capture the true exit — flag if needed.</div>'
      + '<div style="margin-top:6px;color:var(--inkd);font-size:8.5px;line-height:1.4;">Deploy rule of thumb — the per-class production R:R should be set to the column with the highest E[R] for that class. If two columns are within ~0.05R per trade, prefer the one with the higher WR (smoother equity curve, less psychological drawdown).</div>';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🎯 TEST R:R PER CLASS'; }
    if(statusEl) statusEl.textContent = 'Done — see table.';
  }, 50);
}
if(typeof window !== 'undefined'){ window.runRRPerClassTest = runRRPerClassTest; }

// 2026-06-16 — MACD-cross filter test (15m). Mirrors the runRRPerClassTest
// pattern: kicks the backtest engine twice (native profile, native + MACD
// hard gate), aggregates wins/losses by asset class, renders a side-by-
// side comparison table. The production WR cards + caches are untouched
// — results live in dedicated cache slots until the user decides whether
// to promote the filter for some classes, all classes, or none.
//
// Per-pair routing: structural-profile pairs (FX majors except AUD/USD)
// score under 'structural' vs 'structural-macd'; auto-EW-profile pairs
// score under 'auto-ew' vs 'auto-ew-macd'. The native cache is reused
// when fresh so only the MACD-extended variants need fresh compute.
function runMacdCrossTest(){
  var btn = document.getElementById('btMacdTestBtn');
  var statusEl = document.getElementById('btMacdTestStatus');
  var resultEl = document.getElementById('btMacdTestResult');
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🧪 RUNNING...'; }
  if(resultEl) resultEl.innerHTML = '';

  if(typeof computeAllRecentBacktestsAsync !== 'function'){
    if(statusEl) statusEl.textContent = 'Backtest engine not loaded';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🧪 TEST: 15M MACD CROSS FILTER'; }
    return;
  }

  var modes = ['structural', 'structural-macd', 'auto-ew', 'auto-ew-macd'];
  var loaded = {};
  var mIdx = 0;
  function runNext(){
    if(mIdx >= modes.length){
      _renderMacdCrossTestResults(loaded, statusEl, resultEl);
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = '🧪 TEST: 15M MACD CROSS FILTER'; }
      return;
    }
    var mode = modes[mIdx];
    if(statusEl) statusEl.textContent = 'Computing ' + mode + ' (' + (mIdx + 1) + '/' + modes.length + ')…';
    // Force-refresh the two new modes; reuse cache for the production
    // baselines unless they're missing.
    var forceRefresh = (mode === 'structural-macd' || mode === 'auto-ew-macd');
    computeAllRecentBacktestsAsync(forceRefresh, function(done, total){
      if(statusEl) statusEl.textContent = 'Computing ' + mode + ' — ' + done + '/' + total;
    }, function(results){
      loaded[mode] = results || {};
      mIdx++;
      setTimeout(runNext, 0);
    }, mode);
  }
  setTimeout(runNext, 0);
}
if(typeof window !== 'undefined'){ window.runMacdCrossTest = runMacdCrossTest; }

function _renderMacdCrossTestResults(loaded, statusEl, resultEl){
  if(!resultEl){ if(statusEl) statusEl.textContent = '⚠ result host missing'; return; }
  // Aggregate wins/losses per asset class for both production and MACD.
  // Per-pair routing: use the pair's native profile for both columns.
  var classes = ['major','minor','comm','index','crypto'];
  var agg = {};
  classes.forEach(function(cls){ agg[cls] = {prodW:0, prodL:0, prodN:0, macdW:0, macdL:0, macdN:0}; });
  var totals = {prodW:0, prodL:0, prodN:0, macdW:0, macdL:0, macdN:0};

  Object.keys(MKTS).forEach(function(k){
    var cls = (MKTS[k] && MKTS[k].t) || 'unknown';
    if(!agg[cls]) return;
    var profile = (typeof _btProfileFor === 'function') ? _btProfileFor(k) : {source:'auto-ew'};
    var prodMode = profile.source === 'structural' ? 'structural' : 'auto-ew';
    var macdMode = profile.source === 'structural' ? 'structural-macd' : 'auto-ew-macd';
    var prodRb = loaded[prodMode] && loaded[prodMode][k];
    var macdRb = loaded[macdMode] && loaded[macdMode][k];
    if(prodRb && typeof prodRb.wins === 'number'){
      agg[cls].prodW += prodRb.wins;
      agg[cls].prodL += prodRb.losses || 0;
      agg[cls].prodN += (prodRb.wins + (prodRb.losses || 0));
      totals.prodW += prodRb.wins;
      totals.prodL += prodRb.losses || 0;
      totals.prodN += (prodRb.wins + (prodRb.losses || 0));
    }
    if(macdRb && typeof macdRb.wins === 'number'){
      agg[cls].macdW += macdRb.wins;
      agg[cls].macdL += macdRb.losses || 0;
      agg[cls].macdN += (macdRb.wins + (macdRb.losses || 0));
      totals.macdW += macdRb.wins;
      totals.macdL += macdRb.losses || 0;
      totals.macdN += (macdRb.wins + (macdRb.losses || 0));
    }
  });

  function wr(W, N){ return N > 0 ? (W / N * 100) : null; }
  function wrFmt(p){ return p == null ? '—' : p.toFixed(1) + '%'; }
  function wrColor(p){ return p == null ? 'var(--inkd)' : (p >= 70 ? 'var(--bull)' : p >= 60 ? 'var(--gold)' : 'var(--bear)'); }
  function deltaFmt(prodWR, macdWR){
    if(prodWR == null || macdWR == null) return {txt:'—', col:'var(--inkd)'};
    var d = macdWR - prodWR;
    var sign = d >= 0 ? '+' : '';
    return {txt: sign + d.toFixed(1) + 'pp', col: d >= 2 ? 'var(--bull)' : d <= -2 ? 'var(--bear)' : 'var(--inkd)'};
  }

  var rows = classes.map(function(cls){
    var a = agg[cls];
    var pWR = wr(a.prodW, a.prodN);
    var mWR = wr(a.macdW, a.macdN);
    var d = deltaFmt(pWR, mWR);
    return '<tr style="border-top:1px dashed rgba(0,0,0,0.08);">'
      + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">' + cls + '</td>'
      + '<td style="padding:5px 8px;text-align:right;color:' + wrColor(pWR) + ';font-weight:700;">' + wrFmt(pWR) + '</td>'
      + '<td style="padding:5px 8px;text-align:right;color:var(--inkd);font-size:8.5px;">' + a.prodW + 'W / ' + a.prodL + 'L</td>'
      + '<td style="padding:5px 8px;text-align:right;color:' + wrColor(mWR) + ';font-weight:700;">' + wrFmt(mWR) + '</td>'
      + '<td style="padding:5px 8px;text-align:right;color:var(--inkd);font-size:8.5px;">' + a.macdW + 'W / ' + a.macdL + 'L</td>'
      + '<td style="padding:5px 8px;text-align:right;color:' + d.col + ';font-weight:700;">' + d.txt + '</td>'
      + '</tr>';
  }).join('');

  var totPWR = wr(totals.prodW, totals.prodN);
  var totMWR = wr(totals.macdW, totals.macdN);
  var totD = deltaFmt(totPWR, totMWR);
  var totalRow = '<tr style="border-top:2px solid var(--rule);background:rgba(0,0,0,0.02);">'
    + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">ALL</td>'
    + '<td style="padding:5px 8px;text-align:right;color:' + wrColor(totPWR) + ';font-weight:700;">' + wrFmt(totPWR) + '</td>'
    + '<td style="padding:5px 8px;text-align:right;color:var(--inkd);font-size:8.5px;">' + totals.prodW + 'W / ' + totals.prodL + 'L</td>'
    + '<td style="padding:5px 8px;text-align:right;color:' + wrColor(totMWR) + ';font-weight:700;">' + wrFmt(totMWR) + '</td>'
    + '<td style="padding:5px 8px;text-align:right;color:var(--inkd);font-size:8.5px;">' + totals.macdW + 'W / ' + totals.macdL + 'L</td>'
    + '<td style="padding:5px 8px;text-align:right;color:' + totD.col + ';font-weight:700;">' + totD.txt + '</td>'
    + '</tr>';

  resultEl.innerHTML =
    '<div style="margin-top:8px;padding:10px 12px;border:1px solid rgba(77,162,255,0.30);background:rgba(77,162,255,0.04);border-radius:4px;">'
    + '<div style="font-family:Orbitron,monospace;font-size:10px;font-weight:700;letter-spacing:0.8px;color:#4da2ff;margin-bottom:6px;">15M MACD CROSS FILTER — PER-CLASS WR COMPARISON</div>'
    + '<div style="font-size:8.5px;color:var(--inkd);line-height:1.4;margin-bottom:8px;">Filter: require same-direction MACD(12,26,9) cross within 3 m15 bars before trigger. Each pair scored under its native profile (structural for FX majors except AUD/USD, auto-EW for the rest). Delta column shows the WR change per asset class. Promising classes only deploy after live verification.</div>'
    + '<table style="width:100%;border-collapse:collapse;font-size:9px;">'
    + '<thead><tr style="border-bottom:1px solid var(--rule);font-size:8px;color:var(--inkd);letter-spacing:0.5px;">'
    + '<th style="padding:5px 8px;text-align:left;">Class</th>'
    + '<th style="padding:5px 8px;text-align:right;">Production WR</th>'
    + '<th style="padding:5px 8px;text-align:right;">W/L</th>'
    + '<th style="padding:5px 8px;text-align:right;">MACD-filter WR</th>'
    + '<th style="padding:5px 8px;text-align:right;">W/L</th>'
    + '<th style="padding:5px 8px;text-align:right;">Δ</th>'
    + '</tr></thead><tbody>' + rows + totalRow + '</tbody></table>'
    + '<div style="margin-top:8px;font-size:8.5px;color:var(--inkd);line-height:1.5;"><strong>Read:</strong> a positive Δ on a class means the MACD filter would lift that class\'s WR. A negative Δ means it kills more winners than losers and should NOT deploy there. Sample size matters — if a class has &lt;20 trades, treat the delta as directional only.</div>'
    + '</div>';
  if(statusEl){
    var ts = new Date().toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
    statusEl.textContent = '✓ Comparison ready · ' + ts;
  }
}
if(typeof window !== 'undefined'){ window._renderMacdCrossTestResults = _renderMacdCrossTestResults; }


// ── 3-of-4 + MACD CROSS + RSI EXTREME (2026-06-16c) ────────────
// Runs the deep walker once with mode='3of4-rsi-macd' across all pairs,
// aggregates wins/losses per asset class and total, and renders a
// per-class table. Unlike the MACD-only test there's NO production
// baseline column — this mode trades a DIFFERENT cohort (3/4 with NW
// dissenting) that the production engine rejects, so there's no apples-
// to-apples comparison to draw. The table reports the absolute WR of
// this expansion cohort so the user can decide whether to promote
// per-class.
function run3of4RsiMacdTest(){
  var btn = document.getElementById('bt3of4TestBtn');
  var statusEl = document.getElementById('bt3of4TestStatus');
  var resultEl = document.getElementById('bt3of4TestResult');
  var origLabel = '🧪 TEST: 3-OF-4 & 2-OF-4 + MACD + RSI EXTREME';
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🧪 RUNNING...'; }
  if(resultEl) resultEl.innerHTML = '';

  if(typeof computeAllRecentBacktestsAsync !== 'function'){
    if(statusEl) statusEl.textContent = 'Backtest engine not loaded';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
    return;
  }

  var modes = ['3of4-rsi-macd', '2of4-rsi-macd', '1of4-rsi-macd'];
  var loaded = {};
  var mIdx = 0;
  function runNext(){
    if(mIdx >= modes.length){
      _render3of4Results(loaded, statusEl, resultEl);
      if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
      return;
    }
    var mode = modes[mIdx];
    if(statusEl) statusEl.textContent = 'Computing ' + mode + ' (' + (mIdx + 1) + '/' + modes.length + ')…';
    computeAllRecentBacktestsAsync(true, function(done, total){
      if(statusEl) statusEl.textContent = 'Computing ' + mode + ' — ' + done + '/' + total;
    }, function(results){
      loaded[mode] = results || {};
      mIdx++;
      setTimeout(runNext, 0);
    }, mode);
  }
  setTimeout(runNext, 0);
}
if(typeof window !== 'undefined'){ window.run3of4RsiMacdTest = run3of4RsiMacdTest; }

function _render3of4Results(loaded, statusEl, resultEl){
  if(!resultEl){ if(statusEl) statusEl.textContent = '⚠ result host missing'; return; }
  var classes = ['major','minor','comm','index','crypto'];
  var modes = ['3of4-rsi-macd', '2of4-rsi-macd', '1of4-rsi-macd'];
  // Build per-class agg for each mode independently.
  var agg = {}; var totals = {};
  modes.forEach(function(m){
    agg[m] = {};
    classes.forEach(function(cls){ agg[m][cls] = {W:0, L:0, N:0}; });
    totals[m] = {W:0, L:0, N:0};
  });
  Object.keys(MKTS).forEach(function(k){
    var cls = (MKTS[k] && MKTS[k].t) || 'unknown';
    if(!classes.indexOf || classes.indexOf(cls) < 0) return;
    modes.forEach(function(m){
      var rb = loaded[m] && loaded[m][k];
      if(rb && typeof rb.wins === 'number'){
        var w = rb.wins, l = rb.losses || 0;
        agg[m][cls].W += w; agg[m][cls].L += l; agg[m][cls].N += (w + l);
        totals[m].W += w;   totals[m].L += l;   totals[m].N += (w + l);
      }
    });
  });

  function wr(W, N){ return N > 0 ? (W / N * 100) : null; }
  function wrFmt(p){ return p == null ? '—' : p.toFixed(1) + '%'; }
  function wrColor(p, N){
    if(p == null) return 'var(--inkd)';
    if(N < 10) return 'var(--inkd)';
    return p >= 70 ? 'var(--bull)' : p >= 60 ? 'var(--gold)' : 'var(--bear)';
  }
  function deployLabel(p, N){
    if(p == null || N < 10) return {txt: 'INSUFFICIENT', col: 'var(--inkd)'};
    if(p >= 72 && N >= 20) return {txt: 'DEPLOY-READY', col: 'var(--bull)'};
    if(p >= 65) return {txt: 'WATCH', col: 'var(--gold)'};
    return {txt: 'DO NOT DEPLOY', col: 'var(--bear)'};
  }
  function cellFor(stats){
    var w = wr(stats.W, stats.N);
    var dep = deployLabel(w, stats.N);
    return ''
      + '<td style="padding:5px 8px;text-align:right;color:' + wrColor(w, stats.N) + ';font-weight:700;">' + wrFmt(w) + '</td>'
      + '<td style="padding:5px 8px;text-align:right;color:var(--inkd);font-size:8.5px;">' + stats.W + 'W / ' + stats.L + 'L</td>'
      + '<td style="padding:5px 8px;text-align:right;color:var(--inkd);font-size:8.5px;">' + stats.N + '</td>'
      + '<td style="padding:5px 8px;text-align:right;color:' + dep.col + ';font-weight:700;font-size:8px;">' + dep.txt + '</td>';
  }

  var cohortMeta = [
    {key: '3of4-rsi-macd', label: '3-of-4 (NW dissents)',           bg: 'rgba(179,105,230,0.10)'},
    {key: '2of4-rsi-macd', label: '2-of-4 (1 supports EW)',          bg: 'rgba(179,105,230,0.06)'},
    {key: '1of4-rsi-macd', label: '1-of-4 (none support EW)',        bg: 'rgba(179,105,230,0.03)'},
  ];
  var rows = classes.map(function(cls){
    return '<tr style="border-top:1px dashed rgba(0,0,0,0.08);">'
      + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">' + cls + '</td>'
      + cohortMeta.map(function(c){ return cellFor(agg[c.key][cls]); }).join('')
      + '</tr>';
  }).join('');
  var totalRow = '<tr style="border-top:2px solid var(--rule);background:rgba(0,0,0,0.02);">'
    + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">ALL</td>'
    + cohortMeta.map(function(c){ return cellFor(totals[c.key]); }).join('')
    + '</tr>';

  var headerTop = '<tr style="border-bottom:1px solid var(--rule);font-size:8px;color:var(--inkd);letter-spacing:0.5px;">'
    + '<th rowspan="2" style="padding:5px 8px;text-align:left;vertical-align:bottom;">Class</th>'
    + cohortMeta.map(function(c){
        return '<th colspan="4" style="padding:5px 8px;text-align:center;background:' + c.bg + ';border-left:1px solid rgba(0,0,0,0.06);">' + c.label + '</th>';
      }).join('')
    + '</tr>';
  var headerBot = '<tr style="border-bottom:1px solid var(--rule);font-size:7.5px;color:var(--inkd);letter-spacing:0.5px;">'
    + cohortMeta.map(function(){
        return '<th style="padding:4px 8px;text-align:right;border-left:1px solid rgba(0,0,0,0.06);">WR</th>'
             + '<th style="padding:4px 8px;text-align:right;">W/L</th>'
             + '<th style="padding:4px 8px;text-align:right;">n</th>'
             + '<th style="padding:4px 8px;text-align:right;">Verdict</th>';
      }).join('')
    + '</tr>';

  resultEl.innerHTML =
    '<div style="margin-top:8px;padding:10px 12px;border:1px solid rgba(179,105,230,0.30);background:rgba(179,105,230,0.04);border-radius:4px;">'
    + '<div style="font-family:Orbitron,monospace;font-size:10px;font-weight:700;letter-spacing:0.8px;color:#b369e6;margin-bottom:6px;">3-OF-4 / 2-OF-4 / 1-OF-4 + MACD CROSS + RSI EXTREME — EXPANSION COHORTS</div>'
    + '<div style="font-size:8.5px;color:var(--inkd);line-height:1.4;margin-bottom:8px;">Three relaxed-alignment cohorts that the production 4/4 gate normally rejects. <strong>3-of-4</strong>: EW = TL = CL, NW dissents. <strong>2-of-4</strong>: EW direction with exactly ONE of TL/NW/CL agreeing. <strong>1-of-4</strong>: EW direction with ZERO support — fully contrarian. All gated by same-direction MACD(12,26,9)/Signal cross within 3 m15 bars AND 1H RSI extreme (&lt;30 bull, &gt;70 bear). Scroll horizontally on mobile to see all three cohorts. Deploy thresholds: ≥72% WR on ≥20 trades.</div>'
    + '<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;"><table style="width:100%;border-collapse:collapse;font-size:9px;min-width:780px;">'
    + '<thead>' + headerTop + headerBot + '</thead><tbody>' + rows + totalRow + '</tbody></table></div>'
    + '<div style="margin-top:8px;font-size:8.5px;color:var(--inkd);line-height:1.5;"><strong>Read:</strong> any class showing DEPLOY-READY (≥72% WR on ≥20 trades) is a promotion candidate. WATCH (65–72%) needs more data. INSUFFICIENT (&lt;10 trades) means the gate is too restrictive on this cohort to produce a sample — that\'s information too: this combination of conditions simply doesn\'t print often.</div>'
    + '</div>';
  if(statusEl){
    var ts = new Date().toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
    statusEl.textContent = '✓ Simulation ready · ' + ts;
  }
}
if(typeof window !== 'undefined'){ window._render3of4Results = _render3of4Results; }


// ── MACD-PRIMARY TEST (2026-06-16h) ────────────────────────────
// Runs the deep walker once with mode='macd-primary'. Aggregates
// trades by their `confluence` (0-4) bucket and per asset class.
// Output: matrix of WR per (class × confluence bucket) so we can
// see which confluence strength is worth deploying per class.
function runMacdPrimaryTest(){
  var btn = document.getElementById('btMacdPrimaryTestBtn');
  var statusEl = document.getElementById('btMacdPrimaryTestStatus');
  var resultEl = document.getElementById('btMacdPrimaryTestResult');
  var origLabel = '🧪 TEST: MACD-PRIMARY (CONFLUENCE BUCKETS)';
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🧪 RUNNING...'; }
  if(resultEl) resultEl.innerHTML = '';

  if(typeof computeAllRecentBacktestsAsync !== 'function'){
    if(statusEl) statusEl.textContent = 'Backtest engine not loaded';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
    return;
  }

  if(statusEl) statusEl.textContent = 'Computing MACD-primary cohort…';
  computeAllRecentBacktestsAsync(true, function(done, total){
    if(statusEl) statusEl.textContent = 'Computing MACD-primary cohort — ' + done + '/' + total;
  }, function(results){
    _renderMacdPrimaryResults(results || {}, statusEl, resultEl);
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
  }, 'macd-primary');
}
if(typeof window !== 'undefined'){ window.runMacdPrimaryTest = runMacdPrimaryTest; }

function _renderMacdPrimaryResults(loaded, statusEl, resultEl){
  if(!resultEl){ if(statusEl) statusEl.textContent = '⚠ result host missing'; return; }
  var classes = ['major','minor','comm','index','crypto'];
  var buckets = [4, 3, 2, 1, 0];  // 4 = fully aligned, 0 = fully contrarian

  // Initialise: per-class per-bucket {W, L}
  var agg = {};
  classes.forEach(function(cls){
    agg[cls] = {};
    buckets.forEach(function(b){ agg[cls][b] = {W:0, L:0}; });
  });
  var totals = {};
  buckets.forEach(function(b){ totals[b] = {W:0, L:0}; });

  Object.keys(MKTS).forEach(function(k){
    var cls = (MKTS[k] && MKTS[k].t) || 'unknown';
    if(!agg[cls]) return;
    var rb = loaded[k];
    if(!rb || !rb.perConfluence) return;
    buckets.forEach(function(b){
      var pc = rb.perConfluence[b];
      if(!pc) return;
      agg[cls][b].W += pc.W;
      agg[cls][b].L += pc.L;
      totals[b].W += pc.W;
      totals[b].L += pc.L;
    });
  });

  function wr(W, L){ var N = W + L; return N > 0 ? (W / N * 100) : null; }
  function wrFmt(p){ return p == null ? '—' : p.toFixed(1) + '%'; }
  function wrColor(p, N){
    if(p == null || N < 10) return 'var(--inkd)';
    return p >= 72 ? 'var(--bull)' : p >= 60 ? 'var(--gold)' : 'var(--bear)';
  }
  function cellFor(s){
    var w = wr(s.W, s.L);
    var N = s.W + s.L;
    return '<td style="padding:5px 8px;text-align:right;color:' + wrColor(w, N) + ';font-weight:700;border-left:1px solid rgba(0,0,0,0.06);">'
      + wrFmt(w) + '<div style="color:var(--inkd);font-weight:400;font-size:7.5px;">' + N + '</div></td>';
  }

  var headerCells = '<th style="padding:5px 8px;text-align:left;">Class</th>'
    + buckets.map(function(b){
        var lbl = b + '/4';
        var sub = (b === 4) ? 'fully aligned' : (b === 0) ? 'fully contrarian' : (b + ' agree');
        return '<th style="padding:5px 8px;text-align:right;background:rgba(38,196,120,0.06);border-left:1px solid rgba(0,0,0,0.06);">'
          + '<div>' + lbl + '</div><div style="color:var(--inkd);font-weight:400;font-size:7.5px;">' + sub + '</div></th>';
      }).join('');

  var rows = classes.map(function(cls){
    return '<tr style="border-top:1px dashed rgba(0,0,0,0.08);">'
      + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">' + cls + '</td>'
      + buckets.map(function(b){ return cellFor(agg[cls][b]); }).join('')
      + '</tr>';
  }).join('');

  var totalRow = '<tr style="border-top:2px solid var(--rule);background:rgba(0,0,0,0.02);">'
    + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">ALL</td>'
    + buckets.map(function(b){ return cellFor(totals[b]); }).join('')
    + '</tr>';

  resultEl.innerHTML =
    '<div style="margin-top:8px;padding:10px 12px;border:1px solid rgba(38,196,120,0.30);background:rgba(38,196,120,0.04);border-radius:4px;">'
    + '<div style="font-family:Orbitron,monospace;font-size:10px;font-weight:700;letter-spacing:0.8px;color:#26c478;margin-bottom:6px;">MACD-PRIMARY · WR BY CONFLUENCE BUCKET</div>'
    + '<div style="font-size:8.5px;color:var(--inkd);line-height:1.4;margin-bottom:8px;">Trigger: 15m MACD(12,26,9) / Signal cross. Direction: the cross itself. RSI filter: bull cross needs RSI &lt; 50, bear cross needs RSI &gt; 50 (centerline filter, loosened from 40/60 on 16/06 after the tighter bounds produced ALL=49.1% on n=114 — coin flip). Each trade bucketed by how many of EW/TL/NW/CL agree with the MACD cross direction. Cell shows WR / n. Cells with n &lt; 10 are too small to colour. Deploy threshold: ≥72% WR on ≥20 trades per class × bucket.</div>'
    + '<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;"><table style="width:100%;border-collapse:collapse;font-size:9px;min-width:520px;">'
    + '<thead><tr style="border-bottom:1px solid var(--rule);font-size:8px;color:var(--inkd);letter-spacing:0.5px;">'
    + headerCells
    + '</tr></thead><tbody>' + rows + totalRow + '</tbody></table></div>'
    + '<div style="margin-top:8px;font-size:8.5px;color:var(--inkd);line-height:1.5;"><strong>Read:</strong> a high WR in a high-confluence bucket (3/4 or 4/4) on ≥20 trades = strong deployment candidate. WR holding above 65% across multiple buckets = the MACD cross + RSI filter is doing the work and confluence is a refinement, not a gatekeeper. WR collapsing at low confluence = need at least 2-3 layers of agreement.</div>'
    + '</div>';
  if(statusEl){
    var ts = new Date().toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
    statusEl.textContent = '✓ Simulation ready · ' + ts;
  }
}
if(typeof window !== 'undefined'){ window._renderMacdPrimaryResults = _renderMacdPrimaryResults; }


// ── MACD-PRIMARY ON JPY OUTLIERS (2026-06-17) ──────────────────
// USDJPY (major) and CADJPY (minor) are chronic 4/4+creator under-
// performers. This test runs the existing macd-primary backtest mode
// across all pairs (reuses computeAllRecentBacktestsAsync — same cache
// slot as the green button so a previous run is reused if fresh) and
// renders a per-pair × per-confluence-bucket WR matrix specifically
// for those two pairs. Output mirrors the index deploy decision matrix:
// 4/4 / 3/4 / 2/4 / 1/4 / 0/4 columns × 2 pair rows + an ALL row.
function runMacdPrimaryJpyTest(){
  var btn = document.getElementById('btMacdJpyTestBtn');
  var statusEl = document.getElementById('btMacdJpyTestStatus');
  var resultEl = document.getElementById('btMacdJpyTestResult');
  var origLabel = '🧪 TEST: MACD-PRIMARY ON JPY OUTLIERS (USDJPY · CADJPY)';
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🧪 RUNNING...'; }
  if(resultEl) resultEl.innerHTML = '';

  if(typeof computeAllRecentBacktestsAsync !== 'function'){
    if(statusEl) statusEl.textContent = 'Backtest engine not loaded';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
    return;
  }

  if(statusEl) statusEl.textContent = 'Computing MACD-primary across all pairs (will filter to JPY outliers)…';
  computeAllRecentBacktestsAsync(true, function(done, total){
    if(statusEl) statusEl.textContent = 'Computing — ' + done + '/' + total + ' (filtering to USDJPY + CADJPY at the end)';
  }, function(results){
    _renderMacdPrimaryJpyResults(results || {}, statusEl, resultEl);
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
  }, 'macd-primary');
}
if(typeof window !== 'undefined'){ window.runMacdPrimaryJpyTest = runMacdPrimaryJpyTest; }

function _renderMacdPrimaryJpyResults(loaded, statusEl, resultEl){
  if(!resultEl){ if(statusEl) statusEl.textContent = '⚠ result host missing'; return; }
  var jpyPairs = ['usdjpy', 'cadjpy'];
  var buckets = [4, 3, 2, 1, 0];

  // Aggregate by (pair × bucket)
  var agg = {};
  jpyPairs.forEach(function(p){
    agg[p] = {};
    buckets.forEach(function(b){ agg[p][b] = {W:0, L:0}; });
  });
  var totals = {};
  buckets.forEach(function(b){ totals[b] = {W:0, L:0}; });

  jpyPairs.forEach(function(p){
    var rb = loaded[p];
    if(!rb || !rb.perConfluence) return;
    buckets.forEach(function(b){
      var pc = rb.perConfluence[b];
      if(!pc) return;
      agg[p][b].W += pc.W;
      agg[p][b].L += pc.L;
      totals[b].W += pc.W;
      totals[b].L += pc.L;
    });
  });

  function wr(W, L){ var N = W + L; return N > 0 ? (W / N * 100) : null; }
  function wrFmt(p){ return p == null ? '—' : p.toFixed(1) + '%'; }
  function wrColor(p, N){
    if(p == null || N < 10) return 'var(--inkd)';
    return p >= 72 ? 'var(--bull)' : p >= 60 ? 'var(--gold)' : 'var(--bear)';
  }
  function cellFor(s){
    var w = wr(s.W, s.L);
    var N = s.W + s.L;
    return '<td style="padding:5px 8px;text-align:right;color:' + wrColor(w, N) + ';font-weight:700;border-left:1px solid rgba(0,0,0,0.06);">'
      + wrFmt(w) + '<div style="color:var(--inkd);font-weight:400;font-size:7.5px;">' + N + '</div></td>';
  }

  var headerCells = '<th style="padding:5px 8px;text-align:left;">Pair</th>'
    + buckets.map(function(b){
        var lbl = b + '/4';
        var sub = (b === 4) ? 'fully aligned' : (b === 0) ? 'fully contrarian' : (b + ' agree');
        return '<th style="padding:5px 8px;text-align:right;background:rgba(240,179,64,0.08);border-left:1px solid rgba(0,0,0,0.06);">'
          + '<div>' + lbl + '</div><div style="color:var(--inkd);font-weight:400;font-size:7.5px;">' + sub + '</div></th>';
      }).join('');

  var rows = jpyPairs.map(function(p){
    var label = (MKTS[p] && MKTS[p].sym) || p.toUpperCase();
    return '<tr style="border-top:1px dashed rgba(0,0,0,0.08);">'
      + '<td style="padding:5px 8px;font-weight:700;letter-spacing:0.5px;">' + label + '</td>'
      + buckets.map(function(b){ return cellFor(agg[p][b]); }).join('')
      + '</tr>';
  }).join('');

  var totalRow = '<tr style="border-top:2px solid var(--rule);background:rgba(0,0,0,0.02);">'
    + '<td style="padding:5px 8px;font-weight:700;letter-spacing:0.5px;">COMBINED</td>'
    + buckets.map(function(b){ return cellFor(totals[b]); }).join('')
    + '</tr>';

  resultEl.innerHTML =
    '<div style="margin-top:8px;padding:10px 12px;border:1px solid rgba(240,179,64,0.30);background:rgba(240,179,64,0.04);border-radius:4px;">'
    + '<div style="font-family:Orbitron,monospace;font-size:10px;font-weight:700;letter-spacing:0.8px;color:#c8860a;margin-bottom:6px;">MACD-PRIMARY · JPY OUTLIERS · PER-CONFLUENCE BUCKET</div>'
    + '<div style="font-size:8.5px;color:var(--inkd);line-height:1.4;margin-bottom:8px;">USDJPY (major) and CADJPY (minor) under the same MACD-primary trigger that ships in production for indices. Trigger: 15m MACD/Signal cross with RSI &lt;50 (bull) / &gt;50 (bear) gate, direction from the cross. Each trade bucketed by how many of the four macro layers agree with the cross direction. Per-pair deploy threshold: ≥72% WR on ≥20 trades. If both pairs show that pattern in 3/4 or 4/4, the deploy mirrors the indices: ship per-pair, skip the contrarian 0/4 bucket.</div>'
    + '<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;"><table style="width:100%;border-collapse:collapse;font-size:9px;min-width:520px;">'
    + '<thead><tr style="border-bottom:1px solid var(--rule);font-size:8px;color:var(--inkd);letter-spacing:0.5px;">'
    + headerCells
    + '</tr></thead><tbody>' + rows + totalRow + '</tbody></table></div>'
    + '<div style="margin-top:8px;font-size:8.5px;color:var(--inkd);line-height:1.5;"><strong>Read:</strong> any cell ≥72% on ≥20 trades is a per-pair deployment candidate. If USDJPY 4/4 and CADJPY 3/4 both clear the bar, the deploy is per-pair (mirror the index pattern). If only one shows up clean, deploy just that one. If the COMBINED row also clears the bar, the trigger is robust enough that it justifies a class-wide cohort test for the rest of FX.</div>'
    + '</div>';
  if(statusEl){
    var ts = new Date().toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
    statusEl.textContent = '✓ Simulation ready · ' + ts;
  }
}
if(typeof window !== 'undefined'){ window._renderMacdPrimaryJpyResults = _renderMacdPrimaryJpyResults; }


// ── MACD-DIVERGENCE PHASE 1 (2026-06-17) ───────────────────────
// Standalone test of price/MACD divergence as an entry signal. No
// RSI gate, no confluence requirement at gate time — we want to see
// whether divergence ALONE carries an edge before layering filters
// in Phase 2. Confluence IS tracked per trade so the per-bucket
// matrix still renders and tells us if confluence is a useful refine.
function runMacdDivergenceTest(){
  var btn = document.getElementById('btMacdDivTestBtn');
  var statusEl = document.getElementById('btMacdDivTestStatus');
  var resultEl = document.getElementById('btMacdDivTestResult');
  var origLabel = '🧪 TEST: MACD DIVERGENCE — PHASE 1 (STANDALONE)';
  if(btn){ btn.disabled = true; btn.style.opacity = '0.6'; btn.textContent = '🧪 RUNNING...'; }
  if(resultEl) resultEl.innerHTML = '';

  if(typeof computeAllRecentBacktestsAsync !== 'function'){
    if(statusEl) statusEl.textContent = 'Backtest engine not loaded';
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
    return;
  }

  if(statusEl) statusEl.textContent = 'Computing MACD-divergence cohort…';
  computeAllRecentBacktestsAsync(true, function(done, total){
    if(statusEl) statusEl.textContent = 'Computing MACD-divergence — ' + done + '/' + total;
  }, function(results){
    _renderMacdDivergenceResults(results || {}, statusEl, resultEl);
    if(btn){ btn.disabled = false; btn.style.opacity = '1'; btn.textContent = origLabel; }
  }, 'macd-divergence');
}
if(typeof window !== 'undefined'){ window.runMacdDivergenceTest = runMacdDivergenceTest; }

function _renderMacdDivergenceResults(loaded, statusEl, resultEl){
  if(!resultEl){ if(statusEl) statusEl.textContent = '⚠ result host missing'; return; }
  var classes = ['major','minor','comm','index','crypto'];
  var buckets = [4, 3, 2, 1, 0];

  var agg = {};
  classes.forEach(function(cls){
    agg[cls] = {};
    buckets.forEach(function(b){ agg[cls][b] = {W:0, L:0}; });
  });
  var totals = {};
  buckets.forEach(function(b){ totals[b] = {W:0, L:0}; });

  Object.keys(MKTS).forEach(function(k){
    var cls = (MKTS[k] && MKTS[k].t) || 'unknown';
    if(!agg[cls]) return;
    var rb = loaded[k];
    if(!rb || !rb.perConfluence) return;
    buckets.forEach(function(b){
      var pc = rb.perConfluence[b];
      if(!pc) return;
      agg[cls][b].W += pc.W;
      agg[cls][b].L += pc.L;
      totals[b].W += pc.W;
      totals[b].L += pc.L;
    });
  });

  function wr(W, L){ var N = W + L; return N > 0 ? (W / N * 100) : null; }
  function wrFmt(p){ return p == null ? '—' : p.toFixed(1) + '%'; }
  function wrColor(p, N){
    if(p == null || N < 10) return 'var(--inkd)';
    return p >= 72 ? 'var(--bull)' : p >= 60 ? 'var(--gold)' : 'var(--bear)';
  }
  function cellFor(s){
    var w = wr(s.W, s.L);
    var N = s.W + s.L;
    return '<td style="padding:5px 8px;text-align:right;color:' + wrColor(w, N) + ';font-weight:700;border-left:1px solid rgba(0,0,0,0.06);">'
      + wrFmt(w) + '<div style="color:var(--inkd);font-weight:400;font-size:7.5px;">' + N + '</div></td>';
  }

  var headerCells = '<th style="padding:5px 8px;text-align:left;">Class</th>'
    + buckets.map(function(b){
        var lbl = b + '/4';
        var sub = (b === 4) ? 'fully aligned' : (b === 0) ? 'fully contrarian' : (b + ' agree');
        return '<th style="padding:5px 8px;text-align:right;background:rgba(45,166,166,0.08);border-left:1px solid rgba(0,0,0,0.06);">'
          + '<div>' + lbl + '</div><div style="color:var(--inkd);font-weight:400;font-size:7.5px;">' + sub + '</div></th>';
      }).join('');

  var rows = classes.map(function(cls){
    return '<tr style="border-top:1px dashed rgba(0,0,0,0.08);">'
      + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">' + cls + '</td>'
      + buckets.map(function(b){ return cellFor(agg[cls][b]); }).join('')
      + '</tr>';
  }).join('');

  var totalRow = '<tr style="border-top:2px solid var(--rule);background:rgba(0,0,0,0.02);">'
    + '<td style="padding:5px 8px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">ALL</td>'
    + buckets.map(function(b){ return cellFor(totals[b]); }).join('')
    + '</tr>';

  resultEl.innerHTML =
    '<div style="margin-top:8px;padding:10px 12px;border:1px solid rgba(45,166,166,0.30);background:rgba(45,166,166,0.04);border-radius:4px;">'
    + '<div style="font-family:Orbitron,monospace;font-size:10px;font-weight:700;letter-spacing:0.8px;color:#2da6a6;margin-bottom:6px;">MACD DIVERGENCE · PHASE 1 STANDALONE · PER CONFLUENCE</div>'
    + '<div style="font-size:8.5px;color:var(--inkd);line-height:1.4;margin-bottom:8px;">Trigger: bullish/bearish divergence between price and MACD line at confirmed swing extremes within 30 m15 bars. Bullish = price lower-low + MACD higher-low (exhaustion + reversal). Direction comes from the divergence side. No RSI gate, no confluence threshold at trigger time — confluence is recorded per trade so the bucket pattern surfaces whether confluence is a useful refinement. Cell shows WR / n. Cells with n &lt; 10 are too small to colour. Deploy threshold: ≥72% WR on ≥20 trades per class × bucket.</div>'
    + '<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;"><table style="width:100%;border-collapse:collapse;font-size:9px;min-width:520px;">'
    + '<thead><tr style="border-bottom:1px solid var(--rule);font-size:8px;color:var(--inkd);letter-spacing:0.5px;">'
    + headerCells
    + '</tr></thead><tbody>' + rows + totalRow + '</tbody></table></div>'
    + '<div style="margin-top:8px;font-size:8.5px;color:var(--inkd);line-height:1.5;"><strong>Read:</strong> any cell ≥72% WR on ≥20 trades = deploy-grade. If divergence alone clears the bar broadly, ship it standalone. If only top-confluence buckets clear, confluence becomes the gate. If nothing meaningful clears, Phase 2 layers MACD cross + RSI on top of divergence to see if the combination is stronger than either alone.</div>'
    + '</div>';
  if(statusEl){
    var ts = new Date().toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
    statusEl.textContent = '✓ Simulation ready · ' + ts;
  }
}
if(typeof window !== 'undefined'){ window._renderMacdDivergenceResults = _renderMacdDivergenceResults; }
