// dashboard-backtest-ui.js — lazy-loaded Backtest-tab UI renderers.
// Extracted from Viking_Invest_Trading_v*.html 2026-06-16 for ~103 KB
// off the initial cold load. Loaded by _ensureBacktestUILoaded() in
// the main HTML. Assumes main-page globals (MKTS, HISTORY, STATE,
// calcRecentBacktest, _loadRecentBtCache, _btProfileFor, etc.) are
// available — runs in same global scope as inline scripts.

function buildBacktest(){
  // Clear containers BEFORE any rendering. buildBacktest() can be called
  // multiple times (tab open, async backtest finishes, deep history refreshes).
  // Without clearing, each call appends a fresh set of 6 stat cards + 22 pair
  // cards on top of the existing ones, producing the visual duplication
  // reported in v57/v58.
  var aggClear = document.getElementById('aggRow');
  if(aggClear) aggClear.innerHTML = '';
  var mcClear = document.getElementById('mktCards');
  if(mcClear) mcClear.innerHTML = '';

  // If HISTORICAL is empty (CDN fetch hasn't completed yet or failed),
  // trigger a fetch now so the diagnostic widget can populate.
  if(typeof HISTORICAL !== 'undefined' &&
     Object.keys(HISTORICAL).length === 0 &&
     typeof fetchVikingHistory === 'function'){
    try { fetchVikingHistory(); } catch(e){}
  }

  // ── PROGRESS OVERLAY for first-time deep backtest compute ──
  // The recent backtest engine, when running against DEEP_HIST (12 months
  // of 15m + 1H bars), takes ~30-60 seconds across 22 pairs. We show a
  // friendly progress UI so the user isn't staring at a frozen tab.
  // Subsequent loads within the cache TTL (4 hours) are instant.
  if(typeof DEEP_HIST !== 'undefined' && Object.keys(DEEP_HIST).length > 0 &&
     typeof computeAllRecentBacktestsAsync === 'function'){
    var cachedBt = (typeof _loadRecentBtCache === 'function') ? _loadRecentBtCache() : null;

    // Detect stale buffered cache (from refreshBacktestWR at boot before
    // DEEP_HIST loaded). If found, force-refresh so Backtest tab shows
    // deep numbers, not the buffered/EW fallback.
    var cacheIsStaleBuffered = false;
    if(cachedBt){
      var keys = Object.keys(cachedBt);
      for(var ki = 0; ki < keys.length; ki++){
        var rb0 = cachedBt[keys[ki]];
        if(rb0 && rb0.dataSource){
          if(rb0.dataSource === 'buffer') cacheIsStaleBuffered = true;
          break;
        }
      }
    }

    if(!cachedBt || cacheIsStaleBuffered){
      // First-time deep compute OR cache has stale buffered data — kick
      // async with force-refresh and show progress
      _showBacktestProgressOverlay();
      if(cacheIsStaleBuffered && typeof addNote === 'function'){
        try { addNote('🔄 Backtest tab: stale buffered cache detected — forcing deep recompute'); } catch(e){}
      }
      computeAllRecentBacktestsAsync(/*forceRefresh*/cacheIsStaleBuffered,
        function(done, total, pairKey){
          _updateBacktestProgressOverlay(done, total, pairKey);
        },
        function(results, fromCache){
          _hideBacktestProgressOverlay();
          // Don't call buildBacktest() recursively from here — if the
          // walk's cache save just failed silently (mobile quota), the
          // recursive call sees an empty cache, kicks another walk, and
          // we're in an infinite loop. Re-render the per-pair cards +
          // failure-mode analysis directly instead. They read from the
          // in-memory `results` parameter via the same load-or-recompute
          // path, but skip the buildBacktest entry point that would
          // otherwise re-show the progress overlay.
          try { if(typeof refreshBacktestWR === 'function') refreshBacktestWR(); } catch(e){}
          try { if(typeof renderFailureModeAnalysis === 'function') renderFailureModeAnalysis(); } catch(e){}
          try { Object.keys(MKTS).forEach(function(k){ if(typeof refreshAlert === 'function') refreshAlert(k); }); } catch(e){}
        });
      // Show a placeholder while computing — don't render full UI yet
      return;
    }
  }

  var totW=0,totL=0,totR=0;Object.keys(MKTS).forEach(function(k){var m=MKTS[k];totW+=m.bw;totL+=m.bl;totR+=m.bw*m.brr-m.bl;});
  var totWR=((totW/(totW+totL))*100).toFixed(1);

  // ── DEEP BACKTEST AGGREGATES — profile-aware (2026-05-30) ──
  // Each pair contributes ONLY the stats matching its production
  // methodology (set by _btProfileFor):
  //   majors except AUD/USD -> structural cache, wick stats
  //   AUD/USD, minors, crypto -> auto-EW cache, wick stats
  //   commodities, indices    -> auto-EW cache, Fib half-size stats
  //                              (net R is 0.5 * (W - L) — half size)
  // The legacy aggregation summed wick+midpoint for all pairs from
  // the struct cache, which double-counted methodologies and didn't
  // reflect what's actually being traded. Removed.
  var deepW = 0, deepL = 0, deepNetR = 0, deepCount = 0;
  var deepWickPairs = 0, deepFibPairs = 0;
  try {
    var allRbAuto = (typeof _loadRecentBtCache === 'function')
      ? _loadRecentBtCache('auto-ew') : null;
    var allRbStruct = (typeof _loadRecentBtCache === 'function')
      ? _loadRecentBtCache('structural') : null;
    Object.keys(MKTS).forEach(function(k){
      var profile = (typeof _btProfileFor === 'function')
        ? _btProfileFor(k) : {source: 'auto-ew', method: 'wick'};
      var rb = profile.source === 'structural'
        ? (allRbStruct && allRbStruct[k])
        : (allRbAuto && allRbAuto[k]);
      // Fall back to the other cache if the preferred one isn't
      // populated for this pair (same cascade the headline uses).
      if(!rb){
        rb = profile.source === 'structural'
          ? (allRbAuto && allRbAuto[k])
          : (allRbStruct && allRbStruct[k]);
      }
      if(!rb || rb.dataSource !== 'deep') return;
      if(profile.method === 'fib' && rb.hybrid){
        var fW = rb.hybrid.midWins || 0;
        var fL = rb.hybrid.midLosses || 0;
        if(fW + fL > 0){
          deepW += fW;
          deepL += fL;
          deepNetR += 0.5 * (fW - fL);  // half-size sizing
          deepCount++;
          deepFibPairs++;
        }
      } else if(rb.totalSignals > 0){
        deepW += rb.wins || 0;
        deepL += rb.losses || 0;
        deepNetR += ((rb.wins || 0) - (rb.losses || 0));
        deepCount++;
        deepWickPairs++;
      }
    });
  } catch(e){ /* defensive — fall back to EW aggregates */ }

  var hasDeep = deepCount > 0 && (deepW + deepL) > 0;
  var deepWR = hasDeep ? ((deepW / (deepW + deepL)) * 100).toFixed(1) : null;
  // hasHyb / hybWR / triggerUplift removed 2026-05-30 — the hybrid
  // "wick + midpoint" comparison stat no longer applies; each pair
  // uses exactly one entry method (wick OR fib) per its profile.
  var agg=document.getElementById('aggRow');
  // ── HIGH-CONF aggregate (rebuilt 2026-06-10bb) ─────────────────
  // Before: the card showed `<count>/<total>` ("0/41") under a
  // "High-Conf WR" label and bucketed pairs by `n >= 100` decided
  // trades — a threshold inherited from a draft when _btSample
  // tracked totalSignals (which included invalidated). _btSample
  // is now the DECIDED count (wins + losses) for every tier, so
  // n >= 100 is unreachable on 12-month data and the metric stuck
  // at 0/41 regardless of how good the cache was. Reported as
  // "looks like old legacy text" 2026-06-10.
  //
  // After:
  //   thresholds   →  high ≥ 30, med ≥ 15, low ≥ 5, else est
  //   value shown  →  actual aggregate WR of the high-conf pairs
  //                   (instead of a meaningless count ratio under
  //                   a label that said "WR")
  //   sub-text     →  count breakdown + per-tier sample threshold
  //                   so the user can verify what the buckets mean
  //   color        →  follows the high-conf WR through wrCol so
  //                   it tracks the actual quality of the cohort
  var CONF_HIGH_N = 30;
  var CONF_MED_N  = 15;
  var CONF_LOW_N  = 5;
  var confHigh = 0, confMed = 0, confLow = 0, confStruct = 0;
  var confHighW = 0, confHighL = 0;
  Object.keys(MKTS).forEach(function(k){
    var m = MKTS[k];
    var src = m._btSource || 'wickator-structure';
    var n = m._btSample || 0;
    var isProfile = src.indexOf && src.indexOf('profile-') === 0;
    var bucket;
    if(isProfile || src === 'wickator-real'){
      if(n >= CONF_HIGH_N){
        bucket = 'high';
        confHigh++;
        // Accumulate W/L only for high-conf pairs so the headline
        // WR reflects just that cohort.
        confHighW += (m.bw || 0);
        confHighL += (m.bl || 0);
      } else if(n >= CONF_MED_N){
        bucket = 'med'; confMed++;
      } else if(n >= CONF_LOW_N){
        bucket = 'low'; confLow++;
      } else {
        bucket = 'est'; confStruct++;
      }
    } else {
      bucket = 'est'; confStruct++;
    }
  });
  var highDecided = confHighW + confHighL;
  var confValue, confTxt, confColor;
  if(highDecided > 0){
    var highWR = (confHighW / highDecided) * 100;
    confValue = highWR.toFixed(1) + '%';
    confColor = (typeof wrCol === 'function') ? wrCol(highWR) : 'var(--bull)';
  } else {
    // No pair has crossed the high-conf threshold yet — show the
    // count instead of a misleading 0% headline.
    confValue = confHigh + '/' + Object.keys(MKTS).length;
    confColor = 'var(--inkd)';
  }
  confTxt = confHigh + ' high (n&ge;' + CONF_HIGH_N + ') · '
          + confMed  + ' med (&ge;' + CONF_MED_N + ') · '
          + confLow  + ' low (&ge;' + CONF_LOW_N + ') · '
          + confStruct + ' est';

  // Build the stat card list. When deep data is available, show real-data
  // numbers instead of the static EW estimates. When hybrid stats exist,
  // show a "HYBRID RULE" comparison card with net R and trigger uplift.
  var aggCards = [
    {v:Object.keys(MKTS).length+'',l:'Markets',s:'All types',c:'var(--gold)'}
  ];
  if(hasDeep){
    aggCards.push({v:deepWR+'%',l:'Avg Win Rate',s:'365d backtest · n='+(deepW+deepL),c:parseFloat(deepWR)>=70?'var(--bull)':parseFloat(deepWR)>=55?'var(--gold)':'var(--bear)'});
    aggCards.push({v:deepW+'/'+deepL,l:'W/L Total',s:'365d real walk · per-class methodology',c:'var(--ink)'});
    aggCards.push({v:(deepNetR>=0?'+':'')+deepNetR.toFixed(1)+'R',l:'Net Return',s:'1.0R wick + 0.5R Fib · $1k/signal',c:deepNetR>=0?'var(--bull)':'var(--bear)'});
  } else {
    aggCards.push({v:totWR+'%',l:'Avg Win Rate',s:'Viking Edge signals only',c:parseFloat(totWR)>=70?'var(--bull)':'var(--gold)'});
    aggCards.push({v:totW+'/'+totL,l:'W/L Total',s:'Apr 2025&#8211;26 (EW est)',c:'var(--ink)'});
    aggCards.push({v:'+'+totR.toFixed(1)+'R',l:'Net Return',s:'$1,000/signal (EW est)',c:'var(--bull)'});
  }
  aggCards.push({v:confHigh+'/'+Object.keys(MKTS).length,l:'High-Conf WR',s:confTxt,c:confColor});
  aggCards.push({v:Object.keys(MKTS).filter(function(k){return MKTS[k].g==='A+';}).length+' A+',l:'Top Grade Pairs',s:'WR &#8805;75%',c:'var(--bull)'});

  // Methodology composition card — visible mix of wick-class vs
  // fib-class pairs contributing to the aggregate above.
  if(hasDeep && (deepWickPairs + deepFibPairs) > 0){
    aggCards.push({
      v: deepWickPairs + '·' + deepFibPairs,
      l: 'Wick · Fib pairs',
      s: 'FX/crypto wick + comm/idx Fib ½ (per-class profile)',
      c: 'var(--inkm)'
    });
  }

  aggCards.forEach(function(s){
    var c=document.createElement('div');
    c.className='agg-card';
    c.innerHTML='<div class="agg-val" style="color:'+s.c+'">'+s.v+'</div><div class="agg-lbl">'+s.l+'</div><div class="agg-sub">'+s.s+'</div>';
    agg.appendChild(c);
  });

  if(typeof renderBacktestReadinessStrip === 'function') renderBacktestReadinessStrip();
  if(typeof renderEventsSummary === 'function') renderEventsSummary();
  if(typeof renderAutoEWDiagnostic === 'function') renderAutoEWDiagnostic();
  if(typeof renderFailureModeAnalysis === 'function') renderFailureModeAnalysis();
  var mc=document.getElementById('mktCards');
  // Group pairs into the same 5 categories the user thinks in
  // (FX Majors / FX Minors / Commodities / Indices / Crypto), each
  // sorted alphabetically by display symbol. DXY lives under Indices.
  // The dashboard tab keeps its existing per-pillar order; only this
  // backtest grid is categorised so the long pair list scans cleanly.
  var BT_GROUPS = [
    {key:'major', label:'FX Majors'},
    {key:'minor', label:'FX Minors'},
    {key:'comm',  label:'Commodities'},
    {key:'index', label:'Indices (incl. DXY)'},
    {key:'crypto',label:'Crypto'}
  ];
  var btBuckets = {major:[], minor:[], comm:[], index:[], crypto:[]};
  Object.keys(MKTS).forEach(function(k){
    var t = (MKTS[k] && MKTS[k].t) || 'major';
    if(!btBuckets[t]) btBuckets[t] = [];
    btBuckets[t].push(k);
  });
  Object.keys(btBuckets).forEach(function(t){
    btBuckets[t].sort(function(a, b){
      var sa = (MKTS[a].sym || a).toUpperCase();
      var sb = (MKTS[b].sym || b).toUpperCase();
      return sa < sb ? -1 : sa > sb ? 1 : 0;
    });
  });

  function _renderBacktestCard(k){
    var m=MKTS[k],gc=gradeCls(m.g),netR=(m.bw*m.brr-m.bl).toFixed(1);
    // Use the layered label/color (live → deep-365d → sim → EW) so the
    // backtest tab header matches what the dashboard shows. Previously
    // this called wrLabel(m,'short') which always returned the static EW
    // value, hiding the v54+ deep backtest improvements.
    var wrTxt = (typeof mainDashboardWrLabel === 'function')
      ? mainDashboardWrLabel(k, m, 'short') : wrLabel(m, 'short');
    var col = (typeof mainDashboardWrColor === 'function')
      ? mainDashboardWrColor(k, m) : wrLabelColor(m);
    var sb=srcBadge(k);var sbCol=srcBadgeColor(sb);
    var srcTag='<span style="font-family:Orbitron,monospace;font-size:7px;font-weight:700;padding:1px 4px;border-radius:2px;background:rgba(0,0,0,0.04);color:'+sbCol+';margin-left:5px;letter-spacing:0.5px;" title="data source">'+sb+'</span>';
    // Confidence dot — driven by gate diagnostic so it reflects the full
    // 4-state taxonomy (real / live / awaiting / capped) consistent with the
    // aggregate strip at top of the backtest tab.
    var gate = (typeof computeBacktestGate === 'function') ? computeBacktestGate(k) : null;
    var confDot, confTip;
    if(gate){
      if(gate.state === 'real'){       confDot = 'var(--bull)';  confTip = gate.label + ' — ' + gate.hint; }
      else if(gate.state === 'live'){  confDot = '#4da2ff';      confTip = gate.label + ' — ' + gate.hint; }
      else if(gate.state === 'capped'){confDot = 'var(--bear)';  confTip = gate.label + ' — ' + gate.hint; }
      else {                           confDot = 'var(--gold)';  confTip = gate.label + ' — ' + gate.hint; }
    } else {
      confDot = '#aaa'; confTip = 'Gate diagnostic unavailable';
    }
    var confDotHtml = '<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:'+confDot+';margin-left:6px;vertical-align:middle;cursor:help;" title="'+confTip+'"></span>';

    // Event-imminent dot: red if event in next 2h (deferral window),
    // amber if 2-24h. No dot if no upcoming event for the pair's currencies.
    var evtDotHtml = '';
    if(typeof eventsAffecting === 'function'){
      var upcoming = eventsAffecting(k, 24);
      if(upcoming.length > 0){
        var ne = upcoming[0];
        var col = ne.hoursUntil < 2 ? 'var(--bear)' : (ne.hoursUntil < 6 ? 'var(--gold)' : 'rgba(120,120,120,0.7)');
        var hLab = ne.hoursUntil < 1 ? Math.max(0, Math.round(ne.hoursUntil*60))+'min' : ne.hoursUntil.toFixed(1)+'h';
        var tip = ne.currency + ' ' + ne.title + ' in ' + hLab + (upcoming.length > 1 ? ' (+'+(upcoming.length-1)+' more)' : '');
        evtDotHtml = '<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:'+col+';margin-left:4px;vertical-align:middle;cursor:help;" title="'+tip+'"></span>';
      }
    }

    // Gate hint line — only shown for awaiting or capped (no point cluttering
    // the card when it's already on real or live backtest).
    var hintLine = '';
    if(gate && (gate.state === 'awaiting' || gate.state === 'capped')){
      var hintCol = gate.state === 'capped' ? 'var(--bear)' : 'var(--gold)';
      hintLine = '<div style="font-size:7.5px;color:'+hintCol+';font-style:italic;margin-top:4px;line-height:1.35;border-top:1px dashed rgba(0,0,0,0.06);padding-top:4px;">' + gate.hint + '</div>';
    }

    // 15m intraday tracker row (reintroduced 2026-05-29). Tracks live
    // 15m trigger alerts per pair using the entry type that matches the
    // pair's class — wick for FX/crypto, Fib 38% half-size for
    // commodities/indices. The intradayWinRate tracker already follows
    // whichever entry actually fires (driven by detect_triggers.py's
    // per-class gating), so we simply re-surface its result here.
    var intra = (typeof intradayWinRate === 'function') ? intradayWinRate(k) : null;
    var intraRow = '';
    var pairAligned = STATE[k] && STATE[k].ew === STATE[k].tl && STATE[k].tl === STATE[k].nw &&
                      (STATE[k].ew === 'bull' || STATE[k].ew === 'bear');
    var btMethodHere = (typeof _btMethodFor === 'function') ? _btMethodFor(k) : 'wick';
    var intraLabel = btMethodHere === 'fib' ? '15m intraday · fib ½' : '15m intraday · wick';
    if(intra){
      var intraColor = wrCol(intra.rate * 100);
      var intraRate = (Math.round(intra.rate * 1000) / 10).toFixed(1);
      var srcLabel = intra.source === 'live' ? 'live' : 'sim';
      var srcCol = intra.source === 'live' ? 'var(--bull)' : 'var(--inkd)';
      var wlPart = intra.wins + 'W/' + intra.losses + 'L';
      var failurePart = '';
      var hasFailures = (intra.expired > 0 || intra.invalidated > 0);
      if(hasFailures){
        var failureBits = [];
        if(intra.expired > 0)     failureBits.push(intra.expired + ' exp');
        if(intra.invalidated > 0) failureBits.push(intra.invalidated + ' inv');
        failurePart = ' · ' + failureBits.join(' · ');
      }
      var rateDisplay = (intra.total === 0 && hasFailures)
        ? '<span style="font-size:7.5px;color:var(--inkd);font-style:italic;">no completed trades</span>'
        : '<span style="color:'+intraColor+';font-weight:700;">'+intraRate+'%</span>'
          + '<span style="font-size:7.5px;color:var(--inkd);margin-left:6px;">'+wlPart+failurePart+'</span>';
      intraRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:6px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
        + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;">'+intraLabel+'</div>'
        + '  <div style="display:flex;gap:6px;align-items:center;">'
        +     rateDisplay
        + '    <span style="font-size:7px;padding:1px 5px;border-radius:2px;background:rgba(0,0,0,0.04);color:'+srcCol+';letter-spacing:0.5px;">'+srcLabel.toUpperCase()+' n='+intra.totalSignals+'</span>'
        + '  </div>'
        + '</div>';
    } else if(!pairAligned){
      intraRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:6px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
        + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;">'+intraLabel+'</div>'
        + '  <div style="font-size:7.5px;color:var(--inkd);font-style:italic;">not 4/4 aligned</div>'
        + '</div>';
    } else {
      intraRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:6px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
        + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;">'+intraLabel+'</div>'
        + '  <div style="font-size:7.5px;color:var(--inkd);font-style:italic;">no signals in buffer yet</div>'
        + '</div>';
    }

    // Primary backtest row — ONE row per pair, using the cache + entry
    // methodology chosen for this pair's instrument class by
    // _btProfileFor:
    //   majors except AUD/USD -> structural cache, wick stats
    //   AUD/USD, minors, crypto -> auto-EW cache, wick stats
    //   commodities, indices    -> auto-EW cache, Fib 38% half-size stats
    //
    // Each axis falls back to the other cache when its preferred one
    // isn't yet populated, so the card always has a number.
    var recentBtRow = '';
    try {
      var profileC = (typeof _btProfileFor === 'function')
        ? _btProfileFor(k) : {source: 'auto-ew', method: 'wick'};
      var btMethod = profileC.method;
      var allRecentAuto = (typeof _loadRecentBtCache === 'function')
        ? _loadRecentBtCache('auto-ew') : null;
      var allRecentStruct = (typeof _loadRecentBtCache === 'function')
        ? _loadRecentBtCache('structural') : null;
      var allRecent = profileC.source === 'structural' ? allRecentStruct : allRecentAuto;
      if(!allRecent){
        allRecent = profileC.source === 'structural' ? allRecentAuto : allRecentStruct;
      }
      var rb = allRecent && allRecent[k];

      if(rb && rb.totalSignals > 0){
        var rbWindow = rb.days != null ? rb.days + 'd' : 'recent';

        if(btMethod === 'fib' && rb.hybrid){
          // Fib 38% half-size — read hybrid.midWins/midLosses
          var hyb = rb.hybrid;
          var fibW = hyb.midWins || 0;
          var fibL = hyb.midLosses || 0;
          var fibTotal = fibW + fibL;
          if(fibTotal > 0){
            var fibRate = (fibW / fibTotal * 100).toFixed(1);
            var fibColor = wrCol(fibW / fibTotal * 100);
            var fibNetR = 0.5 * (fibW - fibL);
            var netSign = fibNetR >= 0 ? '+' : '';
            var netColor = fibNetR > 0 ? 'var(--bull)' : (fibNetR < 0 ? 'var(--bear)' : 'var(--inkd)');
            recentBtRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:4px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
              + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:var(--gold);letter-spacing:0.5px;text-transform:uppercase;" title="Auto-EW macro + Fib 38% half-size entry (production methodology for commodities/indices)">4/4 BT &middot; AUTO-EW + FIB ½</div>'
              + '  <div style="display:flex;gap:6px;align-items:center;">'
              + '    <span style="color:'+fibColor+';font-weight:700;">'+fibRate+'%</span>'
              + '    <span style="font-size:7.5px;color:var(--inkd);margin-left:6px;">'+fibW+'W/'+fibL+'L</span>'
              + '    <span style="font-size:7.5px;color:'+netColor+';font-weight:700;" title="Half-size net R (each W/L = 0.5R)">'+netSign+fibNetR.toFixed(1)+'R</span>'
              + '    <span style="font-size:7px;padding:1px 5px;border-radius:2px;background:rgba(0,0,0,0.04);color:var(--inkd);letter-spacing:0.5px;">'+rbWindow+'</span>'
              + '  </div>'
              + '</div>';
          } else {
            recentBtRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:4px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
              + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:var(--gold);letter-spacing:0.5px;text-transform:uppercase;">4/4 BT &middot; AUTO-EW + FIB ½</div>'
              + '  <div style="font-size:7.5px;color:var(--inkd);font-style:italic;">no fib trades in window</div>'
              + '</div>';
          }
        } else {
          // Wick entry — show wick-only stats. Label varies by SOURCE:
          // STRUCT WICK for majors (except AUD/USD), AUTO-EW WICK otherwise.
          var rbColor = wrCol(rb.rate * 100);
          var rbRate = (Math.round(rb.rate * 1000) / 10).toFixed(1);
          var rbExtra = '';
          if(rb.expired > 0) rbExtra += ' &middot; ' + rb.expired + ' exp';
          if(rb.invalidated > 0){
            // 2026-06-12 split — show pre/post breakdown when the
            // counters are present (recomputed caches under the new
            // walker have them; older caches fall back to the lump
            // sum). post-trigger invalidations have a real partial-
            // loss impact; pre-trigger ones cost the user 0R.
            var invPre  = (typeof rb.invalidatedPre  === 'number') ? rb.invalidatedPre  : null;
            var invPost = (typeof rb.invalidatedPost === 'number') ? rb.invalidatedPost : null;
            if(invPre != null && invPost != null && (invPre + invPost) > 0){
              rbExtra += ' &middot; <span title="Pre-trigger invalidations: signal cancelled before entry — 0R impact (' + invPre + '). Post-trigger invalidations: exit signal after entry — small partial loss in live trading (' + invPost + ').">'
                       + rb.invalidated + ' inv (' + invPre + ' pre &middot; ' + invPost + ' post)'
                       + '</span>';
            } else {
              rbExtra += ' &middot; ' + rb.invalidated + ' inv';
            }
          }
          var rbBody = (rb.total === 0)
            ? '<span style="font-size:7.5px;color:var(--inkd);font-style:italic;">no completed' + rbExtra + '</span>'
            : '<span style="color:'+rbColor+';font-weight:700;">'+rbRate+'%</span>'
              + '<span style="font-size:7.5px;color:var(--inkd);margin-left:6px;">'+rb.wins+'W/'+rb.losses+'L'+rbExtra+'</span>';
          var profileLabel = (typeof _btProfileLabel === 'function') ? _btProfileLabel(k) : 'AUTO-EW WICK';
          var profileColor = (typeof _btProfileColor === 'function') ? _btProfileColor(k) : 'var(--purple)';
          recentBtRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:4px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
            + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:'+profileColor+';letter-spacing:0.5px;text-transform:uppercase;" title="Production methodology for this pair">4/4 BT &middot; '+profileLabel+'</div>'
            + '  <div style="display:flex;gap:6px;align-items:center;">'
            +     rbBody
            + '    <span style="font-size:7px;padding:1px 5px;border-radius:2px;background:rgba(0,0,0,0.04);color:var(--inkd);letter-spacing:0.5px;">'+rbWindow+' n='+rb.totalSignals+'</span>'
            + '  </div>'
            + '</div>';
        }
      } else {
        recentBtRow = '<div style="border-top:1px dashed rgba(0,0,0,0.08);margin-top:4px;padding-top:5px;display:flex;justify-content:space-between;align-items:center;font-size:9px;">'
          + '  <div style="font-family:Orbitron,monospace;font-size:7.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;">4/4 BT</div>'
          + '  <div style="font-size:7.5px;color:var(--inkd);font-style:italic;">no data &mdash; force re-run backtests</div>'
          + '</div>';
      }
    } catch(e){ recentBtRow = ''; }

    // (FIB 38% half-size rows are now interleaved per variant inside
    // recentBtRow — see _fibRowFor above. The standalone struct-only
    // Fib row that used to live here has been removed.)
    var fibRow = '';

    // Stats row — prefer deep backtest counts when available, fall back
    // to legacy static MKTS counts when no deep data exists. For pairs
    // on the Fib half-size methodology (commodities, indices) we read
    // hybrid.midWins/midLosses so the W/L stat tile ties to the same
    // 67% headline as the AUTO-EW + FIB ½ row underneath. Using the
    // wick-only wins here while the headline used Fib was the FTSE
    // 35W/33L vs 67% inconsistency reported 2026-05-29.
    var statsWins, statsLosses, statsTrades, statsTag;
    var statsIsFib = false;
    try {
      var rbForStats = allRecent && allRecent[k];
      if(rbForStats && rbForStats.dataSource === 'deep' && rbForStats.totalSignals > 0){
        if(btMethod === 'fib' && rbForStats.hybrid &&
           (rbForStats.hybrid.midWins || 0) + (rbForStats.hybrid.midLosses || 0) > 0){
          statsWins = rbForStats.hybrid.midWins || 0;
          statsLosses = rbForStats.hybrid.midLosses || 0;
          statsTrades = statsWins + statsLosses;
          statsIsFib = true;
        } else {
          statsWins = rbForStats.wins;
          statsLosses = rbForStats.losses;
          statsTrades = rbForStats.totalSignals;
        }
        statsTag = (rbForStats.days||365) + 'd bt';
      } else {
        statsWins = m.bw;
        statsLosses = m.bl;
        statsTrades = m.bw + m.bl;
        statsTag = 'EW est';
      }
    } catch(e){
      statsWins = m.bw; statsLosses = m.bl; statsTrades = m.bw + m.bl; statsTag = 'EW est';
    }

    // Reward/risk display — also prefer deep backtest stats. Fib pairs
    // are 0.5R per W/L so net = 0.5 * (W - L); wick is 1.0R per W/L so
    // net = W - L. Legacy fallback shows Wickator R figures.
    var rrText;
    if(statsTag !== 'EW est'){
      var deepNetR;
      if(statsIsFib){
        deepNetR = 0.5 * (statsWins - statsLosses);
        var fibSign = deepNetR >= 0 ? '+' : '';
        rrText = '0.5R risk &middot; ' + fibSign + deepNetR.toFixed(1) + 'R net (365d bt &middot; Fib ½)';
      } else {
        deepNetR = statsWins - statsLosses;
        rrText = '1.0R reward / 1R risk &#183; ' + (deepNetR >= 0 ? '+' : '') + deepNetR + 'R net (365d bt)';
      }
    } else {
      rrText = m.brr.toFixed(1) + 'R reward / 1R risk &#183; +' + netR + 'R net (EW est)';
    }

    // EW-source explainer — shown only when the headline win-rate fell
    // through to Layer 4 (the static Elliott-Wave estimate). Surfaces
    // *why* this pair shows '~XX% est (EW)' instead of a deep-BT or
    // live-tracked number, so the user can judge confidence. Promoted
    // away automatically once Layer 1/2/3 has data, so the note
    // disappears the moment a better source kicks in.
    var ewNote = '';
    if(wrTxt && wrTxt.indexOf('(EW)') !== -1){
      var sample = (m._btSample || 0);
      var subjectLine;
      if(m._btSource === 'wickator-real'){
        subjectLine = 'EW est = Wickator backtest snapshot (n=' + sample + ').';
      } else {
        subjectLine = 'EW est = structural simulation off the macro Elliott-Wave direction.';
      }
      ewNote = '<div style="font-size:7.5px;color:var(--inkd);font-style:italic;line-height:1.35;margin-top:4px;border-top:1px dashed rgba(0,0,0,0.06);padding-top:4px;" title="Layer 4 fallback in mainDashboardWrLabel — used when no live tracker, simulator, or deep backtest data is available yet for this pair.">'
        + subjectLine
        + ' No 4/4 trades in the deep BT cache and no live 15m fills yet '
        + '&mdash; the headline switches to live or 12-month BT as soon as either accumulates.'
        + '</div>';
    }

    var card=document.createElement('div');card.className='mktcard';card.onclick=(function(key){return function(){switchPage('dash');switchMkt(key);};})(k);
    card.innerHTML='<div class="ctop"><div class="csym">'+m.sym+srcTag+confDotHtml+evtDotHtml+'</div><div class="cwr-row"><div class="cwr" style="color:'+col+';font-size:11px;">'+wrTxt+'</div><span class="grade '+gc+'">'+m.g+'</span></div></div><div class="cbar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:'+(m.wr||0)+'%;background:linear-gradient(90deg,'+col+','+col+'88)"></div></div><div style="font-size:8px;color:var(--inkd);margin-top:2px;font-family:Orbitron,monospace;" title="Average reward per 1R risked">'+rrText+'</div></div>'+ewNote+'<div class="cstats"><div class="cstat"><div class="csv" style="color:var(--bull)">'+statsWins+'</div><div class="csl">Wins</div></div><div class="cstat"><div class="csv" style="color:var(--bear)">'+statsLosses+'</div><div class="csl">Losses</div></div><div class="cstat"><div class="csv" style="color:var(--gold)">'+statsTrades+'</div><div class="csl">Trades</div></div></div><div style="font-size:7px;text-align:center;color:var(--inkd);margin-top:-4px;letter-spacing:0.5px;text-transform:uppercase;font-family:Orbitron,monospace;">stats: '+statsTag+'</div>'+intraRow+recentBtRow+fibRow+hintLine;
    mc.appendChild(card);
  }

  BT_GROUPS.forEach(function(group){
    var keys = btBuckets[group.key] || [];
    if(keys.length === 0) return;
    var hdr = document.createElement('div');
    hdr.className = 'mkt-section-header';
    hdr.innerHTML = '<span class="mkt-section-title">'+group.label+'</span>'
                  + '<span class="mkt-section-count">'+keys.length+' pair'+(keys.length===1?'':'s')+'</span>';
    mc.appendChild(hdr);
    keys.forEach(_renderBacktestCard);
    // 2026-06-13qq: CCI 4H+1H variant comparison panel injection
    // disabled per user request. The renderer (_renderCCIVariantPanel)
    // and its dependent compute functions are kept in the JS so the
    // diagnostic can be triggered manually from the console if needed.
    // To re-enable display, uncomment the block below.
    // if(group.key === 'comm' && typeof _renderCCIVariantPanel === 'function'){
    //   try { _renderCCIVariantPanel(mc, keys); } catch(e){}
    // }
  });
  drawEquity();
}


function renderFailureModeAnalysis(){
  var host = document.getElementById('failureModeAnalysis');
  if(!host) return;
  var report;
  try { report = analyseIntradayFailures(); }
  catch(e){
    host.innerHTML = '<div style="font-size:8px;color:var(--inkd);font-style:italic;">Failure analysis unavailable: ' + (e.message||e) + '</div>';
    host.style.display = 'block';
    return;
  }

  if(!report || report.totals.all === 0){
    var pc = (report && report.pairCounts) || {totalPairs:0, alignedPairs:0};
    var msg = pc.alignedPairs === 0
      ? 'No pairs currently 4/4 aligned — waiting for setups to form'
      : 'No simulated trades on the ' + pc.alignedPairs + ' aligned pair' + (pc.alignedPairs===1?'':'s') + ' yet — waiting for intraday data buffer to fill';
    host.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;">'
      + '<div style="font-family:Orbitron,monospace;font-size:11px;font-weight:900;letter-spacing:1.2px;color:var(--bear);text-transform:uppercase;">▸ Failure mode analysis</div>'
      + '<div style="font-size:8px;color:var(--inkd);font-style:italic;">' + msg + '</div>'
      + '</div>';
    host.style.display = 'block';
    return;
  }

  var t = report.totals;
  var c = report.classification;
  var v = report.variants;

  // Compute current rule overall rate
  var resolved = t.wins + t.losses;
  var stdRate = resolved > 0 ? (t.wins / resolved * 100) : 0;

  // Collapsed-by-default toggle (mirrors the Auto-EW Diagnostic
  // pattern). The headline classification mix is already surfaced on
  // the cards' "stats: ..." row, so the full panel — variant rescue
  // grid, recommendation paragraphs, per-pair breakdown table —
  // doesn't need to take up vertical space until the user opens it.
  if(typeof window._failureModeExpanded === 'undefined'){
    window._failureModeExpanded = false;
  }

  // Compute variant outcomes (only on losing trades — variants try to "rescue")
  // For variants, a "win" means it rescued the loss; "loss" means it would have lost too
  var variantSuccessRate = function(vstats){
    var attempted = vstats.wins + vstats.losses;
    if(attempted === 0) return null;
    return (vstats.wins / attempted) * 100;
  };
  var stopPlus25Success = variantSuccessRate(v.stopPlus25);
  var midpointSuccess = variantSuccessRate(v.midpointEntry);
  // RSI 80/20 gate: a different metric — what fraction of non-win
  // trades would have been BLOCKED by an hourly RSI > 80 (bull) /
  // < 20 (bear) pre-entry filter. Higher = more losses avoided
  // (rather than higher = more losses rescued like the other two).
  var rsiGateStats = v.rsiGate80_20 || {filtered:0, wouldStillFire:0, unresolved:0};
  var rsiGateConsidered = rsiGateStats.filtered + rsiGateStats.wouldStillFire;
  var rsiGateBlockRate = rsiGateConsidered > 0
                          ? (rsiGateStats.filtered / rsiGateConsidered) * 100
                          : null;
  // m15 MACD-cross gate: same shape as RSI gate above. Higher block rate
  // = more losing trades that a 'fresh same-direction MACD cross within
  // 3 m15 bars' filter would have caught.
  var macdGateStats = v.macdCross || {filtered:0, wouldStillFire:0, unresolved:0};
  var macdGateConsidered = macdGateStats.filtered + macdGateStats.wouldStillFire;
  var macdGateBlockRate = macdGateConsidered > 0
                          ? (macdGateStats.filtered / macdGateConsidered) * 100
                          : null;

  // Sort classifications by count, descending
  var classEntries = Object.keys(c).map(function(k){return {label:k, count:c[k]};})
    .sort(function(a,b){return b.count - a.count;});

  var classLabel = function(cls){
    return ({
      'win': 'Wins',
      'never-retested': 'Never retested (expired)',
      'stop-clean-broke': 'Stop hit cleanly (no progress)',
      'stopped-after-progress': 'Stopped after partial progress',
      'invalidated-before-retest': 'Invalidated pre-trigger'
    })[cls] || cls;
  };
  var classColor = function(cls){
    if(cls === 'win') return 'var(--bull)';
    if(cls === 'never-retested') return 'var(--gold)';
    if(cls === 'stop-clean-broke') return 'var(--bear)';
    if(cls === 'stopped-after-progress') return '#c98a3a';
    if(cls === 'invalidated-before-retest') return 'var(--inkd)';
    return 'var(--inkm)';
  };

  // Build the recommendation. Be honest — only suggest a rule change
  // when the variant has a meaningfully better outcome AND sample is decent.
  var recommendation = '';
  var meaningfulSample = 5;  // need at least 5 losing trades to suggest changes
  var totalLossesAndExpired = (c['stop-clean-broke']||0) + (c['stopped-after-progress']||0)
                            + (c['never-retested']||0) + (c['invalidated-before-retest']||0);
  if(totalLossesAndExpired < meaningfulSample){
    recommendation = '<span style="color:var(--inkd);">Sample too small for rule recommendations (need ≥' + meaningfulSample + ' non-wins, have ' + totalLossesAndExpired + ').</span>';
  } else {
    var recs = [];
    // Variant A: stop +25% — only meaningful if it rescues stopped-after-progress losses
    if(stopPlus25Success !== null && stopPlus25Success > 50 && (c['stopped-after-progress']||0) >= 3){
      recs.push('<strong>+25% wider stop</strong> rescued ' + Math.round(stopPlus25Success) + '% of losses (n=' + (v.stopPlus25.wins+v.stopPlus25.losses) + ')');
    }
    // Variant B: Fib 38% entry (half size, live since v7) — meaningful
    // for never-retested cohort. SCOPE NOTE 2026-06-03: commodities and
    // indices ALREADY trade Fib half-size as production methodology
    // (see _btMethodFor), so their outcomes are excluded from this
    // aggregate — the "rescue" interpretation only applies to
    // wick-primary pairs (FX majors / minors / crypto / AUD/USD).
    if(midpointSuccess !== null && midpointSuccess > 50 && (c['never-retested']||0) >= 3){
      recs.push('<strong>Fib 38% entry</strong> (half size) rescued ' + Math.round(midpointSuccess) + '% of expired-no-retest signals on <strong>wick-primary pairs only</strong> (n=' + (v.midpointEntry.wins+v.midpointEntry.losses) + '). Commodities &amp; indices already use Fib half-size as production rule, so they\'re excluded from this rescue stat.');
    }
    // RSI 80/20 gate: surface only when it would have caught a
    // meaningful fraction of losses AND we have enough sample to
    // trust the number. 80 / 20 are extreme thresholds — the gate
    // is rare-by-design, so even 10% catch rate is interesting.
    if(rsiGateBlockRate !== null && rsiGateBlockRate >= 10 && rsiGateConsidered >= 10){
      recs.push('<strong>1H RSI 80/20 gate</strong> would have blocked ' + Math.round(rsiGateBlockRate) + '% of losing trades pre-entry (n=' + rsiGateConsidered + '). Adding this as a hard filter (no signal if hourly RSI &gt; 80 on bull setups or &lt; 20 on bear) trades off some winners for fewer losses — measure on the next 20+ live signals before adopting.');
    }
    // 15m MACD cross gate: experimental. Surface when block rate is
    // meaningful AND we have a credible sample. NOTE this is a "would
    // have blocked among non-wins" rate — it doesn't tell us how many
    // winners the gate would also have killed. Read together with the
    // per-pair WR table to gauge whether the gate is selective for
    // losers or just generally suppresses signal volume.
    if(macdGateBlockRate !== null && macdGateBlockRate >= 15 && macdGateConsidered >= 10){
      recs.push('<strong>15m MACD cross gate</strong> would have blocked ' + Math.round(macdGateBlockRate) + '% of non-win trades (n=' + macdGateConsidered + '). Requires a same-direction MACD/Signal cross within 3 m15 bars before the trigger. Promising if winners survive the cut — verify by re-running the backtest with the gate active before promoting.');
    }

    // Diagnostic-driven hints (separate from variant rescues). Surface the
    // dominant failure mode so the user can reason about it even when no
    // variant cleanly rescues. These are HYPOTHESES — flagged as such.
    var hints = [];
    var totalAll = t.all || 0;
    var pct = function(n){ return totalAll > 0 ? Math.round(n / totalAll * 100) : 0; };
    var neverPct = pct(c['never-retested'] || 0);
    var invalPct = pct(c['invalidated-before-retest'] || 0);
    var cleanPct = pct(c['stop-clean-broke'] || 0);
    var progressPct = pct(c['stopped-after-progress'] || 0);
    if(neverPct >= 35){
      hints.push('<strong>' + neverPct + '% never retested</strong> — price walked away from the setup extreme without coming back. Worth backtesting: a wider entry zone, midpoint entry as a half-size fallback, or extending expiry.');
    }
    if(invalPct >= 25){
      hints.push('<strong>' + invalPct + '% invalidated pre-trigger</strong> — 4/4 alignment broke before retest. The 4/4 gate is doing its job protecting you here; widening the entry won\'t help. Consider tightening the alignment freshness window (only trade if 4/4 has held ≥ N bars).');
    }
    if(cleanPct + progressPct >= 30){
      hints.push('<strong>' + (cleanPct + progressPct) + '% stop-driven losses</strong> — the 1:1 stop is being hit too easily. Try a 1.5R / 2R target with the same stop, or place the stop one ATR beyond the swept BoS instead of right at it.');
    }
    var dominant = classEntries.find(function(e){return e.label !== 'win';});
    if(hints.length === 0 && dominant){
      hints.push('Dominant failure mode is <strong>' + classLabel(dominant.label) + '</strong> (' + dominant.count + ' trades) but no single mode is >30%. Look at per-pair breakdown — pair-specific filtering may help more than a global rule change.');
    }

    if(recs.length === 0 && hints.length === 0){
      recommendation = '<span style="color:var(--inkd);">No variant tested produced meaningfully better outcomes. Current rule is reasonable for the data; accept the win rate as inherent.</span>';
    } else {
      var parts = [];
      if(recs.length){
        parts.push('<span style="color:var(--bull);">▸ Variants worth testing:</span> ' + recs.join('; ') + '.');
      }
      if(hints.length){
        parts.push('<span style="color:var(--gold);">▸ Where the trades are failing:</span><br>&nbsp;&nbsp;• ' + hints.join('<br>&nbsp;&nbsp;• '));
      }
      parts.push('<span style="color:var(--inkd);font-style:italic;">(Hypotheses only — validate any rule change with ≥20 LIVE trades before adopting.)</span>');
      recommendation = parts.join('<br>');
    }
  }

  // Aggregate header — show what's being filtered + data source
  var pc = report.pairCounts || {totalPairs:0, alignedPairs:0, alignedWithSignals:0};
  var srcTag = report.dataSource === 'deep'
    ? '<span style="font-size:7.5px;padding:1px 5px;border-radius:2px;background:rgba(107,63,160,0.12);color:var(--purple);letter-spacing:0.5px;font-weight:700;">365d BACKTEST</span>'
    : '<span style="font-size:7.5px;padding:1px 5px;border-radius:2px;background:rgba(200,134,10,0.12);color:var(--gold);letter-spacing:0.5px;font-weight:700;">15m SIM</span>';
  // Rules-version chip — hover shows the changelog. Tells the user which
  // ruleset produced the displayed stats, so they don't wonder whether a
  // recent rule change has propagated. The fingerprint check inside
  // _loadRecentBtCache guarantees the cache is recomputed under the
  // current rules; this chip just surfaces which version that is.
  var verTag = '<span title="Recent rule changes:\n' + RULES_VERSION_NOTES.join('\n').replace(/"/g,'&quot;')
             + '" style="font-size:7.5px;padding:1px 5px;border-radius:2px;background:rgba(77,162,255,0.10);color:#4da2ff;letter-spacing:0.5px;font-weight:700;cursor:help;">RULES ' + RULES_VERSION + '</span>';
  var headerLabel = report.dataSource === 'deep' ? 'Failure mode analysis' : 'Failure mode analysis (simulated)';
  var tradesLabel = report.dataSource === 'deep' ? 'backtested trades' : 'simulated trades';
  var filterTag = '<span style="font-size:7.5px;color:var(--gold);font-style:italic;">'
                + 'filtered to ' + pc.alignedPairs + ' currently 4/4-aligned pair' + (pc.alignedPairs===1?'':'s')
                + ' (of ' + pc.totalPairs + ')</span>';

  // ── COLLAPSED VIEW ──
  // Compact one-line summary so the panel doesn't dominate the
  // backtest tab until the user wants to inspect failure modes.
  // Shows the dominant failure mode at a glance + an EXPAND chip;
  // the variant rescue grid and per-pair breakdown only render when
  // opened. State sticks to window._failureModeExpanded for the
  // session and defaults to collapsed on every full reload.
  if(!window._failureModeExpanded){
    // Pick a representative failure to surface in the compact view
    var topClass = null, topCount = 0;
    Object.keys(c).forEach(function(cls){
      if(cls === 'win') return;
      if(c[cls] > topCount){ topCount = c[cls]; topClass = cls; }
    });
    var topPct = t.all > 0 ? Math.round(topCount / t.all * 100) : 0;
    var compactClassLabel = {
      'never-retested': 'Never retested',
      'stop-clean-broke': 'Stop hit cleanly',
      'stopped-after-progress': 'Stopped after partial progress',
      'invalidated-before-retest': 'Invalidated pre-trigger'
    }[topClass] || topClass || '—';
    host.innerHTML =
      '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;">'
      + '  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">'
      + '    <div style="font-family:Orbitron,monospace;font-size:11px;font-weight:900;letter-spacing:1.2px;text-transform:uppercase;color:var(--bear);">&#9656; ' + headerLabel + '</div>'
      +      srcTag + verTag
      + '  </div>'
      + '  <button onclick="window._failureModeExpanded=true;renderFailureModeAnalysis();" style="font-family:Orbitron,monospace;font-size:8px;padding:4px 10px;border:1px solid var(--bear);background:transparent;color:var(--bear);border-radius:3px;cursor:pointer;letter-spacing:0.7px;font-weight:700;">EXPAND &#9662;</button>'
      + '</div>'
      + '<div style="margin-top:5px;font-size:8.5px;color:var(--inkd);line-height:1.6;">'
      +   t.all + ' ' + tradesLabel + ' &middot; '
      +   '<span style="color:var(--ink);font-weight:700;">' + t.wins + 'W/' + t.losses + 'L</span> resolved &middot; '
      +   '<span style="color:' + (stdRate >= 70 ? 'var(--bull)' : stdRate >= 50 ? 'var(--gold)' : 'var(--bear)') + ';font-weight:700;">'
      +     Math.round(stdRate) + '% win rate</span>'
      +   (topClass ? ' &middot; dominant failure: <strong style="color:var(--bear);">' + compactClassLabel + '</strong> (' + topPct + '%)' : '')
      + '</div>'
      + '<div style="margin-top:3px;">' + filterTag + '</div>';
    host.style.display = 'block';
    return;
  }
  var collapseBtn = '<button onclick="window._failureModeExpanded=false;renderFailureModeAnalysis();" title="Hide details" style="font-family:Orbitron,monospace;font-size:8px;padding:4px 10px;border:1px solid var(--inkd);background:transparent;color:var(--inkd);border-radius:3px;cursor:pointer;letter-spacing:0.7px;font-weight:700;margin-left:6px;">COLLAPSE &#9652;</button>';
  var aggHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;flex-wrap:wrap;gap:6px;">'
    + '<div style="display:flex;align-items:center;gap:8px;"><div style="font-family:Orbitron,monospace;font-size:11px;font-weight:900;letter-spacing:1.2px;color:var(--bear);text-transform:uppercase;">▸ ' + headerLabel + '</div>' + srcTag + verTag + collapseBtn + '</div>'
    + '<div style="font-size:8.5px;color:var(--inkd);">'+t.all+' ' + tradesLabel + ' &middot; '
    +   '<span title="Win rate of trades that resolved as W/L (excludes the ' + (t.expired||0) + ' that expired before retest and the ' + (t.invalidated||0) + ' invalidated pre-trigger). Of all ' + t.all + ' simulated trades, ' + t.wins + ' were wins (' + (t.all > 0 ? Math.round(t.wins/t.all*100) : 0) + '% raw).">'
    +     t.wins + 'W/' + t.losses + 'L resolved · ' + Math.round(stdRate) + '% win rate'
    +   '</span> · current rule</div>'
    + '</div>'
    + '<div style="margin-bottom:8px;">' + filterTag + '</div>';

  // Classification breakdown bar chart (text-based for compactness)
  var classBars = classEntries.map(function(e){
    var pct = Math.round(e.count / t.all * 100);
    var color = classColor(e.label);
    return '<div style="display:flex;align-items:center;gap:8px;font-size:8.5px;line-height:1.4;">'
      + '<div style="width:170px;color:' + color + ';">' + classLabel(e.label) + '</div>'
      + '<div style="flex:1;background:rgba(0,0,0,0.05);height:8px;border-radius:1px;overflow:hidden;">'
      +   '<div style="width:'+pct+'%;height:100%;background:'+color+';"></div>'
      + '</div>'
      + '<div style="width:60px;text-align:right;color:var(--inkm);font-weight:700;">' + e.count + ' (' + pct + '%)</div>'
      + '</div>';
  }).join('');

  // ── Per-asset-class WR breakdown (RULES_VERSION 2026-06-10b) ────────
  // Surfaces the per-class win rate so the user can verify forward-going
  // whether the deployed A1 per-class RSI thresholds are producing the
  // pattern the backtest sweep predicted. Hidden when sample size is
  // too small (<10 decided per class) to avoid noise; pairs/classes with
  // zero trades silently drop out.
  var perClass = report.perClass || {};
  var classOrder = ['major','minor','comm','index','crypto'];
  var classDisplayName = { major:'FX majors', minor:'FX minors', comm:'Commodities', index:'Indices', crypto:'Crypto' };
  var classGateLabel = { major:'90/10', minor:'70/30', comm:'70/30', index:'70/30', crypto:'75/25' };
  var perClassCells = classOrder.map(function(cls){
    var p = perClass[cls];
    if(!p || p.all === 0) return '';
    var decided = p.wins + p.losses;
    var wr = decided > 0 ? (p.wins / decided * 100) : null;
    var wrColor = wr === null ? 'var(--inkd)' : (wr >= 65 ? 'var(--bull)' : wr >= 50 ? 'var(--gold)' : 'var(--bear)');
    var wrText = wr === null ? '—' : Math.round(wr) + '%';
    var gateLabel = classGateLabel[cls] || '80/20';
    return '<div style="background:rgba(0,0,0,0.02);padding:6px 8px;border-radius:2px;" title="' + classDisplayName[cls] + ' — ' + p.all + ' simulated trades. A1 gate: 1H RSI ' + gateLabel + '. ' + p.wins + 'W / ' + p.losses + 'L of ' + decided + ' decided · ' + (p.expired||0) + ' expired · ' + (p.invalidated||0) + ' invalidated.">'
      + '<div style="font-size:8px;color:var(--inkm);font-weight:700;letter-spacing:0.4px;text-transform:uppercase;">' + classDisplayName[cls] + '</div>'
      + '<div style="font-size:11px;color:' + wrColor + ';font-weight:700;">' + wrText + '</div>'
      + '<div style="font-size:7.5px;color:var(--inkd);">' + p.wins + 'W/' + p.losses + 'L · gate ' + gateLabel + '</div>'
      + '</div>';
  }).filter(function(s){ return s; }).join('');
  var perClassHTML = perClassCells
    ? '<div style="margin-top:10px;padding-top:8px;border-top:1px dashed rgba(0,0,0,0.08);">'
        + '<div style="font-size:8.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:5px;" title="Per-asset-class WR — verifies whether the deployed A1 per-class RSI thresholds (RULES_VERSION 2026-06-10a) are producing the pattern the sweep predicted: minors/commodities/indices at 70/30, majors at 90/10, crypto at 75/25.">WR by asset class <span style="color:var(--inkd);font-weight:400;font-style:italic;">· post-A1 per-class gate</span></div>'
        + '<div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(110px, 1fr));gap:8px;">'
        + perClassCells
        + '</div>'
      + '</div>'
    : '';

  // Per-SR-tier WR row — DE40 + DJ30 forward-tracked tier samples.
  // Surfaces progress toward n=30 (the locked-in hard-gate deploy
  // threshold) so we can see at a glance when each tier becomes
  // ready to promote from informational badge to engine gate.
  var perSR = report.perSRtier || {};
  var srPairLabel = { de40: 'DAX 30', dj30: 'DJ 30' };
  var srTierLabels = [ ['5/5', '⭐ 5/5'], ['4/5', '🟡 4/5'], ['3/5', '🟠 3/5'] ];
  var srCells = [];
  Object.keys(srPairLabel).forEach(function(pairKey){
    var bucket = perSR[pairKey];
    if(!bucket) return;
    srTierLabels.forEach(function(tl){
      var tier = tl[0]; var label = tl[1];
      var b = bucket[tier];
      if(!b) return;
      // Hide empty tiers to keep the row sparse — early on most cells
      // will be n=0 and that's just noise. Show once at least one
      // resolved trade lands per tier per pair.
      if(b.n === 0) return;
      var wr = b.wr;
      var wrColor = wr === null ? 'var(--inkd)' : (wr >= 65 ? 'var(--bull)' : wr >= 50 ? 'var(--gold)' : 'var(--bear)');
      var wrText = wr === null ? '—' : Math.round(wr) + '%';
      var promoted = b.n >= 30 ? ' · ★ DEPLOY-READY' : (' · ' + b.n + '/30');
      var tip = srPairLabel[pairKey] + ' · School Run ' + tier + ' tier · '
              + b.w + 'W / ' + b.l + 'L of ' + b.n + ' decided trades. '
              + (b.n >= 30
                  ? 'Crossed the n=30 deploy threshold — eligible for promotion to engine gate per the locked decision rule.'
                  : 'Need ' + (30 - b.n) + ' more decided trades before this tier is eligible for hard-gate deployment.');
      srCells.push(
        '<div style="background:rgba(0,0,0,0.02);padding:6px 8px;border-radius:2px;" title="' + tip + '">'
        + '<div style="font-size:8px;color:var(--inkm);font-weight:700;letter-spacing:0.4px;text-transform:uppercase;">'
        +   srPairLabel[pairKey] + ' · ' + label
        + '</div>'
        + '<div style="font-size:11px;color:' + wrColor + ';font-weight:700;">' + wrText + '</div>'
        + '<div style="font-size:7.5px;color:var(--inkd);">' + b.w + 'W/' + b.l + 'L' + promoted + '</div>'
        + '</div>'
      );
    });
  });
  var perSRHTML = srCells.length
    ? '<div style="margin-top:10px;padding-top:8px;border-top:1px dashed rgba(0,0,0,0.08);">'
        + '<div style="font-size:8.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:5px;" title="Forward-tracked School Run tier WR for DE40 and DJ30. Sample accumulates as live signals fire and resolve; the badge in each pill on the relevant card lights up DEPLOY-READY once the locked threshold (n=30 decided per tier) is crossed.">School Run tier WR <span style="color:var(--inkd);font-weight:400;font-style:italic;">· DE40 / DJ30 forward-tracked · target n=30 per tier</span></div>'
        + '<div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(130px, 1fr));gap:8px;">'
        + srCells.join('')
        + '</div>'
      + '</div>'
    : '';

  // Variants section
  var variantHTML = '<div style="margin-top:10px;padding-top:8px;border-top:1px dashed rgba(0,0,0,0.08);">'
    + '<div style="font-size:8.5px;color:var(--inkd);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:5px;">Rule variant tests (only on non-wins)</div>'
    + '<div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(150px, 1fr));gap:8px;">'
    + '  <div style="background:rgba(0,0,0,0.02);padding:6px 8px;border-radius:2px;">'
    + '    <div style="font-size:8px;color:var(--inkm);font-weight:700;">+25% wider stop</div>'
    + '    <div style="font-size:9px;color:'+(stopPlus25Success > 50 ? 'var(--bull)' : 'var(--inkd)')+';font-weight:700;">'
    +       (stopPlus25Success === null ? 'no data' : Math.round(stopPlus25Success) + '% rescue rate')
    + '    </div>'
    + '    <div style="font-size:7.5px;color:var(--inkd);">'+v.stopPlus25.wins+'W / '+v.stopPlus25.losses+'L</div>'
    + '  </div>'
    + '  <div style="background:rgba(0,0,0,0.02);padding:6px 8px;border-radius:2px;" title="Wick-primary pairs only (FX + crypto + AUD/USD). Commodities &amp; indices are excluded — they already use Fib half-size as production methodology, so counting them here would double-claim the rescue.">'
    + '    <div style="font-size:8px;color:var(--inkm);font-weight:700;">Fib 38% entry (half size) <span style="color:var(--inkd);font-weight:400;font-style:italic;">· wick-primary only</span></div>'
    + '    <div style="font-size:9px;color:'+(midpointSuccess > 50 ? 'var(--bull)' : 'var(--inkd)')+';font-weight:700;">'
    +       (midpointSuccess === null ? 'no data' : Math.round(midpointSuccess) + '% rescue rate')
    + '    </div>'
    + '    <div style="font-size:7.5px;color:var(--inkd);">'+v.midpointEntry.wins+'W / '+v.midpointEntry.losses+'L</div>'
    + '  </div>'
    + '  <div style="background:rgba(0,0,0,0.02);padding:6px 8px;border-radius:2px;" title="Different metric — this is a pre-entry FILTER, not a post-loss rescue. Counts what fraction of non-win trades would have been blocked entirely by an hourly RSI > 80 (bull) / < 20 (bear) gate. Higher % = more losses avoided.">'
    + '    <div style="font-size:8px;color:var(--inkm);font-weight:700;">1H RSI 80/20 gate <span style="color:var(--inkd);font-weight:400;font-style:italic;">· pre-entry filter</span></div>'
    + '    <div style="font-size:9px;color:'+(rsiGateBlockRate !== null && rsiGateBlockRate >= 10 ? 'var(--bull)' : 'var(--inkd)')+';font-weight:700;">'
    +       (rsiGateBlockRate === null ? 'no data' : Math.round(rsiGateBlockRate) + '% would be blocked')
    + '    </div>'
    + '    <div style="font-size:7.5px;color:var(--inkd);">'
    +       rsiGateStats.filtered + ' blocked / ' + rsiGateStats.wouldStillFire + ' still fire'
    +       (rsiGateStats.unresolved > 0 ? ' &middot; ' + rsiGateStats.unresolved + ' no-data' : '')
    + '    </div>'
    + '  </div>'
    + '  <div style="background:rgba(0,0,0,0.02);padding:6px 8px;border-radius:2px;" title="Pre-entry filter — requires a same-direction MACD/Signal cross on the 15m chart within 3 bars (45 min) before the trigger. Bull setups need MACD crossing ABOVE Signal; bear setups need MACD crossing BELOW Signal. Higher % = more non-win trades the filter would have blocked. Higher is better only if winners survive the cut too; check the same metric on wins-only via the per-pair table below if results look promising.">'
    + '    <div style="font-size:8px;color:var(--inkm);font-weight:700;">15m MACD cross gate <span style="color:var(--inkd);font-weight:400;font-style:italic;">· pre-entry filter</span></div>'
    + '    <div style="font-size:9px;color:'+(macdGateBlockRate !== null && macdGateBlockRate >= 10 ? 'var(--bull)' : 'var(--inkd)')+';font-weight:700;">'
    +       (macdGateBlockRate === null ? 'no data' : Math.round(macdGateBlockRate) + '% would be blocked')
    + '    </div>'
    + '    <div style="font-size:7.5px;color:var(--inkd);">'
    +       macdGateStats.filtered + ' blocked / ' + macdGateStats.wouldStillFire + ' still fire'
    +       (macdGateStats.unresolved > 0 ? ' &middot; ' + macdGateStats.unresolved + ' no-data' : '')
    + '    </div>'
    + '  </div>'
    + '</div>'
    + '<div style="margin-top:8px;font-size:8.5px;line-height:1.5;">' + recommendation + '</div>'
    + '</div>';

  // Per-pair section (collapsed by default)
  var perPairKeys = Object.keys(report.perPair).sort();
  var perPairHTML = '';
  if(perPairKeys.length > 0){
    var rowsHTML = perPairKeys.map(function(k){
      var p = report.perPair[k];
      var sym = MKTS[k] ? MKTS[k].sym : k.toUpperCase();
      // Find the dominant non-win pattern
      var nonWins = Object.keys(p.classification).filter(function(c){return c !== 'win';})
        .map(function(c){return {label:c, count:p.classification[c]};})
        .sort(function(a,b){return b.count-a.count;});
      var dominant = nonWins.length > 0 ? nonWins[0] : null;
      var winCount = p.classification.win || 0;
      var totalCount = p.trades;
      var rate = totalCount > 0 ? Math.round(winCount / totalCount * 100) : 0;
      return '<tr style="border-top:1px dashed rgba(0,0,0,0.05);">'
        + '<td style="padding:4px 6px;font-weight:700;">'+sym+'</td>'
        + '<td style="padding:4px 6px;text-align:right;color:'+(rate>=60?'var(--bull)':rate>=40?'var(--gold)':'var(--bear)')+';font-weight:700;">'+rate+'%</td>'
        + '<td style="padding:4px 6px;text-align:right;color:var(--inkm);">'+winCount+'/'+totalCount+'</td>'
        + '<td style="padding:4px 6px;color:'+(dominant ? classColor(dominant.label) : 'var(--inkd)')+';">'+(dominant ? classLabel(dominant.label)+' ('+dominant.count+')' : '—')+'</td>'
        + '</tr>';
    }).join('');

    perPairHTML = '<details style="margin-top:10px;border-top:1px dashed rgba(0,0,0,0.08);padding-top:8px;">'
      + '<summary style="cursor:pointer;font-size:8.5px;color:var(--inkm);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;">▸ Per-pair breakdown</summary>'
      + '<table style="width:100%;margin-top:6px;border-collapse:collapse;font-size:8.5px;">'
      + '<thead><tr style="border-bottom:1px solid var(--rule);">'
      + '<th style="padding:4px 6px;text-align:left;color:var(--inkd);font-weight:700;letter-spacing:0.5px;">Pair</th>'
      + '<th style="padding:4px 6px;text-align:right;color:var(--inkd);font-weight:700;letter-spacing:0.5px;">Rate</th>'
      + '<th style="padding:4px 6px;text-align:right;color:var(--inkd);font-weight:700;letter-spacing:0.5px;">W/Total</th>'
      + '<th style="padding:4px 6px;text-align:left;color:var(--inkd);font-weight:700;letter-spacing:0.5px;">Dominant failure mode</th>'
      + '</tr></thead><tbody>'
      + rowsHTML
      + '</tbody></table>'
      + '</details>';
  }

  host.innerHTML = aggHTML + classBars + perClassHTML + perSRHTML + variantHTML + perPairHTML;
  host.style.display = 'block';
}


function renderAutoEWDiagnostic(){
  // Track A: render auto-EW comparison for any pair (selectable).
  // Default to EUR/USD on first render. The user can switch via dropdown.
  // Collapsed by default — the per-pair confidence/pattern is now surfaced
  // on the Macro card itself via the ⚡ auto-EW badge, so this panel is
  // demoted to a QA tool for spotting engine drift or stale Wickator
  // seeds across the fleet. Click the EXPAND chip to open it.
  var host = document.getElementById('autoEWDiag');
  if(!host) return;
  if(!window._autoEWPair) window._autoEWPair = 'eurusd';
  if(typeof window._autoEWExpanded === 'undefined') window._autoEWExpanded = false;
  var k = window._autoEWPair;
  if(!MKTS[k]) k = 'eurusd';

  // Build the pair selector + summary scan strip
  var pairOptions = Object.keys(MKTS).map(function(key){
    return '<option value="'+key+'"'+(key===k?' selected':'')+'>'+MKTS[key].sym+'</option>';
  }).join('');
  var selectorHTML =
    '<select id="autoEWPairSel" onchange="window._autoEWPair=this.value;renderAutoEWDiagnostic();" '
    + 'style="font-family:Orbitron,monospace;font-size:9px;padding:3px 6px;border:1px solid var(--rule);background:rgba(255,255,255,0.5);border-radius:3px;letter-spacing:0.4px;">'
    + pairOptions + '</select>';

  // Summary scan: agree / refine / disagree / no-seed / no-pattern across
  // the fleet. Split the previous catch-all "no-pattern" into two so the
  // user can tell whether the engine had nothing to compare against
  // (no_seed — most of the new minors / crypto / extra indices) vs the
  // engine genuinely failed to detect a pattern (no_valid_pattern,
  // insufficient_history). They have different implications: no-seed is
  // "we just haven't added a manual Wickator note yet", no-pattern is
  // "the engine looked and couldn't find a clean impulse/ABC/WXY".
  var scanCounts = { agree:0, refine:0, disagree:0, noPattern:0, noSeed:0, error:0 };
  var scanByPair = {};
  Object.keys(MKTS).forEach(function(key){
    try {
      var c = compareAutoVsSeed(key);
      if(!c.ok){
        if(c.reason === 'no_seed'){
          scanCounts.noSeed++;
          scanByPair[key] = { state: 'noSeed', reason: c.reason };
        } else {
          scanCounts.noPattern++;
          scanByPair[key] = { state: 'noPattern', reason: c.reason };
        }
      } else if(!c.agree){
        scanCounts.disagree++;
        scanByPair[key] = { state: 'disagree' };
      } else if(Math.abs(c.deltas.zoneMid) > 10){
        scanCounts.refine++;
        scanByPair[key] = { state: 'refine' };
      } else {
        scanCounts.agree++;
        scanByPair[key] = { state: 'agree' };
      }
    } catch(e){
      scanCounts.error++;
      scanByPair[key] = { state: 'error' };
    }
  });
  var scanHTML =
    '<div style="display:flex;gap:5px;flex-wrap:wrap;margin-top:6px;font-size:8px;letter-spacing:0.4px;">'
    + '<div style="padding:3px 6px;background:rgba(26,122,74,0.1);border:1px solid rgba(26,122,74,0.3);border-radius:2px;color:var(--bull);"><strong>'+scanCounts.agree+'</strong> AGREE</div>'
    + '<div style="padding:3px 6px;background:rgba(240,185,64,0.1);border:1px solid rgba(240,185,64,0.3);border-radius:2px;color:var(--gold);"><strong>'+scanCounts.refine+'</strong> REFINE</div>'
    + '<div style="padding:3px 6px;background:rgba(192,40,26,0.1);border:1px solid rgba(192,40,26,0.3);border-radius:2px;color:var(--bear);"><strong>'+scanCounts.disagree+'</strong> DISAGREE</div>'
    + '<div title="Engine has a pattern but no manual Wickator seed exists for these pairs — add a WICKATOR_EW entry to get them off this list." style="padding:3px 6px;background:rgba(77,162,255,0.08);border:1px solid rgba(77,162,255,0.3);border-radius:2px;color:#4da2ff;cursor:help;"><strong>'+scanCounts.noSeed+'</strong> no-seed</div>'
    + '<div title="Engine couldn\'t detect a clean impulse / ABC / WXY at any degree — usually insufficient daily history, sometimes a complex correction the algorithm doesn\'t recognise yet." style="padding:3px 6px;background:rgba(0,0,0,0.04);border:1px solid var(--rule);border-radius:2px;color:var(--inkd);cursor:help;"><strong>'+scanCounts.noPattern+'</strong> no-pattern</div>'
    + (scanCounts.error > 0 ? '<div style="padding:3px 6px;background:rgba(192,40,26,0.15);border:1px solid var(--bear);border-radius:2px;color:var(--bear);"><strong>'+scanCounts.error+'</strong> error</div>' : '')
    + '</div>';

  // Collapsed view — compact one-line summary with the fleet counts plus
  // an EXPAND chip. This is the default on every render so the panel
  // stays out of the way until the user wants to QA the engine.
  if(!window._autoEWExpanded){
    host.style.display = 'block';
    host.innerHTML =
      '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;">'
      + '  <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
      + '    <div style="font-size:11px;font-weight:900;letter-spacing:1.2px;text-transform:uppercase;color:#4da2ff;">&#9656; Auto-EW Diagnostic</div>'
      + '    <div style="font-size:8px;color:var(--inkd);font-style:italic;letter-spacing:0.3px;">engine health across all pairs &mdash; per-pair pattern now shown on each Macro card</div>'
      + '  </div>'
      + '  <button onclick="window._autoEWExpanded=true;renderAutoEWDiagnostic();" style="font-family:Orbitron,monospace;font-size:8px;padding:4px 10px;border:1px solid #4da2ff;background:transparent;color:#4da2ff;border-radius:3px;cursor:pointer;letter-spacing:0.7px;font-weight:700;">EXPAND &#9662;</button>'
      + '</div>'
      + scanHTML;
    return;
  }

  // Per-pair quick-pick chips: click to switch
  var chipColor = function(s){
    if(s === 'agree') return 'var(--bull)';
    if(s === 'refine') return 'var(--gold)';
    if(s === 'disagree') return 'var(--bear)';
    if(s === 'error') return 'var(--bear)';
    if(s === 'noSeed') return '#4da2ff';  // matches no-seed pill above
    return 'var(--inkd)';  // noPattern
  };
  var chipsHTML = '<div style="display:flex;gap:3px;flex-wrap:wrap;margin-top:5px;font-size:7.5px;letter-spacing:0.3px;">'
    + Object.keys(MKTS).map(function(key){
        var st = scanByPair[key] || {};
        var col = chipColor(st.state);
        var bg = (key === k) ? col : 'transparent';
        var fg = (key === k) ? '#fff' : col;
        return '<span onclick="window._autoEWPair=\''+key+'\';renderAutoEWDiagnostic();" '
          + 'style="cursor:pointer;padding:2px 5px;border:1px solid '+col+';background:'+bg+';color:'+fg+';border-radius:2px;font-family:Orbitron,monospace;font-weight:700;">'
          + MKTS[key].sym + '</span>';
      }).join('')
    + '</div>';

  var k_label = MKTS[k] ? MKTS[k].sym : k;
  var headerHTML =
    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;flex-wrap:wrap;gap:6px;">'
    + '  <div style="font-size:11px;font-weight:900;letter-spacing:1.2px;text-transform:uppercase;color:#4da2ff;">▸ Auto-EW Diagnostic — '+k_label+' (Track A, read-only)</div>'
    + '  <div style="display:flex;gap:6px;align-items:center;">'
    +     selectorHTML
    + '    <button onclick="if(typeof fetchVikingHistory===\'function\'){fetchVikingHistory().then(function(){renderAutoEWDiagnostic();});}" style="font-family:Orbitron,monospace;font-size:8px;padding:4px 10px;border:1px solid #4da2ff;background:transparent;color:#4da2ff;border-radius:3px;cursor:pointer;letter-spacing:0.5px;">↻ REFRESH</button>'
    + '    <button onclick="window._autoEWExpanded=false;renderAutoEWDiagnostic();" title="Hide diagnostic" style="font-family:Orbitron,monospace;font-size:8px;padding:4px 10px;border:1px solid var(--inkd);background:transparent;color:var(--inkd);border-radius:3px;cursor:pointer;letter-spacing:0.7px;font-weight:700;">COLLAPSE &#9652;</button>'
    + '  </div>'
    + '</div>'
    + scanHTML
    + chipsHTML;

  var cmp;
  try { cmp = compareAutoVsSeed(k); }
  catch(e){
    host.style.display = 'block';
    host.innerHTML = headerHTML
      + '<div style="margin-top:8px;color:var(--bear);font-size:9px;line-height:1.4;">Auto-EW error: '+(e.message||e)+'</div>';
    return;
  }

  if(!cmp.ok){
    host.style.display = 'block';
    var have = (cmp.auto && cmp.auto.dailyBars) || 0;
    var fetchInflight = (typeof window !== 'undefined' && window._histCount === undefined);
    var reasonText;
    if(cmp.reason === 'insufficient_history'){
      if(have === 0){
        reasonText = fetchInflight
          ? 'Loading daily candle history from CDN — should populate within 5-30s. If this persists, tap Refresh.'
          : 'No daily candles loaded for ' + k_label + '. history.json may not include this pair, or fetch failed — tap Refresh to retry.';
      } else {
        reasonText = 'Need 30+ daily candles. Currently have ' + have + ' for ' + k_label + '.';
      }
    } else if(cmp.reason === 'no_valid_pattern'){
      reasonText = 'No clean 5-wave / ABC / WXY / in-progress pattern detected for ' + k_label + ' (' + have + ' bars). The pattern may be mid-formation or a complex correction the algorithm does not recognise yet. Manual seed remains in use.';
    } else if(cmp.reason === 'no_seed'){
      reasonText = k_label + ' has auto-detected pattern but no manual WICKATOR_EW seed entry to compare against. Auto output: ' + (cmp.auto && cmp.auto.dir ? cmp.auto.dir.toUpperCase() : '?');
    } else {
      reasonText = cmp.reason;
    }
    host.innerHTML = headerHTML
      + '<div style="margin-top:8px;font-size:9px;color:var(--inkd);line-height:1.4;">' + reasonText + '</div>';
    return;
  }

  var advCol = cmp.agree ? (Math.abs(cmp.deltas.zoneMid) > 10 ? 'var(--gold)' : 'var(--bull)') : 'var(--bear)';
  var advLabel = cmp.agree ? (Math.abs(cmp.deltas.zoneMid) > 10 ? 'REFINE' : 'AGREE') : 'DISAGREE';

  var seed = cmp.seed, auto = cmp.auto;
  var fmt = function(n){ return n == null ? '—' : (Math.abs(n) < 10 ? n.toFixed(4) : n.toFixed(2)); };
  var fmtZone = function(z){ return '['+fmt(z[0])+' – '+fmt(z[1])+']'; };

  host.style.display = 'block';
  host.innerHTML = headerHTML
    + '<div style="margin-top:8px;display:flex;justify-content:flex-end;margin-bottom:6px;">'
    + '  <div style="padding:3px 8px;border:1.5px solid '+advCol+';color:'+advCol+';font-weight:900;letter-spacing:0.7px;border-radius:3px;font-size:9px;">'+advLabel+'</div>'
    + '</div>'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;font-size:8.5px;line-height:1.5;">'
    + '  <div style="background:rgba(0,0,0,0.025);border:1px solid var(--rule);border-radius:3px;padding:8px;">'
    + '    <div style="font-weight:700;color:var(--inkd);text-transform:uppercase;letter-spacing:0.6px;margin-bottom:4px;">Seed (manual / Wickator)</div>'
    + '    <div>dir: <strong>'+seed.dir.toUpperCase()+'</strong></div>'
    + '    <div>anchor: '+fmt(seed.anchor)+'</div>'
    + '    <div>pivot: '+fmt(seed.pivot)+'</div>'
    + '    <div>w2Zone: '+fmtZone(seed.w2Zone)+'</div>'
    + '    <div>invalid: '+fmt(seed.invalid)+'</div>'
    + '    <div>source: '+(seed.source||'—')+'</div>'
    + '  </div>'
    + '  <div style="background:rgba(77,162,255,0.04);border:1px solid rgba(77,162,255,0.25);border-radius:3px;padding:8px;">'
    + '    <div style="font-weight:700;color:#4da2ff;text-transform:uppercase;letter-spacing:0.6px;margin-bottom:4px;">Auto (Trendoscope+LuxAlgo port)</div>'
    + '    <div>dir: <strong>'+auto.dir.toUpperCase()+'</strong></div>'
    + '    <div>anchor: '+fmt(auto.anchor)+'</div>'
    + '    <div>pivot: '+fmt(auto.pivot)+'</div>'
    + '    <div>w2Zone: '+fmtZone(auto.w2Zone)+'</div>'
    + '    <div>invalid: '+fmt(auto.invalid)+'</div>'
    + '    <div>conf '+(auto.confidence*100).toFixed(0)+'% · pivot threshold '+(auto.thresholdPct||'?')+'%</div>'
    + '    <div style="color:#4da2ff;font-weight:700;margin-top:2px;">wave pos: '+(auto.wavePos||'—')+' · degree '+(auto.degree!=null?auto.degree:'?')+'</div>'
    + '  </div>'
    + '</div>'
    + (cmp.sub
        ? ('<div style="margin-top:6px;padding:6px 8px;background:rgba(77,162,255,0.03);border:1px dashed rgba(77,162,255,0.3);border-radius:3px;font-size:8.5px;line-height:1.5;">'
            + '<span style="color:var(--inkd);text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Sub-degree:</span> '
            + '<strong style="color:'+(cmp.sub.dir===auto.dir?'var(--bull)':'var(--bear)')+';">'+cmp.sub.dir.toUpperCase()+'</strong> · '
            + (cmp.sub.wavePos || cmp.sub.pattern) + ' · degree ' + cmp.sub.degree + ' (@'+cmp.sub.thresholdPct+'%) · '
            + 'anchor '+fmt(cmp.sub.anchor)+' pivot '+fmt(cmp.sub.pivot)
            + (cmp.sub.dir === auto.dir
                ? ' <span style="color:var(--bull);">— confirms macro</span>'
                : ' <span style="color:var(--gold);">— sub-fractal in opposite direction (normal during retracements)</span>')
            + '</div>')
        : '')
    + (cmp.macro
        ? ('<div style="margin-top:6px;padding:6px 8px;background:rgba(0,0,0,0.025);border:1px dashed rgba(0,0,0,0.15);border-radius:3px;font-size:8.5px;line-height:1.5;">'
            + '<span style="color:var(--inkd);text-transform:uppercase;letter-spacing:0.5px;font-weight:700;">Macro context:</span> '
            + '<strong style="color:'+(cmp.macro.dir==='bull'?'var(--bull)':'var(--bear)')+';">'+cmp.macro.dir.toUpperCase()+'</strong> at higher degree '+cmp.macro.degree+' (@'+cmp.macro.thresholdPct+'%) · '
            + 'anchor '+fmt(cmp.macro.anchor)+' pivot '+fmt(cmp.macro.pivot)
            + ' <span style="color:var(--inkd);">— larger-fractal view, FYI only</span>'
            + '</div>')
        : '')
    + '<div style="margin-top:6px;padding:6px 8px;background:rgba(0,0,0,0.025);border-left:3px solid '+advCol+';font-size:8.5px;color:var(--ink);font-style:italic;">'
    +   cmp.advisory
    +   ' &middot; <span style="color:var(--inkd);">deltas: anchor '+(cmp.deltas.anchor==null?'—':cmp.deltas.anchor.toFixed(2)+'%')+', pivot '+(cmp.deltas.pivot==null?'—':cmp.deltas.pivot.toFixed(2)+'%')+', zone '+cmp.deltas.zoneMid.toFixed(2)+'% &middot; '+(cmp.levelsWithPattern||0)+'/'+(cmp.levelsExplored||0)+' levels with pattern</span>'
    + '</div>';
}


function classifySimulatedTrades(k){
  // Filter: only run analysis on pairs currently 4/4 aligned (EW + TL +
  // NW + 4H EMA cloud). Matches the live signal logic so the failure-
  // mode counts reflect trades that would actually have fired.
  if(!STATE[k]) return [];
  var ewDir = STATE[k].ew, tlDir = STATE[k].tl, nwDir = STATE[k].nw, clDir = STATE[k].cl;
  if(ewDir !== tlDir || tlDir !== nwDir || nwDir !== clDir) return [];
  if(ewDir !== 'bull' && ewDir !== 'bear') return [];
  var alignedDir = ewDir;  // the direction the live signal would test

  // Seed/engine conflict guard removed 2026-05-30 — engine is
  // authoritative; the classifier now follows whatever the live engine
  // reads regardless of the legacy Wickator seed direction.

  var ticks = null;
  if(typeof INTRADAY !== 'undefined' && INTRADAY[k] && INTRADAY[k].length >= 20){
    ticks = INTRADAY[k];
  } else if(HISTORY[k] && HISTORY[k].m15 && HISTORY[k].m15.length >= 20){
    ticks = HISTORY[k].m15;
  }
  if(!ticks) return [];
  var n = ticks.length;
  var hasNative = ticks[n-1].h !== undefined && ticks[n-1].l !== undefined &&
                  ticks[n-1].o !== undefined && ticks[n-1].c !== undefined;
  var bars = [];
  for(var i = 0; i < n; i++){
    var t = ticks[i], o, c, h, l;
    if(hasNative && t.o !== undefined){
      o = t.o; c = t.c; h = t.h; l = t.l;
    } else {
      o = i > 0 ? (ticks[i-1].c != null ? ticks[i-1].c : ticks[i-1].p) : t.p;
      c = t.c != null ? t.c : t.p;
      h = Math.max(o, c); l = Math.min(o, c);
    }
    bars.push({idx:i, o:o, c:c, h:h, l:l});
  }

  // Prominence-aware BoS detection — must match detectIntradaySignal
  // and simulateIntradayWinRate so all three sources agree on stops.
  var clsPxAbs = ticks[n-1].c != null ? Math.abs(ticks[n-1].c) : Math.abs(ticks[n-1].p);
  var clsMinProminence;
  if(clsPxAbs > 1000)      clsMinProminence = clsPxAbs * 0.001;
  else if(clsPxAbs > 50)   clsMinProminence = clsPxAbs * 0.0008;
  else if(clsPxAbs > 5)    clsMinProminence = clsPxAbs * 0.0008;
  else                     clsMinProminence = 0.0005;
  var CLS_BOS_LOOKBACK = 24;

  var findRecentHigh = function(slice){
    for(var j = slice.length - 2; j >= 2; j--){
      if(j < 1 || j >= slice.length - 1) continue;
      var thisH = slice[j].h;
      var leftH = Math.max(slice[j-1].h, slice[j-2] ? slice[j-2].h : slice[j-1].h);
      var rightH = slice[j+1].h;
      if(thisH > leftH && thisH > rightH && (thisH - Math.max(leftH, rightH)) >= clsMinProminence){
        return thisH;
      }
    }
    return Math.max.apply(null, slice.map(function(b){return b.h;}));
  };
  var findRecentLow = function(slice){
    for(var j = slice.length - 2; j >= 2; j--){
      if(j < 1 || j >= slice.length - 1) continue;
      var thisL = slice[j].l;
      var leftL = Math.min(slice[j-1].l, slice[j-2] ? slice[j-2].l : slice[j-1].l);
      var rightL = slice[j+1].l;
      if(thisL < leftL && thisL < rightL && (Math.min(leftL, rightL) - thisL) >= clsMinProminence){
        return thisL;
      }
    }
    return Math.min.apply(null, slice.map(function(b){return b.l;}));
  };

  // Helper: simulate a setup forward through bars[i+1..]. Returns:
  //   {outcome: 'win'|'loss'|'expired'|'invalidated'|'unresolved',
  //    triggeredAt, resolvedAt, exitReason, reachToTarget}
  // reachToTarget: how far toward target price went after trigger before reversing (0-1+)
  var runForward = function(setup, fromIdx, lookaheadCap){
    var triggeredAt = -1, resolved = null, resolvedAt = -1;
    var reachToTarget = 0;
    var endIdx = Math.min(n, fromIdx + 1 + lookaheadCap);
    for(var j = fromIdx + 1; j < endIdx; j++){
      var b = bars[j];
      if(triggeredAt === -1){
        if(setup.dir === 'bear' && b.c > setup.stop){ resolved='invalidated'; resolvedAt=j; break; }
        if(setup.dir === 'bull' && b.c < setup.stop){ resolved='invalidated'; resolvedAt=j; break; }
        if(j - fromIdx > 8){ resolved='expired'; resolvedAt=j; break; }
        // Fast momentum-reversal pre-trigger (parity with live detector)
        if(typeof detectConsecutiveCounterBars === 'function'){
          var ctrPreCls = detectConsecutiveCounterBars(bars, setup.creatorIdx, j, setup.dir, clsMinProminence);
          if(ctrPreCls >= 0){ resolved='invalidated'; resolvedAt=j; break; }
        }
        var reaches = (setup.dir === 'bear' && b.h >= setup.entry) ||
                      (setup.dir === 'bull' && b.l <= setup.entry);
        if(reaches){ triggeredAt = j; }
      } else {
        // Post-trigger: track furthest reach toward target
        var movedTowardTarget;
        if(setup.dir === 'bear'){
          movedTowardTarget = setup.entry - b.l;
          var pctToTarget = movedTowardTarget / setup.R;
          if(pctToTarget > reachToTarget) reachToTarget = pctToTarget;
          if(b.l <= setup.target){ resolved='win'; resolvedAt=j; break; }
          if(b.h >= setup.stop){ resolved='loss'; resolvedAt=j; break; }
        } else {
          movedTowardTarget = b.h - setup.entry;
          var pctToTarget = movedTowardTarget / setup.R;
          if(pctToTarget > reachToTarget) reachToTarget = pctToTarget;
          if(b.h >= setup.target){ resolved='win'; resolvedAt=j; break; }
          if(b.l <= setup.stop){ resolved='loss'; resolvedAt=j; break; }
        }
        // PR #3 parity (post-trigger opposing CHoCH). Match the live
        // detector by exiting if structure has flipped on m15 after
        // trigger. classifySimulatedTrades doesn't carry EW/TL per-bar
        // state so we only check m15 here.
        if(typeof detectOpposingCHoCH === 'function'){
          var clsOppPost = detectOpposingCHoCH(bars, setup.creatorIdx, j, setup.dir, clsMinProminence);
          if(clsOppPost > triggeredAt){
            resolved='invalidated'; resolvedAt=j; break;
          }
        }
        if(typeof detectConsecutiveCounterBars === 'function'){
          var clsCtrPost = detectConsecutiveCounterBars(bars, triggeredAt, j, setup.dir, clsMinProminence);
          if(clsCtrPost > triggeredAt){
            resolved='invalidated'; resolvedAt=j; break;
          }
        }
        // PR #4 parity. After TRIGGERED_MAX_BARS (16) without resolution
        // the live detector now exits with outcome='stale-expired'.
        if(triggeredAt >= 0 && (j - triggeredAt) > 16){
          resolved='expired'; resolvedAt=j; break;
        }
      }
    }
    return { outcome: resolved || 'unresolved', triggeredAt, resolvedAt, reachToTarget };
  };

  var trades = [];
  var i = 8;
  while(i < n - 1){
    var lookback = bars.slice(Math.max(0, i - 8), i);
    if(lookback.length < 5){ i++; continue; }
    var swingHi = Math.max.apply(null, lookback.map(function(b){return b.h;}));
    var swingLo = Math.min.apply(null, lookback.map(function(b){return b.l;}));
    var setup = null;
    // Use wider BoS slice for stop placement — same approach as live detector
    var bosSlice = bars.slice(Math.max(0, i - CLS_BOS_LOOKBACK), i);
    // Only test the direction matching current 4/4 alignment.
    if(alignedDir === 'bear' && bars[i].c < swingLo){
      var entry = bars[i].h, stop = findRecentHigh(bosSlice);
      if(stop > entry){
        setup = {dir:'bear', entry, stop, target: entry - (stop - entry), R: stop - entry, creatorIdx: i, creatorBar: bars[i]};
      }
    } else if(alignedDir === 'bull' && bars[i].c > swingHi){
      var entry = bars[i].l, stop = findRecentLow(bosSlice);
      if(stop < entry){
        setup = {dir:'bull', entry, stop, target: entry + (entry - stop), R: entry - stop, creatorIdx: i, creatorBar: bars[i]};
      }
    }
    if(!setup){ i++; continue; }

    // Run the standard rule
    var stdResult = runForward(setup, i, 8 + 32);

    // Classify the failure mode
    var classification;
    if(stdResult.outcome === 'win'){
      classification = 'win';
    } else if(stdResult.outcome === 'expired'){
      classification = 'never-retested';
    } else if(stdResult.outcome === 'invalidated'){
      classification = 'invalidated-before-retest';
    } else if(stdResult.outcome === 'loss'){
      // Distinguish: did price wick past stop and then turn? Or break cleanly?
      // We do this by simulating with stop +25% — if that variant would have won,
      // the original stop was tight (wick).
      classification = stdResult.reachToTarget >= 0.5
        ? 'stopped-after-progress'  // got partway then reversed
        : 'stop-clean-broke';        // never made meaningful progress
    } else {
      // unresolved (ran out of bars) — exclude from analysis
      i = i + 9; continue;
    }

    // Run variants (only meaningful for losses & never-retested)
    var variants = {};
    if(classification !== 'win'){
      // Variant A: stop +25% beyond original
      var stopPlus25;
      if(setup.dir === 'bear'){
        stopPlus25 = setup.entry + setup.R * 1.25;
      } else {
        stopPlus25 = setup.entry - setup.R * 1.25;
      }
      var setupV1 = {
        dir: setup.dir, entry: setup.entry, stop: stopPlus25,
        target: setup.dir === 'bear' ? setup.entry - setup.R*1.25 : setup.entry + setup.R*1.25,
        R: setup.R * 1.25
      };
      var v1Result = runForward(setupV1, i, 8 + 32);
      variants.stopPlus25 = v1Result.outcome;

      // Variant B: midpoint entry (body midpoint)
      var midpoint = (setup.creatorBar.o + setup.creatorBar.c) / 2;
      var setupV2 = {
        dir: setup.dir,
        entry: midpoint,
        stop: setup.stop,
        target: setup.dir === 'bear' ? midpoint - (setup.stop - midpoint) : midpoint + (midpoint - setup.stop),
        R: setup.dir === 'bear' ? (setup.stop - midpoint) : (midpoint - setup.stop)
      };
      if(setupV2.R > 0){
        var v2Result = runForward(setupV2, i, 8 + 32);
        variants.midpointEntry = v2Result.outcome;
      }
    }

    trades.push({
      dir: setup.dir,
      entry: setup.entry,
      stop: setup.stop,
      target: setup.target,
      R: setup.R,
      outcome: stdResult.outcome,
      classification: classification,
      reachToTarget: stdResult.reachToTarget,
      variants: variants,
      // Signal-bar timestamp so downstream analyses (e.g. the
      // hourly-RSI-gate hypothesis in analyseIntradayFailures) can
      // correlate each trade back to the contemporary 1H bar.
      signalTs: (ticks[i] && ticks[i].t) ? ticks[i].t : null
    });

    i = i + 9;
  }
  return trades;
}

// Aggregate failure-mode analysis across pairs.
// Returns:
//   { totals: {wins, losses, expired, invalidated, all},
//     classification: { 'never-retested': N, 'stop-clean-broke': N, ... },
//     variants: { stopPlus25: {wins, losses}, midpointEntry: {wins, losses} },
//     perPair: { eurusd: {classification:..., variants:..., trades:N}, ... } }


function analyseIntradayFailures(){
  var perPair = {};
  var aggClassification = {};
  var aggVariants = {
    stopPlus25:    {wins:0, losses:0, unresolved:0},
    midpointEntry: {wins:0, losses:0, unresolved:0},
    // RSI 80/20 gate hypothesis (2026-06-04). For each NON-WIN trade,
    // compute the contemporary 1H RSI(14). If it would have crossed
    // the extreme threshold at signal time (bull setup with RSI > 80
    // or bear setup with RSI < 20), the trade would have been blocked
    // by the gate and the loss avoided. `filtered` counts those;
    // `wouldStillFire` counts non-wins that the gate doesn't catch.
    // The metric to read is filtered / (filtered + wouldStillFire) —
    // the fraction of losing trades the gate WOULD have prevented.
    rsiGate80_20:  {filtered:0, wouldStillFire:0, unresolved:0},
    // m15 MACD(12,26,9) cross gate hypothesis (2026-06-16). Mirrors the
    // RSI gate above but with a different filter: a trade would have
    // been BLOCKED unless a same-direction MACD/Signal cross occurred
    // within MACD_CROSS_LB m15 bars before the signal (bull setup needs
    // MACD crossing above Signal; bear setup needs the inverse). The
    // hypothesis under test: requiring fresh 15-min momentum confirm
    // filters out triggers that fire against fading momentum, lifting
    // WR without changing the structural entry rules.
    macdCross:     {filtered:0, wouldStillFire:0, unresolved:0}
  };
  var aggTotals = {wins:0, losses:0, expired:0, invalidated:0, all:0};
  // Per-class win/loss tally — populated alongside aggTotals so the panel
  // can render a "WR by asset class" row. Driven by the user request
  // (2026-06-10) to verify forward-going whether the per-class A1 RSI
  // gate is producing the per-class WR pattern the sweep predicted.
  var perClassTotals = {};
  var _classOf = function(k){ return (MKTS[k] && MKTS[k].t) || 'unknown'; };
  var _bumpClass = function(cls, outcome){
    perClassTotals[cls] = perClassTotals[cls] || {wins:0, losses:0, expired:0, invalidated:0, all:0};
    perClassTotals[cls].all++;
    if(outcome === 'win') perClassTotals[cls].wins++;
    else if(outcome === 'loss') perClassTotals[cls].losses++;
    else if(outcome === 'expired') perClassTotals[cls].expired++;
    else if(outcome === 'invalidated') perClassTotals[cls].invalidated++;
  };
  var pairCounts = { totalPairs: 0, alignedPairs: 0, alignedWithSignals: 0 };
  var alignedPairKeys = [];
  var dataSource = 'sim';  // 'deep' (12-month) or 'sim' (buffered ~10-15 days)

  // Determine if deep backtest is available — if yes, pull failure data from
  // the per-trade outcomes recorded during the 12-month walk. This gives the
  // failure mode analysis 10-30x the sample size of the buffered simulation.
  var allDeep = null;
  try {
    if(typeof DEEP_HIST !== 'undefined' && Object.keys(DEEP_HIST).length > 0 &&
       typeof computeAllRecentBacktests === 'function'){
      allDeep = computeAllRecentBacktests(false);
      // Check if any pair has deep data
      var hasDeep = Object.keys(allDeep || {}).some(function(k){
        return allDeep[k] && allDeep[k].dataSource === 'deep' && allDeep[k].trades && allDeep[k].trades.length > 0;
      });
      if(hasDeep) dataSource = 'deep';
    }
  } catch(e){ /* fall through to sim path */ }

  // Map deep backtest failureMode codes onto the legacy classification
  // codes used by the UI (renderFailureModeAnalysis):
  //   never-retested           ← never-retested            (expired, no trigger)
  //   stop-clean-broke         ← stop-hit-cleanly          (loss with no progress)
  //   stopped-after-progress   ← stopped-after-partial-progress
  //   invalidated-before-retest ← stop-breached-pre-trigger, choch-pre-trigger
  //   (post-trigger CHoCH gets folded into stopped-after-progress since it's
  //    an exit after the trade had a chance to develop)
  var mapFailureMode = function(failureMode, outcome){
    if(outcome === 'win') return 'win';
    if(failureMode === 'never-retested') return 'never-retested';
    if(failureMode === 'stop-hit-cleanly') return 'stop-clean-broke';
    if(failureMode === 'stopped-after-partial-progress') return 'stopped-after-progress';
    if(failureMode === 'stop-breached-pre-trigger') return 'invalidated-before-retest';
    if(failureMode === 'choch-pre-trigger') return 'invalidated-before-retest';
    if(failureMode === 'counter-bars-pre-trigger') return 'invalidated-before-retest';
    if(failureMode === 'choch-post-trigger') return 'stopped-after-progress';
    if(failureMode === 'counter-bars-post-trigger') return 'stopped-after-progress';
    // Fall through for unknown / null
    if(outcome === 'expired') return 'never-retested';
    if(outcome === 'loss') return 'stop-clean-broke';
    if(outcome === 'invalidated') return 'invalidated-before-retest';
    return 'unknown';
  };

  Object.keys(MKTS).forEach(function(k){
    pairCounts.totalPairs++;
    // Check 4/4 alignment up-front (EW + TL + NW + 4H EMA cloud) so the
    // counts match the live gate. The buffered sim path applies the
    // same gate inside classifySimulatedTrades.
    if(STATE[k] && STATE[k].ew === STATE[k].tl && STATE[k].tl === STATE[k].nw &&
       STATE[k].nw === STATE[k].cl &&
       (STATE[k].ew === 'bull' || STATE[k].ew === 'bear')){
      pairCounts.alignedPairs++;
      alignedPairKeys.push(k);
    }

    // ── DEEP BACKTEST PATH ──
    // When deep history is loaded, use the per-trade outcomes recorded
    // during calcRecentBacktest's 12-month walk. Provides 10-30x larger
    // sample size than the buffered simulator path below.
    if(dataSource === 'deep' && allDeep && allDeep[k] && allDeep[k].trades && allDeep[k].trades.length > 0){
      var deepTrades = allDeep[k].trades;
      pairCounts.alignedWithSignals++;
      var pair = { trades: deepTrades.length, classification: {}, variants: {stopPlus25: {wins:0, losses:0}, midpointEntry: {wins:0, losses:0}} };
      var _kClass = _classOf(k);
      deepTrades.forEach(function(dt){
        var cls = mapFailureMode(dt.failureMode, dt.outcome);
        pair.classification[cls] = (pair.classification[cls]||0) + 1;
        aggClassification[cls] = (aggClassification[cls]||0) + 1;
        aggTotals.all++;
        if(dt.outcome === 'win') aggTotals.wins++;
        else if(dt.outcome === 'loss') aggTotals.losses++;
        else if(dt.outcome === 'expired') aggTotals.expired++;
        else if(dt.outcome === 'invalidated') aggTotals.invalidated++;
        _bumpClass(_kClass, dt.outcome);
        // Note: deep backtest doesn't currently track variant rescues; that
        // would require running each trade through variant rule too. The
        // variant column shows '0' in deep mode — could add later if useful.
      });
      perPair[k] = pair;
      return;
    }

    // ── BUFFERED SIM FALLBACK ──
    // When deep history isn't loaded, fall back to classifySimulatedTrades
    // walking the ~10-15 day buffered m15 data. Smaller sample, useful while
    // deep history is loading or unavailable.
    var trades;
    try { trades = classifySimulatedTrades(k); }
    catch(e){ return; }
    if(!trades || trades.length === 0) return;
    pairCounts.alignedWithSignals++;

    // ── 1H RSI series for the RSI 80/20 gate hypothesis ──
    // Pre-compute once per pair so each trade's signal-time RSI is an
    // O(log n) lookup rather than a fresh walk. Bars without numeric
    // closes are dropped; if the buffer is too short we just leave
    // rsiGate stats as "unresolved".
    var h1Closes = [], h1Times = [];
    if(HISTORY[k] && HISTORY[k].h1){
      HISTORY[k].h1.forEach(function(b){
        var c = b.c != null ? b.c : b.p;
        if(c != null && isFinite(c)){
          h1Closes.push(c);
          h1Times.push(b.t || null);
        }
      });
    }
    var h1Rsi = (h1Closes.length >= 16 && typeof _rsiSeries === 'function')
                  ? _rsiSeries(h1Closes, 14) : null;
    function _rsiAtSignal(signalTs){
      if(!h1Rsi || !signalTs) return null;
      var ts = new Date(signalTs).getTime();
      if(!isFinite(ts)) return null;
      // Find the latest h1 idx whose timestamp is at-or-before signal.
      // Linear scan is fine — the m15 buffer rarely exceeds ~960 bars
      // and the h1 buffer is bounded by the workflow's retention.
      var idx = -1;
      for(var i = 0; i < h1Times.length; i++){
        var hi = new Date(h1Times[i]).getTime();
        if(!isFinite(hi)) continue;
        if(hi <= ts) idx = i; else break;
      }
      if(idx < 0) return null;
      return h1Rsi[idx];
    }

    // ── m15 MACD(12,26,9) cross series for the macdCross gate ──
    // Lookback: a same-direction cross within MACD_CROSS_LB m15 bars
    // before the trigger keeps the trade. No cross → blocked. 3 bars
    // = 45-min window, tight enough that the cross is "fresh momentum"
    // not stale, loose enough that the cross has room to print on the
    // bar before the trigger.
    var MACD_CROSS_LB = 3;
    var m15Closes = [], m15Times = [];
    if(HISTORY[k] && HISTORY[k].m15){
      HISTORY[k].m15.forEach(function(b){
        var c = b.c != null ? b.c : b.p;
        if(c != null && isFinite(c)){
          m15Closes.push(c);
          m15Times.push(b.t || null);
        }
      });
    }
    var m15Macd = (m15Closes.length >= 35 && typeof _macdSeries === 'function')
                    ? _macdSeries(m15Closes, 12, 26, 9) : null;
    function _macdCrossAtSignal(signalTs, dir){
      if(!m15Macd || !signalTs || (dir !== 'bull' && dir !== 'bear')) return null;
      var ts = new Date(signalTs).getTime();
      if(!isFinite(ts)) return null;
      var idx = -1;
      for(var i = 0; i < m15Times.length; i++){
        var bt = new Date(m15Times[i]).getTime();
        if(!isFinite(bt)) continue;
        if(bt <= ts) idx = i; else break;
      }
      if(idx < 1) return null;
      var start = Math.max(1, idx - MACD_CROSS_LB + 1);
      for(var j = start; j <= idx; j++){
        var m0 = m15Macd.macd[j-1], m1 = m15Macd.macd[j];
        var s0 = m15Macd.signal[j-1], s1 = m15Macd.signal[j];
        if(m0 == null || m1 == null || s0 == null || s1 == null) return null;
        if(dir === 'bull' && m0 <= s0 && m1 > s1) return true;
        if(dir === 'bear' && m0 >= s0 && m1 < s1) return true;
      }
      return false;
    }

    var pair = { trades: trades.length, classification: {}, variants: {stopPlus25: {wins:0, losses:0}, midpointEntry: {wins:0, losses:0}, rsiGate80_20: {filtered:0, wouldStillFire:0}, macdCross: {filtered:0, wouldStillFire:0}} };
    var _kClassSim = _classOf(k);
    trades.forEach(function(t){
      pair.classification[t.classification] = (pair.classification[t.classification]||0) + 1;
      aggClassification[t.classification] = (aggClassification[t.classification]||0) + 1;
      aggTotals.all++;
      if(t.outcome === 'win') aggTotals.wins++;
      else if(t.outcome === 'loss') aggTotals.losses++;
      else if(t.outcome === 'expired') aggTotals.expired++;
      else if(t.outcome === 'invalidated') aggTotals.invalidated++;
      _bumpClass(_kClassSim, t.outcome);

      // Variants only meaningful for non-wins
      if(t.classification !== 'win'){
        if(t.variants.stopPlus25){
          if(t.variants.stopPlus25 === 'win'){ pair.variants.stopPlus25.wins++; aggVariants.stopPlus25.wins++; }
          else if(t.variants.stopPlus25 === 'loss'){ pair.variants.stopPlus25.losses++; aggVariants.stopPlus25.losses++; }
          else aggVariants.stopPlus25.unresolved++;
        }
        if(t.variants.midpointEntry){
          // The Fib 38% / midpoint entry variant is meaningless as a
          // "rescue" for pairs that ALREADY use Fib half-size as their
          // production methodology (commodities + indices via
          // _btMethodFor). For those pairs the variant outcome IS what
          // they're actually trading — counting it would double-claim
          // a rescue that's already the primary rule. Restrict the
          // aggregate (and the on-card pair count) to wick-primary
          // pairs only so the "rescue rate" answers the question
          // the user is actually asking: should the WICK-primary
          // pairs (FX majors / minors / crypto / AUD/USD) switch to
          // Fib half-size?
          var pairUsesFibPrimary = (typeof _btMethodFor === 'function')
            ? (_btMethodFor(k) === 'fib') : false;
          if(!pairUsesFibPrimary){
            if(t.variants.midpointEntry === 'win'){ pair.variants.midpointEntry.wins++; aggVariants.midpointEntry.wins++; }
            else if(t.variants.midpointEntry === 'loss'){ pair.variants.midpointEntry.losses++; aggVariants.midpointEntry.losses++; }
            else aggVariants.midpointEntry.unresolved++;
          }
        }
        // RSI 80/20 gate hypothesis. Apply to every non-win trade
        // regardless of pair class — the gate is a pre-entry filter,
        // not a rescue rule, so it's class-agnostic. A trade would
        // have been BLOCKED (loss avoided) if:
        //   - bull setup AND 1H RSI > 80
        //   - bear setup AND 1H RSI < 20
        var rsiAtSignal = _rsiAtSignal(t.signalTs);
        if(rsiAtSignal == null){
          aggVariants.rsiGate80_20.unresolved++;
        } else {
          var blocked = (t.dir === 'bull' && rsiAtSignal > 80) ||
                        (t.dir === 'bear' && rsiAtSignal < 20);
          if(blocked){
            pair.variants.rsiGate80_20.filtered++;
            aggVariants.rsiGate80_20.filtered++;
          } else {
            pair.variants.rsiGate80_20.wouldStillFire++;
            aggVariants.rsiGate80_20.wouldStillFire++;
          }
        }

        // m15 MACD/Signal cross gate hypothesis. Blocks trades whose
        // entry direction wasn't confirmed by a fresh (≤3 m15 bars)
        // same-direction MACD cross. Returns true (fires anyway),
        // false (gate would have blocked) or null (warm-up / no data).
        var macdFires = _macdCrossAtSignal(t.signalTs, t.dir);
        if(macdFires == null){
          aggVariants.macdCross.unresolved++;
        } else if(macdFires){
          pair.variants.macdCross.wouldStillFire++;
          aggVariants.macdCross.wouldStillFire++;
        } else {
          pair.variants.macdCross.filtered++;
          aggVariants.macdCross.filtered++;
        }
      }
    });
    perPair[k] = pair;
  });

  return {
    totals: aggTotals,
    classification: aggClassification,
    variants: aggVariants,
    perPair: perPair,
    perClass: perClassTotals,  // {major:{wins,losses,…}, minor:…, …}
    // Per-SR-tier WR — DE40 + DJ30 only. Aggregates the same live-trade
    // records the SR pill reads, surfaced into the failure-mode panel
    // so the forward-tracked progress toward n=30 (the hard-gate
    // promotion threshold) is visible in one place. Computed on
    // demand via liveSRTierWR per pair. Pairs without SR_REF_TIMES
    // entries are skipped silently.
    perSRtier: (function(){
      var out = {};
      if(typeof liveSRTierWR !== 'function') return out;
      Object.keys(SR_REF_TIMES || {}).forEach(function(k){
        try {
          var w = liveSRTierWR(k);
          if(w) out[k] = w;
        } catch(_){}
      });
      return out;
    })(),
    pairCounts: pairCounts,
    alignedPairKeys: alignedPairKeys,
    dataSource: dataSource  // 'deep' or 'sim' — UI shows this in the header
  };
}

// Public win-rate getter: returns the appropriate rate per the rules.
// {rate, total, source: 'live' | 'sim'} or null if no data.

