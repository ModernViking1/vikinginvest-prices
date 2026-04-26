// fetch-prices.js — Viking Invest price fetcher v4 (Free 15-min coverage)
//
// Architecture:
//   - TwelveData: 8 major pairs + XAG/USD = 9 pairs (15m live + daily history)
//   - OANDA v20 demo API: 9 FX crosses + USOIL + DE40 = 11 pairs (15m live + daily history)
//   - Coinbase public exchange: BTC/USD + SUI/USD = 2 pairs (15m live + daily history)
//   - Baked override: DXY only
//
// Total: 22 pairs, 21 with live 15-min data.
//
// API budgets (all comfortably under free tier limits):
//   TwelveData: 9 × 96 runs/day = 864/800 — wait that's over. We keep at 8 (768/800)
//               and route XAG through OANDA instead.
//   OANDA demo: ~10 × 96 = 960/day — well under their generous limit
//   Coinbase: 2 × 96 = 192/day — Coinbase public is rate-limited at 10/sec, fine
//
// Outputs:
//   prices.json   — current 15-min prices for all 21 live-sourced pairs
//   history.json  — 500 daily candles per pair (refreshed once per day at 00:05 UTC)

const fs = require('fs');
const https = require('https');

const TD_KEY = process.env.TD_API_KEY;
const OANDA_TOKEN = process.env.OANDA_TOKEN;

if(!TD_KEY){ console.error('Missing TD_API_KEY env var'); process.exit(1); }
if(!OANDA_TOKEN){
  console.warn('WARNING: Missing OANDA_TOKEN — OANDA pairs will be skipped.');
}

// ── TwelveData: 8 majors ─────────────────────────────────────
const TD_PAIRS = {
  eurusd: 'EUR/USD', gbpusd: 'GBP/USD', usdjpy: 'USD/JPY', usdcad: 'USD/CAD',
  usdchf: 'USD/CHF', xauusd: 'XAU/USD', euraud: 'EUR/AUD', audusd: 'AUD/USD'
};

// ── OANDA: FX crosses + commodities ──────────────────────────
// Format: PAIR_KEY → OANDA instrument symbol (underscore-delimited)
// Verified pairs in OANDA's 90+ instrument universe:
const OANDA_PAIRS = {
  nzdusd: 'NZD_USD',
  usdsgd: 'USD_SGD',
  cadjpy: 'CAD_JPY',
  eurnzd: 'EUR_NZD',
  gbpaud: 'GBP_AUD',
  audnzd: 'AUD_NZD',
  eurgbp: 'EUR_GBP',
  audchf: 'AUD_CHF',
  xagusd: 'XAG_USD',     // silver
  usoil:  'BCO_USD',     // Brent Crude Oil (matches TradingView XBRUSD reference)
  de40:   'DE30_EUR',    // DAX (OANDA still uses old DE30 ticker)
  // usdsek: not in dashboard MKTS list, but included in prices.json so the
  // dashboard can compute DXY synthetically using the official ICE formula.
  usdsek: 'USD_SEK'
};

// ── Coinbase: crypto pairs ───────────────────────────────────
const COINBASE_PAIRS = {
  btcusd: 'BTC-USD',
  suiusd: 'SUI-USD'
};

// ── HTTP helpers ─────────────────────────────────────────────
function getJSON(url, headers){
  return new Promise((resolve, reject) => {
    https.get(url, { headers: headers || { 'User-Agent': 'ViKingInvest/1.0' } }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if(res.statusCode >= 400){
          return reject(new Error('HTTP ' + res.statusCode + ': ' + data.slice(0,200)));
        }
        try { resolve(JSON.parse(data)); }
        catch(e){ reject(new Error('JSON parse failed: ' + e.message + ' | first 300: ' + data.slice(0,300))); }
      });
    }).on('error', reject);
  });
}

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

// ── TwelveData LIVE (8 majors via batch) ─────────────────────
async function fetchTDLivePrices(){
  console.log('\n=== TwelveData LIVE PRICES (15-min) ===');
  const symbols = Object.values(TD_PAIRS).join(',');
  const url = 'https://api.twelvedata.com/time_series?symbol=' + symbols + '&interval=15min&outputsize=2&apikey=' + TD_KEY;

  let resp;
  try { resp = await getJSON(url); }
  catch(e){ console.error('TD live fetch failed:', e.message); return {}; }

  if(resp.code && resp.message){
    console.error('TD API error:', resp.code, resp.message);
    return {};
  }

  const out = {};
  let ok = 0, fail = 0;
  Object.entries(TD_PAIRS).forEach(([key, sym]) => {
    const row = resp[sym] || (resp.meta && resp.meta.symbol === sym ? resp : null);
    if(!row || row.code || !row.values || !row.values.length){ fail++; return; }
    const latest = parseFloat(row.values[0].close);
    const prev = row.values[1] ? parseFloat(row.values[1].close) : latest;
    if(!isFinite(latest) || latest <= 0){ fail++; return; }
    const chgPct = prev ? ((latest - prev) / prev) * 100 : 0;
    out[key] = { price: latest, chgPct: +chgPct.toFixed(3) };
    ok++;
  });
  console.log('TD live: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

// ── TwelveData HISTORY (8 majors via batch) ──────────────────
async function fetchTDHistory(){
  console.log('\n=== TwelveData DAILY HISTORY ===');
  const symbols = Object.values(TD_PAIRS).join(',');
  const url = 'https://api.twelvedata.com/time_series?symbol=' + symbols + '&interval=1day&outputsize=500&apikey=' + TD_KEY;

  let resp;
  try { resp = await getJSON(url); }
  catch(e){ console.error('TD history fetch failed:', e.message); return {}; }

  if(resp.code && resp.message){
    console.error('TD History API error:', resp.code, resp.message);
    return {};
  }

  const out = {};
  let ok = 0, fail = 0;
  Object.entries(TD_PAIRS).forEach(([key, sym]) => {
    const row = resp[sym] || (resp.meta && resp.meta.symbol === sym ? resp : null);
    if(!row || row.code || !row.values || !row.values.length){ fail++; return; }
    const candles = row.values.slice().reverse().map(v => ({
      t: v.datetime,
      o: parseFloat(v.open), h: parseFloat(v.high),
      l: parseFloat(v.low),  c: parseFloat(v.close)
    })).filter(c => isFinite(c.o) && isFinite(c.h) && isFinite(c.l) && isFinite(c.c));
    if(candles.length === 0){ fail++; return; }
    out[key] = candles;
    console.log('  TD ' + sym + ': ' + candles.length + ' candles');
    ok++;
  });
  console.log('TD history: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

// ── OANDA fetch (one pair at a time, but free + fast) ────────
async function fetchOANDACandles(pairKey, instrument, granularity, count){
  const url = 'https://api-fxpractice.oanda.com/v3/instruments/'
    + encodeURIComponent(instrument)
    + '/candles?granularity=' + granularity
    + '&count=' + count
    + '&price=M';  // M = mid prices (between bid/ask)

  const headers = {
    'Authorization': 'Bearer ' + OANDA_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'ViKingInvest/1.0'
  };

  try {
    const resp = await getJSON(url, headers);
    if(!resp.candles || !Array.isArray(resp.candles) || resp.candles.length === 0){
      console.warn('  OANDA ' + pairKey + ' (' + instrument + '): empty candles array');
      return null;
    }
    // Convert OANDA format: {time, mid: {o, h, l, c}, complete, volume}
    // to our format: {t, o, h, l, c}
    const candles = resp.candles
      .filter(c => c.complete && c.mid)
      .map(c => ({
        t: granularity === 'D' ? c.time.slice(0,10) : c.time.slice(0,16),
        o: parseFloat(c.mid.o),
        h: parseFloat(c.mid.h),
        l: parseFloat(c.mid.l),
        c: parseFloat(c.mid.c)
      }))
      .filter(c => isFinite(c.o) && isFinite(c.h) && isFinite(c.l) && isFinite(c.c));
    if(candles.length === 0) return null;
    return candles;
  } catch(e){
    console.warn('  OANDA ' + pairKey + ' (' + instrument + ') ERROR: ' + e.message.slice(0,150));
    return null;
  }
}

async function fetchOANDALivePrices(){
  if(!OANDA_TOKEN) return {};
  console.log('\n=== OANDA LIVE PRICES (M15 candles) ===');
  const out = {};
  let ok = 0, fail = 0;

  for(const [key, instrument] of Object.entries(OANDA_PAIRS)){
    const candles = await fetchOANDACandles(key, instrument, 'M15', 2);
    if(!candles || candles.length === 0){ fail++; continue; }
    const latest = candles[candles.length - 1].c;
    const prev = candles.length > 1 ? candles[candles.length - 2].c : latest;
    if(!isFinite(latest) || latest <= 0){ fail++; continue; }
    const chgPct = prev ? ((latest - prev) / prev) * 100 : 0;
    out[key] = { price: latest, chgPct: +chgPct.toFixed(3) };
    console.log('  OANDA ' + instrument + ': ' + latest.toFixed(5));
    ok++;
    await sleep(100); // courteous pacing
  }
  console.log('OANDA live: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

async function fetchOANDAHistory(){
  if(!OANDA_TOKEN) return {};
  console.log('\n=== OANDA DAILY HISTORY ===');
  const out = {};
  let ok = 0, fail = 0;

  for(const [key, instrument] of Object.entries(OANDA_PAIRS)){
    const candles = await fetchOANDACandles(key, instrument, 'D', 500);
    if(!candles || candles.length === 0){ fail++; continue; }
    out[key] = candles;
    console.log('  OANDA ' + instrument + ': ' + candles.length + ' candles');
    ok++;
    await sleep(100);
  }
  console.log('OANDA history: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

// ── Coinbase public exchange (no auth) ───────────────────────
async function fetchCoinbaseCandles(pairKey, product, granularitySec, candleCount){
  // Coinbase candles endpoint: max 300 per request, so we cap candleCount accordingly.
  // For 500 daily candles we'd need 2 calls; keep it simple at 300 for now.
  const now = Math.floor(Date.now() / 1000);
  const start = now - (candleCount * granularitySec);
  const startISO = new Date(start * 1000).toISOString();
  const endISO = new Date(now * 1000).toISOString();

  const url = 'https://api.exchange.coinbase.com/products/' + product
    + '/candles?granularity=' + granularitySec
    + '&start=' + encodeURIComponent(startISO)
    + '&end=' + encodeURIComponent(endISO);

  try {
    const resp = await getJSON(url);
    if(!Array.isArray(resp) || resp.length === 0){
      console.warn('  Coinbase ' + pairKey + ': empty response');
      return null;
    }
    // Coinbase format: [time, low, high, open, close, volume] (newest first)
    // Reverse to oldest-first to match TD/OANDA
    const candles = resp.slice().reverse().map(c => ({
      t: granularitySec >= 86400
        ? new Date(c[0] * 1000).toISOString().slice(0,10)
        : new Date(c[0] * 1000).toISOString().slice(0,16),
      o: c[3], h: c[2], l: c[1], c: c[4]
    })).filter(c => isFinite(c.o) && isFinite(c.h) && isFinite(c.l) && isFinite(c.c));
    if(candles.length === 0) return null;
    return candles;
  } catch(e){
    console.warn('  Coinbase ' + pairKey + ' ERROR: ' + e.message.slice(0,150));
    return null;
  }
}

async function fetchCoinbaseLivePrices(){
  console.log('\n=== Coinbase LIVE PRICES (15-min candles) ===');
  const out = {};
  let ok = 0, fail = 0;

  for(const [key, product] of Object.entries(COINBASE_PAIRS)){
    const candles = await fetchCoinbaseCandles(key, product, 900, 2); // 900 = 15 min
    if(!candles || candles.length === 0){ fail++; continue; }
    const latest = candles[candles.length - 1].c;
    const prev = candles.length > 1 ? candles[candles.length - 2].c : latest;
    if(!isFinite(latest) || latest <= 0){ fail++; continue; }
    const chgPct = prev ? ((latest - prev) / prev) * 100 : 0;
    out[key] = { price: latest, chgPct: +chgPct.toFixed(3) };
    console.log('  Coinbase ' + product + ': ' + latest);
    ok++;
    await sleep(150); // Coinbase rate limit is 10/sec; we use 6.6/sec to be safe
  }
  console.log('Coinbase live: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

async function fetchCoinbaseHistory(){
  console.log('\n=== Coinbase DAILY HISTORY ===');
  const out = {};
  let ok = 0, fail = 0;

  for(const [key, product] of Object.entries(COINBASE_PAIRS)){
    // Daily granularity = 86400 sec; 300 candles ≈ 10 months of history
    const candles = await fetchCoinbaseCandles(key, product, 86400, 300);
    if(!candles || candles.length === 0){ fail++; continue; }
    out[key] = candles;
    console.log('  Coinbase ' + product + ': ' + candles.length + ' candles (' + candles[0].t + ' → ' + candles[candles.length-1].t + ')');
    ok++;
    await sleep(150);
  }
  console.log('Coinbase history: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

// ── Main orchestration ───────────────────────────────────────
(async () => {
  console.log('=== STARTING FETCH (v4 — TD + OANDA + Coinbase) ===');
  console.log('Time:', new Date().toISOString());
  console.log('TD_KEY:', TD_KEY ? 'set ('+TD_KEY.length+' chars)' : 'MISSING');
  console.log('OANDA_TOKEN:', OANDA_TOKEN ? 'set ('+OANDA_TOKEN.length+' chars)' : 'MISSING');

  // Live prices: parallel fetch from all three providers
  const [tdLive, oandaLive, cbLive] = await Promise.all([
    fetchTDLivePrices(),
    fetchOANDALivePrices(),
    fetchCoinbaseLivePrices()
  ]);

  const allLive = Object.assign({}, tdLive, oandaLive, cbLive);
  const liveCount = Object.keys(allLive).length;

  if(liveCount === 0){
    console.error('CATASTROPHIC: all 3 providers returned empty — refusing to overwrite prices.json');
    process.exit(1);
  }

  fs.writeFileSync('prices.json', JSON.stringify({
    updated: new Date().toISOString(),
    prices: allLive
  }, null, 2));
  console.log('\n✓ Wrote prices.json with ' + liveCount + ' pairs (TD:' + Object.keys(tdLive).length
    + ', OANDA:' + Object.keys(oandaLive).length
    + ', Coinbase:' + Object.keys(cbLive).length + ')');

  // ── 15-MIN INTRADAY ACCUMULATOR ─────────────────────────────
  // Each run, append the current 15-min snapshot to a rolling intraday history.
  // After ~7 days we'll have ~672 candles per pair (96 candles/day × 7).
  // This unlocks 15-min OB/FVG detection in the dashboard without paying for
  // TwelveData Grow tier.
  //
  // Schema: { updated: ISO, intraday: { pair: [{t, o, h, l, c}, ...] } }
  // Each candle is a 15-min snapshot (OHLC approximated from the live tick —
  // since we don't have actual OHLC from a single fetch, we use price as
  // close, and reconstruct H/L from min/max of consecutive snapshots).
  //
  // For this to be useful, we need to track high/low ACROSS the 15-min window.
  // First implementation (simpler): just record close prices with timestamps.
  // The dashboard can then compute its own OHLC from groupings of these.
  // After 1 week of accumulation, we'll have enough data for OB detection.

  try {
    let intraday = {};
    try {
      if(fs.existsSync('intraday.json')){
        const existing = JSON.parse(fs.readFileSync('intraday.json', 'utf8'));
        intraday = existing.intraday || {};
      }
    } catch(e){
      console.warn('  Warning: intraday.json read failed, starting fresh:', e.message);
      intraday = {};
    }

    const ts = new Date().toISOString();
    const MAX_BARS_PER_PAIR = 700; // ~7.3 days at 15-min resolution
    let appended = 0;

    Object.keys(allLive).forEach(function(k){
      if(!intraday[k]) intraday[k] = [];
      const px = allLive[k] && allLive[k].price;
      if(!isFinite(px) || px <= 0) return;

      // Append the new tick. Each entry: {t, p}.
      // Note: this is not true OHLC — it's a single price snapshot every 15 min.
      // The dashboard will treat consecutive snapshots as bar boundaries.
      const last = intraday[k][intraday[k].length - 1];
      // Skip duplicate timestamps (workflow re-run within same minute)
      if(last && last.t === ts) return;
      intraday[k].push({ t: ts, p: px });
      appended++;

      // Trim to rolling window
      if(intraday[k].length > MAX_BARS_PER_PAIR){
        intraday[k] = intraday[k].slice(-MAX_BARS_PER_PAIR);
      }
    });

    fs.writeFileSync('intraday.json', JSON.stringify({
      updated: ts,
      intraday: intraday
    }));

    // Stats: total candles, candles per pair, oldest timestamp
    const pairCount = Object.keys(intraday).length;
    let totalCandles = 0;
    let oldestTs = ts;
    Object.keys(intraday).forEach(function(k){
      totalCandles += intraday[k].length;
      if(intraday[k].length > 0 && intraday[k][0].t < oldestTs){
        oldestTs = intraday[k][0].t;
      }
    });
    console.log('✓ Wrote intraday.json — ' + appended + ' new ticks, '
      + totalCandles + ' total across ' + pairCount + ' pairs');
    console.log('  Oldest tick: ' + oldestTs);
    console.log('  At ~672 ticks/pair (7 days), 15-min OB detection will activate.');
  } catch(e){
    console.error('  Error in intraday accumulator (non-fatal):', e.message);
  }

  // History — only in 00:00-00:30 UTC, or if FORCE_HISTORY=true
  const now = new Date();
  const isHistoryWindow = (now.getUTCHours() === 0 && now.getUTCMinutes() < 30);
  const forceHistory = process.env.FORCE_HISTORY === 'true';

  if(!isHistoryWindow && !forceHistory){
    console.log('\nSkipping history (not 00:00-00:30 UTC, FORCE_HISTORY not set)');
    console.log('Done.');
    return;
  }

  console.log('\n=== HISTORY MODE ACTIVE ===');
  console.log('Reason:', forceHistory ? 'FORCE_HISTORY=true' : 'in 00:00-00:30 UTC window');

  // Wait 65 seconds between TD live and TD history to clear 8/min rate window
  console.log('\nWaiting 65 seconds for TD rate window to clear...');
  await sleep(65000);

  // Fetch all three providers' history (TD sequential, OANDA + Coinbase in parallel after)
  const tdHist = await fetchTDHistory();

  const [oandaHist, cbHist] = await Promise.all([
    fetchOANDAHistory(),
    fetchCoinbaseHistory()
  ]);

  const allHist = Object.assign({}, tdHist, oandaHist, cbHist);
  const histCount = Object.keys(allHist).length;

  if(histCount > 0){
    fs.writeFileSync('history.json', JSON.stringify({
      updated: new Date().toISOString(),
      history: allHist
    }));
    console.log('\n✓ Wrote history.json with ' + histCount + ' pairs (TD:' + Object.keys(tdHist).length
      + ', OANDA:' + Object.keys(oandaHist).length
      + ', Coinbase:' + Object.keys(cbHist).length + ')');
  } else {
    console.log('\n✗ NO HISTORY FETCHED — preserving existing history.json (if any)');
  }

  console.log('Done.');
})().catch(e => {
  console.error('FATAL TOP-LEVEL ERROR:', e && e.message ? e.message : e);
  console.error(e && e.stack ? e.stack : '');
  process.exit(1);
});
