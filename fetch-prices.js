// fetch-prices.js — Viking Invest price fetcher v5 (OANDA + Coinbase only)
//
// Architecture (TwelveData removed in v5):
//   - OANDA v20 demo API: all FX, commodities, indices
//   - Coinbase public exchange: all crypto
//   - Baked override: DXY synthesised from majors
//
// Why TwelveData was dropped: it required an API key, capped at 8 calls/min
// on the free tier (~768 calls/day with 96 cycles), and the 8 majors it
// covered are all available on OANDA's demo API at no additional cost.
// Removing it eliminates the TD_API_KEY env var requirement and a second
// failure surface.
//
// API budgets (all comfortably under provider limits):
//   OANDA demo:  ~30 calls × 96 cycles = 2880/day. OANDA demo allows
//                ~120 requests/sec; trivially under.
//   Coinbase:    ~6 calls × 96 cycles = 576/day. Public rate-limited at
//                10/sec; trivially under.
//
// Outputs:
//   prices.json   — current 15-min prices for all live-sourced pairs
//   history.json  — 500 daily candles per pair (00:05 UTC + on-demand)

const fs = require('fs');
const https = require('https');

const OANDA_TOKEN = process.env.OANDA_TOKEN;

if(!OANDA_TOKEN){
  console.error('Missing OANDA_TOKEN env var — cannot fetch FX/index/commodity prices');
  process.exit(1);
}

// ── OANDA pairs ─────────────────────────────────────────────
// All FX, commodities, and equity-index instruments. Pair-key → OANDA
// instrument symbol (underscore-delimited per OANDA convention).
const OANDA_PAIRS = {
  // FX majors (moved from TwelveData in v5)
  eurusd: 'EUR_USD',
  gbpusd: 'GBP_USD',
  usdjpy: 'USD_JPY',
  usdcad: 'USD_CAD',
  usdchf: 'USD_CHF',
  audusd: 'AUD_USD',
  euraud: 'EUR_AUD',
  // FX crosses (existing)
  nzdusd: 'NZD_USD',
  usdsgd: 'USD_SGD',
  cadjpy: 'CAD_JPY',
  eurnzd: 'EUR_NZD',
  // gbpaud removed 2026-06-10h — chronic ~50% win rate, no improvement
  // from tighter RSI gate. Re-add: 'gbpaud': 'GBP_AUD'.
  audnzd: 'AUD_NZD',
  eurgbp: 'EUR_GBP',
  // audchf removed 2026-06-08 — low win-rate drag on aggregate (was hurting
  // overall stats). Re-add: 'audchf': 'AUD_CHF'.
  // FX additions (v5 — user requested)
  // audcad removed 2026-06-10i — low win-rate drag. Re-add: 'audcad': 'AUD_CAD'.
  gbpcad:  'GBP_CAD',
  nzdjpy:  'NZD_JPY',
  // usdnok removed 2026-06-08 — low win-rate drag. Re-add: 'usdnok': 'USD_NOK'.
  gbpnzd:  'GBP_NZD',
  // eursek removed 2026-06-08 — low win-rate drag. Re-add: 'eursek': 'EUR_SEK'.
  // FX additions (v7 — 2026-06-03)
  // nzdcad removed 2026-06-10 — low win-rate drag. Re-add: 'nzdcad': 'NZD_CAD'.
  eurnok:  'EUR_NOK',
  nzdchf:  'NZD_CHF',
  // gbpchf removed 2026-06-10 — low win-rate drag. Re-add: 'gbpchf': 'GBP_CHF'.
  usdzar:  'USD_ZAR',
  // usdcnh removed 2026-06-10 — low win-rate drag. Re-add: 'usdcnh': 'USD_CNH'.
  eursgd:  'EUR_SGD',
  // Commodities
  xauusd: 'XAU_USD',     // gold
  xagusd: 'XAG_USD',     // silver
  usoil:  'BCO_USD',     // Brent Crude Oil (matches TradingView XBRUSD)
  wtiusd: 'WTICO_USD',   // WTI Crude Oil (NYMEX) — added 2026-06-10
  natgas: 'NATGAS_USD',  // Natural Gas (Henry Hub) — added 2026-06-10.
                         // User requested "XNG_USD" — that's a Bloomberg/TV
                         // identifier; OANDA's native ticker is NATGAS_USD.
  xptusd: 'XPT_USD',     // Platinum — added 2026-06-10
  // Equity indices
  // de40 REINSTATED 2026-06-15lll — see detect_triggers.py FIB_ENTRY_PAIRS.
  de40:    'DE30_EUR',   // DAX 40 (OANDA's DE30 instrument actually tracks DAX 40)
  ftse100: 'UK100_GBP',  // FTSE 100
  // dj30 reinstated 2026-06-17 — index-MACD gates deployed today give it
  // a fresh shot at a real edge before the earlier 44% WR audit applies.
  dj30:    'US30_USD',   // Dow Jones Industrial Average
  nas100:  'NAS100_USD', // Nasdaq 100
  spx500:  'SPX500_USD', // S&P 500
  // Equity index additions (v7)
  jp225:   'JP225_USD',  // Nikkei 225 (note: index level is 225, not 250)
  // fra40 (CAC 40) removed 2026-06-10 — low win-rate drag. Re-add: 'fra40': 'FR40_EUR'.
  // IBEX 35 removed 2026-06-08 — neither ES35_EUR nor ESP35_EUR returned
  // valid candles on OANDA's practice endpoint, so the instrument was
  // dropped rather than block the rest of the v7 batch. To re-add: try
  // alternate tickers (ESPIX_EUR is another option some accounts have)
  // and restore the row to MKTS, fetch-prices.js, fetch_historical_ohlc.py,
  // publish_intraday_ohlc.py, detect_triggers.py FIB_ENTRY_PAIRS +
  // PAIR_DISPLAY, and RULES_FINGERPRINT().fibPairs.
  // Helper: USD/SEK needed for synthetic DXY computation in the dashboard,
  // even though it doesn't have a tradeable MKTS entry of its own.
  usdsek: 'USD_SEK',
};

// ── Coinbase: crypto pairs ───────────────────────────────────
const COINBASE_PAIRS = {
  btcusd: 'BTC-USD',
  suiusd: 'SUI-USD',
  // v5 additions (user requested)
  ethusd: 'ETH-USD',
  solusd: 'SOL-USD',
  xrpusd: 'XRP-USD',
  taousd: 'TAO-USD',
  // v6 additions (user requested)
  nearusd: 'NEAR-USD',
  // hypeusd removed 2026-06-10 — low win-rate drag. Re-add: 'hypeusd': 'HYPE-USD'.
  // ondousd dropped 2026-06-17 (user). Re-add: 'ondousd': 'ONDO-USD'.
  // ltcusd removed 2026-06-10 — low win-rate drag. Re-add: 'ltcusd': 'LTC-USD'.
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
    await sleep(80); // courteous pacing — total ~2.5s for 30 pairs
  }
  console.log('OANDA live: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

async function fetchOANDAHistory(){
  console.log('\n=== OANDA DAILY HISTORY ===');
  const out = {};
  let ok = 0, fail = 0;

  for(const [key, instrument] of Object.entries(OANDA_PAIRS)){
    const candles = await fetchOANDACandles(key, instrument, 'D', 500);
    if(!candles || candles.length === 0){ fail++; continue; }
    out[key] = candles;
    console.log('  OANDA ' + instrument + ': ' + candles.length + ' candles');
    ok++;
    await sleep(80);
  }
  console.log('OANDA history: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

// ── Coinbase public exchange (no auth) ───────────────────────
async function fetchCoinbaseCandles(pairKey, product, granularitySec, candleCount){
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
    const candles = await fetchCoinbaseCandles(key, product, 900, 2);
    if(!candles || candles.length === 0){ fail++; continue; }
    const latest = candles[candles.length - 1].c;
    const prev = candles.length > 1 ? candles[candles.length - 2].c : latest;
    if(!isFinite(latest) || latest <= 0){ fail++; continue; }
    const chgPct = prev ? ((latest - prev) / prev) * 100 : 0;
    out[key] = { price: latest, chgPct: +chgPct.toFixed(3) };
    console.log('  Coinbase ' + product + ': ' + latest);
    ok++;
    await sleep(150);
  }
  console.log('Coinbase live: ' + ok + ' OK, ' + fail + ' failed');
  return out;
}

async function fetchCoinbaseHistory(){
  console.log('\n=== Coinbase DAILY HISTORY ===');
  const out = {};
  let ok = 0, fail = 0;

  for(const [key, product] of Object.entries(COINBASE_PAIRS)){
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
  console.log('=== STARTING FETCH (v5 — OANDA + Coinbase, TwelveData dropped) ===');
  console.log('Time:', new Date().toISOString());
  console.log('OANDA_TOKEN:', OANDA_TOKEN ? 'set ('+OANDA_TOKEN.length+' chars)' : 'MISSING');

  // Live prices: parallel fetch from both providers
  const [oandaLive, cbLive] = await Promise.all([
    fetchOANDALivePrices(),
    fetchCoinbaseLivePrices()
  ]);

  const allLive = Object.assign({}, oandaLive, cbLive);
  const liveCount = Object.keys(allLive).length;

  if(liveCount === 0){
    console.error('CATASTROPHIC: both providers returned empty — refusing to overwrite prices.json');
    process.exit(1);
  }

  fs.writeFileSync('prices.json', JSON.stringify({
    updated: new Date().toISOString(),
    prices: allLive
  }, null, 2));
  console.log('\n✓ Wrote prices.json with ' + liveCount + ' pairs (OANDA:' + Object.keys(oandaLive).length
    + ', Coinbase:' + Object.keys(cbLive).length + ')');

  // ── 15-MIN INTRADAY ACCUMULATOR (DISABLED v5+) ──────────────
  // Superseded by intraday-ohlc.json from publish_intraday_ohlc.py.
  // See FORCE_LEGACY_INTRADAY=true escape hatch below for emergency use.
  const forceLegacy = process.env.FORCE_LEGACY_INTRADAY === 'true';
  if(forceLegacy){
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
      const MAX_BARS_PER_PAIR = 700;
      let appended = 0;

      Object.keys(allLive).forEach(function(k){
        if(!intraday[k]) intraday[k] = [];
        const px = allLive[k] && allLive[k].price;
        if(!isFinite(px) || px <= 0) return;
        const last = intraday[k][intraday[k].length - 1];
        if(last && last.t === ts) return;
        intraday[k].push({ t: ts, p: px });
        appended++;
        if(intraday[k].length > MAX_BARS_PER_PAIR){
          intraday[k] = intraday[k].slice(-MAX_BARS_PER_PAIR);
        }
      });

      fs.writeFileSync('intraday.json', JSON.stringify({
        updated: ts,
        intraday: intraday
      }));
      console.log('✓ Wrote intraday.json (FORCE_LEGACY_INTRADAY=true) — ' + appended + ' new ticks');
    } catch(e){
      console.error('  Error in legacy intraday accumulator (non-fatal):', e.message);
    }
  } else {
    console.log('Skipping legacy intraday.json write (use FORCE_LEGACY_INTRADAY=true to override). The OHLC publisher (publish_intraday_ohlc.py) is the canonical source.');
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

  const [oandaHist, cbHist] = await Promise.all([
    fetchOANDAHistory(),
    fetchCoinbaseHistory()
  ]);

  const allHist = Object.assign({}, oandaHist, cbHist);
  const histCount = Object.keys(allHist).length;

  if(histCount > 0){
    fs.writeFileSync('history.json', JSON.stringify({
      updated: new Date().toISOString(),
      history: allHist
    }));
    console.log('\n✓ Wrote history.json with ' + histCount + ' pairs (OANDA:' + Object.keys(oandaHist).length
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
