// fetch-prices.js — Viking Invest price fetcher (TwelveData + Finnhub)
// Outputs:
//   prices.json   — current 15-min prices across ALL pairs
//   history.json  — 500 daily candles per pair for real backtest
//
// Architecture:
//   - TwelveData delivers: 8 major pairs (EUR/USD, GBP/USD, USD/JPY, USD/CAD,
//     USD/CHF, XAU/USD, EUR/AUD, AUD/USD) → 15m + daily history
//   - Finnhub delivers: 11 remaining pairs + 2 crypto (NZD/USD, USD/SGD, CAD/JPY,
//     EUR/NZD, GBP/AUD, AUD/NZD, EUR/GBP, AUD/CHF, XAG/USD, BTC/USD, SUI/USD)
//     → 15m + daily history
//   - 3 remaining (USOIL, DE40, DXY) stay on baked overrides until a free
//     source is identified.
//
// API budget:
//   TwelveData: 8 symbols × 96 runs/day = 768/800 credits (under cap)
//   Finnhub:    13 calls × 96 runs/day = 1,248/day (far under 60/min cap)
//   Daily history (once per day): ~21 extra calls

const fs = require(‘fs’);
const https = require(‘https’);

const TD_KEY = process.env.TD_API_KEY;
const FH_KEY = process.env.FH_API_KEY;

if(!TD_KEY){ console.error(‘Missing TD_API_KEY env var’); process.exit(1); }
if(!FH_KEY){
console.warn(‘WARNING: Missing FH_API_KEY — Finnhub pairs will be skipped. Set FH_API_KEY as GitHub secret.’);
}

// ── Pair maps ─────────────────────────────────────────────────
// TwelveData: “EUR/USD” style symbols
const TD_PAIRS = {
eurusd: ‘EUR/USD’,
gbpusd: ‘GBP/USD’,
usdjpy: ‘USD/JPY’,
usdcad: ‘USD/CAD’,
usdchf: ‘USD/CHF’,
xauusd: ‘XAU/USD’,
euraud: ‘EUR/AUD’,
audusd: ‘AUD/USD’
};

// Finnhub: “OANDA:EUR_USD” for forex, “BINANCE:BTCUSDT” for crypto
const FH_PAIRS = {
// Forex via OANDA exchange
nzdusd: { sym: ‘OANDA:NZD_USD’, kind: ‘forex’ },
usdsgd: { sym: ‘OANDA:USD_SGD’, kind: ‘forex’ },
cadjpy: { sym: ‘OANDA:CAD_JPY’, kind: ‘forex’ },
eurnzd: { sym: ‘OANDA:EUR_NZD’, kind: ‘forex’ },
gbpaud: { sym: ‘OANDA:GBP_AUD’, kind: ‘forex’ },
audnzd: { sym: ‘OANDA:AUD_NZD’, kind: ‘forex’ },
eurgbp: { sym: ‘OANDA:EUR_GBP’, kind: ‘forex’ },
audchf: { sym: ‘OANDA:AUD_CHF’, kind: ‘forex’ },
xagusd: { sym: ‘OANDA:XAG_USD’, kind: ‘forex’ },
// Crypto via Binance exchange
btcusd: { sym: ‘BINANCE:BTCUSDT’, kind: ‘crypto’ },
suiusd: { sym: ‘BINANCE:SUIUSDT’, kind: ‘crypto’ }
};

function getJSON(url){
return new Promise((resolve, reject) => {
https.get(url, { headers: { ‘User-Agent’: ‘ViKingInvest-GH-Actions/1.0’ } }, res => {
let data = ‘’;
res.on(‘data’, chunk => data += chunk);
res.on(‘end’, () => {
if(res.statusCode >= 400){
return reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0,200)}`));
}
try { resolve(JSON.parse(data)); }
catch(e){ reject(new Error(’JSON parse failed: ’ + e.message + ’ | first 300: ’ + data.slice(0,300))); }
});
}).on(‘error’, reject);
});
}

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

// ── TwelveData fetch (unchanged from last build) ──────────────
async function fetchTDLivePrices(){
console.log(’\n=== TwelveData LIVE PRICES (15-min) ===’);
const symbols = Object.values(TD_PAIRS).join(’,’);
const url = `https://api.twelvedata.com/time_series?symbol=${symbols}&interval=15min&outputsize=2&apikey=${TD_KEY}`;

let resp;
try { resp = await getJSON(url); }
catch(e){ console.error(‘TD live fetch failed:’, e.message); return {}; }

if(resp.code && resp.message){
console.error(‘TD API error:’, resp.code, resp.message);
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
console.log(`TD: ${ok} OK, ${fail} failed`);
return out;
}

async function fetchTDHistory(){
console.log(’\n=== TwelveData DAILY HISTORY (500 candles) ===’);
const symbols = Object.values(TD_PAIRS).join(’,’);
const url = `https://api.twelvedata.com/time_series?symbol=${symbols}&interval=1day&outputsize=500&apikey=${TD_KEY}`;

let resp;
try { resp = await getJSON(url); }
catch(e){ console.error(‘TD history fetch failed:’, e.message); return {}; }

if(resp.code && resp.message){
console.error(‘TD History API error:’, resp.code, resp.message);
return {};
}

const out = {};
let ok = 0, fail = 0;
Object.entries(TD_PAIRS).forEach(([key, sym]) => {
const row = resp[sym] || (resp.meta && resp.meta.symbol === sym ? resp : null);
if(!row || row.code || !row.values || !row.values.length){ fail++; return; }
const candles = row.values.slice().reverse().map(v => ({
t: v.datetime,
o: parseFloat(v.open),
h: parseFloat(v.high),
l: parseFloat(v.low),
c: parseFloat(v.close)
})).filter(c => isFinite(c.o) && isFinite(c.h) && isFinite(c.l) && isFinite(c.c));
if(candles.length === 0){ fail++; return; }
out[key] = candles;
console.log(`  TD ${sym}: ${candles.length} candles`);
ok++;
});
console.log(`TD history: ${ok} OK, ${fail} failed`);
return out;
}

// ── Finnhub fetch ─────────────────────────────────────────────
// Finnhub returns: {c:[…], h:[…], l:[…], o:[…], t:[…], s:‘ok’|‘no_data’}
async function fetchFHCandles(pairKey, spec, resolution, count){
// `count` = number of candles wanted. Finnhub needs a time range.
// For 15-min candles: span = count × 15min = 15*count minutes
// For daily: span = count days (plus buffer for weekends)
const now = Math.floor(Date.now() / 1000);
let secsSpan;
if(resolution === ‘15’){
secsSpan = count * 15 * 60 * 1.2; // +20% buffer for weekends/gaps
} else if(resolution === ‘D’){
secsSpan = count * 24 * 3600 * 1.5; // +50% buffer for weekends
} else {
secsSpan = count * 60 * 60;
}
const from = now - Math.floor(secsSpan);

const endpoint = spec.kind === ‘crypto’ ? ‘crypto/candle’ : ‘forex/candle’;
const url = `https://finnhub.io/api/v1/${endpoint}?symbol=${encodeURIComponent(spec.sym)}&resolution=${resolution}&from=${from}&to=${now}&token=${FH_KEY}`;

try {
const resp = await getJSON(url);
if(resp.s !== ‘ok’){
// ‘s’ field is ‘no_data’ or ‘error’ when Finnhub has nothing
console.warn(`  FH ${pairKey} (${spec.sym}): status='${resp.s}' — ${resp.error || 'no data'}`);
return null;
}
if(!Array.isArray(resp.c) || resp.c.length === 0) return null;
// Normalize to our {t,o,h,l,c} format
const candles = resp.t.map((tsec, i) => ({
t: new Date(tsec * 1000).toISOString().slice(0, resolution === ‘D’ ? 10 : 16),
o: resp.o[i], h: resp.h[i], l: resp.l[i], c: resp.c[i]
})).filter(c => isFinite(c.o) && isFinite(c.h) && isFinite(c.l) && isFinite(c.c));
return candles;
} catch(e){
console.warn(`  FH ${pairKey} ERROR: ${e.message.slice(0,80)}`);
return null;
}
}

async function fetchFHLivePrices(){
if(!FH_KEY) return {};
console.log(’\n=== Finnhub LIVE PRICES (15-min candles, last 2) ===’);
const out = {};
let ok = 0, fail = 0;

for(const [key, spec] of Object.entries(FH_PAIRS)){
const candles = await fetchFHCandles(key, spec, ‘15’, 2);
if(!candles || candles.length === 0){ fail++; continue; }
const latest = candles[candles.length - 1].c;
const prev = candles.length > 1 ? candles[candles.length - 2].c : latest;
if(!isFinite(latest) || latest <= 0){ fail++; continue; }
const chgPct = prev ? ((latest - prev) / prev) * 100 : 0;
out[key] = { price: latest, chgPct: +chgPct.toFixed(3) };
ok++;
// Be nice to Finnhub: 60/min = 1/sec; we use 100ms gap = 10/sec worst case,
// still well under cap. In practice, 13 sequential calls takes ~2 seconds.
await sleep(100);
}
console.log(`FH live: ${ok} OK, ${fail} failed`);
return out;
}

async function fetchFHHistory(){
if(!FH_KEY) return {};
console.log(’\n=== Finnhub DAILY HISTORY (500 candles) ===’);
const out = {};
let ok = 0, fail = 0;

for(const [key, spec] of Object.entries(FH_PAIRS)){
const candles = await fetchFHCandles(key, spec, ‘D’, 500);
if(!candles || candles.length === 0){ fail++; continue; }
out[key] = candles;
console.log(`  FH ${spec.sym}: ${candles.length} candles`);
ok++;
await sleep(100);
}
console.log(`FH history: ${ok} OK, ${fail} failed`);
return out;
}

// ── Main orchestration ────────────────────────────────────────
(async () => {
// Always fetch live prices from BOTH providers, merge
const [tdLive, fhLive] = await Promise.all([
fetchTDLivePrices(),
fetchFHLivePrices()
]);

const allLive = { …tdLive, …fhLive };
const liveCount = Object.keys(allLive).length;

if(liveCount === 0){
console.error(‘Both providers returned empty — refusing to overwrite prices.json’);
process.exit(1);
}

fs.writeFileSync(‘prices.json’, JSON.stringify({
updated: new Date().toISOString(),
prices: allLive
}, null, 2));
console.log(`\nWrote prices.json with ${liveCount} pairs (TD:${Object.keys(tdLive).length}, FH:${Object.keys(fhLive).length})`);

// History: only in 00:00-00:30 UTC window, or if FORCE_HISTORY=true
const now = new Date();
const isHistoryWindow = (now.getUTCHours() === 0 && now.getUTCMinutes() < 30);
const forceHistory = process.env.FORCE_HISTORY === ‘true’;

if(!isHistoryWindow && !forceHistory){
console.log(’\nSkipping history (not in 00:00-00:30 UTC window, FORCE_HISTORY not set)’);
console.log(‘Done.’);
return;
}

const [tdHist, fhHist] = await Promise.all([
fetchTDHistory(),
fetchFHHistory()
]);

const allHist = { …tdHist, …fhHist };
const histCount = Object.keys(allHist).length;

if(histCount > 0){
fs.writeFileSync(‘history.json’, JSON.stringify({
updated: new Date().toISOString(),
history: allHist
}));
console.log(`\nWrote history.json with ${histCount} pairs (TD:${Object.keys(tdHist).length}, FH:${Object.keys(fhHist).length})`);
} else {
console.log(’\nNo history fetched — preserving existing history.json’);
}

console.log(‘Done.’);
})();