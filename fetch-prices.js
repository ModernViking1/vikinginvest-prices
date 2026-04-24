// fetch-prices.js — Viking Invest 15-min price fetcher (rate-limit aware)
const fs = require('fs');
const https = require('https');

const API_KEY = process.env.TD_API_KEY;
if(!API_KEY){ console.error('Missing TD_API_KEY env var'); process.exit(1); }

const PAIRS = {
  eurusd:'EUR/USD',
  gbpusd:'GBP/USD',
  usdjpy:'USD/JPY',
  usdcad:'USD/CAD',
  usdchf:'USD/CHF',
  xauusd:'XAU/USD',
  euraud:'EUR/AUD',
  audusd:'AUD/USD'
};

function getJSON(url){
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e){ reject(new Error('JSON parse failed: ' + e.message + ' | first 300 chars: ' + data.slice(0,300))); }
      });
    }).on('error', reject);
  });
}

(async () => {
  const symbols = Object.values(PAIRS).join(',');
  const url = `https://api.twelvedata.com/time_series?symbol=${symbols}&interval=15min&outputsize=2&apikey=${API_KEY}`;
  console.log('Requesting', Object.keys(PAIRS).length, 'symbols from Twelve Data');
  console.log('URL (key redacted):', url.replace(API_KEY, '***'));

  let resp;
  try {
    resp = await getJSON(url);
  } catch(e){
    console.error('Fetch failed:', e.message);
    process.exit(1);
  }

  console.log('Response top-level keys:', Object.keys(resp).slice(0,8).join(', '),
              (Object.keys(resp).length > 8 ? '... (' + Object.keys(resp).length + ' total)' : ''));

  if(resp.code && resp.message){
    console.error('TD API error:', resp.code, resp.message);
    if(resp.code === 429){
      console.error('Hit rate limit — prices.json unchanged this cycle');
    }
    process.exit(1);
  }

  const out = { updated: new Date().toISOString(), prices: {} };
  let ok = 0, fail = 0;

  Object.entries(PAIRS).forEach(([key, sym]) => {
    const row = resp[sym] || (resp.meta && resp.meta.symbol === sym ? resp : null);
    if(!row){
      console.warn('  MISSING:', sym);
      fail++; return;
    }
    if(row.code || row.status === 'error'){
      console.warn('  ERROR for', sym + ':', row.message || JSON.stringify(row).slice(0,100));
      fail++; return;
    }
    if(!row.values || !row.values.length){
      console.warn('  NO VALUES for', sym);
      fail++; return;
    }
    const latest = parseFloat(row.values[0].close);
    const prev = row.values[1] ? parseFloat(row.values[1].close) : latest;
    if(!isFinite(latest) || latest <= 0){
      console.warn('  BAD PRICE for', sym + ':', row.values[0].close);
      fail++; return;
    }
    const chgPct = prev ? ((latest - prev) / prev) * 100 : 0;
    out.prices[key] = { price: latest, chgPct: +chgPct.toFixed(3) };
    ok++;
  });

  console.log('Result: ' + ok + ' OK, ' + fail + ' failed (of ' + Object.keys(PAIRS).length + ')');

  if(ok === 0){
    console.error('All pairs failed — refusing to write empty prices.json');
    process.exit(1);
  }

  fs.writeFileSync('prices.json', JSON.stringify(out, null, 2));
  console.log('Wrote prices.json with', ok, 'pairs');
})();
