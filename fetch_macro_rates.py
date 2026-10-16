"""Fetch central-bank policy rates from FRED (free, no API key) -> macro-rates.json.

Hooks up a free macro indicator: each currency's short-term central-bank / policy rate,
so we can study how rate hikes/cuts move FX / indices / equities / commodities / crypto
(macro_rate_impact.py). FRED's fredgraph.csv endpoint is keyless and public. Per currency
we try a list of candidate series (policy rate first, interbank fallback) and keep the first
that returns data — robust to a discontinued series.

FRED is blocked by the local agent proxy but reachable from GitHub Actions, so this runs in
CI (fetch-macro-rates.yml). Produces macro-rates.json in the macro-rates.sample.json shape:
  {meta, rates: {USD:[{date,rate}], EUR:[...], ...}}

    python fetch_macro_rates.py
"""
import json, os, sys, datetime as dt
from urllib.request import urlopen, Request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'macro-rates.json')
START = '2022-06-01'          # ~3.5yr of history so a 3yr study has lead-in context

# currency -> candidate FRED series (first with data wins). Policy rate preferred; interbank fallback.
SERIES = {
    'USD': ['DFEDTARU', 'FEDFUNDS'],              # Fed funds target upper / effective
    'EUR': ['ECBDFR', 'ECBMRRFR'],                # ECB deposit facility / main refi
    'GBP': ['IRSTCB01GBM156N', 'IR3TIB01GBM156N'],
    'JPY': ['IRSTCB01JPM156N', 'INTDSRJPM193N'],
    'CAD': ['IRSTCB01CAM156N', 'IR3TIB01CAM156N'],
    'CHF': ['IRSTCB01CHM156N', 'IR3TIB01CHM156N'],
    'AUD': ['IRSTCB01AUM156N', 'IR3TIB01AUM156N'],
    'NZD': ['IRSTCB01NZM156N', 'IR3TIB01NZM156N'],
    'SGD': ['IR3TIB01SGM156N'],
    'NOK': ['IRSTCB01NOM156N', 'IR3TIB01NOM156N'],
    'ZAR': ['IRSTCB01ZAM156N', 'IR3TIB01ZAM156N'],
}


def fetch_series(sid):
    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={START}'
    req = Request(url, headers={'User-Agent': 'viking-macro/1.0'})
    with urlopen(req, timeout=40) as r:
        text = r.read().decode()
    rows = []
    for line in text.strip().split('\n')[1:]:          # skip header
        parts = line.split(',')
        if len(parts) < 2:
            continue
        d, v = parts[0].strip(), parts[-1].strip()
        if v in ('', '.'):
            continue
        try:
            rows.append({'date': d, 'rate': round(float(v), 4)})
        except ValueError:
            continue
    return rows


def main():
    rates = {}; used = {}
    for ccy, candidates in SERIES.items():
        for sid in candidates:
            try:
                rows = fetch_series(sid)
                if rows:
                    rates[ccy] = rows; used[ccy] = sid
                    print(f"  {ccy}: {sid} -> {len(rows)} obs ({rows[0]['date']}..{rows[-1]['date']}, {rows[-1]['rate']}%)")
                    break
            except Exception as ex:
                print(f"  {ccy}: {sid} fail ({str(ex)[:50]})")
        else:
            print(f"  {ccy}: no series returned data")
    if 'USD' not in rates:
        print('::error::USD rate missing — FRED unreachable?'); return 1
    doc = {
        'meta': {'source': 'FRED (fredgraph.csv, keyless)', 'field': 'central-bank / policy rate, % p.a.',
                 'as_of': dt.date.today().isoformat(), 'series_used': used, 'start': START},
        'rates': rates,
    }
    with open(OUT, 'w') as f:
        json.dump(doc, f, indent=1)
    print(f"wrote {OUT}: {len(rates)} currencies")
    return 0


if __name__ == '__main__':
    sys.exit(main())
