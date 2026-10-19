"""Fetch historical EARNINGS dates for the US equity universe -> earnings-dates.json.

Single-stock earnings are the biggest per-name event and the one that GAPS through stops.
equity_gap_study.py already finds earnings-SCALE gaps straight from price (no key needed);
this fetcher adds the PRECISE earnings calendar so gaps can be labelled earnings-vs-other
and a flat-before-earnings rule can key off real dates.

Uses the TwelveData /earnings endpoint. Needs TWELVEDATA_API_KEY (a GitHub Actions secret —
not present in the local sandbox), so run it in CI or anywhere the key is set:

    TWELVEDATA_API_KEY=... python fetch_earnings.py

Writes earnings-dates.json: {generated, source, symbols: {TICKER: ["YYYY-MM-DD", ...]}}.
Free tier is US-only and rate-limited (8 req/min) — fail-open per symbol.
"""
import json, os, sys, time, datetime as dt
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'earnings-dates.json')
START = '2023-01-01'


def _us_tickers():
    """US bridge symbols -> bare TwelveData tickers (strip .NAS/.NYSE). BRK-B -> BRK.B.

    Parse the SYMBOLS dict literal straight out of equity_bridge.py with ast — do NOT import
    the bridge. Importing it drags in the whole MT5/detector stack (detect_triggers hard
    `sys.exit`s if `requests` is missing, and that SystemExit punches through except Exception),
    which killed every CI run and left the earnings blackout off. ast keeps this fetcher
    stdlib-only so it runs anywhere the TD key is set."""
    import ast
    syms = []
    try:
        src = open(os.path.join(HERE, 'equity_bridge.py'), encoding='utf-8').read()
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == 'SYMBOLS' for t in node.targets):
                d = ast.literal_eval(node.value)
                syms = [v for v in d.values() if v.endswith('.NAS') or v.endswith('.NYSE')]
                break
    except Exception as ex:
        print(f"::warning::could not parse SYMBOLS from equity_bridge.py ({ex})")
        syms = []
    out = []
    for s in syms:
        t = s.rsplit('.', 1)[0].replace('-', '.')     # BRK-B.NYSE -> BRK.B
        out.append(t)
    return sorted(set(out))


def fetch_one(ticker, key):
    end = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    url = 'https://api.twelvedata.com/earnings?' + urlencode(
        {'symbol': ticker, 'apikey': key, 'start_date': START, 'end_date': end, 'outputsize': 5000})
    with urlopen(url, timeout=30) as r:
        d = json.loads(r.read().decode())
    if isinstance(d, dict) and d.get('status') == 'error':
        raise RuntimeError(d.get('message', 'TD error'))
    rows = d.get('earnings', d if isinstance(d, list) else [])
    dates = sorted({row['date'] for row in rows if row.get('date')})
    return dates


def main():
    key = os.environ.get('TWELVEDATA_API_KEY', '').strip()
    if not key:
        print('::error::TWELVEDATA_API_KEY not set — run in CI or set the key. '
              '(equity_gap_study.py needs no key and already measures gap risk.)')
        return 1
    tickers = _us_tickers()
    print(f"fetching earnings for {len(tickers)} US tickers since {START}...")
    syms = {}
    for i, t in enumerate(tickers):
        try:
            syms[t] = fetch_one(t, key)
            print(f"  {t:<8} {len(syms[t])} dates")
        except Exception as ex:
            print(f"  {t:<8} skip ({str(ex)[:60]})")
        time.sleep(8.0)                               # free-tier 8 req/min
    doc = {'generated': dt.datetime.now(dt.timezone.utc).isoformat(),
           'source': 'twelvedata.com /earnings', 'start': START,
           'symbols': syms, 'count': sum(len(v) for v in syms.values())}
    with open(OUT, 'w') as f:
        json.dump(doc, f, indent=1)
    print(f"wrote {OUT}: {doc['count']} earnings dates across {len(syms)} tickers")
    return 0


if __name__ == '__main__':
    sys.exit(main())
