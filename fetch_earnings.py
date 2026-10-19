"""Fetch EARNINGS dates for the US equity universe -> earnings-dates.json.

Single-stock earnings are the biggest per-name event and the one that GAPS through stops; the
bridge blacks out NEW entries in the days before a symbol's next report so we never hold into it.

Source: yfinance (Yahoo Finance) — KEYLESS. TwelveData's /earnings endpoint 403s on our plan
(it's a paid add-on), so this uses Yahoo's per-ticker earnings calendar instead: no API key, no
secret. Yahoo is unofficial/scraped and rate-limits, so fail-open per symbol (a miss just leaves
that name un-blacked-out that week). Needs the dep (CI does `pip install yfinance`); Yahoo is
blocked from the dev sandbox proxy, so validate in CI.

    pip install yfinance && python fetch_earnings.py

Writes earnings-dates.json: {generated, source, symbols: {TICKER: ["YYYY-MM-DD", ...]}}.
We only need the NEXT report per name for a forward blackout; recent past dates are harmless.
"""
import json, os, sys, time, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'earnings-dates.json')
PER_TICKER_LIMIT = 16      # ~4yr of quarterly reports + the next upcoming one
SLEEP_SEC = 2.0            # polite spacing — Yahoo rate-limits aggressively


def _us_tickers():
    """US bridge symbols -> bare TwelveData tickers (strip .NAS/.NYSE). BRK-B -> BRK.B.

    Parse the SYMBOLS dict literal straight out of equity_bridge.py with ast — do NOT import
    the bridge. Importing it drags in the whole MT5/detector stack (detect_triggers hard
    `sys.exit`s if `requests` is missing, and that SystemExit punches through except Exception),
    which killed every CI run and left the earnings blackout off. ast keeps this parse
    independent of the bridge's runtime deps. The KEY stored per ticker is BRK.B (matches the
    bridge's _ticker()); fetch_one converts to Yahoo's BRK-B form only for the query."""
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


def fetch_one(ticker):
    """Earnings dates for one ticker via yfinance. Returns sorted ['YYYY-MM-DD', ...] (may be
    empty). Yahoo uses BRK-B (not BRK.B); convert for the query only. Fail-open: raises on hard
    failure so the caller can skip, returns [] if the name simply has no calendar."""
    import yfinance as yf
    ysym = ticker.replace('.', '-')                   # BRK.B -> BRK-B (Yahoo convention)
    t = yf.Ticker(ysym)
    dates = set()
    try:
        df = t.get_earnings_dates(limit=PER_TICKER_LIMIT)
        if df is not None:
            for idx in df.index:
                try:
                    dates.add(idx.strftime('%Y-%m-%d'))
                except Exception:
                    continue
    except Exception:
        pass
    if not dates:                                     # fall back to the single upcoming date
        cal = t.calendar
        ed = cal.get('Earnings Date') if isinstance(cal, dict) else None
        for d in (ed if isinstance(ed, (list, tuple)) else ([ed] if ed else [])):
            try:
                dates.add(d.strftime('%Y-%m-%d'))
            except Exception:
                s = str(d)[:10]
                if len(s) == 10 and s[4] == '-':
                    dates.add(s)
    return sorted(dates)


def main():
    tickers = _us_tickers()
    if not tickers:
        print('::error::no US tickers parsed from equity_bridge.py — nothing to fetch')
        return 1
    print(f"fetching earnings for {len(tickers)} US tickers via yfinance...")
    syms = {}
    for t in tickers:
        try:
            syms[t] = fetch_one(t)
            print(f"  {t:<8} {len(syms[t])} dates")
        except Exception as ex:
            print(f"  {t:<8} skip ({type(ex).__name__}: {str(ex)[:50]})")
        time.sleep(SLEEP_SEC)
    got = {k: v for k, v in syms.items() if v}
    doc = {'generated': dt.datetime.now(dt.timezone.utc).isoformat(),
           'source': 'yahoo finance (yfinance)',
           'symbols': got, 'count': sum(len(v) for v in got.values())}
    with open(OUT, 'w') as f:
        json.dump(doc, f, indent=1)
    print(f"wrote {OUT}: {doc['count']} earnings dates across {len(got)} tickers")
    # Guard: don't let an all-Yahoo-failure silently publish an EMPTY calendar that the commit
    # step would push (leaving the blackout off while looking 'refreshed'). Non-zero so the run
    # is visibly red and the previous good file (if any) is kept.
    return 0 if got else 1


if __name__ == '__main__':
    sys.exit(main())
