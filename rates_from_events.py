"""Reconstruct the in-sim central-bank policy-rate path from events-history.json -> macro-rates.json.

External rate feeds (FRED/Yahoo/ECB) are blocked from both the sandbox (proxy policy) and CI
(FRED times out). But the ForexFactory feed we already accumulate (events-history.json) carries
the actual in-sim policy-rate LEVEL at each rate decision via the forecast/prev fields — and this
world's path differs from the real one (e.g. USD 3.75% -> 4.00%), so this is the only correct
source. Window is bounded by events-history retention (~400d) and grows as fetch_events.py runs.

Per currency we map its rate-decision event title, parse the % level (prev before, forecast at the
decision), and build a dated step series in the macro-rates.sample.json shape.

    python rates_from_events.py
"""
import json, os, re, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
EVENTS = os.path.join(HERE, 'events-history.json')
OUT = os.path.join(HERE, 'macro-rates.json')

# currency -> rate-decision event title (the one that carries the % level)
RATE_TITLE = {
    'USD': 'federal funds rate', 'EUR': 'main refinancing rate', 'GBP': 'official bank rate',
    'JPY': 'boj policy rate', 'AUD': 'cash rate', 'NZD': 'official cash rate',
    'CAD': 'overnight rate', 'CHF': 'snb policy rate',
}
_PCT = re.compile(r'(-?\d+(?:\.\d+)?)\s*%')


def _pct(v):
    if not v:
        return None
    m = _PCT.search(v.replace('<', '').replace('>', ''))
    return float(m.group(1)) if m else None


def main():
    evs = json.load(open(EVENTS)).get('events', [])
    rates = {}
    for ccy, title in RATE_TITLE.items():
        pts = {}
        for e in evs:
            if (e.get('currency') != ccy) or (title not in (e.get('title', '').lower())):
                continue
            d = (e.get('time') or '')[:10]
            if not d:
                continue
            # the level going INTO the decision (prev) anchors the pre-date rate; the set level
            # (actual if present, else forecast) anchors from the decision date forward.
            prev = _pct(e.get('previous')); setv = _pct(e.get('actual')) or _pct(e.get('forecast'))
            dd = dt.date.fromisoformat(d)
            if prev is not None:
                pts.setdefault((dd - dt.timedelta(days=1)).isoformat(), prev)
            if setv is not None:
                pts[d] = setv
        if pts:
            rates[ccy] = [{'date': k, 'rate': v} for k, v in sorted(pts.items())]
    if 'USD' not in rates:
        print('::warn:: no USD rate decisions found in events-history.json');
    doc = {
        'meta': {'source': 'events-history.json (ForexFactory in-sim rate decisions)',
                 'field': 'central-bank policy rate, % p.a. (prev/forecast levels)',
                 'as_of': dt.date.today().isoformat(),
                 'note': 'Window bounded by events-history retention (~400d); grows as fetch_events.py runs. '
                         'In-sim path (differs from real-world). Use FRED (fetch_macro_rates.py) once reachable for deeper history.'},
        'rates': rates,
    }
    json.dump(doc, open(OUT, 'w'), indent=1)
    for ccy, s in rates.items():
        print(f"  {ccy}: {len(s)} pts  {s[0]['date']}({s[0]['rate']}%) .. {s[-1]['date']}({s[-1]['rate']}%)")
    print(f"wrote {OUT}: {len(rates)} currencies")


if __name__ == '__main__':
    main()
