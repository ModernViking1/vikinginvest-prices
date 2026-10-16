"""US macro-event schedule (UTC) + a persisted historical store (macro-events.json).

The live feed (events-history.json / fetch_events.py) only pulls the CURRENT week from
forexfactory/faireconomy and keeps a 400-day rolling window — it cannot backfill 3 years.
So we persist the three biggest USD events to macro-events.json for easy retrieval:

  NFP  — Non-Farm Payrolls: FIRST FRIDAY 08:30 ET (deterministic, DST-aware).
  FOMC — Fed rate decision: 8 scheduled meetings/yr, 14:00 ET announcement (exact published dates).
  CPI  — Consumer Price Index: 08:30 ET (BLS dates vary month-to-month; dates below are best-effort).

ET->UTC is DST-aware: EDT=UTC-4 (2nd Sun Mar..1st Sun Nov), else EST=UTC-5. So an 08:30 ET event is
12:30 UTC in summer / 13:30 UTC in winter; a 14:00 ET event is 18:00 / 19:00 UTC.

  python event_schedule.py            # print all events
  python event_schedule.py --build    # (re)write macro-events.json

  from event_schedule import load_store            # {'nfp':[epochs], 'fomc':[...], 'cpi':[...]}
"""
import datetime as dt, json, os, argparse

HERE = os.path.dirname(os.path.abspath(__file__))
STORE = os.path.join(HERE, 'macro-events.json')

# FOMC rate-decision announcement days (14:00 ET). Exact published dates.
FOMC_DATES = [
    '2023-02-01', '2023-03-22', '2023-05-03', '2023-06-14', '2023-07-26', '2023-09-20', '2023-11-01', '2023-12-13',
    '2024-01-31', '2024-03-20', '2024-05-01', '2024-06-12', '2024-07-31', '2024-09-18', '2024-11-07', '2024-12-18',
    '2025-01-29', '2025-03-19', '2025-05-07', '2025-06-18', '2025-07-30', '2025-09-17', '2025-10-29', '2025-12-10',
    '2026-01-28', '2026-03-18', '2026-04-29', '2026-06-17', '2026-07-29', '2026-09-16', '2026-10-28', '2026-12-09',
]
# CPI release days (08:30 ET). BLS dates shift a few days month-to-month — BEST-EFFORT; 2023-24 firmer
# than 2025-26. The store can be corrected/extended from the live feed over time (see merge_live).
CPI_DATES = [
    '2023-01-12', '2023-02-14', '2023-03-14', '2023-04-12', '2023-05-10', '2023-06-13', '2023-07-12', '2023-08-10', '2023-09-13', '2023-10-12', '2023-11-14', '2023-12-12',
    '2024-01-11', '2024-02-13', '2024-03-12', '2024-04-10', '2024-05-15', '2024-06-12', '2024-07-11', '2024-08-14', '2024-09-11', '2024-10-10', '2024-11-13', '2024-12-11',
    '2025-01-15', '2025-02-12', '2025-03-12', '2025-04-10', '2025-05-13', '2025-06-11', '2025-07-15', '2025-08-12', '2025-09-11', '2025-10-15', '2025-11-13', '2025-12-10',
    '2026-01-13', '2026-02-11', '2026-03-11', '2026-04-10', '2026-05-12', '2026-06-10', '2026-07-14', '2026-08-12', '2026-09-11', '2026-10-13',
]


def _nth_weekday(year, month, weekday, n):
    d = dt.date(year, month, 1)
    return d + dt.timedelta(days=(weekday - d.weekday()) % 7 + 7 * (n - 1))


def us_dst(d):
    return _nth_weekday(d.year, 3, 6, 2) <= d < _nth_weekday(d.year, 11, 6, 1)


def _et_to_utc(d, hour, minute):
    off = 4 if us_dst(d) else 5
    return dt.datetime(d.year, d.month, d.day, 0, 0, tzinfo=dt.timezone.utc) + dt.timedelta(hours=hour + off, minutes=minute)


def nfp_times(start_year=2023, end_year=2026):
    out = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            out.append(_et_to_utc(_nth_weekday(y, m, 4, 1), 8, 30))
    return out


def _from_dates(dates, hour, minute):
    return [_et_to_utc(dt.date.fromisoformat(s), hour, minute) for s in dates]


def tw_times(start_year=2023, end_year=2026):
    """Triple Witching: 3rd Friday of Mar/Jun/Sep/Dec, timed at the 16:00 ET close (peak
    volume/volatility). Equity-specific (options+index-futures+single-stock expiry) and fully
    deterministic — the biggest scheduled EQUITY event after FOMC."""
    out = []
    for y in range(start_year, end_year + 1):
        for m in (3, 6, 9, 12):
            out.append(_et_to_utc(_nth_weekday(y, m, 4, 3), 16, 0))
    return out


def all_events():
    """[(type, datetime_utc)] sorted by time."""
    evs = [('nfp', t) for t in nfp_times()] + \
          [('fomc', t) for t in _from_dates(FOMC_DATES, 14, 0)] + \
          [('cpi', t) for t in _from_dates(CPI_DATES, 8, 30)] + \
          [('tw', t) for t in tw_times()]
    return sorted(evs, key=lambda x: x[1])


def build_store(path=STORE):
    evs = all_events()
    doc = {
        'generated': dt.datetime.now(dt.timezone.utc).isoformat(),
        'note': 'US macro events for historical event-window analysis. NFP=first-Friday 08:30 ET '
                '(deterministic); FOMC=14:00 ET exact published dates; CPI=08:30 ET best-effort dates. '
                'DST-aware UTC. Built by event_schedule.build_store; retrievable via load_store.',
        'counts': {t: sum(1 for x, _ in evs if x == t) for t in sorted({x for x, _ in evs})},
        'events': [{'type': t, 'ts': x.timestamp(), 'utc': x.isoformat()} for t, x in evs],
    }
    with open(path, 'w') as f:
        json.dump(doc, f, indent=1)
    return doc['counts']


def load_store(path=STORE):
    """{'nfp':[epochs], 'fomc':[...], 'cpi':[...]} — falls back to generating if the file is missing."""
    try:
        evs = json.load(open(path))['events']
    except Exception:
        evs = [{'type': t, 'ts': x.timestamp()} for t, x in all_events()]
    out = {'nfp': [], 'fomc': [], 'cpi': []}
    for e in evs:
        out.setdefault(e['type'], []).append(e['ts'])
    for k in out:
        out[k].sort()
    return out


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--build', action='store_true')
    a = ap.parse_args()
    if a.build:
        print('wrote', STORE, build_store())
    else:
        for t, x in all_events():
            print(f"{x.strftime('%Y-%m-%d %H:%M UTC')}  {t.upper():<4} {'(EDT)' if us_dst(x.date()) else '(EST)'}")
