"""Deterministic US macro-event schedule (UTC), for historical event-window analysis.

The live feed (events-history.json, fetch_events.py) only pulls the CURRENT week from
forexfactory/faireconomy and keeps a 400-day rolling window — it cannot backfill 3 years.
NFP timing is deterministic, so we generate it: Non-Farm Payrolls releases on the FIRST
FRIDAY of each month at 08:30 ET. ET->UTC is DST-aware (EDT=UTC-4 2nd Sun Mar..1st Sun Nov,
else EST=UTC-5), so NFP is 12:30 UTC in summer, 13:30 UTC in winter.

Note: a handful of months shift NFP off the first Friday (holiday weeks, e.g. early July),
but for a multi-hour event WINDOW over ~36 releases that day-level imprecision is immaterial.
FOMC/CPI can be added as explicit published-date lists if needed (not formulaic like NFP).

    from event_schedule import nfp_times            # list[datetime UTC]
    from event_schedule import nfp_epochs           # list[float] epoch seconds
"""
import datetime as dt


def _nth_weekday(year, month, weekday, n):
    """n-th `weekday` (Mon=0..Sun=6) of month, as a date."""
    d = dt.date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + dt.timedelta(days=offset + 7 * (n - 1))


def us_dst(d):
    """True if date d is in US daylight time (2nd Sun Mar .. 1st Sun Nov)."""
    start = _nth_weekday(d.year, 3, 6, 2)
    end = _nth_weekday(d.year, 11, 6, 1)
    return start <= d < end


def _et_to_utc(d, hour, minute):
    off = 4 if us_dst(d) else 5          # EDT=UTC-4, EST=UTC-5
    return dt.datetime(d.year, d.month, d.day, 0, 0, tzinfo=dt.timezone.utc) \
        + dt.timedelta(hours=hour + off, minutes=minute)


def nfp_times(start_year=2023, end_year=2026):
    """First Friday 08:30 ET of each month in [start_year, end_year], as UTC datetimes."""
    out = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            fri = _nth_weekday(y, m, 4, 1)    # Friday = 4
            out.append(_et_to_utc(fri, 8, 30))
    return out


def nfp_epochs(start_year=2023, end_year=2026):
    return [t.timestamp() for t in nfp_times(start_year, end_year)]


if __name__ == '__main__':
    for t in nfp_times():
        print(t.strftime('%Y-%m-%d %H:%M UTC'), '(EDT)' if us_dst(t.date()) else '(EST)')
