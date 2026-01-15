from __future__ import annotations

from whenever import DateDelta, TimeDelta

ZERO_DAYS = DateDelta()
ZERO_TIME = TimeDelta()
MICROSECOND = TimeDelta(microseconds=1)
MILLISECOND = TimeDelta(milliseconds=1)
SECOND = TimeDelta(seconds=1)
MINUTE = TimeDelta(minutes=1)
HOUR = TimeDelta(hours=1)
DAY = DateDelta(days=1)
WEEK = DateDelta(weeks=1)
MONTH = DateDelta(months=1)
YEAR = DateDelta(years=1)


__all__ = [
    "DAY",
    "HOUR",
    "MICROSECOND",
    "MILLISECOND",
    "MINUTE",
    "MONTH",
    "SECOND",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
]
