from __future__ import annotations

from whenever import Date, DateDelta, DateTimeDelta, PlainDateTime, TimeDelta

from utilities.zoneinfo import UTC

# whenever


ZONED_DATE_TIME_MIN = PlainDateTime.MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX = PlainDateTime.MAX.assume_tz(UTC.key)


DATE_TIME_DELTA_MIN = DateTimeDelta(
    weeks=-521722,
    days=-5,
    hours=-23,
    minutes=-59,
    seconds=-59,
    milliseconds=-999,
    microseconds=-999,
    nanoseconds=-999,
)
DATE_TIME_DELTA_MAX = DateTimeDelta(
    weeks=521722,
    days=5,
    hours=23,
    minutes=59,
    seconds=59,
    milliseconds=999,
    microseconds=999,
    nanoseconds=999,
)
DATE_DELTA_MIN = DATE_TIME_DELTA_MIN.date_part()
DATE_DELTA_MAX = DATE_TIME_DELTA_MAX.date_part()
TIME_DELTA_MIN = TimeDelta(hours=-87831216)
TIME_DELTA_MAX = TimeDelta(hours=87831216)


DATE_TIME_DELTA_PARSABLE_MIN = DateTimeDelta(
    weeks=-142857,
    hours=-23,
    minutes=-59,
    seconds=-59,
    milliseconds=-999,
    microseconds=-999,
    nanoseconds=-999,
)
DATE_TIME_DELTA_PARSABLE_MAX = DateTimeDelta(
    weeks=142857,
    hours=23,
    minutes=59,
    seconds=59,
    milliseconds=999,
    microseconds=999,
    nanoseconds=999,
)
DATE_DELTA_PARSABLE_MIN = DateDelta(days=-999999)
DATE_DELTA_PARSABLE_MAX = DateDelta(days=999999)


DATE_TWO_DIGIT_YEAR_MIN = Date(1969, 1, 1)
DATE_TWO_DIGIT_YEAR_MAX = Date(DATE_TWO_DIGIT_YEAR_MIN.year + 99, 12, 31)


## common constants


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
    "DATE_DELTA_MAX",
    "DATE_DELTA_MIN",
    "DATE_DELTA_PARSABLE_MAX",
    "DATE_DELTA_PARSABLE_MIN",
    "DATE_TIME_DELTA_MAX",
    "DATE_TIME_DELTA_MIN",
    "DATE_TIME_DELTA_PARSABLE_MAX",
    "DATE_TIME_DELTA_PARSABLE_MIN",
    "DATE_TWO_DIGIT_YEAR_MAX",
    "DATE_TWO_DIGIT_YEAR_MIN",
    "DAY",
    "HOUR",
    "MICROSECOND",
    "MILLISECOND",
    "MINUTE",
    "MONTH",
    "SECOND",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
]
