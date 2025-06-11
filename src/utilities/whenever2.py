from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from whenever import Date, PlainDateTime, ZonedDateTime

from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from utilities.types import TimeZoneLike


DATE_MIN = Date.from_py_date(dt.date.min)
DATE_MAX = Date.from_py_date(dt.date.max)
PLAIN_DATETIME_MIN = PlainDateTime.from_py_datetime(dt.datetime.min)  # noqa: DTZ901
PLAIN_DATETIME_MAX = PlainDateTime.from_py_datetime(dt.datetime.max)  # noqa: DTZ901
ZONED_DATETIME_MIN = PLAIN_DATETIME_MIN.assume_utc()
ZONED_DATETIME_MAX = PLAIN_DATETIME_MAX.assume_utc()


##


def from_timestamp(i: float, /, *, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get a zoned datetime from a timestamp."""
    return ZonedDateTime.from_timestamp(i, tz=get_time_zone_name(time_zone))


def from_timestamp_millis(i: int, /, *, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get a zoned datetime from a timestamp (in milliseconds)."""
    return ZonedDateTime.from_timestamp_millis(i, tz=get_time_zone_name(time_zone))


def from_timestamp_nanos(i: int, /, *, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get a zoned datetime from a timestamp (in nanoseconds)."""
    return ZonedDateTime.from_timestamp_nanos(i, tz=get_time_zone_name(time_zone))


##


def get_now(*, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get the current zoned datetime."""
    return ZonedDateTime.now(get_time_zone_name(time_zone))


NOW_UTC = get_now(time_zone=UTC)


def get_now_local() -> ZonedDateTime:
    """Get the current local time."""
    return get_now(time_zone="local")


__all__ = [
    "DATE_MAX",
    "DATE_MIN",
    "PLAIN_DATETIME_MAX",
    "PLAIN_DATETIME_MIN",
    "ZONED_DATETIME_MAX",
    "ZONED_DATETIME_MIN",
    "from_timestamp",
    "from_timestamp_millis",
    "from_timestamp_nanos",
    "get_now",
    "get_now",
    "get_now_local",
]
