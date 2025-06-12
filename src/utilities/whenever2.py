from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from logging import LogRecord
from typing import TYPE_CHECKING, Any, assert_never, overload, override

from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

from utilities.datetime import maybe_sub_pct_y
from utilities.math import sign
from utilities.sentinel import Sentinel, sentinel
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME
from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import (
        MaybeCallableDate,
        MaybeCallableZonedDateTime,
        TimeZoneLike,
    )


## bounds


DATE_MIN = Date.from_py_date(dt.date.min)
DATE_MAX = Date.from_py_date(dt.date.max)
TIME_MIN = Time.from_py_time(dt.time.min)
TIME_MAX = Time.from_py_time(dt.time.max)


PLAIN_DATE_TIME_MIN = PlainDateTime.from_py_datetime(dt.datetime.min)  # noqa: DTZ901
PLAIN_DATE_TIME_MAX = PlainDateTime.from_py_datetime(dt.datetime.max)  # noqa: DTZ901
ZONED_DATE_TIME_MIN = PLAIN_DATE_TIME_MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX = PLAIN_DATE_TIME_MAX.assume_tz(UTC.key)


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
TIME_DELTA_MIN = DATE_TIME_DELTA_MIN.time_part()
TIME_DELTA_MAX = DATE_TIME_DELTA_MAX.time_part()


DATE_TIME_DELTA_PARSABLE_MIN = DateTimeDelta(days=-999999, seconds=-316192377600)
DATE_TIME_DELTA_PARSABLE_MAX = DateTimeDelta(days=999999, seconds=316192377600)
DATE_DELTA_PARSABLE_MIN = DateDelta(days=-999999)
DATE_DELTA_PARSABLE_MAX = DateDelta(days=999999)


## common constants


ZERO_TIME = TimeDelta()
MICROSECOND = TimeDelta(microseconds=1)
MILLISECOND = TimeDelta(milliseconds=1)
SECOND = TimeDelta(seconds=1)
MINUTE = TimeDelta(minutes=1)
HOUR = TimeDelta(hours=1)
DAY = DateDelta(days=1)
WEEK = DateDelta(weeks=1)


##


def format_compact(datetime: ZonedDateTime, /) -> str:
    """Convert a zoned datetime to the local time zone, then format."""
    py_datetime = datetime.round().to_tz(LOCAL_TIME_ZONE_NAME).to_plain().py_datetime()
    return py_datetime.strftime(maybe_sub_pct_y("%Y%m%dT%H%M%S"))


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
    return get_now(time_zone=LOCAL_TIME_ZONE)


NOW_LOCAL = get_now_local()


##


def get_today(*, time_zone: TimeZoneLike = UTC) -> Date:
    """Get the current, timezone-aware local date."""
    return get_now(time_zone=time_zone).date()


TODAY_UTC = get_today(time_zone=UTC)


def get_today_local() -> Date:
    """Get the current, timezone-aware local date."""
    return get_today(time_zone=LOCAL_TIME_ZONE)


TODAY_LOCAL = get_today_local()

##


@overload
def to_date(*, date: MaybeCallableDate) -> Date: ...
@overload
def to_date(*, date: None) -> None: ...
@overload
def to_date(*, date: Sentinel) -> Sentinel: ...
@overload
def to_date(*, date: MaybeCallableDate | Sentinel) -> Date | Sentinel: ...
@overload
def to_date(
    *, date: MaybeCallableDate | None | Sentinel = sentinel
) -> Date | None | Sentinel: ...
def to_date(
    *, date: MaybeCallableDate | None | Sentinel = sentinel
) -> Date | None | Sentinel:
    """Get the date."""
    match date:
        case Date() | None | Sentinel():
            return date
        case Callable() as func:
            return to_date(date=func())
        case _ as never:
            assert_never(never)


##


def to_days(delta: DateDelta, /) -> int:
    """Compute the number of days in a date delta."""
    months, days = delta.in_months_days()
    if months != 0:
        raise ToDaysError(months=months)
    return days


@dataclass(kw_only=True, slots=True)
class ToDaysError:
    months: int

    @override
    def __str__(self) -> str:
        return f"Date delta must have no months; got {self.months}"


##


def to_date_time_delta(nanos: int, /) -> DateTimeDelta:
    """Construct a date-time delta."""
    sign_use = sign(nanos)
    micros, nanos = divmod(nanos, int(1e3))
    millis, micros = divmod(micros, int(1e3))
    secs, millis = divmod(millis, int(1e3))
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    days, hours = divmod(hours, 24)
    weeks, days = divmod(days, 7)
    match sign_use:
        case 1:
            if nanos < 0:
                nanos += int(1e3)
                micros -= 1
            if micros < 0:
                micros += int(1e3)
                millis -= 1
            if millis < 0:
                millis += int(1e3)
                secs -= 1
            if secs < 0:
                secs += 60
                mins -= 1
            if mins < 0:
                mins += 60
                hours -= 1
            if hours < 0:
                hours += 24
                days -= 1
            if days < 0:
                days += 7
                weeks -= 1
        case -1:
            if nanos > 0:
                nanos -= int(1e3)
                micros += 1
            if micros > 0:
                micros -= int(1e3)
                millis += 1
            if millis > 0:
                millis -= int(1e3)
                secs += 1
            if secs > 0:
                secs -= 60
                mins += 1
            if mins > 0:
                mins -= 60
                hours += 1
            if hours > 0:
                hours -= 24
                days += 1
            if days > 0:
                days -= 7
                weeks += 1
        case 0:
            ...
    return DateTimeDelta(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=mins,
        seconds=secs,
        microseconds=micros,
        milliseconds=millis,
        nanoseconds=nanos,
    )


##


def to_nanos(delta: DateTimeDelta, /) -> int:
    """Compute the number of nanoseconds in a date-time delta."""
    months, days, secs, nanos = delta.in_months_days_secs_nanos()
    if months != 0:
        raise ToNanosError(months=months)
    total_secs = 24 * 60 * 60 * days + secs
    return int(1e9) * total_secs + nanos


@dataclass(kw_only=True, slots=True)
class ToNanosError:
    months: int

    @override
    def __str__(self) -> str:
        return f"Date-time delta must have no months; got {self.months}"


##


@overload
def to_zoned_date_time(*, date_time: MaybeCallableZonedDateTime) -> ZonedDateTime: ...
@overload
def to_zoned_date_time(*, date_time: None) -> None: ...
@overload
def to_zoned_date_time(*, date_time: Sentinel) -> Sentinel: ...
def to_zoned_date_time(
    *, date_time: MaybeCallableZonedDateTime | None | Sentinel = sentinel
) -> ZonedDateTime | None | Sentinel:
    """Resolve into a zoned date_time."""
    match date_time:
        case ZonedDateTime() | None | Sentinel():
            return date_time
        case Callable() as func:
            return to_zoned_date_time(date_time=func())
        case _ as never:
            assert_never(never)


##


class WheneverLogRecord(LogRecord):
    """Log record powered by `whenever`."""

    zoned_datetime: str

    @override
    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: object,
        args: Any,
        exc_info: Any,
        func: str | None = None,
        sinfo: str | None = None,
    ) -> None:
        super().__init__(
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )
        length = self._get_length()
        plain = format(get_now_local().to_plain().format_common_iso(), f"{length}s")
        time_zone = self._get_time_zone_key()
        self.zoned_datetime = f"{plain}[{time_zone}]"

    @classmethod
    @cache
    def _get_time_zone(cls) -> ZoneInfo:
        """Get the local timezone."""
        try:
            from utilities.tzlocal import get_local_time_zone
        except ModuleNotFoundError:  # pragma: no cover
            return UTC
        return get_local_time_zone()

    @classmethod
    @cache
    def _get_time_zone_key(cls) -> str:
        """Get the local timezone as a string."""
        return cls._get_time_zone().key

    @classmethod
    @cache
    def _get_length(cls) -> int:
        """Get maximum length of a formatted string."""
        now = get_now_local().replace(nanosecond=1000).to_plain()
        return len(now.format_common_iso())


__all__ = [
    "DATE_DELTA_MAX",
    "DATE_DELTA_MIN",
    "DATE_DELTA_PARSABLE_MAX",
    "DATE_DELTA_PARSABLE_MIN",
    "DATE_MAX",
    "DATE_MIN",
    "DATE_TIME_DELTA_MAX",
    "DATE_TIME_DELTA_MIN",
    "DATE_TIME_DELTA_PARSABLE_MAX",
    "DATE_TIME_DELTA_PARSABLE_MIN",
    "DAY",
    "HOUR",
    "MICROSECOND",
    "MILLISECOND",
    "MINUTE",
    "NOW_LOCAL",
    "PLAIN_DATE_TIME_MAX",
    "PLAIN_DATE_TIME_MIN",
    "SECOND",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "TIME_MAX",
    "TIME_MIN",
    "TODAY_LOCAL",
    "TODAY_UTC",
    "WEEK",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
    "ToDaysError",
    "ToNanosError",
    "WheneverLogRecord",
    "format_compact",
    "format_compact",
    "from_timestamp",
    "from_timestamp_millis",
    "from_timestamp_nanos",
    "get_now",
    "get_now_local",
    "get_today",
    "get_today_local",
    "to_date",
    "to_date_time_delta",
    "to_days",
    "to_nanos",
    "to_zoned_date_time",
]
