from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from whenever import DateTimeDelta, TimeZoneNotFoundError, ZonedDateTime

from utilities.datetime import (
    _MICROSECONDS_PER_DAY,
    _MICROSECONDS_PER_SECOND,
    ZERO_TIME,
    datetime_duration_to_microseconds,
)
from utilities.math import ParseNumberError, parse_number
from utilities.re import (
    ExtractGroupError,
    ExtractGroupsError,
    extract_group,
    extract_groups,
)
from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

if TYPE_CHECKING:
    from utilities.types import Duration

MAX_SERIALIZABLE_TIMEDELTA = dt.timedelta(days=3652060, microseconds=-1)
MIN_SERIALIZABLE_TIMEDELTA = -MAX_SERIALIZABLE_TIMEDELTA


##


def check_valid_zoned_datetime(datetime: dt.datetime, /) -> None:
    """Check if a zoned datetime is valid."""
    time_zone = ensure_time_zone(datetime)  # skipif-ci-and-windows
    datetime2 = datetime.replace(tzinfo=time_zone)  # skipif-ci-and-windows
    try:  # skipif-ci-and-windows
        result = (
            ZonedDateTime.from_py_datetime(datetime2)
            .to_tz(get_time_zone_name(UTC))
            .to_tz(get_time_zone_name(time_zone))
            .py_datetime()
        )
    except TimeZoneNotFoundError:  # pragma: no cover
        raise _CheckValidZonedDateTimeInvalidTimeZoneError(datetime=datetime) from None
    if result != datetime2:  # skipif-ci-and-windows
        raise _CheckValidZonedDateTimeUnequalError(datetime=datetime, result=result)


@dataclass(kw_only=True, slots=True)
class CheckValidZonedDateTimeError(Exception):
    datetime: dt.datetime


@dataclass(kw_only=True, slots=True)
class _CheckValidZonedDateTimeInvalidTimeZoneError(CheckValidZonedDateTimeError):
    @override
    def __str__(self) -> str:
        return f"Invalid timezone; got {self.datetime.tzinfo}"  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class _CheckValidZonedDateTimeUnequalError(CheckValidZonedDateTimeError):
    result: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Zoned datetime must be valid; got {self.datetime} != {self.result}"  # skipif-ci-and-windows


##


def parse_duration(duration: str, /) -> Duration:
    """Parse a string into a Duration."""
    with suppress(ParseNumberError):
        return parse_number(duration)
    try:
        return parse_timedelta(duration)
    except ParseTimedeltaError:
        raise ParseDurationError(duration=duration) from None


@dataclass(kw_only=True, slots=True)
class ParseDurationError(Exception):
    duration: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse duration; got {self.duration!r}"


##


def parse_timedelta(timedelta: str, /) -> dt.timedelta:
    """Parse a string into a timedelta."""
    with suppress(ExtractGroupError):
        rest = extract_group(r"^-([\w\.]+)$", timedelta)
        return -parse_timedelta(rest)
    try:
        days_str, time_str = extract_groups(r"^P(?:(\d+)D)?(?:T([\w\.]*))?$", timedelta)
    except ExtractGroupsError:
        raise _ParseTimedeltaParseError(timedelta=timedelta) from None
    days = ZERO_TIME if days_str == "" else dt.timedelta(days=int(days_str))
    if time_str == "":
        time = ZERO_TIME
    else:
        time_part = DateTimeDelta.parse_common_iso(f"PT{time_str}").time_part()
        _, nanoseconds = divmod(time_part.in_nanoseconds(), 1000)
        if nanoseconds != 0:
            raise _ParseTimedeltaNanosecondError(
                timedelta=timedelta, nanoseconds=nanoseconds
            )
        time = dt.timedelta(microseconds=int(time_part.in_microseconds()))
    return days + time


@dataclass(kw_only=True, slots=True)
class ParseTimedeltaError(Exception):
    timedelta: str


@dataclass(kw_only=True, slots=True)
class _ParseTimedeltaParseError(ParseTimedeltaError):
    @override
    def __str__(self) -> str:
        return f"Unable to parse timedelta; got {self.timedelta!r}"


@dataclass(kw_only=True, slots=True)
class _ParseTimedeltaNanosecondError(ParseTimedeltaError):
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Unable to parse timedelta; got {self.nanoseconds} nanoseconds"


##


def serialize_duration(duration: Duration, /) -> str:
    """Serialize a duration."""
    if isinstance(duration, int | float):
        return str(duration)
    try:
        return serialize_timedelta(duration)
    except SerializeTimeDeltaError as error:
        raise SerializeDurationError(duration=error.timedelta) from None


@dataclass(kw_only=True, slots=True)
class SerializeDurationError(Exception):
    duration: Duration

    @override
    def __str__(self) -> str:
        return f"Unable to serialize duration; got {self.duration}"


##


def serialize_timedelta(timedelta: dt.timedelta, /) -> str:
    """Serialize a timedelta."""
    try:
        dtd = _to_datetime_delta(timedelta)
    except _ToDateTimeDeltaError as error:
        raise SerializeTimeDeltaError(timedelta=error.timedelta) from None
    return dtd.format_common_iso()


@dataclass(kw_only=True, slots=True)
class SerializeTimeDeltaError(Exception):
    timedelta: dt.timedelta

    @override
    def __str__(self) -> str:
        return f"Unable to serialize timedelta; got {self.timedelta}"


##


def _to_datetime_delta(timedelta: dt.timedelta, /) -> DateTimeDelta:
    """Serialize a timedelta."""
    total_microseconds = datetime_duration_to_microseconds(timedelta)
    if total_microseconds == 0:
        return DateTimeDelta()
    if total_microseconds >= 1:
        days, remainder = divmod(total_microseconds, _MICROSECONDS_PER_DAY)
        seconds, microseconds = divmod(remainder, _MICROSECONDS_PER_SECOND)
        try:
            dtd = DateTimeDelta(days=days, seconds=seconds, microseconds=microseconds)
        except (OverflowError, ValueError):
            raise _ToDateTimeDeltaError(timedelta=timedelta) from None
        months, days, seconds, nanoseconds = dtd.in_months_days_secs_nanos()
        return DateTimeDelta(
            months=months, days=days, seconds=seconds, nanoseconds=nanoseconds
        )
    return -_to_datetime_delta(-timedelta)


@dataclass(kw_only=True, slots=True)
class _ToDateTimeDeltaError(Exception):
    timedelta: dt.timedelta

    @override
    def __str__(self) -> str:
        return f"Unable to create DateTimeDelta; got {self.timedelta}"


__all__ = [
    "MAX_SERIALIZABLE_TIMEDELTA",
    "MIN_SERIALIZABLE_TIMEDELTA",
    "CheckValidZonedDateTimeError",
    "ParseDurationError",
    "ParseTimedeltaError",
    "SerializeDurationError",
    "SerializeTimeDeltaError",
    "check_valid_zoned_datetime",
    "check_valid_zoned_datetime",
    "parse_duration",
    "parse_timedelta",
    "serialize_duration",
    "serialize_timedelta",
]
