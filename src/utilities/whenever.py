from __future__ import annotations

import datetime as dt
import re
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, override
from zoneinfo import ZoneInfo

from whenever import Date, DateTimeDelta, LocalDateTime, Time, ZonedDateTime

from utilities.datetime import (
    _MICROSECONDS_PER_DAY,
    _MICROSECONDS_PER_SECOND,
    ZERO_TIME,
    check_date_not_datetime,
    parse_two_digit_year,
    timedelta_to_microseconds,
)
from utilities.re import (
    ExtractGroupError,
    ExtractGroupsError,
    extract_group,
    extract_groups,
)
from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

if TYPE_CHECKING:
    from utilities.types import Duration

MAX_SERIALIZABLE_TIMEDELTA = dt.timedelta(days=3659635, microseconds=-1)
MIN_SERIALIZABLE_TIMEDELTA = -MAX_SERIALIZABLE_TIMEDELTA


##


def check_valid_zoned_datetime(datetime: dt.datetime, /) -> None:
    """Check if a zoned datetime is valid."""
    time_zone = ensure_time_zone(datetime)  # skipif-ci-and-windows
    datetime2 = datetime.replace(tzinfo=time_zone)  # skipif-ci-and-windows
    result = (  # skipif-ci-and-windows
        ZonedDateTime.from_py_datetime(datetime2)
        .to_tz(get_time_zone_name(UTC))
        .to_tz(get_time_zone_name(time_zone))
        .py_datetime()
    )
    if result != datetime2:  # skipif-ci-and-windows
        raise CheckValidZonedDateimeError(datetime=datetime, result=result)


@dataclass(kw_only=True, slots=True)
class CheckValidZonedDateimeError(Exception):
    datetime: dt.datetime
    result: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Zoned datetime must be valid; got {self.datetime} != {self.result}"  # skipif-ci-and-windows


##


def ensure_date(date: dt.date | str, /) -> dt.date:
    """Ensure the object is a date."""
    if isinstance(date, dt.date):
        check_date_not_datetime(date)
        return date
    try:
        return parse_date(date)
    except ParseDateError as error:
        raise EnsureDateError(date=error.date) from None


@dataclass(kw_only=True, slots=True)
class EnsureDateError(Exception):
    date: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure date; got {self.date!r}"


##


def ensure_duration(duration: Duration | str, /) -> Duration:
    """Ensure the object is a Duration."""
    if isinstance(duration, int | float | dt.timedelta):
        return duration
    try:
        return parse_duration(duration)
    except ParseDurationError as error:
        raise EnsureDurationError(duration=error.duration) from None


@dataclass(kw_only=True, slots=True)
class EnsureDurationError(Exception):
    duration: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure duration; got {self.duration!r}"


##


def ensure_local_datetime(datetime: dt.datetime | str, /) -> dt.datetime:
    """Ensure the object is a local datetime."""
    if isinstance(datetime, dt.datetime):
        return datetime
    try:
        return parse_local_datetime(datetime)
    except ParseLocalDateTimeError as error:
        raise EnsureLocalDateTimeError(datetime=error.datetime) from None


@dataclass(kw_only=True, slots=True)
class EnsureLocalDateTimeError(Exception):
    datetime: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure local datetime; got {self.datetime!r}"


##


def ensure_time(time: dt.time | str, /) -> dt.time:
    """Ensure the object is a time."""
    if isinstance(time, dt.time):
        return time
    try:
        return parse_time(time)
    except ParseTimeError as error:
        raise EnsureTimeError(time=error.time) from None


@dataclass(kw_only=True, slots=True)
class EnsureTimeError(Exception):
    time: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure time; got {self.time!r}"


##


def ensure_timedelta(timedelta: dt.timedelta | str, /) -> dt.timedelta:
    """Ensure the object is a timedelta."""
    if isinstance(timedelta, dt.timedelta):
        return timedelta
    try:
        return parse_timedelta(timedelta)
    except _ParseTimedeltaParseError as error:
        raise _EnsureTimedeltaParseError(timedelta=error.timedelta) from None
    except _ParseTimedeltaNanosecondError as error:
        raise _EnsureTimedeltaNanosecondError(
            timedelta=error.timedelta, nanoseconds=error.nanoseconds
        ) from None


@dataclass(kw_only=True, slots=True)
class EnsureTimedeltaError(Exception):
    timedelta: str


@dataclass(kw_only=True, slots=True)
class _EnsureTimedeltaParseError(EnsureTimedeltaError):
    @override
    def __str__(self) -> str:
        return f"Unable to ensure timedelta; got {self.timedelta!r}"


@dataclass(kw_only=True, slots=True)
class _EnsureTimedeltaNanosecondError(EnsureTimedeltaError):
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Unable to ensure timedelta; got {self.nanoseconds} nanoseconds"


##


def ensure_zoned_datetime(datetime: dt.datetime | str, /) -> dt.datetime:
    """Ensure the object is a zoned datetime."""
    if isinstance(datetime, dt.datetime):
        return datetime
    try:
        return parse_zoned_datetime(datetime)
    except ParseZonedDateTimeError as error:
        raise EnsureZonedDateTimeError(datetime=error.datetime) from None


@dataclass(kw_only=True, slots=True)
class EnsureZonedDateTimeError(Exception):
    datetime: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure zoned datetime; got {self.datetime!r}"


##


_PARSE_DATE_YYYYMMDD_REGEX = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
_PARSE_DATE_YYMMDD_REGEX = re.compile(r"^(\d{2})(\d{2})(\d{2})$")


def parse_date(date: str, /) -> dt.date:
    """Parse a string into a date."""
    try:
        w_date = Date.parse_common_iso(date)
    except ValueError:
        pass
    else:
        return w_date.py_date()
    try:
        ((year, month, day),) = _PARSE_DATE_YYYYMMDD_REGEX.findall(date)
    except ValueError:
        try:
            ((year2, month, day),) = _PARSE_DATE_YYMMDD_REGEX.findall(date)
        except ValueError:
            raise ParseDateError(date=date) from None
        year = parse_two_digit_year(year2)
    return dt.date(year=int(year), month=int(month), day=int(day))


@dataclass(kw_only=True, slots=True)
class ParseDateError(Exception):
    date: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse date; got {self.date!r}"


##


def parse_duration(duration: str, /) -> Duration:
    """Parse a string into a Duration."""
    with suppress(ValueError):
        return int(duration)
    with suppress(ValueError):
        return float(duration)
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


_PARSE_LOCAL_DATETIME_REGEX = re.compile(
    r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})\.?(\d{6})?$"
)


def parse_local_datetime(datetime: str, /) -> dt.datetime:
    """Parse a string into a local datetime."""
    try:
        ldt = LocalDateTime.parse_common_iso(datetime)
    except ValueError:
        pass
    else:
        return ldt.py_datetime()
    try:
        ((year, month, day, hour, minute, second, microsecond),) = (
            _PARSE_LOCAL_DATETIME_REGEX.findall(datetime)
        )
    except ValueError:
        raise ParseLocalDateTimeError(datetime=datetime) from None
    try:
        microsecond_use = int(microsecond)
    except ValueError:
        microsecond_use = 0
    return dt.datetime(
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=microsecond_use,
        tzinfo=UTC,
    ).replace(tzinfo=None)


@dataclass(kw_only=True, slots=True)
class ParseLocalDateTimeError(Exception):
    datetime: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse local datetime; got {self.datetime!r}"


##


def parse_time(time: str, /) -> dt.time:
    """Parse a string into a time."""
    try:
        w_time = Time.parse_common_iso(time)
    except ValueError:
        raise ParseTimeError(time=time) from None
    return w_time.py_time()


@dataclass(kw_only=True, slots=True)
class ParseTimeError(Exception):
    time: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse time; got {self.time!r}"


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


_PARSE_ZONED_DATETIME_REGEX = re.compile(
    r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})\.?(\d{6})?\[([\w/]+)\]$"
)


def parse_zoned_datetime(datetime: str, /) -> dt.datetime:
    """Parse a string into a zoned datetime."""
    try:
        zdt = ZonedDateTime.parse_common_iso(datetime)
    except ValueError:
        pass
    else:
        return zdt.py_datetime()
    try:
        ((year, month, day, hour, minute, second, microsecond, timezone),) = (
            _PARSE_ZONED_DATETIME_REGEX.findall(datetime)
        )
    except ValueError:
        raise ParseZonedDateTimeError(datetime=datetime) from None
    try:  # skipif-ci-and-windows
        microsecond_use = int(microsecond)
    except ValueError:  # skipif-ci-and-windows
        microsecond_use = 0
    return dt.datetime(  # skipif-ci-and-windows
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=microsecond_use,
        tzinfo=ZoneInfo(timezone),
    )


@dataclass(kw_only=True, slots=True)
class ParseZonedDateTimeError(Exception):
    datetime: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse zoned datetime; got {self.datetime!r}"


##


def serialize_date(date: dt.date, /) -> str:
    """Serialize a date."""
    check_date_not_datetime(date)
    return Date.from_py_date(date).format_common_iso()


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


def serialize_local_datetime(datetime: dt.datetime, /) -> str:
    """Serialize a local datetime."""
    try:
        ldt = LocalDateTime.from_py_datetime(datetime)
    except ValueError:
        raise SerializeLocalDateTimeError(datetime=datetime) from None
    return ldt.format_common_iso()


@dataclass(kw_only=True, slots=True)
class SerializeLocalDateTimeError(Exception):
    datetime: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Unable to serialize local datetime; got {self.datetime}"


##


def serialize_time(time: dt.time, /) -> str:
    """Serialize a time."""
    return Time.from_py_time(time).format_common_iso()


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


def serialize_zoned_datetime(datetime: dt.datetime, /) -> str:
    """Serialize a zoned datetime."""
    if datetime.tzinfo is dt.UTC:
        return serialize_zoned_datetime(  # skipif-ci-and-windows
            datetime.replace(tzinfo=UTC)
        )
    try:
        zdt = ZonedDateTime.from_py_datetime(datetime)
    except ValueError:
        raise SerializeZonedDateTimeError(datetime=datetime) from None
    return zdt.format_common_iso()


@dataclass(kw_only=True, slots=True)
class SerializeZonedDateTimeError(Exception):
    datetime: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Unable to serialize zoned datetime; got {self.datetime}"


##


def _to_datetime_delta(timedelta: dt.timedelta, /) -> DateTimeDelta:
    """Serialize a timedelta."""
    total_microseconds = timedelta_to_microseconds(timedelta)
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
    "CheckValidZonedDateimeError",
    "EnsureDateError",
    "EnsureLocalDateTimeError",
    "EnsureTimeError",
    "EnsureTimedeltaError",
    "EnsureZonedDateTimeError",
    "ParseDateError",
    "ParseDurationError",
    "ParseLocalDateTimeError",
    "ParseTimeError",
    "ParseTimedeltaError",
    "ParseZonedDateTimeError",
    "SerializeDurationError",
    "SerializeLocalDateTimeError",
    "SerializeTimeDeltaError",
    "SerializeZonedDateTimeError",
    "check_valid_zoned_datetime",
    "ensure_date",
    "ensure_duration",
    "ensure_local_datetime",
    "ensure_time",
    "ensure_timedelta",
    "ensure_zoned_datetime",
    "parse_date",
    "parse_duration",
    "parse_local_datetime",
    "parse_time",
    "parse_timedelta",
    "parse_zoned_datetime",
    "serialize_date",
    "serialize_duration",
    "serialize_local_datetime",
    "serialize_time",
    "serialize_timedelta",
    "serialize_zoned_datetime",
]
