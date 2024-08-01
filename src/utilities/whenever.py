from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from re import search
from typing import TYPE_CHECKING

from numpy import datetime_data
from typing_extensions import override
from whenever import Date, DateTimeDelta, LocalDateTime, ZonedDateTime, microseconds

from utilities.datetime import _DAYS_PER_YEAR, get_months
from utilities.iterables import one
from utilities.text import ensure_str
from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo


def ensure_date(date: dt.date | str, /) -> dt.date:
    """Ensure the object is a date."""
    return date if isinstance(date, dt.date) else parse_date(date)


def ensure_local_datetime(datetime: dt.datetime | str, /) -> dt.datetime:
    """Ensure the object is a local datetime."""
    if isinstance(datetime, dt.datetime):
        return datetime
    return parse_local_datetime(datetime)


def ensure_timedelta(timedelta: dt.timedelta | str, /) -> dt.timedelta:
    """Ensure the object is a timedelta."""
    if isinstance(timedelta, dt.timedelta):
        return timedelta
    return parse_timedelta(timedelta)


def ensure_zoned_datetime(datetime: dt.datetime | str, /) -> dt.datetime:
    """Ensure the object is a zoned datetime."""
    if isinstance(datetime, dt.datetime):
        return datetime
    return parse_zoned_datetime(datetime)


def parse_date(date: str, /) -> dt.date:
    """Parse a string into a date."""
    try:
        delta = Date.parse_common_iso(date)
    except ValueError:
        raise ParseDateError(date=date) from None
    return delta.py_date()


@dataclass(kw_only=True, slots=True)
class ParseDateError(Exception):
    date: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse date; got {self.date!r}"


def parse_local_datetime(datetime: str, /) -> dt.datetime:
    """Parse a string into a local datetime."""
    try:
        ldt = LocalDateTime.parse_common_iso(datetime)
    except ValueError:
        raise ParseLocalDateTimeError(datetime=datetime) from None
    return ldt.py_datetime()


@dataclass(kw_only=True, slots=True)
class ParseLocalDateTimeError(Exception):
    datetime: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse local datetime; got {self.datetime!r}"


def parse_timedelta(timedelta: str, /) -> dt.timedelta:
    """Parse a string into a timedelta."""
    try:
        delta = DateTimeDelta.parse_common_iso(timedelta)
    except ValueError:
        raise _ParseTimedeltaParseError(timedelta=timedelta) from None
    date_part = delta.date_part()
    months, days = date_part.in_months_days()
    months_as_days = get_months(n=months).days
    total_days = months_as_days + days
    time_part = delta.time_part()
    _, nanoseconds = divmod(time_part.in_nanoseconds(), 1000)
    if nanoseconds != 0:
        raise _ParseTimedeltaNanosecondError(
            timedelta=timedelta, nanoseconds=nanoseconds
        )
    total_micros = time_part.in_microseconds()
    return dt.timedelta(days=total_days, microseconds=total_micros)


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


def parse_zoned_datetime(datetime: str, /) -> dt.datetime:
    """Parse a string into a zoned datetime."""
    try:
        ztd = ZonedDateTime.parse_common_iso(datetime)
    except ValueError:
        raise ParseZonedDateTimeError(datetime=datetime) from None
    return ztd.py_datetime()


@dataclass(kw_only=True, slots=True)
class ParseZonedDateTimeError(Exception):
    datetime: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse zoned datetime; got {self.datetime!r}"


def serialize_date(date: dt.date, /) -> str:
    """Serialize a date."""
    if isinstance(date, dt.datetime):
        return serialize_date(date.date())
    return Date.from_py_date(date).format_common_iso()


def serialize_local_datetime(datetime: dt.datetime, /) -> str:
    """Serialize a local datetime."""
    return LocalDateTime.from_py_datetime(datetime).format_common_iso()


def serialize_timedelta(timedelta: dt.timedelta, /) -> str:
    """Serialize a timedelta."""
    return str(_to_datetime_delta(timedelta))


def serialize_zoned_datetime(datetime: dt.datetime, /) -> str:
    """Serialize a zoned datetime."""
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


def _to_datetime_delta(timedelta: dt.timedelta, /) -> DateTimeDelta:
    """Serialize a timedelta."""
    total_seconds = 24 * 60 * 60 * timedelta.days + timedelta.seconds
    total_micros = int(1e6) * total_seconds + timedelta.microseconds
    return DateTimeDelta(
        microseconds=total_micros,
    )


__all__ = [
    "ParseDateError",
    "ParseLocalDateTimeError",
    "ParseTimedeltaError",
    "ParseZonedDateTimeError",
    "ensure_date",
    "ensure_local_datetime",
    "ensure_timedelta",
    "ensure_zoned_datetime",
    "parse_date",
    "parse_local_datetime",
    "parse_timedelta",
    "parse_zoned_datetime",
    "serialize_date",
    "serialize_local_datetime",
    "serialize_timedelta",
    "serialize_zoned_datetime",
]
