from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

from numpy import datetime_data
from typing_extensions import override
from whenever import DateTimeDelta, microseconds

from utilities.datetime import _DAYS_PER_YEAR, get_months


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


def serialize_timedelta(timedelta: dt.timedelta, /) -> str:
    """Serialize a timedelta."""
    return str(_to_datetime_delta(timedelta))


def _to_datetime_delta(timedelta: dt.timedelta, /) -> DateTimeDelta:
    """Serialize a timedelta."""
    total_seconds = 24 * 60 * 60 * timedelta.days + timedelta.seconds
    total_micros = int(1e6) * total_seconds + timedelta.microseconds
    return DateTimeDelta(
        microseconds=total_micros,
    )


__all__ = ["ParseTimedeltaError", "parse_timedelta", "serialize_timedelta"]
