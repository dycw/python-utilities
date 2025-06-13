from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import override

from whenever import TimeZoneNotFoundError, ZonedDateTime

from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

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


__all__ = [
    "MAX_SERIALIZABLE_TIMEDELTA",
    "MIN_SERIALIZABLE_TIMEDELTA",
    "CheckValidZonedDateTimeError",
    "check_valid_zoned_datetime",
]
