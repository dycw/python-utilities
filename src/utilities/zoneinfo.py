from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, assert_never, cast, override
from zoneinfo import ZoneInfo

from whenever import ZonedDateTime

from utilities.types import TIME_ZONES
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME

if TYPE_CHECKING:
    from utilities.types import TimeZone, TimeZoneLike


UTC = ZoneInfo("UTC")


##


def to_zone_info(obj: TimeZoneLike, /) -> ZoneInfo:
    """Convert to a time-zone."""
    match obj:
        case ZoneInfo() as zone_info:
            return zone_info
        case ZonedDateTime() as date_time:
            return ZoneInfo(date_time.tz)
        case "local" | "localtime":
            return LOCAL_TIME_ZONE
        case str() as key:
            return ZoneInfo(key)
        case dt.tzinfo() as tzinfo:
            if tzinfo is dt.UTC:
                return UTC
            raise _ToZoneInfoInvalidTZInfoError(time_zone=obj)
        case dt.datetime() as date_time:
            if date_time.tzinfo is None:
                raise _ToZoneInfoPlainDateTimeError(date_time=date_time)
            return to_zone_info(date_time.tzinfo)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToTimeZoneError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToZoneInfoInvalidTZInfoError(ToTimeZoneError):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone}"


@dataclass(kw_only=True, slots=True)
class _ToZoneInfoPlainDateTimeError(ToTimeZoneError):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Plain date-time: {self.date_time}"


##


def to_time_zone_name(obj: TimeZoneLike, /) -> TimeZone:
    """Convert to a time zone name."""
    match obj:
        case ZoneInfo() as zone_info:
            return cast("TimeZone", zone_info.key)
        case ZonedDateTime() as date_time:
            return cast("TimeZone", date_time.tz)
        case "local" | "localtime":
            return LOCAL_TIME_ZONE_NAME
        case str() as time_zone:
            if time_zone in TIME_ZONES:
                return time_zone
            raise _ToTimeZoneNameInvalidKeyError(time_zone=time_zone)
        case dt.tzinfo() as tzinfo:
            if tzinfo is dt.UTC:
                return cast("TimeZone", UTC.key)
            raise _ToTimeZoneNameInvalidTZInfoError(time_zone=obj)
        case dt.datetime() as date_time:
            if date_time.tzinfo is None:
                raise _ToTimeZoneNamePlainDateTimeError(date_time=date_time)
            return to_time_zone_name(date_time.tzinfo)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToTimeZoneNameError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToTimeZoneNameInvalidKeyError(ToTimeZoneNameError):
    time_zone: str

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone!r}"


@dataclass(kw_only=True, slots=True)
class _ToTimeZoneNameInvalidTZInfoError(ToTimeZoneNameError):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone}"


@dataclass(kw_only=True, slots=True)
class _ToTimeZoneNamePlainDateTimeError(ToTimeZoneNameError):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Plain date-time: {self.date_time}"


__all__ = [
    "UTC",
    "ToTimeZoneError",
    "ToTimeZoneNameError",
    "to_time_zone_name",
    "to_zone_info",
]
