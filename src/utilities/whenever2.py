from __future__ import annotations

import datetime as dt
from functools import cache
from logging import LogRecord
from typing import TYPE_CHECKING, Any, override

from whenever import Date, DateTimeDelta, PlainDateTime, ZonedDateTime

from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import TimeZoneLike


DATE_MIN = Date.from_py_date(dt.date.min)
DATE_MAX = Date.from_py_date(dt.date.max)
PLAIN_DATE_TIME_MIN = PlainDateTime.from_py_datetime(dt.datetime.min)  # noqa: DTZ901
PLAIN_DATE_TIME_MAX = PlainDateTime.from_py_datetime(dt.datetime.max)  # noqa: DTZ901
ZONED_DATE_TIME_MIN = PLAIN_DATE_TIME_MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX = PLAIN_DATE_TIME_MAX.assume_tz(UTC.key)
DATE_TIME_DELTA_MIN = DateTimeDelta(days=-3652059, seconds=-316192377600)
DATE_TIME_DELTA_MAX = DateTimeDelta(days=3652059, seconds=316192377600)
DATE_DELTA_MIN = DATE_TIME_DELTA_MIN.date_part()
DATE_DELTA_MAX = DATE_TIME_DELTA_MAX.date_part()
TIME_DELTA_MIN = DATE_TIME_DELTA_MIN.time_part()
TIME_DELTA_MAX = DATE_TIME_DELTA_MAX.time_part()


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
    "DATE_MAX",
    "DATE_MIN",
    "DATE_TIME_DELTA_MAX",
    "DATE_TIME_DELTA_MIN",
    "PLAIN_DATE_TIME_MAX",
    "PLAIN_DATE_TIME_MIN",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
    "WheneverLogRecord",
    "from_timestamp",
    "from_timestamp_millis",
    "from_timestamp_nanos",
    "get_now",
    "get_now",
    "get_now_local",
]
