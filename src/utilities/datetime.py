from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from re import sub
from typing import TYPE_CHECKING, Any, Self, TypeGuard, assert_never

from typing_extensions import override

from utilities.platform import SYSTEM, System
from utilities.zoneinfo import (
    HONG_KONG,
    TOKYO,
    UTC,
    ensure_time_zone,
    get_time_zone_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from zoneinfo import ZoneInfo

    from utilities.types import Duration

_DAYS_PER_YEAR = 365.25
_MICROSECONDS_PER_SECOND = int(1e6)
_MICROSECONDS_PER_DAY = 24 * 60 * 60 * _MICROSECONDS_PER_SECOND
SECOND = dt.timedelta(seconds=1)
MINUTE = dt.timedelta(minutes=1)
HOUR = dt.timedelta(hours=1)
DAY = dt.timedelta(days=1)
WEEK = dt.timedelta(weeks=1)
EPOCH_UTC = dt.datetime.fromtimestamp(0, tz=UTC)


def add_weekdays(date: dt.date, /, *, n: int = 1) -> dt.date:
    """Add a number of a weekdays to a given date.

    If the initial date is a weekend, then moving to the adjacent weekday
    counts as 1 move.
    """
    if n == 0 and not is_weekday(date):
        raise AddWeekdaysError(date)
    if n >= 1:
        for _ in range(n):
            date = round_to_next_weekday(date + dt.timedelta(days=1))
    elif n <= -1:
        for _ in range(-n):
            date = round_to_prev_weekday(date - dt.timedelta(days=1))
    return date


class AddWeekdaysError(Exception): ...


def date_to_datetime(
    date: dt.date, /, *, time: dt.time | None = None, time_zone: ZoneInfo | str = UTC
) -> dt.datetime:
    """Expand a date into a datetime."""
    time_use = dt.time(0) if time is None else time
    time_zone_use = ensure_time_zone(time_zone)
    return dt.datetime.combine(date, time_use, tzinfo=time_zone_use)


def date_to_month(date: dt.date, /) -> Month:
    """Collapse a date into a month."""
    return Month(year=date.year, month=date.month)


def duration_to_float(duration: Duration, /) -> float:
    """Ensure the duration is a float."""
    if isinstance(duration, int):
        return float(duration)
    if isinstance(duration, float):
        return duration
    return duration.total_seconds()


def duration_to_timedelta(duration: Duration, /) -> dt.timedelta:
    """Ensure the duration is a timedelta."""
    if isinstance(duration, int | float):
        return dt.timedelta(seconds=duration)
    return duration


def ensure_month(month: Month | str, /) -> Month:
    """Ensure the object is a month."""
    return month if isinstance(month, Month) else parse_month(month)


def format_datetime_local_and_utc(datetime: dt.datetime, /) -> str:
    """Format a local datetime locally & in UTC."""
    if (tzinfo := datetime.tzinfo) is None:
        raise FormatDatetimeLocalAndUTCError(datetime=datetime)
    time_zone = ensure_time_zone(tzinfo)
    if time_zone is UTC:
        return datetime.strftime("%Y-%m-%d %H:%M:%S (%a, UTC)")
    as_utc = datetime.astimezone(UTC)
    local = get_time_zone_name(time_zone)
    if datetime.year != as_utc.year:
        return f"{datetime:%Y-%m-%d %H:%M:%S (%a}, {local}, {as_utc:%Y-%m-%d %H:%M:%S} UTC)"
    if (datetime.month != as_utc.month) or (datetime.day != as_utc.day):
        return (
            f"{datetime:%Y-%m-%d %H:%M:%S (%a}, {local}, {as_utc:%m-%d %H:%M:%S} UTC)"
        )
    return f"{datetime:%Y-%m-%d %H:%M:%S (%a}, {local}, {as_utc:%H:%M:%S} UTC)"


@dataclass(kw_only=True)
class FormatDatetimeLocalAndUTCError(Exception):
    datetime: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Datetime must have a time zone; got {self.datetime}"


def get_half_years(*, n: int = 1) -> dt.timedelta:
    """Get a number of half-years as a timedelta."""
    days_per_half_year = _DAYS_PER_YEAR / 2
    return dt.timedelta(days=round(n * days_per_half_year))


HALF_YEAR = get_half_years(n=1)


def get_months(*, n: int = 1) -> dt.timedelta:
    """Get a number of months as a timedelta."""
    days_per_month = _DAYS_PER_YEAR / 12
    return dt.timedelta(days=round(n * days_per_month))


MONTH = get_months(n=1)


def get_now(*, time_zone: ZoneInfo | str = UTC) -> dt.datetime:
    """Get the current, timezone-aware time."""
    if time_zone == "local":
        from tzlocal import get_localzone

        time_zone_use = get_localzone()
    else:
        time_zone_use = ensure_time_zone(time_zone)
    return dt.datetime.now(tz=time_zone_use)


NOW_UTC = get_now(time_zone=UTC)


def get_now_hk() -> dt.datetime:
    """Get the current time in Hong Kong."""
    return dt.datetime.now(tz=HONG_KONG)


NOW_HK = get_now_hk()


def get_now_tokyo() -> dt.datetime:
    """Get the current time in Tokyo."""
    return dt.datetime.now(tz=TOKYO)


NOW_TOKYO = get_now_tokyo()


def get_quarters(*, n: int = 1) -> dt.timedelta:
    """Get a number of quarters as a timedelta."""
    days_per_quarter = _DAYS_PER_YEAR / 4
    return dt.timedelta(days=round(n * days_per_quarter))


QUARTER = get_quarters(n=1)


def get_today(*, time_zone: ZoneInfo | str = UTC) -> dt.date:
    """Get the current, timezone-aware date."""
    return get_now(time_zone=time_zone).date()


TODAY_UTC = get_today(time_zone=UTC)


def get_today_hk() -> dt.date:
    """Get the current date in Hong Kong."""
    return get_now_hk().date()


TODAY_HK = get_today_hk()


def get_today_tokyo() -> dt.date:
    """Get the current date in Tokyo."""
    return get_now_tokyo().date()


TODAY_TOKYO = get_today_tokyo()


def get_years(*, n: int = 1) -> dt.timedelta:
    """Get a number of years as a timedelta."""
    return dt.timedelta(days=round(n * _DAYS_PER_YEAR))


YEAR = get_years(n=1)


def is_equal_mod_tz(x: dt.datetime, y: dt.datetime, /) -> bool:
    """Check if x == y, modulo timezone."""
    x_aware, y_aware = x.tzinfo is not None, y.tzinfo is not None
    if x_aware and not y_aware:
        return x.astimezone(UTC).replace(tzinfo=None) == y
    if not x_aware and y_aware:
        return x == y.astimezone(UTC).replace(tzinfo=None)
    return x == y


def is_local_datetime(obj: Any, /) -> TypeGuard[dt.datetime]:
    """Check if an object is a local datetime."""
    return isinstance(obj, dt.datetime) and (obj.tzinfo is None)


def is_weekday(date: dt.date, /) -> bool:
    """Check if a date is a weekday."""
    friday = 5
    return date.isoweekday() <= friday


def is_zoned_datetime(obj: Any, /) -> TypeGuard[dt.datetime]:
    """Check if an object is a zoned datetime."""
    return isinstance(obj, dt.datetime) and (obj.tzinfo is not None)


def maybe_sub_pct_y(text: str, /) -> str:
    """Substitute the `%Y' token with '%4Y' if necessary."""
    match SYSTEM:
        case System.windows:  # pragma: os-ne-windows
            return text
        case System.mac:  # pragma: os-ne-macos
            return text
        case System.linux:  # pragma: os-ne-linux
            return sub("%Y", "%4Y", text)
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


@dataclass(order=True, frozen=True)
class Month:
    """Represents a month in time."""

    year: int
    month: int

    def __post_init__(self) -> None:
        try:
            _ = dt.date(self.year, self.month, 1)
        except ValueError:
            raise MonthError(year=self.year, month=self.month) from None

    @override
    def __repr__(self) -> str:
        return serialize_month(self)

    @override
    def __str__(self) -> str:
        return repr(self)

    def __add__(self, other: Any, /) -> Self:
        if not isinstance(other, int):  # pragma: no cover
            return NotImplemented
        years, month = divmod(self.month + other - 1, 12)
        month += 1
        year = self.year + years
        return replace(self, year=year, month=month)

    def __sub__(self, other: Any, /) -> Self:
        if not isinstance(other, int):  # pragma: no cover
            return NotImplemented
        return self + (-other)

    @classmethod
    def from_date(cls, date: dt.date, /) -> Self:
        return cls(year=date.year, month=date.month)

    def to_date(self, /, *, day: int = 1) -> dt.date:
        return dt.date(self.year, self.month, day)


@dataclass(kw_only=True)
class MonthError(Exception):
    year: int
    month: int

    @override
    def __str__(self) -> str:
        return f"Invalid year and month: {self.year}, {self.month}"


MIN_MONTH = Month(dt.date.min.year, dt.date.min.month)
MAX_MONTH = Month(dt.date.max.year, dt.date.max.month)


def parse_month(month: str, /) -> Month:
    """Parse a string into a month."""
    for fmt in ["%Y-%m", "%Y%m", "%Y %m"]:
        try:
            date = dt.datetime.strptime(month, fmt).replace(tzinfo=UTC).date()
        except ValueError:
            pass
        else:
            return Month(date.year, date.month)
    raise ParseMonthError(month=month)


@dataclass(kw_only=True, slots=True)
class ParseMonthError(Exception):
    month: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse month; got {self.month!r}"


def round_to_next_weekday(date: dt.date, /) -> dt.date:
    """Round a date to the next weekday."""
    return _round_to_weekday(date, is_next=True)


def round_to_prev_weekday(date: dt.date, /) -> dt.date:
    """Round a date to the previous weekday."""
    return _round_to_weekday(date, is_next=False)


def _round_to_weekday(date: dt.date, /, *, is_next: bool) -> dt.date:
    """Round a date to the previous weekday."""
    n = 1 if is_next else -1
    while not is_weekday(date):
        date = add_weekdays(date, n=n)
    return date


def serialize_month(month: Month, /) -> str:
    """Serialize a month."""
    return f"{month.year:04}-{month.month:02}"


def yield_days(
    *, start: dt.date | None = None, end: dt.date | None = None, days: int | None = None
) -> Iterator[dt.date]:
    """Yield the days in a range."""
    if (start is not None) and (end is not None) and (days is None):
        date = start
        while date <= end:
            yield date
            date += dt.timedelta(days=1)
        return
    if (start is not None) and (end is None) and (days is not None):
        date = start
        for _ in range(days):
            yield date
            date += dt.timedelta(days=1)
        return
    if (start is None) and (end is not None) and (days is not None):
        date = end
        for _ in range(days):
            yield date
            date -= dt.timedelta(days=1)
        return
    raise YieldDaysError(start=start, end=end, days=days)


@dataclass(kw_only=True, slots=True)
class YieldDaysError(Exception):
    start: dt.date | None
    end: dt.date | None
    days: int | None

    @override
    def __str__(self) -> str:
        return (
            f"Invalid arguments: start={self.start}, end={self.end}, days={self.days}"
        )


def yield_weekdays(
    *, start: dt.date | None = None, end: dt.date | None = None, days: int | None = None
) -> Iterator[dt.date]:
    """Yield the weekdays in a range."""
    if (start is not None) and (end is not None) and (days is None):
        date = round_to_next_weekday(start)
        while date <= end:
            yield date
            date = round_to_next_weekday(date + dt.timedelta(days=1))
        return
    if (start is not None) and (end is None) and (days is not None):
        date = round_to_next_weekday(start)
        for _ in range(days):
            yield date
            date = round_to_next_weekday(date + dt.timedelta(days=1))
        return
    if (start is None) and (end is not None) and (days is not None):
        date = round_to_prev_weekday(end)
        for _ in range(days):
            yield date
            date = round_to_prev_weekday(date - dt.timedelta(days=1))
        return
    raise YieldWeekdaysError(start=start, end=end, days=days)


@dataclass(kw_only=True, slots=True)
class YieldWeekdaysError(Exception):
    start: dt.date | None
    end: dt.date | None
    days: int | None

    @override
    def __str__(self) -> str:
        return (
            f"Invalid arguments: start={self.start}, end={self.end}, days={self.days}"
        )


__all__ = [
    "DAY",
    "EPOCH_UTC",
    "HALF_YEAR",
    "HOUR",
    "MAX_MONTH",
    "MINUTE",
    "MIN_MONTH",
    "MONTH",
    "NOW_HK",
    "NOW_TOKYO",
    "NOW_UTC",
    "QUARTER",
    "SECOND",
    "TODAY_HK",
    "TODAY_TOKYO",
    "TODAY_UTC",
    "WEEK",
    "YEAR",
    "AddWeekdaysError",
    "FormatDatetimeLocalAndUTCError",
    "Month",
    "MonthError",
    "ParseMonthError",
    "YieldDaysError",
    "YieldWeekdaysError",
    "add_weekdays",
    "date_to_datetime",
    "date_to_month",
    "duration_to_float",
    "duration_to_timedelta",
    "ensure_month",
    "format_datetime_local_and_utc",
    "get_half_years",
    "get_months",
    "get_now",
    "get_now_hk",
    "get_now_tokyo",
    "get_quarters",
    "get_today",
    "get_today_hk",
    "get_today_tokyo",
    "get_years",
    "is_local_datetime",
    "is_weekday",
    "is_zoned_datetime",
    "maybe_sub_pct_y",
    "parse_month",
    "round_to_next_weekday",
    "round_to_prev_weekday",
    "serialize_month",
    "yield_days",
    "yield_weekdays",
]
