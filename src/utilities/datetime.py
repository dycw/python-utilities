from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from re import sub
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Self,
    TypeAlias,
    TypeGuard,
    assert_never,
    overload,
)

from typing_extensions import override

from utilities.functions import ensure_not_none
from utilities.platform import SYSTEM
from utilities.zoneinfo import (
    UTC,
    HongKong,
    Tokyo,
    ensure_time_zone,
    get_time_zone_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import Duration, ZoneInfoLike

_DAYS_PER_YEAR = 365.25
_MICROSECONDS_PER_MILLISECOND = int(1e3)
_MICROSECONDS_PER_SECOND = int(1e6)
_SECONDS_PER_DAY = 24 * 60 * 60
_MICROSECONDS_PER_DAY = _MICROSECONDS_PER_SECOND * _SECONDS_PER_DAY
ZERO_TIME = dt.timedelta(0)
MICROSECOND = dt.timedelta(microseconds=1)
MILLISECOND = dt.timedelta(milliseconds=1)
SECOND = dt.timedelta(seconds=1)
MINUTE = dt.timedelta(minutes=1)
HOUR = dt.timedelta(hours=1)
DAY = dt.timedelta(days=1)
WEEK = dt.timedelta(weeks=1)
EPOCH_UTC = dt.datetime.fromtimestamp(0, tz=UTC)
EPOCH_DATE = EPOCH_UTC.date()
EPOCH_NAIVE = EPOCH_UTC.replace(tzinfo=None)


##


def add_weekdays(date: dt.date, /, *, n: int = 1) -> dt.date:
    """Add a number of a weekdays to a given date.

    If the initial date is a weekend, then moving to the adjacent weekday
    counts as 1 move.
    """
    check_date_not_datetime(date)
    if n == 0 and not is_weekday(date):
        raise AddWeekdaysError(date)
    if n >= 1:
        for _ in range(n):
            date = round_to_next_weekday(date + DAY)
    elif n <= -1:
        for _ in range(-n):
            date = round_to_prev_weekday(date - DAY)
    return date


class AddWeekdaysError(Exception): ...


##


def check_date_not_datetime(date: dt.date, /) -> None:
    """Check if a date is not a datetime."""
    if not is_instance_date_not_datetime(date):
        raise CheckDateNotDatetimeError(date=date)


@dataclass(kw_only=True, slots=True)
class CheckDateNotDatetimeError(Exception):
    date: dt.date

    @override
    def __str__(self) -> str:
        return f"Date must not be a datetime; got {self.date}"


##


def check_zoned_datetime(datetime: dt.datetime, /) -> None:
    """Check if a datetime is zoned."""
    if datetime.tzinfo is None:
        raise CheckZonedDatetimeError(datetime=datetime)


@dataclass(kw_only=True, slots=True)
class CheckZonedDatetimeError(Exception):
    datetime: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Datetime must be zoned; got {self.datetime}"


##


def date_add_timedelta(date: dt.date, timedelta: dt.timedelta, /) -> dt.date:
    """Add a timedelta to a date, checking that it remains a date."""
    check_date_not_datetime(date)
    if (timedelta.seconds != 0) or (timedelta.microseconds != 0):
        raise DateAddTimeDeltaError(timedelta=timedelta)
    return date + timedelta


@dataclass(kw_only=True, slots=True)
class DateAddTimeDeltaError(Exception):
    timedelta: dt.timedelta

    @override
    def __str__(self) -> str:
        return f"Timedelta must be day-only; got {self.timedelta}"


##


def date_to_datetime(
    date: dt.date, /, *, time: dt.time | None = None, time_zone: ZoneInfoLike = UTC
) -> dt.datetime:
    """Expand a date into a datetime."""
    check_date_not_datetime(date)
    time_use = dt.time(0) if time is None else time
    time_zone_use = ensure_time_zone(time_zone)
    return dt.datetime.combine(date, time_use, tzinfo=time_zone_use)


##


def date_to_month(date: dt.date, /) -> Month:
    """Collapse a date into a month."""
    check_date_not_datetime(date)
    return Month(year=date.year, month=date.month)


##


def days_since_epoch(date: dt.date, /) -> int:
    """Compute the number of days since the epoch."""
    check_date_not_datetime(date)
    return timedelta_since_epoch(date).days


def days_since_epoch_to_date(days: int, /) -> dt.date:
    """Convert a number of days since the epoch to a date."""
    return EPOCH_DATE + days * DAY


##


def drop_microseconds(datetime: dt.datetime, /) -> dt.datetime:
    """Drop the microseconds of a datetime object."""
    milliseconds, _ = divmod(datetime.microsecond, _MICROSECONDS_PER_MILLISECOND)
    microseconds = _MICROSECONDS_PER_MILLISECOND * milliseconds
    return datetime.replace(microsecond=microseconds)


def drop_milli_and_microseconds(datetime: dt.datetime, /) -> dt.datetime:
    """Drop the milliseconds & microseconds of a datetime object."""
    return datetime.replace(microsecond=0)


##


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


##


def ensure_month(month: Month | str, /) -> Month:
    """Ensure the object is a month."""
    if isinstance(month, Month):
        return month
    try:
        return parse_month(month)
    except ParseMonthError as error:
        raise EnsureMonthError(month=error.month) from None


@dataclass(kw_only=True, slots=True)
class EnsureMonthError(Exception):
    month: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure month; got {self.month!r}"


##


def format_datetime_local_and_utc(datetime: dt.datetime, /) -> str:
    """Format a local datetime locally & in UTC."""
    check_zoned_datetime(datetime)
    time_zone = ensure_time_zone(
        ensure_not_none(datetime.tzinfo, desc="datetime.tzinfo")
    )
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


##


def get_half_years(*, n: int = 1) -> dt.timedelta:
    """Get a number of half-years as a timedelta."""
    days_per_half_year = _DAYS_PER_YEAR / 2
    return dt.timedelta(days=round(n * days_per_half_year))


HALF_YEAR = get_half_years(n=1)


##


def get_months(*, n: int = 1) -> dt.timedelta:
    """Get a number of months as a timedelta."""
    days_per_month = _DAYS_PER_YEAR / 12
    return dt.timedelta(days=round(n * days_per_month))


MONTH = get_months(n=1)


##


def get_now(*, time_zone: ZoneInfoLike = UTC) -> dt.datetime:
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
    return dt.datetime.now(tz=HongKong)


NOW_HK = get_now_hk()


def get_now_tokyo() -> dt.datetime:
    """Get the current time in Tokyo."""
    return dt.datetime.now(tz=Tokyo)


NOW_TOKYO = get_now_tokyo()


##


def get_quarters(*, n: int = 1) -> dt.timedelta:
    """Get a number of quarters as a timedelta."""
    days_per_quarter = _DAYS_PER_YEAR / 4
    return dt.timedelta(days=round(n * days_per_quarter))


QUARTER = get_quarters(n=1)


##


def get_today(*, time_zone: ZoneInfoLike = UTC) -> dt.date:
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


##


def get_years(*, n: int = 1) -> dt.timedelta:
    """Get a number of years as a timedelta."""
    return dt.timedelta(days=round(n * _DAYS_PER_YEAR))


YEAR = get_years(n=1)


##


def is_equal_as_months(x: DateOrMonth, y: DateOrMonth, /) -> bool:
    """Check if x == y as months."""
    x_month = Month.from_date(x) if isinstance(x, dt.date) else x
    y_month = Month.from_date(y) if isinstance(y, dt.date) else y
    return x_month == y_month


##


def is_equal_mod_tz(x: dt.datetime, y: dt.datetime, /) -> bool:
    """Check if x == y, modulo timezone."""
    x_aware, y_aware = x.tzinfo is not None, y.tzinfo is not None
    if x_aware and (not y_aware):
        return x.astimezone(UTC).replace(tzinfo=None) == y
    if (not x_aware) and y_aware:
        return x == y.astimezone(UTC).replace(tzinfo=None)
    return x == y


##


def is_instance_date_not_datetime(obj: Any, /) -> TypeGuard[dt.date]:
    """Check if an object is a date, and not a datetime."""
    return isinstance(obj, dt.date) and not isinstance(obj, dt.datetime)


##


def is_local_datetime(obj: Any, /) -> TypeGuard[dt.datetime]:
    """Check if an object is a local datetime."""
    return isinstance(obj, dt.datetime) and (obj.tzinfo is None)


##


def is_subclass_date_not_datetime(obj: type[Any], /) -> TypeGuard[type[dt.date]]:
    """Check if a class is a date, and not a datetime."""
    return issubclass(obj, dt.date) and not issubclass(obj, dt.datetime)


##


_FRIDAY = 5


def is_weekday(date: dt.date, /) -> bool:
    """Check if a date is a weekday."""
    check_date_not_datetime(date)
    return date.isoweekday() <= _FRIDAY


##


def is_zoned_datetime(obj: Any, /) -> TypeGuard[dt.datetime]:
    """Check if an object is a zoned datetime."""
    return isinstance(obj, dt.datetime) and (obj.tzinfo is not None)


##


def maybe_sub_pct_y(text: str, /) -> str:
    """Substitute the `%Y' token with '%4Y' if necessary."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return text
        case "mac":  # skipif-not-macos
            return text
        case "linux":  # skipif-not-linux
            return sub("%Y", "%4Y", text)
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


##


def microseconds_since_epoch(datetime: dt.datetime, /) -> int:
    """Compute the number of microseconds since the epoch."""
    return timedelta_to_microseconds(timedelta_since_epoch(datetime))


def microseconds_to_timedelta(microseconds: int, /) -> dt.timedelta:
    """Compute a timedelta given a number of microseconds."""
    if microseconds == 0:
        return ZERO_TIME
    if microseconds >= 1:
        days, remainder = divmod(microseconds, _MICROSECONDS_PER_DAY)
        seconds, micros = divmod(remainder, _MICROSECONDS_PER_SECOND)
        return dt.timedelta(days=days, seconds=seconds, microseconds=micros)
    return -microseconds_to_timedelta(-microseconds)


def microseconds_since_epoch_to_datetime(microseconds: int, /) -> dt.datetime:
    """Convert a number of microseconds since the epoch to a datetime."""
    return EPOCH_UTC + microseconds_to_timedelta(microseconds)


##


@overload
def milliseconds_since_epoch(
    datetime: dt.datetime, /, *, strict: Literal[True]
) -> int: ...
@overload
def milliseconds_since_epoch(
    datetime: dt.datetime, /, *, strict: bool = False
) -> float: ...
def milliseconds_since_epoch(
    datetime: dt.datetime, /, *, strict: bool = False
) -> float:
    """Compute the number of milliseconds since the epoch."""
    microseconds = microseconds_since_epoch(datetime)
    milliseconds, remainder = divmod(microseconds, _MICROSECONDS_PER_MILLISECOND)
    if strict:
        if remainder == 0:
            return milliseconds
        raise MillisecondsSinceEpochError(datetime=datetime, remainder=remainder)
    return milliseconds + remainder / _MICROSECONDS_PER_MILLISECOND


@dataclass(kw_only=True, slots=True)
class MillisecondsSinceEpochError(Exception):
    datetime: dt.datetime
    remainder: int

    @override
    def __str__(self) -> str:
        return f"Unable to convert {self.datetime} to milliseconds since epoch; got {self.remainder} microsecond(s)"


def milliseconds_since_epoch_to_datetime(milliseconds: int, /) -> dt.datetime:
    """Convert a number of milliseconds since the epoch to a datetime."""
    return EPOCH_UTC + milliseconds_to_timedelta(milliseconds)


def milliseconds_to_timedelta(milliseconds: int, /) -> dt.timedelta:
    """Compute a timedelta given a number of milliseconds."""
    return microseconds_to_timedelta(_MICROSECONDS_PER_MILLISECOND * milliseconds)


##


@dataclass(order=True, unsafe_hash=True, slots=True)
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

    @overload
    def __sub__(self, other: Self, /) -> int: ...
    @overload
    def __sub__(self, other: int, /) -> Self: ...
    def __sub__(self, other: Self | int, /) -> Self | int:
        if isinstance(other, int):  # pragma: no cover
            return self + (-other)
        if isinstance(other, type(self)):
            self_as_int = 12 * self.year + self.month
            other_as_int = 12 * other.year + other.month
            return self_as_int - other_as_int
        return NotImplemented  # pragma: no cover

    @classmethod
    def from_date(cls, date: dt.date, /) -> Self:
        check_date_not_datetime(date)
        return cls(year=date.year, month=date.month)

    def to_date(self, /, *, day: int = 1) -> dt.date:
        return dt.date(self.year, self.month, day)


@dataclass(kw_only=True, slots=True)
class MonthError(Exception):
    year: int
    month: int

    @override
    def __str__(self) -> str:
        return f"Invalid year and month: {self.year}, {self.month}"


DateOrMonth: TypeAlias = dt.date | Month
MIN_MONTH = Month(dt.date.min.year, dt.date.min.month)
MAX_MONTH = Month(dt.date.max.year, dt.date.max.month)


##


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


##


def round_to_next_weekday(date: dt.date, /) -> dt.date:
    """Round a date to the next weekday."""
    return _round_to_weekday(date, prev_or_next="next")


def round_to_prev_weekday(date: dt.date, /) -> dt.date:
    """Round a date to the previous weekday."""
    return _round_to_weekday(date, prev_or_next="prev")


def _round_to_weekday(
    date: dt.date, /, *, prev_or_next: Literal["prev", "next"]
) -> dt.date:
    """Round a date to the previous weekday."""
    check_date_not_datetime(date)
    match prev_or_next:
        case "prev":
            n = -1
        case "next":
            n = 1
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    while not is_weekday(date):
        date = add_weekdays(date, n=n)
    return date


##


def serialize_month(month: Month, /) -> str:
    """Serialize a month."""
    return f"{month.year:04}-{month.month:02}"


##


def timedelta_since_epoch(date: dt.date | dt.datetime, /) -> dt.timedelta:
    """Compute the timedelta since the epoch."""
    if isinstance(date, dt.datetime):
        check_zoned_datetime(date)
        return date.astimezone(UTC) - EPOCH_UTC
    return date - EPOCH_DATE


def timedelta_to_microseconds(timedelta: dt.timedelta, /) -> int:
    """Compute the number of microseconds in a timedelta."""
    return (
        _MICROSECONDS_PER_DAY * timedelta.days
        + _MICROSECONDS_PER_SECOND * timedelta.seconds
        + timedelta.microseconds
    )


@overload
def timedelta_to_milliseconds(
    timedelta: dt.timedelta, /, *, strict: Literal[True]
) -> int: ...
@overload
def timedelta_to_milliseconds(
    timedelta: dt.timedelta, /, *, strict: bool = False
) -> float: ...
def timedelta_to_milliseconds(
    timedelta: dt.timedelta, /, *, strict: bool = False
) -> float:
    """Compute the number of milliseconds in a timedelta."""
    microseconds = timedelta_to_microseconds(timedelta)
    milliseconds, remainder = divmod(microseconds, _MICROSECONDS_PER_MILLISECOND)
    if strict:
        if remainder == 0:
            return milliseconds
        raise TimedeltaToMillisecondsError(timedelta=timedelta, remainder=remainder)
    return milliseconds + remainder / _MICROSECONDS_PER_MILLISECOND


@dataclass(kw_only=True, slots=True)
class TimedeltaToMillisecondsError(Exception):
    timedelta: dt.timedelta
    remainder: int

    @override
    def __str__(self) -> str:
        return f"Unable to convert {self.timedelta} to milliseconds; got {self.remainder} microsecond(s)"


##


def yield_days(
    *, start: dt.date | None = None, end: dt.date | None = None, days: int | None = None
) -> Iterator[dt.date]:
    """Yield the days in a range."""
    if (start is not None) and (end is not None) and (days is None):
        check_date_not_datetime(start)
        check_date_not_datetime(end)
        date = start
        while date <= end:
            yield date
            date += dt.timedelta(days=1)
        return
    if (start is not None) and (end is None) and (days is not None):
        check_date_not_datetime(start)
        date = start
        for _ in range(days):
            yield date
            date += dt.timedelta(days=1)
        return
    if (start is None) and (end is not None) and (days is not None):
        check_date_not_datetime(end)
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


##


def yield_weekdays(
    *, start: dt.date | None = None, end: dt.date | None = None, days: int | None = None
) -> Iterator[dt.date]:
    """Yield the weekdays in a range."""
    if (start is not None) and (end is not None) and (days is None):
        check_date_not_datetime(start)
        check_date_not_datetime(end)
        date = round_to_next_weekday(start)
        while date <= end:
            yield date
            date = round_to_next_weekday(date + dt.timedelta(days=1))
        return
    if (start is not None) and (end is None) and (days is not None):
        check_date_not_datetime(start)
        date = round_to_next_weekday(start)
        for _ in range(days):
            yield date
            date = round_to_next_weekday(date + dt.timedelta(days=1))
        return
    if (start is None) and (end is not None) and (days is not None):
        check_date_not_datetime(end)
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
    "EPOCH_DATE",
    "EPOCH_NAIVE",
    "EPOCH_UTC",
    "HALF_YEAR",
    "HOUR",
    "MAX_MONTH",
    "MILLISECOND",
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
    "ZERO_TIME",
    "AddWeekdaysError",
    "CheckDateNotDatetimeError",
    "CheckZonedDatetimeError",
    "DateOrMonth",
    "EnsureMonthError",
    "MillisecondsSinceEpochError",
    "Month",
    "MonthError",
    "ParseMonthError",
    "TimedeltaToMillisecondsError",
    "YieldDaysError",
    "YieldWeekdaysError",
    "add_weekdays",
    "check_date_not_datetime",
    "check_zoned_datetime",
    "date_add_timedelta",
    "date_to_datetime",
    "date_to_month",
    "days_since_epoch",
    "days_since_epoch_to_date",
    "drop_microseconds",
    "drop_milli_and_microseconds",
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
    "is_equal_as_months",
    "is_instance_date_not_datetime",
    "is_local_datetime",
    "is_subclass_date_not_datetime",
    "is_weekday",
    "is_zoned_datetime",
    "maybe_sub_pct_y",
    "microseconds_since_epoch",
    "microseconds_since_epoch_to_datetime",
    "microseconds_to_timedelta",
    "milliseconds_since_epoch",
    "milliseconds_since_epoch_to_datetime",
    "milliseconds_to_timedelta",
    "parse_month",
    "round_to_next_weekday",
    "round_to_prev_weekday",
    "serialize_month",
    "timedelta_since_epoch",
    "timedelta_to_microseconds",
    "timedelta_to_milliseconds",
    "yield_days",
    "yield_weekdays",
]
