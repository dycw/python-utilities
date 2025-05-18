from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from re import search, sub
from statistics import fmean
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Self,
    SupportsFloat,
    TypeGuard,
    assert_never,
    overload,
    override,
)

from utilities.iterables import OneEmptyError, one
from utilities.math import SafeRoundError, round_, safe_round
from utilities.platform import SYSTEM
from utilities.sentinel import Sentinel, sentinel
from utilities.types import MaybeStr
from utilities.typing import is_instance_gen
from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import (
        DateOrDateTime,
        Duration,
        MaybeCallableDate,
        MaybeCallableDateTime,
        RoundMode,
        TimeZoneLike,
    )


_DAYS_PER_YEAR = 365.25
_MICROSECONDS_PER_MILLISECOND = int(1e3)
_MICROSECONDS_PER_SECOND = int(1e6)
_SECONDS_PER_DAY = 24 * 60 * 60
_MICROSECONDS_PER_DAY = _MICROSECONDS_PER_SECOND * _SECONDS_PER_DAY
DATETIME_MIN_UTC = dt.datetime.min.replace(tzinfo=UTC)
DATETIME_MAX_UTC = dt.datetime.max.replace(tzinfo=UTC)
DATETIME_MIN_NAIVE = DATETIME_MIN_UTC.replace(tzinfo=None)
DATETIME_MAX_NAIVE = DATETIME_MAX_UTC.replace(tzinfo=None)
EPOCH_UTC = dt.datetime.fromtimestamp(0, tz=UTC)
EPOCH_DATE = EPOCH_UTC.date()
EPOCH_NAIVE = EPOCH_UTC.replace(tzinfo=None)
ZERO_TIME = dt.timedelta(0)
MICROSECOND = dt.timedelta(microseconds=1)
MILLISECOND = dt.timedelta(milliseconds=1)
SECOND = dt.timedelta(seconds=1)
MINUTE = dt.timedelta(minutes=1)
HOUR = dt.timedelta(hours=1)
DAY = dt.timedelta(days=1)
WEEK = dt.timedelta(weeks=1)


##


@overload
def add_duration(
    date: dt.datetime, /, *, duration: Duration | None = ...
) -> dt.datetime: ...
@overload
def add_duration(date: dt.date, /, *, duration: Duration | None = ...) -> dt.date: ...
def add_duration(
    date: DateOrDateTime, /, *, duration: Duration | None = None
) -> dt.date:
    """Add a duration to a date/datetime."""
    if duration is None:
        return date
    if isinstance(date, dt.datetime):
        return date + datetime_duration_to_timedelta(duration)
    try:
        timedelta = date_duration_to_timedelta(duration)
    except DateDurationToTimeDeltaError:
        raise AddDurationError(date=date, duration=duration) from None
    return date + timedelta


@dataclass(kw_only=True, slots=True)
class AddDurationError(Exception):
    date: dt.date
    duration: Duration

    @override
    def __str__(self) -> str:
        return f"Date {self.date} must be paired with an integral duration; got {self.duration}"


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


def are_equal_date_durations(x: Duration, y: Duration, /) -> bool:
    """Check if x == y for durations."""
    x_timedelta = date_duration_to_timedelta(x)
    y_timedelta = date_duration_to_timedelta(y)
    return x_timedelta == y_timedelta


##


def are_equal_dates_or_datetimes(
    x: DateOrDateTime, y: DateOrDateTime, /, *, strict: bool = False
) -> bool:
    """Check if x == y for dates/datetimes."""
    if is_instance_gen(x, dt.date) and is_instance_gen(y, dt.date):
        return x == y
    if is_instance_gen(x, dt.datetime) and is_instance_gen(y, dt.datetime):
        return are_equal_datetimes(x, y, strict=strict)
    raise AreEqualDatesOrDateTimesError(x=x, y=y)


@dataclass(kw_only=True, slots=True)
class AreEqualDatesOrDateTimesError(Exception):
    x: DateOrDateTime
    y: DateOrDateTime

    @override
    def __str__(self) -> str:
        return f"Cannot compare date and datetime ({self.x}, {self.y})"


##


def are_equal_datetime_durations(x: Duration, y: Duration, /) -> bool:
    """Check if x == y for durations."""
    x_timedelta = datetime_duration_to_timedelta(x)
    y_timedelta = datetime_duration_to_timedelta(y)
    return x_timedelta == y_timedelta


##


def are_equal_datetimes(
    x: dt.datetime, y: dt.datetime, /, *, strict: bool = False
) -> bool:
    """Check if x == y for datetimes."""
    match x.tzinfo is None, y.tzinfo is None:
        case True, True:
            return x == y
        case False, False if x == y:
            return (x.tzinfo is y.tzinfo) or not strict
        case False, False if x != y:
            return False
        case _:
            raise AreEqualDateTimesError(x=x, y=y)


@dataclass(kw_only=True, slots=True)
class AreEqualDateTimesError(Exception):
    x: dt.datetime
    y: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Cannot compare local and zoned datetimes ({self.x}, {self.y})"


##


def are_equal_months(x: DateOrMonth, y: DateOrMonth, /) -> bool:
    """Check if x == y as months."""
    x_month = Month.from_date(x) if isinstance(x, dt.date) else x
    y_month = Month.from_date(y) if isinstance(y, dt.date) else y
    return x_month == y_month


##


def check_date_not_datetime(date: dt.date, /) -> None:
    """Check if a date is not a datetime."""
    if not is_instance_gen(date, dt.date):
        raise CheckDateNotDateTimeError(date=date)


@dataclass(kw_only=True, slots=True)
class CheckDateNotDateTimeError(Exception):
    date: dt.date

    @override
    def __str__(self) -> str:
        return f"Date must not be a datetime; got {self.date}"


##


def date_to_datetime(
    date: dt.date, /, *, time: dt.time | None = None, time_zone: TimeZoneLike = UTC
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


def date_duration_to_int(duration: Duration, /) -> int:
    """Ensure a date duration is a float."""
    match duration:
        case int():
            return duration
        case float():
            try:
                return safe_round(duration)
            except SafeRoundError:
                raise _DateDurationToIntFloatError(duration=duration) from None
        case dt.timedelta():
            if is_integral_timedelta(duration):
                return duration.days
            raise _DateDurationToIntTimeDeltaError(duration=duration) from None
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class DateDurationToIntError(Exception):
    duration: Duration


@dataclass(kw_only=True, slots=True)
class _DateDurationToIntFloatError(DateDurationToIntError):
    @override
    def __str__(self) -> str:
        return f"Float duration must be integral; got {self.duration}"


@dataclass(kw_only=True, slots=True)
class _DateDurationToIntTimeDeltaError(DateDurationToIntError):
    @override
    def __str__(self) -> str:
        return f"Timedelta duration must be integral; got {self.duration}"


def date_duration_to_timedelta(duration: Duration, /) -> dt.timedelta:
    """Ensure a date duration is a timedelta."""
    match duration:
        case int():
            return dt.timedelta(days=duration)
        case float():
            try:
                as_int = safe_round(duration)
            except SafeRoundError:
                raise _DateDurationToTimeDeltaFloatError(duration=duration) from None
            return dt.timedelta(days=as_int)
        case dt.timedelta():
            if is_integral_timedelta(duration):
                return duration
            raise _DateDurationToTimeDeltaTimeDeltaError(duration=duration) from None
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class DateDurationToTimeDeltaError(Exception):
    duration: Duration


@dataclass(kw_only=True, slots=True)
class _DateDurationToTimeDeltaFloatError(DateDurationToTimeDeltaError):
    @override
    def __str__(self) -> str:
        return f"Float duration must be integral; got {self.duration}"


@dataclass(kw_only=True, slots=True)
class _DateDurationToTimeDeltaTimeDeltaError(DateDurationToTimeDeltaError):
    @override
    def __str__(self) -> str:
        return f"Timedelta duration must be integral; got {self.duration}"


##


def datetime_duration_to_float(duration: Duration, /) -> float:
    """Ensure a datetime duration is a float."""
    match duration:
        case int():
            return float(duration)
        case float():
            return duration
        case dt.timedelta():
            return duration.total_seconds()
        case _ as never:
            assert_never(never)


def datetime_duration_to_microseconds(duration: Duration, /) -> int:
    """Compute the number of microseconds in a datetime duration."""
    timedelta = datetime_duration_to_timedelta(duration)
    return (
        _MICROSECONDS_PER_DAY * timedelta.days
        + _MICROSECONDS_PER_SECOND * timedelta.seconds
        + timedelta.microseconds
    )


@overload
def datetime_duration_to_milliseconds(
    duration: Duration, /, *, strict: Literal[True]
) -> int: ...
@overload
def datetime_duration_to_milliseconds(
    duration: Duration, /, *, strict: bool = False
) -> float: ...
def datetime_duration_to_milliseconds(
    duration: Duration, /, *, strict: bool = False
) -> int | float:
    """Compute the number of milliseconds in a datetime duration."""
    timedelta = datetime_duration_to_timedelta(duration)
    microseconds = datetime_duration_to_microseconds(timedelta)
    milliseconds, remainder = divmod(microseconds, _MICROSECONDS_PER_MILLISECOND)
    match remainder, strict:
        case 0, _:
            return milliseconds
        case _, True:
            raise TimedeltaToMillisecondsError(duration=duration, remainder=remainder)
        case _, False:
            return milliseconds + remainder / _MICROSECONDS_PER_MILLISECOND
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class TimedeltaToMillisecondsError(Exception):
    duration: Duration
    remainder: int

    @override
    def __str__(self) -> str:
        return f"Unable to convert {self.duration} to milliseconds; got {self.remainder} microsecond(s)"


def datetime_duration_to_timedelta(duration: Duration, /) -> dt.timedelta:
    """Ensure a datetime duration is a timedelta."""
    match duration:
        case int() | float():
            return dt.timedelta(seconds=duration)
        case dt.timedelta():
            return duration
        case _ as never:
            assert_never(never)


##


def datetime_utc(
    year: int,
    month: int,
    day: int,
    /,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> dt.datetime:
    """Create a UTC-zoned datetime."""
    return dt.datetime(
        year,
        month,
        day,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
        tzinfo=UTC,
    )


##


def days_since_epoch(date: dt.date, /) -> int:
    """Compute the number of days since the epoch."""
    check_date_not_datetime(date)
    return timedelta_since_epoch(date).days


def days_since_epoch_to_date(days: int, /) -> dt.date:
    """Convert a number of days since the epoch to a date."""
    return EPOCH_DATE + days * DAY


##


def ensure_month(month: MonthLike, /) -> Month:
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
    """Format a plain datetime locally & in UTC."""
    time_zone = ensure_time_zone(datetime)
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


@overload
def get_date(*, date: MaybeCallableDate) -> dt.date: ...
@overload
def get_date(*, date: None) -> None: ...
@overload
def get_date(*, date: Sentinel) -> Sentinel: ...
@overload
def get_date(*, date: MaybeCallableDate | Sentinel) -> dt.date | Sentinel: ...
@overload
def get_date(
    *, date: MaybeCallableDate | None | Sentinel = sentinel
) -> dt.date | None | Sentinel: ...
def get_date(
    *, date: MaybeCallableDate | None | Sentinel = sentinel
) -> dt.date | None | Sentinel:
    """Get the date."""
    match date:
        case dt.date() | None | Sentinel():
            return date
        case Callable() as func:
            return get_date(date=func())
        case _ as never:
            assert_never(never)


##


@overload
def get_datetime(*, datetime: MaybeCallableDateTime) -> dt.datetime: ...
@overload
def get_datetime(*, datetime: None) -> None: ...
@overload
def get_datetime(*, datetime: Sentinel) -> Sentinel: ...
@overload
def get_datetime(
    *, datetime: MaybeCallableDateTime | Sentinel
) -> dt.datetime | Sentinel: ...
@overload
def get_datetime(
    *, datetime: MaybeCallableDateTime | None | Sentinel = sentinel
) -> dt.datetime | None | Sentinel: ...
def get_datetime(
    *, datetime: MaybeCallableDateTime | None | Sentinel = sentinel
) -> dt.datetime | None | Sentinel:
    """Get the datetime."""
    match datetime:
        case dt.datetime() | None | Sentinel():
            return datetime
        case Callable() as func:
            return get_datetime(datetime=func())
        case _ as never:
            assert_never(never)


##


def get_half_years(*, n: int = 1) -> dt.timedelta:
    """Get a number of half-years as a timedelta."""
    days_per_half_year = _DAYS_PER_YEAR / 2
    return dt.timedelta(days=round(n * days_per_half_year))


HALF_YEAR = get_half_years(n=1)

##


def get_min_max_date(
    *,
    min_date: dt.date | None = None,
    max_date: dt.date | None = None,
    min_age: Duration | None = None,
    max_age: Duration | None = None,
    time_zone: TimeZoneLike = UTC,
) -> tuple[dt.date | None, dt.date | None]:
    """Get the min/max date given a combination of dates/ages."""
    today = get_today(time_zone=time_zone)
    min_parts: Sequence[dt.date] = []
    if min_date is not None:
        if min_date > today:
            raise _GetMinMaxDateMinDateError(min_date=min_date, today=today)
        min_parts.append(min_date)
    if max_age is not None:
        if date_duration_to_timedelta(max_age) < ZERO_TIME:
            raise _GetMinMaxDateMaxAgeError(max_age=max_age)
        min_parts.append(sub_duration(today, duration=max_age))
    min_date_use = max(min_parts, default=None)
    max_parts: Sequence[dt.date] = []
    if max_date is not None:
        if max_date > today:
            raise _GetMinMaxDateMaxDateError(max_date=max_date, today=today)
        max_parts.append(max_date)
    if min_age is not None:
        if date_duration_to_timedelta(min_age) < ZERO_TIME:
            raise _GetMinMaxDateMinAgeError(min_age=min_age)
        max_parts.append(sub_duration(today, duration=min_age))
    max_date_use = min(max_parts, default=None)
    if (
        (min_date_use is not None)
        and (max_date_use is not None)
        and (min_date_use > max_date_use)
    ):
        raise _GetMinMaxDatePeriodError(min_date=min_date_use, max_date=max_date_use)
    return min_date_use, max_date_use


@dataclass(kw_only=True, slots=True)
class GetMinMaxDateError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _GetMinMaxDateMinDateError(GetMinMaxDateError):
    min_date: dt.date
    today: dt.date

    @override
    def __str__(self) -> str:
        return f"Min date must be at most today; got {self.min_date} > {self.today}"


@dataclass(kw_only=True, slots=True)
class _GetMinMaxDateMinAgeError(GetMinMaxDateError):
    min_age: Duration

    @override
    def __str__(self) -> str:
        return f"Min age must be non-negative; got {self.min_age}"


@dataclass(kw_only=True, slots=True)
class _GetMinMaxDateMaxDateError(GetMinMaxDateError):
    max_date: dt.date
    today: dt.date

    @override
    def __str__(self) -> str:
        return f"Max date must be at most today; got {self.max_date} > {self.today}"


@dataclass(kw_only=True, slots=True)
class _GetMinMaxDateMaxAgeError(GetMinMaxDateError):
    max_age: Duration

    @override
    def __str__(self) -> str:
        return f"Max age must be non-negative; got {self.max_age}"


@dataclass(kw_only=True, slots=True)
class _GetMinMaxDatePeriodError(GetMinMaxDateError):
    min_date: dt.date
    max_date: dt.date

    @override
    def __str__(self) -> str:
        return (
            f"Min date must be at most max date; got {self.min_date} > {self.max_date}"
        )


##


def get_months(*, n: int = 1) -> dt.timedelta:
    """Get a number of months as a timedelta."""
    days_per_month = _DAYS_PER_YEAR / 12
    return dt.timedelta(days=round(n * days_per_month))


MONTH = get_months(n=1)


##


def get_now(*, time_zone: TimeZoneLike = UTC) -> dt.datetime:
    """Get the current, timezone-aware time."""
    return dt.datetime.now(tz=ensure_time_zone(time_zone))


NOW_UTC = get_now(time_zone=UTC)


##


def get_quarters(*, n: int = 1) -> dt.timedelta:
    """Get a number of quarters as a timedelta."""
    days_per_quarter = _DAYS_PER_YEAR / 4
    return dt.timedelta(days=round(n * days_per_quarter))


QUARTER = get_quarters(n=1)


##


def get_today(*, time_zone: TimeZoneLike = UTC) -> dt.date:
    """Get the current, timezone-aware date."""
    return get_now(time_zone=time_zone).date()


TODAY_UTC = get_today(time_zone=UTC)


##


def get_years(*, n: int = 1) -> dt.timedelta:
    """Get a number of years as a timedelta."""
    return dt.timedelta(days=round(n * _DAYS_PER_YEAR))


YEAR = get_years(n=1)


##


def is_integral_timedelta(duration: Duration, /) -> bool:
    """Check if a duration is integral."""
    timedelta = datetime_duration_to_timedelta(duration)
    return (timedelta.seconds == 0) and (timedelta.microseconds == 0)


##


def is_plain_datetime(obj: Any, /) -> TypeGuard[dt.datetime]:
    """Check if an object is a plain datetime."""
    return isinstance(obj, dt.datetime) and (obj.tzinfo is None)


##


_FRIDAY = 5


def is_weekday(date: dt.date, /) -> bool:
    """Check if a date is a weekday."""
    check_date_not_datetime(date)
    return date.isoweekday() <= _FRIDAY


##


def is_zero_time(duration: Duration, /) -> bool:
    """Check if a timedelta is 0."""
    return datetime_duration_to_timedelta(duration) == ZERO_TIME


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
        case _ as never:
            assert_never(never)


##


def mean_datetime(
    datetimes: Iterable[dt.datetime],
    /,
    *,
    weights: Iterable[SupportsFloat] | None = None,
    mode: RoundMode = "standard",
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> dt.datetime:
    """Compute the mean of a set of datetimes."""
    datetimes = list(datetimes)
    match len(datetimes):
        case 0:
            raise MeanDateTimeError from None
        case 1:
            return one(datetimes)
        case _:
            microseconds = list(map(microseconds_since_epoch, datetimes))
            mean_float = fmean(microseconds, weights=weights)
            mean_int = round_(mean_float, mode=mode, rel_tol=rel_tol, abs_tol=abs_tol)
            return microseconds_since_epoch_to_datetime(
                mean_int, time_zone=datetimes[0].tzinfo
            )


@dataclass(kw_only=True, slots=True)
class MeanDateTimeError(Exception):
    @override
    def __str__(self) -> str:
        return "Mean requires at least 1 datetime"


##


def mean_timedelta(
    timedeltas: Iterable[dt.timedelta],
    /,
    *,
    weights: Iterable[SupportsFloat] | None = None,
    mode: RoundMode = "standard",
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> dt.timedelta:
    """Compute the mean of a set of timedeltas."""
    timedeltas = list(timedeltas)
    match len(timedeltas):
        case 0:
            raise MeanTimeDeltaError from None
        case 1:
            return one(timedeltas)
        case _:
            microseconds = list(map(datetime_duration_to_microseconds, timedeltas))
            mean_float = fmean(microseconds, weights=weights)
            mean_int = round_(mean_float, mode=mode, rel_tol=rel_tol, abs_tol=abs_tol)
            return microseconds_to_timedelta(mean_int)


@dataclass(kw_only=True, slots=True)
class MeanTimeDeltaError(Exception):
    @override
    def __str__(self) -> str:
        return "Mean requires at least 1 timedelta"


##


def microseconds_since_epoch(datetime: dt.datetime, /) -> int:
    """Compute the number of microseconds since the epoch."""
    return datetime_duration_to_microseconds(timedelta_since_epoch(datetime))


def microseconds_to_timedelta(microseconds: int, /) -> dt.timedelta:
    """Compute a timedelta given a number of microseconds."""
    if microseconds == 0:
        return ZERO_TIME
    if microseconds >= 1:
        days, remainder = divmod(microseconds, _MICROSECONDS_PER_DAY)
        seconds, micros = divmod(remainder, _MICROSECONDS_PER_SECOND)
        return dt.timedelta(days=days, seconds=seconds, microseconds=micros)
    return -microseconds_to_timedelta(-microseconds)


def microseconds_since_epoch_to_datetime(
    microseconds: int, /, *, time_zone: dt.tzinfo | None = None
) -> dt.datetime:
    """Convert a number of microseconds since the epoch to a datetime."""
    epoch = EPOCH_NAIVE if time_zone is None else EPOCH_UTC
    timedelta = microseconds_to_timedelta(microseconds)
    return epoch + timedelta


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


def milliseconds_since_epoch_to_datetime(
    milliseconds: int, /, *, time_zone: dt.tzinfo | None = None
) -> dt.datetime:
    """Convert a number of milliseconds since the epoch to a datetime."""
    epoch = EPOCH_NAIVE if time_zone is None else EPOCH_UTC
    timedelta = milliseconds_to_timedelta(milliseconds)
    return epoch + timedelta


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


type DateOrMonth = dt.date | Month
type MonthLike = MaybeStr[Month]
MIN_MONTH = Month(dt.date.min.year, dt.date.min.month)
MAX_MONTH = Month(dt.date.max.year, dt.date.max.month)


##


_TWO_DIGIT_YEAR_MIN = 1969
_TWO_DIGIT_YEAR_MAX = _TWO_DIGIT_YEAR_MIN + 99
MIN_DATE_TWO_DIGIT_YEAR = dt.date(
    _TWO_DIGIT_YEAR_MIN, dt.date.min.month, dt.date.min.day
)
MAX_DATE_TWO_DIGIT_YEAR = dt.date(
    _TWO_DIGIT_YEAR_MAX, dt.date.max.month, dt.date.max.day
)


def parse_two_digit_year(year: int | str, /) -> int:
    """Parse a 2-digit year into a year."""
    match year:
        case int():
            years = range(_TWO_DIGIT_YEAR_MIN, _TWO_DIGIT_YEAR_MAX + 1)
            try:
                return one(y for y in years if y % 100 == year)
            except OneEmptyError:
                raise _ParseTwoDigitYearInvalidIntegerError(year=year) from None
        case str():
            if search(r"^\d{1,2}$", year):
                return parse_two_digit_year(int(year))
            raise _ParseTwoDigitYearInvalidStringError(year=year)
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ParseTwoDigitYearError(Exception):
    year: int | str


@dataclass(kw_only=True, slots=True)
class _ParseTwoDigitYearInvalidIntegerError(Exception):
    year: int | str

    @override
    def __str__(self) -> str:
        return f"Unable to parse year; got {self.year!r}"


@dataclass(kw_only=True, slots=True)
class _ParseTwoDigitYearInvalidStringError(Exception):
    year: int | str

    @override
    def __str__(self) -> str:
        return f"Unable to parse year; got {self.year!r}"


##


def round_datetime(
    datetime: dt.datetime,
    duration: Duration,
    /,
    *,
    mode: RoundMode = "standard",
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> dt.datetime:
    """Round a datetime to a timedelta."""
    if datetime.tzinfo is None:
        dividend = microseconds_since_epoch(datetime)
        divisor = datetime_duration_to_microseconds(duration)
        quotient, remainder = divmod(dividend, divisor)
        rnd_remainder = round_(
            remainder / divisor, mode=mode, rel_tol=rel_tol, abs_tol=abs_tol
        )
        rnd_quotient = quotient + rnd_remainder
        microseconds = rnd_quotient * divisor
        return microseconds_since_epoch_to_datetime(microseconds)
    local = datetime.replace(tzinfo=None)
    rounded = round_datetime(
        local, duration, mode=mode, rel_tol=rel_tol, abs_tol=abs_tol
    )
    return rounded.replace(tzinfo=datetime.tzinfo)


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
        case _ as never:
            assert_never(never)
    while not is_weekday(date):
        date = add_weekdays(date, n=n)
    return date


##


def serialize_compact(date_or_datetime: DateOrDateTime, /) -> str:
    """Serialize a date/datetime using a compact format."""
    match date_or_datetime:
        case dt.datetime() as datetime:
            if datetime.tzinfo is None:
                raise SerializeCompactError(datetime=datetime)
            format_ = "%Y%m%dT%H%M%S"
        case dt.date():
            format_ = "%Y%m%d"
        case _ as never:
            assert_never(never)
    return date_or_datetime.strftime(maybe_sub_pct_y(format_))


@dataclass(kw_only=True, slots=True)
class SerializeCompactError(Exception):
    datetime: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Unable to serialize plain datetime {self.datetime}"


def parse_date_compact(text: str, /) -> dt.date:
    """Parse a compact string into a date."""
    try:
        datetime = dt.datetime.strptime(text, "%Y%m%d").replace(tzinfo=UTC)
    except ValueError:
        raise ParseDateCompactError(text=text) from None
    return datetime.date()


@dataclass(kw_only=True, slots=True)
class ParseDateCompactError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.text!r} into a date"


def parse_datetime_compact(
    text: str, /, *, time_zone: TimeZoneLike = UTC
) -> dt.datetime:
    """Parse a compact string into a datetime."""
    time_zone = ensure_time_zone(time_zone)
    try:
        return dt.datetime.strptime(text, "%Y%m%dT%H%M%S").replace(tzinfo=time_zone)
    except ValueError:
        raise ParseDateTimeCompactError(text=text) from None


@dataclass(kw_only=True, slots=True)
class ParseDateTimeCompactError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.text!r} into a datetime"


##


def serialize_month(month: Month, /) -> str:
    """Serialize a month."""
    return f"{month.year:04}-{month.month:02}"


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


@overload
def sub_duration(
    date: dt.datetime, /, *, duration: Duration | None = ...
) -> dt.datetime: ...
@overload
def sub_duration(date: dt.date, /, *, duration: Duration | None = ...) -> dt.date: ...
def sub_duration(
    date: DateOrDateTime, /, *, duration: Duration | None = None
) -> dt.date:
    """Subtract a duration from a date/datetime."""
    if duration is None:
        return date
    try:
        return add_duration(date, duration=-duration)
    except AddDurationError:
        raise SubDurationError(date=date, duration=duration) from None


@dataclass(kw_only=True, slots=True)
class SubDurationError(Exception):
    date: dt.date
    duration: Duration

    @override
    def __str__(self) -> str:
        return f"Date {self.date} must be paired with an integral duration; got {self.duration}"


##


def timedelta_since_epoch(date_or_datetime: DateOrDateTime, /) -> dt.timedelta:
    """Compute the timedelta since the epoch."""
    match date_or_datetime:
        case dt.datetime() as datetime:
            if datetime.tzinfo is None:
                return datetime - EPOCH_NAIVE
            return datetime.astimezone(UTC) - EPOCH_UTC
        case dt.date() as date:
            return date - EPOCH_DATE
        case _ as never:
            assert_never(never)


##


def yield_days(
    *, start: dt.date | None = None, end: dt.date | None = None, days: int | None = None
) -> Iterator[dt.date]:
    """Yield the days in a range."""
    match start, end, days:
        case dt.date(), dt.date(), None:
            check_date_not_datetime(start)
            check_date_not_datetime(end)
            date = start
            while date <= end:
                yield date
                date += DAY
        case dt.date(), None, int():
            check_date_not_datetime(start)
            date = start
            for _ in range(days):
                yield date
                date += DAY
        case None, dt.date(), int():
            check_date_not_datetime(end)
            date = end
            for _ in range(days):
                yield date
                date -= DAY
        case _:
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
    match start, end, days:
        case dt.date(), dt.date(), None:
            check_date_not_datetime(start)
            check_date_not_datetime(end)
            date = round_to_next_weekday(start)
            while date <= end:
                yield date
                date = round_to_next_weekday(date + DAY)
        case dt.date(), None, int():
            check_date_not_datetime(start)
            date = round_to_next_weekday(start)
            for _ in range(days):
                yield date
                date = round_to_next_weekday(date + DAY)
        case None, dt.date(), int():
            check_date_not_datetime(end)
            date = round_to_prev_weekday(end)
            for _ in range(days):
                yield date
                date = round_to_prev_weekday(date - DAY)
        case _:
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
    "DATETIME_MAX_NAIVE",
    "DATETIME_MAX_UTC",
    "DATETIME_MIN_NAIVE",
    "DATETIME_MIN_UTC",
    "DAY",
    "EPOCH_DATE",
    "EPOCH_NAIVE",
    "EPOCH_UTC",
    "HALF_YEAR",
    "HOUR",
    "MAX_DATE_TWO_DIGIT_YEAR",
    "MAX_MONTH",
    "MILLISECOND",
    "MINUTE",
    "MIN_DATE_TWO_DIGIT_YEAR",
    "MIN_MONTH",
    "MONTH",
    "NOW_UTC",
    "QUARTER",
    "SECOND",
    "TODAY_UTC",
    "WEEK",
    "YEAR",
    "ZERO_TIME",
    "AddDurationError",
    "AddWeekdaysError",
    "AreEqualDateTimesError",
    "AreEqualDatesOrDateTimesError",
    "CheckDateNotDateTimeError",
    "DateOrMonth",
    "EnsureMonthError",
    "GetMinMaxDateError",
    "MeanDateTimeError",
    "MeanTimeDeltaError",
    "MillisecondsSinceEpochError",
    "Month",
    "MonthError",
    "MonthLike",
    "ParseDateCompactError",
    "ParseDateTimeCompactError",
    "ParseMonthError",
    "SerializeCompactError",
    "SubDurationError",
    "TimedeltaToMillisecondsError",
    "YieldDaysError",
    "YieldWeekdaysError",
    "add_duration",
    "add_weekdays",
    "are_equal_date_durations",
    "are_equal_dates_or_datetimes",
    "are_equal_datetime_durations",
    "are_equal_datetimes",
    "are_equal_months",
    "check_date_not_datetime",
    "date_duration_to_int",
    "date_duration_to_timedelta",
    "date_to_datetime",
    "date_to_month",
    "datetime_duration_to_float",
    "datetime_duration_to_microseconds",
    "datetime_duration_to_milliseconds",
    "datetime_duration_to_timedelta",
    "datetime_utc",
    "days_since_epoch",
    "days_since_epoch_to_date",
    "ensure_month",
    "format_datetime_local_and_utc",
    "get_date",
    "get_datetime",
    "get_half_years",
    "get_min_max_date",
    "get_months",
    "get_now",
    "get_quarters",
    "get_today",
    "get_years",
    "is_integral_timedelta",
    "is_plain_datetime",
    "is_weekday",
    "is_zero_time",
    "is_zoned_datetime",
    "maybe_sub_pct_y",
    "mean_datetime",
    "mean_timedelta",
    "microseconds_since_epoch",
    "microseconds_since_epoch_to_datetime",
    "microseconds_to_timedelta",
    "milliseconds_since_epoch",
    "milliseconds_since_epoch_to_datetime",
    "milliseconds_to_timedelta",
    "parse_date_compact",
    "parse_datetime_compact",
    "parse_month",
    "parse_two_digit_year",
    "round_datetime",
    "round_to_next_weekday",
    "round_to_prev_weekday",
    "serialize_compact",
    "serialize_month",
    "sub_duration",
    "timedelta_since_epoch",
    "yield_days",
    "yield_weekdays",
]
