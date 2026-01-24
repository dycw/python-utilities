from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
from functools import cache
from logging import LogRecord
from statistics import fmean
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Self,
    SupportsFloat,
    TypedDict,
    assert_never,
    cast,
    overload,
    override,
)
from zoneinfo import ZoneInfo

from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    Weekday,
    YearMonth,
    ZonedDateTime,
)

from utilities.constants import (
    HOURS_PER_DAY,
    LOCAL_TIME_ZONE_NAME,
    MICROSECONDS_PER_MILLISECOND,
    MILLISECONDS_PER_SECOND,
    MINUTES_PER_HOUR,
    NANOSECONDS_PER_MICROSECOND,
    SECONDS_PER_MINUTE,
    UTC,
    Sentinel,
    sentinel,
)
from utilities.core import (
    NumDaysError,
    NumHoursError,
    NumMicroSecondsError,
    NumMilliSecondsError,
    NumMinutesError,
    NumNanoSecondsError,
    NumSecondsError,
    NumWeeksError,
    get_now,
    get_now_local,
    get_time,
    get_today,
    num_days,
    num_hours,
    num_microseconds,
    num_milliseconds,
    num_minutes,
    num_nanoseconds,
    num_seconds,
    num_weeks,
    replace_non_sentinel,
    to_time_zone_name,
)
from utilities.functions import get_class_name
from utilities.math import sign
from utilities.platform import get_strftime

if TYPE_CHECKING:
    from utilities.types import (
        DateTimeRoundMode,
        Delta,
        MaybeCallableTimeLike,
        MaybeCallableZonedDateTimeLike,
        TimeZoneLike,
    )


DATE_TIME_DELTA_MIN = DateTimeDelta(
    weeks=-521722,
    days=-5,
    hours=-23,
    minutes=-59,
    seconds=-59,
    milliseconds=-999,
    microseconds=-999,
    nanoseconds=-999,
)
DATE_TIME_DELTA_MAX = DateTimeDelta(
    weeks=521722,
    days=5,
    hours=23,
    minutes=59,
    seconds=59,
    milliseconds=999,
    microseconds=999,
    nanoseconds=999,
)


DATE_TIME_DELTA_PARSABLE_MIN = DateTimeDelta(
    weeks=-142857,
    hours=-23,
    minutes=-59,
    seconds=-59,
    milliseconds=-999,
    microseconds=-999,
    nanoseconds=-999,
)
DATE_TIME_DELTA_PARSABLE_MAX = DateTimeDelta(
    weeks=142857,
    hours=23,
    minutes=59,
    seconds=59,
    milliseconds=999,
    microseconds=999,
    nanoseconds=999,
)
DATE_DELTA_PARSABLE_MIN = DateDelta(days=-999999)
DATE_DELTA_PARSABLE_MAX = DateDelta(days=999999)


DATE_TWO_DIGIT_YEAR_MIN = Date(1969, 1, 1)
DATE_TWO_DIGIT_YEAR_MAX = Date(DATE_TWO_DIGIT_YEAR_MIN.year + 99, 12, 31)


## common constants


def add_year_month(x: YearMonth, /, *, years: int = 0, months: int = 0) -> YearMonth:
    """Add to a year-month."""
    y = x.on_day(1) + DateDelta(years=years, months=months)
    return y.year_month()


def sub_year_month(x: YearMonth, /, *, years: int = 0, months: int = 0) -> YearMonth:
    """Subtract from a year-month."""
    y = x.on_day(1) - DateDelta(years=years, months=months)
    return y.year_month()


##


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=False)
class DatePeriod:
    """A period of dates."""

    start: Date
    end: Date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise DatePeriodError(start=self.start, end=self.end)

    def __add__(self, other: DateDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    def __contains__(self, other: Date, /) -> bool:
        """Check if a date/datetime lies in the period."""
        return self.start <= other <= self.end

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        return f"{cls}({self.start}, {self.end})"

    def __sub__(self, other: DateDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start - other, end=self.end - other)

    def at(
        self, obj: Time | tuple[Time, Time], /, *, time_zone: TimeZoneLike = UTC
    ) -> ZonedDateTimePeriod:
        """Combine a date with a time to create a datetime."""
        match obj:
            case Time() as time:
                start = end = time
            case Time() as start, Time() as end:
                ...
            case never:
                assert_never(never)
        tz = to_time_zone_name(time_zone)
        return ZonedDateTimePeriod(
            self.start.at(start).assume_tz(tz), self.end.at(end).assume_tz(tz)
        )

    @property
    def delta(self) -> DateDelta:
        """The delta of the period."""
        return self.end - self.start

    def format_compact(self) -> str:
        """Format the period in a compact fashion."""
        fc, start, end = format_compact, self.start, self.end
        if self.start == self.end:
            return f"{fc(start)}="
        if self.start.year_month() == self.end.year_month():
            return f"{fc(start)}-{fc(end, fmt='%d')}"
        if self.start.year == self.end.year:
            return f"{fc(start)}-{fc(end, fmt='%m%d')}"
        return f"{fc(start)}-{fc(end)}"

    @classmethod
    def from_dict(cls, mapping: PeriodDict[Date] | PeriodDict[dt.date], /) -> Self:
        """Convert the dictionary to a period."""
        match mapping["start"]:
            case Date() as start:
                ...
            case dt.date() as py_date:
                start = Date.from_py_date(py_date)
            case never:
                assert_never(never)
        match mapping["end"]:
            case Date() as end:
                ...
            case dt.date() as py_date:
                end = Date.from_py_date(py_date)
            case never:
                assert_never(never)
        return cls(start=start, end=end)

    def replace(
        self, *, start: Date | Sentinel = sentinel, end: Date | Sentinel = sentinel
    ) -> Self:
        """Replace elements of the period."""
        return replace_non_sentinel(self, start=start, end=end)

    def to_dict(self) -> PeriodDict[Date]:
        """Convert the period to a dictionary."""
        return PeriodDict(start=self.start, end=self.end)

    def to_py_dict(self) -> PeriodDict[dt.date]:
        """Convert the period to a dictionary."""
        return PeriodDict(start=self.start.py_date(), end=self.end.py_date())


@dataclass(kw_only=True, slots=True)
class DatePeriodError(Exception):
    start: Date
    end: Date

    @override
    def __str__(self) -> str:
        return f"Invalid period; got {self.start} > {self.end}"


##


def datetime_utc(
    year: int,
    month: int,
    day: int,
    /,
    *,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    millisecond: int = 0,
    microsecond: int = 0,
    nanosecond: int = 0,
) -> ZonedDateTime:
    """Create a UTC-zoned datetime."""
    nanos = int(1e6) * millisecond + int(1e3) * microsecond + nanosecond
    return ZonedDateTime(
        year,
        month,
        day,
        hour=hour,
        minute=minute,
        second=second,
        nanosecond=nanos,
        tz=UTC.key,
    )


##


@overload
def diff_year_month(
    x: YearMonth, y: YearMonth, /, *, years: Literal[True]
) -> tuple[int, int]: ...
@overload
def diff_year_month(
    x: YearMonth, y: YearMonth, /, *, years: Literal[False] = False
) -> int: ...
@overload
def diff_year_month(
    x: YearMonth, y: YearMonth, /, *, years: bool = False
) -> int | tuple[int, int]: ...
def diff_year_month(
    x: YearMonth, y: YearMonth, /, *, years: bool = False
) -> int | tuple[int, int]:
    """Compute the difference between two year-months."""
    x_date, y_date = x.on_day(1), y.on_day(1)
    diff = x_date - y_date
    if years:
        yrs, mth, _ = diff.in_years_months_days()
        return yrs, mth
    mth, _ = diff.in_months_days()
    return mth


##


def format_compact(
    obj: Date | Time | PlainDateTime | ZonedDateTime,
    /,
    *,
    fmt: str | None = None,
    path: bool = False,
) -> str:
    """Format the date/datetime in a compact fashion."""
    match obj:
        case Date() as date:
            obj_use = date.py_date()
            fmt_use = "%Y%m%d" if fmt is None else fmt
        case Time() as time:
            obj_use = time.round().py_time()
            fmt_use = "%H%M%S" if fmt is None else fmt
        case PlainDateTime() as date_time:
            obj_use = date_time.round().py_datetime()
            fmt_use = "%Y%m%dT%H%M%S" if fmt is None else fmt
        case ZonedDateTime() as date_time:
            plain = format_compact(date_time.to_plain(), fmt=fmt)
            tz = date_time.tz
            if path:
                tz = tz.replace("/", "~")
            return f"{plain}[{tz}]"
        case never:
            assert_never(never)
    return obj_use.strftime(get_strftime(fmt_use))


##


def from_timestamp(i: float, /, *, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get a zoned datetime from a timestamp."""
    return ZonedDateTime.from_timestamp(i, tz=to_time_zone_name(time_zone))


def from_timestamp_millis(i: int, /, *, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get a zoned datetime from a timestamp (in milliseconds)."""
    return ZonedDateTime.from_timestamp_millis(i, tz=to_time_zone_name(time_zone))


def from_timestamp_nanos(i: int, /, *, time_zone: TimeZoneLike = UTC) -> ZonedDateTime:
    """Get a zoned datetime from a timestamp (in nanoseconds)."""
    return ZonedDateTime.from_timestamp_nanos(i, tz=to_time_zone_name(time_zone))


##


def is_weekend(
    date_time: ZonedDateTime,
    /,
    *,
    start: tuple[Weekday, Time] = (Weekday.SATURDAY, Time.MIN),
    end: tuple[Weekday, Time] = (Weekday.SUNDAY, Time.MAX),
) -> bool:
    """Check if a datetime is in the weekend."""
    weekday, time = date_time.date().day_of_week(), date_time.time()
    start_weekday, start_time = start
    end_weekday, end_time = end
    if start_weekday.value == end_weekday.value:
        return start_time <= time <= end_time
    if start_weekday.value < end_weekday.value:
        return (
            ((weekday == start_weekday) and (time >= start_time))
            or (start_weekday.value < weekday.value < end_weekday.value)
            or ((weekday == end_weekday) and (time <= end_time))
        )
    return (
        ((weekday == start_weekday) and (time >= start_time))
        or (weekday.value > start_weekday.value)
        or (weekday.value < end_weekday.value)
        or ((weekday == end_weekday) and (time <= end_time))
    )


##


def mean_datetime(
    datetimes: Iterable[ZonedDateTime],
    /,
    *,
    weights: Iterable[SupportsFloat] | None = None,
) -> ZonedDateTime:
    """Compute the mean of a set of datetimes."""
    datetimes = list(datetimes)
    match len(datetimes):
        case 0:
            raise MeanDateTimeError from None
        case 1:
            return datetimes[0]
        case _:
            timestamps = [d.timestamp_nanos() for d in datetimes]
            timestamp = round(fmean(timestamps, weights=weights))
            return ZonedDateTime.from_timestamp_nanos(timestamp, tz=datetimes[0].tz)


@dataclass(kw_only=True, slots=True)
class MeanDateTimeError(Exception):
    @override
    def __str__(self) -> str:
        return "Mean requires at least 1 datetime"


##


def min_max_date(
    *,
    min_date: Date | None = None,
    max_date: Date | None = None,
    min_age: DateDelta | None = None,
    max_age: DateDelta | None = None,
    time_zone: TimeZoneLike = UTC,
) -> tuple[Date | None, Date | None]:
    """Compute the min/max date given a combination of dates/ages."""
    today = get_today(time_zone)
    min_parts: list[Date] = []
    if min_date is not None:
        min_parts.append(min_date)
    if max_age is not None:
        min_parts.append(today - max_age)
    min_date_use = max(min_parts, default=None)
    max_parts: list[Date] = []
    if max_date is not None:
        max_parts.append(max_date)
    if min_age is not None:
        max_parts.append(today - min_age)
    max_date_use = min(max_parts, default=None)
    if (
        (min_date_use is not None)
        and (max_date_use is not None)
        and (min_date_use > max_date_use)
    ):
        raise _MinMaxDatePeriodError(min_date=min_date_use, max_date=max_date_use)
    return min_date_use, max_date_use


@dataclass(kw_only=True, slots=True)
class MinMaxDateError(Exception):
    min_date: Date
    max_date: Date


@dataclass(kw_only=True, slots=True)
class _MinMaxDatePeriodError(MinMaxDateError):
    @override
    def __str__(self) -> str:
        return (
            f"Min date must be at most max date; got {self.min_date} > {self.max_date}"
        )


##


class PeriodDict[T: Date | Time | ZonedDateTime | dt.date | dt.time | dt.datetime](
    TypedDict
):
    """A period as a dictionary."""

    start: T
    end: T


##


type _RoundDateDailyUnit = Literal["W", "D"]
type _RoundDateTimeUnit = Literal["H", "M", "S", "ms", "us", "ns"]
type _RoundDateOrDateTimeUnit = _RoundDateDailyUnit | _RoundDateTimeUnit


def round_date_or_date_time[T: Date | PlainDateTime | ZonedDateTime](
    date_or_date_time: T,
    delta: Delta,
    /,
    *,
    mode: DateTimeRoundMode = "half_even",
    weekday: Weekday | None = None,
) -> T:
    """Round a datetime."""
    increment, unit = _round_datetime_decompose(delta)
    match date_or_date_time, unit, weekday:
        case Date() as date, "W" | "D", _:
            return _round_date_weekly_or_daily(
                date, increment, unit, mode=mode, weekday=weekday
            )
        case Date() as date, "H" | "M" | "S" | "ms" | "us" | "ns", _:
            raise _RoundDateOrDateTimeDateWithIntradayDeltaError(date=date, delta=delta)
        case (PlainDateTime() | ZonedDateTime() as date_time, "W" | "D", _):
            return _round_date_time_weekly_or_daily(
                date_time, increment, unit, mode=mode, weekday=weekday
            )
        case (
            PlainDateTime() | ZonedDateTime() as date_time,
            "H" | "M" | "S" | "ms" | "us" | "ns",
            None,
        ):
            return _round_date_time_intraday(date_time, increment, unit, mode=mode)
        case (
            PlainDateTime() | ZonedDateTime() as date_time,
            "H" | "M" | "S" | "ms" | "us" | "ns",
            Weekday(),
        ):
            raise _RoundDateOrDateTimeDateTimeIntraDayWithWeekdayError(
                date_time=date_time, delta=delta, weekday=weekday
            )
        case never:
            assert_never(never)


def _round_datetime_decompose(delta: Delta, /) -> tuple[int, _RoundDateOrDateTimeUnit]:
    with suppress(NumWeeksError):
        return num_weeks(delta), "W"
    with suppress(NumDaysError):
        return num_days(delta), "D"
    with suppress(NumHoursError):
        hours = num_hours(delta)
        divisor = HOURS_PER_DAY
        if (0 < hours < divisor) and (divisor % hours == 0):
            return hours, "H"
        raise _RoundDateOrDateTimeIncrementError(
            delta=delta, increment=hours, divisor=divisor
        )
    with suppress(NumMinutesError):
        minutes = num_minutes(delta)
        divisor = MINUTES_PER_HOUR
        if (0 < minutes < divisor) and (divisor % minutes == 0):
            return minutes, "M"
        raise _RoundDateOrDateTimeIncrementError(
            delta=delta, increment=minutes, divisor=divisor
        )
    with suppress(NumSecondsError):
        seconds = num_seconds(delta)
        divisor = SECONDS_PER_MINUTE
        if (0 < seconds < divisor) and (divisor % seconds == 0):
            return seconds, "S"
        raise _RoundDateOrDateTimeIncrementError(
            delta=delta, increment=seconds, divisor=divisor
        )
    with suppress(NumMilliSecondsError):
        milliseconds = num_milliseconds(delta)
        divisor = MILLISECONDS_PER_SECOND
        if (0 < milliseconds < divisor) and (divisor % milliseconds == 0):
            return milliseconds, "ms"
        raise _RoundDateOrDateTimeIncrementError(
            delta=delta, increment=milliseconds, divisor=divisor
        )
    with suppress(NumMicroSecondsError):
        microseconds = num_microseconds(delta)
        divisor = MICROSECONDS_PER_MILLISECOND
        if (0 < microseconds < divisor) and (divisor % microseconds == 0):
            return microseconds, "us"
        raise _RoundDateOrDateTimeIncrementError(
            delta=delta, increment=microseconds, divisor=divisor
        )
    try:
        nanoseconds = num_nanoseconds(delta)
    except NumNanoSecondsError:
        raise _RoundDateOrDateTimeInvalidDeltaError(delta=delta) from None
    divisor = NANOSECONDS_PER_MICROSECOND
    if (0 < nanoseconds < divisor) and (divisor % nanoseconds == 0):
        return nanoseconds, "ns"
    raise _RoundDateOrDateTimeIncrementError(
        delta=delta, increment=nanoseconds, divisor=divisor
    )


def _round_date_weekly_or_daily(
    date: Date,
    increment: int,
    unit: _RoundDateDailyUnit,
    /,
    *,
    mode: DateTimeRoundMode = "half_even",
    weekday: Weekday | None = None,
) -> Date:
    match unit, weekday:
        case "W", _:
            return _round_date_weekly(date, increment, mode=mode, weekday=weekday)
        case "D", None:
            return _round_date_daily(date, increment, mode=mode)
        case "D", Weekday():
            raise _RoundDateOrDateTimeDateWithWeekdayError(weekday=weekday)
        case never:
            assert_never(never)


def _round_date_weekly(
    date: Date,
    increment: int,
    /,
    *,
    mode: DateTimeRoundMode = "half_even",
    weekday: Weekday | None = None,
) -> Date:
    mapping = {
        None: 0,
        Weekday.MONDAY: 0,
        Weekday.TUESDAY: 1,
        Weekday.WEDNESDAY: 2,
        Weekday.THURSDAY: 3,
        Weekday.FRIDAY: 4,
        Weekday.SATURDAY: 5,
        Weekday.SUNDAY: 6,
    }
    base = Date.MIN.add(days=mapping[weekday])
    return _round_date_daily(date, 7 * increment, mode=mode, base=base)


def _round_date_daily(
    date: Date,
    increment: int,
    /,
    *,
    mode: DateTimeRoundMode = "half_even",
    base: Date = Date.MIN,
) -> Date:
    quotient, remainder = divmod(date.days_since(base), increment)
    match mode:
        case "half_even":
            threshold = increment // 2 + (quotient % 2 == 0) or 1
        case "ceil":
            threshold = 1
        case "floor":
            threshold = increment + 1
        case "half_floor":
            threshold = increment // 2 + 1
        case "half_ceil":
            threshold = increment // 2 or 1
        case never:
            assert_never(never)
    round_up = remainder >= threshold
    return base.add(days=(quotient + round_up) * increment)


def _round_date_time_intraday[T: PlainDateTime | ZonedDateTime](
    date_time: T,
    increment: int,
    unit: _RoundDateTimeUnit,
    /,
    *,
    mode: DateTimeRoundMode = "half_even",
) -> T:
    match unit:
        case "H":
            unit_use = "hour"
        case "M":
            unit_use = "minute"
        case "S":
            unit_use = "second"
        case "ms":
            unit_use = "millisecond"
        case "us":
            unit_use = "microsecond"
        case "ns":
            unit_use = "nanosecond"
        case never:
            assert_never(never)
    return date_time.round(unit_use, increment=increment, mode=mode)


def _round_date_time_weekly_or_daily[T: PlainDateTime | ZonedDateTime](
    date_time: T,
    increment: int,
    unit: _RoundDateDailyUnit,
    /,
    *,
    mode: DateTimeRoundMode = "half_even",
    weekday: Weekday | None = None,
) -> T:
    rounded = cast("T", date_time.round("day", mode=mode))
    new_date = _round_date_weekly_or_daily(
        rounded.date(), increment, unit, mode=mode, weekday=weekday
    )
    return date_time.replace_date(new_date).replace_time(Time())


@dataclass(kw_only=True, slots=True)
class RoundDateOrDateTimeError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _RoundDateOrDateTimeIncrementError(RoundDateOrDateTimeError):
    delta: Delta
    increment: int
    divisor: int

    @override
    def __str__(self) -> str:
        return f"Delta {self.delta} increment must be a proper divisor of {self.divisor}; got {self.increment}"


@dataclass(kw_only=True, slots=True)
class _RoundDateOrDateTimeInvalidDeltaError(RoundDateOrDateTimeError):
    delta: Delta

    @override
    def __str__(self) -> str:
        return f"Delta must be valid; got {self.delta}"


@dataclass(kw_only=True, slots=True)
class _RoundDateOrDateTimeDateWithIntradayDeltaError(RoundDateOrDateTimeError):
    date: Date
    delta: Delta

    @override
    def __str__(self) -> str:
        return f"Dates must not be given intraday durations; got {self.date} and {self.delta}"


@dataclass(kw_only=True, slots=True)
class _RoundDateOrDateTimeDateWithWeekdayError(RoundDateOrDateTimeError):
    weekday: Weekday

    @override
    def __str__(self) -> str:
        return f"Daily rounding must not be given a weekday; got {self.weekday}"


@dataclass(kw_only=True, slots=True)
class _RoundDateOrDateTimeDateTimeIntraDayWithWeekdayError(RoundDateOrDateTimeError):
    date_time: PlainDateTime | ZonedDateTime
    delta: Delta
    weekday: Weekday

    @override
    def __str__(self) -> str:
        return f"Date-times and intraday rounding must not be given a weekday; got {self.date_time}, {self.delta} and {self.weekday}"


##


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=False)
class TimePeriod:
    """A period of times."""

    start: Time
    end: Time

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        return f"{cls}({self.start}, {self.end})"

    def at(
        self, obj: Date | tuple[Date, Date], /, *, time_zone: TimeZoneLike = UTC
    ) -> ZonedDateTimePeriod:
        """Combine a date with a time to create a datetime."""
        match obj:
            case Date() as date:
                start = end = date
            case Date() as start, Date() as end:
                ...
            case never:
                assert_never(never)
        return DatePeriod(start, end).at((self.start, self.end), time_zone=time_zone)

    @classmethod
    def from_dict(cls, mapping: PeriodDict[Time] | PeriodDict[dt.time], /) -> Self:
        """Convert the dictionary to a period."""
        match mapping["start"]:
            case Time() as start:
                ...
            case dt.time() as py_time:
                start = Time.from_py_time(py_time)
            case never:
                assert_never(never)
        match mapping["end"]:
            case Time() as end:
                ...
            case dt.time() as py_time:
                end = Time.from_py_time(py_time)
            case never:
                assert_never(never)
        return cls(start=start, end=end)

    def replace(
        self, *, start: Time | Sentinel = sentinel, end: Time | Sentinel = sentinel
    ) -> Self:
        """Replace elements of the period."""
        return replace_non_sentinel(self, start=start, end=end)

    def to_dict(self) -> PeriodDict[Time]:
        """Convert the period to a dictionary."""
        return PeriodDict(start=self.start, end=self.end)

    def to_py_dict(self) -> PeriodDict[dt.time]:
        """Convert the period to a dictionary."""
        return PeriodDict(start=self.start.py_time(), end=self.end.py_time())


##


def to_date_time_delta(nanos: int, /) -> DateTimeDelta:
    """Construct a date-time delta."""
    components = _to_time_delta_components(nanos)
    days, hours = divmod(components.hours, 24)
    weeks, days = divmod(days, 7)
    match sign(nanos):  # pragma: no cover
        case 1:
            if hours < 0:
                hours += 24
                days -= 1
            if days < 0:
                days += 7
                weeks -= 1
        case -1:
            if hours > 0:
                hours -= 24
                days += 1
            if days > 0:
                days -= 7
                weeks += 1
        case 0:
            ...
    return DateTimeDelta(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=components.minutes,
        seconds=components.seconds,
        microseconds=components.microseconds,
        milliseconds=components.milliseconds,
        nanoseconds=components.nanoseconds,
    )


##


@overload
def to_py_date_or_date_time(date_or_date_time: Date, /) -> dt.date: ...
@overload
def to_py_date_or_date_time(date_or_date_time: ZonedDateTime, /) -> dt.datetime: ...
@overload
def to_py_date_or_date_time(date_or_date_time: None, /) -> None: ...
def to_py_date_or_date_time(
    date_or_date_time: Date | ZonedDateTime | None, /
) -> dt.date | None:
    """Convert a Date or ZonedDateTime into a standard library equivalent."""
    match date_or_date_time:
        case Date() as date:
            return date.py_date()
        case ZonedDateTime() as date_time:
            return date_time.py_datetime()
        case None:
            return None
        case never:
            assert_never(never)


##


@overload
def to_py_time_delta(delta: Delta, /) -> dt.timedelta: ...
@overload
def to_py_time_delta(delta: None, /) -> None: ...
def to_py_time_delta(delta: Delta | None, /) -> dt.timedelta | None:
    """Try convert a DateDelta to a standard library timedelta."""
    match delta:
        case DateDelta():
            return dt.timedelta(days=num_days(delta))
        case TimeDelta():
            nanos = delta.in_nanoseconds()
            micros, remainder = divmod(nanos, 1000)
            if remainder != 0:
                raise ToPyTimeDeltaError(nanoseconds=remainder)
            return dt.timedelta(microseconds=micros)
        case DateTimeDelta():
            return to_py_time_delta(delta.date_part()) + to_py_time_delta(
                delta.time_part()
            )
        case None:
            return None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToPyTimeDeltaError(Exception):
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Time delta must not contain nanoseconds; got {self.nanoseconds}"


##


@overload
def to_time(time: Sentinel, /, *, time_zone: TimeZoneLike = UTC) -> Sentinel: ...
@overload
def to_time(
    time: MaybeCallableTimeLike | None | dt.time = get_time,
    /,
    *,
    time_zone: TimeZoneLike = UTC,
) -> Time: ...
def to_time(
    time: MaybeCallableTimeLike | dt.time | None | Sentinel = get_time,
    /,
    *,
    time_zone: TimeZoneLike = UTC,
) -> Time | Sentinel:
    """Convert to a time."""
    match time:
        case Time() | Sentinel():
            return time
        case None:
            return get_time(time_zone)
        case str():
            return Time.parse_iso(time)
        case dt.time():
            return Time.from_py_time(time)
        case Callable() as func:
            return to_time(func(), time_zone=time_zone)
        case never:
            assert_never(never)


##


def to_time_delta(nanos: int, /) -> TimeDelta:
    """Construct a time delta."""
    components = _to_time_delta_components(nanos)
    return TimeDelta(
        hours=components.hours,
        minutes=components.minutes,
        seconds=components.seconds,
        microseconds=components.microseconds,
        milliseconds=components.milliseconds,
        nanoseconds=components.nanoseconds,
    )


@dataclass(kw_only=True, slots=True)
class _TimeDeltaComponents:
    hours: int
    minutes: int
    seconds: int
    microseconds: int
    milliseconds: int
    nanoseconds: int


def _to_time_delta_components(nanos: int, /) -> _TimeDeltaComponents:
    sign_use = sign(nanos)
    micros, nanos = divmod(nanos, int(1e3))
    millis, micros = divmod(micros, int(1e3))
    secs, millis = divmod(millis, int(1e3))
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    match sign_use:  # pragma: no cover
        case 1:
            if nanos < 0:
                nanos += int(1e3)
                micros -= 1
            if micros < 0:
                micros += int(1e3)
                millis -= 1
            if millis < 0:
                millis += int(1e3)
                secs -= 1
            if secs < 0:
                secs += 60
                mins -= 1
            if mins < 0:
                mins += 60
                hours -= 1
        case -1:
            if nanos > 0:
                nanos -= int(1e3)
                micros += 1
            if micros > 0:
                micros -= int(1e3)
                millis += 1
            if millis > 0:
                millis -= int(1e3)
                secs += 1
            if secs > 0:
                secs -= 60
                mins += 1
            if mins > 0:
                mins -= 60
                hours += 1
        case 0:
            ...
    return _TimeDeltaComponents(
        hours=hours,
        minutes=mins,
        seconds=secs,
        microseconds=micros,
        milliseconds=millis,
        nanoseconds=nanos,
    )


##


@overload
def to_zoned_date_time(
    date_time: Sentinel, /, *, time_zone: TimeZoneLike | None = None
) -> Sentinel: ...
@overload
def to_zoned_date_time(
    date_time: MaybeCallableZonedDateTimeLike | dt.datetime | None = get_now,
    /,
    *,
    time_zone: TimeZoneLike | None = None,
) -> ZonedDateTime: ...
def to_zoned_date_time(
    date_time: MaybeCallableZonedDateTimeLike | dt.datetime | None | Sentinel = get_now,
    /,
    *,
    time_zone: TimeZoneLike | None = None,
) -> ZonedDateTime | Sentinel:
    """Convert to a zoned date-time."""
    match date_time:
        case ZonedDateTime() as date_time_use:
            ...
        case Sentinel():
            return sentinel
        case None:
            return get_now(UTC if time_zone is None else time_zone)
        case str() as text:
            date_time_use = ZonedDateTime.parse_iso(text.replace("~", "/"))
        case dt.datetime() as py_date_time:
            if isinstance(date_time.tzinfo, ZoneInfo):
                py_date_time_use = py_date_time
            elif date_time.tzinfo is dt.UTC:
                py_date_time_use = py_date_time.astimezone(UTC)
            else:
                raise ToZonedDateTimeError(date_time=date_time)
            date_time_use = ZonedDateTime.from_py_datetime(py_date_time_use)
        case Callable() as func:
            return to_zoned_date_time(func(), time_zone=time_zone)
        case never:
            assert_never(never)
    if time_zone is None:
        return date_time_use
    return date_time_use.to_tz(to_time_zone_name(time_zone))


@dataclass(kw_only=True, slots=True)
class ToZonedDateTimeError(Exception):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Expected date-time to have a `ZoneInfo` or `dt.UTC` as its timezone; got {self.date_time.tzinfo}"


##


def two_digit_year_month(year: int, month: int, /) -> YearMonth:
    """Construct a year-month from a 2-digit year."""
    min_year = DATE_TWO_DIGIT_YEAR_MIN.year
    max_year = DATE_TWO_DIGIT_YEAR_MAX.year
    years = range(min_year, max_year + 1)
    (year_use,) = (y for y in years if y % 100 == year)
    return YearMonth(year_use, month)


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
        plain = format(get_now_local().to_plain().format_iso(), f"{length}s")
        self.zoned_datetime = f"{plain}[{LOCAL_TIME_ZONE_NAME}]"

    @classmethod
    @cache
    def _get_length(cls) -> int:
        """Get maximum length of a formatted string."""
        now = get_now_local().replace(nanosecond=1000).to_plain()
        return len(now.format_iso())


##


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=False)
class ZonedDateTimePeriod:
    """A period of time."""

    start: ZonedDateTime
    end: ZonedDateTime

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise _ZonedDateTimePeriodInvalidError(start=self.start, end=self.end)
        if self.start.tz != self.end.tz:
            raise _ZonedDateTimePeriodTimeZoneError(
                start=ZoneInfo(self.start.tz), end=ZoneInfo(self.end.tz)
            )

    def __add__(self, other: TimeDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    def __contains__(self, other: ZonedDateTime, /) -> bool:
        """Check if a date/datetime lies in the period."""
        return self.start <= other <= self.end

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        return f"{cls}({self.start.to_plain()}, {self.end.to_plain()}[{self.time_zone.key}])"

    def __sub__(self, other: TimeDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start - other, end=self.end - other)

    @property
    def delta(self) -> TimeDelta:
        """The duration of the period."""
        return self.end - self.start

    @overload
    def exact_eq(self, period: ZonedDateTimePeriod, /) -> bool: ...
    @overload
    def exact_eq(self, start: ZonedDateTime, end: ZonedDateTime, /) -> bool: ...
    @overload
    def exact_eq(
        self, start: PlainDateTime, end: PlainDateTime, time_zone: ZoneInfo, /
    ) -> bool: ...
    def exact_eq(self, *args: Any) -> bool:
        """Check if a period is exactly equal to another."""
        if (len(args) == 1) and isinstance(args[0], ZonedDateTimePeriod):
            return self.start.exact_eq(args[0].start) and self.end.exact_eq(args[0].end)
        if (
            (len(args) == 2)
            and isinstance(args[0], ZonedDateTime)
            and isinstance(args[1], ZonedDateTime)
        ):
            return self.exact_eq(ZonedDateTimePeriod(args[0], args[1]))
        if (
            (len(args) == 3)
            and isinstance(args[0], PlainDateTime)
            and isinstance(args[1], PlainDateTime)
            and isinstance(args[2], ZoneInfo)
        ):
            return self.exact_eq(
                ZonedDateTimePeriod(
                    args[0].assume_tz(args[2].key), args[1].assume_tz(args[2].key)
                )
            )
        raise _ZonedDateTimePeriodExactEqError(args=args)

    def format_compact(self) -> str:
        """Format the period in a compact fashion."""
        fc, start, end = format_compact, self.start, self.end
        if start == end:
            if end.second != 0:
                return f"{fc(start)}="
            if end.minute != 0:
                return f"{fc(start, fmt='%Y%m%dT%H%M')}="
            return f"{fc(start, fmt='%Y%m%dT%H')}="
        if start.date() == end.date():
            if end.second != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%H%M%S')}"
            if end.minute != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%H%M')}"
            return f"{fc(start.to_plain())}-{fc(end, fmt='%H')}"
        if start.date().year_month() == end.date().year_month():
            if end.second != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%dT%H%M%S')}"
            if end.minute != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%dT%H%M')}"
            return f"{fc(start.to_plain())}-{fc(end, fmt='%dT%H')}"
        if start.year == end.year:
            if end.second != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%m%dT%H%M%S')}"
            if end.minute != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%m%dT%H%M')}"
            return f"{fc(start.to_plain())}-{fc(end, fmt='%m%dT%H')}"
        if end.second != 0:
            return f"{fc(start.to_plain())}-{fc(end)}"
        if end.minute != 0:
            return f"{fc(start.to_plain())}-{fc(end, fmt='%Y%m%dT%H%M')}"
        return f"{fc(start.to_plain())}-{fc(end, fmt='%Y%m%dT%H')}"

    @classmethod
    def from_dict(
        cls, mapping: PeriodDict[ZonedDateTime] | PeriodDict[dt.datetime], /
    ) -> Self:
        """Convert the dictionary to a period."""
        match mapping["start"]:
            case ZonedDateTime() as start:
                ...
            case dt.date() as py_datetime:
                start = ZonedDateTime.from_py_datetime(py_datetime)
            case never:
                assert_never(never)
        match mapping["end"]:
            case ZonedDateTime() as end:
                ...
            case dt.date() as py_datetime:
                end = ZonedDateTime.from_py_datetime(py_datetime)
            case never:
                assert_never(never)
        return cls(start=start, end=end)

    def replace(
        self,
        *,
        start: ZonedDateTime | Sentinel = sentinel,
        end: ZonedDateTime | Sentinel = sentinel,
    ) -> Self:
        """Replace elements of the period."""
        return replace_non_sentinel(self, start=start, end=end)

    @property
    def time_zone(self) -> ZoneInfo:
        """The time zone of the period."""
        return ZoneInfo(self.start.tz)

    def to_dict(self) -> PeriodDict[ZonedDateTime]:
        """Convert the period to a dictionary."""
        return PeriodDict(start=self.start, end=self.end)

    def to_py_dict(self) -> PeriodDict[dt.datetime]:
        """Convert the period to a dictionary."""
        return PeriodDict(start=self.start.py_datetime(), end=self.end.py_datetime())

    def to_tz(self, time_zone: TimeZoneLike, /) -> Self:
        """Convert the time zone."""
        tz = to_time_zone_name(time_zone)
        return self.replace(start=self.start.to_tz(tz), end=self.end.to_tz(tz))


@dataclass(kw_only=True, slots=True)
class ZonedDateTimePeriodError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ZonedDateTimePeriodInvalidError[T: Date | ZonedDateTime](
    ZonedDateTimePeriodError
):
    start: T
    end: T

    @override
    def __str__(self) -> str:
        return f"Invalid period; got {self.start} > {self.end}"


@dataclass(kw_only=True, slots=True)
class _ZonedDateTimePeriodTimeZoneError(ZonedDateTimePeriodError):
    start: ZoneInfo
    end: ZoneInfo

    @override
    def __str__(self) -> str:
        return f"Period must contain exactly one time zone; got {self.start} and {self.end}"


@dataclass(kw_only=True, slots=True)
class _ZonedDateTimePeriodExactEqError(ZonedDateTimePeriodError):
    args: tuple[Any, ...]

    @override
    def __str__(self) -> str:
        return f"Invalid arguments; got {self.args}"


__all__ = [
    "DATE_DELTA_PARSABLE_MAX",
    "DATE_DELTA_PARSABLE_MIN",
    "DATE_TIME_DELTA_PARSABLE_MAX",
    "DATE_TIME_DELTA_PARSABLE_MIN",
    "DATE_TWO_DIGIT_YEAR_MAX",
    "DATE_TWO_DIGIT_YEAR_MIN",
    "DatePeriod",
    "DatePeriodError",
    "MeanDateTimeError",
    "MinMaxDateError",
    "PeriodDict",
    "RoundDateOrDateTimeError",
    "TimePeriod",
    "ToPyTimeDeltaError",
    "WheneverLogRecord",
    "ZonedDateTimePeriod",
    "ZonedDateTimePeriodError",
    "add_year_month",
    "datetime_utc",
    "diff_year_month",
    "format_compact",
    "from_timestamp",
    "from_timestamp_millis",
    "from_timestamp_nanos",
    "is_weekend",
    "mean_datetime",
    "min_max_date",
    "round_date_or_date_time",
    "sub_year_month",
    "to_date_time_delta",
    "to_py_date_or_date_time",
    "to_py_time_delta",
    "to_time",
    "to_zoned_date_time",
    "two_digit_year_month",
]
