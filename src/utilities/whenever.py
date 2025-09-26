from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
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

from utilities.dataclasses import replace_non_sentinel
from utilities.functions import get_class_name
from utilities.math import sign
from utilities.platform import get_strftime
from utilities.sentinel import Sentinel, sentinel
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME
from utilities.zoneinfo import UTC, to_time_zone_name

if TYPE_CHECKING:
    from utilities.types import (
        DateOrDateTimeDelta,
        DateTimeRoundMode,
        Delta,
        MaybeCallableDateLike,
        MaybeCallableTimeLike,
        MaybeCallableZonedDateTimeLike,
        TimeOrDateTimeDelta,
        TimeZoneLike,
    )


# bounds


ZONED_DATE_TIME_MIN = PlainDateTime.MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX = PlainDateTime.MAX.assume_tz(UTC.key)


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
DATE_DELTA_MIN = DATE_TIME_DELTA_MIN.date_part()
DATE_DELTA_MAX = DATE_TIME_DELTA_MAX.date_part()
TIME_DELTA_MIN = TimeDelta(hours=-87831216)
TIME_DELTA_MAX = TimeDelta(hours=87831216)


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


ZERO_DAYS = DateDelta()
ZERO_TIME = TimeDelta()
MICROSECOND = TimeDelta(microseconds=1)
MILLISECOND = TimeDelta(milliseconds=1)
SECOND = TimeDelta(seconds=1)
MINUTE = TimeDelta(minutes=1)
HOUR = TimeDelta(hours=1)
DAY = DateDelta(days=1)
WEEK = DateDelta(weeks=1)
MONTH = DateDelta(months=1)
YEAR = DateDelta(years=1)


##


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


def get_now(time_zone: TimeZoneLike = UTC, /) -> ZonedDateTime:
    """Get the current zoned date-time."""
    return ZonedDateTime.now(to_time_zone_name(time_zone))


NOW_UTC = get_now(UTC)


def get_now_local() -> ZonedDateTime:
    """Get the current zoned date-time in the local time-zone."""
    return get_now(LOCAL_TIME_ZONE)


NOW_LOCAL = get_now_local()


def get_now_plain(time_zone: TimeZoneLike = UTC, /) -> PlainDateTime:
    """Get the current plain date-time."""
    return get_now(time_zone).to_plain()


NOW_PLAIN = get_now_plain()


def get_now_local_plain() -> PlainDateTime:
    """Get the current plain date-time in the local time-zone."""
    return get_now_local().to_plain()


NOW_LOCAL_PLAIN = get_now_local_plain()


##


def get_time(time_zone: TimeZoneLike = UTC, /) -> Time:
    """Get the current time."""
    return get_now(time_zone).time()


TIME_UTC = get_time(UTC)


def get_time_local() -> Time:
    """Get the current time in the local time-zone."""
    return get_time(LOCAL_TIME_ZONE)


TIME_LOCAL = get_time_local()


##


def get_today(time_zone: TimeZoneLike = UTC, /) -> Date:
    """Get the current, timezone-aware local date."""
    return get_now(time_zone).date()


TODAY_UTC = get_today(UTC)


def get_today_local() -> Date:
    """Get the current, timezone-aware local date."""
    return get_today(LOCAL_TIME_ZONE)


TODAY_LOCAL = get_today_local()


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
    try:
        weeks = to_weeks(delta)
    except ToWeeksError:
        pass
    else:
        return weeks, "W"
    try:
        days = to_days(delta)
    except ToDaysError:
        pass
    else:
        return days, "D"
    try:
        hours = to_hours(delta)
    except ToHoursError:
        pass
    else:
        if (0 < hours < 24) and (24 % hours == 0):
            return hours, "H"
        raise _RoundDateOrDateTimeIncrementError(
            duration=delta, increment=hours, divisor=24
        )
    try:
        minutes = to_minutes(delta)
    except ToMinutesError:
        pass
    else:
        if (0 < minutes < 60) and (60 % minutes == 0):
            return minutes, "M"
        raise _RoundDateOrDateTimeIncrementError(
            duration=delta, increment=minutes, divisor=60
        )
    try:
        seconds = to_seconds(delta)
    except ToSecondsError:
        pass
    else:
        if (0 < seconds < 60) and (60 % seconds == 0):
            return seconds, "S"
        raise _RoundDateOrDateTimeIncrementError(
            duration=delta, increment=seconds, divisor=60
        )
    try:
        milliseconds = to_milliseconds(delta)
    except ToMillisecondsError:
        pass
    else:
        if (0 < milliseconds < 1000) and (1000 % milliseconds == 0):
            return milliseconds, "ms"
        raise _RoundDateOrDateTimeIncrementError(
            duration=delta, increment=milliseconds, divisor=1000
        )
    try:
        microseconds = to_microseconds(delta)
    except ToMicrosecondsError:
        pass
    else:
        if (0 < microseconds < 1000) and (1000 % microseconds == 0):
            return microseconds, "us"
        raise _RoundDateOrDateTimeIncrementError(
            duration=delta, increment=microseconds, divisor=1000
        )
    try:
        nanoseconds = to_nanoseconds(delta)
    except ToNanosecondsError:
        raise _RoundDateOrDateTimeInvalidDurationError(duration=delta) from None
    if (0 < nanoseconds < 1000) and (1000 % nanoseconds == 0):
        return nanoseconds, "ns"
    raise _RoundDateOrDateTimeIncrementError(
        duration=delta, increment=nanoseconds, divisor=1000
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
    duration: Delta
    increment: int
    divisor: int

    @override
    def __str__(self) -> str:
        return f"Duration {self.duration} increment must be a proper divisor of {self.divisor}; got {self.increment}"


@dataclass(kw_only=True, slots=True)
class _RoundDateOrDateTimeInvalidDurationError(RoundDateOrDateTimeError):
    duration: Delta

    @override
    def __str__(self) -> str:
        return f"Duration must be valid; got {self.duration}"


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


@overload
def to_date(date: Sentinel, /, *, time_zone: TimeZoneLike = UTC) -> Sentinel: ...
@overload
def to_date(
    date: MaybeCallableDateLike | None | dt.date = get_today,
    /,
    *,
    time_zone: TimeZoneLike = UTC,
) -> Date: ...
def to_date(
    date: MaybeCallableDateLike | dt.date | None | Sentinel = get_today,
    /,
    *,
    time_zone: TimeZoneLike = UTC,
) -> Date | Sentinel:
    """Convert to a date."""
    match date:
        case Date() | Sentinel():
            return date
        case None:
            return get_today(time_zone)
        case str():
            return Date.parse_iso(date)
        case dt.date():
            return Date.from_py_date(date)
        case Callable() as func:
            return to_date(func(), time_zone=time_zone)
        case never:
            assert_never(never)


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


def to_days(delta: Delta, /) -> int:
    """Compute the number of days in a delta."""
    match delta:
        case DateDelta():
            months, days = delta.in_months_days()
            if months != 0:
                raise _ToDaysMonthsError(delta=delta, months=months)
            return days
        case TimeDelta():
            nanos = to_nanoseconds(delta)
            days, remainder = divmod(nanos, 24 * 60 * 60 * int(1e9))
            if remainder != 0:
                raise _ToDaysNanosecondsError(delta=delta, nanoseconds=remainder)
            return days
        case DateTimeDelta():
            try:
                return to_days(delta.date_part()) + to_days(delta.time_part())
            except _ToDaysMonthsError as error:
                raise _ToDaysMonthsError(delta=delta, months=error.months) from None
            except _ToDaysNanosecondsError as error:
                raise _ToDaysNanosecondsError(
                    delta=delta, nanoseconds=error.nanoseconds
                ) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToDaysError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToDaysMonthsError(ToDaysError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToDaysNanosecondsError(ToDaysError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


##


def to_hours(delta: Delta, /) -> int:
    """Compute the number of hours in a delta."""
    match delta:
        case DateDelta():
            try:
                days = to_days(delta)
            except _ToDaysMonthsError as error:
                raise _ToHoursMonthsError(delta=delta, months=error.months) from None
            return 24 * days
        case TimeDelta():
            nanos = to_nanoseconds(delta)
            divisor = 60 * 60 * int(1e9)
            hours, remainder = divmod(nanos, divisor)
            if remainder != 0:
                raise _ToHoursNanosecondsError(delta=delta, nanoseconds=remainder)
            return hours
        case DateTimeDelta():
            try:
                return to_hours(delta.date_part()) + to_hours(delta.time_part())
            except _ToHoursMonthsError as error:
                raise _ToHoursMonthsError(delta=delta, months=error.months) from None
            except _ToHoursNanosecondsError as error:
                raise _ToHoursNanosecondsError(
                    delta=delta, nanoseconds=error.nanoseconds
                ) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToHoursError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToHoursMonthsError(ToHoursError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToHoursNanosecondsError(ToHoursError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


##


def to_microseconds(delta: Delta, /) -> int:
    """Compute the number of microseconds in a delta."""
    match delta:
        case DateDelta():
            try:
                days = to_days(delta)
            except _ToDaysMonthsError as error:
                raise _ToMicrosecondsMonthsError(
                    delta=delta, months=error.months
                ) from None
            return 24 * 60 * 60 * int(1e6) * days
        case TimeDelta():
            nanos = to_nanoseconds(delta)
            microseconds, remainder = divmod(nanos, int(1e3))
            if remainder != 0:
                raise _ToMicrosecondsNanosecondsError(
                    delta=delta, nanoseconds=remainder
                )
            return microseconds
        case DateTimeDelta():
            try:
                return to_microseconds(delta.date_part()) + to_microseconds(
                    delta.time_part()
                )
            except _ToMicrosecondsMonthsError as error:
                raise _ToMicrosecondsMonthsError(
                    delta=delta, months=error.months
                ) from None
            except _ToMicrosecondsNanosecondsError as error:
                raise _ToMicrosecondsNanosecondsError(
                    delta=delta, nanoseconds=error.nanoseconds
                ) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToMicrosecondsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToMicrosecondsMonthsError(ToMicrosecondsError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToMicrosecondsNanosecondsError(ToMicrosecondsError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


##


def to_milliseconds(delta: Delta, /) -> int:
    """Compute the number of milliseconds in a delta."""
    match delta:
        case DateDelta():
            try:
                days = to_days(delta)
            except _ToDaysMonthsError as error:
                raise _ToMillisecondsMonthsError(
                    delta=delta, months=error.months
                ) from None
            return 24 * 60 * 60 * int(1e3) * days
        case TimeDelta():
            nanos = to_nanoseconds(delta)
            milliseconds, remainder = divmod(nanos, int(1e6))
            if remainder != 0:
                raise _ToMillisecondsNanosecondsError(
                    delta=delta, nanoseconds=remainder
                )
            return milliseconds
        case DateTimeDelta():
            try:
                return to_milliseconds(delta.date_part()) + to_milliseconds(
                    delta.time_part()
                )
            except _ToMillisecondsMonthsError as error:
                raise _ToMillisecondsMonthsError(
                    delta=delta, months=error.months
                ) from None
            except _ToMillisecondsNanosecondsError as error:
                raise _ToMillisecondsNanosecondsError(
                    delta=delta, nanoseconds=error.nanoseconds
                ) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToMillisecondsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToMillisecondsMonthsError(ToMillisecondsError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToMillisecondsNanosecondsError(ToMillisecondsError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


##


def to_minutes(delta: Delta, /) -> int:
    """Compute the number of minutes in a delta."""
    match delta:
        case DateDelta():
            try:
                days = to_days(delta)
            except _ToDaysMonthsError as error:
                raise _ToMinutesMonthsError(delta=delta, months=error.months) from None
            return 24 * 60 * days
        case TimeDelta():
            nanos = to_nanoseconds(delta)
            minutes, remainder = divmod(nanos, 60 * int(1e9))
            if remainder != 0:
                raise _ToMinutesNanosecondsError(delta=delta, nanoseconds=remainder)
            return minutes
        case DateTimeDelta():
            try:
                return to_minutes(delta.date_part()) + to_minutes(delta.time_part())
            except _ToMinutesMonthsError as error:
                raise _ToMinutesMonthsError(delta=delta, months=error.months) from None
            except _ToMinutesNanosecondsError as error:
                raise _ToMinutesNanosecondsError(
                    delta=delta, nanoseconds=error.nanoseconds
                ) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToMinutesError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToMinutesMonthsError(ToMinutesError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToMinutesNanosecondsError(ToMinutesError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


##


def to_months(delta: DateOrDateTimeDelta, /) -> int:
    """Compute the number of months in a delta."""
    match delta:
        case DateDelta():
            months, days = delta.in_months_days()
            if days != 0:
                raise _ToMonthsDaysError(delta=delta, days=days)
            return months
        case DateTimeDelta():
            if delta.time_part() != TimeDelta():
                raise _ToMonthsTimeError(delta=delta)
            try:
                return to_months(delta.date_part())
            except _ToMonthsDaysError as error:
                raise _ToMonthsDaysError(delta=delta, days=error.days) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToMonthsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToMonthsDaysError(ToMonthsError):
    delta: DateOrDateTimeDelta
    days: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain days; got {self.days}"


@dataclass(kw_only=True, slots=True)
class _ToMonthsTimeError(ToMonthsError):
    delta: DateTimeDelta

    @override
    def __str__(self) -> str:
        return f"Delta must not contain a time part; got {self.delta.time_part()}"


##


def to_months_and_days(delta: DateOrDateTimeDelta, /) -> tuple[int, int]:
    """Compute the number of months & days in a delta."""
    match delta:
        case DateDelta():
            return delta.in_months_days()
        case DateTimeDelta():
            if delta.time_part() != TimeDelta():
                raise ToMonthsAndDaysError(delta=delta)
            return to_months_and_days(delta.date_part())
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToMonthsAndDaysError(Exception):
    delta: DateTimeDelta

    @override
    def __str__(self) -> str:
        return f"Delta must not contain a time part; got {self.delta.time_part()}"


##


def to_nanoseconds(delta: Delta, /) -> int:
    """Compute the number of nanoseconds in a date-time delta."""
    match delta:
        case DateDelta():
            try:
                days = to_days(delta)
            except _ToDaysMonthsError as error:
                raise ToNanosecondsError(delta=delta, months=error.months) from None
            return 24 * 60 * 60 * int(1e9) * days
        case TimeDelta():
            return delta.in_nanoseconds()
        case DateTimeDelta():
            try:
                return to_nanoseconds(delta.date_part()) + to_nanoseconds(
                    delta.time_part()
                )
            except ToNanosecondsError as error:
                raise ToNanosecondsError(delta=delta, months=error.months) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToNanosecondsError(Exception):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


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
            return dt.timedelta(days=to_days(delta))
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


def to_seconds(delta: Delta, /) -> int:
    """Compute the number of seconds in a delta."""
    match delta:
        case DateDelta():
            try:
                days = to_days(delta)
            except _ToDaysMonthsError as error:
                raise _ToSecondsMonthsError(delta=delta, months=error.months) from None
            return 24 * 60 * 60 * days
        case TimeDelta():
            nanos = to_nanoseconds(delta)
            seconds, remainder = divmod(nanos, int(1e9))
            if remainder != 0:
                raise _ToSecondsNanosecondsError(delta=delta, nanoseconds=remainder)
            return seconds
        case DateTimeDelta():
            try:
                return to_seconds(delta.date_part()) + to_seconds(delta.time_part())
            except _ToSecondsMonthsError as error:
                raise _ToSecondsMonthsError(delta=delta, months=error.months) from None
            except _ToSecondsNanosecondsError as error:
                raise _ToSecondsNanosecondsError(
                    delta=delta, nanoseconds=error.nanoseconds
                ) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToSecondsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToSecondsMonthsError(ToSecondsError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToSecondsNanosecondsError(ToSecondsError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


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


def to_weeks(delta: Delta, /) -> int:
    """Compute the number of weeks in a delta."""
    try:
        days = to_days(delta)
    except _ToDaysMonthsError as error:
        raise _ToWeeksMonthsError(delta=error.delta, months=error.months) from None
    except _ToDaysNanosecondsError as error:
        raise _ToWeeksNanosecondsError(
            delta=error.delta, nanoseconds=error.nanoseconds
        ) from None
    weeks, remainder = divmod(days, 7)
    if remainder != 0:
        raise _ToWeeksDaysError(delta=delta, days=remainder) from None
    return weeks


@dataclass(kw_only=True, slots=True)
class ToWeeksError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToWeeksMonthsError(ToWeeksError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToWeeksNanosecondsError(ToWeeksError):
    delta: TimeOrDateTimeDelta
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra nanoseconds; got {self.nanoseconds}"


@dataclass(kw_only=True, slots=True)
class _ToWeeksDaysError(ToWeeksError):
    delta: Delta
    days: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain extra days; got {self.days}"


##


def to_years(delta: DateOrDateTimeDelta, /) -> int:
    """Compute the number of years in a delta."""
    match delta:
        case DateDelta():
            years, months, days = delta.in_years_months_days()
            if months != 0:
                raise _ToYearsMonthsError(delta=delta, months=months)
            if days != 0:
                raise _ToYearsDaysError(delta=delta, days=days)
            return years
        case DateTimeDelta():
            if delta.time_part() != TimeDelta():
                raise _ToYearsTimeError(delta=delta)
            try:
                return to_years(delta.date_part())
            except _ToYearsMonthsError as error:
                raise _ToYearsMonthsError(delta=delta, months=error.months) from None
            except _ToYearsDaysError as error:
                raise _ToYearsDaysError(delta=delta, days=error.days) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToYearsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToYearsMonthsError(ToYearsError):
    delta: DateOrDateTimeDelta
    months: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months; got {self.months}"


@dataclass(kw_only=True, slots=True)
class _ToYearsDaysError(ToYearsError):
    delta: DateOrDateTimeDelta
    days: int

    @override
    def __str__(self) -> str:
        return f"Delta must not contain days; got {self.days}"


@dataclass(kw_only=True, slots=True)
class _ToYearsTimeError(ToYearsError):
    delta: DateTimeDelta

    @override
    def __str__(self) -> str:
        return f"Delta must not contain a time part; got {self.delta.time_part()}"


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
    "DATE_DELTA_MAX",
    "DATE_DELTA_MIN",
    "DATE_DELTA_PARSABLE_MAX",
    "DATE_DELTA_PARSABLE_MIN",
    "DATE_TIME_DELTA_MAX",
    "DATE_TIME_DELTA_MIN",
    "DATE_TIME_DELTA_PARSABLE_MAX",
    "DATE_TIME_DELTA_PARSABLE_MIN",
    "DATE_TWO_DIGIT_YEAR_MAX",
    "DATE_TWO_DIGIT_YEAR_MIN",
    "DAY",
    "HOUR",
    "MICROSECOND",
    "MILLISECOND",
    "MINUTE",
    "MONTH",
    "NOW_LOCAL",
    "NOW_LOCAL_PLAIN",
    "NOW_PLAIN",
    "SECOND",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "TIME_LOCAL",
    "TIME_UTC",
    "TODAY_LOCAL",
    "TODAY_UTC",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
    "DatePeriod",
    "DatePeriodError",
    "MeanDateTimeError",
    "MinMaxDateError",
    "PeriodDict",
    "RoundDateOrDateTimeError",
    "TimePeriod",
    "ToDaysError",
    "ToMinutesError",
    "ToMonthsAndDaysError",
    "ToMonthsError",
    "ToNanosecondsError",
    "ToPyTimeDeltaError",
    "ToSecondsError",
    "ToWeeksError",
    "ToYearsError",
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
    "get_now",
    "get_now_local",
    "get_now_local_plain",
    "get_now_plain",
    "get_time",
    "get_time_local",
    "get_today",
    "get_today_local",
    "is_weekend",
    "mean_datetime",
    "min_max_date",
    "round_date_or_date_time",
    "sub_year_month",
    "to_date",
    "to_date_time_delta",
    "to_days",
    "to_microseconds",
    "to_milliseconds",
    "to_minutes",
    "to_months",
    "to_months_and_days",
    "to_nanoseconds",
    "to_py_date_or_date_time",
    "to_py_time_delta",
    "to_seconds",
    "to_time",
    "to_weeks",
    "to_years",
    "to_zoned_date_time",
    "two_digit_year_month",
]
