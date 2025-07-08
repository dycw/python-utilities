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
    SupportsFloat,
    assert_never,
    cast,
    overload,
    override,
)

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

from utilities.math import sign
from utilities.platform import get_strftime
from utilities.sentinel import Sentinel, sentinel
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME
from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import (
        DateOrDateTimeDelta,
        DateTimeRoundMode,
        Delta,
        MaybeCallableDate,
        MaybeCallableZonedDateTime,
        TimeOrDateTimeDelta,
        TimeZoneLike,
    )


## bounds


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
    obj: Date | Time | PlainDateTime | ZonedDateTime, /, *, fmt: str | None = None
) -> str:
    """Format the date/datetime in a compact fashion."""
    match obj:
        case Date() as date:
            obj_use = date.py_date()
            fmt_use = "%Y%m%d" if fmt is None else fmt
        case Time() as time:
            obj_use = time.round().py_time()
            fmt_use = "%H%M%S" if fmt is None else fmt
        case PlainDateTime() as datetime:
            obj_use = datetime.round().py_datetime()
            fmt_use = "%Y%m%dT%H%M%S" if fmt is None else fmt
        case ZonedDateTime() as datetime:
            return f"{format_compact(datetime.to_plain(), fmt=fmt)}[{datetime.tz}]"
        case _ as never:
            assert_never(never)
    return obj_use.strftime(get_strftime(fmt_use))


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
    return get_now(time_zone=LOCAL_TIME_ZONE)


NOW_LOCAL = get_now_local()


##


def get_today(*, time_zone: TimeZoneLike = UTC) -> Date:
    """Get the current, timezone-aware local date."""
    return get_now(time_zone=time_zone).date()


TODAY_UTC = get_today(time_zone=UTC)


def get_today_local() -> Date:
    """Get the current, timezone-aware local date."""
    return get_today(time_zone=LOCAL_TIME_ZONE)


TODAY_LOCAL = get_today_local()


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
    """Ucompute the min/max date given a combination of dates/ages."""
    today = get_today(time_zone=time_zone)
    min_parts: list[Date] = []
    if min_date is not None:
        if min_date > today:
            raise _MinMaxDateMinDateError(min_date=min_date, today=today)
        min_parts.append(min_date)
    if max_age is not None:
        min_parts.append(today - max_age)
    min_date_use = max(min_parts, default=None)
    max_parts: list[Date] = []
    if max_date is not None:
        if max_date > today:
            raise _MinMaxDateMaxDateError(max_date=max_date, today=today)
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
class MinMaxDateError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _MinMaxDateMinDateError(MinMaxDateError):
    min_date: Date
    today: Date

    @override
    def __str__(self) -> str:
        return f"Min date must be at most today; got {self.min_date} > {self.today}"


@dataclass(kw_only=True, slots=True)
class _MinMaxDateMaxDateError(MinMaxDateError):
    max_date: Date
    today: Date

    @override
    def __str__(self) -> str:
        return f"Max date must be at most today; got {self.max_date} > {self.today}"


@dataclass(kw_only=True, slots=True)
class _MinMaxDatePeriodError(MinMaxDateError):
    min_date: Date
    max_date: Date

    @override
    def __str__(self) -> str:
        return (
            f"Min date must be at most max date; got {self.min_date} > {self.max_date}"
        )


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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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


@overload
def to_date(*, date: MaybeCallableDate) -> Date: ...
@overload
def to_date(*, date: None) -> None: ...
@overload
def to_date(*, date: Sentinel) -> Sentinel: ...
@overload
def to_date(*, date: MaybeCallableDate | Sentinel) -> Date | Sentinel: ...
@overload
def to_date(
    *, date: MaybeCallableDate | None | Sentinel = sentinel
) -> Date | None | Sentinel: ...
def to_date(
    *, date: MaybeCallableDate | None | Sentinel = sentinel
) -> Date | None | Sentinel:
    """Get the date."""
    match date:
        case Date() | None | Sentinel():
            return date
        case Callable() as func:
            return to_date(date=func())
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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


def to_local_plain(date_time: ZonedDateTime, /) -> PlainDateTime:
    """Convert a datetime to its local/plain variant."""
    return date_time.to_tz(LOCAL_TIME_ZONE_NAME).to_plain()


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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
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
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToPyTimeDeltaError(Exception):
    nanoseconds: int

    @override
    def __str__(self) -> str:
        return f"Time delta must not contain nanoseconds; got {self.nanoseconds}"


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
        case _ as never:
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
        case _ as never:
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
def to_zoned_date_time(*, date_time: MaybeCallableZonedDateTime) -> ZonedDateTime: ...
@overload
def to_zoned_date_time(*, date_time: None) -> None: ...
@overload
def to_zoned_date_time(*, date_time: Sentinel) -> Sentinel: ...
def to_zoned_date_time(
    *, date_time: MaybeCallableZonedDateTime | None | Sentinel = sentinel
) -> ZonedDateTime | None | Sentinel:
    """Resolve into a zoned date_time."""
    match date_time:
        case ZonedDateTime() | None | Sentinel():
            return date_time
        case Callable() as func:
            return to_zoned_date_time(date_time=func())
        case _ as never:
            assert_never(never)


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
    "SECOND",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "TODAY_LOCAL",
    "TODAY_UTC",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
    "MeanDateTimeError",
    "MinMaxDateError",
    "RoundDateOrDateTimeError",
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
    "add_year_month",
    "datetime_utc",
    "diff_year_month",
    "format_compact",
    "from_timestamp",
    "from_timestamp_millis",
    "from_timestamp_nanos",
    "get_now",
    "get_now_local",
    "get_today",
    "get_today_local",
    "mean_datetime",
    "min_max_date",
    "round_date_or_date_time",
    "sub_year_month",
    "to_date",
    "to_date_time_delta",
    "to_days",
    "to_local_plain",
    "to_microseconds",
    "to_milliseconds",
    "to_minutes",
    "to_months",
    "to_months_and_days",
    "to_nanoseconds",
    "to_py_date_or_date_time",
    "to_py_time_delta",
    "to_seconds",
    "to_weeks",
    "to_years",
    "to_zoned_date_time",
    "two_digit_year_month",
]
