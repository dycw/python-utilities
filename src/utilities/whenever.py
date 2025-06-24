from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import cache
from logging import LogRecord
from statistics import fmean
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Self,
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
    YearMonth,
    ZonedDateTime,
)

from utilities.math import sign
from utilities.platform import get_strftime
from utilities.re import ExtractGroupsError, extract_groups
from utilities.sentinel import Sentinel, sentinel
from utilities.types import DateTimeRoundUnit, MaybeStr
from utilities.tzlocal import LOCAL_TIME_ZONE, LOCAL_TIME_ZONE_NAME
from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import (
        MaybeCallableDate,
        MaybeCallableZonedDateTime,
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


class Freq:
    """A rounding frequency."""

    unit: DateTimeRoundUnit
    increment: int
    _mapping: ClassVar[Mapping[DateTimeRoundUnit, _DateTimeRoundUnitAbbrev]] = {
        "day": "D",
        "hour": "H",
        "minute": "M",
        "second": "S",
        "millisecond": "ms",
        "microsecond": "us",
        "nanosecond": "ns",
    }

    def __init__(
        self, *, unit: DateTimeRoundUnit = "second", increment: int = 1
    ) -> None:
        super().__init__()
        if (unit == "day") and (increment != 1):
            raise _FreqDayIncrementError(increment=increment)
        if (unit == "hour") and not ((0 < increment < 24) and (24 % increment == 0)):
            raise _FreqIncrementError(unit=unit, increment=increment, divisor=24)
        if (unit in {"minute", "second"}) and not (
            (0 < increment < 60) and (60 % increment == 0)
        ):
            raise _FreqIncrementError(unit=unit, increment=increment, divisor=60)
        if (unit in {"millisecond", "microsecond", "nanosecond"}) and not (
            (0 < increment < 1000) and (1000 % increment == 0)
        ):
            raise _FreqIncrementError(unit=unit, increment=increment, divisor=1000)
        self.unit = unit
        self.increment = increment

    @override
    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, Freq):
            return NotImplemented
        return (self.unit == other.unit) and (self.increment == other.increment)

    @override
    def __hash__(self) -> int:
        return hash((self.unit, self.increment))

    @override
    def __repr__(self) -> str:
        return f"{type(self).__name__}(unit={self.unit!r}, increment={self.increment})"

    @classmethod
    def parse(cls, text: str, /) -> Self:
        try:
            increment, abbrev = extract_groups(r"^(\d*)(D|H|M|S|ms|us|ns)$", text)
        except ExtractGroupsError:
            raise _FreqParseError(text=text) from None
        return cls(
            unit=cls._expand(cast("_DateTimeRoundUnitAbbrev", abbrev)),
            increment=int(increment) if len(increment) >= 1 else 1,
        )

    def serialize(self) -> str:
        if self.increment == 1:
            return self._abbreviation
        return f"{self.increment}{self._abbreviation}"

    @classmethod
    def _abbreviate(cls, unit: DateTimeRoundUnit, /) -> _DateTimeRoundUnitAbbrev:
        return cls._mapping[unit]

    @property
    def _abbreviation(self) -> _DateTimeRoundUnitAbbrev:
        return self._mapping[self.unit]

    @classmethod
    def _expand(cls, unit: _DateTimeRoundUnitAbbrev, /) -> DateTimeRoundUnit:
        values: set[DateTimeRoundUnit] = {
            k for k, v in cls._mapping.items() if v == unit
        }
        (value,) = values
        return value


type FreqLike = MaybeStr[Freq]
type _DateTimeRoundUnitAbbrev = Literal["D", "H", "M", "S", "ms", "us", "ns"]


@dataclass(kw_only=True, slots=True)
class FreqError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _FreqDayIncrementError(FreqError):
    increment: int

    @override
    def __str__(self) -> str:
        return f"Increment must be 1 for the 'day' unit; got {self.increment}"


@dataclass(kw_only=True, slots=True)
class _FreqIncrementError(FreqError):
    unit: DateTimeRoundUnit
    increment: int
    divisor: int

    @override
    def __str__(self) -> str:
        return f"Increment must be a proper divisor of {self.divisor} for the {self.unit!r} unit; got {self.increment}"


@dataclass(kw_only=True, slots=True)
class _FreqParseError(FreqError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse frequency; got {self.text!r}"


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


def to_days(delta: DateDelta, /) -> int:
    """Compute the number of days in a date delta."""
    months, days = delta.in_months_days()
    if months != 0:
        raise ToDaysError(months=months)
    return days


@dataclass(kw_only=True, slots=True)
class ToDaysError(Exception):
    months: int

    @override
    def __str__(self) -> str:
        return f"Date delta must not contain months; got {self.months}"


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


def to_local_plain(date_time: ZonedDateTime, /) -> PlainDateTime:
    """Convert a datetime to its local/plain variant."""
    return date_time.to_tz(LOCAL_TIME_ZONE_NAME).to_plain()


##


def to_nanos(delta: DateTimeDelta, /) -> int:
    """Compute the number of nanoseconds in a date-time delta."""
    months, days, _, _ = delta.in_months_days_secs_nanos()
    if months != 0:
        raise ToNanosError(months=months)
    return 24 * 60 * 60 * int(1e9) * days + delta.time_part().in_nanoseconds()


@dataclass(kw_only=True, slots=True)
class ToNanosError(Exception):
    months: int

    @override
    def __str__(self) -> str:
        return f"Date-time delta must not contain months; got {self.months}"


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
    "Freq",
    "FreqError",
    "FreqLike",
    "MeanDateTimeError",
    "MinMaxDateError",
    "ToDaysError",
    "ToNanosError",
    "WheneverLogRecord",
    "datetime_utc",
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
    "to_date",
    "to_date_time_delta",
    "to_days",
    "to_local_plain",
    "to_nanos",
    "to_zoned_date_time",
    "two_digit_year_month",
]
