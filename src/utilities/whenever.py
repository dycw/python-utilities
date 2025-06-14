from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from functools import cache
from logging import LogRecord
from statistics import fmean
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
    SupportsFloat,
    assert_never,
    overload,
    override,
)

from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    TimeDelta,
    ZonedDateTime,
)

from utilities.math import sign
from utilities.platform import get_strftime
from utilities.re import ExtractGroupsError, extract_groups
from utilities.sentinel import Sentinel, sentinel
from utilities.types import MaybeStr
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


PLAIN_DATE_TIME_MIN = PlainDateTime(1, 1, 1)
PLAIN_DATE_TIME_MAX = PlainDateTime(
    9999, 12, 31, hour=23, minute=59, second=59, nanosecond=999999999
)
DATE_MIN = PLAIN_DATE_TIME_MIN.date()
DATE_MAX = PLAIN_DATE_TIME_MAX.date()
TIME_MIN = PLAIN_DATE_TIME_MIN.time()
TIME_MAX = PLAIN_DATE_TIME_MIN.time()
ZONED_DATE_TIME_MIN = PLAIN_DATE_TIME_MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX = PLAIN_DATE_TIME_MAX.assume_tz(UTC.key)


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


def format_compact(datetime: ZonedDateTime, /) -> str:
    """Convert a zoned datetime to the local time zone, then format."""
    py_datetime = datetime.round().to_tz(LOCAL_TIME_ZONE_NAME).to_plain().py_datetime()
    return py_datetime.strftime(get_strftime("%Y%m%dT%H%M%S"))


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


@dataclass(order=True, unsafe_hash=True, slots=True)
class Month:
    """Represents a month in time."""

    year: int
    month: int

    def __post_init__(self) -> None:
        try:
            _ = Date(self.year, self.month, 1)
        except ValueError:
            raise _MonthInvalidError(year=self.year, month=self.month) from None

    @override
    def __repr__(self) -> str:
        return self.format_common_iso()

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
    def ensure(cls, obj: MonthLike, /) -> Month:
        """Ensure the object is a month."""
        match obj:
            case Month() as month:
                return month
            case str() as text:
                return cls.parse_common_iso(text)
            case _ as never:
                assert_never(never)

    def format_common_iso(self) -> str:
        return f"{self.year:04}-{self.month:02}"

    @classmethod
    def from_date(cls, date: Date, /) -> Self:
        return cls(year=date.year, month=date.month)

    @classmethod
    def parse_common_iso(cls, text: str, /) -> Self:
        try:
            year, month = extract_groups(r"^(\d{2,4})[\-\. ]?(\d{2})$", text)
        except ExtractGroupsError:
            raise _MonthParseCommonISOError(text=text) from None
        return cls(year=cls._parse_year(year), month=int(month))

    def to_date(self, /, *, day: int = 1) -> Date:
        return Date(self.year, self.month, day)

    @classmethod
    def _parse_year(cls, year: str, /) -> int:
        match len(year):
            case 4:
                return int(year)
            case 2:
                min_year = DATE_TWO_DIGIT_YEAR_MIN.year
                max_year = DATE_TWO_DIGIT_YEAR_MAX.year
                years = range(min_year, max_year + 1)
                (result,) = (y for y in years if y % 100 == int(year))
                return result
            case _:
                raise _MonthParseCommonISOError(text=year) from None


@dataclass(kw_only=True, slots=True)
class MonthError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _MonthInvalidError(MonthError):
    year: int
    month: int

    @override
    def __str__(self) -> str:
        return f"Invalid year and month: {self.year}, {self.month}"


@dataclass(kw_only=True, slots=True)
class _MonthParseCommonISOError(MonthError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse month; got {self.text!r}"


type DateOrMonth = Date | Month
type MonthLike = MaybeStr[Month]
MONTH_MIN = Month.from_date(DATE_MIN)
MONTH_MAX = Month.from_date(DATE_MAX)


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
    "DATE_MAX",
    "DATE_MIN",
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
    "MONTH_MAX",
    "MONTH_MIN",
    "NOW_LOCAL",
    "PLAIN_DATE_TIME_MAX",
    "PLAIN_DATE_TIME_MIN",
    "SECOND",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "TIME_MAX",
    "TIME_MIN",
    "TODAY_LOCAL",
    "TODAY_UTC",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
    "DateOrMonth",
    "MeanDateTimeError",
    "MinMaxDateError",
    "Month",
    "MonthError",
    "MonthLike",
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
    "to_nanos",
    "to_zoned_date_time",
]
