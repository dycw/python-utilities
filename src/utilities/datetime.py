from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from re import sub
from typing import TYPE_CHECKING, cast

from typing_extensions import Never, assert_never, override

from utilities.errors import ImpossibleCaseError
from utilities.platform import SYSTEM, System
from utilities.re import ExtractGroupsError, extract_groups
from utilities.zoneinfo import HONG_KONG, TOKYO

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import Duration

UTC = dt.timezone.utc
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
    date: dt.date, /, *, time: dt.time | None = None, tzinfo: dt.tzinfo | None = UTC
) -> dt.datetime:
    """Expand a date into a datetime."""
    time_use = dt.time(0) if time is None else time
    return dt.datetime.combine(date, time_use, tzinfo=tzinfo)


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


def ensure_date(date: dt.date | str, /) -> dt.date:
    """Ensure the object is a date."""
    return date if isinstance(date, dt.date) else parse_date(date)


def ensure_datetime(
    datetime: dt.datetime | str, /, *, tzinfo: dt.tzinfo = UTC
) -> dt.datetime:
    """Ensure the object is a datetime."""
    if isinstance(datetime, dt.datetime):
        return datetime
    return parse_datetime(datetime, tzinfo=tzinfo)


def ensure_time(time: dt.time | str, /) -> dt.time:
    """Ensure the object is a time."""
    return time if isinstance(time, dt.time) else parse_time(time)


def ensure_timedelta(timedelta: dt.timedelta | str, /) -> dt.timedelta:
    """Ensure the object is a timedelta."""
    if isinstance(timedelta, dt.timedelta):
        return timedelta
    return parse_timedelta(timedelta)


def get_now(*, tz: dt.tzinfo | None = UTC) -> dt.datetime:
    """Get the current, timezone-aware time."""
    return dt.datetime.now(tz=tz)


NOW_UTC = get_now()


def get_now_hk() -> dt.datetime:
    """Get the current time in Hong Kong."""
    return dt.datetime.now(tz=HONG_KONG)


NOW_HK = get_now_hk()


def get_now_tokyo() -> dt.datetime:
    """Get the current time in Tokyo."""
    return dt.datetime.now(tz=TOKYO)


NOW_TOKYO = get_now_tokyo()


def get_today(*, tz: dt.tzinfo | None = UTC) -> dt.date:
    """Get the current, timezone-aware date."""
    return get_now(tz=tz).date()


TODAY_UTC = get_today()


def get_today_hk() -> dt.date:
    """Get the current date in Hong Kong."""
    return get_now_hk().date()


TODAY_HK = get_today_hk()


def get_today_tokyo() -> dt.date:
    """Get the current date in Tokyo."""
    return get_now_tokyo().date()


TODAY_TOKYO = get_today_tokyo()


def is_equal_mod_tz(x: dt.datetime, y: dt.datetime, /) -> bool:
    """Check if x == y, modulo timezone."""
    x_aware, y_aware = x.tzinfo is not None, y.tzinfo is not None
    match x_aware, y_aware:
        case (False, False) | (True, True):
            return x == y
        case True, False:
            return x.astimezone(UTC).replace(tzinfo=None) == y
        case False, True:
            return x == y.astimezone(UTC).replace(tzinfo=None)
        case _ as never:
            assert_never(cast(Never, never))


def is_weekday(date: dt.date, /) -> bool:
    """Check if a date is a weekday."""
    friday = 5
    return date.isoweekday() <= friday


def local_timezone() -> dt.tzinfo:
    """Get the local timezone."""
    tz = get_now(tz=None).astimezone().tzinfo
    if tz is None:  # pragma: no cover
        raise ImpossibleCaseError(case=[])
    return tz


def maybe_sub_pct_y(text: str, /) -> str:
    """Substitute the `%Y' token with '%4Y' if necessary."""
    match SYSTEM:
        case System.windows:  # pragma: os-ne-windows
            return text
        case System.mac:  # pragma: os-ne-macos
            return text
        case System.linux:  # pragma: os-ne-linux
            return sub("%Y", "%4Y", text)
        case _ as never:  # type: ignore[]
            assert_never(never)


def parse_date(date: str, /, *, tzinfo: dt.tzinfo = UTC) -> dt.date:
    """Parse a string into a date."""
    with suppress(ValueError):
        return dt.date.fromisoformat(date)
    for fmt in ["%Y%m%d", "%Y %m %d", "%d%b%Y", "%d %b %Y"]:
        with suppress(ValueError):  # pragma: version-ge-311
            return dt.datetime.strptime(date, fmt).replace(tzinfo=tzinfo).date()
    raise ParseDateError(date)


class ParseDateError(Exception): ...


def parse_datetime(datetime: str, /, *, tzinfo: dt.tzinfo = UTC) -> dt.datetime:
    """Parse a string into a datetime."""
    with suppress(ValueError):
        return dt.datetime.fromisoformat(datetime).replace(tzinfo=tzinfo)
    for fmt in [
        "%Y%m%d",
        "%Y%m%dT%H",
        "%Y%m%dT%H%M",
        "%Y%m%dT%H%M%S",
        "%Y%m%dT%H%M%S.%f",
    ]:
        with suppress(ValueError):  # pragma: version-ge-311
            return dt.datetime.strptime(datetime, fmt).replace(tzinfo=tzinfo)
    for fmt in ["%Y-%m-%d %H:%M:%S.%f%z", "%Y%m%dT%H%M%S.%f%z"]:
        with suppress(ValueError):  # pragma: version-ge-311
            return dt.datetime.strptime(datetime, fmt)  # noqa: DTZ007
    raise ParseDateTimeError(datetime)


class ParseDateTimeError(Exception): ...


def parse_time(time: str, /) -> dt.time:
    """Parse a string into a time."""
    with suppress(ValueError):
        return dt.time.fromisoformat(time)
    for fmt in ["%H", "%H%M", "%H%M%S", "%H%M%S.%f"]:  # pragma: version-ge-311
        with suppress(ValueError):
            return dt.datetime.strptime(time, fmt).replace(tzinfo=UTC).time()
    raise ParseTimeError(time)


class ParseTimeError(Exception): ...


def parse_timedelta(timedelta: str, /) -> dt.timedelta:
    """Parse a string into a timedelta."""

    def try_parse(fmt: str, /) -> dt.datetime | None:
        try:
            return dt.datetime.strptime(timedelta, fmt).replace(tzinfo=UTC)
        except ValueError:
            return None

    try:
        as_dt = next(
            parsed
            for fmt in ("%H:%M:%S", "%H:%M:%S.%f")
            if (parsed := try_parse(fmt)) is not None
        )
    except StopIteration:
        pass
    else:
        return dt.timedelta(
            hours=as_dt.hour,
            minutes=as_dt.minute,
            seconds=as_dt.second,
            microseconds=as_dt.microsecond,
        )
    try:
        days, tail = extract_groups(r"([-\d]+)\s*(?:days?)?,?\s*([\d:\.]+)", timedelta)
    except ExtractGroupsError as error:
        raise ParseTimedeltaError(pattern=error.pattern, timedelta=timedelta) from None
    return dt.timedelta(days=int(days)) + parse_timedelta(tail)


@dataclass(kw_only=True, slots=True)
class ParseTimedeltaError(Exception):
    pattern: str
    timedelta: str

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.timedelta}."


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


def serialize_date(date: dt.date, /) -> str:
    """Serialize a date."""
    if isinstance(date, dt.datetime):
        return serialize_date(date.date())
    return date.isoformat()


def serialize_datetime(datetime: dt.datetime, /) -> str:
    """Serialize a datetime."""
    return datetime.isoformat()


def serialize_time(time: dt.time, /) -> str:
    """Serialize a time."""
    return time.isoformat()


def serialize_timedelta(timedelta: dt.timedelta, /) -> str:
    """Serialize a timedelta."""
    if (days := timedelta.days) == 0:
        return str(timedelta)
    tail = serialize_timedelta(timedelta - dt.timedelta(days=days))
    return f"d{days},{tail}"


def yield_weekdays(
    *, start: dt.date | None = None, end: dt.date | None = None, days: int | None = None
) -> Iterator[dt.date]:
    """Yield the weekdays in a range."""
    if (start is not None) and (end is not None) and (days is None):
        date = round_to_next_weekday(start)
        while date <= end:
            yield date
            date = round_to_next_weekday(date + dt.timedelta(days=1))
    elif (start is not None) and (end is None) and (days is not None):
        date = round_to_next_weekday(start)
        for _ in range(days):
            yield date
            date = round_to_next_weekday(date + dt.timedelta(days=1))
    elif (start is None) and (end is not None) and (days is not None):
        date = round_to_prev_weekday(end)
        for _ in range(days):
            yield date
            date = round_to_prev_weekday(date - dt.timedelta(days=1))
    else:
        msg = f"Invalid arguments: {start=}, {end=}, {days=}"
        raise YieldWeekdaysError(msg)


class YieldWeekdaysError(Exception): ...


__all__ = [
    "AddWeekdaysError",
    "EPOCH_UTC",
    "NOW_HK",
    "NOW_TOKYO",
    "NOW_UTC",
    "ParseDateError",
    "ParseDateTimeError",
    "ParseTimeError",
    "ParseTimedeltaError",
    "TODAY_HK",
    "TODAY_TOKYO",
    "TODAY_UTC",
    "UTC",
    "YieldWeekdaysError",
    "add_weekdays",
    "date_to_datetime",
    "duration_to_float",
    "duration_to_timedelta",
    "ensure_date",
    "ensure_datetime",
    "ensure_time",
    "ensure_timedelta",
    "get_now",
    "get_now_hk",
    "get_now_tokyo",
    "get_today",
    "get_today_hk",
    "get_today_tokyo",
    "is_weekday",
    "local_timezone",
    "maybe_sub_pct_y",
    "parse_date",
    "parse_datetime",
    "parse_time",
    "parse_timedelta",
    "round_to_next_weekday",
    "round_to_prev_weekday",
    "serialize_date",
    "serialize_datetime",
    "serialize_time",
    "serialize_timedelta",
    "yield_weekdays",
]
