import datetime as dt
from collections.abc import Iterator
from contextlib import suppress
from datetime import tzinfo
from typing import Optional, Union

from beartype import beartype

from utilities.re import extract_groups

UTC = dt.timezone.utc
EPOCH_UTC = dt.datetime.fromtimestamp(0, tz=UTC)


@beartype
def add_weekdays(date: dt.date, /, *, n: int = 1) -> dt.date:
    """Add a number of a weekdays to a given date.

    If the initial date is a weekend, then moving to the adjacent weekday
    counts as 1 move.
    """
    if n == 0 and not is_weekday(date):
        raise IsWeekendError(date)
    if n >= 1:
        for _ in range(n):
            date = round_to_next_weekday(date + dt.timedelta(days=1))
    elif n <= -1:
        for _ in range(-n):
            date = round_to_prev_weekday(date - dt.timedelta(days=1))
    return date


class IsWeekendError(ValueError):
    """Raised when 0 days is added to a weekend."""


@beartype
def date_to_datetime(
    date: dt.date, /, *, time: dt.time = dt.time(0), tzinfo: tzinfo = UTC
) -> dt.datetime:
    """Expand a date into a datetime."""
    return dt.datetime.combine(date, time, tzinfo=tzinfo)


@beartype
def ensure_date(date: Union[dt.date, str], /) -> dt.date:
    """Ensure the object is a date."""
    return date if isinstance(date, dt.date) else parse_date(date)


@beartype
def ensure_datetime(datetime: Union[dt.datetime, str], /) -> dt.datetime:
    """Ensure the object is a datetime."""
    if isinstance(datetime, dt.datetime):
        return datetime
    return parse_datetime(datetime)


@beartype
def ensure_time(time: Union[dt.time, str], /) -> dt.time:
    """Ensure the object is a time."""
    return time if isinstance(time, dt.time) else parse_time(time)


@beartype
def ensure_timedelta(timedelta: Union[dt.timedelta, str], /) -> dt.timedelta:
    """Ensure the object is a timedelta."""
    if isinstance(timedelta, dt.timedelta):
        return timedelta
    return parse_timedelta(timedelta)


@beartype
def is_weekday(date: dt.date, /) -> bool:
    """Check if a date is a weekday."""
    friday = 5
    return date.isoweekday() <= friday


@beartype
def local_timezone() -> tzinfo:
    """Get the local timezone."""
    tz = dt.datetime.now().astimezone().tzinfo  # noqa: DTZ005
    if tz is None:  # pragma: no cover
        msg = f"{tz=}"
        raise LocalTimeZoneError(msg)
    return tz


class LocalTimeZoneError(ValueError):
    """Raised when the local timezone cannot be found."""


@beartype
def parse_date(date: str, /) -> dt.date:
    """Parse a string into a date."""
    with suppress(ValueError):
        return dt.date.fromisoformat(date)
    with suppress(ValueError):  # pragma: py-ge-311
        return dt.datetime.strptime(date, "%Y%m%d").replace(tzinfo=UTC).date()
    raise ParseDateError(date)


class ParseDateError(ValueError):
    """Raised when a `dt.date` cannot be parsed."""


@beartype
def parse_datetime(datetime: str, /) -> dt.datetime:
    """Parse a string into a datetime."""
    with suppress(ValueError):
        return dt.datetime.fromisoformat(datetime).replace(tzinfo=UTC)
    for fmt in [
        "%Y%m%d",
        "%Y%m%dT%H",
        "%Y%m%dT%H%M",
        "%Y%m%dT%H%M%S",
        "%Y%m%dT%H%M%S.%f",
    ]:
        with suppress(ValueError):  # pragma: py-ge-311
            return dt.datetime.strptime(datetime, fmt).replace(tzinfo=dt.timezone.utc)
    for fmt in ["%Y-%m-%d %H:%M:%S.%f%z", "%Y%m%dT%H%M%S.%f%z"]:
        with suppress(ValueError):  # pragma: py-ge-311
            return dt.datetime.strptime(datetime, fmt)  # noqa: DTZ007
    raise ParseDateTimeError(datetime)


class ParseDateTimeError(ValueError):
    """Raised when a `dt.datetime` cannot be parsed."""


@beartype
def parse_time(time: str) -> dt.time:
    """Parse a string into a time."""
    with suppress(ValueError):
        return dt.time.fromisoformat(time)
    for fmt in ["%H", "%H%M", "%H%M%S", "%H%M%S.%f"]:  # pragma: py-ge-311
        with suppress(ValueError):
            return dt.datetime.strptime(time, fmt).replace(tzinfo=UTC).time()
    raise ParseTimeError(time)


class ParseTimeError(ValueError):
    """Raised when a `dt.time` cannot be parsed."""


@beartype
def parse_timedelta(timedelta: str) -> dt.timedelta:
    """Parse a string into a timedelta."""
    for fmt in ["%H:%M:%S", "%H:%M:%S.%f"]:
        try:
            as_dt = dt.datetime.strptime(timedelta, fmt).replace(tzinfo=UTC)
        except ValueError:
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
    except ValueError:
        raise TimedeltaError(timedelta) from None
    else:
        return dt.timedelta(days=int(days)) + parse_timedelta(tail)


class TimedeltaError(ValueError):
    """Raised when a `dt.timedelta` cannot be parsed."""


@beartype
def round_to_next_weekday(date: dt.date, /) -> dt.date:
    """Round a date to the next weekday."""
    return _round_to_weekday(date, is_next=True)


@beartype
def round_to_prev_weekday(date: dt.date, /) -> dt.date:
    """Round a date to the previous weekday."""
    return _round_to_weekday(date, is_next=False)


@beartype
def _round_to_weekday(date: dt.date, /, *, is_next: bool) -> dt.date:
    """Round a date to the previous weekday."""
    n = 1 if is_next else -1
    while not is_weekday(date):
        date = add_weekdays(date, n=n)
    return date


@beartype
def serialize_date(date: dt.date, /) -> str:
    """Serialize a date."""
    if isinstance(date, dt.datetime):
        return serialize_date(date.date())
    return date.isoformat()


@beartype
def serialize_datetime(datetime: dt.datetime, /) -> str:
    """Serialize a datetime."""
    return datetime.isoformat()


@beartype
def serialize_time(time: dt.time, /) -> str:
    """Serialize a time."""
    return time.isoformat()


@beartype
def serialize_timedelta(timedelta: dt.timedelta, /) -> str:
    """Serialize a timedelta."""
    if (days := timedelta.days) == 0:
        return str(timedelta)
    tail = serialize_timedelta(timedelta - dt.timedelta(days=days))
    return f"d{days},{tail}"


@beartype
def yield_weekdays(
    *,
    start: Optional[dt.date] = None,
    end: Optional[dt.date] = None,
    days: Optional[int] = None,
) -> Iterator[dt.date]:
    """Yield the weekdays in a range."""
    if (start is not None) and (end is not None) and (days is None):
        date = round_to_next_weekday(start)
        while date < end:
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
        msg = f"{start=}, {end=}, {days=}"
        raise CallYieldWeekdaysError(msg)


class CallYieldWeekdaysError(ValueError):
    """Raised when an invalid call to `yield_weekdays` is made."""
