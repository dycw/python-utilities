import datetime as dt
from contextlib import suppress

from beartype import beartype

from utilities.re import extract_groups


@beartype
def ensure_date(date: dt.date | str, /) -> dt.date:
    """Ensure the object is a date."""

    return date if isinstance(date, dt.date) else parse_date(date)


@beartype
def ensure_datetime(datetime: dt.datetime | str, /) -> dt.datetime:
    """Ensure the object is a datetime."""

    if isinstance(datetime, dt.datetime):
        return datetime
    else:
        return parse_datetime(datetime)


@beartype
def ensure_time(time: dt.time | str, /) -> dt.time:
    """Ensure the object is a time."""

    return time if isinstance(time, dt.time) else parse_time(time)


@beartype
def ensure_timedelta(timedelta: dt.timedelta | str, /) -> dt.timedelta:
    """Ensure the object is a timedelta."""

    if isinstance(timedelta, dt.timedelta):
        return timedelta
    else:
        return parse_timedelta(timedelta)


@beartype
def parse_date(date: str, /) -> dt.date:
    """Parse a string into a date."""

    with suppress(ValueError):
        return dt.date.fromisoformat(date)
    with suppress(ValueError):
        return dt.datetime.strptime(date, "%Y%m%d").date()
    raise InvalidDate(date)


class InvalidDate(ValueError):
    ...


@beartype
def parse_datetime(datetime: str, /) -> dt.datetime:
    """Parse a string into a datetime."""

    with suppress(ValueError):
        return dt.datetime.fromisoformat(datetime)
    for format in [
        "%Y%m%d",
        "%Y%m%dT%H",
        "%Y%m%dT%H%M",
        "%Y%m%dT%H%M%S",
        "%Y%m%dT%H%M%S.%f",
    ]:
        with suppress(ValueError):
            return dt.datetime.strptime(datetime, format)
    raise InvalidDateTime(datetime)


class InvalidDateTime(ValueError):
    ...


@beartype
def parse_time(time: str) -> dt.time:
    """Parse a string into a time."""

    with suppress(ValueError):
        return dt.time.fromisoformat(time)
    for format in ["%H", "%H%M", "%H%M%S", "%H%M%S.%f"]:
        with suppress(ValueError):
            return dt.datetime.strptime(time, format).time()
    raise InvalidTime(time)


class InvalidTime(ValueError):
    ...


@beartype
def parse_timedelta(timedelta: str) -> dt.timedelta:
    """Parse a string into a timedelta."""

    for format in ["%H:%M:%S", "%H:%M:%S.%f"]:
        try:
            as_dt = dt.datetime.strptime(timedelta, format)
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
        days, tail = extract_groups(
            r"([-\d]+)\s*(?:days?)?,?\s*([\d:\.]+)", timedelta
        )
    except ValueError:
        raise InvalidTimedelta(timedelta)
    else:
        return dt.timedelta(days=int(days)) + parse_timedelta(tail)


class InvalidTimedelta(ValueError):
    ...


@beartype
def serialize_date(date: dt.date, /) -> str:
    """Serialize a date."""

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

    if (days := timedelta.days) != 0:
        tail = serialize_timedelta(timedelta - dt.timedelta(days=days))
        return f"d{days},{tail}"
    else:
        return str(timedelta)
