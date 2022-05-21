import datetime as dt
from typing import Any
from typing import Optional
from typing import cast

from pandas import NaT
from pandas import Timestamp


TIMESTAMP_MIN = cast(Timestamp, Timestamp.min)
TIMESTAMP_MAX = cast(Timestamp, Timestamp.max)
Int64 = "Int64"
boolean = "boolean"
string = "string"


def timestamp_max_as_date() -> dt.date:
    """Get the maximum Timestamp as a date."""

    return _timestamp_minmax_as_date(TIMESTAMP_MAX, "floor")


def timestamp_min_as_date() -> dt.date:
    """Get the maximum Timestamp as a date."""

    return _timestamp_minmax_as_date(TIMESTAMP_MIN, "ceil")


def _timestamp_minmax_as_date(
    timestamp: Timestamp, method_name: str, /
) -> dt.date:
    """Get the maximum Timestamp as a date."""

    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("D"))
    date = timestamp_to_date(rounded)
    if date is None:
        raise ValueError(f"Invalid value: {date}")
    else:
        return date


def timestamp_max_as_datetime() -> dt.datetime:
    """Get the maximum Timestamp as a datetime."""

    return _timestamp_minmax_as_datetime(TIMESTAMP_MAX, "floor")


def timestamp_min_as_datetime() -> dt.datetime:
    """Get the maximum Timestamp as a datetime."""

    return _timestamp_minmax_as_datetime(TIMESTAMP_MIN, "ceil")


def _timestamp_minmax_as_datetime(
    timestamp: Timestamp, method_name: str, /
) -> dt.datetime:
    """Get the maximum Timestamp as a datetime."""

    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("us"))
    datetime = timestamp_to_datetime(rounded)
    if datetime is None:
        raise ValueError(f"Invalid value: {datetime}")
    else:
        return datetime


def timestamp_to_date(timestamp: Any, /) -> Optional[dt.date]:
    """Convert a timestamp to a date."""

    if (value := timestamp_to_datetime(timestamp)) is None:
        return None
    else:
        return value.date()


def timestamp_to_datetime(timestamp: Any, /) -> Optional[dt.datetime]:
    """Convert a timestamp to a datetime."""

    if isinstance(timestamp, Timestamp):
        return timestamp.to_pydatetime()
    elif timestamp is NaT:
        return None
    else:
        raise TypeError(f"Invalid type: {type(timestamp).__name__!r}")
