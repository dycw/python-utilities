import datetime as dt
from typing import Any, cast

from beartype import beartype
from pandas import DatetimeTZDtype, NaT, Timestamp

from utilities.datetime import UTC

Int64 = "Int64"
boolean = "boolean"
string = "string"
datetime64nsutc = DatetimeTZDtype(tz=UTC)


@beartype
def timestamp_to_date(timestamp: Any, /) -> dt.date:
    """Convert a timestamp to a date."""
    return timestamp_to_datetime(timestamp).date()


@beartype
def timestamp_to_datetime(timestamp: Any, /) -> dt.datetime:
    """Convert a timestamp to a datetime."""
    if timestamp is NaT:
        msg = f"Invalid value: {timestamp}"
        raise ValueError(msg)
    datetime = cast(dt.datetime, timestamp.to_pydatetime())
    if datetime.tzinfo is None:
        return datetime.replace(tzinfo=UTC)
    return datetime


@beartype
def _timestamp_minmax_to_date(
    timestamp: Timestamp,
    method_name: str,
    /,
) -> dt.date:
    """Get the maximum Timestamp as a date."""
    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("D"))
    return timestamp_to_date(rounded)


TIMESTAMP_MIN_AS_DATE = _timestamp_minmax_to_date(Timestamp.min, "ceil")
TIMESTAMP_MAX_AS_DATE = _timestamp_minmax_to_date(Timestamp.max, "floor")


@beartype
def _timestamp_minmax_to_datetime(
    timestamp: Timestamp,
    method_name: str,
    /,
) -> dt.datetime:
    """Get the maximum Timestamp as a datetime."""
    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("us"))
    return timestamp_to_datetime(rounded)


TIMESTAMP_MIN_AS_DATETIME = _timestamp_minmax_to_datetime(Timestamp.min, "ceil")
TIMESTAMP_MAX_AS_DATETIME = _timestamp_minmax_to_datetime(
    Timestamp.max,
    "floor",
)
