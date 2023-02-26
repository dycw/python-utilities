import datetime as dt
from typing import Any
from typing import Union
from typing import cast

from beartype import beartype
from pandas import DataFrame
from pandas import DatetimeTZDtype
from pandas import Index
from pandas import NaT
from pandas import RangeIndex
from pandas import Series
from pandas import Timestamp

from utilities.datetime import UTC

Int64 = "Int64"
boolean = "boolean"
category = "category"
string = "string"
datetime64nsutc = DatetimeTZDtype(tz=UTC)


@beartype
def check_range_index(obj: Union[Index, Series, DataFrame], /) -> None:
    """Check if a RangeIndex is the default one."""
    if isinstance(obj, Index):
        if not isinstance(obj, RangeIndex):
            msg = f"Invalid type: {obj=}"
            raise TypeError(msg)
        if obj.start != 0:
            msg = f"{obj=}"
            raise RangeIndexStartError(msg)
        if obj.step != 1:
            msg = f"{obj=}"
            raise RangeIndexStepError(msg)
        if obj.name is not None:
            msg = f"{obj=}"
            raise RangeIndexNameError(msg)
    else:
        try:
            check_range_index(obj.index)
        except (
            TypeError,
            RangeIndexStartError,
            RangeIndexStepError,
            RangeIndexNameError,
        ) as error:
            msg = f"{obj=}"
            if isinstance(obj, Series):
                raise SeriesRangeIndexError(msg) from error
            raise DataFrameRangeIndexError(msg) from error


class RangeIndexStartError(ValueError):
    """Raised when a RangeIndex start is not 0."""


class RangeIndexStepError(ValueError):
    """Raised when a RangeIndex step is not 1."""


class RangeIndexNameError(ValueError):
    """Raised when a RangeIndex name is not None."""


class SeriesRangeIndexError(ValueError):
    """Raised when Series does not have a standard RangeIndex."""


class DataFrameRangeIndexError(ValueError):
    """Raised when DataFrame does not have a standard RangeIndex."""


@beartype
def timestamp_to_date(timestamp: Any, /, *, warn: bool = True) -> dt.date:
    """Convert a timestamp to a date."""
    return timestamp_to_datetime(timestamp, warn=warn).date()


@beartype
def timestamp_to_datetime(
    timestamp: Any,
    /,
    *,
    warn: bool = True,
) -> dt.datetime:
    """Convert a timestamp to a datetime."""
    if timestamp is NaT:
        msg = f"{timestamp=}"
        raise TimestampIsNaTError(msg)
    datetime = cast(dt.datetime, timestamp.to_pydatetime(warn=warn))
    if datetime.tzinfo is None:
        return datetime.replace(tzinfo=UTC)
    return datetime


class TimestampIsNaTError(ValueError):
    """Raised when a NaT is received."""


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
