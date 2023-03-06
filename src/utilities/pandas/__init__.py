import datetime as dt
from functools import partial, reduce
from itertools import permutations
from typing import Any, Literal, NoReturn, Union, cast

from beartype import beartype
from pandas import NA, DataFrame, Index, NaT, RangeIndex, Series, Timestamp
from pandas.testing import assert_index_equal

from utilities.datetime import UTC
from utilities.errors import redirect_error
from utilities.numpy import has_dtype
from utilities.pandas.typing import (
    Int64,
    boolean,
    category,
    datetime64nshk,
    datetime64nsutc,
    string,
)

_ = (Int64, boolean, category, string, datetime64nsutc, datetime64nshk)


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
def redirect_to_empty_pandas_concat_error(error: ValueError, /) -> NoReturn:
    """Redirect to the `EmptyPandasConcatError`."""
    redirect_error(error, "No objects to concatenate", EmptyPandasConcatError)


class EmptyPandasConcatError(ValueError):
    """Raised when there are no objects to concatenate."""


@beartype
def series_max(*series: Series) -> Series:
    """Compute the maximum of a set of Series."""
    return reduce(partial(_series_minmax, kind="lower"), series)


@beartype
def series_min(*series: Series) -> Series:
    """Compute the minimum of a set of Series."""
    return reduce(partial(_series_minmax, kind="upper"), series)


@beartype
def _series_minmax(
    x: Series, y: Series, /, *, kind: Literal["lower", "upper"]
) -> Series:
    """Compute the minimum/maximum of a pair of Series."""
    assert_index_equal(x.index, y.index)
    if not (has_dtype(x, y.dtype) and has_dtype(y, x.dtype)):
        msg = f"{x=}, {y=}"
        raise DifferentDTypeError(msg)
    out = x.copy()
    for first, second in permutations([x, y]):
        i = first.notna() & second.isna()
        out.loc[i] = first.loc[i]
    i = x.notna() & y.notna()
    out.loc[i] = x.loc[i].clip(**{kind: y.loc[i]})
    out.loc[x.isna() & y.isna()] = NA
    return out


class DifferentDTypeError(ValueError):
    """Raised when two series have different dtypes."""


@beartype
def timestamp_to_date(timestamp: Any, /, *, warn: bool = True) -> dt.date:
    """Convert a timestamp to a date."""
    return timestamp_to_datetime(timestamp, warn=warn).date()


@beartype
def timestamp_to_datetime(timestamp: Any, /, *, warn: bool = True) -> dt.datetime:
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
def _timestamp_minmax_to_date(timestamp: Timestamp, method_name: str, /) -> dt.date:
    """Get the maximum Timestamp as a date."""
    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("D"))
    return timestamp_to_date(rounded)


TIMESTAMP_MIN_AS_DATE = _timestamp_minmax_to_date(Timestamp.min, "ceil")
TIMESTAMP_MAX_AS_DATE = _timestamp_minmax_to_date(Timestamp.max, "floor")


@beartype
def _timestamp_minmax_to_datetime(
    timestamp: Timestamp, method_name: str, /
) -> dt.datetime:
    """Get the maximum Timestamp as a datetime."""
    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("us"))
    return timestamp_to_datetime(rounded)


TIMESTAMP_MIN_AS_DATETIME = _timestamp_minmax_to_datetime(Timestamp.min, "ceil")
TIMESTAMP_MAX_AS_DATETIME = _timestamp_minmax_to_datetime(Timestamp.max, "floor")
