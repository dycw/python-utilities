from __future__ import annotations

import datetime as dt
from collections.abc import Hashable, Mapping, Sequence
from functools import partial, reduce
from itertools import permutations
from math import isclose
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TypeAlias, TypeVar, cast

from numpy import where
from pandas import (
    NA,
    BooleanDtype,
    CategoricalDtype,
    DataFrame,
    DatetimeTZDtype,
    Index,
    Int64Dtype,
    NaT,
    RangeIndex,
    Series,
    StringDtype,
    Timestamp,
)
from pandas.testing import assert_frame_equal, assert_index_equal

from utilities.datetime import UTC
from utilities.errors import redirect_error
from utilities.numpy import NDArray1, datetime64ns, has_dtype
from utilities.zoneinfo import HONG_KONG

if TYPE_CHECKING:  # pragma: no cover
    IndexA: TypeAlias = Index[Any]
    IndexB: TypeAlias = Index[bool]
    IndexBn: TypeAlias = Index[BooleanDtype]
    IndexC: TypeAlias = Index[CategoricalDtype]
    IndexD: TypeAlias = Index[dt.datetime]
    IndexDhk: TypeAlias = Index[DatetimeTZDtype]
    IndexDutc: TypeAlias = Index[DatetimeTZDtype]
    IndexF: TypeAlias = Index[float]
    IndexI: TypeAlias = Index[int]
    IndexI64: TypeAlias = Index[Int64Dtype]
    IndexS: TypeAlias = Index[StringDtype]

    SeriesA: TypeAlias = Series[Any]
    SeriesB: TypeAlias = Series[bool]
    SeriesBn: TypeAlias = Series[BooleanDtype]
    SeriesC: TypeAlias = Series[CategoricalDtype]
    SeriesD: TypeAlias = Series[dt.datetime]
    SeriesDhk: TypeAlias = Series[DatetimeTZDtype]
    SeriesDutc: TypeAlias = Series[DatetimeTZDtype]
    SeriesF: TypeAlias = Series[float]
    SeriesI: TypeAlias = Series[int]
    SeriesI64: TypeAlias = Series[Int64Dtype]
    SeriesS: TypeAlias = Series[StringDtype]


Int64 = "Int64"
boolean = "boolean"
category = "category"
string = "string"
datetime64nsutc = DatetimeTZDtype(tz=UTC)
datetime64nshk = DatetimeTZDtype(tz=HONG_KONG)


_Index = TypeVar("_Index", bound=Index)


def astype(df: DataFrame, dtype: Any, /) -> DataFrame:
    """Wrapper around `.astype`."""
    return cast(Any, df).astype(dtype)


def check_dataframe(
    df: DataFrame,
    /,
    *,
    columns: Sequence[Hashable] | None = None,
    dtypes: Mapping[Hashable, Any] | None = None,
    length: int | tuple[int, float] | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    sorted: Hashable | Sequence[Hashable] | None = None,  # noqa: A002
    unique: Hashable | Sequence[Hashable] | None = None,
) -> None:
    """Check the properties of a DataFrame."""
    check_range_index(df.index)
    if df.columns.name is not None:
        msg = f"{df=}"
        raise DataFrameColumnsNameError(msg)
    if df.columns.duplicated().any():
        msg = f"{df=}"
        raise DataFrameColumnsDuplicatedError(msg)
    if (columns is not None) and (list(df.columns) != columns):
        msg = f"{df=}, {columns=}"
        raise DataFrameColumnsError(msg)
    if (dtypes is not None) and (dict(df.dtypes) != dict(dtypes)):
        msg = f"{df=}, {dtypes=}"
        raise DataFrameDTypesError(msg)
    if length is not None:
        _check_dataframe_length(df, length)
    if (min_length is not None) and (len(df) < min_length):
        msg = f"{df=}, {min_length=}"
        raise DataFrameMinLengthError(msg)
    if (max_length is not None) and (len(df) > max_length):
        msg = f"{df=}, {max_length=}"
        raise DataFrameMaxLengthError(msg)
    if sorted is not None:
        df_sorted: DataFrame = df.sort_values(by=sorted).reset_index(drop=True)  # type: ignore
        try:
            assert_frame_equal(df, df_sorted)
        except AssertionError:
            msg = f"{df=}, {sorted=}"
            raise DataFrameSortedError(msg) from None
    if (unique is not None) and df.duplicated(subset=unique).any():
        msg = f"{df=}, {unique=}"
        raise DataFrameUniqueError(msg)


class DataFrameColumnsNameError(Exception):
    """Raised when a DataFrame's columns' index name is not None."""


class DataFrameColumnsDuplicatedError(Exception):
    """Raised when a DataFrame's columns has duplicated values."""


class DataFrameColumnsError(Exception):
    """Raised when a DataFrame has the incorrect columns."""


class DataFrameDTypesError(Exception):
    """Raised when a DataFrame has the incorrect dtypes."""


class DataFrameMinLengthError(Exception):
    """Raised when a DataFrame does not reach the minimum length."""


class DataFrameMaxLengthError(Exception):
    """Raised when a DataFrame exceeds the maximum length."""


class DataFrameSortedError(Exception):
    """Raised when a DataFrame has non-sorted values."""


class DataFrameUniqueError(Exception):
    """Raised when a DataFrame has non-unique values."""


def _check_dataframe_length(df: DataFrame, length: int | tuple[int, float], /) -> None:
    if isinstance(length, int) and (len(df) != length):
        msg = f"{df=}, {length=}"
        raise DataFrameLengthError(msg)
    if isinstance(length, tuple):
        length_int, rel_tol = length
        if not isclose(len(df), length_int, rel_tol=rel_tol):
            msg = f"{df=}, {length=}"
            raise DataFrameLengthError(msg)


class DataFrameLengthError(Exception):
    """Raised when a DataFrame has the incorrect length."""


def check_range_index(obj: IndexA | SeriesA | DataFrame, /) -> None:
    """Check if a RangeIndex is the default one."""
    if isinstance(obj, Index):
        if not isinstance(obj, RangeIndex):
            msg = f"Invalid type: {obj=}"
            raise TypeError(msg)
        if obj.start != 0:  # type: ignore
            msg = f"{obj=}"
            raise RangeIndexStartError(msg)
        if obj.step != 1:  # type: ignore
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


class RangeIndexStartError(Exception):
    """Raised when a RangeIndex start is not 0."""


class RangeIndexStepError(Exception):
    """Raised when a RangeIndex step is not 1."""


class RangeIndexNameError(Exception):
    """Raised when a RangeIndex name is not None."""


class SeriesRangeIndexError(Exception):
    """Raised when Series does not have a standard RangeIndex."""


class DataFrameRangeIndexError(Exception):
    """Raised when DataFrame does not have a standard RangeIndex."""


def redirect_to_empty_pandas_concat_error(error: ValueError, /) -> NoReturn:
    """Redirect to the `EmptyPandasConcatError`."""
    redirect_error(error, "No objects to concatenate", EmptyPandasConcatError)


class EmptyPandasConcatError(Exception):
    """Raised when there are no objects to concatenate."""


def rename_index(index: _Index, name: Hashable, /) -> _Index:
    """Wrapper around `.rename`."""
    return cast(_Index, index.rename(name))


def series_max(*series: SeriesA) -> SeriesA:
    """Compute the maximum of a set of Series."""
    return reduce(partial(_series_minmax, kind="lower"), series)


def series_min(*series: SeriesA) -> SeriesA:
    """Compute the minimum of a set of Series."""
    return reduce(partial(_series_minmax, kind="upper"), series)


def _series_minmax(
    x: SeriesA, y: SeriesA, /, *, kind: Literal["lower", "upper"]
) -> SeriesA:
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
    out.loc[i] = x.loc[i].clip(**{kind: cast(Any, y.loc[i])})
    out.loc[x.isna() & y.isna()] = NA
    return out


class DifferentDTypeError(Exception):
    """Raised when two series have different dtypes."""


def sort_index(index: _Index, /) -> _Index:
    return cast(_Index, index.sort_values())


def timestamp_to_date(timestamp: Any, /, *, warn: bool = True) -> dt.date:
    """Convert a timestamp to a date."""
    return timestamp_to_datetime(timestamp, warn=warn).date()


def timestamp_to_datetime(timestamp: Any, /, *, warn: bool = True) -> dt.datetime:
    """Convert a timestamp to a datetime."""
    if timestamp is NaT:
        msg = f"{timestamp=}"
        raise TimestampIsNaTError(msg)
    datetime = cast(dt.datetime, timestamp.to_pydatetime(warn=warn))
    if datetime.tzinfo is None:
        return datetime.replace(tzinfo=UTC)
    return datetime


class TimestampIsNaTError(Exception):
    """Raised when a NaT is received."""


def _timestamp_minmax_to_date(timestamp: Timestamp, method_name: str, /) -> dt.date:
    """Get the maximum Timestamp as a date."""
    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("D"))
    return timestamp_to_date(rounded)


TIMESTAMP_MIN_AS_DATE = _timestamp_minmax_to_date(Timestamp.min, "ceil")
TIMESTAMP_MAX_AS_DATE = _timestamp_minmax_to_date(Timestamp.max, "floor")


def _timestamp_minmax_to_datetime(
    timestamp: Timestamp, method_name: str, /
) -> dt.datetime:
    """Get the maximum Timestamp as a datetime."""
    method = getattr(timestamp, method_name)
    rounded = cast(Timestamp, method("us"))
    return timestamp_to_datetime(rounded)


TIMESTAMP_MIN_AS_DATETIME = _timestamp_minmax_to_datetime(Timestamp.min, "ceil")
TIMESTAMP_MAX_AS_DATETIME = _timestamp_minmax_to_datetime(Timestamp.max, "floor")


def to_numpy(series: SeriesA, /) -> NDArray1:
    """Convert a series into a 1-dimensional `ndarray`."""
    if has_dtype(series, (bool, datetime64ns, int, float)):
        return series.to_numpy()
    if has_dtype(series, (boolean, Int64, string)):
        return where(
            series.notna(), series.to_numpy(dtype=object), cast(Any, None)
        ).astype(object)
    msg = f"Invalid dtype: {series=}"  # pragma: no cover
    raise TypeError(msg)  # pragma: no cover


__all__ = [
    "astype",
    "boolean",
    "category",
    "check_dataframe",
    "check_range_index",
    "DataFrameColumnsDuplicatedError",
    "DataFrameColumnsError",
    "DataFrameColumnsNameError",
    "DataFrameDTypesError",
    "DataFrameLengthError",
    "DataFrameMaxLengthError",
    "DataFrameMinLengthError",
    "DataFrameRangeIndexError",
    "DataFrameSortedError",
    "DataFrameUniqueError",
    "datetime64nshk",
    "datetime64nsutc",
    "DifferentDTypeError",
    "EmptyPandasConcatError",
    "Int64",
    "RangeIndexNameError",
    "RangeIndexStartError",
    "RangeIndexStepError",
    "redirect_to_empty_pandas_concat_error",
    "rename_index",
    "series_max",
    "series_min",
    "SeriesRangeIndexError",
    "sort_index",
    "string",
    "TIMESTAMP_MAX_AS_DATE",
    "TIMESTAMP_MAX_AS_DATETIME",
    "TIMESTAMP_MIN_AS_DATE",
    "TIMESTAMP_MIN_AS_DATETIME",
    "timestamp_to_date",
    "timestamp_to_datetime",
    "TimestampIsNaTError",
    "to_numpy",
]
