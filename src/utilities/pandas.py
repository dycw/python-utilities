from __future__ import annotations

import datetime as dt
from collections.abc import Hashable, Mapping, Sequence
from functools import partial, reduce
from itertools import permutations
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
    IndexA: TypeAlias = Index[Any]  # type: ignore
    IndexB: TypeAlias = Index[bool]  # type: ignore
    IndexBn: TypeAlias = Index[BooleanDtype]  # type: ignore
    IndexC: TypeAlias = Index[CategoricalDtype]  # type: ignore
    IndexD: TypeAlias = Index[dt.datetime]  # type: ignore
    IndexDhk: TypeAlias = Index[DatetimeTZDtype]  # type: ignore
    IndexDutc: TypeAlias = Index[DatetimeTZDtype]  # type: ignore
    IndexF: TypeAlias = Index[float]  # type: ignore
    IndexI: TypeAlias = Index[int]  # type: ignore
    IndexI64: TypeAlias = Index[Int64Dtype]  # type: ignore
    IndexS: TypeAlias = Index[StringDtype]  # type: ignore

    SeriesA: TypeAlias = Series[Any]  # type: ignore
    SeriesB: TypeAlias = Series[bool]  # type: ignore
    SeriesBn: TypeAlias = Series[BooleanDtype]  # type: ignore
    SeriesC: TypeAlias = Series[CategoricalDtype]  # type: ignore
    SeriesD: TypeAlias = Series[dt.datetime]  # type: ignore
    SeriesDhk: TypeAlias = Series[DatetimeTZDtype]  # type: ignore
    SeriesDutc: TypeAlias = Series[DatetimeTZDtype]  # type: ignore
    SeriesF: TypeAlias = Series[float]  # type: ignore
    SeriesI: TypeAlias = Series[int]  # type: ignore
    SeriesI64: TypeAlias = Series[Int64Dtype]  # type: ignore
    SeriesS: TypeAlias = Series[StringDtype]  # type: ignore


Int64 = "Int64"
boolean = "boolean"
category = "category"
string = "string"
datetime64nsutc = DatetimeTZDtype(tz=UTC)
datetime64nshk = DatetimeTZDtype(tz=HONG_KONG)


_Index = TypeVar("_Index", bound=Index)


def check_dataframe(
    df: DataFrame,
    /,
    *,
    columns: Sequence[Hashable] | None = None,
    dtypes: Mapping[Hashable, Any] | None = None,
    length: int | None = None,
    sorted: Hashable | Sequence[Hashable] | None = None,  # noqa: A002
    unique: Hashable | Sequence[Hashable] | None = None,
) -> None:
    """Check if the properties of a DataFrame."""
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
    if (length is not None) and (len(df) != length):
        msg = f"{df=}, {length=}"
        raise DataFrameLengthError(msg)
    if sorted is not None:
        df_sorted = df.sort_values(by=sorted).reset_index(drop=True)
        try:
            assert_frame_equal(df, df_sorted)
        except AssertionError:
            msg = f"{df=}, {sorted=}"
            raise DataFrameSortedError(msg) from None
    if (unique is not None) and df.duplicated(subset=unique).any():
        msg = f"{df=}, {unique=}"
        raise DataFrameUniqueError(msg)


class DataFrameColumnsNameError(ValueError):
    """Raised when a DataFrame's columns' index name is not None."""


class DataFrameColumnsDuplicatedError(ValueError):
    """Raised when a DataFrame's columns has duplicated values."""


class DataFrameColumnsError(ValueError):
    """Raised when a DataFrame has the incorrect columns."""


class DataFrameDTypesError(ValueError):
    """Raised when a DataFrame has the incorrect dtypes."""


class DataFrameLengthError(ValueError):
    """Raised when a DataFrame has the incorrect length."""


class DataFrameSortedError(ValueError):
    """Raised when a DataFrame has non-sorted values."""


class DataFrameUniqueError(ValueError):
    """Raised when a DataFrame has non-unique values."""


def check_range_index(obj: IndexA | SeriesA | DataFrame, /) -> None:
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


def redirect_to_empty_pandas_concat_error(error: ValueError, /) -> NoReturn:
    """Redirect to the `EmptyPandasConcatError`."""
    redirect_error(error, "No objects to concatenate", EmptyPandasConcatError)


class EmptyPandasConcatError(ValueError):
    """Raised when there are no objects to concatenate."""


def rename_index(index: _Index, name: Hashable, /) -> _Index:
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


class DifferentDTypeError(ValueError):
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


class TimestampIsNaTError(ValueError):
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
