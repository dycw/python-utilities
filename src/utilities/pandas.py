from __future__ import annotations

import datetime as dt
from collections.abc import Hashable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from functools import partial, reduce
from itertools import permutations
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar, cast

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
from utilities.iterables import CheckLengthError, check_length
from utilities.numpy import NDArray1, dt64ns, has_dtype
from utilities.sentinel import Sentinel, sentinel
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


def check_index(
    index: IndexA,
    /,
    *,
    length: int | tuple[int, float] | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    name: Hashable | Sentinel = sentinel,
    sorted: bool = False,  # noqa: A002
    unique: bool = False,
) -> None:
    """Check the properties of an Index."""
    if length is not None:
        with redirect_error(CheckLengthError, CheckIndexError(f"{index=}, {length=}")):
            check_length(index, equal_or_approx=length)
    if min_length is not None:
        with redirect_error(
            CheckLengthError, CheckIndexError(f"{index=}, {min_length=}")
        ):
            check_length(index, min=min_length)
    if max_length is not None:
        with redirect_error(
            CheckLengthError, CheckIndexError(f"{index=}, {max_length=}")
        ):
            check_length(index, max=max_length)
    if (not isinstance(name, Sentinel)) and (index.name != name):
        msg = f"{index=}, {name=}"
        raise CheckIndexError(msg)
    if sorted:
        with redirect_error(AssertionError, CheckIndexError(f"{index=}")):
            assert_index_equal(index, index.sort_values())
    if unique and index.has_duplicates:
        msg = f"{index=}"
        raise CheckIndexError(msg)


class CheckIndexError(Exception):
    ...


def check_pandas_dataframe(
    df: DataFrame,
    /,
    *,
    standard: bool = False,
    columns: Sequence[Hashable] | None = None,
    dtypes: Mapping[Hashable, Any] | None = None,
    length: int | tuple[int, float] | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    sorted: Hashable | Sequence[Hashable] | None = None,  # noqa: A002
    unique: Hashable | Sequence[Hashable] | None = None,
) -> None:
    """Check the properties of a DataFrame."""
    if standard:
        if not isinstance(df.index, RangeIndex):
            msg = f"{df.index=}"
            raise CheckPandasDataFrameError(msg)
        with redirect_error(
            CheckRangeIndexError, CheckPandasDataFrameError(f"{df.index=}, {length=}")
        ):
            check_range_index(df.index, start=0, step=1, name=None)
        with redirect_error(
            CheckIndexError, CheckPandasDataFrameError(f"{df.index=}, {length=}")
        ):
            check_index(df.columns, name=None, unique=True)
    if (columns is not None) and (list(df.columns) != columns):
        msg = f"{df=}, {columns=}"
        raise CheckPandasDataFrameError(msg)
    if (dtypes is not None) and (dict(df.dtypes) != dict(dtypes)):
        msg = f"{df=}, {dtypes=}"
        raise CheckPandasDataFrameError(msg)
    if length is not None:
        with redirect_error(
            CheckLengthError, CheckPandasDataFrameError(f"{df=}, {length=}")
        ):
            check_length(df, equal_or_approx=length)
    if min_length is not None:
        with redirect_error(
            CheckLengthError, CheckPandasDataFrameError(f"{df=}, {min_length=}")
        ):
            check_length(df, min=min_length)
    if max_length is not None:
        with redirect_error(
            CheckLengthError, CheckPandasDataFrameError(f"{df=}, {max_length=}")
        ):
            check_length(df, max=max_length)
    if sorted is not None:
        df_sorted: DataFrame = df.sort_values(by=sorted).reset_index(drop=True)  # type: ignore
        with redirect_error(AssertionError, CheckPandasDataFrameError(f"{df=}")):
            assert_frame_equal(df, df_sorted)
    if (unique is not None) and df.duplicated(subset=unique).any():
        msg = f"{df=}, {unique=}"
        raise CheckPandasDataFrameError(msg)


class CheckPandasDataFrameError(Exception):
    ...


def check_range_index(
    index: RangeIndex,
    /,
    *,
    start: int | None = None,
    stop: int | None = None,
    step: int | None = None,
    length: int | tuple[int, float] | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    name: Hashable | Sentinel = sentinel,
) -> None:
    """Check the properties of a RangeIndex."""
    if (start is not None) and (cast(int, index.start) != start):
        msg = f"{index=}, {start=}"
        raise CheckRangeIndexError(msg)
    if (stop is not None) and (cast(int, index.stop) != stop):
        msg = f"{index=}, {stop=}"
        raise CheckRangeIndexError(msg)
    if (step is not None) and (cast(int, index.step) != step):
        msg = f"{index=}, {step=}"
        raise CheckRangeIndexError(msg)
    if length is not None:
        with redirect_error(
            CheckIndexError, CheckRangeIndexError(f"{index=}, {length=}")
        ):
            check_index(index, length=length)
    if min_length is not None:
        with redirect_error(
            CheckIndexError, CheckRangeIndexError(f"{index=}, {min_length=}")
        ):
            check_index(index, min_length=min_length)
    if max_length is not None:
        with redirect_error(
            CheckIndexError, CheckRangeIndexError(f"{index=}, {max_length=}")
        ):
            check_index(index, max_length=max_length, name=name)
    if not isinstance(name, Sentinel):
        with redirect_error(
            CheckIndexError, CheckRangeIndexError(f"{index=}, {name=}")
        ):
            check_index(index, name=name)


class CheckRangeIndexError(Exception):
    ...


@contextmanager
def redirect_empty_pandas_concat() -> Iterator[None]:
    """Redirect to the `EmptyPandasConcatError`."""
    with redirect_error(
        ValueError, EmptyPandasConcatError, match="No objects to concatenate"
    ):
        yield


class EmptyPandasConcatError(Exception):
    ...


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
        msg = f"{x.dtype=}, {y.dtype=}"
        raise SeriesMinMaxError(msg)
    out = x.copy()
    for first, second in permutations([x, y]):
        i = first.notna() & second.isna()
        out.loc[i] = first.loc[i]
    i = x.notna() & y.notna()
    out.loc[i] = x.loc[i].clip(**{kind: cast(Any, y.loc[i])})
    out.loc[x.isna() & y.isna()] = NA
    return out


class SeriesMinMaxError(Exception):
    ...


def sort_index(index: _Index, /) -> _Index:
    return cast(_Index, index.sort_values())


def timestamp_to_date(timestamp: Any, /, *, warn: bool = True) -> dt.date:
    """Convert a timestamp to a date."""
    return timestamp_to_datetime(timestamp, warn=warn).date()


def timestamp_to_datetime(timestamp: Any, /, *, warn: bool = True) -> dt.datetime:
    """Convert a timestamp to a datetime."""
    if timestamp is NaT:
        msg = f"{timestamp=}"
        raise TimestampToDateTimeError(msg)
    datetime = cast(dt.datetime, timestamp.to_pydatetime(warn=warn))
    if datetime.tzinfo is None:
        return datetime.replace(tzinfo=UTC)
    return datetime


class TimestampToDateTimeError(Exception):
    ...


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
    if has_dtype(series, (bool, dt64ns, int, float)):
        return series.to_numpy()
    if has_dtype(series, (boolean, Int64, string)):
        return where(
            series.notna(), series.to_numpy(dtype=object), cast(Any, None)
        ).astype(object)
    msg = f"{series=}"  # pragma: no cover
    raise ToNumpyError(msg)  # pragma: no cover


class ToNumpyError(Exception):
    ...


__all__ = [
    "CheckIndexError",
    "CheckPandasDataFrameError",
    "CheckRangeIndexError",
    "EmptyPandasConcatError",
    "Int64",
    "SeriesMinMaxError",
    "TIMESTAMP_MAX_AS_DATE",
    "TIMESTAMP_MAX_AS_DATETIME",
    "TIMESTAMP_MIN_AS_DATE",
    "TIMESTAMP_MIN_AS_DATETIME",
    "TimestampToDateTimeError",
    "astype",
    "boolean",
    "category",
    "check_index",
    "check_pandas_dataframe",
    "check_range_index",
    "datetime64nshk",
    "datetime64nsutc",
    "redirect_empty_pandas_concat",
    "rename_index",
    "series_max",
    "series_min",
    "sort_index",
    "string",
    "timestamp_to_date",
    "timestamp_to_datetime",
    "to_numpy",
]
