from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial, reduce
from itertools import chain, permutations
from operator import ge, gt, le, lt
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar, cast

from numpy import arange, where
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
    concat,
)
from pandas.testing import assert_frame_equal, assert_index_equal
from typing_extensions import assert_never, override

from utilities.errors import redirect_error
from utilities.functions import CheckNameError, check_name
from utilities.iterables import (
    CheckIterablesEqualError,
    CheckLengthError,
    CheckMappingsEqualError,
    CheckSetsEqualError,
    CheckSubSetError,
    CheckSuperSetError,
    check_iterables_equal,
    check_length,
    check_mappings_equal,
    check_sets_equal,
    check_subset,
    check_superset,
)
from utilities.numpy import (
    FlatN0Error,
    NDArray1,
    NDArrayB1,
    NDArrayI1,
    datetime64ns,
    flatn0,
    has_dtype,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import (
        Callable,
        Hashable,
        Iterable,
        Iterator,
        Mapping,
        Sequence,
    )

    IndexA: TypeAlias = Index[Any]  # type: ignore[]
    IndexB: TypeAlias = Index[bool]  # type: ignore[]
    IndexBn: TypeAlias = Index[BooleanDtype]  # type: ignore[]
    IndexC: TypeAlias = Index[CategoricalDtype]  # type: ignore[]
    IndexD: TypeAlias = Index[dt.datetime]  # type: ignore[]
    IndexDhk: TypeAlias = Index[DatetimeTZDtype]  # type: ignore[]
    IndexDutc: TypeAlias = Index[DatetimeTZDtype]  # type: ignore[]
    IndexF: TypeAlias = Index[float]  # type: ignore[]
    IndexI: TypeAlias = Index[int]  # type: ignore[]
    IndexI64: TypeAlias = Index[Int64Dtype]  # type: ignore[]
    IndexS: TypeAlias = Index[str]  # type: ignore[]
    IndexSt: TypeAlias = Index[StringDtype]  # type: ignore[]

    SeriesA: TypeAlias = Series[Any]  # type: ignore[]
    SeriesB: TypeAlias = Series[bool]  # type: ignore[]
    SeriesBn: TypeAlias = Series[BooleanDtype]  # type: ignore[]
    SeriesC: TypeAlias = Series[CategoricalDtype]  # type: ignore[]
    SeriesD: TypeAlias = Series[dt.datetime]  # type: ignore[]
    SeriesDhk: TypeAlias = Series[DatetimeTZDtype]  # type: ignore[]
    SeriesDutc: TypeAlias = Series[DatetimeTZDtype]  # type: ignore[]
    SeriesF: TypeAlias = Series[float]  # type: ignore[]
    SeriesI: TypeAlias = Series[int]  # type: ignore[]
    SeriesI64: TypeAlias = Series[Int64Dtype]  # type: ignore[]
    SeriesS: TypeAlias = Series[str]  # type: ignore[]
    SeriesSt: TypeAlias = Series[StringDtype]  # type: ignore[]
else:
    IndexA = IndexB = IndexBn = IndexC = IndexD = IndexDhk = IndexDutc = IndexF = (
        IndexI
    ) = IndexI64 = IndexS = IndexSt = Index
    SeriesA = SeriesB = SeriesBn = SeriesC = SeriesD = SeriesDhk = SeriesDutc = (
        SeriesF
    ) = SeriesI = SeriesI64 = SeriesS = SeriesSt = Series


Int64 = "Int64"
boolean = "boolean"
category = "category"
string = "string"
datetime64nsutc = DatetimeTZDtype(tz=UTC)
datetime64nshk = DatetimeTZDtype(tz=HONG_KONG)


_Index = TypeVar("_Index", bound=Index)


def assign_after(df: DataFrame, key: Hashable, value: SeriesA, /) -> DataFrame:
    """Assign a series after an existing column."""
    return _assign_before_or_after(df, key, value, le, gt)


def assign_before(df: DataFrame, key: Hashable, value: SeriesA, /) -> DataFrame:
    """Assign a series before an existing column."""
    return _assign_before_or_after(df, key, value, lt, ge)


def _assign_before_or_after(
    df: DataFrame,
    key: Hashable,
    value: SeriesA,
    left: Callable[[NDArrayI1, int], NDArrayB1],
    right: Callable[[NDArrayI1, int], NDArrayB1],
    /,
) -> DataFrame:
    cols = df.columns.to_numpy()
    try:
        index = flatn0(cols == key)
    except FlatN0Error:
        raise AssignBeforeOrAfterError(df=df, key=key) from None
    ar = arange(len(cols))
    return concat(
        [df.iloc[:, left(ar, index)], value, df.iloc[:, right(ar, index)]], axis=1
    )


@dataclass(kw_only=True)
class AssignBeforeOrAfterError(Exception):
    df: DataFrame
    key: Hashable

    @override
    def __str__(self) -> str:
        return f"DataFrame must contain exactly one column named {self.key!r}:\n\n{self.df}"


def assign_between(
    df: DataFrame, left: Hashable, right: Hashable, value: SeriesA, /
) -> DataFrame:
    """Assign a series in between two existing columns."""
    cols = df.columns.to_numpy()
    try:
        index_left = flatn0(cols == left)
        index_right = flatn0(cols == right)
    except FlatN0Error:
        raise AssignBetweenIndexError(df=df, left=left, right=right) from None
    if (index_left + 1) != index_right:
        raise AssignBetweenIndicesError(
            df=df,
            left=left,
            right=right,
            index_left=index_left,
            index_right=index_right,
        )
    return assign_after(df, left, value)


@dataclass(kw_only=True)
class AssignBetweenError(Exception):
    df: DataFrame
    left: Hashable
    right: Hashable


@dataclass(kw_only=True)
class AssignBetweenIndexError(AssignBetweenError):
    @override
    def __str__(self) -> str:
        return f"DataFrame must contain exactly one column named {self.left!r} and {self.right!r}:\n\n{self.df}"


@dataclass(kw_only=True)
class AssignBetweenIndicesError(AssignBetweenError):
    index_left: int
    index_right: int

    @override
    def __str__(self) -> str:
        return f"DataFrame must specify consecutive indices; got {self.index_left} and {self.index_right}"


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
    _check_index_length(index, equal_or_approx=length, min=min_length, max=max_length)
    _check_index_name(index, name)
    if sorted:
        _check_index_sorted(index)
    if unique:
        _check_index_unique(index)


@dataclass(kw_only=True)
class CheckIndexError(Exception):
    index: IndexA


def _check_index_length(
    index: IndexA,
    /,
    *,
    equal_or_approx: int | tuple[int, float] | None = None,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> None:
    try:
        check_length(index, equal_or_approx=equal_or_approx, min=min, max=max)
    except CheckLengthError as error:
        raise _CheckIndexLengthError(index=index) from error


@dataclass(kw_only=True)
class _CheckIndexLengthError(CheckIndexError):
    @override
    def __str__(self) -> str:
        return f"Index {self.index} must satisfy the length requirements."


def _check_index_name(index: IndexA, name: Any, /) -> None:
    if not isinstance(name, Sentinel):
        try:
            check_name(index, name)
        except CheckNameError as error:
            raise _CheckIndexNameError(index=index) from error


@dataclass(kw_only=True)
class _CheckIndexNameError(CheckIndexError):
    @override
    def __str__(self) -> str:
        return f"Index {self.index} must satisfy the name requirement."


def _check_index_sorted(index: IndexA, /) -> None:
    try:
        assert_index_equal(index, index.sort_values())
    except AssertionError as error:
        raise _CheckIndexSortedError(index=index) from error


@dataclass(kw_only=True)
class _CheckIndexSortedError(CheckIndexError):
    @override
    def __str__(self) -> str:
        return f"Index {self.index} must be sorted."


def _check_index_unique(index: IndexA, /) -> None:
    if index.has_duplicates:
        raise _CheckIndexUniqueError(index=index)


@dataclass(kw_only=True)
class _CheckIndexUniqueError(CheckIndexError):
    @override
    def __str__(self) -> str:
        return f"Index {self.index} must be unique."


def check_pandas_dataframe(
    df: DataFrame,
    /,
    *,
    standard: bool = False,
    columns: Iterable[Hashable] | None = None,
    dtypes: Mapping[Hashable, Any] | None = None,
    length: int | tuple[int, float] | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    sorted: str | Sequence[str] | None = None,  # noqa: A002
    unique: Hashable | Sequence[Hashable] | None = None,
    width: int | None = None,
) -> None:
    """Check the properties of a DataFrame."""
    _check_pandas_dataframe_length(
        df, equal_or_approx=length, min=min_length, max=max_length
    )
    if standard:
        _check_pandas_dataframe_standard(df)
    if columns is not None:
        _check_pandas_dataframe_columns(df, columns)
    if dtypes is not None:
        _check_pandas_dataframe_dtypes(df, dtypes)
    if sorted is not None:
        _check_pandas_dataframe_sorted(df, sorted)
    if unique is not None:
        _check_pandas_dataframe_unique(df, unique)
    if width is not None:
        _check_pandas_dataframe_width(df, width)


@dataclass(kw_only=True)
class CheckPandasDataFrameError(Exception):
    df: DataFrame


def _check_pandas_dataframe_columns(
    df: DataFrame, columns: Iterable[Hashable], /
) -> None:
    try:
        check_iterables_equal(df.columns, columns)
    except CheckIterablesEqualError as error:
        raise _CheckPandasDataFrameColumnsError(df=df, columns=columns) from error


@dataclass(kw_only=True)
class _CheckPandasDataFrameColumnsError(CheckPandasDataFrameError):
    columns: Iterable[Hashable]

    @override
    def __str__(self) -> str:
        return f"DataFrame must have columns {self.columns}; got {self.df.columns}\n\n{self.df}."


def _check_pandas_dataframe_dtypes(
    df: DataFrame, dtypes: Mapping[Hashable, Any], /
) -> None:
    try:
        check_mappings_equal(dict(df.dtypes), dtypes)
    except CheckMappingsEqualError as error:
        raise _CheckPandasDataFrameDTypesError(df=df, dtypes=dtypes) from error
    try:
        _check_pandas_dataframe_columns(df, dtypes)
    except _CheckPandasDataFrameColumnsError as error:
        raise _CheckPandasDataFrameDTypesError(df=df, dtypes=dtypes) from error


@dataclass(kw_only=True)
class _CheckPandasDataFrameDTypesError(CheckPandasDataFrameError):
    dtypes: Iterable[Any]

    @override
    def __str__(self) -> str:
        return f"DataFrame must have dtypes {self.dtypes}; got {self.df.dtypes}\n\n{self.df}."


def _check_pandas_dataframe_length(
    df: DataFrame,
    /,
    *,
    equal_or_approx: int | tuple[int, float] | None = None,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> None:
    try:
        check_length(df, equal_or_approx=equal_or_approx, min=min, max=max)
    except CheckLengthError as error:
        raise _CheckPandasDataFrameLengthError(df=df) from error


@dataclass(kw_only=True)
class _CheckPandasDataFrameLengthError(CheckPandasDataFrameError):
    @override
    def __str__(self) -> str:
        return f"DataFrame must satisfy the length requirements; got {len(self.df)}\n\n{self.df}."


def _check_pandas_dataframe_standard(df: DataFrame, /) -> None:
    if not isinstance(df.index, RangeIndex):
        raise _CheckPandasDataFrameStandardIndexError(df=df)
    try:
        check_range_index(df.index, start=0, step=1, name=None)
    except CheckRangeIndexError as error:
        raise _CheckPandasDataFrameStandardIndexError(df=df) from error
    try:
        check_index(df.columns, name=None, unique=True)
    except CheckIndexError as error:
        raise _CheckPandasDataFrameStandardColumnsError(df=df) from error


class _CheckPandasDataFrameStandardIndexError(CheckPandasDataFrameError):
    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have a standard index; got {self.df.index}\n\n{self.df}."
        )


class _CheckPandasDataFrameStandardColumnsError(CheckPandasDataFrameError):
    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have standard columns; got {self.df.columns}\n\n{self.df}."
        )


def _check_pandas_dataframe_sorted(df: DataFrame, by: str | Sequence[str], /) -> None:
    df_sorted = df.sort_values(by=by)
    try:
        assert_frame_equal(df, df_sorted)
    except AssertionError as error:
        raise _CheckPandasDataFrameSortedError(df=df, by=by) from error


@dataclass(kw_only=True)
class _CheckPandasDataFrameSortedError(CheckPandasDataFrameError):
    by: str | Sequence[str]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be sorted on {self.by}\n\n{self.df}."


def _check_pandas_dataframe_unique(
    df: DataFrame, by: Hashable | Sequence[Hashable], /
) -> None:
    if df.duplicated(subset=by).any():
        raise _CheckPandasDataFrameUniqueError(df=df, by=by)


@dataclass(kw_only=True)
class _CheckPandasDataFrameUniqueError(CheckPandasDataFrameError):
    by: Hashable | Sequence[Hashable]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be unique on {self.by}\n\n{self.df}."


def _check_pandas_dataframe_width(df: DataFrame, width: int, /) -> None:
    if len(df.columns) != width:
        raise _CheckPandasDataFrameWidthError(df=df, width=width)


@dataclass(kw_only=True)
class _CheckPandasDataFrameWidthError(CheckPandasDataFrameError):
    width: int

    @override
    def __str__(self) -> str:
        return f"DataFrame must have width {self.width}; got {len(self.df.columns)}\n\n{self.df}."


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
    if (start is not None) and (index.start != start):
        msg = f"{index=}, {start=}"
        raise CheckRangeIndexError(msg)
    if (stop is not None) and (index.stop != stop):
        msg = f"{index=}, {stop=}"
        raise CheckRangeIndexError(msg)
    if (step is not None) and (index.step != step):
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


class CheckRangeIndexError(Exception): ...


@contextmanager
def redirect_empty_pandas_concat() -> Iterator[None]:
    """Redirect to the `EmptyPandasConcatError`."""
    with redirect_error(
        ValueError, EmptyPandasConcatError, match="No objects to concatenate"
    ):
        yield


class EmptyPandasConcatError(Exception): ...


def reindex_to_set(index: _Index, target: Iterable[Any], /) -> _Index:
    """Re-index an Index to a strict permutation of its elements."""
    target_as_list = list(target)
    try:
        check_sets_equal(index, target_as_list)
    except CheckSetsEqualError as error:
        raise ReindexToSetError(index=index, target=target_as_list) from error
    new_index, _ = index.reindex(target_as_list)
    return new_index


@dataclass(kw_only=True)
class ReindexToSetError(Exception):
    index: IndexA
    target: list[Any]

    @override
    def __str__(self) -> str:
        return f"Index {self.index} and {self.target} must be equal as sets."


def reindex_to_subset(index: _Index, target: Iterable[Any], /) -> _Index:
    """Re-index an Index to a strict subset of its elements."""
    target_as_list = list(target)
    try:
        check_superset(index, target_as_list)
    except CheckSuperSetError as error:
        raise ReindexToSubSetError(index=index, target=target_as_list) from error
    new_index, _ = index.reindex(target_as_list)
    return new_index


@dataclass(kw_only=True)
class ReindexToSubSetError(Exception):
    index: IndexA
    target: list[Any]

    @override
    def __str__(self) -> str:
        return f"Index {self.index} must be a superset of {self.target}."


def reindex_to_superset(index: _Index, target: Iterable[Any], /) -> _Index:
    """Re-index an Index to a strict superset of its elements."""
    target_as_list = list(target)
    try:
        check_subset(index, target_as_list)
    except CheckSubSetError as error:
        raise ReindexToSuperSetError(index=index, target=target_as_list) from error
    new_index, _ = index.reindex(target_as_list)
    return new_index


@dataclass(kw_only=True)
class ReindexToSuperSetError(Exception):
    index: IndexA
    target: list[Any]

    @override
    def __str__(self) -> str:
        return f"Index {self.index} must be a subset of {self.target}."


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
        raise SeriesMinMaxError(x=x, y=y)
    out = x.copy()
    for first, second in permutations([x, y]):
        i = first.notna() & second.isna()
        out.loc[i] = first.loc[i]
    i = x.notna() & y.notna()
    out.loc[i] = x.loc[i].clip(**{kind: cast(Any, y.loc[i])})
    out.loc[x.isna() & y.isna()] = NA
    return out


@dataclass(kw_only=True)
class SeriesMinMaxError(Exception):
    x: SeriesA
    y: SeriesA

    @override
    def __str__(self) -> str:
        return f"Series {self.x} and {self.y} must have the same dtype; got {self.x.dtype} and {self.y.dtype}."


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


class TimestampToDateTimeError(Exception): ...


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
    msg = f"{series=}"  # pragma: no cover
    raise ToNumpyError(msg)  # pragma: no cover


class ToNumpyError(Exception): ...


def union_indexes(
    index: IndexA,
    *more_indexes: IndexA,
    names: Literal["first", "last", "raise"] = "raise",
) -> IndexA:
    """Take the union of an arbitrary number of indexes."""
    indexes = chain([index], more_indexes)

    def func(left: IndexA, right: IndexA, /) -> IndexA:
        lname, rname = left.name, right.name
        if (lname == rname) or ((lname is not None) and (rname is None)):
            name = lname
        elif (lname is None) and (rname is not None):
            name = rname
        else:
            match names:
                case "first":
                    name = lname
                case "last":
                    name = rname
                case "raise":
                    raise UnionIndexesError(left=left, right=right)
                case _ as never:  # type: ignore[]
                    assert_never(never)
        return left.union(right).rename(name)

    return reduce(func, indexes)


@dataclass(kw_only=True)
class UnionIndexesError(Exception):
    left: IndexA
    right: IndexA

    @override
    def __str__(self) -> str:
        return f"Indexes {self.left} and {self.right} must have the same name; got {self.left.name} and {self.right.name}."


__all__ = [
    "TIMESTAMP_MAX_AS_DATE",
    "TIMESTAMP_MAX_AS_DATETIME",
    "TIMESTAMP_MIN_AS_DATE",
    "TIMESTAMP_MIN_AS_DATETIME",
    "AssignBeforeOrAfterError",
    "AssignBetweenError",
    "AssignBetweenIndexError",
    "AssignBetweenIndicesError",
    "CheckIndexError",
    "CheckPandasDataFrameError",
    "CheckRangeIndexError",
    "EmptyPandasConcatError",
    "IndexA",
    "IndexB",
    "IndexBn",
    "IndexC",
    "IndexD",
    "IndexDhk",
    "IndexDutc",
    "IndexF",
    "IndexI",
    "IndexI64",
    "IndexS",
    "IndexSt",
    "Int64",
    "ReindexToSetError",
    "ReindexToSubSetError",
    "ReindexToSuperSetError",
    "SeriesA",
    "SeriesB",
    "SeriesBn",
    "SeriesC",
    "SeriesD",
    "SeriesDhk",
    "SeriesDutc",
    "SeriesF",
    "SeriesI",
    "SeriesI64",
    "SeriesMinMaxError",
    "SeriesS",
    "SeriesSt",
    "TimestampToDateTimeError",
    "UnionIndexesError",
    "assign_after",
    "assign_before",
    "assign_between",
    "boolean",
    "category",
    "check_index",
    "check_pandas_dataframe",
    "check_range_index",
    "datetime64nshk",
    "datetime64nsutc",
    "redirect_empty_pandas_concat",
    "reindex_to_set",
    "reindex_to_subset",
    "reindex_to_superset",
    "series_max",
    "series_min",
    "string",
    "timestamp_to_date",
    "timestamp_to_datetime",
    "to_numpy",
    "union_indexes",
]
