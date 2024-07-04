from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from enum import Enum
from functools import reduce
from itertools import chain
from types import NoneType, UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from polars import (
    Boolean,
    DataFrame,
    Date,
    Datetime,
    Expr,
    Float64,
    Int64,
    List,
    Series,
    Struct,
    Utf8,
    col,
    lit,
    when,
)
from polars._typing import (
    IntoExpr,
    IntoExprColumn,
    JoinStrategy,
    JoinValidation,
    PolarsDataType,
    SchemaDict,
)
from polars.exceptions import ColumnNotFoundError, OutOfBoundsError
from polars.testing import assert_frame_equal
from typing_extensions import Never, assert_never, override

from utilities.dataclasses import Dataclass, is_dataclass_class
from utilities.errors import redirect_error
from utilities.iterables import (
    CheckIterablesEqualError,
    CheckMappingsEqualError,
    CheckSuperMappingError,
    check_iterables_equal,
    check_mappings_equal,
    check_supermapping,
    is_iterable_not_str,
    one,
)
from utilities.math import CheckIntegerError, check_integer
from utilities.zoneinfo import get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from collections.abc import Set as AbstractSet
    from zoneinfo import ZoneInfo

    from pyarrow import DataType

    from utilities.types import IterableStrs


@overload
def ceil_datetime(column: Expr | str, every: Expr | str, /) -> Expr: ...
@overload
def ceil_datetime(column: Series, every: Expr | str, /) -> Series: ...
def ceil_datetime(column: IntoExprColumn, every: Expr | str, /) -> Expr | Series:
    """Compute the `ceil` of a datetime column."""
    column = ensure_expr_or_series(column)
    rounded = column.dt.round(every)
    ceil = (
        when(column <= rounded)
        .then(rounded)
        .otherwise(column.dt.offset_by(every).dt.round(every))
    )
    if isinstance(column, Expr):
        return ceil
    return DataFrame().with_columns(ceil.alias(column.name))[column.name]


def check_polars_dataframe(
    df: DataFrame,
    /,
    *,
    columns: IterableStrs | None = None,
    dtypes: Iterable[PolarsDataType] | None = None,
    height: int | tuple[int, float] | None = None,
    min_height: int | None = None,
    max_height: int | None = None,
    predicates: Mapping[str, Callable[[Any], bool]] | None = None,
    schema_list: SchemaDict | None = None,
    schema_set: SchemaDict | None = None,
    schema_subset: SchemaDict | None = None,
    shape: tuple[int, int] | None = None,
    sorted: IntoExpr | Iterable[IntoExpr] | None = None,  # noqa: A002
    unique: IntoExpr | Iterable[IntoExpr] | None = None,
    width: int | None = None,
) -> None:
    """Check the properties of a DataFrame."""
    _check_polars_dataframe_height(
        df, equal_or_approx=height, min=min_height, max=max_height
    )
    if columns is not None:
        _check_polars_dataframe_columns(df, columns)
    if dtypes is not None:
        _check_polars_dataframe_dtypes(df, dtypes)
    if predicates is not None:
        _check_polars_dataframe_predicates(df, predicates)
    if schema_list is not None:
        _check_polars_dataframe_schema_list(df, schema_list)
    if schema_set is not None:
        _check_polars_dataframe_schema_set(df, schema_set)
    if schema_subset is not None:
        _check_polars_dataframe_schema_subset(df, schema_subset)
    if shape is not None:
        _check_polars_dataframe_shape(df, shape)
    if sorted is not None:
        _check_polars_dataframe_sorted(df, sorted)
    if unique is not None:
        _check_polars_dataframe_unique(df, unique)
    if width is not None:
        _check_polars_dataframe_width(df, width)


@dataclass(kw_only=True)
class CheckPolarsDataFrameError(Exception):
    df: DataFrame


def _check_polars_dataframe_columns(df: DataFrame, columns: IterableStrs, /) -> None:
    try:
        check_iterables_equal(df.columns, columns)
    except CheckIterablesEqualError as error:
        raise _CheckPolarsDataFrameColumnsError(df=df, columns=columns) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameColumnsError(CheckPolarsDataFrameError):
    columns: IterableStrs

    @override
    def __str__(self) -> str:
        return f"DataFrame must have columns {self.columns}; got {self.df.columns}\n\n{self.df}"


def _check_polars_dataframe_dtypes(
    df: DataFrame, dtypes: Iterable[PolarsDataType], /
) -> None:
    try:
        check_iterables_equal(df.dtypes, dtypes)
    except CheckIterablesEqualError as error:
        raise _CheckPolarsDataFrameDTypesError(df=df, dtypes=dtypes) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameDTypesError(CheckPolarsDataFrameError):
    dtypes: Iterable[PolarsDataType]

    @override
    def __str__(self) -> str:
        return f"DataFrame must have dtypes {self.dtypes}; got {self.df.dtypes}\n\n{self.df}"


def _check_polars_dataframe_height(
    df: DataFrame,
    /,
    *,
    equal_or_approx: int | tuple[int, float] | None = None,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> None:
    try:
        check_integer(df.height, equal_or_approx=equal_or_approx, min=min, max=max)
    except CheckIntegerError as error:
        raise _CheckPolarsDataFrameHeightError(df=df) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameHeightError(CheckPolarsDataFrameError):
    @override
    def __str__(self) -> str:
        return f"DataFrame must satisfy the height requirements; got {self.df.height}\n\n{self.df}"


def _check_polars_dataframe_predicates(
    df: DataFrame, predicates: Mapping[str, Callable[[Any], bool]], /
) -> None:
    missing: set[str] = set()
    failed: set[str] = set()
    for column, predicate in predicates.items():
        try:
            sr = df[column]
        except ColumnNotFoundError:
            missing.add(column)
        else:
            if not sr.map_elements(predicate, return_dtype=Boolean).all():
                failed.add(column)
    if (len(missing) >= 1) or (len(failed)) >= 1:
        raise _CheckPolarsDataFramePredicatesError(
            df=df, predicates=predicates, missing=missing, failed=failed
        )


@dataclass(kw_only=True)
class _CheckPolarsDataFramePredicatesError(CheckPolarsDataFrameError):
    predicates: Mapping[str, Callable[[Any], bool]]
    missing: AbstractSet[str]
    failed: AbstractSet[str]

    @override
    def __str__(self) -> str:
        match list(self._yield_parts()):
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return f"DataFrame must satisfy the predicates; {desc}\n\n"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.missing) >= 1:
            yield f"missing columns were {self.missing}"
        if len(self.failed) >= 1:
            yield f"failed predicates were {self.failed}"


def _check_polars_dataframe_schema_list(df: DataFrame, schema: SchemaDict, /) -> None:
    try:
        _check_polars_dataframe_schema_set(df, schema)
    except _CheckPolarsDataFrameSchemaSetError as error:
        raise _CheckPolarsDataFrameSchemaListError(df=df, schema=schema) from error
    try:
        _check_polars_dataframe_columns(df, schema)
    except _CheckPolarsDataFrameColumnsError as error:
        raise _CheckPolarsDataFrameSchemaListError(df=df, schema=schema) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameSchemaListError(CheckPolarsDataFrameError):
    schema: SchemaDict

    @override
    def __str__(self) -> str:
        return f"DataFrame must have schema {self.schema}; got {self.df.columns}\n\n{self.df}"


def _check_polars_dataframe_schema_set(df: DataFrame, schema: SchemaDict, /) -> None:
    try:
        check_mappings_equal(df.schema, schema)
    except CheckMappingsEqualError as error:
        raise _CheckPolarsDataFrameSchemaSetError(df=df, schema=schema) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameSchemaSetError(CheckPolarsDataFrameError):
    schema: SchemaDict

    @override
    def __str__(self) -> str:
        return f"DataFrame must have schema {self.schema}; got {self.df.columns}\n\n{self.df}"


def _check_polars_dataframe_schema_subset(df: DataFrame, schema: SchemaDict, /) -> None:
    try:
        check_supermapping(df.schema, schema)
    except CheckSuperMappingError as error:
        raise _CheckPolarsDataFrameSchemaSubsetError(df=df, schema=schema) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameSchemaSubsetError(CheckPolarsDataFrameError):
    schema: SchemaDict

    @override
    def __str__(self) -> str:
        return f"DataFrame schema must include {self.schema}; got {self.df.schema}\n\n{self.df}"


def _check_polars_dataframe_shape(df: DataFrame, shape: tuple[int, int], /) -> None:
    if df.shape != shape:
        raise _CheckPolarsDataFrameShapeError(df=df, shape=shape) from None


@dataclass(kw_only=True)
class _CheckPolarsDataFrameShapeError(CheckPolarsDataFrameError):
    shape: tuple[int, int]

    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have shape {self.shape}; got {self.df.shape}\n\n{self.df}"
        )


def _check_polars_dataframe_sorted(
    df: DataFrame, by: IntoExpr | Iterable[IntoExpr], /
) -> None:
    by_use = cast(
        IntoExpr | list[IntoExpr], list(by) if is_iterable_not_str(by) else by
    )
    df_sorted = df.sort(by_use)
    try:
        assert_frame_equal(df, df_sorted)
    except AssertionError as error:
        raise _CheckPolarsDataFrameSortedError(df=df, by=by_use) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameSortedError(CheckPolarsDataFrameError):
    by: IntoExpr | list[IntoExpr]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be sorted on {self.by}\n\n{self.df}"


def _check_polars_dataframe_unique(
    df: DataFrame, by: IntoExpr | Iterable[IntoExpr], /
) -> None:
    by_use = cast(
        IntoExpr | list[IntoExpr], list(by) if is_iterable_not_str(by) else by
    )
    if df.select(by_use).is_duplicated().any():
        raise _CheckPolarsDataFrameUniqueError(df=df, by=by_use)


@dataclass(kw_only=True)
class _CheckPolarsDataFrameUniqueError(CheckPolarsDataFrameError):
    by: IntoExpr | list[IntoExpr]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be unique on {self.by}\n\n{self.df}"


def _check_polars_dataframe_width(df: DataFrame, width: int, /) -> None:
    if df.width != width:
        raise _CheckPolarsDataFrameWidthError(df=df, width=width)


@dataclass(kw_only=True)
class _CheckPolarsDataFrameWidthError(CheckPolarsDataFrameError):
    width: int

    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have width {self.width}; got {self.df.width}\n\n{self.df}"
        )


def collect_series(expr: Expr, /) -> Series:
    """Collect a column expression into a Series."""
    data = DataFrame().with_columns(expr)
    return data[one(data.columns)]


def columns_to_dict(df: DataFrame, key: str, value: str, /) -> dict[Any, Any]:
    """Map a pair of columns into a dictionary. Must be unique on `key`."""
    col_key = df[key]
    if col_key.is_duplicated().any():
        raise ColumnsToDictError(df=df, key=key)
    col_value = df[value]
    return dict(zip(col_key, col_value, strict=True))


@dataclass(kw_only=True)
class ColumnsToDictError(Exception):
    df: DataFrame
    key: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must be unique on {self.key!r}\n\n{self.df}"


@overload
def ensure_expr_or_series(column: Expr | str, /) -> Expr: ...
@overload
def ensure_expr_or_series(column: Series, /) -> Series: ...
def ensure_expr_or_series(column: IntoExprColumn, /) -> Expr | Series:
    """Ensure a column expression or Series is returned."""
    return col(column) if isinstance(column, str) else column


@overload
def floor_datetime(column: Expr | str, every: Expr | str, /) -> Expr: ...
@overload
def floor_datetime(column: Series, every: Expr | str, /) -> Series: ...
def floor_datetime(column: IntoExprColumn, every: Expr | str, /) -> Expr | Series:
    """Compute the `floor` of a datetime column."""
    column = ensure_expr_or_series(column)
    rounded = column.dt.round(every)
    floor = (
        when(column >= rounded)
        .then(rounded)
        .otherwise(column.dt.offset_by("-" + every).dt.round(every))
    )
    if isinstance(column, Expr):
        return floor
    return DataFrame().with_columns(floor.alias(column.name))[column.name]


def join(
    df: DataFrame,
    *dfs: DataFrame,
    on: str | Expr | Sequence[str | Expr],
    how: JoinStrategy = "inner",
    validate: JoinValidation = "m:m",
) -> DataFrame:
    """Join a set of DataFrames."""

    def inner(left: DataFrame, right: DataFrame, /) -> DataFrame:
        return left.join(right, on=on, how=how, validate=validate)

    return reduce(inner, chain([df], dfs))


def nan_sum_agg(column: str | Expr, /, *, dtype: PolarsDataType | None = None) -> Expr:
    """Nan sum aggregation."""
    col_use = col(column) if isinstance(column, str) else column
    return (
        when(col_use.is_not_null().any())
        .then(col_use.sum())
        .otherwise(lit(None, dtype=dtype))
    )


def nan_sum_cols(
    column: str | Expr, *columns: str | Expr, dtype: PolarsDataType | None = None
) -> Expr:
    """Nan sum across columns."""
    all_columns = chain([column], columns)
    all_exprs = (
        col(column) if isinstance(column, str) else column for column in all_columns
    )

    def func(x: Expr, y: Expr, /) -> Expr:
        return (
            when(x.is_not_null() & y.is_not_null())
            .then(x + y)
            .when(x.is_not_null() & y.is_null())
            .then(x)
            .when(x.is_null() & y.is_not_null())
            .then(y)
            .otherwise(lit(None, dtype=dtype))
        )

    return reduce(func, all_exprs)


@contextmanager
def redirect_empty_polars_concat() -> Iterator[None]:
    """Redirect to the `EmptyPolarsConcatError`."""
    with redirect_error(
        ValueError, EmptyPolarsConcatError, match="cannot concat empty list"
    ):
        yield


class EmptyPolarsConcatError(Exception): ...


def set_first_row_as_columns(df: DataFrame, /) -> DataFrame:
    """Set the first row of a DataFrame as its columns."""
    with redirect_error(OutOfBoundsError, SetFirstRowAsColumnsError(f"{df=}")):
        row = df.row(0)
    mapping = dict(zip(df.columns, row, strict=True))
    return df[1:].rename(mapping)


class SetFirstRowAsColumnsError(Exception): ...


def struct_data_type(
    cls: type[Dataclass], /, *, time_zone: ZoneInfo | str | None = None
) -> Struct:
    """Construct the Struct data type for a dataclass."""
    if not is_dataclass_class(cls):
        raise _StructDataTypeNotADataclassError(cls=cls)
    anns = get_type_hints(cls)
    data_types = {
        k: _struct_data_type_one(v, time_zone=time_zone) for k, v in anns.items()
    }
    return Struct(data_types)


def _struct_data_type_one(
    ann: Any, /, *, time_zone: ZoneInfo | str | None = None
) -> DataType:
    with suppress(KeyError):
        return {bool: Boolean, dt.date: Date, float: Float64, int: Int64, str: Utf8}[
            ann
        ]
    if ann is dt.datetime:
        if time_zone is None:
            raise _StructDataTypeTimeZoneMissingError
        return Datetime(time_zone=get_time_zone_name(time_zone))
    if is_dataclass_class(ann):
        return struct_data_type(ann, time_zone=time_zone)
    if isinstance(ann, type) and issubclass(ann, Enum):
        return Utf8
    if (origin := get_origin(ann)) in {frozenset, list, set}:
        return List(_struct_data_type_one(one(get_args(ann)), time_zone=time_zone))
    if origin is Literal:
        args = get_args(ann)
        if all(isinstance(arg, str) for arg in args):
            return Utf8
        raise _StructDataTypeNonStringLiteralError(args=args)
    if origin is UnionType:
        inner = one(arg for arg in get_args(ann) if arg is not NoneType)
        return _struct_data_type_one(inner, time_zone=time_zone)
    raise _StructDataTypeTypeError(ann=ann)


@dataclass(kw_only=True)
class StructDataTypeError(Exception): ...


@dataclass(kw_only=True)
class _StructDataTypeNotADataclassError(StructDataTypeError):
    cls: type[Dataclass]

    @override
    def __str__(self) -> str:
        return f"Object must be a dataclass; got {self.cls}"


@dataclass(kw_only=True)
class _StructDataTypeTimeZoneMissingError(StructDataTypeError):
    @override
    def __str__(self) -> str:
        return "Time-zone must be given"


@dataclass(kw_only=True)
class _StructDataTypeNonStringLiteralError(StructDataTypeError):
    args: tuple[Any, ...]

    @override
    def __str__(self) -> str:
        return f"Literal arguments must be strings; got {self.args}"


@dataclass(kw_only=True)
class _StructDataTypeTypeError(StructDataTypeError):
    ann: Any

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {self.ann}"


def yield_struct_series_elements(
    series: Series, /
) -> Iterator[Mapping[str, Any] | None]:
    """Yield the elements of a struct-dtype Series as optional mappings."""
    if not isinstance(series.dtype, Struct):
        raise YieldStructSeriesElementsError(series=series)
    for is_null, value in zip(series.is_null(), series, strict=False):
        yield None if is_null else value


@dataclass(kw_only=True)
class YieldStructSeriesElementsError(Exception):
    series: Series

    @override
    def __str__(self) -> str:
        return f"Series must have Struct-dtype; got {self.series.dtype}"


__all__ = [
    "CheckPolarsDataFrameError",
    "ColumnsToDictError",
    "EmptyPolarsConcatError",
    "SetFirstRowAsColumnsError",
    "YieldStructSeriesElementsError",
    "ceil_datetime",
    "check_polars_dataframe",
    "collect_series",
    "columns_to_dict",
    "ensure_expr_or_series",
    "floor_datetime",
    "join",
    "nan_sum_agg",
    "nan_sum_cols",
    "redirect_empty_polars_concat",
    "set_first_row_as_columns",
    "struct_data_type",
    "yield_struct_series_elements",
]
