from __future__ import annotations

import datetime as dt
import reprlib
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from contextlib import suppress
from dataclasses import _MISSING_TYPE, asdict, dataclass, fields
from datetime import timezone
from enum import Enum
from functools import partial, reduce
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Never,
    TypeGuard,
    TypeVar,
    assert_never,
    cast,
    get_type_hints,
    overload,
)
from zoneinfo import ZoneInfo

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
    all_horizontal,
    col,
    concat,
    lit,
    struct,
    when,
)
from polars._typing import (
    IntoExpr,
    IntoExprColumn,
    JoinStrategy,
    JoinValidation,
    PolarsDataType,
    SchemaDict,
    TimeUnit,
)
from polars.datatypes import DataType
from polars.exceptions import ColumnNotFoundError, OutOfBoundsError
from polars.testing import assert_frame_equal
from typing_extensions import override

from utilities.dataclasses import Dataclass, is_dataclass_class
from utilities.iterables import (
    CheckIterablesEqualError,
    CheckMappingsEqualError,
    CheckSubSetError,
    CheckSuperMappingError,
    CheckSuperSetError,
    MaybeIterable,
    check_iterables_equal,
    check_mappings_equal,
    check_subset,
    check_supermapping,
    check_superset,
    is_iterable_not_str,
    one,
)
from utilities.math import (
    CheckIntegerError,
    _EWMParameters,
    check_integer,
    ewm_parameters,
)
from utilities.types import ensure_datetime
from utilities.typing import (
    get_args,
    is_frozenset_type,
    is_list_type,
    is_literal_type,
    is_optional_type,
    is_set_type,
)
from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from collections.abc import Set as AbstractSet


_T = TypeVar("_T")
_TDataclass = TypeVar("_TDataclass", bound=Dataclass)
DatetimeHongKong = Datetime(time_zone="Asia/Hong_Kong")
DatetimeTokyo = Datetime(time_zone="Asia/Tokyo")
DatetimeUSCentral = Datetime(time_zone="US/Central")
DatetimeUSEastern = Datetime(time_zone="US/Eastern")
DatetimeUTC = Datetime(time_zone="UTC")


def append_dataclass(df: DataFrame, obj: Dataclass, /) -> DataFrame:
    """Append a dataclass object to a DataFrame."""
    non_null_fields = {k: v for k, v in asdict(obj).items() if v is not None}
    try:
        check_subset(non_null_fields, df.columns)
    except CheckSubSetError as error:
        raise AppendDataClassError(
            left=error.left, right=error.right, extra=error.extra
        ) from None
    row_cols = set(df.columns) & set(non_null_fields)
    row = dataclass_to_row(obj).select(*row_cols)
    return concat([df, row], how="diagonal")


@dataclass(kw_only=True, slots=True)
class AppendDataClassError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return f"Dataclass fields {reprlib.repr(self.left)} must be a subset of DataFrame columns {reprlib.repr(self.right)}; dataclass had extra items {reprlib.repr(self.extra)}"


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
    columns: Iterable[str] | None = None,
    dtypes: Iterable[PolarsDataType] | None = None,
    height: int | tuple[int, float] | None = None,
    min_height: int | None = None,
    max_height: int | None = None,
    predicates: Mapping[str, Callable[[Any], bool]] | None = None,
    schema_list: SchemaDict | None = None,
    schema_set: SchemaDict | None = None,
    schema_subset: SchemaDict | None = None,
    shape: tuple[int, int] | None = None,
    sorted: MaybeIterable[IntoExpr] | None = None,  # noqa: A002
    unique: MaybeIterable[IntoExpr] | None = None,
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


@dataclass(kw_only=True, slots=True)
class CheckPolarsDataFrameError(Exception):
    df: DataFrame


def _check_polars_dataframe_columns(df: DataFrame, columns: Iterable[str], /) -> None:
    columns = list(columns)
    try:
        check_iterables_equal(df.columns, columns)
    except CheckIterablesEqualError as error:
        raise _CheckPolarsDataFrameColumnsError(df=df, columns=columns) from error


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameColumnsError(CheckPolarsDataFrameError):
    columns: Sequence[str]

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


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameSchemaSubsetError(CheckPolarsDataFrameError):
    schema: SchemaDict

    @override
    def __str__(self) -> str:
        return f"DataFrame schema must include {self.schema}; got {self.df.schema}\n\n{self.df}"


def _check_polars_dataframe_shape(df: DataFrame, shape: tuple[int, int], /) -> None:
    if df.shape != shape:
        raise _CheckPolarsDataFrameShapeError(df=df, shape=shape) from None


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameShapeError(CheckPolarsDataFrameError):
    shape: tuple[int, int]

    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have shape {self.shape}; got {self.df.shape}\n\n{self.df}"
        )


def _check_polars_dataframe_sorted(
    df: DataFrame, by: MaybeIterable[IntoExpr], /
) -> None:
    by_use = cast(
        IntoExpr | list[IntoExpr], list(by) if is_iterable_not_str(by) else by
    )
    df_sorted = df.sort(by_use)
    try:
        assert_frame_equal(df, df_sorted)
    except AssertionError as error:
        raise _CheckPolarsDataFrameSortedError(df=df, by=by_use) from error


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameSortedError(CheckPolarsDataFrameError):
    by: IntoExpr | list[IntoExpr]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be sorted on {self.by}\n\n{self.df}"


def _check_polars_dataframe_unique(
    df: DataFrame, by: MaybeIterable[IntoExpr], /
) -> None:
    by_use = cast(
        IntoExpr | list[IntoExpr], list(by) if is_iterable_not_str(by) else by
    )
    if df.select(by_use).is_duplicated().any():
        raise _CheckPolarsDataFrameUniqueError(df=df, by=by_use)


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameUniqueError(CheckPolarsDataFrameError):
    by: IntoExpr | list[IntoExpr]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be unique on {self.by}\n\n{self.df}"


def _check_polars_dataframe_width(df: DataFrame, width: int, /) -> None:
    if df.width != width:
        raise _CheckPolarsDataFrameWidthError(df=df, width=width)


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
class ColumnsToDictError(Exception):
    df: DataFrame
    key: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must be unique on {self.key!r}\n\n{self.df}"


@overload
def convert_time_zone(obj: Series, /, *, time_zone: ZoneInfo | str = ...) -> Series: ...
@overload
def convert_time_zone(
    obj: DataFrame, /, *, time_zone: ZoneInfo | str = ...
) -> DataFrame: ...
def convert_time_zone(
    obj: Series | DataFrame, /, *, time_zone: ZoneInfo | str = UTC
) -> Series | DataFrame:
    """Convert the time zone(s) of a Series or Dataframe."""
    match obj:
        case Series() as series:
            return _convert_time_zone_series(series, time_zone=time_zone)
        case DataFrame() as df:
            return df.select(
                *(
                    _convert_time_zone_series(df[c], time_zone=time_zone)
                    for c in df.columns
                )
            )
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _convert_time_zone_series(
    sr: Series, /, *, time_zone: ZoneInfo | str = UTC
) -> Series:
    if isinstance(sr.dtype, Struct):
        df = sr.struct.unnest()
        df = convert_time_zone(df, time_zone=time_zone).select(
            struct("*").alias(sr.name)
        )
        return df[sr.name]
    if isinstance(sr.dtype, Datetime):
        return sr.dt.convert_time_zone(get_time_zone_name(time_zone))
    return sr


def dataclass_to_row(obj: Dataclass, /) -> DataFrame:
    """Convert a dataclass into a 1-row DataFrame."""
    df = DataFrame([obj], orient="row")
    return reduce(partial(_dataclass_to_row_reducer, obj=obj), df.columns, df)


def _dataclass_to_row_reducer(
    df: DataFrame, column: str, /, *, obj: Dataclass
) -> DataFrame:
    dtype = df[column].dtype
    if isinstance(dtype, Datetime):
        datetime = ensure_datetime(getattr(obj, column), nullable=True)
        if (datetime is None) or (datetime.tzinfo is None):
            return df
        time_zone = ensure_time_zone(datetime.tzinfo)
        return df.with_columns(col(column).cast(zoned_datetime(time_zone=time_zone)))
    if isinstance(dtype, Struct):
        inner = dataclass_to_row(getattr(obj, column))
        return df.with_columns(**{column: inner.select(all=struct("*"))["all"]})
    return df


def drop_null_struct_series(series: Series, /) -> Series:
    """Drop nulls in a struct-dtype Series as per the <= 1.1 definition."""
    try:
        is_not_null = is_not_null_struct_series(series)
    except IsNotNullStructSeriesError as error:
        raise DropNullStructSeriesError(series=error.series) from None
    return series.filter(is_not_null)


@dataclass(kw_only=True, slots=True)
class DropNullStructSeriesError(Exception):
    series: Series

    @override
    def __str__(self) -> str:
        return f"Series must have Struct-dtype; got {self.series.dtype}"


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


def get_data_type_or_series_time_zone(
    dtype_or_series: DataType | Series, /
) -> ZoneInfo:
    """Get the time zone of a dtype/series."""
    if isinstance(dtype_or_series, DataType):
        dtype = dtype_or_series
    else:
        dtype = dtype_or_series.dtype
    if not isinstance(dtype, Datetime):
        raise _GetDataTypeOrSeriesTimeZoneNotDatetimeError(dtype=dtype)
    if dtype.time_zone is None:
        raise _GetDataTypeOrSeriesTimeZoneNotZonedError(dtype=dtype)
    return ZoneInfo(dtype.time_zone)


@dataclass(kw_only=True, slots=True)
class GetDataTypeOrSeriesTimeZoneError(Exception):
    dtype: DataType


@dataclass(kw_only=True, slots=True)
class _GetDataTypeOrSeriesTimeZoneNotDatetimeError(GetDataTypeOrSeriesTimeZoneError):
    @override
    def __str__(self) -> str:
        return f"Data type must be Datetime; got {self.dtype}"


@dataclass(kw_only=True, slots=True)
class _GetDataTypeOrSeriesTimeZoneNotZonedError(GetDataTypeOrSeriesTimeZoneError):
    @override
    def __str__(self) -> str:
        return f"Data type must be zoned; got {self.dtype}"


def is_not_null_struct_series(series: Series, /) -> Series:
    """Check if a struct-dtype Series is not null as per the <= 1.1 definition."""
    try:
        return ~is_null_struct_series(series)
    except IsNullStructSeriesError as error:
        raise IsNotNullStructSeriesError(series=error.series) from None


@dataclass(kw_only=True, slots=True)
class IsNotNullStructSeriesError(Exception):
    series: Series

    @override
    def __str__(self) -> str:
        return f"Series must have Struct-dtype; got {self.series.dtype}"


def is_null_struct_series(series: Series, /) -> Series:
    """Check if a struct-dtype Series is null as per the <= 1.1 definition."""
    if not isinstance(series.dtype, Struct):
        raise IsNullStructSeriesError(series=series)
    paths = _is_null_struct_series_one(series.dtype)
    paths = list(paths)
    exprs = map(_is_null_struct_to_expr, paths)
    expr = all_horizontal(*exprs)
    return (
        series.struct.unnest().with_columns(_result=expr)["_result"].rename(series.name)
    )


def _is_null_struct_series_one(
    dtype: Struct, /, *, root: Iterable[str] = ()
) -> Iterator[Sequence[str]]:
    for field in dtype.fields:
        name = field.name
        inner = field.dtype
        path = list(chain(root, [name]))
        if isinstance(inner, Struct):
            yield from _is_null_struct_series_one(inner, root=path)
        else:
            yield path


def _is_null_struct_to_expr(path: Iterable[str], /) -> Expr:
    head, *tail = path
    return reduce(_is_null_struct_to_expr_reducer, tail, col(head)).is_null()


def _is_null_struct_to_expr_reducer(expr: Expr, path: str, /) -> Expr:
    return expr.struct[path]


@dataclass(kw_only=True, slots=True)
class IsNullStructSeriesError(Exception):
    series: Series

    @override
    def __str__(self) -> str:
        return f"Series must have Struct-dtype; got {self.series.dtype}"


def join(
    df: DataFrame,
    *dfs: DataFrame,
    on: MaybeIterable[str | Expr],
    how: JoinStrategy = "inner",
    validate: JoinValidation = "m:m",
) -> DataFrame:
    """Join a set of DataFrames."""
    on_use = on if isinstance(on, str | Expr) else list(on)

    def inner(left: DataFrame, right: DataFrame, /) -> DataFrame:
        return left.join(right, on=on_use, how=how, validate=validate)

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


@overload
def replace_time_zone(
    obj: Series, /, *, time_zone: ZoneInfo | str | None = ...
) -> Series: ...
@overload
def replace_time_zone(
    obj: DataFrame, /, *, time_zone: ZoneInfo | str | None = ...
) -> DataFrame: ...
def replace_time_zone(
    obj: Series | DataFrame, /, *, time_zone: ZoneInfo | str | None = UTC
) -> Series | DataFrame:
    """Replace the time zone(s) of a Series or Dataframe."""
    match obj:
        case Series() as series:
            return _replace_time_zone_series(series, time_zone=time_zone)
        case DataFrame() as df:
            return df.select(
                *(
                    _replace_time_zone_series(df[c], time_zone=time_zone)
                    for c in df.columns
                )
            )
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _replace_time_zone_series(
    sr: Series, /, *, time_zone: ZoneInfo | str | None = UTC
) -> Series:
    if isinstance(sr.dtype, Struct):
        df = sr.struct.unnest()
        df = replace_time_zone(df, time_zone=time_zone).select(
            struct("*").alias(sr.name)
        )
        return df[sr.name]
    if isinstance(sr.dtype, Datetime):
        time_zone_use = None if time_zone is None else get_time_zone_name(time_zone)
        return sr.dt.replace_time_zone(time_zone_use)
    return sr


@overload
def rolling_parameters(
    *,
    s_window: int,
    e_com: None = None,
    e_span: None = None,
    e_half_life: None = None,
    e_alpha: None = None,
    min_periods: int | None = None,
) -> RollingParametersSimple: ...
@overload
def rolling_parameters(
    *,
    s_window: None = None,
    e_com: float | None = None,
    e_span: float | None = None,
    e_half_life: float | None = None,
    e_alpha: float | None = None,
    min_periods: int,
) -> RollingParametersExponential: ...
@overload
def rolling_parameters(
    *,
    s_window: int | None = None,
    e_com: float | None = None,
    e_span: float | None = None,
    e_half_life: float | None = None,
    e_alpha: float | None = None,
    min_periods: int | None = None,
) -> RollingParametersSimple | RollingParametersExponential: ...
def rolling_parameters(
    *,
    s_window: int | None = None,
    e_com: float | None = None,
    e_span: float | None = None,
    e_half_life: float | None = None,
    e_alpha: float | None = None,
    min_periods: int | None = None,
) -> RollingParametersSimple | RollingParametersExponential:
    """Resolve a set of rolling parameters."""
    if (
        (s_window is not None)
        and (e_com is None)
        and (e_span is None)
        and (e_half_life is None)
        and (e_alpha is None)
    ):
        return RollingParametersSimple(window=s_window, min_periods=min_periods)
    if (s_window is None) and (
        (e_com is not None)
        or (e_span is not None)
        or (e_half_life is not None)
        or (e_alpha is not None)
    ):
        if min_periods is None:
            raise _RollingParametersMinPeriodsError(
                e_com=e_com,
                e_span=e_span,
                e_half_life=e_half_life,
                e_alpha=e_alpha,
                min_periods=min_periods,
            )
        params = ewm_parameters(
            com=e_com, span=e_span, half_life=e_half_life, alpha=e_alpha
        )
        return RollingParametersExponential(
            com=params.com,
            span=params.span,
            half_life=params.half_life,
            alpha=params.alpha,
            min_periods=min_periods,
        )
    raise _RollingParametersArgumentsError(
        s_window=s_window,
        e_com=e_com,
        e_span=e_span,
        e_half_life=e_half_life,
        e_alpha=e_alpha,
    )


@dataclass(kw_only=True, slots=True)
class RollingParametersError(Exception):
    s_window: int | None = None
    e_com: float | None = None
    e_span: float | None = None
    e_half_life: float | None = None
    e_alpha: float | None = None


@dataclass(kw_only=True, slots=True)
class _RollingParametersArgumentsError(RollingParametersError):
    @override
    def __str__(self) -> str:
        return f"Exactly one of simple window, exponential center of mass (γ), exponential span (θ), exponential half-life (λ) or exponential smoothing factor (α) must be given; got s_window={self.s_window}, γ={self.e_com}, θ={self.e_span}, λ={self.e_half_life} and α={self.e_alpha}"  # noqa: RUF001


@dataclass(kw_only=True, slots=True)
class _RollingParametersMinPeriodsError(RollingParametersError):
    min_periods: int | None = None

    @override
    def __str__(self) -> str:
        return f"Exponential rolling requires 'min_periods' to be set; got {self.min_periods}"


@dataclass(kw_only=True, slots=True)
class RollingParametersSimple:
    window: int
    min_periods: int | None = None


@dataclass(kw_only=True, slots=True)
class RollingParametersExponential(_EWMParameters):
    min_periods: int


def set_first_row_as_columns(df: DataFrame, /) -> DataFrame:
    """Set the first row of a DataFrame as its columns."""
    try:
        row = df.row(0)
    except OutOfBoundsError:
        raise SetFirstRowAsColumnsError(df=df) from None
    mapping = dict(zip(df.columns, row, strict=True))
    return df[1:].rename(mapping)


@dataclass(kw_only=True, slots=True)
class SetFirstRowAsColumnsError(Exception):
    df: DataFrame

    @override
    def __str__(self) -> str:
        return f"DataFrame must have at least 1 row; got {self.df}"


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
) -> PolarsDataType:
    mapping = {bool: Boolean, dt.date: Date, float: Float64, int: Int64, str: Utf8}
    with suppress(KeyError):
        return mapping[ann]
    if ann is dt.datetime:
        if time_zone is None:
            raise _StructDataTypeTimeZoneMissingError
        return zoned_datetime(time_zone=time_zone)
    if is_dataclass_class(ann):
        return struct_data_type(ann, time_zone=time_zone)
    if (isinstance(ann, type) and issubclass(ann, Enum)) or (
        is_literal_type(ann) and all(isinstance(a, str) for a in get_args(ann))
    ):
        return Utf8
    if is_optional_type(ann):
        return _struct_data_type_one(one(get_args(ann)), time_zone=time_zone)
    if is_frozenset_type(ann) or is_list_type(ann) or is_set_type(ann):
        return List(_struct_data_type_one(one(get_args(ann)), time_zone=time_zone))
    raise _StructDataTypeTypeError(ann=ann)


@dataclass(kw_only=True, slots=True)
class StructDataTypeError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _StructDataTypeNotADataclassError(StructDataTypeError):
    cls: type[Dataclass]

    @override
    def __str__(self) -> str:
        return f"Object must be a dataclass; got {self.cls}"


@dataclass(kw_only=True, slots=True)
class _StructDataTypeTimeZoneMissingError(StructDataTypeError):
    @override
    def __str__(self) -> str:
        return "Time-zone must be given"


@dataclass(kw_only=True, slots=True)
class _StructDataTypeTypeError(StructDataTypeError):
    ann: Any

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {self.ann}"


def yield_rows_as_dataclasses(
    df: DataFrame,
    cls: type[_TDataclass],
    /,
    *,
    check_types: Literal["none", "first", "all"] = "first",
) -> Iterator[_TDataclass]:
    """Yield the rows of a DataFrame as dataclasses."""
    from dacite import from_dict
    from dacite.exceptions import WrongTypeError

    columns = df.columns
    required: set[str] = set()
    for field in fields(cls):
        if isinstance(field.default, _MISSING_TYPE) and isinstance(
            field.default_factory, _MISSING_TYPE
        ):
            required.add(field.name)
    try:
        check_superset(columns, required)
    except CheckSuperSetError as error:
        raise _YieldRowsAsDataClassesColumnsSuperSetError(
            df=df, cls=cls, left=error.left, right=error.right, extra=error.extra
        ) from None
    rows = df.iter_rows(named=True)
    match check_types:
        case "none":
            yield from _yield_rows_as_dataclasses_no_check_types(rows, cls)
        case "first":
            try:
                first = next(rows)
            except StopIteration:
                return
            try:
                yield from_dict(cls, first)
            except WrongTypeError as error:
                raise _YieldRowsAsDataClassesWrongTypeError(
                    df=df, cls=cls, msg=str(error)
                ) from None
            yield from _yield_rows_as_dataclasses_no_check_types(rows, cls)
        case "all":
            try:
                for row in rows:
                    yield from_dict(cls, row)
            except WrongTypeError as error:
                raise _YieldRowsAsDataClassesWrongTypeError(
                    df=df, cls=cls, msg=str(error)
                ) from None
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _yield_rows_as_dataclasses_no_check_types(
    rows: Iterator[dict[str, Any]], cls: type[_TDataclass], /
) -> Iterator[_TDataclass]:
    """Yield the rows of a DataFrame as dataclasses without type checking."""
    from dacite import Config, from_dict

    config = Config(check_types=False)
    for row in rows:
        yield from_dict(cls, row, config=config)


@dataclass(kw_only=True, slots=True)
class YieldRowsAsDataClassesError(Exception):
    df: DataFrame
    cls: type[Dataclass]


@dataclass(kw_only=True, slots=True)
class _YieldRowsAsDataClassesColumnsSuperSetError(YieldRowsAsDataClassesError):
    left: AbstractSet[str]
    right: AbstractSet[str]
    extra: AbstractSet[str]

    @override
    def __str__(self) -> str:
        return f"DataFrame columns {reprlib.repr(self.left)} must be a superset of dataclass fields {reprlib.repr(self.right)}; dataclass had extra fields {reprlib.repr(self.extra)}."


@dataclass(kw_only=True, slots=True)
class _YieldRowsAsDataClassesWrongTypeError(YieldRowsAsDataClassesError):
    msg: str

    @override
    def __str__(self) -> str:
        return self.msg


@overload
def yield_struct_series_elements(
    series: Series, /, *, strict: Literal[True]
) -> Iterator[Mapping[str, Any]]: ...
@overload
def yield_struct_series_elements(
    series: Series, /, *, strict: bool = False
) -> Iterator[Mapping[str, Any] | None]: ...
def yield_struct_series_elements(
    series: Series, /, *, strict: bool = False
) -> Iterator[Mapping[str, Any] | None]:
    """Yield the elements of a struct-dtype Series as optional mappings."""
    if not isinstance(series.dtype, Struct):
        raise _YieldStructSeriesElementsDTypeError(series=series)
    if strict and series.is_null().any():
        raise _YieldStructSeriesElementsNullElementsError(series=series)
    for value in series:
        yield _yield_struct_series_element_remove_nulls(value)


def _yield_struct_series_element_remove_nulls(obj: Any, /) -> Any:
    if not _yield_struct_series_element_is_mapping_of_str(obj):
        return obj
    if any(_yield_struct_series_element_is_mapping_of_str(v) for v in obj.values()):
        result = {
            k: _yield_struct_series_element_remove_nulls(v) for k, v in obj.items()
        }
        if result == obj:
            return result
        return _yield_struct_series_element_remove_nulls(result)
    return None if all(v is None for v in obj.values()) else obj


def _yield_struct_series_element_is_mapping_of_str(
    obj: Any, /
) -> TypeGuard[Mapping[str, Any]]:
    return isinstance(obj, Mapping) and all(isinstance(k, str) for k in obj)


@dataclass(kw_only=True, slots=True)
class YieldStructSeriesElementsError(Exception):
    series: Series


@dataclass(kw_only=True, slots=True)
class _YieldStructSeriesElementsDTypeError(YieldStructSeriesElementsError):
    @override
    def __str__(self) -> str:
        return f"Series must have Struct-dtype; got {self.series.dtype}"


@dataclass(kw_only=True, slots=True)
class _YieldStructSeriesElementsNullElementsError(YieldStructSeriesElementsError):
    @override
    def __str__(self) -> str:
        return f"Series must not have nulls; got {self.series}"


@overload
def yield_struct_series_dataclasses(
    series: Series,
    cls: type[_TDataclass],
    /,
    *,
    check_types: bool = ...,
    strict: Literal[True],
) -> Iterator[_TDataclass]: ...
@overload
def yield_struct_series_dataclasses(
    series: Series,
    cls: type[_TDataclass],
    /,
    *,
    check_types: bool = ...,
    strict: bool = False,
) -> Iterator[_TDataclass | None]: ...
def yield_struct_series_dataclasses(
    series: Series,
    cls: type[_TDataclass],
    /,
    *,
    check_types: bool = True,
    strict: bool = False,
) -> Iterator[_TDataclass | None]:
    """Yield the elements of a struct-dtype Series as dataclasses."""
    from dacite import Config, from_dict

    config = Config(check_types=check_types, strict=True)
    for value in yield_struct_series_elements(series, strict=strict):
        yield None if value is None else from_dict(cls, value, config=config)


def zoned_datetime(
    *, time_unit: TimeUnit = "us", time_zone: ZoneInfo | str | timezone = UTC
) -> Datetime:
    """Create a zoned datetime data type."""
    return Datetime(time_unit=time_unit, time_zone=get_time_zone_name(time_zone))


__all__ = [
    "CheckPolarsDataFrameError",
    "ColumnsToDictError",
    "DatetimeHongKong",
    "DatetimeTokyo",
    "DatetimeUSCentral",
    "DatetimeUSEastern",
    "DatetimeUTC",
    "DropNullStructSeriesError",
    "GetDataTypeOrSeriesTimeZoneError",
    "IsNullStructSeriesError",
    "RollingParametersError",
    "RollingParametersExponential",
    "RollingParametersSimple",
    "SetFirstRowAsColumnsError",
    "YieldRowsAsDataClassesError",
    "YieldStructSeriesElementsError",
    "append_dataclass",
    "ceil_datetime",
    "check_polars_dataframe",
    "collect_series",
    "columns_to_dict",
    "convert_time_zone",
    "dataclass_to_row",
    "drop_null_struct_series",
    "ensure_expr_or_series",
    "floor_datetime",
    "get_data_type_or_series_time_zone",
    "is_not_null_struct_series",
    "is_null_struct_series",
    "join",
    "nan_sum_agg",
    "nan_sum_cols",
    "replace_time_zone",
    "rolling_parameters",
    "set_first_row_as_columns",
    "struct_data_type",
    "yield_rows_as_dataclasses",
    "yield_struct_series_dataclasses",
    "yield_struct_series_elements",
    "zoned_datetime",
]
