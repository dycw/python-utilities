from __future__ import annotations

import datetime as dt
import enum
from collections.abc import Callable, Iterator, Sequence
from collections.abc import Set as AbstractSet
from contextlib import suppress
from dataclasses import asdict, dataclass
from functools import partial, reduce
from itertools import chain, product
from math import ceil, log
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    assert_never,
    cast,
    overload,
    override,
)
from uuid import UUID
from zoneinfo import ZoneInfo

import polars as pl
from polars import (
    Boolean,
    DataFrame,
    Date,
    Datetime,
    Expr,
    Float64,
    Int64,
    List,
    Object,
    Series,
    String,
    Struct,
    UInt32,
    all_horizontal,
    any_horizontal,
    col,
    concat,
    int_range,
    lit,
    struct,
    sum_horizontal,
    when,
)
from polars.datatypes import DataType, DataTypeClass
from polars.exceptions import (
    ColumnNotFoundError,
    NoRowsReturnedError,
    OutOfBoundsError,
    PolarsInefficientMapWarning,
)
from polars.testing import assert_frame_equal

from utilities.dataclasses import _YieldFieldsInstance, yield_fields
from utilities.errors import ImpossibleCaseError
from utilities.functions import (
    EnsureIntError,
    ensure_int,
    is_dataclass_class,
    is_dataclass_instance,
    is_iterable_of,
    make_isinstance,
)
from utilities.iterables import (
    CheckIterablesEqualError,
    CheckMappingsEqualError,
    CheckSubSetError,
    CheckSuperMappingError,
    OneEmptyError,
    OneNonUniqueError,
    always_iterable,
    check_iterables_equal,
    check_mappings_equal,
    check_subset,
    check_supermapping,
    is_iterable_not_str,
    one,
)
from utilities.math import (
    CheckIntegerError,
    check_integer,
    ewm_parameters,
    is_less_than,
    is_non_negative,
    number_of_decimals,
)
from utilities.reprlib import get_repr
from utilities.types import MaybeStr, Number, WeekDay
from utilities.typing import (
    get_args,
    get_type_hints,
    is_frozenset_type,
    is_instance_gen,
    is_list_type,
    is_literal_type,
    is_optional_type,
    is_set_type,
    is_union_type,
)
from utilities.warnings import suppress_warnings
from utilities.zoneinfo import UTC, ensure_time_zone, get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from polars._typing import (
        IntoExpr,  # pyright: ignore[reportPrivateImportUsage]
        IntoExprColumn,  # pyright: ignore[reportPrivateImportUsage]
        JoinStrategy,  # pyright: ignore[reportPrivateImportUsage]
        JoinValidation,  # pyright: ignore[reportPrivateImportUsage]
        PolarsDataType,  # pyright: ignore[reportPrivateImportUsage]
        RollingInterpolationMethod,  # pyright: ignore[reportPrivateImportUsage]
        SchemaDict,  # pyright: ignore[reportPrivateImportUsage]
        TimeUnit,  # pyright: ignore[reportPrivateImportUsage]
    )

    from utilities.numpy import NDArrayB, NDArrayF
    from utilities.statsmodels import ACFMissing
    from utilities.types import Dataclass, MaybeIterable, StrMapping, TimeZoneLike


_T = TypeVar("_T")
type ExprLike = MaybeStr[Expr]
DatetimeHongKong = Datetime(time_zone="Asia/Hong_Kong")
DatetimeTokyo = Datetime(time_zone="Asia/Tokyo")
DatetimeUSCentral = Datetime(time_zone="US/Central")
DatetimeUSEastern = Datetime(time_zone="US/Eastern")
DatetimeUTC = Datetime(time_zone="UTC")
_FINITE_EWM_MIN_WEIGHT = 0.9999

##


def ac_halflife(
    series: Series,
    /,
    *,
    adjusted: bool = False,
    fft: bool = True,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
    step: float = 0.01,
) -> float:
    """Compute the autocorrelation halflife."""
    import utilities.statsmodels

    array = series.to_numpy()
    return utilities.statsmodels.ac_halflife(
        array,
        adjusted=adjusted,
        fft=fft,
        bartlett_confint=bartlett_confint,
        missing=missing,
        step=step,
    )


##


def acf(
    series: Series,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: bool = False,
    fft: bool = True,
    alpha: float | None = None,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> DataFrame:
    """Compute the autocorrelations of a series."""
    from numpy import ndarray

    import utilities.statsmodels

    array = series.to_numpy()
    result = utilities.statsmodels.acf(
        array,
        adjusted=adjusted,
        nlags=nlags,
        qstat=qstat,
        fft=fft,
        alpha=alpha,
        bartlett_confint=bartlett_confint,
        missing=missing,
    )
    match result:
        case ndarray() as acfs:
            return _acf_process_acfs(acfs)
        case ndarray() as acfs, ndarray() as confints:
            df_acfs = _acf_process_acfs(acfs)
            df_confints = _acf_process_confints(confints)
            return df_acfs.join(df_confints, on=["lag"])
        case ndarray() as acfs, ndarray() as qstats, ndarray() as pvalues:
            df_acfs = _acf_process_acfs(acfs)
            df_qstats_pvalues = _acf_process_qstats_pvalues(qstats, pvalues)
            return df_acfs.join(df_qstats_pvalues, on=["lag"], how="left")
        case (
            ndarray() as acfs,
            ndarray() as confints,
            ndarray() as qstats,
            ndarray() as pvalues,
        ):
            df_acfs = _acf_process_acfs(acfs)
            df_confints = _acf_process_confints(confints)
            df_qstats_pvalues = _acf_process_qstats_pvalues(qstats, pvalues)
            return join(df_acfs, df_confints, df_qstats_pvalues, on=["lag"], how="left")
        case _ as never:
            assert_never(never)


def _acf_process_acfs(acfs: NDArrayF, /) -> DataFrame:
    return (
        Series(name="autocorrelation", values=acfs, dtype=Float64)
        .to_frame()
        .with_row_index(name="lag")
    )


def _acf_process_confints(confints: NDArrayF, /) -> DataFrame:
    return DataFrame(
        data=confints, schema={"lower": Float64, "upper": Float64}
    ).with_row_index(name="lag")


def _acf_process_qstats_pvalues(qstats: NDArrayF, pvalues: NDArrayF, /) -> DataFrame:
    from numpy import hstack

    data = hstack([qstats.reshape(-1, 1), pvalues.reshape(-1, 1)])
    return DataFrame(
        data=data, schema={"qstat": Float64, "pvalue": Float64}
    ).with_row_index(name="lag", offset=1)


##


# def acf_halflife(series: Series,/)

##


def adjust_frequencies(
    series: Series,
    /,
    *,
    filters: MaybeIterable[Callable[[NDArrayF], NDArrayB]] | None = None,
    weights: MaybeIterable[Callable[[NDArrayF], NDArrayF]] | None = None,
    d: int = 1,
) -> Series:
    """Adjust a Series via its FFT frequencies."""
    import utilities.numpy

    array = series.to_numpy()
    adjusted = utilities.numpy.adjust_frequencies(
        array, filters=filters, weights=weights, d=d
    )
    return Series(name=series.name, values=adjusted, dtype=Float64)


##


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
    row = dataclass_to_dataframe(obj).select(*row_cols)
    return concat([df, row], how="diagonal")


@dataclass(kw_only=True, slots=True)
class AppendDataClassError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return f"Dataclass fields {get_repr(self.left)} must be a subset of DataFrame columns {get_repr(self.right)}; dataclass had extra items {get_repr(self.extra)}"


##


def are_frames_equal(
    left: DataFrame,
    right: DataFrame,
    /,
    *,
    check_row_order: bool = True,
    check_column_order: bool = True,
    check_dtypes: bool = True,
    check_exact: bool = False,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    categorical_as_str: bool = False,
) -> bool:
    """Check if two DataFrames are equal."""
    try:
        assert_frame_equal(
            left,
            right,
            check_row_order=check_row_order,
            check_column_order=check_column_order,
            check_dtypes=check_dtypes,
            check_exact=check_exact,
            rtol=rtol,
            atol=atol,
            categorical_as_str=categorical_as_str,
        )
    except AssertionError:
        return False
    return True


##


def bernoulli(
    obj: int | Series | DataFrame,
    /,
    *,
    true: float = 0.5,
    seed: int | None = None,
    name: str | None = None,
) -> Series:
    """Construct a series of Bernoulli-random variables."""
    match obj:
        case int() as height:
            import utilities.numpy

            values = utilities.numpy.bernoulli(true=true, seed=seed, size=height)
            return Series(name=name, values=values)
        case Series() as series:
            return bernoulli(series.len(), true=true, seed=seed, name=name)
        case DataFrame() as df:
            return bernoulli(df.height, true=true, seed=seed, name=name)
        case _ as never:
            assert_never(never)


##


def boolean_value_counts(
    obj: Series | DataFrame, /, *exprs: IntoExprColumn, **named_exprs: IntoExprColumn
) -> DataFrame:
    """Conduct a set of boolean value counts."""
    match obj:
        case Series() as series:
            return boolean_value_counts(series.to_frame(), *exprs, **named_exprs)
        case DataFrame() as df:
            all_exprs = ensure_expr_or_series_many(*exprs, **named_exprs)
            rows = [_boolean_value_counts_one(df, expr) for expr in all_exprs]
            true, false, null = [col(c) for c in ["true", "false", "null"]]
            total = sum_horizontal(true, false, null).alias("total")
            return DataFrame(
                rows,
                schema={
                    "name": String,
                    "true": UInt32,
                    "false": UInt32,
                    "null": UInt32,
                },
                orient="row",
            ).with_columns(
                total,
                (true / total).alias("true (%)"),
                (false / total).alias("false (%)"),
                (null / total).alias("null (%)"),
            )
        case _ as never:
            assert_never(never)


def _boolean_value_counts_one(
    df: DataFrame, expr: IntoExprColumn, /
) -> Mapping[str, Any]:
    name = get_expr_name(df, expr)
    sr = df.select(expr)[name]
    if not isinstance(sr.dtype, Boolean):
        raise BooleanValueCountsError(name=name, dtype=sr.dtype)
    counts = sr.value_counts()
    truth = col(name)
    try:
        true = counts.row(by_predicate=truth.is_not_null() & truth, named=True)["count"]
    except NoRowsReturnedError:
        true = 0
    try:
        false = counts.row(by_predicate=(truth.is_not_null() & ~truth), named=True)[
            "count"
        ]
    except NoRowsReturnedError:
        false = 0
    try:
        null = counts.row(by_predicate=truth.is_null(), named=True)["count"]
    except NoRowsReturnedError:
        null = 0
    return {"name": name, "true": true, "false": false, "null": null}


@dataclass(kw_only=True, slots=True)
class BooleanValueCountsError(Exception):
    name: str
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"Column {self.name!r} must be Boolean; got {self.dtype!r}"


##


@overload
def ceil_datetime(column: ExprLike, every: ExprLike, /) -> Expr: ...
@overload
def ceil_datetime(column: Series, every: ExprLike, /) -> Series: ...
@overload
def ceil_datetime(column: IntoExprColumn, every: ExprLike, /) -> Expr | Series: ...
def ceil_datetime(column: IntoExprColumn, every: ExprLike, /) -> Expr | Series:
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


##


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
        return f"DataFrame must have columns {get_repr(self.columns)}; got {get_repr(self.df.columns)}:\n\n{self.df}"


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
        return f"DataFrame must have dtypes {get_repr(self.dtypes)}; got {get_repr(self.df.dtypes)}:\n\n{self.df}"


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
        return f"DataFrame must satisfy the height requirements; got {self.df.height}:\n\n{self.df}"


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
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"DataFrame must satisfy the predicates; {desc}:\n\n{self.df}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.missing) >= 1:
            yield f"missing columns were {get_repr(self.missing)}"
        if len(self.failed) >= 1:
            yield f"failed predicates were {get_repr(self.failed)}"


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
        return f"DataFrame must have schema {get_repr(self.schema)} (ordered); got {get_repr(self.df.schema)}:\n\n{self.df}"


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
        return f"DataFrame must have schema {get_repr(self.schema)} (unordered); got {get_repr(self.df.schema)}:\n\n{self.df}"


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
        return f"DataFrame schema must include {get_repr(self.schema)} (unordered); got {get_repr(self.df.schema)}:\n\n{self.df}"


def _check_polars_dataframe_shape(df: DataFrame, shape: tuple[int, int], /) -> None:
    if df.shape != shape:
        raise _CheckPolarsDataFrameShapeError(df=df, shape=shape) from None


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameShapeError(CheckPolarsDataFrameError):
    shape: tuple[int, int]

    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have shape {self.shape}; got {self.df.shape}:\n\n{self.df}"
        )


def _check_polars_dataframe_sorted(
    df: DataFrame, by: MaybeIterable[IntoExpr], /
) -> None:
    by_use = cast(
        "IntoExpr | list[IntoExpr]", list(by) if is_iterable_not_str(by) else by
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
        return f"DataFrame must be sorted on {get_repr(self.by)}:\n\n{self.df}"


def _check_polars_dataframe_unique(
    df: DataFrame, by: MaybeIterable[IntoExpr], /
) -> None:
    by_use = cast(
        "IntoExpr | list[IntoExpr]", list(by) if is_iterable_not_str(by) else by
    )
    if df.select(by_use).is_duplicated().any():
        raise _CheckPolarsDataFrameUniqueError(df=df, by=by_use)


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameUniqueError(CheckPolarsDataFrameError):
    by: IntoExpr | list[IntoExpr]

    @override
    def __str__(self) -> str:
        return f"DataFrame must be unique on {get_repr(self.by)}:\n\n{self.df}"


def _check_polars_dataframe_width(df: DataFrame, width: int, /) -> None:
    if df.width != width:
        raise _CheckPolarsDataFrameWidthError(df=df, width=width)


@dataclass(kw_only=True, slots=True)
class _CheckPolarsDataFrameWidthError(CheckPolarsDataFrameError):
    width: int

    @override
    def __str__(self) -> str:
        return (
            f"DataFrame must have width {self.width}; got {self.df.width}:\n\n{self.df}"
        )


##


def choice(
    obj: int | Series | DataFrame,
    elements: Iterable[Any],
    /,
    *,
    replace: bool = True,
    p: Iterable[float] | None = None,
    seed: int | None = None,
    name: str | None = None,
    dtype: PolarsDataType = Float64,
) -> Series:
    """Construct a series of random samples."""
    match obj:
        case int() as height:
            from numpy.random import default_rng

            rng = default_rng(seed=seed)
            elements = list(elements)
            p = None if p is None else list(p)
            values = rng.choice(elements, size=height, replace=replace, p=p)
            return Series(name=name, values=values.tolist(), dtype=dtype)
        case Series() as series:
            return choice(
                series.len(),
                elements,
                replace=replace,
                p=p,
                seed=seed,
                name=name,
                dtype=dtype,
            )
        case DataFrame() as df:
            return choice(
                df.height,
                elements,
                replace=replace,
                p=p,
                seed=seed,
                name=name,
                dtype=dtype,
            )
        case _ as never:
            assert_never(never)


##


def collect_series(expr: Expr, /) -> Series:
    """Collect a column expression into a Series."""
    data = DataFrame().with_columns(expr)
    return data[one(data.columns)]


##


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
        return f"DataFrame must be unique on {self.key!r}:\n\n{self.df}"


##


def concat_series(*series: Series) -> DataFrame:
    """Horizontally concatenate a set of Series."""
    return concat([s.to_frame() for s in series], how="horizontal")


##


@overload
def convert_time_zone(obj: Series, /, *, time_zone: TimeZoneLike = UTC) -> Series: ...
@overload
def convert_time_zone(
    obj: DataFrame, /, *, time_zone: TimeZoneLike = UTC
) -> DataFrame: ...
@overload
def convert_time_zone(
    obj: Series | DataFrame, /, *, time_zone: TimeZoneLike = UTC
) -> Series | DataFrame: ...
def convert_time_zone(
    obj: Series | DataFrame, /, *, time_zone: TimeZoneLike = UTC
) -> Series | DataFrame:
    """Convert the time zone(s) of a Series or Dataframe."""
    return map_over_columns(partial(_convert_time_zone_one, time_zone=time_zone), obj)


def _convert_time_zone_one(sr: Series, /, *, time_zone: TimeZoneLike = UTC) -> Series:
    if isinstance(sr.dtype, Datetime):
        return sr.dt.convert_time_zone(get_time_zone_name(time_zone))
    return sr


##


@overload
def cross(
    expr: ExprLike, up_or_down: Literal["up", "down"], other: Number | ExprLike, /
) -> Expr: ...
@overload
def cross(
    expr: Series, up_or_down: Literal["up", "down"], other: Number | Series, /
) -> Series: ...
@overload
def cross(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> Expr | Series: ...
def cross(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> Expr | Series:
    """Compute when a cross occurs."""
    return _cross_or_touch(expr, "cross", up_or_down, other)


@overload
def touch(
    expr: ExprLike, up_or_down: Literal["up", "down"], other: Number | ExprLike, /
) -> Expr: ...
@overload
def touch(
    expr: Series, up_or_down: Literal["up", "down"], other: Number | Series, /
) -> Series: ...
@overload
def touch(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> Expr | Series: ...
def touch(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> Expr | Series:
    """Compute when a touch occurs."""
    return _cross_or_touch(expr, "touch", up_or_down, other)


def _cross_or_touch(
    expr: IntoExprColumn,
    cross_or_touch: Literal["cross", "touch"],
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> Expr | Series:
    """Compute when a column crosses/touches a threshold."""
    expr = ensure_expr_or_series(expr)
    match other:
        case int() | float():
            ...
        case str() | Expr() | Series():
            other = ensure_expr_or_series(other)
        case _ as never:
            assert_never(never)
    enough = int_range(end=pl.len()) >= 1
    match cross_or_touch, up_or_down:
        case "cross", "up":
            current = expr > other
        case "cross", "down":
            current = expr < other
        case "touch", "up":
            current = expr >= other
        case "touch", "down":
            current = expr <= other
        case _ as never:
            assert_never(never)
    prev = current.shift()
    result = when(enough & expr.is_finite()).then(current & ~prev)
    match expr, other:
        case Series(), int() | float() | Series():
            return expr.to_frame().with_columns(result.alias(expr.name))[expr.name]
        case _:
            return result


##


@overload
def cross_rolling_quantile(
    expr: ExprLike,
    up_or_down: Literal["up", "down"],
    quantile: float,
    /,
    *,
    interpolation: RollingInterpolationMethod = "nearest",
    window_size: int = 2,
    weights: list[float] | None = None,
    min_samples: int | None = None,
    center: bool = False,
) -> Expr: ...
@overload
def cross_rolling_quantile(
    expr: Series,
    up_or_down: Literal["up", "down"],
    quantile: float,
    /,
    *,
    interpolation: RollingInterpolationMethod = "nearest",
    window_size: int = 2,
    weights: list[float] | None = None,
    min_samples: int | None = None,
    center: bool = False,
) -> Series: ...
@overload
def cross_rolling_quantile(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    quantile: float,
    /,
    *,
    interpolation: RollingInterpolationMethod = "nearest",
    window_size: int = 2,
    weights: list[float] | None = None,
    min_samples: int | None = None,
    center: bool = False,
) -> Expr | Series: ...
def cross_rolling_quantile(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    quantile: float,
    /,
    *,
    interpolation: RollingInterpolationMethod = "nearest",
    window_size: int = 2,
    weights: list[float] | None = None,
    min_samples: int | None = None,
    center: bool = False,
) -> Expr | Series:
    """Compute when a column crosses its rolling quantile."""
    expr = ensure_expr_or_series(expr)
    rolling = expr.rolling_quantile(
        quantile,
        interpolation=interpolation,
        window_size=window_size,
        weights=weights,
        min_samples=min_samples,
        center=center,
    )
    return cross(expr, up_or_down, rolling)


##


def dataclass_to_dataframe(
    objs: MaybeIterable[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> DataFrame:
    """Convert a dataclass/es into a DataFrame."""
    objs = list(always_iterable(objs))
    try:
        _ = one(set(map(type, objs)))
    except OneEmptyError:
        raise _DataClassToDataFrameEmptyError from None
    except OneNonUniqueError as error:
        raise _DataClassToDataFrameNonUniqueError(
            objs=objs, first=error.first, second=error.second
        ) from None
    data = list(map(asdict, objs))
    first, *_ = objs
    schema = dataclass_to_schema(first, globalns=globalns, localns=localns)
    df = DataFrame(data, schema=schema, orient="row")
    return map_over_columns(_dataclass_to_dataframe_uuid, df)


def _dataclass_to_dataframe_uuid(series: Series, /) -> Series:
    if series.dtype == Object:
        is_path = series.map_elements(make_isinstance(Path), return_dtype=Boolean).all()
        is_uuid = series.map_elements(make_isinstance(UUID), return_dtype=Boolean).all()
        if is_path or is_uuid:
            with suppress_warnings(category=PolarsInefficientMapWarning):
                return series.map_elements(str, return_dtype=String)
        else:  # pragma: no cover
            msg = f"{is_path=}, f{is_uuid=}"
            raise NotImplementedError(msg)
    return series


@dataclass(kw_only=True, slots=True)
class DataClassToDataFrameError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _DataClassToDataFrameEmptyError(DataClassToDataFrameError):
    @override
    def __str__(self) -> str:
        return "At least 1 dataclass must be given; got 0"


@dataclass(kw_only=True, slots=True)
class _DataClassToDataFrameNonUniqueError(DataClassToDataFrameError):
    objs: list[Dataclass]
    first: Any
    second: Any

    @override
    def __str__(self) -> str:
        return f"Iterable {get_repr(self.objs)} must contain exactly 1 class; got {self.first}, {self.second} and perhaps more"


##


def dataclass_to_schema(
    obj: Dataclass,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
) -> SchemaDict:
    """Cast a dataclass as a schema dict."""
    out: dict[str, Any] = {}
    for field in yield_fields(
        obj, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
    ):
        if is_dataclass_instance(field.value):
            dtypes = dataclass_to_schema(
                field.value, globalns=globalns, localns=localns
            )
            dtype = struct_dtype(**dtypes)
        elif field.type_ is dt.datetime:
            dtype = _dataclass_to_schema_datetime(field)
        elif is_union_type(field.type_) and set(
            get_args(field.type_, optional_drop_none=True)
        ) == {dt.date, dt.datetime}:
            if is_instance_gen(field.value, dt.date):
                dtype = Date
            else:
                dtype = _dataclass_to_schema_datetime(field)
        else:
            dtype = _dataclass_to_schema_one(
                field.type_, globalns=globalns, localns=localns
            )
        out[field.name] = dtype
    return out


def _dataclass_to_schema_datetime(
    field: _YieldFieldsInstance[dt.datetime], /
) -> PolarsDataType:
    if field.value.tzinfo is None:
        return Datetime
    return zoned_datetime(time_zone=ensure_time_zone(field.value.tzinfo))


def _dataclass_to_schema_one(
    obj: Any,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> PolarsDataType:
    if obj is bool:
        return Boolean
    if obj is int:
        return Int64
    if obj is float:
        return Float64
    if obj is str:
        return String
    if obj is dt.date:
        return Date
    if obj in {Path, UUID}:
        return Object
    if isinstance(obj, type) and issubclass(obj, enum.Enum):
        return pl.Enum([e.name for e in obj])
    if is_dataclass_class(obj):
        out: dict[str, Any] = {}
        for field in yield_fields(obj, globalns=globalns, localns=localns):
            out[field.name] = _dataclass_to_schema_one(
                field.type_, globalns=globalns, localns=localns
            )
        return struct_dtype(**out)
    if is_frozenset_type(obj) or is_list_type(obj) or is_set_type(obj):
        inner_type = one(get_args(obj))
        inner_dtype = _dataclass_to_schema_one(
            inner_type, globalns=globalns, localns=localns
        )
        return List(inner_dtype)
    if is_literal_type(obj):
        return pl.Enum(get_args(obj))
    if is_optional_type(obj):
        inner_type = one(get_args(obj, optional_drop_none=True))
        return _dataclass_to_schema_one(inner_type, globalns=globalns, localns=localns)
    msg = f"{obj=}"
    raise NotImplementedError(msg)


##


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


##


def ensure_data_type(dtype: PolarsDataType, /) -> DataType:
    """Ensure a data type is returned."""
    return dtype if isinstance(dtype, DataType) else dtype()


##


@overload
def ensure_expr_or_series(column: ExprLike, /) -> Expr: ...
@overload
def ensure_expr_or_series(column: Series, /) -> Series: ...
@overload
def ensure_expr_or_series(column: IntoExprColumn, /) -> Expr | Series: ...
def ensure_expr_or_series(column: IntoExprColumn, /) -> Expr | Series:
    """Ensure a column expression or Series is returned."""
    return col(column) if isinstance(column, str) else column


##


def ensure_expr_or_series_many(
    *columns: IntoExprColumn, **named_columns: IntoExprColumn
) -> Sequence[Expr | Series]:
    """Ensure a set of column expressions and/or Series are returned."""
    args = map(ensure_expr_or_series, columns)
    kwargs = (ensure_expr_or_series(v).alias(k) for k, v in named_columns.items())
    return list(chain(args, kwargs))


##


@overload
def finite_ewm_mean(
    column: ExprLike,
    /,
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
    min_weight: float = _FINITE_EWM_MIN_WEIGHT,
) -> Expr: ...
@overload
def finite_ewm_mean(
    column: Series,
    /,
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
    min_weight: float = _FINITE_EWM_MIN_WEIGHT,
) -> Series: ...
@overload
def finite_ewm_mean(
    column: IntoExprColumn,
    /,
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
    min_weight: float = _FINITE_EWM_MIN_WEIGHT,
) -> Expr | Series: ...
def finite_ewm_mean(
    column: IntoExprColumn,
    /,
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
    min_weight: float = _FINITE_EWM_MIN_WEIGHT,
) -> Expr | Series:
    """Compute a finite EWMA."""
    try:
        weights = _finite_ewm_weights(
            com=com, span=span, half_life=half_life, alpha=alpha, min_weight=min_weight
        )
    except _FiniteEWMWeightsError as error:
        raise FiniteEWMMeanError(min_weight=error.min_weight) from None
    column = ensure_expr_or_series(column)
    mean = column.fill_null(value=0.0).rolling_mean(len(weights), weights=list(weights))
    expr = when(column.is_not_null()).then(mean)
    return try_reify_expr(expr, column)


@dataclass(kw_only=True)
class FiniteEWMMeanError(Exception):
    min_weight: float = _FINITE_EWM_MIN_WEIGHT

    @override
    def __str__(self) -> str:
        return f"Min weight must be at least 0 and less than 1; got {self.min_weight}"


##


def _finite_ewm_weights(
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
    min_weight: float = _FINITE_EWM_MIN_WEIGHT,
    raw: bool = False,
) -> Sequence[float]:
    """Construct the finite EWM weights."""
    if not (is_non_negative(min_weight) and is_less_than(min_weight, 1.0)):
        raise _FiniteEWMWeightsError(min_weight=min_weight)
    params = ewm_parameters(com=com, span=span, half_life=half_life, alpha=alpha)
    alpha_ = params.alpha
    one_minus_alpha = 1 - alpha_
    min_terms = ceil(log(1 - min_weight) / log(one_minus_alpha))
    window_size = min_terms + 2
    raw_weights = [alpha_ * one_minus_alpha**i for i in reversed(range(window_size))]
    if raw:
        return raw_weights
    return [w / sum(raw_weights) for w in raw_weights]


@dataclass(kw_only=True)
class _FiniteEWMWeightsError(Exception):
    min_weight: float = _FINITE_EWM_MIN_WEIGHT

    @override
    def __str__(self) -> str:
        return f"Min weight must be at least 0 and less than 1; got {self.min_weight}"


##


@overload
def floor_datetime(column: ExprLike, every: ExprLike, /) -> Expr: ...
@overload
def floor_datetime(column: Series, every: ExprLike, /) -> Series: ...
@overload
def floor_datetime(column: IntoExprColumn, every: ExprLike, /) -> Expr | Series: ...
def floor_datetime(column: IntoExprColumn, every: ExprLike, /) -> Expr | Series:
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


##


def get_data_type_or_series_time_zone(
    dtype_or_series: PolarsDataType | Series, /
) -> ZoneInfo:
    """Get the time zone of a dtype/series."""
    match dtype_or_series:
        case DataType() as dtype:
            ...
        case DataTypeClass() as dtype_cls:
            dtype = dtype_cls()
        case Series() as series:
            dtype = series.dtype
        case _ as never:
            assert_never(never)
    if not isinstance(dtype, Datetime):
        raise _GetDataTypeOrSeriesTimeZoneNotDateTimeError(dtype=dtype)
    if dtype.time_zone is None:
        raise _GetDataTypeOrSeriesTimeZoneNotZonedError(dtype=dtype)
    return ZoneInfo(dtype.time_zone)


@dataclass(kw_only=True, slots=True)
class GetDataTypeOrSeriesTimeZoneError(Exception):
    dtype: DataType


@dataclass(kw_only=True, slots=True)
class _GetDataTypeOrSeriesTimeZoneNotDateTimeError(GetDataTypeOrSeriesTimeZoneError):
    @override
    def __str__(self) -> str:
        return f"Data type must be Datetime; got {self.dtype}"


@dataclass(kw_only=True, slots=True)
class _GetDataTypeOrSeriesTimeZoneNotZonedError(GetDataTypeOrSeriesTimeZoneError):
    @override
    def __str__(self) -> str:
        return f"Data type must be zoned; got {self.dtype}"


##


def get_expr_name(obj: Series | DataFrame, expr: IntoExprColumn, /) -> str:
    """Get the name of an expression."""
    match obj:
        case Series() as series:
            return get_expr_name(series.to_frame(), expr)
        case DataFrame() as df:
            selected = df.select(expr)
            return one(selected.columns)
        case _ as never:
            assert_never(never)


##


def get_frequency_spectrum(series: Series, /, *, d: int = 1) -> DataFrame:
    """Get the frequency spectrum."""
    import utilities.numpy

    array = series.to_numpy()
    spectrum = utilities.numpy.get_frequency_spectrum(array, d=d)
    return DataFrame(
        data=spectrum, schema={"frequency": Float64, "amplitude": Float64}, orient="row"
    )


##


@overload
def get_series_number_of_decimals(
    series: Series, /, *, nullable: Literal[True]
) -> int | None: ...
@overload
def get_series_number_of_decimals(
    series: Series, /, *, nullable: Literal[False] = False
) -> int: ...
@overload
def get_series_number_of_decimals(
    series: Series, /, *, nullable: bool = False
) -> int | None: ...
def get_series_number_of_decimals(
    series: Series, /, *, nullable: bool = False
) -> int | None:
    """Get the number of decimals of a series."""
    if not isinstance(dtype := series.dtype, Float64):
        raise _GetSeriesNumberOfDecimalsNotFloatError(dtype=dtype)
    decimals = series.map_elements(number_of_decimals, return_dtype=Int64).max()
    try:
        return ensure_int(decimals, nullable=nullable)
    except EnsureIntError:
        raise _GetSeriesNumberOfDecimalsAllNullError(series=series) from None


@dataclass(kw_only=True, slots=True)
class GetSeriesNumberOfDecimalsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _GetSeriesNumberOfDecimalsNotFloatError(GetSeriesNumberOfDecimalsError):
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"Data type must be Float64; got {self.dtype}"


@dataclass(kw_only=True, slots=True)
class _GetSeriesNumberOfDecimalsAllNullError(GetSeriesNumberOfDecimalsError):
    series: Series

    @override
    def __str__(self) -> str:
        return f"Series must not be all-null; got {self.series}"


##


def insert_after(df: DataFrame, column: str, value: IntoExprColumn, /) -> DataFrame:
    """Insert a series after an existing column; not in-place."""
    columns = df.columns
    try:
        index = columns.index(column)
    except ValueError:
        raise InsertAfterError(df=df, column=column) from None
    return df.select(*columns[: index + 1], value, *columns[index + 1 :])


@dataclass(kw_only=True)
class InsertAfterError(Exception):
    df: DataFrame
    column: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have column {self.column!r}; got {self.df.columns}"


def insert_before(df: DataFrame, column: str, value: IntoExprColumn, /) -> DataFrame:
    """Insert a series before an existing column; not in-place."""
    columns = df.columns
    try:
        index = columns.index(column)
    except ValueError:
        raise InsertBeforeError(df=df, column=column) from None
    return df.select(*columns[:index], value, *columns[index:])


@dataclass(kw_only=True)
class InsertBeforeError(Exception):
    df: DataFrame
    column: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have column {self.column!r}; got {self.df.columns}"


def insert_between(
    df: DataFrame, left: str, right: str, value: IntoExprColumn, /
) -> DataFrame:
    """Insert a series in between two existing columns; not in-place."""
    columns = df.columns
    try:
        index_left = columns.index(left)
        index_right = columns.index(right)
    except ValueError:
        raise _InsertBetweenMissingColumnsError(df=df, left=left, right=right) from None
    if (index_left + 1) != index_right:
        raise _InsertBetweenNonConsecutiveError(
            df=df,
            left=left,
            right=right,
            index_left=index_left,
            index_right=index_right,
        )
    return df.select(*columns[: index_left + 1], value, *columns[index_right:])


@dataclass(kw_only=True)
class InsertBetweenError(Exception):
    df: DataFrame
    left: str
    right: str


@dataclass(kw_only=True)
class _InsertBetweenMissingColumnsError(InsertBetweenError):
    @override
    def __str__(self) -> str:
        return f"DataFrame must have columns {self.left!r} and {self.right!r}; got {self.df.columns}"


@dataclass(kw_only=True)
class _InsertBetweenNonConsecutiveError(InsertBetweenError):
    index_left: int
    index_right: int

    @override
    def __str__(self) -> str:
        return f"DataFrame columns {self.left!r} and {self.right!r} must be consecutive; got indices {self.index_left} and {self.index_right}"


##


def integers(
    obj: int | Series | DataFrame,
    low: int,
    /,
    *,
    high: int | None = None,
    seed: int | None = None,
    endpoint: bool = False,
    name: str | None = None,
    dtype: PolarsDataType = Int64,
) -> Series:
    """Construct a series of normally-distributed numbers."""
    match obj:
        case int() as height:
            from numpy.random import default_rng

            rng = default_rng(seed=seed)
            values = rng.integers(low, high=high, size=height, endpoint=endpoint)
            return Series(name=name, values=values, dtype=dtype)
        case Series() as series:
            return integers(
                series.len(),
                low,
                high=high,
                seed=seed,
                endpoint=endpoint,
                name=name,
                dtype=dtype,
            )
        case DataFrame() as df:
            return integers(
                df.height,
                low,
                high=high,
                seed=seed,
                endpoint=endpoint,
                name=name,
                dtype=dtype,
            )
        case _ as never:
            assert_never(never)


##


@overload
def is_near_event(
    *exprs: ExprLike, before: int = 0, after: int = 0, **named_exprs: ExprLike
) -> Expr: ...
@overload
def is_near_event(
    *exprs: Series, before: int = 0, after: int = 0, **named_exprs: Series
) -> Series: ...
@overload
def is_near_event(
    *exprs: IntoExprColumn,
    before: int = 0,
    after: int = 0,
    **named_exprs: IntoExprColumn,
) -> Expr | Series: ...
def is_near_event(
    *exprs: IntoExprColumn,
    before: int = 0,
    after: int = 0,
    **named_exprs: IntoExprColumn,
) -> Expr | Series:
    """Compute the rows near any event."""
    if before <= -1:
        raise _IsNearEventBeforeError(before=before)
    if after <= -1:
        raise _IsNearEventAfterError(after=after)
    all_exprs = ensure_expr_or_series_many(*exprs, **named_exprs)
    shifts = range(-before, after + 1)
    if len(all_exprs) == 0:
        near = lit(value=False, dtype=Boolean)
    else:
        near_exprs = (
            e.shift(s).fill_null(value=False) for e, s in product(all_exprs, shifts)
        )
        near = any_horizontal(*near_exprs)
    return try_reify_expr(near, *exprs, **named_exprs)


@dataclass(kw_only=True, slots=True)
class IsNearEventError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _IsNearEventBeforeError(IsNearEventError):
    before: int

    @override
    def __str__(self) -> str:
        return f"'Before' must be non-negative; got {self.before}"


@dataclass(kw_only=True, slots=True)
class _IsNearEventAfterError(IsNearEventError):
    after: int

    @override
    def __str__(self) -> str:
        return f"'After' must be non-negative; got {self.after}"


##


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


##


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


##


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


##


@overload
def map_over_columns(func: Callable[[Series], Series], obj: Series, /) -> Series: ...
@overload
def map_over_columns(
    func: Callable[[Series], Series], obj: DataFrame, /
) -> DataFrame: ...
@overload
def map_over_columns(
    func: Callable[[Series], Series], obj: Series | DataFrame, /
) -> Series | DataFrame: ...
def map_over_columns(
    func: Callable[[Series], Series], obj: Series | DataFrame, /
) -> Series | DataFrame:
    """Map a function over the columns of a Struct-Series/DataFrame."""
    match obj:
        case Series() as series:
            return _map_over_series_one(func, series)
        case DataFrame() as df:
            return df.select(*(_map_over_series_one(func, df[c]) for c in df.columns))
        case _ as never:
            assert_never(never)


def _map_over_series_one(func: Callable[[Series], Series], series: Series, /) -> Series:
    if isinstance(series.dtype, Struct):
        unnested = series.struct.unnest()
        name = series.name
        return map_over_columns(func, unnested).select(struct("*").alias(name))[name]
    return func(series)


##


def nan_sum_agg(column: str | Expr, /, *, dtype: PolarsDataType | None = None) -> Expr:
    """Nan sum aggregation."""
    col_use = col(column) if isinstance(column, str) else column
    return (
        when(col_use.is_not_null().any())
        .then(col_use.sum())
        .otherwise(lit(None, dtype=dtype))
    )


##


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


##


def normal(
    obj: int | Series | DataFrame,
    /,
    *,
    loc: float = 0.0,
    scale: float = 1.0,
    seed: int | None = None,
    name: str | None = None,
    dtype: PolarsDataType = Float64,
) -> Series:
    """Construct a series of normally-distributed numbers."""
    match obj:
        case int() as height:
            from numpy.random import default_rng

            rng = default_rng(seed=seed)
            values = rng.normal(loc=loc, scale=scale, size=height)
            return Series(name=name, values=values, dtype=dtype)
        case Series() as series:
            return normal(
                series.len(), loc=loc, scale=scale, seed=seed, name=name, dtype=dtype
            )
        case DataFrame() as df:
            return normal(
                df.height, loc=loc, scale=scale, seed=seed, name=name, dtype=dtype
            )
        case _ as never:
            assert_never(never)


##


def reify_exprs(
    *exprs: IntoExprColumn, **named_exprs: IntoExprColumn
) -> Expr | Series | DataFrame:
    """Reify a set of expressions."""
    all_exprs = ensure_expr_or_series_many(*exprs, **named_exprs)
    if len(all_exprs) == 0:
        raise _ReifyExprsEmptyError from None
    series = [s for s in all_exprs if isinstance(s, Series)]
    lengths = {s.len() for s in series}
    try:
        length = one(lengths)
    except OneEmptyError:
        match len(all_exprs):
            case 0:
                raise ImpossibleCaseError(
                    case=[f"{all_exprs=}"]
                ) from None  # pragma: no cover
            case 1:
                return one(all_exprs)
            case _:
                return struct(*all_exprs)
    except OneNonUniqueError as error:
        raise _ReifyExprsSeriesNonUniqueError(
            first=error.first, second=error.second
        ) from None
    df = (
        int_range(end=length, eager=True)
        .alias("_index")
        .to_frame()
        .with_columns(*all_exprs)
        .drop("_index")
    )
    match len(df.columns):
        case 0:
            raise ImpossibleCaseError(case=[f"{df.columns=}"])  # pragma: no cover
        case 1:
            return df[one(df.columns)]
        case _:
            return df


@dataclass(kw_only=True, slots=True)
class ReifyExprsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ReifyExprsEmptyError(ReifyExprsError):
    @override
    def __str__(self) -> str:
        return "At least 1 Expression or Series must be given"


@dataclass
class _ReifyExprsSeriesNonUniqueError(ReifyExprsError):
    first: int
    second: int

    @override
    def __str__(self) -> str:
        return f"Series must contain exactly one length; got {self.first}, {self.second} and perhaps more"


##


@overload
def replace_time_zone(
    obj: Series, /, *, time_zone: TimeZoneLike | None = UTC
) -> Series: ...
@overload
def replace_time_zone(
    obj: DataFrame, /, *, time_zone: TimeZoneLike | None = UTC
) -> DataFrame: ...
@overload
def replace_time_zone(
    obj: Series | DataFrame, /, *, time_zone: TimeZoneLike | None = UTC
) -> Series | DataFrame: ...
def replace_time_zone(
    obj: Series | DataFrame, /, *, time_zone: TimeZoneLike | None = UTC
) -> Series | DataFrame:
    """Replace the time zone(s) of a Series or Dataframe."""
    return map_over_columns(partial(_replace_time_zone_one, time_zone=time_zone), obj)


def _replace_time_zone_one(
    sr: Series, /, *, time_zone: TimeZoneLike | None = UTC
) -> Series:
    if isinstance(sr.dtype, Datetime):
        time_zone_use = None if time_zone is None else get_time_zone_name(time_zone)
        return sr.dt.replace_time_zone(time_zone_use)
    return sr


##


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


##


def struct_dtype(**kwargs: PolarsDataType) -> Struct:
    """Construct a Struct data type from a set of keyword arguments."""
    return Struct(kwargs)


##


def struct_from_dataclass(
    cls: type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    time_zone: TimeZoneLike | None = None,
) -> Struct:
    """Construct the Struct data type for a dataclass."""
    if not is_dataclass_class(cls):
        raise _StructFromDataClassNotADataclassError(cls=cls)
    anns = get_type_hints(
        cls, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
    )
    data_types = {
        k: _struct_from_dataclass_one(v, time_zone=time_zone) for k, v in anns.items()
    }
    return Struct(data_types)


def _struct_from_dataclass_one(
    ann: Any, /, *, time_zone: TimeZoneLike | None = None
) -> PolarsDataType:
    mapping = {bool: Boolean, dt.date: Date, float: Float64, int: Int64, str: String}
    with suppress(KeyError):
        return mapping[ann]
    if ann is dt.datetime:
        if time_zone is None:
            raise _StructFromDataClassTimeZoneMissingError
        return zoned_datetime(time_zone=time_zone)
    if is_dataclass_class(ann):
        return struct_from_dataclass(ann, time_zone=time_zone)
    if (isinstance(ann, type) and issubclass(ann, enum.Enum)) or (
        is_literal_type(ann) and is_iterable_of(get_args(ann), str)
    ):
        return String
    if is_optional_type(ann):
        return _struct_from_dataclass_one(
            one(get_args(ann, optional_drop_none=True)), time_zone=time_zone
        )
    if is_frozenset_type(ann) or is_list_type(ann) or is_set_type(ann):
        return List(_struct_from_dataclass_one(one(get_args(ann)), time_zone=time_zone))
    raise _StructFromDataClassTypeError(ann=ann)


@dataclass(kw_only=True, slots=True)
class StructFromDataClassError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _StructFromDataClassNotADataclassError(StructFromDataClassError):
    cls: type[Dataclass]

    @override
    def __str__(self) -> str:
        return f"Object must be a dataclass; got {self.cls}"


@dataclass(kw_only=True, slots=True)
class _StructFromDataClassTimeZoneMissingError(StructFromDataClassError):
    @override
    def __str__(self) -> str:
        return "Time-zone must be given"


@dataclass(kw_only=True, slots=True)
class _StructFromDataClassTypeError(StructFromDataClassError):
    ann: Any

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {self.ann}"


##


def try_reify_expr(
    expr: IntoExprColumn, /, *exprs: IntoExprColumn, **named_exprs: IntoExprColumn
) -> Expr | Series:
    """Try reify an expression."""
    expr = ensure_expr_or_series(expr)
    all_exprs = ensure_expr_or_series_many(*exprs, **named_exprs)
    all_exprs = [e.alias(f"_{i}") for i, e in enumerate(all_exprs)]
    result = reify_exprs(expr, *all_exprs)
    match result:
        case Expr():
            return expr
        case Series() as series:
            return series
        case DataFrame() as df:
            return df[get_expr_name(df, expr)]
        case _ as never:
            assert_never(never)


##


def uniform(
    obj: int | Series | DataFrame,
    /,
    *,
    low: float = 0.0,
    high: float = 1.0,
    seed: int | None = None,
    name: str | None = None,
    dtype: PolarsDataType = Float64,
) -> Series:
    """Construct a series of uniformly-distributed numbers."""
    match obj:
        case int() as height:
            from numpy.random import default_rng

            rng = default_rng(seed=seed)
            values = rng.uniform(low=low, high=high, size=height)
            return Series(name=name, values=values, dtype=dtype)
        case Series() as series:
            return uniform(
                series.len(), low=low, high=high, seed=seed, name=name, dtype=dtype
            )
        case DataFrame() as df:
            return uniform(
                df.height, low=low, high=high, seed=seed, name=name, dtype=dtype
            )
        case _ as never:
            assert_never(never)


##


def unique_element(column: ExprLike, /) -> Expr:
    """Get the unique element in a list."""
    column = ensure_expr_or_series(column)
    return when(column.list.len() == 1).then(column.list.first())


##


@overload
def week_num(column: ExprLike, /, *, start: WeekDay = "mon") -> Expr: ...
@overload
def week_num(column: Series, /, *, start: WeekDay = "mon") -> Series: ...
@overload
def week_num(column: IntoExprColumn, /, *, start: WeekDay = "mon") -> Expr | Series: ...
def week_num(column: IntoExprColumn, /, *, start: WeekDay = "mon") -> Expr | Series:
    """Compute the week number of a date column."""
    column = ensure_expr_or_series(column)
    epoch = column.dt.epoch(time_unit="d").alias("epoch")
    offset = get_args(WeekDay).index(start)
    return (epoch + 3 - offset) // 7


##


def zoned_datetime(
    *, time_unit: TimeUnit = "us", time_zone: TimeZoneLike = UTC
) -> Datetime:
    """Create a zoned datetime data type."""
    return Datetime(time_unit=time_unit, time_zone=get_time_zone_name(time_zone))


__all__ = [
    "BooleanValueCountsError",
    "CheckPolarsDataFrameError",
    "ColumnsToDictError",
    "DataClassToDataFrameError",
    "DatetimeHongKong",
    "DatetimeTokyo",
    "DatetimeUSCentral",
    "DatetimeUSEastern",
    "DatetimeUTC",
    "DropNullStructSeriesError",
    "FiniteEWMMeanError",
    "GetDataTypeOrSeriesTimeZoneError",
    "GetSeriesNumberOfDecimalsError",
    "InsertAfterError",
    "InsertBeforeError",
    "InsertBetweenError",
    "IsNearEventError",
    "IsNullStructSeriesError",
    "SetFirstRowAsColumnsError",
    "StructFromDataClassError",
    "acf",
    "adjust_frequencies",
    "append_dataclass",
    "are_frames_equal",
    "bernoulli",
    "boolean_value_counts",
    "ceil_datetime",
    "check_polars_dataframe",
    "choice",
    "collect_series",
    "columns_to_dict",
    "concat_series",
    "convert_time_zone",
    "cross",
    "dataclass_to_dataframe",
    "dataclass_to_schema",
    "drop_null_struct_series",
    "ensure_data_type",
    "ensure_expr_or_series",
    "ensure_expr_or_series_many",
    "finite_ewm_mean",
    "floor_datetime",
    "get_data_type_or_series_time_zone",
    "get_expr_name",
    "get_frequency_spectrum",
    "get_series_number_of_decimals",
    "insert_after",
    "insert_before",
    "insert_between",
    "integers",
    "is_near_event",
    "is_not_null_struct_series",
    "is_null_struct_series",
    "join",
    "map_over_columns",
    "nan_sum_agg",
    "nan_sum_cols",
    "normal",
    "replace_time_zone",
    "set_first_row_as_columns",
    "struct_dtype",
    "struct_from_dataclass",
    "touch",
    "try_reify_expr",
    "uniform",
    "unique_element",
    "zoned_datetime",
]
