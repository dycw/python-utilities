from __future__ import annotations

import enum
from collections.abc import Callable, Iterator, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import asdict, dataclass
from functools import partial, reduce
from itertools import chain, pairwise, product
from math import ceil, log, pi, sqrt
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, assert_never, cast, overload, override
from uuid import UUID
from zoneinfo import ZoneInfo

import polars as pl
import whenever
from polars import (
    Boolean,
    DataFrame,
    Datetime,
    Duration,
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
    concat_list,
    datetime_range,
    int_range,
    lit,
    max_horizontal,
    struct,
    sum_horizontal,
    when,
)
from polars._typing import PolarsDataType
from polars.datatypes import DataType, DataTypeClass
from polars.exceptions import (
    ColumnNotFoundError,
    NoRowsReturnedError,
    OutOfBoundsError,
    PolarsInefficientMapWarning,
)
from polars.schema import Schema
from polars.testing import assert_frame_equal, assert_series_equal
from rich.pretty import pretty_repr
from whenever import DateDelta, DateTimeDelta, PlainDateTime, TimeDelta, ZonedDateTime

import utilities.math
from utilities.constants import UTC
from utilities.core import (
    OneEmptyError,
    OneNonUniqueError,
    always_iterable,
    one,
    read_bytes,
    suppress_warnings,
    to_time_zone_name,
    write_bytes,
)
from utilities.dataclasses import yield_fields
from utilities.errors import ImpossibleCaseError
from utilities.functions import get_class_name
from utilities.iterables import (
    CheckIterablesEqualError,
    CheckMappingsEqualError,
    CheckSuperMappingError,
    check_iterables_equal,
    check_mappings_equal,
    check_supermapping,
    is_iterable_not_str,
    resolve_include_and_exclude,
)
from utilities.math import (
    MAX_DECIMALS,
    CheckIntegerError,
    check_integer,
    ewm_parameters,
    is_less_than,
    is_non_negative,
)
from utilities.types import MaybeStr, Number, PathLike, StrDict, WeekDay
from utilities.typing import (
    get_args,
    is_dataclass_class,
    is_dataclass_instance,
    is_frozenset_type,
    is_list_type,
    is_literal_type,
    is_optional_type,
    is_set_type,
    make_isinstance,
)
from utilities.whenever import (
    DatePeriod,
    TimePeriod,
    ZonedDateTimePeriod,
    to_py_time_delta,
)

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from polars._typing import (
        IntoExpr,
        IntoExprColumn,
        JoinStrategy,
        JoinValidation,
        PolarsDataType,
        QuantileMethod,
        RoundMode,
        SchemaDict,
        TimeUnit,
    )

    from utilities.numpy import NDArrayB, NDArrayF
    from utilities.statsmodels import ACFMissing
    from utilities.types import Dataclass, MaybeIterable, StrMapping, TimeZoneLike


type ExprLike = MaybeStr[Expr]
type ExprOrSeries = Expr | Series
DatetimeHongKong = Datetime(time_zone="Asia/Hong_Kong")
DatetimeTokyo = Datetime(time_zone="Asia/Tokyo")
DatetimeUSCentral = Datetime(time_zone="US/Central")
DatetimeUSEastern = Datetime(time_zone="US/Eastern")
DatetimeUTC = Datetime(time_zone="UTC")
DatePeriodDType = Struct({"start": pl.Date, "end": pl.Date})
TimePeriodDType = Struct({"start": pl.Time, "end": pl.Time})


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
        case never:
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


def all_dataframe_columns(
    df: DataFrame, expr: IntoExprColumn, /, *exprs: IntoExprColumn
) -> Series:
    """Return a DataFrame column with `AND` applied to additional exprs/series."""
    name = get_expr_name(df, expr)
    return df.select(all_horizontal(expr, *exprs).alias(name))[name]


def any_dataframe_columns(
    df: DataFrame, expr: IntoExprColumn, /, *exprs: IntoExprColumn
) -> Series:
    """Return a DataFrame column with `OR` applied to additional exprs/series."""
    name = get_expr_name(df, expr)
    return df.select(any_horizontal(expr, *exprs).alias(name))[name]


def all_series(series: Series, /, *columns: ExprOrSeries) -> Series:
    """Return a Series with `AND` applied to additional exprs/series."""
    return all_dataframe_columns(series.to_frame(), series.name, *columns)


def any_series(series: Series, /, *columns: ExprOrSeries) -> Series:
    """Return a Series with `OR` applied to additional exprs/series."""
    df = series.to_frame()
    name = series.name
    return df.select(any_horizontal(name, *columns).alias(name))[name]


##


def append_row(
    df: DataFrame,
    row: StrMapping,
    /,
    *,
    predicate: Callable[[StrMapping], bool] | None = None,
    disallow_extra: bool = False,
    disallow_missing: bool | MaybeIterable[str] = False,
    disallow_null: bool | MaybeIterable[str] = False,
    in_place: bool = False,
) -> DataFrame:
    """Append a row to a DataFrame."""
    if (predicate is not None) and not predicate(row):
        raise _AppendRowPredicateError(df=df, row=row)
    if disallow_extra and (len(extra := set(row) - set(df.columns)) >= 1):
        raise _AppendRowExtraKeysError(df=df, row=row, extra=extra)
    if disallow_missing is not False:
        missing = set(df.columns) - set(row)
        if disallow_missing is not True:
            missing &= set(always_iterable(disallow_missing))
        if len(missing) >= 1:
            raise _AppendRowMissingKeysError(df=df, row=row, missing=missing)
    other = DataFrame(data=[row], schema=df.schema)
    if disallow_null:
        other_null = other.select(col(c).is_null().any() for c in other.columns)
        null = {k for k, v in other_null.row(0, named=True).items() if v}
        if disallow_null is not True:
            null &= set(always_iterable(disallow_null))
        if len(null) >= 1:
            raise _AppendRowNullColumnsError(df=df, row=row, columns=null)
    return df.extend(other) if in_place else df.vstack(other)


@dataclass(kw_only=True, slots=True)
class AppendRowError(Exception):
    df: DataFrame
    row: StrMapping


@dataclass(kw_only=True, slots=True)
class _AppendRowPredicateError(AppendRowError):
    @override
    def __str__(self) -> str:
        return f"Predicate failed; got {pretty_repr(self.row)}"


@dataclass(kw_only=True, slots=True)
class _AppendRowExtraKeysError(AppendRowError):
    extra: AbstractSet[str]

    @override
    def __str__(self) -> str:
        return f"Extra key(s) found; got {pretty_repr(self.extra)}"


@dataclass(kw_only=True, slots=True)
class _AppendRowMissingKeysError(AppendRowError):
    missing: AbstractSet[str]

    @override
    def __str__(self) -> str:
        return f"Missing key(s) found; got {pretty_repr(self.missing)}"


@dataclass(kw_only=True, slots=True)
class _AppendRowNullColumnsError(AppendRowError):
    columns: AbstractSet[str]

    @override
    def __str__(self) -> str:
        return f"Null column(s) found; got {pretty_repr(self.columns)}"


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
    rel_tol: float = 1e-5,
    abs_tol: float = 1e-8,
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
            rel_tol=rel_tol,
            abs_tol=abs_tol,
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
        case never:
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
        case never:
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
    columns: list[str]

    @override
    def __str__(self) -> str:
        return f"DataFrame must have columns {pretty_repr(self.columns)}; got {pretty_repr(self.df.columns)}:\n\n{self.df}"


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
        return f"DataFrame must have dtypes {pretty_repr(self.dtypes)}; got {pretty_repr(self.df.dtypes)}:\n\n{self.df}"


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
                ...
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"DataFrame must satisfy the predicates; {desc}:\n\n{self.df}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.missing) >= 1:
            yield f"missing columns were {pretty_repr(self.missing)}"
        if len(self.failed) >= 1:
            yield f"failed predicates were {pretty_repr(self.failed)}"


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
        return f"DataFrame must have schema {pretty_repr(self.schema)} (ordered); got {pretty_repr(self.df.schema)}:\n\n{self.df}"


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
        return f"DataFrame must have schema {pretty_repr(self.schema)} (unordered); got {pretty_repr(self.df.schema)}:\n\n{self.df}"


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
        return f"DataFrame schema must include {pretty_repr(self.schema)} (unordered); got {pretty_repr(self.df.schema)}:\n\n{self.df}"


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
        return f"DataFrame must be sorted on {pretty_repr(self.by)}:\n\n{self.df}"


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
        return f"DataFrame must be unique on {pretty_repr(self.by)}:\n\n{self.df}"


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
        case never:
            assert_never(never)


##


def columns_to_dict(
    df: DataFrame, key: IntoExprColumn, value: IntoExprColumn, /
) -> dict[Any, Any]:
    """Map a pair of columns into a dictionary. Must be unique on `key`."""
    df = df.select(key, value)
    key_col, value_col = [df[get_expr_name(df, expr)] for expr in [key, value]]
    if key_col.is_duplicated().any():
        raise ColumnsToDictError(df=df, key=key_col.name)
    return dict(zip(key_col, value_col, strict=True))


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
        return sr.dt.convert_time_zone(to_time_zone_name(time_zone))
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
) -> ExprOrSeries: ...
def cross(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> ExprOrSeries:
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
) -> ExprOrSeries: ...
def touch(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> ExprOrSeries:
    """Compute when a touch occurs."""
    return _cross_or_touch(expr, "touch", up_or_down, other)


def _cross_or_touch(
    expr: IntoExprColumn,
    cross_or_touch: Literal["cross", "touch"],
    up_or_down: Literal["up", "down"],
    other: Number | IntoExprColumn,
    /,
) -> ExprOrSeries:
    """Compute when a column crosses/touches a threshold."""
    expr = ensure_expr_or_series(expr)
    match other:
        case int() | float():
            ...
        case str() | Expr() | Series():
            other = ensure_expr_or_series(other)
        case never:
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
        case never:
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
    interpolation: QuantileMethod = "nearest",
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
    interpolation: QuantileMethod = "nearest",
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
    interpolation: QuantileMethod = "nearest",
    window_size: int = 2,
    weights: list[float] | None = None,
    min_samples: int | None = None,
    center: bool = False,
) -> ExprOrSeries: ...
def cross_rolling_quantile(
    expr: IntoExprColumn,
    up_or_down: Literal["up", "down"],
    quantile: float,
    /,
    *,
    interpolation: QuantileMethod = "nearest",
    window_size: int = 2,
    weights: list[float] | None = None,
    min_samples: int | None = None,
    center: bool = False,
) -> ExprOrSeries:
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
    warn_name_errors: bool = False,
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
    schema = dataclass_to_schema(
        first, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
    )
    df = DataFrame(data, schema=schema, orient="row")
    return map_over_columns(_dataclass_to_dataframe_cast, df)


def _dataclass_to_dataframe_cast(series: Series, /) -> Series:
    if series.dtype != Object:
        return series
    if series.map_elements(make_isinstance(whenever.Date), return_dtype=Boolean).all():
        return series.map_elements(lambda x: x.py_date(), return_dtype=pl.Date)
    if series.map_elements(make_isinstance(DateDelta), return_dtype=Boolean).all():
        return series.map_elements(to_py_time_delta, return_dtype=Duration)
    if series.map_elements(make_isinstance(DateTimeDelta), return_dtype=Boolean).all():
        return series.map_elements(to_py_time_delta, return_dtype=Duration)
    is_path = series.map_elements(make_isinstance(Path), return_dtype=Boolean).all()
    is_uuid = series.map_elements(make_isinstance(UUID), return_dtype=Boolean).all()
    if is_path or is_uuid:
        with suppress_warnings(
            category=cast("type[Warning]", PolarsInefficientMapWarning)
        ):
            return series.map_elements(str, return_dtype=String)
    if series.map_elements(make_isinstance(whenever.Time), return_dtype=Boolean).all():
        return series.map_elements(lambda x: x.py_time(), return_dtype=pl.Time)
    if series.map_elements(make_isinstance(TimeDelta), return_dtype=Boolean).all():
        return series.map_elements(to_py_time_delta, return_dtype=Duration)
    if series.map_elements(make_isinstance(ZonedDateTime), return_dtype=Boolean).all():
        return_dtype = zoned_date_time_dtype(time_zone=one({dt.tz for dt in series}))
        return series.map_elements(lambda x: x.py_datetime(), return_dtype=return_dtype)
    if series.map_elements(
        lambda x: isinstance(x, dict) and (set(x) == {"start", "end"}),
        return_dtype=Boolean,
    ).all():
        start = _dataclass_to_dataframe_cast(
            series.map_elements(lambda x: x["start"], return_dtype=Object)
        ).alias("start")
        end = _dataclass_to_dataframe_cast(
            series.map_elements(lambda x: x["end"], return_dtype=Object)
        ).alias("end")
        name = series.name
        return concat_series(start, end).select(
            struct(start=start, end=end).alias(name)
        )[name]
    raise NotImplementedError(series)  # pragma: no cover


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
        return f"Iterable {pretty_repr(self.objs)} must contain exactly 1 class; got {self.first}, {self.second} and perhaps more"


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
    out: StrDict = {}
    for field in yield_fields(
        obj, globalns=globalns, localns=localns, warn_name_errors=warn_name_errors
    ):
        if is_dataclass_instance(field.value) and not (
            isinstance(field.type_, type)
            and issubclass(field.type_, (DatePeriod, TimePeriod, ZonedDateTimePeriod))
        ):
            dtypes = dataclass_to_schema(
                field.value, globalns=globalns, localns=localns
            )
            dtype = struct_dtype(**dtypes)
        else:
            dtype = _dataclass_to_schema_one(
                field.type_, globalns=globalns, localns=localns
            )
        out[field.name] = dtype
    return out


def _dataclass_to_schema_one(
    obj: Any,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> PolarsDataType:
    if isinstance(obj, type):
        if issubclass(obj, bool):
            return Boolean
        if issubclass(obj, int):
            return Int64
        if issubclass(obj, float):
            return Float64
        if issubclass(obj, str):
            return String
        if issubclass(
            obj,
            (
                DateDelta,
                DatePeriod,
                DateTimeDelta,
                Path,
                PlainDateTime,
                TimeDelta,
                TimePeriod,
                UUID,
                ZonedDateTime,
                ZonedDateTimePeriod,
                whenever.Date,
                whenever.Time,
            ),
        ):
            return Object
        if issubclass(obj, enum.Enum):
            return pl.Enum([e.name for e in obj])
    if is_dataclass_class(obj):
        out: StrDict = {}
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


def ensure_data_type(dtype: PolarsDataType, /) -> DataType:
    """Ensure a data type is returned."""
    return dtype if isinstance(dtype, DataType) else dtype()


##


@overload
def ensure_expr_or_series(column: ExprLike, /) -> Expr: ...
@overload
def ensure_expr_or_series(column: Series, /) -> Series: ...
@overload
def ensure_expr_or_series(column: IntoExprColumn, /) -> ExprOrSeries: ...
def ensure_expr_or_series(column: IntoExprColumn, /) -> ExprOrSeries:
    """Ensure a column expression or Series is returned."""
    return col(column) if isinstance(column, str) else column


##


def ensure_expr_or_series_many(
    *columns: IntoExprColumn, **named_columns: IntoExprColumn
) -> Sequence[ExprOrSeries]:
    """Ensure a set of column expressions and/or Series are returned."""
    args = map(ensure_expr_or_series, columns)
    kwargs = (ensure_expr_or_series(v).alias(k) for k, v in named_columns.items())
    return list(chain(args, kwargs))


##


def expr_to_series(expr: Expr, /) -> Series:
    """Collect a column expression into a Series."""
    return one_column(DataFrame().with_columns(expr))


##


@overload
def filter_date(
    column: ExprLike = "datetime",
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[whenever.Date] | None = None,
    exclude: MaybeIterable[whenever.Date] | None = None,
) -> Expr: ...
@overload
def filter_date(
    column: Series,
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[whenever.Date] | None = None,
    exclude: MaybeIterable[whenever.Date] | None = None,
) -> Series: ...
@overload
def filter_date(
    column: IntoExprColumn = "datetime",
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[whenever.Date] | None = None,
    exclude: MaybeIterable[whenever.Date] | None = None,
) -> ExprOrSeries: ...
def filter_date(
    column: IntoExprColumn = "datetime",
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[whenever.Date] | None = None,
    exclude: MaybeIterable[whenever.Date] | None = None,
) -> ExprOrSeries:
    """Compute the filter based on a set of dates."""
    column = ensure_expr_or_series(column)
    if time_zone is not None:
        column = column.dt.convert_time_zone(time_zone.key)
    keep = true_like(column)
    date = column.dt.date()
    include, exclude = resolve_include_and_exclude(include=include, exclude=exclude)
    if include is not None:
        keep &= date.is_in([d.py_date() for d in include])
    if exclude is not None:
        keep &= ~date.is_in([d.py_date() for d in exclude])
    return try_reify_expr(keep, column)


@overload
def filter_time(
    column: ExprLike = "datetime",
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
    exclude: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
) -> Expr: ...
@overload
def filter_time(
    column: Series,
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
    exclude: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
) -> Series: ...
@overload
def filter_time(
    column: IntoExprColumn = "datetime",
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
    exclude: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
) -> ExprOrSeries: ...
def filter_time(
    column: IntoExprColumn = "datetime",
    /,
    *,
    time_zone: ZoneInfo | None = None,
    include: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
    exclude: MaybeIterable[tuple[whenever.Time, whenever.Time]] | None = None,
) -> ExprOrSeries:
    """Compute the filter based on a set of times."""
    column = ensure_expr_or_series(column)
    if time_zone is not None:
        column = column.dt.convert_time_zone(time_zone.key)
    keep = true_like(column)
    time = column.dt.time()
    include, exclude = resolve_include_and_exclude(include=include, exclude=exclude)
    if include is not None:
        keep &= any_horizontal(
            time.is_between(s.py_time(), e.py_time()) for s, e in include
        )
    if exclude is not None:
        keep &= ~any_horizontal(
            time.is_between(s.py_time(), e.py_time()) for s, e in exclude
        )
    return try_reify_expr(keep, column)


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
) -> ExprOrSeries: ...
def finite_ewm_mean(
    column: IntoExprColumn,
    /,
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
    min_weight: float = _FINITE_EWM_MIN_WEIGHT,
) -> ExprOrSeries:
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
def first_true_horizontal(*columns: Series) -> Series: ...
@overload
def first_true_horizontal(*columns: IntoExprColumn) -> ExprOrSeries: ...
def first_true_horizontal(*columns: IntoExprColumn) -> ExprOrSeries:
    """Get the index of the first true in each row."""
    columns2 = ensure_expr_or_series_many(*columns)
    expr = when(any_horizontal(*columns2)).then(concat_list(*columns2).list.arg_max())
    return try_reify_expr(expr, *columns2)


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
        case never:
            assert_never(never)
    match dtype:
        case Datetime() as datetime:
            if datetime.time_zone is None:
                raise _GetDataTypeOrSeriesTimeZoneNotZonedError(dtype=datetime)
            return ZoneInfo(datetime.time_zone)
        case Struct() as struct:
            try:
                return one({
                    get_data_type_or_series_time_zone(f.dtype) for f in struct.fields
                })
            except OneNonUniqueError as error:
                raise _GetDataTypeOrSeriesTimeZoneStructNonUniqueError(
                    dtype=struct, first=error.first, second=error.second
                ) from None
        case _:
            raise _GetDataTypeOrSeriesTimeZoneNotDateTimeError(dtype=dtype)


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


@dataclass(kw_only=True, slots=True)
class _GetDataTypeOrSeriesTimeZoneStructNonUniqueError(
    GetDataTypeOrSeriesTimeZoneError
):
    first: ZoneInfo
    second: ZoneInfo

    @override
    def __str__(self) -> str:
        return f"Struct data type must contain exactly one time zone; got {self.first}, {self.second} and perhaps more"


##


def get_expr_name(obj: Series | DataFrame, expr: IntoExprColumn, /) -> str:
    """Get the name of an expression."""
    match obj:
        case Series() as series:
            return get_expr_name(series.to_frame(), expr)
        case DataFrame() as df:
            return one_column(df.select(expr)).name
        case never:
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
def increasing_horizontal(*columns: ExprLike) -> Expr: ...
@overload
def increasing_horizontal(*columns: Series) -> Series: ...
@overload
def increasing_horizontal(*columns: IntoExprColumn) -> ExprOrSeries: ...
def increasing_horizontal(*columns: IntoExprColumn) -> ExprOrSeries:
    """Check if a set of columns are increasing."""
    columns2 = ensure_expr_or_series_many(*columns)
    if len(columns2) == 0:
        return lit(value=True, dtype=Boolean)
    return all_horizontal(prev < curr for prev, curr in pairwise(columns2))


@overload
def decreasing_horizontal(*columns: ExprLike) -> Expr: ...
@overload
def decreasing_horizontal(*columns: Series) -> Series: ...
@overload
def decreasing_horizontal(*columns: IntoExprColumn) -> ExprOrSeries: ...
def decreasing_horizontal(*columns: IntoExprColumn) -> ExprOrSeries:
    """Check if a set of columns are decreasing."""
    columns2 = ensure_expr_or_series_many(*columns)
    if len(columns2) == 0:
        return lit(value=True, dtype=Boolean)
    return all_horizontal(prev > curr for prev, curr in pairwise(columns2))


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
        case never:
            assert_never(never)


##


@overload
def is_close(
    x: ExprLike, y: ExprLike, /, *, rel_tol: float = 1e-9, abs_tol: float = 0
) -> Expr: ...
@overload
def is_close(
    x: Series, y: Series, /, *, rel_tol: float = 1e-9, abs_tol: float = 0
) -> Series: ...
@overload
def is_close(
    x: IntoExprColumn,
    y: IntoExprColumn,
    /,
    *,
    rel_tol: float = 1e-9,
    abs_tol: float = 0,
) -> ExprOrSeries: ...
def is_close(
    x: IntoExprColumn,
    y: IntoExprColumn,
    /,
    *,
    rel_tol: float = 1e-9,
    abs_tol: float = 0,
) -> ExprOrSeries:
    """Check if two columns are close."""
    x, y = map(ensure_expr_or_series, [x, y])
    result = (x - y).abs() <= max_horizontal(
        rel_tol * max_horizontal(x.abs(), y.abs()), abs_tol
    )
    return try_reify_expr(result, x, y)


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
) -> ExprOrSeries: ...
def is_near_event(
    *exprs: IntoExprColumn,
    before: int = 0,
    after: int = 0,
    **named_exprs: IntoExprColumn,
) -> ExprOrSeries:
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


@overload
def is_true(column: ExprLike, /) -> Expr: ...
@overload
def is_true(column: Series, /) -> Series: ...
@overload
def is_true(column: IntoExprColumn, /) -> ExprOrSeries: ...
def is_true(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute when a boolean series is True."""
    column = ensure_expr_or_series(column)
    return (column.is_not_null()) & column


@overload
def is_false(column: ExprLike, /) -> Expr: ...
@overload
def is_false(column: Series, /) -> Series: ...
@overload
def is_false(column: IntoExprColumn, /) -> ExprOrSeries: ...
def is_false(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute when a boolean series is False."""
    column = ensure_expr_or_series(column)
    return (column.is_not_null()) & (~column)


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


def join_into_periods(
    left: DataFrame,
    right: DataFrame,
    /,
    *,
    on: str | None = None,
    left_on: str | None = None,
    right_on: str | None = None,
    suffix: str = "_right",
) -> DataFrame:
    """Join a pair of DataFrames on their periods; left in right."""
    match on, left_on, right_on:
        case None, None, None:
            return _join_into_periods_core(
                left, right, "datetime", "datetime", suffix=suffix
            )
        case str(), None, None:
            return _join_into_periods_core(left, right, on, on, suffix=suffix)
        case None, str(), str():
            return _join_into_periods_core(
                left, right, left_on, right_on, suffix=suffix
            )
        case _:
            raise _JoinIntoPeriodsArgumentsError(
                on=on, left_on=left_on, right_on=right_on
            )


def _join_into_periods_core(
    left: DataFrame,
    right: DataFrame,
    left_on: str,
    right_on: str,
    /,
    *,
    suffix: str = "_right",
) -> DataFrame:
    """Join a pair of DataFrames on their periods; left in right."""
    _join_into_periods_check(left, left_on, "left")
    _join_into_periods_check(right, right_on, "right")
    joined = left.join_asof(
        right,
        left_on=col(left_on).struct["start"],
        right_on=col(right_on).struct["start"],
        strategy="backward",
        suffix=suffix,
        coalesce=False,
    )
    new = f"{left_on}{suffix}" if left_on == right_on else right_on
    new_col = col(new)
    is_correct = (new_col.struct["start"] <= col(left_on).struct["start"]) & (
        col(left_on).struct["end"] <= new_col.struct["end"]
    )
    return joined.with_columns(when(is_correct).then(new_col))


def _join_into_periods_check(
    df: DataFrame, column: str, left_or_right: Literal["left", "right"], /
) -> None:
    start = df[column].struct["start"]
    end = df[column].struct["end"]
    if not (start <= end).all():
        raise _JoinIntoPeriodsPeriodError(left_or_right=left_or_right, column=column)
    try:
        assert_series_equal(start, start.sort())
    except AssertionError:
        raise _JoinIntoPeriodsSortedError(
            left_or_right=left_or_right, column=column, start_or_end="start"
        ) from None
    try:
        assert_series_equal(end, end.sort())
    except AssertionError:
        raise _JoinIntoPeriodsSortedError(
            left_or_right=left_or_right, column=column, start_or_end="end"
        ) from None
    if (df.height >= 2) and (end[:-1] > start[1:]).any():
        raise _JoinIntoPeriodsOverlappingError(
            left_or_right=left_or_right, column=column
        )


@dataclass(kw_only=True, slots=True)
class JoinIntoPeriodsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _JoinIntoPeriodsArgumentsError(JoinIntoPeriodsError):
    on: str | None
    left_on: str | None
    right_on: str | None

    @override
    def __str__(self) -> str:
        return f"Either 'on' must be given or 'left_on' and 'right_on' must be given; got {self.on!r}, {self.left_on!r} and {self.right_on!r}"


@dataclass(kw_only=True, slots=True)
class _JoinIntoPeriodsPeriodError(JoinIntoPeriodsError):
    left_or_right: Literal["left", "right"]
    column: str

    @override
    def __str__(self) -> str:
        return f"{self.left_or_right.title()} DataFrame column {self.column!r} must contain valid periods"


@dataclass(kw_only=True, slots=True)
class _JoinIntoPeriodsSortedError(JoinIntoPeriodsError):
    left_or_right: Literal["left", "right"]
    column: str
    start_or_end: Literal["start", "end"]

    @override
    def __str__(self) -> str:
        return f"{self.left_or_right.title()} DataFrame column '{self.column}/{self.start_or_end}' must be sorted"


@dataclass(kw_only=True, slots=True)
class _JoinIntoPeriodsOverlappingError(JoinIntoPeriodsError):
    left_or_right: Literal["left", "right"]
    column: str

    @override
    def __str__(self) -> str:
        return f"{self.left_or_right.title()} DataFrame column {self.column!r} must not contain overlaps"


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
        case never:
            assert_never(never)


def _map_over_series_one(func: Callable[[Series], Series], series: Series, /) -> Series:
    if isinstance(series.dtype, Struct):
        unnested = series.struct.unnest()
        name = series.name
        return map_over_columns(func, unnested).select(struct("*").alias(name))[name]
    return func(series)


##


def nan_sum_agg(column: str | Expr, /) -> Expr:
    """Nan sum aggregation."""
    col_use = col(column) if isinstance(column, str) else column
    return when(col_use.is_not_null().any()).then(col_use.sum())


##


@overload
def nan_sum_horizontal(*columns: Series) -> Series: ...
@overload
def nan_sum_horizontal(*columns: IntoExprColumn) -> ExprOrSeries: ...
def nan_sum_horizontal(*columns: IntoExprColumn) -> ExprOrSeries:
    """Nan sum across columns."""
    columns2 = ensure_expr_or_series_many(*columns)
    expr = when(any_horizontal(*(c.is_not_null() for c in columns2))).then(
        sum_horizontal(*columns2)
    )
    return try_reify_expr(expr, *columns2)


##


@overload
def normal_pdf(
    x: ExprLike,
    /,
    *,
    loc: float | IntoExprColumn = 0.0,
    scale: float | IntoExprColumn = 1.0,
) -> Expr: ...
@overload
def normal_pdf(
    x: Series,
    /,
    *,
    loc: float | IntoExprColumn = 0.0,
    scale: float | IntoExprColumn = 1.0,
) -> Series: ...
@overload
def normal_pdf(
    x: IntoExprColumn,
    /,
    *,
    loc: float | IntoExprColumn = 0.0,
    scale: float | IntoExprColumn = 1.0,
) -> ExprOrSeries: ...
def normal_pdf(
    x: IntoExprColumn,
    /,
    *,
    loc: float | IntoExprColumn = 0.0,
    scale: float | IntoExprColumn = 1.0,
) -> ExprOrSeries:
    """Compute the PDF of a normal distribution."""
    x = ensure_expr_or_series(x)
    loc = loc if isinstance(loc, int | float) else ensure_expr_or_series(loc)
    scale = scale if isinstance(scale, int | float) else ensure_expr_or_series(scale)
    expr = (1 / (scale * sqrt(2 * pi))) * (-(1 / 2) * ((x - loc) / scale) ** 2).exp()
    return try_reify_expr(expr, x)


##


def normal_rv(
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
            return normal_rv(
                series.len(), loc=loc, scale=scale, seed=seed, name=name, dtype=dtype
            )
        case DataFrame() as df:
            return normal_rv(
                df.height, loc=loc, scale=scale, seed=seed, name=name, dtype=dtype
            )
        case never:
            assert_never(never)


##


@overload
def number_of_decimals(
    column: ExprLike, /, *, max_decimals: int = MAX_DECIMALS
) -> Expr: ...
@overload
def number_of_decimals(
    column: Series, /, *, max_decimals: int = MAX_DECIMALS
) -> Series: ...
@overload
def number_of_decimals(
    column: IntoExprColumn, /, *, max_decimals: int = MAX_DECIMALS
) -> ExprOrSeries: ...
def number_of_decimals(
    column: IntoExprColumn, /, *, max_decimals: int = MAX_DECIMALS
) -> ExprOrSeries:
    """Get the number of decimals."""
    column = ensure_expr_or_series(column)
    frac = column - column.floor()
    results = (
        _number_of_decimals_check_scale(frac, s) for s in range(max_decimals + 1)
    )
    return first_true_horizontal(*results)


def _number_of_decimals_check_scale(frac: ExprOrSeries, scale: int, /) -> ExprOrSeries:
    scaled = 10**scale * frac
    return is_close(scaled, scaled.round()).alias(str(scale))


##


def offset_datetime(
    datetime: ZonedDateTime, offset: str, /, *, n: int = 1
) -> ZonedDateTime:
    """Offset a datetime as `polars` would."""
    sr = Series(values=[datetime.py_datetime()])
    for _ in range(n):
        sr = sr.dt.offset_by(offset)
    return ZonedDateTime.from_py_datetime(sr.item())


##


def one_column(df: DataFrame, /) -> Series:
    """Return the unique column in a DataFrame."""
    try:
        return df[one(df.columns)]
    except OneEmptyError:
        raise OneColumnEmptyError(df=df) from None
    except OneNonUniqueError as error:
        raise OneColumnNonUniqueError(
            df=df, first=error.first, second=error.second
        ) from None


@dataclass(kw_only=True, slots=True)
class OneColumnError(Exception):
    df: DataFrame


@dataclass(kw_only=True, slots=True)
class OneColumnEmptyError(OneColumnError):
    @override
    def __str__(self) -> str:
        return "DataFrame must not be empty"


@dataclass(kw_only=True, slots=True)
class OneColumnNonUniqueError(OneColumnError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must contain exactly one column; got {self.first!r}, {self.second!r} and perhaps more"


##


@overload
def order_of_magnitude(column: ExprLike, /, *, round_: bool = False) -> Expr: ...
@overload
def order_of_magnitude(column: Series, /, *, round_: bool = False) -> Series: ...
@overload
def order_of_magnitude(
    column: IntoExprColumn, /, *, round_: bool = False
) -> ExprOrSeries: ...
def order_of_magnitude(
    column: IntoExprColumn, /, *, round_: bool = False
) -> ExprOrSeries:
    """Compute the order of magnitude of a column."""
    column = ensure_expr_or_series(column)
    result = column.abs().log10()
    return result.round().cast(Int64) if round_ else result


##


@overload
def period_range(
    start: ZonedDateTime,
    end_or_length: ZonedDateTime | int,
    /,
    *,
    interval: str = "1d",
    time_unit: TimeUnit | None = None,
    time_zone: TimeZoneLike | None = None,
    eager: Literal[True],
) -> Series: ...
@overload
def period_range(
    start: ZonedDateTime,
    end_or_length: ZonedDateTime | int,
    /,
    *,
    interval: str = "1d",
    time_unit: TimeUnit | None = None,
    time_zone: TimeZoneLike | None = None,
    eager: Literal[False] = False,
) -> Expr: ...
@overload
def period_range(
    start: ZonedDateTime,
    end_or_length: ZonedDateTime | int,
    /,
    *,
    interval: str = "1d",
    time_unit: TimeUnit | None = None,
    time_zone: TimeZoneLike | None = None,
    eager: bool = False,
) -> Series | Expr: ...
def period_range(
    start: ZonedDateTime,
    end_or_length: ZonedDateTime | int,
    /,
    *,
    interval: str = "1d",
    time_unit: TimeUnit | None = None,
    time_zone: TimeZoneLike | None = None,
    eager: bool = False,
) -> Series | Expr:
    """Construct a period range."""
    time_zone_use = None if time_zone is None else to_time_zone_name(time_zone)
    match end_or_length:
        case ZonedDateTime() as end:
            ...
        case int() as length:
            end = offset_datetime(start, interval, n=length)
        case never:
            assert_never(never)
    starts = datetime_range(
        start.py_datetime(),
        end.py_datetime(),
        interval,
        closed="left",
        time_unit=time_unit,
        time_zone=time_zone_use,
        eager=eager,
    ).alias("start")
    ends = starts.dt.offset_by(interval).alias("end")
    period = struct(starts, ends)
    return try_reify_expr(period, starts, ends)


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
    try:
        return one_column(df)
    except OneColumnNonUniqueError:
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
        time_zone_use = None if time_zone is None else to_time_zone_name(time_zone)
        return sr.dt.replace_time_zone(time_zone_use)
    return sr


##


def read_series(path: PathLike, /, *, decompress: bool = False) -> Series:
    """Read a Series from disk."""
    data = read_bytes(path, decompress=decompress)
    return deserialize_series(data)


def write_series(
    series: Series,
    path: PathLike,
    /,
    *,
    compress: bool = False,
    overwrite: bool = False,
) -> None:
    """Write a Series to disk."""
    data = serialize_series(series)
    write_bytes(path, data, compress=compress, overwrite=overwrite, json=True)


def read_dataframe(path: PathLike, /, *, decompress: bool = False) -> DataFrame:
    """Read a DataFrame from disk."""
    data = read_bytes(path, decompress=decompress)
    return deserialize_dataframe(data)


def write_dataframe(
    df: DataFrame, path: PathLike, /, *, compress: bool = False, overwrite: bool = False
) -> None:
    """Write a DataFrame to disk."""
    data = serialize_dataframe(df)
    write_bytes(path, data, compress=compress, overwrite=overwrite, json=True)


def serialize_series(series: Series, /) -> bytes:
    """Serialize a Series."""
    from utilities.orjson import serialize

    values = series.to_list()
    decon = _deconstruct_dtype(series.dtype)
    return serialize((series.name, values, decon))


def deserialize_series(data: bytes, /) -> Series:
    """Serialize a Series."""
    from utilities.orjson import deserialize

    name, values, decon = deserialize(data)
    dtype = _reconstruct_dtype(decon)
    return Series(name=name, values=values, dtype=dtype)


def serialize_dataframe(df: DataFrame, /) -> bytes:
    """Serialize a DataFrame."""
    from utilities.orjson import serialize

    rows = df.rows()
    decon = _deconstruct_schema(df.schema)
    return serialize((rows, decon))


def deserialize_dataframe(data: bytes, /) -> DataFrame:
    """Serialize a DataFrame."""
    from utilities.orjson import deserialize

    rows, decon = deserialize(data)
    schema = _reconstruct_schema(decon)
    return DataFrame(data=rows, schema=schema, orient="row")


type _DeconSchema = Sequence[tuple[str, _DeconDType]]
type _DeconDType = (
    str
    | tuple[Literal["Datetime"], str, str | None]
    | tuple[Literal["List"], _DeconDType]
    | tuple[Literal["Struct"], _DeconSchema]
)


def _deconstruct_schema(schema: Schema, /) -> _DeconSchema:
    return [(k, _deconstruct_dtype(v)) for k, v in schema.items()]


def _deconstruct_dtype(dtype: PolarsDataType, /) -> _DeconDType:
    match dtype:
        case List() as list_:
            return "List", _deconstruct_dtype(list_.inner)
        case Struct() as struct:
            inner = Schema({f.name: f.dtype for f in struct.fields})
            return "Struct", _deconstruct_schema(inner)
        case Datetime() as datetime:
            return "Datetime", datetime.time_unit, datetime.time_zone
        case _:
            return repr(dtype)


def _reconstruct_schema(schema: _DeconSchema, /) -> Schema:
    return Schema({k: _reconstruct_dtype(v) for k, v in schema})


def _reconstruct_dtype(obj: _DeconDType, /) -> PolarsDataType:
    match obj:
        case str() as name:
            return getattr(pl, name)
        case "Datetime", str() as time_unit, str() | None as time_zone:
            return Datetime(time_unit=cast("TimeUnit", time_unit), time_zone=time_zone)
        case "List", inner:
            return List(_reconstruct_dtype(inner))
        case "Struct", inner:
            return Struct(_reconstruct_schema(inner))
        case never:
            assert_never(never)


##


@overload
def round_to_float(
    x: ExprLike, y: float, /, *, mode: RoundMode = "half_to_even"
) -> Expr: ...
@overload
def round_to_float(
    x: Series, y: float | ExprOrSeries, /, *, mode: RoundMode = "half_to_even"
) -> Series: ...
@overload
def round_to_float(
    x: ExprLike, y: Series, /, *, mode: RoundMode = "half_to_even"
) -> Series: ...
@overload
def round_to_float(
    x: ExprLike, y: Expr, /, *, mode: RoundMode = "half_to_even"
) -> Expr: ...
@overload
def round_to_float(
    x: IntoExprColumn, y: float | Series, /, *, mode: RoundMode = "half_to_even"
) -> ExprOrSeries: ...
def round_to_float(
    x: IntoExprColumn, y: float | IntoExprColumn, /, *, mode: RoundMode = "half_to_even"
) -> ExprOrSeries:
    """Round a column to the nearest multiple of another float."""
    x = ensure_expr_or_series(x)
    y = y if isinstance(y, int | float) else ensure_expr_or_series(y)
    match x, y:
        case Expr() | Series(), int() | float():
            z = (x / y).round(mode=mode) * y
            return z.round(decimals=utilities.math.number_of_decimals(y) + 1)
        case Series(), Expr() | Series():
            df = (
                x
                .to_frame()
                .with_columns(y)
                .with_columns(number_of_decimals(y).alias("_decimals"))
                .with_row_index(name="_index")
                .group_by("_decimals")
                .map_groups(_round_to_float_one)
                .sort("_index")
            )
            return df[df.columns[1]]
        case Expr(), Series():
            df = y.to_frame().with_columns(x)
            return round_to_float(df[df.columns[1]], df[df.columns[0]], mode=mode)
        case Expr(), Expr() | str():
            raise RoundToFloatError(x=x, y=y)
        case never:
            assert_never(never)


def _round_to_float_one(df: DataFrame, /) -> DataFrame:
    decimals: int | None = df["_decimals"].unique().item()
    name = df.columns[1]
    match decimals:
        case int():
            expr = col(name).round(decimals=decimals)
        case None:
            expr = lit(None, dtype=Float64).alias(name)
        case never:
            assert_never(never)
    return df.with_columns(expr)


@dataclass(kw_only=True, slots=True)
class RoundToFloatError(Exception):
    x: IntoExprColumn
    y: IntoExprColumn

    @override
    def __str__(self) -> str:
        return f"At least 1 of the dividend and/or divisor must be a Series; got {get_class_name(self.x)!r} and {get_class_name(self.y)!r}"


##


def search_period(
    series: Series,
    date_time: ZonedDateTime,
    /,
    *,
    start_or_end: Literal["start", "end"] = "end",
) -> int | None:
    """Search a series of periods for the one containing a given date-time."""
    end = series.struct["end"]
    py_date_time = date_time.py_datetime()
    match start_or_end:
        case "start":
            index = end.search_sorted(py_date_time, side="right")
            if index >= len(series):
                return None
            item: dt.datetime = series[index]["start"]
            return index if py_date_time >= item else None
        case "end":
            index = end.search_sorted(py_date_time, side="left")
            if index >= len(series):
                return None
            item: dt.datetime = series[index]["start"]
            return index if py_date_time > item else None
        case never:
            assert_never(never)


##


def select_exact(
    df: DataFrame, /, *columns: IntoExprColumn, drop: MaybeIterable[str] | None = None
) -> DataFrame:
    """Select an exact set of columns from a DataFrame."""
    names = [get_expr_name(df, c) for c in columns]
    drop = set() if drop is None else set(always_iterable(drop))
    union = set(names) | drop
    extra = [c for c in df.columns if c not in union]
    if len(extra) >= 1:
        raise SelectExactError(columns=extra)
    return df.select(*columns)


@dataclass(kw_only=True, slots=True)
class SelectExactError(Exception):
    columns: list[str]

    @override
    def __str__(self) -> str:
        return (
            f"All columns must be selected; got {pretty_repr(self.columns)} remaining"
        )


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


@overload
def to_true(column: ExprLike, /) -> Expr: ...
@overload
def to_true(column: Series, /) -> Series: ...
@overload
def to_true(column: IntoExprColumn, /) -> ExprOrSeries: ...
def to_true(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute when a boolean series turns True."""
    t = is_true(column)
    return ((~t).shift() & t).fill_null(value=False)


@overload
def to_not_true(column: ExprLike, /) -> Expr: ...
@overload
def to_not_true(column: Series, /) -> Series: ...
@overload
def to_not_true(column: IntoExprColumn, /) -> ExprOrSeries: ...
def to_not_true(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute when a boolean series turns non-True."""
    t = is_true(column)
    return (t.shift() & (~t)).fill_null(value=False)


@overload
def to_false(column: ExprLike, /) -> Expr: ...
@overload
def to_false(column: Series, /) -> Series: ...
@overload
def to_false(column: IntoExprColumn, /) -> ExprOrSeries: ...
def to_false(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute when a boolean series turns False."""
    f = is_false(column)
    return ((~f).shift() & f).fill_null(value=False)


@overload
def to_not_false(column: ExprLike, /) -> Expr: ...
@overload
def to_not_false(column: Series, /) -> Series: ...
@overload
def to_not_false(column: IntoExprColumn, /) -> ExprOrSeries: ...
def to_not_false(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute when a boolean series turns non-False."""
    f = is_false(column)
    return (f.shift() & (~f)).fill_null(value=False)


##


@overload
def true_like(column: ExprLike, /) -> Expr: ...
@overload
def true_like(column: Series, /) -> Series: ...
@overload
def true_like(column: IntoExprColumn, /) -> ExprOrSeries: ...
def true_like(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute a column of `True` values."""
    column = ensure_expr_or_series(column)
    return column.is_null() | column.is_not_null()


@overload
def false_like(column: ExprLike, /) -> Expr: ...
@overload
def false_like(column: Series, /) -> Series: ...
@overload
def false_like(column: IntoExprColumn, /) -> ExprOrSeries: ...
def false_like(column: IntoExprColumn, /) -> ExprOrSeries:
    """Compute a column of `False` values."""
    column = ensure_expr_or_series(column)
    return column.is_null() & column.is_not_null()


##


def try_reify_expr(
    expr: IntoExprColumn, /, *exprs: IntoExprColumn, **named_exprs: IntoExprColumn
) -> ExprOrSeries:
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
        case never:
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
        case never:
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
def week_num(column: IntoExprColumn, /, *, start: WeekDay = "mon") -> ExprOrSeries: ...
def week_num(column: IntoExprColumn, /, *, start: WeekDay = "mon") -> ExprOrSeries:
    """Compute the week number of a date column."""
    column = ensure_expr_or_series(column)
    epoch = column.dt.epoch(time_unit="d").alias("epoch")
    offset = get_args(WeekDay).index(start)
    return (epoch + 3 - offset) // 7


##


def zoned_date_time_dtype(
    *, time_unit: TimeUnit = "us", time_zone: TimeZoneLike = UTC
) -> Datetime:
    """Create a zoned date-time data type."""
    return Datetime(time_unit=time_unit, time_zone=to_time_zone_name(time_zone))


def zoned_date_time_period_dtype(
    *,
    time_unit: TimeUnit = "us",
    time_zone: TimeZoneLike | tuple[TimeZoneLike, TimeZoneLike] = UTC,
) -> Struct:
    """Create a zoned date-time period data type."""
    match time_zone:
        case start, end:
            return struct_dtype(
                start=zoned_date_time_dtype(time_unit=time_unit, time_zone=start),
                end=zoned_date_time_dtype(time_unit=time_unit, time_zone=end),
            )
        case _:
            dtype = zoned_date_time_dtype(time_unit=time_unit, time_zone=time_zone)
            return struct_dtype(start=dtype, end=dtype)


__all__ = [
    "AppendRowError",
    "BooleanValueCountsError",
    "CheckPolarsDataFrameError",
    "ColumnsToDictError",
    "DataClassToDataFrameError",
    "DatePeriodDType",
    "DatetimeHongKong",
    "DatetimeTokyo",
    "DatetimeUSCentral",
    "DatetimeUSEastern",
    "DatetimeUTC",
    "ExprOrSeries",
    "FiniteEWMMeanError",
    "GetDataTypeOrSeriesTimeZoneError",
    "InsertAfterError",
    "InsertBeforeError",
    "InsertBetweenError",
    "IsNearEventError",
    "OneColumnEmptyError",
    "OneColumnError",
    "OneColumnNonUniqueError",
    "RoundToFloatError",
    "SelectExactError",
    "SetFirstRowAsColumnsError",
    "TimePeriodDType",
    "acf",
    "adjust_frequencies",
    "all_dataframe_columns",
    "all_series",
    "any_dataframe_columns",
    "any_series",
    "append_row",
    "are_frames_equal",
    "bernoulli",
    "boolean_value_counts",
    "check_polars_dataframe",
    "choice",
    "columns_to_dict",
    "concat_series",
    "convert_time_zone",
    "cross",
    "dataclass_to_dataframe",
    "dataclass_to_schema",
    "decreasing_horizontal",
    "deserialize_dataframe",
    "ensure_data_type",
    "ensure_expr_or_series",
    "ensure_expr_or_series_many",
    "expr_to_series",
    "false_like",
    "filter_date",
    "filter_time",
    "finite_ewm_mean",
    "first_true_horizontal",
    "get_data_type_or_series_time_zone",
    "get_expr_name",
    "get_frequency_spectrum",
    "increasing_horizontal",
    "insert_after",
    "insert_before",
    "insert_between",
    "integers",
    "is_close",
    "is_false",
    "is_near_event",
    "is_true",
    "join",
    "join_into_periods",
    "map_over_columns",
    "nan_sum_agg",
    "nan_sum_horizontal",
    "normal_pdf",
    "normal_rv",
    "number_of_decimals",
    "offset_datetime",
    "one_column",
    "order_of_magnitude",
    "period_range",
    "read_dataframe",
    "read_series",
    "replace_time_zone",
    "round_to_float",
    "search_period",
    "select_exact",
    "serialize_dataframe",
    "set_first_row_as_columns",
    "struct_dtype",
    "to_false",
    "to_not_false",
    "to_not_true",
    "to_true",
    "touch",
    "true_like",
    "try_reify_expr",
    "uniform",
    "unique_element",
    "write_dataframe",
    "write_series",
    "zoned_date_time_dtype",
    "zoned_date_time_period_dtype",
]
