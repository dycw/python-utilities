from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from functools import reduce
from itertools import chain
from typing import TYPE_CHECKING, Any, cast

from polars import Boolean, DataFrame, Expr, PolarsDataType, col, lit, when
from polars.exceptions import ColumnNotFoundError, OutOfBoundsError
from polars.testing import assert_frame_equal
from polars.type_aliases import IntoExpr, JoinStrategy, JoinValidation, SchemaDict
from typing_extensions import Never, assert_never, override

from utilities.errors import redirect_error
from utilities.iterables import (
    CheckIterablesEqualError,
    CheckMappingsEqualError,
    CheckSuperMappingError,
    check_iterables_equal,
    check_mappings_equal,
    check_supermapping,
    is_iterable_not_str,
)
from utilities.math import CheckIntegerError, check_integer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from utilities.types import IterableStrs


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
    schema: SchemaDict | None = None,
    schema_inc: SchemaDict | None = None,
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
    if schema is not None:
        _check_polars_dataframe_schema(df, schema)
    if schema_inc is not None:
        _check_polars_dataframe_schema_inc(df, schema_inc)
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
        except ColumnNotFoundError:  # noqa: PERF203
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


def _check_polars_dataframe_schema(df: DataFrame, schema: SchemaDict, /) -> None:
    try:
        check_mappings_equal(df.schema, schema)
    except CheckMappingsEqualError as error:
        raise _CheckPolarsDataFrameSchemaError(df=df, schema=schema) from error
    try:
        _check_polars_dataframe_columns(df, schema)
    except _CheckPolarsDataFrameColumnsError as error:
        raise _CheckPolarsDataFrameSchemaError(df=df, schema=schema) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameSchemaError(CheckPolarsDataFrameError):
    schema: SchemaDict

    @override
    def __str__(self) -> str:
        return f"DataFrame must have schema {self.schema}; got {self.df.columns}\n\n{self.df}"


def _check_polars_dataframe_schema_inc(df: DataFrame, schema: SchemaDict, /) -> None:
    try:
        check_supermapping(df.schema, schema)
    except CheckSuperMappingError as error:
        raise _CheckPolarsDataFrameSchemaIncError(df=df, schema=schema) from error


@dataclass(kw_only=True)
class _CheckPolarsDataFrameSchemaIncError(CheckPolarsDataFrameError):
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


def join(
    df: DataFrame,
    *dfs: DataFrame,
    on: str | Expr | Sequence[str | Expr],
    how: JoinStrategy = "inner",
    validate: JoinValidation = "m:m",
) -> DataFrame:
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


__all__ = [
    "CheckPolarsDataFrameError",
    "EmptyPolarsConcatError",
    "SetFirstRowAsColumnsError",
    "check_polars_dataframe",
    "join",
    "nan_sum_agg",
    "nan_sum_cols",
    "redirect_empty_polars_concat",
    "set_first_row_as_columns",
]
