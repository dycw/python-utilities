from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from functools import reduce
from itertools import chain
from typing import Any

from polars import Boolean, DataFrame, Expr, PolarsDataType, col, lit, when
from polars.exceptions import ColumnNotFoundError, OutOfBoundsError
from polars.testing import assert_frame_equal
from polars.type_aliases import IntoExpr, JoinStrategy, JoinValidation, SchemaDict

from utilities.errors import redirect_error
from utilities.math import is_equal_or_approx
from utilities.types import SequenceStrs


def check_polars_dataframe(
    df: DataFrame,
    /,
    *,
    columns: SequenceStrs | None = None,
    dtypes: list[PolarsDataType] | None = None,
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
    if (columns is not None) and (df.columns != list(columns)):
        msg = f"{df=}, {columns=}"
        raise CheckPolarsDataFrameError(msg)
    if (dtypes is not None) and (df.dtypes != dtypes):
        msg = f"{df=}, {dtypes=}"
        raise CheckPolarsDataFrameError(msg)
    if (height is not None) and not is_equal_or_approx(df.height, height):
        msg = f"{df=}, {height=}"
        raise CheckPolarsDataFrameError(msg)
    if (min_height is not None) and (len(df) < min_height):
        msg = f"{df=}, {min_height=}"
        raise CheckPolarsDataFrameError(msg)
    if (max_height is not None) and (len(df) > max_height):
        msg = f"{df=}, {max_height=}"
        raise CheckPolarsDataFrameError(msg)
    if predicates is not None:
        _check_polars_dataframe_predicates(df, predicates)
    if schema is not None:
        _check_polars_dataframe_schema(df, schema)
    if schema_inc is not None:
        _check_polars_dataframe_schema_inc(df, schema_inc)
    if (shape is not None) and (df.shape != shape):
        msg = f"{df=}"
        raise CheckPolarsDataFrameError(msg)
    if sorted is not None:
        df_sorted = df.sort(sorted)
        with redirect_error(AssertionError, CheckPolarsDataFrameError(f"{df=}")):
            assert_frame_equal(df, df_sorted)
    if (unique is not None) and df.select(unique).is_duplicated().any():
        msg = f"{df=}, {unique=}"
        raise CheckPolarsDataFrameError(msg)
    if (width is not None) and (df.width != width):
        msg = f"{df=}"
        raise CheckPolarsDataFrameError(msg)


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
        msg = f"{missing=}, {failed=}"
        raise CheckPolarsDataFrameError(msg)


def _check_polars_dataframe_schema(df: DataFrame, schema: SchemaDict, /) -> None:
    if df.schema != schema:
        set_act, set_exp = map(set, [df.schema, schema])
        extra = set_act - set_exp
        missing = set_exp - set_act
        differ = {
            col: (left, right)
            for col in set_act & set_exp
            if (left := df.schema[col]) != (right := schema[col])
        }
        msg = f"{df=}, {extra=}, {missing=}, {differ=}"
        raise CheckPolarsDataFrameError(msg)


def _check_polars_dataframe_schema_inc(
    df: DataFrame, schema_inc: SchemaDict, /
) -> None:
    missing: set[str] = set()
    wrong_dtype: set[str] = set()
    for column, dtype in schema_inc.items():
        try:
            sr = df[column]
        except ColumnNotFoundError:  # noqa: PERF203
            missing.add(column)
        else:
            if sr.dtype != dtype:
                wrong_dtype.add(column)
    if (len(missing) >= 1) or (len(wrong_dtype)) >= 1:
        msg = f"{missing=}, {wrong_dtype=}"
        raise CheckPolarsDataFrameError(msg)


class CheckPolarsDataFrameError(Exception):
    ...


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


class EmptyPolarsConcatError(Exception):
    ...


def set_first_row_as_columns(df: DataFrame, /) -> DataFrame:
    """Set the first row of a DataFrame as its columns."""

    with redirect_error(OutOfBoundsError, SetFirstRowAsColumnsError(f"{df=}")):
        row = df.row(0)
    mapping = dict(zip(df.columns, row, strict=True))
    return df[1:].rename(mapping)


class SetFirstRowAsColumnsError(Exception):
    ...


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


try:
    from utilities._polars.bs4 import (
        TableTagToDataFrameError,
        table_tag_to_dataframe,
        yield_tables,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["TableTagToDataFrameError", "table_tag_to_dataframe", "yield_tables"]
