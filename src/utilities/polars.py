from __future__ import annotations

from collections.abc import Iterable, Sequence
from functools import reduce
from itertools import chain
from math import isclose

from polars import DataFrame, Expr, PolarsDataType
from polars.exceptions import OutOfBoundsError
from polars.testing import assert_frame_equal
from polars.type_aliases import IntoExpr, JoinStrategy, JoinValidation, SchemaDict

from utilities.typing import SequenceStrs


def check_dataframe(
    df: DataFrame,
    /,
    *,
    columns: SequenceStrs | None = None,
    dtypes: list[PolarsDataType] | None = None,
    height: int | tuple[int, float] | None = None,
    min_height: int | None = None,
    max_height: int | None = None,
    schema: SchemaDict | None = None,
    shape: tuple[int, int] | None = None,
    sorted: IntoExpr | Iterable[IntoExpr] | None = None,  # noqa: A002
    unique: IntoExpr | Iterable[IntoExpr] | None = None,
    width: int | None = None,
) -> None:
    """Check the properties of a DataFrame."""
    if (columns is not None) and (df.columns != list(columns)):
        msg = f"{df=}, {columns=}"
        raise CheckDataFrameError(msg)
    if (dtypes is not None) and (df.dtypes != dtypes):
        msg = f"{df=}, {dtypes=}"
        raise CheckDataFrameError(msg)
    if height is not None:
        check_dataframe_height(df, height)
    if (min_height is not None) and (len(df) < min_height):
        msg = f"{df=}, {min_height=}"
        raise CheckDataFrameError(msg)
    if (max_height is not None) and (len(df) > max_height):
        msg = f"{df=}, {max_height=}"
        raise CheckDataFrameError(msg)
    if (schema is not None) and (df.schema != schema):
        set_act, set_exp = map(set, [df.schema, schema])
        extra = set_act - set_exp
        missing = set_exp - set_act
        differ = {
            col: (left, right)
            for col in set_act & set_exp
            if (left := df.schema[col]) != (right := schema[col])
        }
        msg = f"{df=}, {extra=}, {missing=}, {differ=}"
        raise CheckDataFrameError(msg)
    if (shape is not None) and (df.shape != shape):
        msg = f"{df=}"
        raise CheckDataFrameError(msg)
    if sorted is not None:
        df_sorted = df.sort(sorted)
        try:
            assert_frame_equal(df, df_sorted)
        except AssertionError:
            msg = f"{df=}, {sorted=}"
            raise CheckDataFrameError(msg) from None
    if (unique is not None) and df.select(unique).is_duplicated().any():
        msg = f"{df=}, {unique=}"
        raise CheckDataFrameError(msg)
    if (width is not None) and (df.width != width):
        msg = f"{df=}"
        raise CheckDataFrameError(msg)


class CheckDataFrameError(Exception):
    ...


def check_dataframe_height(df: DataFrame, height: int | tuple[int, float], /) -> None:
    """Check the height of a DataFrame."""
    if isinstance(height, int) and (len(df) != height):
        msg = f"{df=}, {height=}"
        raise CheckDataFrameHeightError(msg)
    if isinstance(height, tuple):
        height_int, rel_tol = height
        if not isclose(len(df), height_int, rel_tol=rel_tol):
            msg = f"{df=}, {height=}"
            raise CheckDataFrameHeightError(msg)


class CheckDataFrameHeightError(Exception):
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


def set_first_row_as_columns(df: DataFrame, /) -> DataFrame:
    """Set the first row of a DataFrame as its columns."""

    try:
        row = df.row(0)
    except OutOfBoundsError:
        msg = f"{df=}"
        raise SetFirstRowAsColumnsError(msg) from None
    mapping = dict(zip(df.columns, row, strict=True))
    return df[1:].rename(mapping)


class SetFirstRowAsColumnsError(Exception):
    ...


__all__ = [
    "check_dataframe_height",
    "check_dataframe",
    "CheckDataFrameError",
    "CheckDataFrameHeightError",
    "join",
    "set_first_row_as_columns",
    "SetFirstRowAsColumnsError",
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
    __all__ += ["table_tag_to_dataframe", "TableTagToDataFrameError", "yield_tables"]
