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
        raise DataFrameColumnsError(msg)
    if (dtypes is not None) and (df.dtypes != dtypes):
        msg = f"{df=}, {dtypes=}"
        raise DataFrameDTypesError(msg)
    if height is not None:
        _check_dataframe_height(df, height)
    if (min_height is not None) and (len(df) < min_height):
        msg = f"{df=}, {min_height=}"
        raise DataFrameMinHeightError(msg)
    if (max_height is not None) and (len(df) > max_height):
        msg = f"{df=}, {max_height=}"
        raise DataFrameMaxHeightError(msg)
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
        raise DataFrameSchemaError(msg)
    if (shape is not None) and (df.shape != shape):
        msg = f"{df=}"
        raise DataFrameShapeError(msg)
    if sorted is not None:
        df_sorted = df.sort(sorted)
        try:
            assert_frame_equal(df, df_sorted)
        except AssertionError:
            msg = f"{df=}, {sorted=}"
            raise DataFrameSortedError(msg) from None
    if (unique is not None) and df.select(unique).is_duplicated().any():
        msg = f"{df=}, {unique=}"
        raise DataFrameUniqueError(msg)
    if (width is not None) and (df.width != width):
        msg = f"{df=}"
        raise DataFrameWidthError(msg)


class DataFrameColumnsError(Exception):
    """Raised when a DataFrame has the incorrect columns."""


class DataFrameDTypesError(Exception):
    """Raised when a DataFrame has the incorrect dtypes."""


class DataFrameMinHeightError(Exception):
    """Raised when a DataFrame does not reach the minimum height."""


class DataFrameMaxHeightError(Exception):
    """Raised when a DataFrame exceeds the maximum height."""


class DataFrameSchemaError(Exception):
    """Raised when a DataFrame has the incorrect schema."""


class DataFrameShapeError(Exception):
    """Raised when a DataFrame has the incorrect shape."""


class DataFrameSortedError(Exception):
    """Raised when a DataFrame has non-sorted values."""


class DataFrameUniqueError(Exception):
    """Raised when a DataFrame has non-unique values."""


class DataFrameWidthError(Exception):
    """Raised when a DataFrame has the incorrect width."""


def _check_dataframe_height(df: DataFrame, height: int | tuple[int, float], /) -> None:
    if isinstance(height, int) and (len(df) != height):
        msg = f"{df=}, {height=}"
        raise DataFrameHeightError(msg)
    if isinstance(height, tuple):
        height_int, rel_tol = height
        if not isclose(len(df), height_int, rel_tol=rel_tol):
            msg = f"{df=}, {height=}"
            raise DataFrameHeightError(msg)


class DataFrameHeightError(Exception):
    """Raised when a DataFrame has the incorrect height."""


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
        raise EmptyDataFrameError(msg) from None
    mapping = dict(zip(df.columns, row, strict=True))
    return df[1:].rename(mapping)


class EmptyDataFrameError(Exception):
    """Raised when a DataFrame is empty."""


__all__ = [
    "check_dataframe",
    "DataFrameColumnsError",
    "DataFrameDTypesError",
    "DataFrameHeightError",
    "DataFrameMaxHeightError",
    "DataFrameMinHeightError",
    "DataFrameSchemaError",
    "DataFrameShapeError",
    "DataFrameSortedError",
    "DataFrameUniqueError",
    "DataFrameWidthError",
    "EmptyDataFrameError",
    "join",
    "set_first_row_as_columns",
]
