from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from datetime import timezone
from math import nan
from typing import Any, Literal, cast, overload

import polars as pl
from polars import (
    DataFrame,
    Date,
    Datetime,
    Expr,
    Float64,
    Int64,
    PolarsDataType,
    Utf8,
    col,
    read_database,
    when,
)
from polars.type_aliases import ConnectionOrCursor, SchemaDict
from sqlalchemy import Column, Connection, Engine, Select, Table
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from utilities.datetime import UTC
from utilities.sqlalchemy import (
    CHUNK_SIZE_FRAC,
    check_dataframe_schema_against_table,
    check_selectable_for_duplicate_columns,
    insert_items,
    yield_connection,
)
from utilities.text import snake_case


def insert_polars_dataframe(
    engine_or_conn: Engine | Connection,
    df: DataFrame,
    table_or_mapped_class: Table | type[Any],
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> None:
    """Insert a DataFrame into a database."""
    mapping = check_dataframe_schema_against_table(
        dict(df.schema), table_or_mapped_class, _check_polars_series, snake=snake
    )
    items = df.select(mapping).rename(mapping).to_dicts()
    if (df.height > 0) and (len(items) == 0):
        msg = f"{df=}, {items=}"
        raise PolarsDataFrameYieldsNoRowsError(msg)
    return insert_items(
        engine_or_conn, (items, table_or_mapped_class), chunk_size_frac=chunk_size_frac
    )


class PolarsDataFrameYieldsNoRowsError(Exception):
    """Raised when a DataFrame yields no rows to insert."""


def _check_polars_series(dtype: PolarsDataType, py_type: type, /) -> bool:
    return (
        (dtype is pl.Boolean and issubclass(py_type, bool))
        or (
            dtype is Date
            and issubclass(py_type, dt.date)
            and not issubclass(py_type, dt.datetime)
        )
        or (isinstance(dtype, Datetime) and issubclass(py_type, dt.datetime))
        or (dtype is Float64 and issubclass(py_type, float))
        or (dtype is Int64 and issubclass(py_type, int))
        or (dtype is Utf8 and issubclass(py_type, str))
    )


@overload
def select_to_polars_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    iter_batches: Literal[True],
    batch_size: int | None = ...,
    schema_overrides: SchemaDict | None = ...,
    time_zone: timezone = ...,
    snake: bool = ...,
    **kwargs: Any,
) -> Iterator[DataFrame]:
    ...


@overload
def select_to_polars_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    iter_batches: Literal[False] = ...,
    batch_size: int | None = ...,
    schema_overrides: SchemaDict | None = ...,
    time_zone: timezone = ...,
    snake: bool = ...,
    **kwargs: Any,
) -> DataFrame:
    ...


def select_to_polars_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    iter_batches: bool = False,
    batch_size: int | None = None,
    schema_overrides: SchemaDict | None = None,
    time_zone: timezone = UTC,
    snake: bool = False,
    **kwargs: Any,
) -> DataFrame | Iterator[DataFrame]:
    """Read a table from a database into a DataFrame.

    Optionally stream it in chunks.
    """
    check_selectable_for_duplicate_columns(sel)
    if iter_batches:
        return _stream_dataframes(
            sel,
            engine_or_conn,
            batch_size=batch_size,
            schema_overrides=schema_overrides,
            time_zone=time_zone,
            snake=snake,
            **kwargs,
        )
    with yield_connection(engine_or_conn) as conn:
        df = read_database(
            sel,
            cast(ConnectionOrCursor, conn),
            iter_batches=False,
            schema_overrides=schema_overrides,
            **kwargs,
        )
        return _post_process(df, sel, time_zone=time_zone, snake=snake)


def _post_process(
    df: DataFrame,
    sel: Select[Any],
    /,
    *,
    time_zone: timezone = UTC,
    snake: bool = False,
) -> DataFrame:
    """Post-processing for a DataFrame from the database."""
    columns: ReadOnlyColumnCollection = cast(Any, sel).selected_columns
    exprs = (
        _table_column_to_expr(col, time_zone=time_zone).alias(col.name)
        for col in columns
    )
    df = df.with_columns(exprs)
    return _snake_columns(df) if snake else df


def _table_column_to_expr(column: Column[Any], /, *, time_zone: timezone = UTC) -> Expr:
    """Map a table column to a Expr."""
    expr = col(column.name)
    db_type = column.type
    py_type = db_type.python_type
    if issubclass(py_type, bool):
        return expr.cast(pl.Boolean)
    if issubclass(py_type, dt.date) and not issubclass(py_type, dt.datetime):
        return expr.cast(pl.Date)
    if issubclass(py_type, dt.datetime):
        has_tz: bool = cast(Any, db_type).timezone
        sr_dtype = Datetime(time_zone=time_zone) if has_tz else Datetime
        return expr.cast(sr_dtype)
    if issubclass(py_type, float):
        expr_float = expr.cast(Float64)
        return when(expr_float.is_null()).then(nan).otherwise(expr_float)
    if issubclass(py_type, int):
        return expr.cast(Int64)
    if issubclass(py_type, str):
        return expr.cast(Utf8)
    msg = f"{column=}"
    raise ColumnToPolarsExprError(msg)


class ColumnToPolarsExprError(Exception):
    """Raised when an invalid column type is encountered."""


def _snake_columns(df: DataFrame, /) -> DataFrame:
    mapping = {col: snake_case(col) for col in df.columns}
    return df.rename(mapping)


def _stream_dataframes(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    batch_size: int | None = None,
    schema_overrides: SchemaDict | None = None,
    time_zone: timezone = UTC,
    snake: bool = False,
    **kwargs: Any,
) -> Iterator[DataFrame]:
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield from _stream_dataframes(
                sel,
                conn,
                batch_size=batch_size,
                schema_overrides=schema_overrides,
                time_zone=time_zone,
                snake=snake,
                **kwargs,
            )
    else:
        for df in read_database(
            sel,
            cast(ConnectionOrCursor, engine_or_conn),
            iter_batches=True,
            batch_size=batch_size,
            schema_overrides=schema_overrides,
            **kwargs,
        ):
            yield _post_process(df, sel, time_zone=time_zone, snake=snake)


__all__ = [
    "ColumnToPolarsExprError",
    "insert_polars_dataframe",
    "PolarsDataFrameYieldsNoRowsError",
    "select_to_polars_dataframe",
]
