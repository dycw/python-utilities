from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Iterator, Mapping
from contextlib import suppress
from datetime import timezone
from typing import Any, Literal, cast, overload

import polars as pl
from polars import (
    DataFrame,
    Date,
    Datetime,
    Float64,
    Int64,
    PolarsDataType,
    Utf8,
    read_database,
)
from polars.type_aliases import ConnectionOrCursor, SchemaDict
from sqlalchemy import Column, Connection, Engine, Select, Table, select
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from utilities._sqlalchemy.common import (
    INSERT_ITEMS_CHUNK_SIZE_FRAC,
    get_columns,
    insert_items,
    yield_connection,
)
from utilities.datetime import UTC
from utilities.errors import redirect_context
from utilities.functions import identity
from utilities.humps import snake_case
from utilities.iterables import CheckDuplicatesError, check_duplicates
from utilities.more_itertools import OneError, one


def insert_dataframe(
    engine_or_conn: Engine | Connection,
    df: DataFrame,
    table_or_mapped_class: Table | type[Any],
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = INSERT_ITEMS_CHUNK_SIZE_FRAC,
) -> None:
    """Insert a DataFrame into a database."""
    mapping = _insert_dataframe_map_df_schema_to_table(
        df.schema, table_or_mapped_class, snake=snake
    )
    items = df.select(mapping).rename(mapping).to_dicts()
    if (df.height > 0) and (len(items) == 0):
        msg = f"{df=}, {items=}"
        raise InsertDataFrameError(msg)
    return insert_items(
        engine_or_conn, (items, table_or_mapped_class), chunk_size_frac=chunk_size_frac
    )


class InsertDataFrameError(Exception):
    ...


def _insert_dataframe_map_df_schema_to_table(
    df_schema: SchemaDict,
    table_or_mapped_class: Table | type[Any],
    /,
    *,
    snake: bool = False,
) -> dict[str, str]:
    """Map a DataFrame schema to a table."""
    table_schema = {
        col.name: col.type.python_type for col in get_columns(table_or_mapped_class)
    }
    out: dict[str, str] = {}
    for df_col_name, df_col_type in df_schema.items():
        with suppress(_InsertDataFrameMapDFColumnToTableColumnAndTypeError):
            out[df_col_name] = _insert_dataframe_map_df_column_to_table_schema(
                df_col_name, df_col_type, table_schema, snake=snake
            )
    return out


def _insert_dataframe_map_df_column_to_table_schema(
    df_col_name: str,
    df_col_type: PolarsDataType,
    table_schema: Mapping[str, type],
    /,
    *,
    snake: bool = False,
) -> str:
    """Map a DataFrame column to a table schema."""
    db_col_name, db_col_type = _insert_dataframe_map_df_column_to_table_column_and_type(
        df_col_name, table_schema, snake=snake
    )
    if not _insert_dataframe_check_df_and_db_types(df_col_type, db_col_type):
        msg = f"{df_col_type=}, {db_col_type=}"
        raise _InsertDataFrameMapDFColumnToTableSchemaError(msg)
    return db_col_name


class _InsertDataFrameMapDFColumnToTableSchemaError(Exception):
    ...


def _insert_dataframe_map_df_column_to_table_column_and_type(
    df_col_name: str, table_schema: Mapping[str, type], /, *, snake: bool = False
) -> tuple[str, type]:
    """Map a DataFrame column to a table column and type."""
    items = table_schema.items()
    func = snake_case if snake else identity
    target = func(df_col_name)
    with redirect_context(
        OneError,
        _InsertDataFrameMapDFColumnToTableColumnAndTypeError(
            f"{df_col_name=}, {table_schema=}, {snake=}"
        ),
    ):
        return one((n, t) for n, t in items if func(n) == target)


class _InsertDataFrameMapDFColumnToTableColumnAndTypeError(Exception):
    ...


def _insert_dataframe_check_df_and_db_types(
    dtype: PolarsDataType, db_col_type: type, /
) -> bool:
    return (
        (dtype is pl.Boolean and issubclass(db_col_type, bool))
        or (
            dtype is Date
            and issubclass(db_col_type, dt.date)
            and not issubclass(db_col_type, dt.datetime)
        )
        or (isinstance(dtype, Datetime) and issubclass(db_col_type, dt.datetime))
        or (dtype is Float64 and issubclass(db_col_type, float))
        or (dtype is Int64 and issubclass(db_col_type, int))
        or (dtype is Utf8 and issubclass(db_col_type, str))
    )


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    snake: bool = ...,
    time_zone: timezone = ...,
    iter_batches: Literal[True],
    batch_size: int | None = ...,
    schema_overrides: SchemaDict | None = ...,
    **kwargs: Any,
) -> Iterator[DataFrame]:
    ...


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    snake: bool = ...,
    time_zone: timezone = ...,
    iter_batches: Literal[False] = ...,
    batch_size: int | None = ...,
    schema_overrides: SchemaDict | None = ...,
    **kwargs: Any,
) -> DataFrame:
    ...


def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    snake: bool = False,
    time_zone: timezone = UTC,
    iter_batches: bool = False,
    batch_size: int | None = None,
    **kwargs: Any,
) -> DataFrame | Iterable[DataFrame]:
    """Read a table from a database into a DataFrame."""

    if snake:
        sel = _select_to_dataframe_apply_snake(sel)
    schema = _select_to_dataframe_map_select_to_df_schema(sel, time_zone=time_zone)
    if iter_batches:
        with yield_connection(engine_or_conn) as conn:
            return read_database(
                sel,
                cast(ConnectionOrCursor, conn),
                iter_batches=True,
                batch_size=batch_size,
                schema_overrides=schema,
                **kwargs,
            )
    with yield_connection(engine_or_conn) as conn:
        return read_database(
            sel, cast(ConnectionOrCursor, conn), schema_overrides=schema, **kwargs
        )


def _select_to_dataframe_apply_snake(sel: Select[Any], /) -> Select[Any]:
    """Apply snake-case to a selectable."""

    alias = sel.alias()
    columns = [alias.c[c.name].label(snake_case(c.name)) for c in sel.selected_columns]
    return select(*columns)


def _select_to_dataframe_map_select_to_df_schema(
    sel: Select[Any], /, *, time_zone: timezone = UTC
) -> SchemaDict:
    """Map a select to a DataFrame schema."""
    columns: ReadOnlyColumnCollection = cast(Any, sel).selected_columns
    _select_to_dataframe_check_duplicates(columns)
    return {
        col.name: _select_to_dataframe_map_table_column_to_dtype(
            col, time_zone=time_zone
        )
        for col in columns
    }


def _select_to_dataframe_map_table_column_to_dtype(
    column: Column[Any], /, *, time_zone: timezone = UTC
) -> PolarsDataType:
    """Map a table column to a polars type."""
    db_type = column.type
    py_type = db_type.python_type
    if issubclass(py_type, bool):
        return pl.Boolean
    if issubclass(py_type, dt.date) and not issubclass(py_type, dt.datetime):
        return pl.Date
    if issubclass(py_type, dt.datetime):
        has_tz: bool = cast(Any, db_type).timezone
        return Datetime(time_zone=time_zone) if has_tz else Datetime
    if issubclass(py_type, float):
        return Float64
    if issubclass(py_type, int):
        return Int64
    if issubclass(py_type, str):
        return Utf8
    msg = f"{column=}"
    raise _SelectToDataFrameMapTableColumnToDTypeError(msg)


class _SelectToDataFrameMapTableColumnToDTypeError(Exception):
    ...


def _select_to_dataframe_check_duplicates(columns: ReadOnlyColumnCollection, /) -> None:
    """Check a select for duplicate columns."""
    names = [col.name for col in columns]
    with redirect_context(CheckDuplicatesError, DuplicateColumnError(f"{names=}")):
        check_duplicates(names)


__all__ = ["InsertDataFrameError", "insert_dataframe", "select_to_dataframe"]
