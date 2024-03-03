from __future__ import annotations

import datetime as dt
import decimal
from contextlib import suppress
from datetime import timezone
from itertools import chain
from typing import TYPE_CHECKING, Any, cast, overload
from uuid import UUID

import polars as pl
from polars import (
    Binary,
    DataFrame,
    Date,
    Datetime,
    Duration,
    Float64,
    Int64,
    PolarsDataType,
    Time,
    Utf8,
    concat,
    read_database,
)
from polars.type_aliases import ConnectionOrCursor, SchemaDict
from sqlalchemy import Column, Connection, Engine, Select, Table, select
from sqlalchemy.exc import DuplicateColumnError

from utilities.datetime import UTC
from utilities.errors import redirect_error
from utilities.functions import identity
from utilities.iterables import (
    CheckDuplicatesError,
    OneError,
    check_duplicates,
    chunked,
    one,
)
from utilities.polars import EmptyPolarsConcatError, redirect_empty_polars_concat
from utilities.sqlalchemy import (
    CHUNK_SIZE_FRAC,
    ensure_tables_created,
    get_chunk_size,
    get_columns,
    insert_items,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from sqlalchemy.sql import ColumnCollection
    from sqlalchemy.sql.base import ReadOnlyColumnCollection


def insert_dataframe(
    df: DataFrame,
    table_or_mapped_class: Table | type[Any],
    engine: Engine,
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> None:
    """Insert a DataFrame into a database."""
    mapping = _insert_dataframe_map_df_schema_to_table(
        df.schema, table_or_mapped_class, snake=snake
    )
    items = df.select(mapping).rename(mapping).to_dicts()
    if len(items) == 0:
        if df.height == 0:
            return ensure_tables_created(engine, table_or_mapped_class)
        msg = f"{df=}, {items=}"
        raise InsertDataFrameError(msg)
    return insert_items(
        engine, (items, table_or_mapped_class), chunk_size_frac=chunk_size_frac
    )


class InsertDataFrameError(Exception): ...


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


class _InsertDataFrameMapDFColumnToTableSchemaError(Exception): ...


def _insert_dataframe_map_df_column_to_table_column_and_type(
    df_col_name: str, table_schema: Mapping[str, type], /, *, snake: bool = False
) -> tuple[str, type]:
    """Map a DataFrame column to a table column and type."""
    from utilities.humps import snake_case

    items = table_schema.items()
    func = snake_case if snake else identity
    target = func(df_col_name)
    with redirect_error(
        OneError,
        _InsertDataFrameMapDFColumnToTableColumnAndTypeError(
            f"{df_col_name=}, {table_schema=}, {snake=}"
        ),
    ):
        return one((n, t) for n, t in items if func(n) == target)


class _InsertDataFrameMapDFColumnToTableColumnAndTypeError(Exception): ...


def _insert_dataframe_check_df_and_db_types(
    dtype: PolarsDataType, db_col_type: type, /
) -> bool:
    return (
        (dtype == pl.Boolean and issubclass(db_col_type, bool))
        or (
            dtype == Date
            and issubclass(db_col_type, dt.date)
            and not issubclass(db_col_type, dt.datetime)
        )
        or (dtype == Datetime and issubclass(db_col_type, dt.datetime))
        or (dtype == Float64 and issubclass(db_col_type, float))
        or (dtype == Int64 and issubclass(db_col_type, int))
        or (dtype == Utf8 and issubclass(db_col_type, str))
    )


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine: Engine,
    /,
    *,
    snake: bool = ...,
    time_zone: timezone = ...,
    batch_size: None = ...,
    in_clauses: None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> DataFrame: ...


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine: Engine,
    /,
    *,
    snake: bool = ...,
    time_zone: timezone = ...,
    batch_size: int | None = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> DataFrame: ...


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine: Connection,
    /,
    *,
    snake: bool = ...,
    time_zone: timezone = ...,
    batch_size: int = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> Iterator[DataFrame]: ...


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine: Connection,
    /,
    *,
    snake: bool = ...,
    time_zone: timezone = ...,
    batch_size: int | None = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> Iterator[DataFrame]: ...


def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    snake: bool = False,
    time_zone: timezone = UTC,
    batch_size: int | None = None,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = None,
    in_clauses_chunk_size: int | None = None,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> DataFrame | Iterable[DataFrame]:
    """Read a table from a database into a DataFrame."""

    if snake:
        sel = _select_to_dataframe_apply_snake(sel)
    schema = _select_to_dataframe_map_select_to_df_schema(sel, time_zone=time_zone)
    if (
        isinstance(engine_or_conn, Engine)
        and (batch_size is None)
        and (in_clauses is None)
    ):
        with engine_or_conn.begin() as conn:
            return read_database(
                sel, cast(ConnectionOrCursor, conn), schema_overrides=schema, **kwargs
            )
    if isinstance(engine_or_conn, Engine) and (in_clauses is not None):
        with engine_or_conn.begin() as conn:
            dfs = select_to_dataframe(
                sel,
                conn,
                snake=snake,
                time_zone=time_zone,
                batch_size=batch_size,
                in_clauses=in_clauses,
                in_clauses_chunk_size=in_clauses_chunk_size,
                chunk_size_frac=chunk_size_frac,
                **kwargs,
            )
            try:
                with redirect_empty_polars_concat():
                    return concat(dfs)
            except EmptyPolarsConcatError:
                return DataFrame(schema=schema)
    if (
        isinstance(engine_or_conn, Connection)
        and (batch_size is not None)
        and (in_clauses is None)
    ):
        return read_database(
            sel,
            cast(ConnectionOrCursor, engine_or_conn),
            iter_batches=True,
            batch_size=batch_size,
            schema_overrides=schema,
            **kwargs,
        )
    if (
        isinstance(engine_or_conn, Connection)
        and (batch_size is None)
        and (in_clauses is not None)
    ):
        sels = _select_to_dataframe_yield_selects_with_in_clauses(
            sel,
            engine_or_conn,
            in_clauses,
            in_clauses_chunk_size=in_clauses_chunk_size,
            chunk_size_frac=chunk_size_frac,
        )
        return (
            read_database(
                sel,
                cast(ConnectionOrCursor, engine_or_conn),
                batch_size=batch_size,
                schema_overrides=schema,
                **kwargs,
            )
            for sel in sels
        )
    if (
        isinstance(engine_or_conn, Connection)
        and (batch_size is not None)
        and (in_clauses is not None)
    ):
        sels = _select_to_dataframe_yield_selects_with_in_clauses(
            sel,
            engine_or_conn,
            in_clauses,
            in_clauses_chunk_size=in_clauses_chunk_size,
            chunk_size_frac=chunk_size_frac,
        )
        return chain(
            *(
                read_database(
                    sel,
                    cast(ConnectionOrCursor, engine_or_conn),
                    iter_batches=True,
                    batch_size=batch_size,
                    schema_overrides=schema,
                    **kwargs,
                )
                for sel in sels
            )
        )
    msg = f"{engine_or_conn=}, {batch_size=}, {in_clauses=}"
    raise SelectToDataFrameError(msg)


class SelectToDataFrameError(Exception): ...


def _select_to_dataframe_apply_snake(sel: Select[Any], /) -> Select[Any]:
    """Apply snake-case to a selectable."""

    from utilities.humps import snake_case

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
        col.name: _select_to_dataframe_map_table_column_type_to_dtype(
            col.type, time_zone=time_zone
        )
        for col in columns
    }


def _select_to_dataframe_map_table_column_type_to_dtype(  # noqa: PLR0911
    type_: Any, /, *, time_zone: timezone = UTC
) -> PolarsDataType:
    """Map a table column type to a polars type."""
    type_use = type_() if isinstance(type_, type) else type_
    py_type = type_use.python_type
    if issubclass(py_type, bool):
        return pl.Boolean
    if issubclass(py_type, bytes):
        return Binary
    if issubclass(py_type, decimal.Decimal):
        return pl.Decimal
    if issubclass(py_type, dt.date) and not issubclass(py_type, dt.datetime):
        return pl.Date
    if issubclass(py_type, dt.datetime):
        has_tz: bool = type_use.timezone
        return Datetime(time_zone=time_zone) if has_tz else Datetime()
    if issubclass(py_type, dt.time):
        return Time
    if issubclass(py_type, dt.timedelta):
        return Duration
    if issubclass(py_type, float):
        return Float64
    if issubclass(py_type, int):
        return Int64
    if issubclass(py_type, UUID | str):
        return Utf8
    msg = f"{type_=}, {py_type=}"  # pragma: no cover
    raise _SelectToDataFrameMapTableColumnToDTypeError(msg)  # pragma: no cover


class _SelectToDataFrameMapTableColumnToDTypeError(Exception): ...


def _select_to_dataframe_check_duplicates(
    columns: ColumnCollection[Any, Any], /
) -> None:
    """Check a select for duplicate columns."""
    names = [col.name for col in columns]
    with redirect_error(CheckDuplicatesError, DuplicateColumnError(f"{names=}")):
        check_duplicates(names)


def _select_to_dataframe_yield_selects_with_in_clauses(
    sel: Select[Any],
    conn: Engine | Connection,
    in_clauses: tuple[Column[Any], Iterable[Any]],
    /,
    *,
    in_clauses_chunk_size: int | None = None,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> Iterator[Select[Any]]:
    max_length = len(sel.selected_columns)
    in_col, in_values = in_clauses
    if in_clauses_chunk_size is None:
        chunk_size = get_chunk_size(
            conn, chunk_size_frac=chunk_size_frac, scaling=max_length
        )
    else:
        chunk_size = in_clauses_chunk_size
    return (sel.where(in_col.in_(values)) for values in chunked(in_values, chunk_size))


__all__ = [
    "InsertDataFrameError",
    "SelectToDataFrameError",
    "insert_dataframe",
    "select_to_dataframe",
]
