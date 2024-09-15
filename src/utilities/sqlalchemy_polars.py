from __future__ import annotations

import datetime as dt
import decimal
from contextlib import suppress
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from uuid import UUID

import polars as pl
from polars import (
    Binary,
    DataFrame,
    Date,
    Datetime,
    Duration,
    Float64,
    Int32,
    Int64,
    Time,
    UInt32,
    UInt64,
    Utf8,
    concat,
    read_database,
)
from polars._typing import ConnectionOrCursor, PolarsDataType, SchemaDict
from sqlalchemy import Column, Select, Table, select
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from typing_extensions import override

from utilities.datetime import is_subclass_date_not_datetime
from utilities.errors import redirect_error
from utilities.functions import ensure_not_none, identity
from utilities.iterables import (
    CheckDuplicatesError,
    OneError,
    check_duplicates,
    chunked,
    one,
)
from utilities.polars import zoned_datetime
from utilities.sqlalchemy import (
    CHUNK_SIZE_FRAC,
    EngineOrConnection,
    MaybeAsyncEngineOrConnection,
    TableOrMappedClass,
    ensure_tables_created,
    ensure_tables_created_async,
    get_chunk_size,
    get_columns,
    insert_items,
    insert_items_async,
    upsert_items,
    upsert_items_async,
    yield_connection,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterable,
        AsyncIterator,
        Iterable,
        Iterator,
        Mapping,
        Sequence,
    )
    from zoneinfo import ZoneInfo

    from sqlalchemy.sql import ColumnCollection
    from sqlalchemy.sql.base import ReadOnlyColumnCollection

    from utilities.types import StrMapping


def insert_dataframe(
    df: DataFrame,
    table_or_mapped_class: TableOrMappedClass,
    engine_or_conn: EngineOrConnection,
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
) -> None:
    """Insert a DataFrame into a database."""
    prepared = _insert_dataframe_prepare(df, table_or_mapped_class, snake=snake)
    if prepared.no_items_empty_df:
        if not assume_tables_exist:
            ensure_tables_created(engine_or_conn, table_or_mapped_class)
        return
    if prepared.no_items_non_empty_df:
        raise InsertDataFrameError(df=df)
    insert_items(
        engine_or_conn,
        prepared.items,
        chunk_size_frac=chunk_size_frac,
        assume_tables_exist=assume_tables_exist,
    )


@dataclass(kw_only=True)
class InsertDataFrameError(Exception):
    df: DataFrame

    @override
    def __str__(self) -> str:
        return f"Non-empty DataFrame must resolve to at least 1 item\n\n{self.df}"


async def insert_dataframe_async(
    df: DataFrame,
    table_or_mapped_class: TableOrMappedClass,
    engine: AsyncEngine,  # does not support connection
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
) -> None:
    """Insert a DataFrame into a database."""
    prepared = _insert_dataframe_prepare(df, table_or_mapped_class, snake=snake)
    if prepared.no_items_empty_df:
        if not assume_tables_exist:
            await ensure_tables_created_async(engine, table_or_mapped_class)
        return
    if prepared.no_items_non_empty_df:
        raise InsertDataFrameAsyncError(df=df)
    await insert_items_async(
        engine,
        prepared.items,
        chunk_size_frac=chunk_size_frac,
        assume_tables_exist=assume_tables_exist,
    )


@dataclass(kw_only=True)
class InsertDataFrameAsyncError(Exception):
    df: DataFrame

    @override
    def __str__(self) -> str:
        return f"Non-empty DataFrame must resolve to at least 1 item\n\n{self.df}"


@dataclass(frozen=True, kw_only=True)
class _InsertDataFramePrepare:
    items: tuple[Sequence[StrMapping], TableOrMappedClass]
    no_items_empty_df: bool
    no_items_non_empty_df: bool


def _insert_dataframe_prepare(
    df: DataFrame, table_or_mapped_class: Table | type[Any], /, *, snake: bool = False
) -> _InsertDataFramePrepare:
    """Prepare the arguments for `insert_dataframe`."""
    mapping = _insert_dataframe_map_df_schema_to_table(
        df.schema, table_or_mapped_class, snake=snake
    )
    items = df.select(mapping).rename(mapping).to_dicts()
    no_items = len(items) == 0
    df_is_empty = df.is_empty()
    return _InsertDataFramePrepare(
        items=(items, table_or_mapped_class),
        no_items_empty_df=no_items and df_is_empty,
        no_items_non_empty_df=no_items and not df_is_empty,
    )


def _insert_dataframe_map_df_schema_to_table(
    df_schema: SchemaDict,
    table_or_mapped_class: TableOrMappedClass,
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
        or (dtype == Date and is_subclass_date_not_datetime(db_col_type))
        or (dtype == Datetime and issubclass(db_col_type, dt.datetime))
        or (dtype == Float64 and issubclass(db_col_type, float))
        or (dtype in {Int32, Int64, UInt32, UInt64} and issubclass(db_col_type, int))
        or (dtype == Utf8 and issubclass(db_col_type, str))
    )


@overload
def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: EngineOrConnection,
    /,
    *,
    snake: bool = ...,
    time_zone: ZoneInfo | str = ...,
    batch_size: None = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = ...,
    **kwargs: Any,
) -> DataFrame: ...
@overload
def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: EngineOrConnection,
    /,
    *,
    snake: bool = ...,
    time_zone: ZoneInfo | str = ...,
    batch_size: int = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = ...,
    **kwargs: Any,
) -> Iterable[DataFrame]: ...
def select_to_dataframe(
    sel: Select[Any],
    engine_or_conn: EngineOrConnection,
    /,
    *,
    snake: bool = False,
    time_zone: ZoneInfo | str = UTC,
    batch_size: int | None = None,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = None,
    in_clauses_chunk_size: int | None = None,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> DataFrame | Iterable[DataFrame]:
    """Read a table from a database into a DataFrame."""
    prepared = _select_to_dataframe_prepare(
        sel,
        engine_or_conn,
        snake=snake,
        time_zone=time_zone,
        in_clauses=in_clauses,
        in_clauses_chunk_size=in_clauses_chunk_size,
        chunk_size_frac=chunk_size_frac,
    )
    if in_clauses is None:
        return read_database(
            prepared.sel,
            cast(ConnectionOrCursor, engine_or_conn),
            iter_batches=batch_size is not None,
            batch_size=batch_size,
            schema_overrides=prepared.schema,
            **kwargs,
        )
    sels = ensure_not_none(prepared.sels)
    if batch_size is None:
        with yield_connection(engine_or_conn) as conn:
            dfs = (
                select_to_dataframe(
                    sel,
                    conn,
                    snake=snake,
                    time_zone=time_zone,
                    batch_size=None,
                    in_clauses=None,
                    **kwargs,
                )
                for sel in sels
            )
            try:
                return concat(dfs)
            except ValueError:
                return DataFrame(schema=prepared.schema)
    dfs = (
        select_to_dataframe(
            sel,
            engine_or_conn,
            snake=snake,
            time_zone=time_zone,
            batch_size=batch_size,
            in_clauses=None,
            chunk_size_frac=chunk_size_frac,
            **kwargs,
        )
        for sel in sels
    )
    return chain(*dfs)


@overload
async def select_to_dataframe_async(
    sel: Select[Any],
    engine: AsyncEngine,
    /,
    *,
    snake: bool = ...,
    time_zone: ZoneInfo | str = ...,
    batch_size: None = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = ...,
    **kwargs: Any,
) -> DataFrame: ...
@overload
async def select_to_dataframe_async(
    sel: Select[Any],
    engine: AsyncEngine,
    /,
    *,
    snake: bool = ...,
    time_zone: ZoneInfo | str = ...,
    batch_size: int = ...,
    in_clauses: None = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = ...,
    **kwargs: Any,
) -> Iterable[DataFrame]: ...
@overload
async def select_to_dataframe_async(
    sel: Select[Any],
    engine: AsyncEngine,
    /,
    *,
    snake: bool = ...,
    time_zone: ZoneInfo | str = ...,
    batch_size: int = ...,
    in_clauses: tuple[Column[Any], Iterable[Any]] = ...,
    in_clauses_chunk_size: int | None = ...,
    chunk_size_frac: float = ...,
    **kwargs: Any,
) -> AsyncIterable[DataFrame]: ...
async def select_to_dataframe_async(
    sel: Select[Any],
    engine: AsyncEngine,
    /,
    *,
    snake: bool = False,
    time_zone: ZoneInfo | str = UTC,
    batch_size: int | None = None,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = None,
    in_clauses_chunk_size: int | None = None,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    **kwargs: Any,
) -> DataFrame | Iterable[DataFrame] | AsyncIterable[DataFrame]:
    """Read a table from a database into a DataFrame."""
    if not issubclass(AsyncEngine, type(engine)):
        # for handling testing
        engine = create_async_engine(engine.url)
        return await select_to_dataframe_async(
            sel,
            engine,
            snake=snake,
            time_zone=time_zone,
            batch_size=batch_size,
            in_clauses=in_clauses,
            in_clauses_chunk_size=in_clauses_chunk_size,
            chunk_size_frac=chunk_size_frac,
            **kwargs,
        )
    prepared = _select_to_dataframe_prepare(
        sel,
        engine,
        snake=snake,
        time_zone=time_zone,
        in_clauses=in_clauses,
        in_clauses_chunk_size=in_clauses_chunk_size,
        chunk_size_frac=chunk_size_frac,
    )
    if in_clauses is None:
        return read_database(
            prepared.sel,
            cast(Any, engine),
            iter_batches=batch_size is not None,
            batch_size=batch_size,
            schema_overrides=prepared.schema,
            **kwargs,
        )
    sels = ensure_not_none(prepared.sels)
    if batch_size is None:
        dfs = [
            await select_to_dataframe_async(
                sel,
                engine,
                snake=snake,
                time_zone=time_zone,
                batch_size=None,
                in_clauses=None,
                **kwargs,
            )
            for sel in sels
        ]
        try:
            return concat(dfs)
        except ValueError:
            return DataFrame(schema=prepared.schema)

    async def yield_dfs() -> AsyncIterator[DataFrame]:
        for sel_i in sels:
            for df in await select_to_dataframe_async(
                sel_i,
                engine,
                snake=snake,
                time_zone=time_zone,
                batch_size=batch_size,
                in_clauses=None,
                chunk_size_frac=chunk_size_frac,
                **kwargs,
            ):
                yield df

    return yield_dfs()


@dataclass(frozen=True, kw_only=True)
class _SelectToDataFramePrepare:
    sel: Select[Any]
    schema: SchemaDict
    sels: Iterable[Select[Any]] | None = None


def _select_to_dataframe_prepare(
    sel: Select[Any],
    engine_or_conn: EngineOrConnection | AsyncEngine,
    /,
    *,
    snake: bool = False,
    time_zone: ZoneInfo | str = UTC,
    in_clauses: tuple[Column[Any], Iterable[Any]] | None = None,
    in_clauses_chunk_size: int | None = None,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> _SelectToDataFramePrepare:
    if snake:
        sel = _select_to_dataframe_apply_snake(sel)
    schema = _select_to_dataframe_map_select_to_df_schema(sel, time_zone=time_zone)
    if in_clauses is None:
        sels = None
    else:
        sels = _select_to_dataframe_yield_selects_with_in_clauses(
            sel,
            engine_or_conn,
            in_clauses,
            in_clauses_chunk_size=in_clauses_chunk_size,
            chunk_size_frac=chunk_size_frac,
        )
    return _SelectToDataFramePrepare(sel=sel, schema=schema, sels=sels)


def _select_to_dataframe_apply_snake(sel: Select[Any], /) -> Select[Any]:
    """Apply snake-case to a selectable."""
    from utilities.humps import snake_case

    alias = sel.alias()
    columns = [alias.c[c.name].label(snake_case(c.name)) for c in sel.selected_columns]
    return select(*columns)


def _select_to_dataframe_map_select_to_df_schema(
    sel: Select[Any], /, *, time_zone: ZoneInfo | str = UTC
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


def _select_to_dataframe_map_table_column_type_to_dtype(
    type_: Any, /, *, time_zone: ZoneInfo | str = UTC
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
        return zoned_datetime(time_zone=time_zone) if has_tz else Datetime()
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
    engine_or_conn: MaybeAsyncEngineOrConnection,
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
            engine_or_conn, chunk_size_frac=chunk_size_frac, scaling=max_length
        )
    else:
        chunk_size = in_clauses_chunk_size
    return (sel.where(in_col.in_(values)) for values in chunked(in_values, chunk_size))


def upsert_dataframe(
    df: DataFrame,
    table_or_mapped_class: TableOrMappedClass,
    engine_or_conn: EngineOrConnection,
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> None:
    """Upsert a DataFrame into a database."""
    prepared = _insert_dataframe_prepare(df, table_or_mapped_class, snake=snake)
    if prepared.no_items_empty_df:
        if not assume_tables_exist:
            ensure_tables_created(engine_or_conn, table_or_mapped_class)
        return
    if prepared.no_items_non_empty_df:
        raise UpsertDataFrameError(df=df)
    upsert_items(
        engine_or_conn,
        prepared.items,
        chunk_size_frac=chunk_size_frac,
        assume_tables_exist=assume_tables_exist,
        selected_or_all=selected_or_all,
    )


@dataclass(kw_only=True)
class UpsertDataFrameError(Exception):
    df: DataFrame

    @override
    def __str__(self) -> str:
        return f"Non-empty DataFrame must resolve to at least 1 item\n\n{self.df}"


async def upsert_dataframe_async(
    df: DataFrame,
    table_or_mapped_class: TableOrMappedClass,
    engine: AsyncEngine,  # does not support connection
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> None:
    """Upsert a DataFrame into a database."""
    prepared = _insert_dataframe_prepare(df, table_or_mapped_class, snake=snake)
    if prepared.no_items_empty_df:
        if not assume_tables_exist:
            await ensure_tables_created_async(engine, table_or_mapped_class)
        return
    if prepared.no_items_non_empty_df:
        raise UpsertDataFrameAsyncError(df=df)
    await upsert_items_async(
        engine,
        prepared.items,
        chunk_size_frac=chunk_size_frac,
        assume_tables_exist=assume_tables_exist,
        selected_or_all=selected_or_all,
    )


@dataclass(kw_only=True)
class UpsertDataFrameAsyncError(Exception):
    df: DataFrame

    @override
    def __str__(self) -> str:
        return f"Non-empty DataFrame must resolve to at least 1 item\n\n{self.df}"


__all__ = [
    "InsertDataFrameAsyncError",
    "InsertDataFrameError",
    "UpsertDataFrameAsyncError",
    "UpsertDataFrameError",
    "insert_dataframe",
    "insert_dataframe_async",
    "select_to_dataframe",
    "select_to_dataframe_async",
    "upsert_dataframe",
    "upsert_dataframe_async",
]
