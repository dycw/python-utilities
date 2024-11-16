from __future__ import annotations

import datetime as dt
import decimal
import reprlib
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, assert_never, cast, overload
from uuid import UUID

import polars as pl
from polars import (
    Binary,
    DataFrame,
    Date,
    Datetime,
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
from sqlalchemy import Column, Select, select
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from typing_extensions import override

from utilities.asyncio import timeout_dur
from utilities.datetime import is_subclass_date_not_datetime
from utilities.functions import identity
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
    TableOrMappedClass,
    ensure_tables_created,
    get_chunk_size,
    get_columns,
    insert_items,
    upsert_items,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterable,
        AsyncIterator,
        Iterable,
        Iterator,
        Mapping,
    )
    from zoneinfo import ZoneInfo

    from polars._typing import PolarsDataType, SchemaDict
    from sqlalchemy.sql import ColumnCollection
    from sqlalchemy.sql.base import ReadOnlyColumnCollection

    import utilities.types


async def insert_dataframe(
    df: DataFrame,
    table_or_mapped_class: TableOrMappedClass,
    engine: AsyncEngine,
    /,
    *,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    upsert: Literal["selected", "all"] | None = None,
    timeout_create: utilities.types.Duration | None = None,
    timeout_insert: utilities.types.Duration | None = None,
) -> None:
    """Insert/upsert a DataFrame into a database."""
    mapping = _insert_dataframe_map_df_schema_to_table(
        df.schema, table_or_mapped_class, snake=snake
    )
    items = df.select(mapping).rename(mapping).to_dicts()
    if len(items) == 0:
        if not df.is_empty():
            raise InsertDataFrameError(df=df)
        if not assume_tables_exist:
            await ensure_tables_created(
                engine, table_or_mapped_class, timeout=timeout_create
            )
        return
    match upsert:
        case None:
            await insert_items(
                engine,
                (items, table_or_mapped_class),
                chunk_size_frac=chunk_size_frac,
                assume_tables_exist=assume_tables_exist,
                timeout_create=timeout_create,
                timeout_insert=timeout_insert,
            )
        case "selected" | "all" as selected_or_all:  # skipif-ci-and-not-linux
            await upsert_items(
                engine,
                (items, table_or_mapped_class),
                chunk_size_frac=chunk_size_frac,
                selected_or_all=selected_or_all,
                assume_tables_exist=assume_tables_exist,
                timeout_create=timeout_create,
                timeout_insert=timeout_insert,
            )
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


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
    table_schema: Mapping[str, type[Any]],
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
    df_col_name: str, table_schema: Mapping[str, type[Any]], /, *, snake: bool = False
) -> tuple[str, type[Any]]:
    """Map a DataFrame column to a table column and type."""
    from utilities.humps import snake_case

    items = table_schema.items()
    func = snake_case if snake else identity
    target = func(df_col_name)
    try:
        return one((n, t) for n, t in items if func(n) == target)
    except OneError:
        raise _InsertDataFrameMapDFColumnToTableColumnAndTypeError(
            df_col_name=df_col_name, table_schema=table_schema, snake=snake
        ) from None


@dataclass(kw_only=True, slots=True)
class _InsertDataFrameMapDFColumnToTableColumnAndTypeError(Exception):
    df_col_name: str
    table_schema: Mapping[str, type[Any]]
    snake: bool

    @override
    def __str__(self) -> str:
        return f"Unable to map DataFrame column {self.df_col_name!r} into table schema {reprlib.repr(self.table_schema)} with snake={self.snake}"


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


@dataclass(kw_only=True, slots=True)
class InsertDataFrameError(Exception):
    df: DataFrame

    @override
    def __str__(self) -> str:
        return f"Non-empty DataFrame must resolve to at least 1 item\n\n{self.df}"


@overload
async def select_to_dataframe(
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
    timeout: utilities.types.Duration | None = ...,
    **kwargs: Any,
) -> DataFrame: ...
@overload
async def select_to_dataframe(
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
    timeout: utilities.types.Duration | None = ...,
    **kwargs: Any,
) -> Iterable[DataFrame]: ...
@overload
async def select_to_dataframe(
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
    timeout: utilities.types.Duration | None = ...,
    **kwargs: Any,
) -> AsyncIterable[DataFrame]: ...
async def select_to_dataframe(
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
    timeout: utilities.types.Duration | None = None,
    **kwargs: Any,
) -> DataFrame | Iterable[DataFrame] | AsyncIterable[DataFrame]:
    """Read a table from a database into a DataFrame."""
    if not issubclass(AsyncEngine, type(engine)):
        # for handling testing
        engine = create_async_engine(engine.url)
        return await select_to_dataframe(
            sel,
            engine,
            snake=snake,
            time_zone=time_zone,
            batch_size=batch_size,
            in_clauses=in_clauses,
            in_clauses_chunk_size=in_clauses_chunk_size,
            chunk_size_frac=chunk_size_frac,
            timeout=timeout,
            **kwargs,
        )
    if snake:
        sel = _select_to_dataframe_apply_snake(sel)
    schema = _select_to_dataframe_map_select_to_df_schema(sel, time_zone=time_zone)
    if in_clauses is None:
        async with timeout_dur(duration=timeout):
            return read_database(
                sel,
                cast(Any, engine),
                iter_batches=batch_size is not None,
                batch_size=batch_size,
                schema_overrides=schema,
                **kwargs,
            )
    sels = _select_to_dataframe_yield_selects_with_in_clauses(
        sel,
        engine,
        in_clauses,
        in_clauses_chunk_size=in_clauses_chunk_size,
        chunk_size_frac=chunk_size_frac,
    )
    if batch_size is None:
        async with timeout_dur(duration=timeout):
            dfs = [
                await select_to_dataframe(
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
            return DataFrame(schema=schema)

    async def yield_dfs() -> AsyncIterator[DataFrame]:
        async with timeout_dur(duration=timeout):
            for sel_i in sels:
                for df in await select_to_dataframe(  # skipif-ci-and-not-linux
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
        return pl.Duration
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
    try:
        check_duplicates(names)
    except CheckDuplicatesError as error:
        msg = f"Columns must not contain duplicates; got {error.counts}"
        raise DuplicateColumnError(msg) from None


def _select_to_dataframe_yield_selects_with_in_clauses(
    sel: Select[Any],
    engine: AsyncEngine,
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
            engine, chunk_size_frac=chunk_size_frac, scaling=max_length
        )
    else:
        chunk_size = in_clauses_chunk_size
    return (sel.where(in_col.in_(values)) for values in chunked(in_values, chunk_size))


__all__ = ["InsertDataFrameError", "insert_dataframe", "select_to_dataframe"]
