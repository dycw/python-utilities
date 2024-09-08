from __future__ import annotations

import enum
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Sequence
from collections.abc import Set as AbstractSet
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from enum import auto
from functools import reduce
from math import floor
from operator import ge, itemgetter, le, or_
from re import search
from typing import TYPE_CHECKING, Any, Literal, TypeGuard, assert_never, cast, overload

import sqlalchemy
from sqlalchemy import (
    URL,
    Boolean,
    Column,
    Connection,
    DateTime,
    Engine,
    Float,
    Insert,
    Interval,
    LargeBinary,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    Unicode,
    UnicodeText,
    Uuid,
    and_,
    case,
    insert,
    quoted_name,
    text,
)
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.dialects.mssql import dialect as mssql_dialect
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy.dialects.postgresql import Insert as postgresql_Insert
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.sqlite import Insert as sqlite_Insert
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import ArgumentError, DatabaseError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    InstrumentedAttribute,
    class_mapper,
    declared_attr,
)
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.pool import NullPool, Pool
from sqlalchemy.sql.functions import now
from sqlalchemy.sql.schema import ColumnElementColumnDefault
from typing_extensions import override

from utilities.datetime import get_now
from utilities.errors import redirect_error
from utilities.functions import get_class_name
from utilities.iterables import (
    CheckLengthError,
    MaybeIterable,
    OneEmptyError,
    always_iterable,
    check_length,
    chunked,
    is_iterable_not_str,
    one,
)
from utilities.text import ensure_str
from utilities.types import (
    StrMapping,
    TupleOrStrMapping,
    is_string_mapping,
    is_tuple_or_string_mapping,
)

if TYPE_CHECKING:
    from sqlalchemy.sql.base import ReadOnlyColumnCollection


EngineOrConnection = Engine | Connection
AsyncEngineOrConnection = AsyncEngine | AsyncConnection
MaybeAsyncEngineOrConnection = EngineOrConnection | AsyncEngineOrConnection
TableOrMappedClass = Table | type[DeclarativeBase]
CHUNK_SIZE_FRAC = 0.95


def _check_column_collections_equal(
    x: ReadOnlyColumnCollection[Any, Any],
    y: ReadOnlyColumnCollection[Any, Any],
    /,
    *,
    snake: bool = False,
    allow_permutations: bool = False,
    primary_key: bool = True,
) -> None:
    """Check that a pair of column collections are equal."""
    from utilities.humps import snake_case_mappings

    cols_x, cols_y = (list(cast(Iterable[Column[Any]], i)) for i in [x, y])
    name_to_col_x, name_to_col_y = (
        {ensure_str(col.name): col for col in i} for i in [cols_x, cols_y]
    )
    if len(name_to_col_x) != len(name_to_col_y):
        msg = f"{x=}, {y=}"
        raise _CheckColumnCollectionsEqualError(msg)
    if snake:
        name_to_snake_x, name_to_snake_y = (
            snake_case_mappings(i) for i in [name_to_col_x, name_to_col_y]
        )
        snake_to_name_x, snake_to_name_y = (
            {v: k for k, v in nts.items()} for nts in [name_to_snake_x, name_to_snake_y]
        )
        key_to_col_x, key_to_col_y = (
            {key: name_to_col[snake_to_name[key]] for key in snake_to_name}
            for name_to_col, snake_to_name in [
                (name_to_col_x, snake_to_name_x),
                (name_to_col_y, snake_to_name_y),
            ]
        )
    else:
        key_to_col_x, key_to_col_y = name_to_col_x, name_to_col_y
    if allow_permutations:
        cols_to_check_x, cols_to_check_y = (
            map(itemgetter(1), sorted(key_to_col.items(), key=itemgetter(0)))
            for key_to_col in [key_to_col_x, key_to_col_y]
        )
    else:
        cols_to_check_x, cols_to_check_y = (
            i.values() for i in [key_to_col_x, key_to_col_y]
        )
    diff = set(key_to_col_x).symmetric_difference(set(key_to_col_y))
    if len(diff) >= 1:
        msg = f"{x=}, {y=}"
        raise _CheckColumnCollectionsEqualError(msg)
    for x_i, y_i in zip(cols_to_check_x, cols_to_check_y, strict=True):
        _check_columns_equal(x_i, y_i, snake=snake, primary_key=primary_key)


class _CheckColumnCollectionsEqualError(Exception): ...


def _check_columns_equal(
    x: Column[Any], y: Column[Any], /, *, snake: bool = False, primary_key: bool = True
) -> None:
    """Check that a pair of columns are equal."""
    _check_table_or_column_names_equal(x.name, y.name, snake=snake)
    _check_column_types_equal(x.type, y.type)
    if primary_key and (x.primary_key != y.primary_key):
        msg = f"{x.primary_key=}, {y.primary_key=}"
        raise _CheckColumnsEqualError(msg)
    if x.nullable != y.nullable:
        msg = f"{x.nullable=}, {y.nullable=}"
        raise _CheckColumnsEqualError(msg)


class _CheckColumnsEqualError(Exception): ...


def _check_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of column types are equal."""
    x_inst, y_inst = (i() if isinstance(i, type) else i for i in [x, y])
    x_cls, y_cls = (i._type_affinity for i in [x_inst, y_inst])  # noqa: SLF001
    msg = f"{x=}, {y=}"
    if not (isinstance(x_inst, y_cls) and isinstance(y_inst, x_cls)):
        raise _CheckColumnTypesEqualError(msg)
    if isinstance(x_inst, Boolean) and isinstance(y_inst, Boolean):
        _check_column_types_boolean_equal(x_inst, y_inst)
    if isinstance(x_inst, DateTime) and isinstance(y_inst, DateTime):
        _check_column_types_datetime_equal(x_inst, y_inst)
    if isinstance(x_inst, sqlalchemy.Enum) and isinstance(y_inst, sqlalchemy.Enum):
        _check_column_types_enum_equal(x_inst, y_inst)
    if isinstance(x_inst, Float) and isinstance(y_inst, Float):
        _check_column_types_float_equal(x_inst, y_inst)
    if isinstance(x_inst, Interval) and isinstance(y_inst, Interval):
        _check_column_types_interval_equal(x_inst, y_inst)
    if isinstance(x_inst, LargeBinary) and isinstance(y_inst, LargeBinary):
        _check_column_types_large_binary_equal(x_inst, y_inst)
    if isinstance(x_inst, Numeric) and isinstance(y_inst, Numeric):
        _check_column_types_numeric_equal(x_inst, y_inst)
    if isinstance(x_inst, String | Unicode | UnicodeText) and isinstance(
        y_inst, String | Unicode | UnicodeText
    ):
        _check_column_types_string_equal(x_inst, y_inst)
    if isinstance(x_inst, Uuid) and isinstance(y_inst, Uuid):
        _check_column_types_uuid_equal(x_inst, y_inst)


class _CheckColumnTypesEqualError(Exception): ...


def _check_column_types_boolean_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of boolean column types are equal."""
    msg = f"{x=}, {y=}"
    if x.create_constraint is not y.create_constraint:
        raise _CheckColumnTypesBooleanEqualError(msg)
    if x.name != y.name:
        raise _CheckColumnTypesBooleanEqualError(msg)


class _CheckColumnTypesBooleanEqualError(Exception): ...


def _check_column_types_datetime_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of datetime column types are equal."""
    if x.timezone is not y.timezone:
        msg = f"{x=}, {y=}"
        raise _CheckColumnTypesDateTimeEqualError(msg)


class _CheckColumnTypesDateTimeEqualError(Exception): ...


def _check_column_types_enum_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of enum column types are equal."""
    x_enum, y_enum = (i.enum_class for i in [x, y])
    if (x_enum is None) and (y_enum is None):
        return
    msg = f"{x=}, {y=}"
    if ((x_enum is None) and (y_enum is not None)) or (
        (x_enum is not None) and (y_enum is None)
    ):
        raise _CheckColumnTypesEnumEqualError(msg)
    if not (issubclass(x_enum, y_enum) and issubclass(y_enum, x_enum)):
        raise _CheckColumnTypesEnumEqualError(msg)
    if x.create_constraint is not y.create_constraint:
        raise _CheckColumnTypesEnumEqualError(msg)
    if x.native_enum is not y.native_enum:
        raise _CheckColumnTypesEnumEqualError(msg)
    if x.length != y.length:
        raise _CheckColumnTypesEnumEqualError(msg)
    if x.inherit_schema is not y.inherit_schema:
        raise _CheckColumnTypesEnumEqualError(msg)


class _CheckColumnTypesEnumEqualError(Exception): ...


def _check_column_types_float_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of float column types are equal."""
    msg = f"{x=}, {y=}"
    if x.precision != y.precision:
        raise _CheckColumnTypesFloatEqualError(msg)
    if x.asdecimal is not y.asdecimal:
        raise _CheckColumnTypesFloatEqualError(msg)
    if x.decimal_return_scale != y.decimal_return_scale:
        raise _CheckColumnTypesFloatEqualError(msg)


class _CheckColumnTypesFloatEqualError(Exception): ...


def _check_column_types_interval_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of interval column types are equal."""
    msg = f"{x=}, {y=}"
    if x.native is not y.native:
        raise _CheckColumnTypesIntervalEqualError(msg)
    if x.second_precision != y.second_precision:
        raise _CheckColumnTypesIntervalEqualError(msg)
    if x.day_precision != y.day_precision:
        raise _CheckColumnTypesIntervalEqualError(msg)


class _CheckColumnTypesIntervalEqualError(Exception): ...


def _check_column_types_large_binary_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of large binary column types are equal."""
    if x.length != y.length:
        msg = f"{x=}, {y=}"
        raise _CheckColumnTypesLargeBinaryEqualError(msg)


class _CheckColumnTypesLargeBinaryEqualError(Exception): ...


def _check_column_types_numeric_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of numeric column types are equal."""
    msg = f"{x=}, {y=}"
    if x.precision != y.precision:
        raise _CheckColumnTypesNumericEqualError(msg)
    if x.scale != y.scale:
        raise _CheckColumnTypesNumericEqualError(msg)
    if x.asdecimal != y.asdecimal:
        raise _CheckColumnTypesNumericEqualError(msg)
    if x.decimal_return_scale != y.decimal_return_scale:
        raise _CheckColumnTypesNumericEqualError(msg)


class _CheckColumnTypesNumericEqualError(Exception): ...


def _check_column_types_string_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of string column types are equal."""
    msg = f"{x=}, {y=}"
    if x.length != y.length:
        raise _CheckColumnTypesStringEqualError(msg)
    if x.collation != y.collation:
        raise _CheckColumnTypesStringEqualError(msg)


class _CheckColumnTypesStringEqualError(Exception): ...


def _check_column_types_uuid_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of UUID column types are equal."""
    msg = f"{x=}, {y=}"
    if x.as_uuid is not y.as_uuid:
        raise _CheckColumnTypesUuidEqualError(msg)
    if x.native_uuid is not y.native_uuid:
        raise _CheckColumnTypesUuidEqualError(msg)


class _CheckColumnTypesUuidEqualError(Exception): ...


def check_engine(
    engine_or_conn: EngineOrConnection,
    /,
    *,
    num_tables: int | tuple[int, float] | None = None,
) -> None:
    """Check that an engine can connect.

    Optionally query for the number of tables, or the number of columns in
    such a table.
    """
    match get_dialect(engine_or_conn):
        case (  # skipif-ci-and-not-linux
            Dialect.mssql
            | Dialect.mysql
            | Dialect.postgresql
        ):
            query = "select * from information_schema.tables"
        case Dialect.oracle:  # pragma: no cover
            query = "select * from all_objects"
        case Dialect.sqlite:
            query = "select * from sqlite_master where type='table'"
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    statement = text(query)
    with yield_connection(engine_or_conn) as conn:
        rows = conn.execute(statement).all()
    if num_tables is not None:
        with redirect_error(
            CheckLengthError, CheckEngineError(f"{engine_or_conn=}, {num_tables=}")
        ):
            check_length(rows, equal_or_approx=num_tables)


class CheckEngineError(Exception): ...


def check_table_against_reflection(
    table_or_mapped_class: TableOrMappedClass,
    engine_or_conn: EngineOrConnection,
    /,
    *,
    schema: str | None = None,
    snake_table: bool = False,
    snake_columns: bool = False,
    allow_permutations_columns: bool = False,
    primary_key: bool = True,
) -> None:
    """Check that a table equals its reflection."""
    reflected = reflect_table(table_or_mapped_class, engine_or_conn, schema=schema)
    _check_tables_equal(
        reflected,
        table_or_mapped_class,
        snake_table=snake_table,
        allow_permutations_columns=allow_permutations_columns,
        snake_columns=snake_columns,
        primary_key=primary_key,
    )


def _check_tables_equal(
    x: Any,
    y: Any,
    /,
    *,
    snake_table: bool = False,
    snake_columns: bool = False,
    allow_permutations_columns: bool = False,
    primary_key: bool = True,
) -> None:
    """Check that a pair of tables are equal."""
    x_t, y_t = map(get_table, [x, y])
    _check_table_or_column_names_equal(x_t.name, y_t.name, snake=snake_table)
    _check_column_collections_equal(
        x_t.columns,
        y_t.columns,
        snake=snake_columns,
        allow_permutations=allow_permutations_columns,
        primary_key=primary_key,
    )


def _check_table_or_column_names_equal(
    x: str | quoted_name, y: str | quoted_name, /, *, snake: bool = False
) -> None:
    """Check that a pair of table/columns' names are equal."""
    from utilities.humps import snake_case

    x, y = (str(i) if isinstance(i, quoted_name) else i for i in [x, y])
    msg = f"{x=}, {y=}"
    if (not snake) and (x != y):
        raise _CheckTableOrColumnNamesEqualError(msg)
    if snake and (snake_case(x) != snake_case(y)):
        raise _CheckTableOrColumnNamesEqualError(msg)


class _CheckTableOrColumnNamesEqualError(Exception): ...


def columnwise_max(*columns: Any) -> Any:
    """Compute the columnwise max of a number of columns."""
    return _columnwise_minmax(*columns, op=ge)


def columnwise_min(*columns: Any) -> Any:
    """Compute the columnwise min of a number of columns."""
    return _columnwise_minmax(*columns, op=le)


def _columnwise_minmax(*columns: Any, op: Callable[[Any, Any], Any]) -> Any:
    """Compute the columnwise min of a number of columns."""

    def func(x: Any, y: Any, /) -> Any:
        x_none = x.is_(None)
        y_none = y.is_(None)
        col = case(
            (and_(x_none, y_none), None),
            (and_(~x_none, y_none), x),
            (and_(x_none, ~y_none), y),
            (op(x, y), x),
            else_=y,
        )
        # try auto-label
        names = {
            value for col in [x, y] if (value := getattr(col, "name", None)) is not None
        }
        try:
            (name,) = names
        except ValueError:
            return col
        else:
            return col.label(name)

    return reduce(func, columns)


@overload
def create_engine(
    drivername: str,
    /,
    *,
    username: str | None = ...,
    password: str | None = ...,
    host: str | None = ...,
    port: int | None = ...,
    database: str | None = ...,
    query: StrMapping | None = ...,
    poolclass: type[Pool] | None = ...,
    async_: Literal[True],
) -> AsyncEngine: ...
@overload
def create_engine(
    drivername: str,
    /,
    *,
    username: str | None = ...,
    password: str | None = ...,
    host: str | None = ...,
    port: int | None = ...,
    database: str | None = ...,
    query: StrMapping | None = ...,
    poolclass: type[Pool] | None = ...,
    async_: Literal[False] = False,
) -> Engine: ...
@overload
def create_engine(
    drivername: str,
    /,
    *,
    username: str | None = ...,
    password: str | None = ...,
    host: str | None = ...,
    port: int | None = ...,
    database: str | None = ...,
    query: StrMapping | None = ...,
    poolclass: type[Pool] | None = ...,
    async_: bool = False,
) -> Engine | AsyncEngine: ...
def create_engine(
    drivername: str,
    /,
    *,
    username: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    query: StrMapping | None = None,
    poolclass: type[Pool] | None = NullPool,
    async_: bool = False,
) -> Engine | AsyncEngine:
    """Create a SQLAlchemy engine."""
    if query is None:
        kwargs = {}
    else:

        def func(x: MaybeIterable[str], /) -> list[str] | str:
            return x if isinstance(x, str) else list(x)

        kwargs = {"query": {k: func(v) for k, v in query.items()}}
    url = URL.create(
        drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        **kwargs,
    )
    if async_:
        return create_async_engine(url, poolclass=poolclass)
    return _create_engine(url, poolclass=poolclass)


class Dialect(enum.Enum):
    """An enumeration of the SQL dialects."""

    mssql = auto()
    mysql = auto()
    oracle = auto()
    postgresql = auto()
    sqlite = auto()

    @property
    def max_params(self, /) -> int:
        match self:
            case Dialect.mssql:  # pragma: no cover
                return 2100
            case Dialect.mysql:  # pragma: no cover
                return 65535
            case Dialect.oracle:  # pragma: no cover
                return 1000
            case Dialect.postgresql:  # skipif-ci-and-not-linux
                return 32767
            case Dialect.sqlite:
                return 100
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)


def ensure_engine(engine: Engine | str, /) -> Engine:
    """Ensure the object is an Engine."""
    if isinstance(engine, Engine):
        return engine
    return parse_engine(engine)


def ensure_tables_created(
    engine_or_conn: EngineOrConnection, /, *tables_or_mapped_classes: TableOrMappedClass
) -> None:
    """Ensure a table/set of tables is/are created."""
    prepared = _ensure_tables_created_prepare(engine_or_conn, *tables_or_mapped_classes)
    for table in prepared.tables:
        with yield_connection(engine_or_conn) as conn:
            try:
                table.create(conn)
            except DatabaseError as error:
                _ensure_tables_maybe_reraise(error, prepared.match)


async def ensure_tables_created_async(
    engine_or_conn: AsyncEngineOrConnection,
    /,
    *tables_or_mapped_classes: TableOrMappedClass,
) -> None:
    """Ensure a table/set of tables is/are created."""
    prepared = _ensure_tables_created_prepare(engine_or_conn, *tables_or_mapped_classes)
    for table in prepared.tables:
        async with yield_connection_async(engine_or_conn) as conn:
            try:
                await conn.run_sync(table.create)
            except DatabaseError as error:
                _ensure_tables_maybe_reraise(error, prepared.match)


@dataclass(frozen=True, kw_only=True)
class _EnsureTablesCreatedOrDroppedPrepare:
    match: str
    tables: AbstractSet[Table]


def _ensure_tables_created_prepare(
    engine_or_conn: MaybeAsyncEngineOrConnection,
    /,
    *tables_or_mapped_classes: TableOrMappedClass,
) -> _EnsureTablesCreatedOrDroppedPrepare:
    """Prepare the arguments for `ensure_tables_created`."""
    return _EnsureTablesCreatedOrDroppedPrepare(
        match=_ensure_tables_created_match(engine_or_conn),
        tables=set(map(get_table, tables_or_mapped_classes)),
    )


def _ensure_tables_created_match(
    engine_or_conn: MaybeAsyncEngineOrConnection, /
) -> str:
    """Get the match statement for the given engine."""
    match dialect := get_dialect(engine_or_conn):
        case Dialect.mysql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.postgresql:  # skipif-ci-and-not-linux
            return "relation .* already exists"
        case Dialect.mssql:  # pragma: no cover
            return "There is already an object named .* in the database"
        case Dialect.oracle:  # pragma: no cover
            return "ORA-00955: name is already used by an existing object"
        case Dialect.sqlite:
            return "table .* already exists"
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _ensure_tables_maybe_reraise(error: DatabaseError, match: str, /) -> None:
    """Re-raise the error if it does not match the required statement."""
    if not search(match, ensure_str(one(error.args))):
        raise error  # pragma: no cover


def ensure_tables_dropped(
    engine_or_conn: EngineOrConnection, *tables_or_mapped_classes: TableOrMappedClass
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    prepared = _ensure_tables_dropped_prepare(engine_or_conn, *tables_or_mapped_classes)
    for table in prepared.tables:
        with yield_connection(engine_or_conn) as conn:
            try:
                table.drop(conn)
            except DatabaseError as error:
                _ensure_tables_dropped_maybe_reraise(error, prepared.match)


async def ensure_tables_dropped_async(
    engine_or_conn: AsyncEngineOrConnection,
    *tables_or_mapped_classes: TableOrMappedClass,
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    prepared = _ensure_tables_dropped_prepare(engine_or_conn, *tables_or_mapped_classes)
    for table in prepared.tables:
        async with yield_connection_async(engine_or_conn) as conn:
            try:
                await conn.run_sync(table.drop)
            except DatabaseError as error:
                _ensure_tables_maybe_reraise(error, prepared.match)


def _ensure_tables_dropped_prepare(
    engine_or_conn: MaybeAsyncEngineOrConnection,
    /,
    *tables_or_mapped_classes: TableOrMappedClass,
) -> _EnsureTablesCreatedOrDroppedPrepare:
    """Prepare the arguments for `ensure_tables_dropped`."""
    return _EnsureTablesCreatedOrDroppedPrepare(
        match=_ensure_tables_dropped_match(engine_or_conn),
        tables=set(map(get_table, tables_or_mapped_classes)),
    )


def _ensure_tables_dropped_match(
    engine_or_conn: MaybeAsyncEngineOrConnection, /
) -> str:
    """Get the match statement for the given engine."""
    match dialect := get_dialect(engine_or_conn):
        case Dialect.mysql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.postgresql:  # skipif-ci-and-not-linux
            return "table .* does not exist"
        case Dialect.mssql:  # pragma: no cover
            return "Cannot drop the table .*, because it does not exist or you do not have permission"
        case Dialect.oracle:  # pragma: no cover
            return "ORA-00942: table or view does not exist"
        case Dialect.sqlite:
            return "no such table"
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _ensure_tables_dropped_maybe_reraise(error: DatabaseError, match: str, /) -> None:
    """Re-raise the error if it does not match the required statement."""
    if not search(match, ensure_str(one(error.args))):
        raise error  # pragma: no cover


def get_chunk_size(
    engine_or_conn: MaybeAsyncEngineOrConnection,
    /,
    *,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    scaling: float = 1.0,
) -> int:
    """Get the maximum chunk size for an engine."""
    dialect = get_dialect(engine_or_conn)
    max_params = dialect.max_params
    return max(floor(chunk_size_frac * max_params / scaling), 1)


def get_column_names(table_or_mapped_class: TableOrMappedClass, /) -> list[str]:
    """Get the column names from a table or model."""
    return [col.name for col in get_columns(table_or_mapped_class)]


def get_columns(table_or_mapped_class: TableOrMappedClass, /) -> list[Column[Any]]:
    """Get the columns from a table or model."""
    return list(get_table(table_or_mapped_class).columns)


def get_dialect(engine_or_conn: MaybeAsyncEngineOrConnection, /) -> Dialect:
    """Get the dialect of a database."""
    dialect = engine_or_conn.dialect
    if isinstance(dialect, mssql_dialect):  # pragma: no cover
        return Dialect.mssql
    if isinstance(dialect, mysql_dialect):  # pragma: no cover
        return Dialect.mysql
    if isinstance(dialect, oracle_dialect):  # pragma: no cover
        return Dialect.oracle
    if isinstance(  # skipif-ci-and-not-linux
        dialect, postgresql_dialect | PGDialect_asyncpg
    ):
        return Dialect.postgresql
    if isinstance(dialect, sqlite_dialect):
        return Dialect.sqlite
    raise GetDialectError(dialect=dialect)  # pragma: no cover


@dataclass(kw_only=True)
class GetDialectError(Exception):
    dialect: sqlalchemy.Dialect

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            f"Dialect must be one of MS SQL, MySQL, Oracle, PostgreSQL or SQLite; got {self.dialect} instead"
        )


def get_table(obj: Any, /) -> Table:
    """Get the table from a Table or mapped class."""
    if isinstance(obj, Table):
        return obj
    if is_mapped_class(obj):
        return obj.__table__
    raise GetTableError(obj=obj)


@dataclass(kw_only=True)
class GetTableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be a Table or mapped class; got {get_class_name(self.obj)!r}"


def get_table_updated_column(
    table_or_mapped_class: TableOrMappedClass, /, *, pattern: str = "updated"
) -> str | None:
    """Get the name of the unique `updated_at` column, if it exists."""

    def is_updated_at(column: Column[Any], /) -> bool:
        return (
            bool(search(pattern, column.name))
            and is_date_time_with_time_zone(column.type)
            and is_now(column.onupdate)
        )

    def is_date_time_with_time_zone(type_: Any, /) -> bool:
        return isinstance(type_, DateTime) and type_.timezone

    def is_now(on_update: Any, /) -> bool:
        return isinstance(on_update, ColumnElementColumnDefault) and isinstance(
            on_update.arg, now
        )

    matches = filter(is_updated_at, get_columns(table_or_mapped_class))
    try:
        return one(matches).name
    except OneEmptyError:
        return None


def get_table_name(table_or_mapped_class: TableOrMappedClass, /) -> str:
    """Get the table name from a Table or mapped class."""
    return get_table(table_or_mapped_class).name


_PairOfTupleAndTable = tuple[tuple[Any, ...], TableOrMappedClass]
_PairOfDictAndTable = tuple[StrMapping, TableOrMappedClass]
_PairOfListOfTuplesAndTable = tuple[Sequence[tuple[Any, ...]], TableOrMappedClass]
_PairOfListOfDictsAndTable = tuple[Sequence[StrMapping], TableOrMappedClass]
_ListOfPairOfTupleAndTable = Sequence[tuple[tuple[Any, ...], TableOrMappedClass]]
_ListOfPairOfDictAndTable = Sequence[tuple[StrMapping, TableOrMappedClass]]
_InsertItem = (
    _PairOfTupleAndTable
    | _PairOfDictAndTable
    | _PairOfListOfTuplesAndTable
    | _PairOfListOfDictsAndTable
    | _ListOfPairOfTupleAndTable
    | _ListOfPairOfDictAndTable
    | MaybeIterable[DeclarativeBase]
)


def insert_items(
    engine_or_conn: EngineOrConnection,
    /,
    *items: _InsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
) -> None:
    """Insert a set of items into a database.

    These can be one of the following:
     - pair of tuple & table/class:           (x1, x2, ...), table_cls
     - pair of dict & table/class:            {k1=v1, k2=v2, ...), table_cls
     - pair of list of tuples & table/class:  [(x11, x12, ...),
                                               (x21, x22, ...),
                                               ...], table_cls
     - pair of list of dicts & table/class:   [{k1=v11, k2=v12, ...},
                                               {k1=v21, k2=v22, ...},
                                               ...], table/class
     - list of pairs of tuple & table/class:  [((x11, x12, ...), table_cls1),
                                               ((x21, x22, ...), table_cls2),
                                               ...]
     - list of pairs of dict & table/class:   [({k1=v11, k2=v12, ...}, table_cls1),
                                               ({k1=v21, k2=v22, ...}, table_cls2),
                                               ...]
     - mapped class:                          Obj(k1=v1, k2=v2, ...)
     - list of mapped classes:                [Obj(k1=v11, k2=v12, ...),
                                               Obj(k1=v21, k2=v22, ...),
                                               ...]
    """
    try:
        prepared = _insert_items_prepare(
            engine_or_conn, *items, chunk_size_frac=chunk_size_frac
        )
    except _InsertItemsPrepareError as error:
        raise InsertItemsError(item=error.item) from None
    if not assume_tables_exist:
        ensure_tables_created(engine_or_conn, *prepared.tables)
    for ins, parameters in prepared.yield_pairs():
        with yield_connection(engine_or_conn) as conn:
            _ = conn.execute(ins, parameters=parameters)


@dataclass(kw_only=True)
class InsertItemsError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


async def insert_items_async(
    engine_or_conn: AsyncEngineOrConnection,
    *items: _InsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
) -> None:
    """Insert a set of items into a database.

    These can be one of the following:
     - pair of tuple & table/class:           (x1, x2, ...), table_cls
     - pair of dict & table/class:            {k1=v1, k2=v2, ...), table_cls
     - pair of list of tuples & table/class:  [(x11, x12, ...),
                                               (x21, x22, ...),
                                               ...], table_cls
     - pair of list of dicts & table/class:   [{k1=v11, k2=v12, ...},
                                               {k1=v21, k2=v22, ...},
                                               ...], table/class
     - list of pairs of tuple & table/class:  [((x11, x12, ...), table_cls1),
                                               ((x21, x22, ...), table_cls2),
                                               ...]
     - list of pairs of dict & table/class:   [({k1=v11, k2=v12, ...}, table_cls1),
                                               ({k1=v21, k2=v22, ...}, table_cls2),
                                               ...]
     - mapped class:                          Obj(k1=v1, k2=v2, ...)
     - list of mapped classes:                [Obj(k1=v11, k2=v12, ...),
                                               Obj(k1=v21, k2=v22, ...),
                                               ...]
    """
    try:
        prepared = _insert_items_prepare(
            engine_or_conn, *items, chunk_size_frac=chunk_size_frac
        )
    except _InsertItemsPrepareError as error:
        raise InsertItemsAsyncError(item=error.item) from None
    if not assume_tables_exist:
        await ensure_tables_created_async(engine_or_conn, *prepared.tables)
    for ins, parameters in prepared.yield_pairs():
        async with yield_connection_async(engine_or_conn) as conn:
            _ = await conn.execute(ins, parameters=parameters)


@dataclass(kw_only=True)
class InsertItemsAsyncError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


@dataclass(frozen=True, kw_only=True)
class _PreInsertUpsertItems:
    tables: Sequence[Table]
    yield_pairs: Callable[[], Iterator[tuple[Insert, Any]]]


def _insert_items_prepare(
    engine_or_conn: MaybeAsyncEngineOrConnection,
    /,
    *items: _InsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> _PreInsertUpsertItems:
    """Prepare the arguments for `insert_items`."""
    mapping: dict[Table, list[TupleOrStrMapping]] = defaultdict(list)
    lengths: set[int] = set()
    try:
        for item in items:
            for normed in _normalize_insert_item(item):
                values = normed.values
                mapping[normed.table].append(values)
                lengths.add(len(values))
    except _NormalizeInsertItemError as error:
        raise _InsertItemsPrepareError(item=error.item) from None
    tables = list(mapping)
    max_length = max(lengths, default=1)
    chunk_size = get_chunk_size(
        engine_or_conn, chunk_size_frac=chunk_size_frac, scaling=max_length
    )

    def yield_pairs() -> Iterator[tuple[Insert, Any]]:
        for table, values in mapping.items():
            for chunk in chunked(values, chunk_size):
                match get_dialect(engine_or_conn):
                    case Dialect.oracle:  # pragma: no cover
                        yield insert(table), chunk
                    case _:
                        yield insert(table).values(list(chunk)), None

    return _PreInsertUpsertItems(tables=tables, yield_pairs=yield_pairs)


@dataclass(kw_only=True)
class _InsertItemsPrepareError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


def is_insert_item_pair(
    obj: Any, /
) -> TypeGuard[tuple[TupleOrStrMapping, TableOrMappedClass]]:
    """Check if an object is an insert-ready pair."""
    return _is_insert_or_upsert_pair(obj, is_tuple_or_string_mapping)


def is_upsert_item_pair(
    obj: Any, /
) -> TypeGuard[tuple[StrMapping, TableOrMappedClass]]:
    """Check if an object is an upsert-ready pair."""
    return _is_insert_or_upsert_pair(obj, is_string_mapping)


def _is_insert_or_upsert_pair(
    obj: Any, predicate: Callable[[TupleOrStrMapping], bool], /
) -> bool:
    """Check if an object is an insert/upsert-ready pair."""
    return (
        isinstance(obj, tuple)
        and (len(obj) == 2)
        and predicate(obj[0])
        and is_table_or_mapped_class(obj[1])
    )


def is_mapped_class(obj: Any, /) -> bool:
    """Check if an object is a mapped class."""
    if isinstance(obj, type):
        try:
            _ = class_mapper(cast(Any, obj))
        except (ArgumentError, UnmappedClassError):
            return False
        return True
    return is_mapped_class(type(obj))


def is_table_or_mapped_class(obj: Any, /) -> bool:
    """Check if an object is a Table or a mapped class."""
    return isinstance(obj, Table) or is_mapped_class(obj)


def mapped_class_to_dict(obj: Any, /) -> dict[str, Any]:
    """Construct a dictionary of elements for insertion."""
    cls = type(obj)

    def is_attr(attr: str, key: str, /) -> str | None:
        if isinstance(value := getattr(cls, attr), InstrumentedAttribute) and (
            value.name == key
        ):
            return attr
        return None

    def yield_items() -> Iterator[tuple[str, Any]]:
        for key in get_column_names(cls):
            attr = one(attr for attr in dir(cls) if is_attr(attr, key) is not None)
            yield key, getattr(obj, attr)

    return dict(yield_items())


@dataclass(kw_only=True)
class _NormalizedInsertItem:
    values: TupleOrStrMapping
    table: Table


def _normalize_insert_item(item: _InsertItem, /) -> Iterator[_NormalizedInsertItem]:
    """Normalize an insertion item."""
    try:
        for norm in _normalize_upsert_item(cast(Any, item), selected_or_all="all"):
            yield _NormalizedInsertItem(values=norm.values, table=norm.table)
    except _NormalizeUpsertItemError:
        pass
    else:
        return

    if is_insert_item_pair(item):
        yield _NormalizedInsertItem(values=item[0], table=get_table(item[1]))
        return

    item = cast(_PairOfListOfTuplesAndTable | _ListOfPairOfTupleAndTable, item)

    if (
        isinstance(item, tuple)
        and (len(item) == 2)
        and is_iterable_not_str(item[0])
        and all(is_tuple_or_string_mapping(i) for i in item[0])
        and is_table_or_mapped_class(item[1])
    ):
        item = cast(_PairOfListOfTuplesAndTable, item)
        for i in item[0]:
            yield _NormalizedInsertItem(values=i, table=get_table(item[1]))
        return

    item = cast(_ListOfPairOfDictAndTable, item)

    if is_iterable_not_str(item) and all(is_insert_item_pair(i) for i in item):
        item = cast(_ListOfPairOfTupleAndTable | _ListOfPairOfDictAndTable, item)
        for i in item:
            yield _NormalizedInsertItem(values=i[0], table=get_table(i[1]))
        return

    raise _NormalizeInsertItemError(item=item)


@dataclass(kw_only=True)
class _NormalizeInsertItemError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


@dataclass(kw_only=True)
class _NormalizedUpsertItem:
    values: StrMapping
    table: Table


def _normalize_upsert_item(
    item: _UpsertItem, /, *, selected_or_all: Literal["selected", "all"] = "selected"
) -> Iterator[_NormalizedUpsertItem]:
    """Normalize an upsert item."""
    normalized = _normalize_upsert_item_inner(item)
    match selected_or_all:
        case "selected":
            for norm in normalized:
                values = {k: v for k, v in norm.values.items() if v is not None}
                yield _NormalizedUpsertItem(values=values, table=norm.table)
        case "all":
            yield from normalized
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _normalize_upsert_item_inner(
    item: _UpsertItem, /
) -> Iterator[_NormalizedUpsertItem]:
    if is_upsert_item_pair(item):
        yield _NormalizedUpsertItem(values=item[0], table=get_table(item[1]))
        return

    item = cast(
        _PairOfListOfDictsAndTable
        | _ListOfPairOfDictAndTable
        | DeclarativeBase
        | Sequence[DeclarativeBase],
        item,
    )

    if (
        isinstance(item, tuple)
        and (len(item) == 2)
        and is_iterable_not_str(item[0])
        and all(is_string_mapping(i) for i in item[0])
        and is_table_or_mapped_class(item[1])
    ):
        item = cast(_PairOfListOfDictsAndTable, item)
        for i in item[0]:
            yield _NormalizedUpsertItem(values=i, table=get_table(item[1]))
        return

    item = cast(
        _ListOfPairOfDictAndTable | DeclarativeBase | Sequence[DeclarativeBase], item
    )

    if is_iterable_not_str(item) and all(is_upsert_item_pair(i) for i in item):
        item = cast(_ListOfPairOfDictAndTable, item)
        for i in item:
            yield _NormalizedUpsertItem(values=i[0], table=get_table(i[1]))
        return

    item = cast(MaybeIterable[DeclarativeBase], item)
    if isinstance(item, DeclarativeBase) or (
        is_iterable_not_str(item) and all(isinstance(i, DeclarativeBase) for i in item)
    ):
        for i in always_iterable(item):
            yield _NormalizedUpsertItem(
                values=mapped_class_to_dict(i), table=get_table(i)
            )
        return

    raise _NormalizeUpsertItemError(item=item)


@dataclass(kw_only=True)
class _NormalizeUpsertItemError(Exception):
    item: _UpsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


def parse_engine(engine: str, /) -> Engine:
    """Parse a string into an Engine."""
    with redirect_error(ArgumentError, ParseEngineError(f"{engine=}")):
        return _create_engine(engine, poolclass=NullPool)


class ParseEngineError(Exception): ...


def reflect_table(
    table_or_mapped_class: TableOrMappedClass,
    engine_or_conn: EngineOrConnection,
    /,
    *,
    schema: str | None = None,
) -> Table:
    """Reflect a table from a database."""
    name = get_table_name(table_or_mapped_class)
    metadata = MetaData(schema=schema)
    with yield_connection(engine_or_conn) as conn:
        return Table(name, metadata, autoload_with=conn)


def serialize_engine(engine: Engine, /) -> str:
    """Serialize an Engine."""
    return engine.url.render_as_string(hide_password=False)


class TablenameMixin:
    """Mix-in for an auto-generated tablename."""

    @cast(Any, declared_attr)
    def __tablename__(cls) -> str:  # noqa: N805
        from utilities.humps import snake_case

        return snake_case(get_class_name(cls))


_UpsertItem = (
    _PairOfDictAndTable
    | _PairOfListOfDictsAndTable
    | _ListOfPairOfDictAndTable
    | MaybeIterable[DeclarativeBase]
)


def upsert_items(
    engine_or_conn: EngineOrConnection,
    /,
    *items: _UpsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> None:
    """Upsert a set of items into a database.

    These can be one of the following:
     - pair of dict & table/class:            {k1=v1, k2=v2, ...), table_cls
     - pair of list of dicts & table/class:   [{k1=v11, k2=v12, ...},
                                               {k1=v21, k2=v22, ...},
                                               ...], table/class
     - list of pairs of dict & table/class:   [({k1=v11, k2=v12, ...}, table_cls1),
                                               ({k1=v21, k2=v22, ...}, table_cls2),
                                               ...]
     - mapped class:                          Obj(k1=v1, k2=v2, ...)
     - list of mapped classes:                [Obj(k1=v11, k2=v12, ...),
                                               Obj(k1=v21, k2=v22, ...),
                                               ...]
    """
    try:
        prepared = _upsert_items_prepare(
            engine_or_conn,
            *items,
            chunk_size_frac=chunk_size_frac,
            selected_or_all=selected_or_all,
        )
    except _UpsertItemsPrepareError as error:
        raise UpsertItemsError(item=error.item) from None
    if not assume_tables_exist:
        ensure_tables_created(engine_or_conn, *prepared.tables)
    for ins, parameters in prepared.yield_pairs():
        with yield_connection(engine_or_conn) as conn:
            _ = conn.execute(ins, parameters=parameters)


@dataclass(kw_only=True)
class UpsertItemsError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


def _upsert_items_prepare(
    engine_or_conn: MaybeAsyncEngineOrConnection,
    /,
    *items: _UpsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> _PreInsertUpsertItems:
    """Prepare the arguments for `insert_items`."""
    mapping: dict[Table, list[StrMapping]] = defaultdict(list)
    lengths: set[int] = set()
    try:
        for item in items:
            for normed in _normalize_upsert_item(item, selected_or_all=selected_or_all):
                values = normed.values
                mapping[normed.table].append(values)
                lengths.add(len(values))
    except _NormalizeUpsertItemError as error:
        raise _UpsertItemsPrepareError(item=error.item) from None
    tables = list(mapping)
    max_length = max(lengths, default=1)
    chunk_size = get_chunk_size(
        engine_or_conn, chunk_size_frac=chunk_size_frac, scaling=max_length
    )

    def yield_pairs() -> Iterator[tuple[Insert, Any]]:
        for table, values in mapping.items():
            for chunk in chunked(values, chunk_size):
                ups = _upsert_items_build(
                    engine_or_conn, table, chunk, selected_or_all=selected_or_all
                )
                yield ups, None

    return _PreInsertUpsertItems(tables=tables, yield_pairs=yield_pairs)


def _upsert_items_build(
    engine_or_conn: MaybeAsyncEngineOrConnection,
    table: Table,
    values: Iterable[StrMapping],
    /,
    *,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> Insert:
    values = list(values)
    keys = set(reduce(or_, values))
    dict_nones = {k: None for k in keys}
    values = [{**dict_nones, **v} for v in values]
    if (updated_col := get_table_updated_column(table)) is not None:
        up_map = {updated_col: get_now()}
        values = [{**v, **up_map} for v in values]
    match get_dialect(engine_or_conn):
        case Dialect.postgresql:  # skipif-ci-and-not-linux
            insert = postgresql_insert
        case Dialect.sqlite:
            insert = sqlite_insert
        case (  # pragma: no cover
            (Dialect.mssql | Dialect.mysql | Dialect.oracle) as dialect
        ):
            raise NotImplementedError(dialect)
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    ins = insert(table).values(values)
    primary_key = cast(Any, table.primary_key)
    return _upsert_items_apply_on_conflict_do_update(
        values, ins, primary_key, selected_or_all=selected_or_all
    )


def _upsert_items_apply_on_conflict_do_update(
    values: Iterable[StrMapping],
    insert: postgresql_Insert | sqlite_Insert,
    primary_key: PrimaryKeyConstraint,
    /,
    *,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> Insert:
    match selected_or_all:
        case "selected":
            columns = set(reduce(or_, values))
        case "all":
            columns = {c.name for c in insert.excluded}
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    set_ = {c: getattr(insert.excluded, c) for c in columns}
    match insert:
        case postgresql_Insert():  # skipif-ci
            return insert.on_conflict_do_update(constraint=primary_key, set_=set_)
        case sqlite_Insert():
            return insert.on_conflict_do_update(index_elements=primary_key, set_=set_)
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


@dataclass(kw_only=True)
class _UpsertItemsPrepareError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


async def upsert_items_async(
    engine_or_conn: AsyncEngineOrConnection,
    /,
    *items: _UpsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> None:
    """Upsert a set of items into a database.

    These can be one of the following:
     - pair of dict & table/class:            {k1=v1, k2=v2, ...), table_cls
     - pair of list of dicts & table/class:   [{k1=v11, k2=v12, ...},
                                               {k1=v21, k2=v22, ...},
                                               ...], table/class
     - list of pairs of dict & table/class:   [({k1=v11, k2=v12, ...}, table_cls1),
                                               ({k1=v21, k2=v22, ...}, table_cls2),
                                               ...]
     - mapped class:                          Obj(k1=v1, k2=v2, ...)
     - list of mapped classes:                [Obj(k1=v11, k2=v12, ...),
                                               Obj(k1=v21, k2=v22, ...),
                                               ...]
    """
    try:
        prepared = _upsert_items_prepare(
            engine_or_conn,
            *items,
            chunk_size_frac=chunk_size_frac,
            selected_or_all=selected_or_all,
        )
    except _UpsertItemsPrepareError as error:
        raise UpsertItemsAsyncError(item=error.item) from None
    if not assume_tables_exist:
        await ensure_tables_created_async(engine_or_conn, *prepared.tables)
    for ins, parameters in prepared.yield_pairs():
        async with yield_connection_async(engine_or_conn) as conn:
            _ = await conn.execute(ins, parameters=parameters)


@dataclass(kw_only=True)
class UpsertItemsAsyncError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


@contextmanager
def yield_connection(engine_or_conn: EngineOrConnection, /) -> Iterator[Connection]:
    """Yield a synchronous connection."""
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield conn
    else:
        yield engine_or_conn


@asynccontextmanager
async def yield_connection_async(
    engine_or_conn: AsyncEngineOrConnection, /
) -> AsyncIterator[AsyncConnection]:
    """Yield an asynchronous connection."""
    if isinstance(engine_or_conn, AsyncEngine):
        async with engine_or_conn.begin() as conn:
            yield conn
    else:
        yield engine_or_conn


def yield_primary_key_columns(obj: Any, /) -> Iterator[Column]:
    """Yield the primary key columns of a table."""
    table = get_table(obj)
    yield from table.primary_key


__all__ = [
    "CHUNK_SIZE_FRAC",
    "CheckEngineError",
    "Dialect",
    "GetDialectError",
    "GetTableError",
    "InsertItemsAsyncError",
    "InsertItemsError",
    "ParseEngineError",
    "TablenameMixin",
    "UpsertItemsAsyncError",
    "UpsertItemsError",
    "check_engine",
    "check_table_against_reflection",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "ensure_engine",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "ensure_tables_dropped_async",
    "get_chunk_size",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table",
    "get_table_name",
    "get_table_updated_column",
    "insert_items",
    "insert_items_async",
    "is_insert_item_pair",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "is_upsert_item_pair",
    "is_upsert_item_pair",
    "mapped_class_to_dict",
    "parse_engine",
    "serialize_engine",
    "upsert_items",
    "upsert_items_async",
    "yield_connection",
    "yield_connection_async",
    "yield_primary_key_columns",
]
