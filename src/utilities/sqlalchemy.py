from __future__ import annotations

from collections import defaultdict
from collections.abc import (
    AsyncIterator,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Sequence,
    Sized,
)
from collections.abc import Set as AbstractSet
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import partial, reduce
from itertools import chain
from math import floor
from operator import ge, le
from re import search
from socket import gaierror
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
    assert_never,
    cast,
    overload,
    override,
)

import sqlalchemy
from sqlalchemy import (
    URL,
    Column,
    Connection,
    Engine,
    Insert,
    PrimaryKeyConstraint,
    Selectable,
    Table,
    and_,
    case,
    insert,
    select,
    text,
)
from sqlalchemy.dialects.mssql import dialect as mssql_dialect
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy.dialects.postgresql import Insert as postgresql_Insert
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.dialects.sqlite import Insert as sqlite_Insert
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import (
    ArgumentError,
    DatabaseError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    InstrumentedAttribute,
    class_mapper,
    declared_attr,
)
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.pool import NullPool, Pool

from utilities.asyncio import timeout_td
from utilities.functions import (
    ensure_str,
    get_class_name,
    is_sequence_of_tuple_or_str_mapping,
    is_string_mapping,
    is_tuple,
    is_tuple_or_str_mapping,
    yield_object_attributes,
)
from utilities.iterables import (
    CheckLengthError,
    CheckSubSetError,
    OneEmptyError,
    OneNonUniqueError,
    check_length,
    check_subset,
    chunked,
    merge_sets,
    merge_str_mappings,
    one,
)
from utilities.reprlib import get_repr
from utilities.text import secret_str, snake_case
from utilities.types import (
    Delta,
    MaybeIterable,
    MaybeType,
    StrMapping,
    TupleOrStrMapping,
)

if TYPE_CHECKING:
    from enum import Enum, StrEnum


type EngineOrConnectionOrAsync = Engine | Connection | AsyncEngine | AsyncConnection
type Dialect = Literal["mssql", "mysql", "oracle", "postgresql", "sqlite"]
type DialectOrEngineOrConnectionOrAsync = Dialect | EngineOrConnectionOrAsync
type ORMInstOrClass = DeclarativeBase | type[DeclarativeBase]
type TableOrORMInstOrClass = Table | ORMInstOrClass
CHUNK_SIZE_FRAC = 0.8


##


_SELECT = text("SELECT 1")


def check_connect(engine: Engine, /) -> bool:
    """Check if an engine can connect."""
    try:
        with engine.connect() as conn:
            return bool(conn.execute(_SELECT).scalar_one())
    except (gaierror, ConnectionRefusedError, OperationalError, ProgrammingError):
        return False


async def check_connect_async(
    engine: AsyncEngine, /, *, timeout: Delta | None = None
) -> bool:
    """Check if an engine can connect."""
    try:
        async with timeout_td(timeout), engine.connect() as conn:
            return bool((await conn.execute(_SELECT)).scalar_one())
    except (
        gaierror,
        ConnectionRefusedError,
        OperationalError,
        ProgrammingError,
        TimeoutError,
    ):
        return False


##


async def check_engine(
    engine: AsyncEngine,
    /,
    *,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    num_tables: int | tuple[int, float] | None = None,
) -> None:
    """Check that an engine can connect.

    Optionally query for the number of tables, or the number of columns in
    such a table.
    """
    match _get_dialect(engine):
        case "mssql" | "mysql" | "postgresql":  # skipif-ci-and-not-linux
            query = "select * from information_schema.tables"
        case "oracle":  # pragma: no cover
            query = "select * from all_objects"
        case "sqlite":
            query = "select * from sqlite_master where type='table'"
        case never:
            assert_never(never)
    statement = text(query)
    async with yield_connection(engine, timeout=timeout, error=error) as conn:
        rows = (await conn.execute(statement)).all()
    if num_tables is not None:
        try:
            check_length(rows, equal_or_approx=num_tables)
        except CheckLengthError as err:
            raise CheckEngineError(
                engine=engine, rows=err.obj, expected=num_tables
            ) from None


@dataclass(kw_only=True, slots=True)
class CheckEngineError(Exception):
    engine: AsyncEngine
    rows: Sized
    expected: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        return f"{get_repr(self.engine)} must have {self.expected} table(s); got {len(self.rows)}"


##


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


##


@overload
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
    async_: Literal[True],
) -> AsyncEngine: ...
@overload
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
    async_: Literal[False] = False,
) -> Engine: ...
@overload
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
    match async_:
        case False:
            return sqlalchemy.create_engine(url, poolclass=poolclass)
        case True:
            return create_async_engine(url, poolclass=poolclass)
        case never:
            assert_never(never)


##


async def ensure_database_created(super_: URL, database: str, /) -> None:
    """Ensure a database is created."""
    engine = create_async_engine(super_, isolation_level="AUTOCOMMIT")
    async with engine.begin() as conn:
        try:
            _ = await conn.execute(text(f"CREATE DATABASE {database}"))
        except (OperationalError, ProgrammingError) as error:
            if not search('database ".*" already exists', ensure_str(one(error.args))):
                raise


async def ensure_database_dropped(super_: URL, database: str, /) -> None:
    """Ensure a database is dropped."""
    engine = create_async_engine(super_, isolation_level="AUTOCOMMIT")
    async with engine.begin() as conn:
        _ = await conn.execute(text(f"DROP DATABASE IF EXISTS {database}"))


##


async def ensure_tables_created(
    engine: AsyncEngine,
    /,
    *tables_or_orms: TableOrORMInstOrClass,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
) -> None:
    """Ensure a table/set of tables is/are created."""
    tables = set(map(get_table, tables_or_orms))
    match dialect := _get_dialect(engine):
        case "mysql":  # pragma: no cover
            raise NotImplementedError(dialect)
        case "postgresql":  # skipif-ci-and-not-linux
            match = "relation .* already exists"
        case "mssql":  # pragma: no cover
            match = "There is already an object named .* in the database"
        case "oracle":  # pragma: no cover
            match = "ORA-00955: name is already used by an existing object"
        case "sqlite":
            match = "table .* already exists"
        case never:
            assert_never(never)
    async with yield_connection(engine, timeout=timeout, error=error) as conn:
        for table in tables:
            try:
                await conn.run_sync(table.create)
            except DatabaseError as err:
                _ensure_tables_maybe_reraise(err, match)


async def ensure_tables_dropped(
    engine: AsyncEngine,
    *tables_or_orms: TableOrORMInstOrClass,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    tables = set(map(get_table, tables_or_orms))
    match dialect := _get_dialect(engine):
        case "mysql":  # pragma: no cover
            raise NotImplementedError(dialect)
        case "postgresql":  # skipif-ci-and-not-linux
            match = "table .* does not exist"
        case "mssql":  # pragma: no cover
            match = "Cannot drop the table .*, because it does not exist or you do not have permission"
        case "oracle":  # pragma: no cover
            match = "ORA-00942: table or view does not exist"
        case "sqlite":
            match = "no such table"
        case never:
            assert_never(never)
    async with yield_connection(engine, timeout=timeout, error=error) as conn:
        for table in tables:
            try:
                await conn.run_sync(table.drop)
            except DatabaseError as err:
                _ensure_tables_maybe_reraise(err, match)


##


def enum_name(enum: type[Enum], /) -> str:
    """Get the name of an Enum."""
    return f"{snake_case(get_class_name(enum))}_enum"


##


def enum_values(enum: type[StrEnum], /) -> list[str]:
    """Get the values of a StrEnum."""
    return [e.value for e in enum]


##


@dataclass(kw_only=True, slots=True)
class ExtractURLOutput:
    username: str
    password: secret_str
    host: str
    port: int
    database: str


def extract_url(url: URL, /) -> ExtractURLOutput:
    """Extract the database, host & port from a URL."""
    if url.username is None:
        raise _ExtractURLUsernameError(url=url)
    if url.password is None:
        raise _ExtractURLPasswordError(url=url)
    if url.host is None:
        raise _ExtractURLHostError(url=url)
    if url.port is None:
        raise _ExtractURLPortError(url=url)
    if url.database is None:
        raise _ExtractURLDatabaseError(url=url)
    return ExtractURLOutput(
        username=url.username,
        password=secret_str(url.password),
        host=url.host,
        port=url.port,
        database=url.database,
    )


@dataclass(kw_only=True, slots=True)
class ExtractURLError(Exception):
    url: URL


@dataclass(kw_only=True, slots=True)
class _ExtractURLUsernameError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a user name; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _ExtractURLPasswordError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a password; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _ExtractURLHostError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a host; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _ExtractURLPortError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a port; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _ExtractURLDatabaseError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a database; got {self.url}"


##


def get_chunk_size(
    dialect_or_engine_or_conn: DialectOrEngineOrConnectionOrAsync,
    table_or_orm_or_num_cols: TableOrORMInstOrClass | Sized | int,
    /,
    *,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> int:
    """Get the maximum chunk size for an engine."""
    max_params = _get_dialect_max_params(dialect_or_engine_or_conn)
    match table_or_orm_or_num_cols:
        case Table() | DeclarativeBase() | type() as table_or_orm:
            return get_chunk_size(
                dialect_or_engine_or_conn,
                get_columns(table_or_orm),
                chunk_size_frac=chunk_size_frac,
            )
        case Sized() as sized:
            return get_chunk_size(
                dialect_or_engine_or_conn, len(sized), chunk_size_frac=chunk_size_frac
            )
        case int() as num_cols:
            size = floor(chunk_size_frac * max_params / num_cols)
            return max(size, 1)
        case never:
            assert_never(never)


##


def get_column_names(table_or_orm: TableOrORMInstOrClass, /) -> list[str]:
    """Get the column names from a table or ORM instance/class."""
    return [col.name for col in get_columns(table_or_orm)]


##


def get_columns(table_or_orm: TableOrORMInstOrClass, /) -> list[Column[Any]]:
    """Get the columns from a table or ORM instance/class."""
    return list(get_table(table_or_orm).columns)


##


def get_primary_key_values(orm: DeclarativeBase, /) -> tuple[Any, ...]:
    """Get a tuple of the primary key values."""
    return tuple(getattr(orm, c.name) for c in yield_primary_key_columns(orm))


##


def get_table(table_or_orm: TableOrORMInstOrClass, /) -> Table:
    """Get the table from a Table or mapped class."""
    if isinstance(table_or_orm, Table):
        return table_or_orm
    if is_orm(table_or_orm):
        return cast("Table", table_or_orm.__table__)
    raise GetTableError(obj=table_or_orm)


@dataclass(kw_only=True, slots=True)
class GetTableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be a Table or mapped class; got {get_class_name(self.obj)!r}"


##


def get_table_name(table_or_orm: TableOrORMInstOrClass, /) -> str:
    """Get the table name from a Table or mapped class."""
    return get_table(table_or_orm).name


##


def hash_primary_key_values(orm: DeclarativeBase, /) -> int:
    """Compute a hash of the primary key values."""
    return hash(get_primary_key_values(orm))


##


type _PairOfTupleAndTable = tuple[tuple[Any, ...], TableOrORMInstOrClass]
type _PairOfStrMappingAndTable = tuple[StrMapping, TableOrORMInstOrClass]
type _PairOfTupleOrStrMappingAndTable = tuple[TupleOrStrMapping, TableOrORMInstOrClass]
type _PairOfSequenceOfTupleOrStrMappingAndTable = tuple[
    Sequence[TupleOrStrMapping], TableOrORMInstOrClass
]
type _InsertItem = (
    _PairOfTupleOrStrMappingAndTable
    | _PairOfSequenceOfTupleOrStrMappingAndTable
    | DeclarativeBase
    | Sequence[_PairOfTupleOrStrMappingAndTable]
    | Sequence[DeclarativeBase]
)


async def insert_items(
    engine: AsyncEngine,
    *items: _InsertItem,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    timeout_create: Delta | None = None,
    error_create: type[Exception] = TimeoutError,
    timeout_insert: Delta | None = None,
    error_insert: type[Exception] = TimeoutError,
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

    def build_insert(
        table: Table, values: Iterable[StrMapping], /
    ) -> tuple[Insert, Any]:
        match _get_dialect(engine):
            case "oracle":  # pragma: no cover
                return insert(table), values
            case _:
                return insert(table).values(list(values)), None

    try:
        prepared = _prepare_insert_or_upsert_items(
            partial(_normalize_insert_item, snake=snake),
            engine,
            build_insert,
            *items,
            chunk_size_frac=chunk_size_frac,
        )
    except _PrepareInsertOrUpsertItemsError as error:
        raise InsertItemsError(item=error.item) from None
    if not assume_tables_exist:
        await ensure_tables_created(
            engine, *prepared.tables, timeout=timeout_create, error=error_create
        )
    for ins, parameters in prepared.yield_pairs():
        async with yield_connection(
            engine, timeout=timeout_insert, error=error_insert
        ) as conn:
            _ = await conn.execute(ins, parameters=parameters)


@dataclass(kw_only=True, slots=True)
class InsertItemsError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


##


def is_orm(obj: Any, /) -> TypeGuard[ORMInstOrClass]:
    """Check if an object is an ORM instance/class."""
    if isinstance(obj, type):
        try:
            _ = class_mapper(cast("Any", obj))
        except (ArgumentError, UnmappedClassError):
            return False
        return True
    return is_orm(type(obj))


##


def is_table_or_orm(obj: Any, /) -> TypeGuard[TableOrORMInstOrClass]:
    """Check if an object is a Table or an ORM instance/class."""
    return isinstance(obj, Table) or is_orm(obj)


##


async def migrate_data(
    table_or_orm_from: TableOrORMInstOrClass,
    engine_from: AsyncEngine,
    engine_to: AsyncEngine,
    /,
    *,
    table_or_orm_to: TableOrORMInstOrClass | None = None,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    timeout_create: Delta | None = None,
    error_create: type[Exception] = TimeoutError,
    timeout_insert: Delta | None = None,
    error_insert: type[Exception] = TimeoutError,
) -> None:
    """Migrate the contents of a table from one database to another."""
    table_from = get_table(table_or_orm_from)
    async with engine_from.begin() as conn:
        rows = (await conn.execute(select(table_from))).all()
    mappings = [dict(r._mapping) for r in rows]  # noqa: SLF001
    table_to = table_from if table_or_orm_to is None else get_table(table_or_orm_to)
    items = (mappings, table_to)
    await insert_items(
        engine_to,
        items,
        chunk_size_frac=chunk_size_frac,
        assume_tables_exist=assume_tables_exist,
        timeout_create=timeout_create,
        error_create=error_create,
        timeout_insert=timeout_insert,
        error_insert=error_insert,
    )


##


def _normalize_insert_item(
    item: _InsertItem, /, *, snake: bool = False
) -> list[_NormalizedItem]:
    """Normalize an insertion item."""
    if _is_pair_of_str_mapping_and_table(item):
        mapping, table_or_orm = item
        adjusted = _map_mapping_to_table(mapping, table_or_orm, snake=snake)
        normalized = _NormalizedItem(mapping=adjusted, table=get_table(table_or_orm))
        return [normalized]
    if _is_pair_of_tuple_and_table(item):
        tuple_, table_or_orm = item
        mapping = _tuple_to_mapping(tuple_, table_or_orm)
        return _normalize_insert_item((mapping, table_or_orm), snake=snake)
    if _is_pair_of_sequence_of_tuple_or_string_mapping_and_table(item):
        items, table_or_orm = item
        pairs = [(i, table_or_orm) for i in items]
        normalized = (_normalize_insert_item(p, snake=snake) for p in pairs)
        return list(chain.from_iterable(normalized))
    if isinstance(item, DeclarativeBase):
        mapping = _orm_inst_to_dict(item)
        return _normalize_insert_item((mapping, item), snake=snake)
    try:
        _ = iter(item)
    except TypeError:
        raise _NormalizeInsertItemError(item=item) from None
    if all(map(_is_pair_of_tuple_or_str_mapping_and_table, item)):
        seq = cast("Sequence[_PairOfTupleOrStrMappingAndTable]", item)
        normalized = (_normalize_insert_item(p, snake=snake) for p in seq)
        return list(chain.from_iterable(normalized))
    if all(map(is_orm, item)):
        seq = cast("Sequence[DeclarativeBase]", item)
        normalized = (_normalize_insert_item(p, snake=snake) for p in seq)
        return list(chain.from_iterable(normalized))
    raise _NormalizeInsertItemError(item=item)


@dataclass(kw_only=True, slots=True)
class _NormalizeInsertItemError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


@dataclass(kw_only=True, slots=True)
class _NormalizedItem:
    mapping: StrMapping
    table: Table


def _normalize_upsert_item(
    item: _InsertItem,
    /,
    *,
    snake: bool = False,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> Iterator[_NormalizedItem]:
    """Normalize an upsert item."""
    normalized = _normalize_insert_item(item, snake=snake)
    match selected_or_all:
        case "selected":
            for norm in normalized:
                values = {k: v for k, v in norm.mapping.items() if v is not None}
                yield _NormalizedItem(mapping=values, table=norm.table)
        case "all":
            yield from normalized
        case never:
            assert_never(never)


##


def selectable_to_string(
    selectable: Selectable[Any], engine_or_conn: EngineOrConnectionOrAsync, /
) -> str:
    """Convert a selectable into a string."""
    com = selectable.compile(
        dialect=engine_or_conn.dialect, compile_kwargs={"literal_binds": True}
    )
    return str(com)


##


class TablenameMixin:
    """Mix-in for an auto-generated tablename."""

    @cast("Any", declared_attr)
    def __tablename__(cls) -> str:  # noqa: N805
        return snake_case(get_class_name(cls))


##


type _SelectedOrAll = Literal["selected", "all"]


async def upsert_items(
    engine: AsyncEngine,
    /,
    *items: _InsertItem,
    snake: bool = False,
    selected_or_all: _SelectedOrAll = "selected",
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    timeout_create: Delta | None = None,
    error_create: type[Exception] = TimeoutError,
    timeout_insert: Delta | None = None,
    error_insert: type[Exception] = TimeoutError,
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

    def build_insert(
        table: Table, values: Iterable[StrMapping], /
    ) -> tuple[Insert, None]:
        ups = _upsert_items_build(
            engine, table, values, selected_or_all=selected_or_all
        )
        return ups, None

    try:
        prepared = _prepare_insert_or_upsert_items(
            partial(
                _normalize_upsert_item, snake=snake, selected_or_all=selected_or_all
            ),
            engine,
            build_insert,
            *items,
            chunk_size_frac=chunk_size_frac,
        )
    except _PrepareInsertOrUpsertItemsError as error:
        raise UpsertItemsError(item=error.item) from None
    if not assume_tables_exist:
        await ensure_tables_created(
            engine, *prepared.tables, timeout=timeout_create, error=error_create
        )
    for ups, _ in prepared.yield_pairs():
        async with yield_connection(
            engine, timeout=timeout_insert, error=error_insert
        ) as conn:
            _ = await conn.execute(ups)


def _upsert_items_build(
    engine: AsyncEngine,
    table: Table,
    values: Iterable[StrMapping],
    /,
    *,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> Insert:
    values = list(values)
    keys = merge_sets(*values)
    dict_nones = dict.fromkeys(keys)
    values = [{**dict_nones, **v} for v in values]
    match _get_dialect(engine):
        case "postgresql":  # skipif-ci-and-not-linux
            insert = postgresql_insert
        case "sqlite":
            insert = sqlite_insert
        case "mssql" | "mysql" | "oracle" as dialect:  # pragma: no cover
            raise NotImplementedError(dialect)
        case never:
            assert_never(never)
    ins = insert(table).values(values)
    primary_key = cast("Any", table.primary_key)
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
            columns = merge_sets(*values)
        case "all":
            columns = {c.name for c in insert.excluded}
        case never:
            assert_never(never)
    set_ = {c: getattr(insert.excluded, c) for c in columns}
    match insert:
        case postgresql_Insert():  # skipif-ci
            return insert.on_conflict_do_update(constraint=primary_key, set_=set_)
        case sqlite_Insert():
            return insert.on_conflict_do_update(index_elements=primary_key, set_=set_)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class UpsertItemsError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


##


@asynccontextmanager
async def yield_connection(
    engine: AsyncEngine,
    /,
    *,
    timeout: Delta | None = None,
    error: MaybeType[BaseException] = TimeoutError,
) -> AsyncIterator[AsyncConnection]:
    """Yield an async connection."""
    try:
        async with timeout_td(timeout, error=error), engine.begin() as conn:
            yield conn
    except GeneratorExit:  # pragma: no cover
        from utilities.pytest import is_pytest

        if not is_pytest():
            raise
        return


##


def yield_primary_key_columns(
    obj: TableOrORMInstOrClass,
    /,
    *,
    autoincrement: bool | Literal["auto", "ignore_fk"] | None = None,
) -> Iterator[Column]:
    """Yield the primary key columns of a table."""
    table = get_table(obj)
    for column in table.primary_key:
        if (autoincrement is None) or (autoincrement == column.autoincrement):
            yield column


##


def _ensure_tables_maybe_reraise(error: DatabaseError, match: str, /) -> None:
    """Re-raise the error if it does not match the required statement."""
    if not search(match, ensure_str(one(error.args))):
        raise error  # pragma: no cover


##


def _get_dialect(engine_or_conn: EngineOrConnectionOrAsync, /) -> Dialect:
    """Get the dialect of a database."""
    dialect = engine_or_conn.dialect
    if isinstance(dialect, mssql_dialect):  # pragma: no cover
        return "mssql"
    if isinstance(dialect, mysql_dialect):  # pragma: no cover
        return "mysql"
    if isinstance(dialect, oracle_dialect):  # pragma: no cover
        return "oracle"
    if isinstance(  # skipif-ci-and-not-linux
        dialect, (postgresql_dialect, PGDialect_asyncpg, PGDialect_psycopg)
    ):
        return "postgresql"
    if isinstance(dialect, sqlite_dialect):
        return "sqlite"
    msg = f"Unknown dialect: {dialect}"  # pragma: no cover
    raise NotImplementedError(msg)  # pragma: no cover


##


def _get_dialect_max_params(
    dialect_or_engine_or_conn: DialectOrEngineOrConnectionOrAsync, /
) -> int:
    """Get the max number of parameters of a dialect."""
    match dialect_or_engine_or_conn:
        case "mssql":  # pragma: no cover
            return 2100
        case "mysql":  # pragma: no cover
            return 65535
        case "oracle":  # pragma: no cover
            return 1000
        case "postgresql":  # skipif-ci-and-not-linux
            return 32767
        case "sqlite":
            return 100
        case (
            Engine()
            | Connection()
            | AsyncEngine()
            | AsyncConnection() as engine_or_conn
        ):
            dialect = _get_dialect(engine_or_conn)
            return _get_dialect_max_params(dialect)
        case never:
            assert_never(never)


##


def _is_pair_of_sequence_of_tuple_or_string_mapping_and_table(
    obj: Any, /
) -> TypeGuard[_PairOfSequenceOfTupleOrStrMappingAndTable]:
    """Check if an object is a pair of a sequence of tuples/string mappings and a table."""
    return _is_pair_with_predicate_and_table(obj, is_sequence_of_tuple_or_str_mapping)


def _is_pair_of_str_mapping_and_table(
    obj: Any, /
) -> TypeGuard[_PairOfStrMappingAndTable]:
    """Check if an object is a pair of a string mapping and a table."""
    return _is_pair_with_predicate_and_table(obj, is_string_mapping)


def _is_pair_of_tuple_and_table(obj: Any, /) -> TypeGuard[_PairOfTupleAndTable]:
    """Check if an object is a pair of a tuple and a table."""
    return _is_pair_with_predicate_and_table(obj, is_tuple)


def _is_pair_of_tuple_or_str_mapping_and_table(
    obj: Any, /
) -> TypeGuard[_PairOfTupleOrStrMappingAndTable]:
    """Check if an object is a pair of a tuple/string mapping and a table."""
    return _is_pair_with_predicate_and_table(obj, is_tuple_or_str_mapping)


def _is_pair_with_predicate_and_table[T](
    obj: Any, predicate: Callable[[Any], TypeGuard[T]], /
) -> TypeGuard[tuple[T, TableOrORMInstOrClass]]:
    """Check if an object is pair and a table."""
    return (
        isinstance(obj, tuple)
        and (len(obj) == 2)
        and predicate(obj[0])
        and is_table_or_orm(obj[1])
    )


##


def _map_mapping_to_table(
    mapping: StrMapping, table_or_orm: TableOrORMInstOrClass, /, *, snake: bool = False
) -> StrMapping:
    """Map a mapping to a table."""
    columns = get_column_names(table_or_orm)
    if not snake:
        try:
            check_subset(mapping, columns)
        except CheckSubSetError as error:
            raise _MapMappingToTableExtraColumnsError(
                mapping=mapping, columns=columns, extra=error.extra
            ) from None
        return {k: v for k, v in mapping.items() if k in columns}
    out: dict[str, Any] = {}
    for key, value in mapping.items():
        try:
            col = one(c for c in columns if snake_case(c) == snake_case(key))
        except OneEmptyError:
            raise _MapMappingToTableSnakeMapEmptyError(
                mapping=mapping, columns=columns, key=key
            ) from None
        except OneNonUniqueError as error:
            raise _MapMappingToTableSnakeMapNonUniqueError(
                mapping=mapping,
                columns=columns,
                key=key,
                first=error.first,
                second=error.second,
            ) from None
        else:
            out[col] = value
    return out


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableError(Exception):
    mapping: StrMapping
    columns: Sequence[str]


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableExtraColumnsError(_MapMappingToTableError):
    extra: AbstractSet[str]

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.mapping)} must be a subset of table columns {get_repr(self.columns)}; got extra {self.extra}"


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableSnakeMapEmptyError(_MapMappingToTableError):
    key: str

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.mapping)} must be a subset of table columns {get_repr(self.columns)}; cannot find column to map to {self.key!r} modulo snake casing"


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableSnakeMapNonUniqueError(_MapMappingToTableError):
    key: str
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.mapping)} must be a subset of table columns {get_repr(self.columns)}; found columns {self.first!r}, {self.second!r} and perhaps more to map to {self.key!r} modulo snake casing"


##


def _orm_inst_to_dict(obj: DeclarativeBase, /) -> StrMapping:
    """Map an ORM instance to a dictionary."""
    attrs = {
        k for k, _ in yield_object_attributes(obj, static_type=InstrumentedAttribute)
    }
    return {
        name: _orm_inst_to_dict_one(obj, attrs, name) for name in get_column_names(obj)
    }


def _orm_inst_to_dict_one(
    obj: DeclarativeBase, attrs: AbstractSet[str], name: str, /
) -> Any:
    attr = one(
        attr for attr in attrs if _orm_inst_to_dict_predicate(type(obj), attr, name)
    )
    return getattr(obj, attr)


def _orm_inst_to_dict_predicate(
    cls: type[DeclarativeBase], attr: str, name: str, /
) -> bool:
    cls_attr = getattr(cls, attr)
    try:
        return cls_attr.name == name
    except AttributeError:
        return False


##


@dataclass(kw_only=True, slots=True)
class _PrepareInsertOrUpsertItems:
    mapping: dict[Table, list[StrMapping]] = field(default_factory=dict)
    yield_pairs: Callable[[], Iterator[tuple[Insert, Any]]]

    @property
    def tables(self) -> Sequence[Table]:
        return list(self.mapping)


def _prepare_insert_or_upsert_items(
    normalize_item: Callable[[_InsertItem], Iterable[_NormalizedItem]],
    engine: AsyncEngine,
    build_insert: Callable[[Table, Iterable[StrMapping]], tuple[Insert, Any]],
    /,
    *items: Any,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> _PrepareInsertOrUpsertItems:
    """Prepare a set of insert/upsert items."""
    mapping: defaultdict[Table, list[StrMapping]] = defaultdict(list)
    lengths: set[int] = set()
    try:
        for item in items:
            for normed in normalize_item(item):
                mapping[normed.table].append(normed.mapping)
                lengths.add(len(normed.mapping))
    except _NormalizeInsertItemError as error:
        raise _PrepareInsertOrUpsertItemsError(item=error.item) from None
    merged: dict[Table, list[StrMapping]] = {
        table: _prepare_insert_or_upsert_items_merge_items(table, values)
        for table, values in mapping.items()
    }

    def yield_pairs() -> Iterator[tuple[Insert, None]]:
        for table, values in merged.items():
            chunk_size = get_chunk_size(engine, table, chunk_size_frac=chunk_size_frac)
            for chunk in chunked(values, chunk_size):
                yield build_insert(table, chunk)

    return _PrepareInsertOrUpsertItems(mapping=mapping, yield_pairs=yield_pairs)


@dataclass(kw_only=True, slots=True)
class _PrepareInsertOrUpsertItemsError(Exception):
    item: Any

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


def _prepare_insert_or_upsert_items_merge_items(
    table: Table, items: Iterable[StrMapping], /
) -> list[StrMapping]:
    columns = list(yield_primary_key_columns(table))
    col_names = [c.name for c in columns]
    cols_auto = {c.name for c in columns if c.autoincrement in {True, "auto"}}
    cols_non_auto = set(col_names) - cols_auto
    mapping: defaultdict[tuple[Hashable, ...], list[StrMapping]] = defaultdict(list)
    unchanged: list[StrMapping] = []
    for item in items:
        check_subset(cols_non_auto, item)
        has_all_auto = set(cols_auto).issubset(item)
        if has_all_auto:
            pkey = tuple(item[k] for k in col_names)
            rest: StrMapping = {k: v for k, v in item.items() if k not in col_names}
            mapping[pkey].append(rest)
        else:
            unchanged.append(item)
    merged = {k: merge_str_mappings(*v) for k, v in mapping.items()}
    return [
        dict(zip(col_names, k, strict=True)) | dict(v) for k, v in merged.items()
    ] + unchanged


##


def _tuple_to_mapping(
    values: tuple[Any, ...], table_or_orm: TableOrORMInstOrClass, /
) -> dict[str, Any]:
    columns = get_column_names(table_or_orm)
    mapping = dict(zip(columns, tuple(values), strict=False))
    return {k: v for k, v in mapping.items() if v is not None}


__all__ = [
    "CHUNK_SIZE_FRAC",
    "CheckEngineError",
    "DialectOrEngineOrConnectionOrAsync",
    "EngineOrConnectionOrAsync",
    "ExtractURLError",
    "ExtractURLOutput",
    "GetTableError",
    "InsertItemsError",
    "TablenameMixin",
    "UpsertItemsError",
    "check_connect",
    "check_connect_async",
    "check_engine",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "ensure_database_created",
    "ensure_database_dropped",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "enum_name",
    "enum_values",
    "extract_url",
    "get_chunk_size",
    "get_column_names",
    "get_columns",
    "get_primary_key_values",
    "get_table",
    "get_table_name",
    "hash_primary_key_values",
    "insert_items",
    "is_orm",
    "is_table_or_orm",
    "migrate_data",
    "selectable_to_string",
    "upsert_items",
    "yield_connection",
    "yield_primary_key_columns",
]
