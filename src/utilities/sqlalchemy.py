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
from dataclasses import dataclass
from functools import reduce
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
from rich.pretty import pretty_repr
from sqlalchemy import (
    URL,
    Column,
    Connection,
    Engine,
    Insert,
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
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
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

import utilities.asyncio
from utilities.core import (
    OneEmptyError,
    OneNonUniqueError,
    chunked,
    get_class_name,
    is_pytest,
    normalize_multi_line_str,
    one,
    snake_case,
)
from utilities.functions import ensure_str, yield_object_attributes
from utilities.iterables import (
    CheckLengthError,
    CheckSubSetError,
    check_length,
    check_subset,
    merge_sets,
    merge_str_mappings,
)
from utilities.text import secret_str
from utilities.types import (
    Duration,
    MaybeIterable,
    MaybeType,
    StrDict,
    StrMapping,
    TupleOrStrMapping,
)
from utilities.typing import (
    is_sequence_of_tuple_or_str_mapping,
    is_str_mapping,
    is_tuple,
    is_tuple_or_str_mapping,
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
    except (gaierror, ConnectionRefusedError, DatabaseError):  # pragma: no cover
        return False


async def check_connect_async(
    engine: AsyncEngine,
    /,
    *,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
) -> bool:
    """Check if an engine can connect."""
    try:
        async with (
            utilities.asyncio.timeout(timeout, error=error),
            engine.connect() as conn,
        ):
            return bool((await conn.execute(_SELECT)).scalar_one())
    except (gaierror, ConnectionRefusedError, DatabaseError, TimeoutError):
        return False


##


async def check_engine(
    engine: AsyncEngine,
    /,
    *,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
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
        return f"{pretty_repr(self.engine)} must have {self.expected} table(s); got {len(self.rows)}"


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
        except DatabaseError as error:
            _ensure_tables_maybe_reraise(error, 'database ".*" already exists')


async def ensure_database_dropped(super_: URL, database: str, /) -> None:
    """Ensure a database is dropped."""
    engine = create_async_engine(super_, isolation_level="AUTOCOMMIT")
    async with engine.begin() as conn:
        _ = await conn.execute(text(f"DROP DATABASE IF EXISTS {database}"))


async def ensure_database_users_disconnected(super_: URL, database: str, /) -> None:
    """Ensure a databases' users are disconnected."""
    engine = create_async_engine(super_, isolation_level="AUTOCOMMIT")
    match dialect := _get_dialect(engine):
        case "postgresql":  # skipif-ci-and-not-linux
            query = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = {database!r} AND pid <> pg_backend_pid()"  # noqa: S608
        case "mssql" | "mysql" | "oracle" | "sqlite":  # pragma: no cover
            raise NotImplementedError(dialect)
        case never:
            assert_never(never)
    async with engine.begin() as conn:
        _ = await conn.execute(text(query))


##


async def ensure_tables_created(
    engine: AsyncEngine,
    /,
    *tables_or_orms: TableOrORMInstOrClass,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
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
type _NormalizedItem = tuple[Table, StrMapping]
type _InsertPair = tuple[Table, Sequence[StrMapping]]
type _InsertTriple = tuple[Table, Insert, Sequence[StrMapping] | None]


async def insert_items(
    engine: AsyncEngine,
    *items: _InsertItem,
    snake: bool = False,
    is_upsert: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    timeout_create: Duration | None = None,
    error_create: MaybeType[BaseException] = TimeoutError,
    timeout_insert: Duration | None = None,
    error_insert: MaybeType[BaseException] = TimeoutError,
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
    normalized = chain.from_iterable(
        _insert_items_yield_normalized(i, snake=snake) for i in items
    )
    triples = _insert_items_yield_triples(
        engine, normalized, is_upsert=is_upsert, chunk_size_frac=chunk_size_frac
    )
    if not assume_tables_exist:
        triples = list(triples)
        tables = {table for table, _, _ in triples}
        await ensure_tables_created(
            engine, *tables, timeout=timeout_create, error=error_create
        )
    for _, ins, parameters in triples:
        async with yield_connection(
            engine, timeout=timeout_insert, error=error_insert
        ) as conn:
            _ = await conn.execute(ins, parameters=parameters)


def _insert_items_yield_normalized(
    item: _InsertItem, /, *, snake: bool = False
) -> Iterator[_NormalizedItem]:
    if _is_pair_of_str_mapping_and_table(item):
        mapping, table_or_orm = item
        adjusted = _map_mapping_to_table(mapping, table_or_orm, snake=snake)
        yield (get_table(table_or_orm), adjusted)
        return
    if _is_pair_of_tuple_and_table(item):
        tuple_, table_or_orm = item
        mapping = _tuple_to_mapping(tuple_, table_or_orm)
        yield from _insert_items_yield_normalized((mapping, table_or_orm), snake=snake)
        return
    if _is_pair_of_sequence_of_tuple_or_string_mapping_and_table(item):
        items, table_or_orm = item
        pairs = [(i, table_or_orm) for i in items]
        for p in pairs:
            yield from _insert_items_yield_normalized(p, snake=snake)
        return
    if isinstance(item, DeclarativeBase):
        mapping = _orm_inst_to_dict(item)
        yield from _insert_items_yield_normalized((mapping, item), snake=snake)
        return
    try:
        _ = iter(item)
    except TypeError:
        raise InsertItemsError(item=item) from None
    if all(map(_is_pair_of_tuple_or_str_mapping_and_table, item)):
        pairs = cast("Sequence[_PairOfTupleOrStrMappingAndTable]", item)
        for p in pairs:
            yield from _insert_items_yield_normalized(p, snake=snake)
        return
    if all(map(is_orm, item)):
        classes = cast("Sequence[DeclarativeBase]", item)
        for c in classes:
            yield from _insert_items_yield_normalized(c, snake=snake)
        return
    raise InsertItemsError(item=item)


def _insert_items_yield_triples(
    engine: AsyncEngine,
    items: Iterable[_NormalizedItem],
    /,
    *,
    is_upsert: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> Iterable[_InsertTriple]:
    pairs = _insert_items_yield_chunked_pairs(
        engine, items, is_upsert=is_upsert, chunk_size_frac=chunk_size_frac
    )
    for table, mappings in pairs:
        match is_upsert, _get_dialect(engine):
            case False, "oracle":  # pragma: no cover
                ins = insert(table)
                parameters = mappings
            case False, _:
                ins = insert(table).values(mappings)
                parameters = None
            case True, _:
                ins = _insert_items_build_insert_with_on_conflict_do_update(
                    engine, table, mappings
                )
                parameters = None
            case never:
                assert_never(never)
        yield table, ins, parameters


def _insert_items_yield_chunked_pairs(
    engine: AsyncEngine,
    items: Iterable[_NormalizedItem],
    /,
    *,
    is_upsert: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> Iterable[_InsertPair]:
    for table, mappings in _insert_items_yield_raw_pairs(items, is_upsert=is_upsert):
        chunk_size = get_chunk_size(engine, table, chunk_size_frac=chunk_size_frac)
        for mappings_i in chunked(mappings, chunk_size):
            yield table, list(mappings_i)


def _insert_items_yield_raw_pairs(
    items: Iterable[_NormalizedItem], /, *, is_upsert: bool = False
) -> Iterable[_InsertPair]:
    by_table: defaultdict[Table, list[StrMapping]] = defaultdict(list)
    for table, mapping in items:
        by_table[table].append(mapping)
    for table, mappings in by_table.items():
        yield from _insert_items_yield_raw_pairs_one(
            table, mappings, is_upsert=is_upsert
        )


def _insert_items_yield_raw_pairs_one(
    table: Table, mappings: Iterable[StrMapping], /, *, is_upsert: bool = False
) -> Iterable[_InsertPair]:
    merged = _insert_items_yield_merged_mappings(table, mappings)
    match is_upsert:
        case True:
            by_keys: defaultdict[frozenset[str], list[StrMapping]] = defaultdict(list)
            for mapping in merged:
                non_null = {k: v for k, v in mapping.items() if v is not None}
                by_keys[frozenset(non_null)].append(non_null)
            for mappings_i in by_keys.values():
                yield table, mappings_i
        case False:
            yield table, list(merged)
        case never:
            assert_never(never)


def _insert_items_yield_merged_mappings(
    table: Table, mappings: Iterable[StrMapping], /
) -> Iterable[StrMapping]:
    columns = list(yield_primary_key_columns(table))
    col_names = [c.name for c in columns]
    cols_auto = {c.name for c in columns if c.autoincrement in {True, "auto"}}
    cols_non_auto = set(col_names) - cols_auto
    by_key: defaultdict[tuple[Hashable, ...], list[StrMapping]] = defaultdict(list)
    for mapping in mappings:
        check_subset(cols_non_auto, mapping)
        has_all_auto = set(cols_auto).issubset(mapping)
        if has_all_auto:
            pkey = tuple(mapping[k] for k in col_names)
            rest: StrMapping = {k: v for k, v in mapping.items() if k not in col_names}
            by_key[pkey].append(rest)
        else:
            yield mapping
    for k, v in by_key.items():
        head = dict(zip(col_names, k, strict=True))
        yield merge_str_mappings(head, *v)


def _insert_items_build_insert_with_on_conflict_do_update(
    engine: AsyncEngine, table: Table, mappings: Iterable[StrMapping], /
) -> Insert:
    primary_key = cast("Any", table.primary_key)
    mappings = list(mappings)
    columns = merge_sets(*mappings)
    match _get_dialect(engine):
        case "postgresql":  # skipif-ci-and-not-linux
            ins = postgresql_insert(table).values(mappings)
            set_ = {c: getattr(ins.excluded, c) for c in columns}
            return ins.on_conflict_do_update(constraint=primary_key, set_=set_)
        case "sqlite":
            ins = sqlite_insert(table).values(mappings)
            set_ = {c: getattr(ins.excluded, c) for c in columns}
            return ins.on_conflict_do_update(index_elements=primary_key, set_=set_)
        case "mssql" | "mysql" | "oracle" as dialect:  # pragma: no cover
            raise NotImplementedError(dialect)
        case never:
            assert_never(never)


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
    timeout_create: Duration | None = None,
    error_create: MaybeType[BaseException] = TimeoutError,
    timeout_insert: Duration | None = None,
    error_insert: MaybeType[BaseException] = TimeoutError,
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


def selectable_to_string(
    selectable: Selectable[Any], engine_or_conn: EngineOrConnectionOrAsync, /
) -> str:
    """Convert a selectable into a string."""
    com = selectable.compile(
        dialect=engine_or_conn.dialect, compile_kwargs={"literal_binds": True}
    )
    return normalize_multi_line_str(str(com))


##


class TablenameMixin:
    """Mix-in for an auto-generated tablename."""

    @cast("Any", declared_attr)
    def __tablename__(cls) -> str:  # noqa: N805
        return snake_case(get_class_name(cls))


##


@asynccontextmanager
async def yield_connection(
    engine: AsyncEngine,
    /,
    *,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
) -> AsyncIterator[AsyncConnection]:
    """Yield an async connection."""
    try:
        async with (
            utilities.asyncio.timeout(timeout, error=error),
            engine.begin() as conn,
        ):
            yield conn
    except GeneratorExit:  # pragma: no cover
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
    return _is_pair_with_predicate_and_table(obj, is_str_mapping)


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
    out: StrDict = {}
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
    columns: list[str]


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableExtraColumnsError(_MapMappingToTableError):
    extra: AbstractSet[str]

    @override
    def __str__(self) -> str:
        return f"Mapping {pretty_repr(self.mapping)} must be a subset of table columns {pretty_repr(self.columns)}; got extra {self.extra}"


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableSnakeMapEmptyError(_MapMappingToTableError):
    key: str

    @override
    def __str__(self) -> str:
        return f"Mapping {pretty_repr(self.mapping)} must be a subset of table columns {pretty_repr(self.columns)}; cannot find column to map to {self.key!r} modulo snake casing"


@dataclass(kw_only=True, slots=True)
class _MapMappingToTableSnakeMapNonUniqueError(_MapMappingToTableError):
    key: str
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return f"Mapping {pretty_repr(self.mapping)} must be a subset of table columns {pretty_repr(self.columns)}; found columns {self.first!r}, {self.second!r} and perhaps more to map to {self.key!r} modulo snake casing"


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


def _tuple_to_mapping(
    values: tuple[Any, ...], table_or_orm: TableOrORMInstOrClass, /
) -> StrDict:
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
    "check_connect",
    "check_connect_async",
    "check_engine",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "ensure_database_created",
    "ensure_database_dropped",
    "ensure_database_users_disconnected",
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
    "yield_connection",
    "yield_primary_key_columns",
]
