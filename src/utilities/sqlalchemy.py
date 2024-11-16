from __future__ import annotations

import reprlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence, Sized
from dataclasses import dataclass, field
from functools import partial, reduce
from math import floor
from operator import ge, le, or_
from re import search
from typing import Any, Literal, TypeGuard, assert_never, cast, overload

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
    text,
)
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
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    InstrumentedAttribute,
    class_mapper,
    declared_attr,
)
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.pool import NullPool, Pool
from typing_extensions import override

from utilities.asyncio import timeout_dur
from utilities.functions import get_class_name
from utilities.iterables import (
    CheckLengthError,
    MaybeIterable,
    always_iterable,
    check_length,
    chunked,
    is_iterable_not_str,
    one,
)
from utilities.text import ensure_str
from utilities.types import (
    Duration,
    StrMapping,
    TupleOrStrMapping,
    is_string_mapping,
    is_tuple_or_string_mapping,
)

_EngineOrConnectionOrAsync = Engine | Connection | AsyncEngine | AsyncConnection
Dialect = Literal["mssql", "mysql", "oracle", "postgresql", "sqlite"]
TableOrMappedClass = Table | type[DeclarativeBase]
CHUNK_SIZE_FRAC = 0.95


async def check_engine(
    engine: AsyncEngine,
    /,
    *,
    timeout: Duration | None = None,
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
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    statement = text(query)
    async with timeout_dur(duration=timeout), engine.begin() as conn:
        rows = (await conn.execute(statement)).all()
    if num_tables is not None:
        try:
            check_length(rows, equal_or_approx=num_tables)
        except CheckLengthError as error:
            raise CheckEngineError(
                engine=engine, rows=error.obj, expected=num_tables
            ) from None


@dataclass(kw_only=True, slots=True)
class CheckEngineError(Exception):
    engine: AsyncEngine
    rows: Sized
    expected: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        return f"{reprlib.repr(self.engine)} must have {self.expected} table(s); got {len(self.rows)}"


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


def create_async_engine(
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
) -> AsyncEngine:
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
    return _create_async_engine(url, poolclass=poolclass)


async def ensure_tables_created(
    engine: AsyncEngine,
    /,
    *tables_or_mapped_classes: TableOrMappedClass,
    timeout: Duration | None = None,
) -> None:
    """Ensure a table/set of tables is/are created."""
    tables = set(map(get_table, tables_or_mapped_classes))
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
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    for table in tables:
        async with timeout_dur(duration=timeout), engine.begin() as conn:
            try:
                await conn.run_sync(table.create)
            except DatabaseError as error:
                _ensure_tables_maybe_reraise(error, match)


async def ensure_tables_dropped(
    engine: AsyncEngine,
    *tables_or_mapped_classes: TableOrMappedClass,
    timeout: Duration | None = None,
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    tables = set(map(get_table, tables_or_mapped_classes))
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
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    for table in tables:
        async with timeout_dur(duration=timeout), engine.begin() as conn:
            try:
                await conn.run_sync(table.drop)
            except DatabaseError as error:
                _ensure_tables_maybe_reraise(error, match)


def get_chunk_size(
    engine_or_conn: _EngineOrConnectionOrAsync,
    /,
    *,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    scaling: float = 1.0,
) -> int:
    """Get the maximum chunk size for an engine."""
    max_params = _get_dialect_max_params(engine_or_conn)
    return max(floor(chunk_size_frac * max_params / scaling), 1)


def get_column_names(table_or_mapped_class: TableOrMappedClass, /) -> list[str]:
    """Get the column names from a table or model."""
    return [col.name for col in get_columns(table_or_mapped_class)]


def get_columns(table_or_mapped_class: TableOrMappedClass, /) -> list[Column[Any]]:
    """Get the columns from a table or model."""
    return list(get_table(table_or_mapped_class).columns)


def get_table(obj: TableOrMappedClass, /) -> Table:
    """Get the table from a Table or mapped class."""
    if isinstance(obj, Table):
        return obj
    if is_mapped_class(obj):
        return cast(Table, obj.__table__)
    raise GetTableError(obj=obj)


@dataclass(kw_only=True, slots=True)
class GetTableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be a Table or mapped class; got {get_class_name(self.obj)!r}"


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


async def insert_items(
    engine: AsyncEngine,
    *items: _InsertItem,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    timeout_create: Duration | None = None,
    timeout_insert: Duration | None = None,
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
        table: Table, values: Iterable[TupleOrStrMapping], /
    ) -> tuple[Insert, Any]:
        match _get_dialect(engine):
            case "oracle":  # pragma: no cover
                return insert(table), values
            case _:
                return insert(table).values(list(values)), None

    try:
        prepared = _prepare_insert_or_upsert_items(
            _normalize_insert_item,
            engine,
            build_insert,
            *items,
            chunk_size_frac=chunk_size_frac,
        )
    except _PrepareInsertOrUpsertItemsError as error:
        raise InsertItemsError(item=error.item) from None
    if not assume_tables_exist:
        await ensure_tables_created(engine, *prepared.tables, timeout=timeout_create)
    async with timeout_dur(duration=timeout_insert):
        for ins, parameters in prepared.yield_pairs():
            async with engine.begin() as conn:
                _ = await conn.execute(ins, parameters=parameters)


@dataclass(kw_only=True, slots=True)
class InsertItemsError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


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


@dataclass(kw_only=True, slots=True)
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

    if _is_insert_item_pair(item):
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

    if is_iterable_not_str(item) and all(_is_insert_item_pair(i) for i in item):
        item = cast(_ListOfPairOfTupleAndTable | _ListOfPairOfDictAndTable, item)
        for i in item:
            yield _NormalizedInsertItem(values=i[0], table=get_table(i[1]))
        return

    raise _NormalizeInsertItemError(item=item)


@dataclass(kw_only=True, slots=True)
class _NormalizeInsertItemError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


@dataclass(kw_only=True, slots=True)
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
    if _is_upsert_item_pair(item):
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

    if is_iterable_not_str(item) and all(_is_upsert_item_pair(i) for i in item):
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


@dataclass(kw_only=True, slots=True)
class _NormalizeUpsertItemError(Exception):
    item: _UpsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


def selectable_to_string(
    selectable: Selectable[Any], engine_or_conn: _EngineOrConnectionOrAsync, /
) -> str:
    """Convert a selectable into a string."""
    com = selectable.compile(
        dialect=engine_or_conn.dialect, compile_kwargs={"literal_binds": True}
    )
    return str(com)


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


async def upsert_items(
    engine: AsyncEngine,
    /,
    *items: _UpsertItem,
    selected_or_all: Literal["selected", "all"] = "selected",
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    assume_tables_exist: bool = False,
    timeout_create: Duration | None = None,
    timeout_insert: Duration | None = None,
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
            partial(_normalize_upsert_item, selected_or_all=selected_or_all),
            engine,
            build_insert,
            *items,
            chunk_size_frac=chunk_size_frac,
        )
    except _PrepareInsertOrUpsertItemsError as error:
        raise UpsertItemsError(item=error.item) from None
    if not assume_tables_exist:
        await ensure_tables_created(engine, *prepared.tables, timeout=timeout_create)
    async with timeout_dur(duration=timeout_insert):
        for ups, _ in prepared.yield_pairs():
            async with engine.begin() as conn:
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
    keys = set(reduce(or_, values))
    dict_nones = {k: None for k in keys}
    values = [{**dict_nones, **v} for v in values]
    match _get_dialect(engine):
        case "postgresql":  # skipif-ci-and-not-linux
            insert = postgresql_insert
        case "sqlite":
            insert = sqlite_insert
        case "mssql" | "mysql" | "oracle" as dialect:  # pragma: no cover
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


@dataclass(kw_only=True, slots=True)
class UpsertItemsError(Exception):
    item: _InsertItem

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


def yield_primary_key_columns(obj: TableOrMappedClass, /) -> Iterator[Column]:
    """Yield the primary key columns of a table."""
    table = get_table(obj)
    yield from table.primary_key


def _ensure_tables_maybe_reraise(error: DatabaseError, match: str, /) -> None:
    """Re-raise the error if it does not match the required statement."""
    if not search(match, ensure_str(one(error.args))):
        raise error  # pragma: no cover


def _get_dialect(engine_or_conn: _EngineOrConnectionOrAsync, /) -> Dialect:
    """Get the dialect of a database."""
    dialect = engine_or_conn.dialect
    if isinstance(dialect, mssql_dialect):  # pragma: no cover
        return "mssql"
    if isinstance(dialect, mysql_dialect):  # pragma: no cover
        return "mysql"
    if isinstance(dialect, oracle_dialect):  # pragma: no cover
        return "oracle"
    if isinstance(  # skipif-ci-and-not-linux
        dialect, postgresql_dialect | PGDialect_asyncpg
    ):
        return "postgresql"
    if isinstance(dialect, sqlite_dialect):
        return "sqlite"
    msg = f"Unknown dialect: {dialect}"  # pragma: no cover
    raise NotImplementedError(msg)  # pragma: no cover


def _get_dialect_max_params(
    dialect_or_engine_or_conn: Dialect | _EngineOrConnectionOrAsync, /
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
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _is_insert_item_pair(
    obj: Any, /
) -> TypeGuard[tuple[TupleOrStrMapping, TableOrMappedClass]]:
    """Check if an object is an insert-ready pair."""
    return _is_insert_or_upsert_pair(obj, is_tuple_or_string_mapping)


def _is_upsert_item_pair(
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


@dataclass(kw_only=True, slots=True)
class _PrepareInsertOrUpsertItems:
    mapping: dict[Table, list[Any]] = field(default_factory=dict)
    yield_pairs: Callable[[], Iterator[tuple[Insert, Any]]]

    @property
    def tables(self) -> Sequence[Table]:
        return list(self.mapping)


@overload
def _prepare_insert_or_upsert_items(
    normalize_item: Callable[[_InsertItem], Iterator[_NormalizedInsertItem]],
    engine: AsyncEngine,
    build_insert: Callable[[Table, Iterable[TupleOrStrMapping]], tuple[Insert, Any]],
    /,
    *items: _InsertItem,
    chunk_size_frac: float = ...,
) -> _PrepareInsertOrUpsertItems: ...
@overload
def _prepare_insert_or_upsert_items(
    normalize_item: Callable[[_UpsertItem], Iterator[_NormalizedUpsertItem]],
    engine: AsyncEngine,
    build_insert: Callable[[Table, Iterable[StrMapping]], tuple[Insert, Any]],
    /,
    *items: _UpsertItem,
    chunk_size_frac: float = ...,
) -> _PrepareInsertOrUpsertItems: ...
def _prepare_insert_or_upsert_items(
    normalize_item: Callable[[Any], Iterator[Any]],
    engine: AsyncEngine,
    build_insert: Callable[[Table, Iterable[Any]], tuple[Insert, Any]],
    /,
    *items: Any,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> _PrepareInsertOrUpsertItems:
    """Prepare a set of insert/upsert items."""
    mapping: dict[Table, list[Any]] = defaultdict(list)
    lengths: set[int] = set()
    try:
        for item in items:
            for normed in normalize_item(item):
                values = normed.values
                mapping[normed.table].append(values)
                lengths.add(len(values))
    except (_NormalizeInsertItemError, _NormalizeUpsertItemError) as error:
        raise _PrepareInsertOrUpsertItemsError(item=error.item) from None
    max_length = max(lengths, default=1)
    chunk_size = get_chunk_size(
        engine, chunk_size_frac=chunk_size_frac, scaling=max_length
    )

    def yield_pairs() -> Iterator[tuple[Insert, None]]:
        for table, values in mapping.items():
            for chunk in chunked(values, chunk_size):
                yield build_insert(table, chunk)

    return _PrepareInsertOrUpsertItems(mapping=mapping, yield_pairs=yield_pairs)


@dataclass(kw_only=True, slots=True)
class _PrepareInsertOrUpsertItemsError(Exception):
    item: Any

    @override
    def __str__(self) -> str:
        return f"Item must be valid; got {self.item}"


__all__ = [
    "CHUNK_SIZE_FRAC",
    "CheckEngineError",
    "GetTableError",
    "InsertItemsError",
    "TablenameMixin",
    "UpsertItemsError",
    "check_engine",
    "columnwise_max",
    "columnwise_min",
    "create_async_engine",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "get_chunk_size",
    "get_column_names",
    "get_columns",
    "get_table",
    "get_table_name",
    "insert_items",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "selectable_to_string",
    "upsert_items",
    "yield_primary_key_columns",
]
