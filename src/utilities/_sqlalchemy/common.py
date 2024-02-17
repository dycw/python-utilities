from __future__ import annotations

import enum
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from enum import auto
from itertools import chain
from math import floor
from typing import Any, TypeGuard, cast

import sqlalchemy
from more_itertools import chunked
from sqlalchemy import Column, Connection, Engine, Table, insert
from sqlalchemy.dialects.mssql import dialect as mssql_dialect
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import ArgumentError, DatabaseError
from sqlalchemy.orm import InstrumentedAttribute, class_mapper
from sqlalchemy.orm.exc import UnmappedClassError
from typing_extensions import assert_never, override

from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.more_itertools import one
from utilities.types import get_class_name

CHUNK_SIZE_FRAC = 0.95


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
            case Dialect.postgresql:  # pragma: no cover
                return 32767
            case Dialect.sqlite:
                return 100
            case _ as never:  # type: ignore
                assert_never(never)


def ensure_tables_created(
    engine: Engine, /, *tables_or_mapped_classes: Table | type[Any]
) -> None:
    """Ensure a table/set of tables is/are created."""

    class TableAlreadyExistsError(Exception):
        ...

    match dialect := get_dialect(engine):
        case Dialect.mysql | Dialect.postgresql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.mssql:  # pragma: no cover
            match = "There is already an object named .* in the database"
        case Dialect.oracle:  # pragma: no cover
            match = "ORA-00955: name is already used by an existing object"
        case Dialect.sqlite:
            match = "table .* already exists"
        case _ as never:  # type: ignore
            assert_never(never)

    for table_or_mapped_class in tables_or_mapped_classes:
        table = get_table(table_or_mapped_class)
        with suppress(TableAlreadyExistsError), redirect_error(
            DatabaseError, TableAlreadyExistsError, match=match
        ), engine.begin() as conn:
            table.create(conn)


def get_chunk_size(
    engine_or_conn: Engine | Connection,
    /,
    *,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
    scaling: float = 1.0,
) -> int:
    """Get the maximum chunk size for an engine."""

    dialect = get_dialect(engine_or_conn)
    max_params = dialect.max_params
    return max(floor(chunk_size_frac * max_params / scaling), 1)


def get_column_names(table_or_mapped_class: Table | type[Any], /) -> list[str]:
    """Get the column names from a table or model."""
    return [col.name for col in get_columns(table_or_mapped_class)]


def get_columns(table_or_mapped_class: Table | type[Any], /) -> list[Column[Any]]:
    """Get the columns from a table or model."""
    return list(get_table(table_or_mapped_class).columns)


def get_dialect(engine_or_conn: Engine | Connection, /) -> Dialect:
    """Get the dialect of a database."""
    dialect = engine_or_conn.dialect
    if isinstance(dialect, mssql_dialect):  # pragma: os-ne-linux
        return Dialect.mssql
    if isinstance(dialect, mysql_dialect):  # pragma: os-ne-linux
        return Dialect.mysql
    if isinstance(dialect, oracle_dialect):
        return Dialect.oracle
    if isinstance(dialect, postgresql_dialect):  # pragma: os-ne-linux
        return Dialect.postgresql
    if isinstance(dialect, sqlite_dialect):
        return Dialect.sqlite
    raise GetDialectError(dialect=dialect)  # pragma: no cover


@dataclass(frozen=True, kw_only=True, slots=True)
class GetDialectError(Exception):
    dialect: sqlalchemy.Dialect

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            "Dialect must be one of MS SQL, MySQL, Oracle, PostgreSQL or SQLite; got {} instead".format(
                self.dialect
            )
        )


def get_table(obj: Table | type[Any], /) -> Table:
    """Get the table from a Table or mapped class."""
    if isinstance(obj, Table):
        return obj
    if is_mapped_class(obj):
        return cast(Any, obj).__table__
    raise GetTableError(obj=obj)


@dataclass(frozen=True, kw_only=True, slots=True)
class GetTableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return "Object {} must be a Table or mapped class; got {!r}".format(
            self.obj, get_class_name(self.obj)
        )


def insert_items(
    engine: Engine, *items: Any, chunk_size_frac: float = CHUNK_SIZE_FRAC
) -> None:
    """Insert a set of items into a database.

    These can be either a:
     - tuple[Any, ...], table
     - dict[str, Any], table
     - [tuple[Any ,...]], table
     - [dict[str, Any], table
     - Model
    """

    dialect = get_dialect(engine)
    to_insert: dict[Table, list[_InsertItemValues]] = defaultdict(list)
    lengths: set[int] = set()
    for item in chain(*map(_insert_items_collect, items)):
        values = item.values
        to_insert[item.table].append(values)
        lengths.add(len(values))
    max_length = max(lengths, default=1)
    chunk_size = get_chunk_size(
        engine, chunk_size_frac=chunk_size_frac, scaling=max_length
    )
    for table, values in to_insert.items():
        ensure_tables_created(engine, table)
        ins = insert(table)
        with engine.begin() as conn:
            for chunk in chunked(values, n=chunk_size):
                if dialect is Dialect.oracle:  # pragma: no cover
                    _ = conn.execute(ins, cast(Any, chunk))
                else:
                    _ = conn.execute(ins.values(list(chunk)))


_InsertItemValues = tuple[Any, ...] | dict[str, Any]


@dataclass
class _InsertionItem:
    values: _InsertItemValues
    table: Table


def _insert_items_collect(item: Any, /) -> Iterator[_InsertionItem]:
    """Collect the insertion items."""
    if isinstance(item, tuple):
        with redirect_error(ValueError, _InsertItemsCollectError(f"{item=}")):
            data, table_or_mapped_class = item
        if not is_table_or_mapped_class(table_or_mapped_class):
            msg = f"{table_or_mapped_class=}"
            raise _InsertItemsCollectError(msg)
        if _insert_items_collect_valid(data):
            yield _InsertionItem(values=data, table=get_table(table_or_mapped_class))
        elif is_iterable_not_str(data):
            yield from _insert_items_collect_iterable(data, table_or_mapped_class)
        else:
            msg = f"{data=}"
            raise _InsertItemsCollectError(msg)
    elif is_iterable_not_str(item):
        for i in item:
            yield from _insert_items_collect(i)
    elif is_mapped_class(cls := type(item)):
        yield _InsertionItem(values=mapped_class_to_dict(item), table=get_table(cls))
    else:
        msg = f"{item=}"
        raise _InsertItemsCollectError(msg)


class _InsertItemsCollectError(Exception):
    ...


def _insert_items_collect_iterable(
    obj: Iterable[Any], table_or_mapped_class: Table | type[Any], /
) -> Iterator[_InsertionItem]:
    """Collect the insertion items, for an iterable."""
    table = get_table(table_or_mapped_class)
    for datum in obj:
        if _insert_items_collect_valid(datum):
            yield _InsertionItem(values=datum, table=table)
        else:
            msg = f"{datum=}"
            raise _InsertItemsCollectIterableError(msg)


class _InsertItemsCollectIterableError(Exception):
    ...


def _insert_items_collect_valid(obj: Any, /) -> TypeGuard[_InsertItemValues]:
    """Check if an insertion item being collected is valid."""
    return isinstance(obj, tuple) or (
        isinstance(obj, dict) and all(isinstance(key, str) for key in obj)
    )


def is_mapped_class(obj: type[Any], /) -> bool:
    """Check if an object is a mapped class."""

    try:
        _ = class_mapper(cast(Any, obj))
    except (ArgumentError, UnmappedClassError):
        return False
    return True


def is_table_or_mapped_class(obj: Table | type[Any], /) -> bool:
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


__all__ = [
    "CHUNK_SIZE_FRAC",
    "Dialect",
    "GetDialectError",
    "GetTableError",
    "ensure_tables_created",
    "get_chunk_size",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table",
    "insert_items",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
]
