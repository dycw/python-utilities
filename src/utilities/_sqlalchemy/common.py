from __future__ import annotations

import enum
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import auto
from itertools import chain
from math import floor
from typing import Any, TypeGuard, TypeVar, cast

from more_itertools import chunked
from sqlalchemy import Column, Connection, Engine, Table, insert
from sqlalchemy.dialects.mssql import dialect as mssql_dialect
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import InstrumentedAttribute, class_mapper
from sqlalchemy.orm.exc import UnmappedClassError
from typing_extensions import assert_never

from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.more_itertools import one

_T = TypeVar("_T")
INSERT_ITEMS_CHUNK_SIZE_FRAC = 0.95


def chunk_for_engine(
    engine_or_conn: Engine | Connection,
    items: Iterable[_T],
    /,
    *,
    chunk_size_frac: float = INSERT_ITEMS_CHUNK_SIZE_FRAC,
    scaling: float = 1.0,
) -> Iterator[Iterable[_T]]:
    """Chunk a set of items for a given engine."""

    dialect = get_dialect(engine_or_conn)
    max_params = dialect.max_params
    chunk_size = floor(chunk_size_frac * max_params / scaling)
    return chunked(items, n=chunk_size)


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
            case _:  # pragma: no cover  # type: ignore
                assert_never(self)


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
    msg = f"{dialect=}"  # pragma: no cover
    raise GetDialectError(msg)  # pragma: no cover


class GetDialectError(Exception):
    ...


def get_table(table_or_mapped_class: Table | type[Any], /) -> Table:
    """Get the table from a Table or mapped class."""
    if isinstance(table_or_mapped_class, Table):
        return table_or_mapped_class
    if is_mapped_class(table_or_mapped_class):
        return cast(Any, table_or_mapped_class).__table__
    msg = f"{table_or_mapped_class=}"
    raise GetTableError(msg)


class GetTableError(Exception):
    ...


def insert_items(
    engine_or_conn: Engine | Connection,
    *items: Any,
    chunk_size_frac: float = INSERT_ITEMS_CHUNK_SIZE_FRAC,
) -> None:
    """Insert a set of items into a database.

    These can be either a:
     - tuple[Any, ...], table
     - dict[str, Any], table
     - [tuple[Any ,...]], table
     - [dict[str, Any], table
     - Model
    """

    dialect = get_dialect(engine_or_conn)
    to_insert: dict[Table, list[_InsertItemValues]] = defaultdict(list)
    lengths: set[int] = set()
    for item in chain(*map(_insert_items_collect, items)):
        values = item.values
        to_insert[item.table].append(values)
        lengths.add(len(values))
    max_length = max(lengths, default=1)
    with yield_connection(engine_or_conn) as conn:
        for table, values in to_insert.items():
            ins = insert(table)
            chunks = chunk_for_engine(
                engine_or_conn,
                values,
                chunk_size_frac=chunk_size_frac,
                scaling=max_length,
            )
            for chunk in chunks:
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
        _ = class_mapper(obj)
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


@contextmanager
def yield_connection(engine_or_conn: Engine | Connection, /) -> Iterator[Connection]:
    """Yield a connection."""
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield conn
    else:
        yield engine_or_conn


__all__ = [
    "Dialect",
    "GetDialectError",
    "GetTableError",
    "INSERT_ITEMS_CHUNK_SIZE_FRAC",
    "chunk_for_engine",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table",
    "insert_items",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "yield_connection",
]
