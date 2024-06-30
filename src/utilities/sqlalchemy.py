from __future__ import annotations

import enum
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import auto
from functools import reduce
from itertools import chain
from math import floor
from operator import ge, itemgetter, le
from re import search
from typing import TYPE_CHECKING, Any, Literal, TypeGuard, cast, overload

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
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import ArgumentError, DatabaseError
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
from typing_extensions import assert_never, override

from utilities.datetime import get_now
from utilities.errors import redirect_error
from utilities.iterables import (
    CheckLengthError,
    OneEmptyError,
    check_length,
    chunked,
    is_iterable_not_str,
    one,
)
from utilities.text import ensure_str
from utilities.types import IterableStrs, get_class_name

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy.sql.base import ReadOnlyColumnCollection

    from utilities.math import FloatFinNonNeg, IntNonNeg

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
    engine: Engine,
    /,
    *,
    num_tables: IntNonNeg | tuple[IntNonNeg, FloatFinNonNeg] | None = None,
) -> None:
    """Check that an engine can connect.

    Optionally query for the number of tables, or the number of columns in
    such a table.
    """
    match get_dialect(engine):
        case Dialect.mssql | Dialect.mysql | Dialect.postgresql:  # pragma: no cover
            query = "select * from information_schema.tables"
        case Dialect.oracle:  # pragma: no cover
            query = "select * from all_objects"
        case Dialect.sqlite:
            query = "select * from sqlite_master where type='table'"
        case _ as never:  # type: ignore[]
            assert_never(never)
    statement = text(query)
    with engine.begin() as conn:
        rows = conn.execute(statement).all()
    if num_tables is not None:
        with redirect_error(
            CheckLengthError, CheckEngineError(f"{engine=}, {num_tables=}")
        ):
            check_length(rows, equal_or_approx=num_tables)


class CheckEngineError(Exception): ...


def check_table_against_reflection(
    table_or_mapped_class: Table | type[Any],
    engine: Engine,
    /,
    *,
    schema: str | None = None,
    snake_table: bool = False,
    snake_columns: bool = False,
    allow_permutations_columns: bool = False,
    primary_key: bool = True,
) -> None:
    """Check that a table equals its reflection."""
    reflected = reflect_table(table_or_mapped_class, engine, schema=schema)
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


def create_engine(
    drivername: str,
    /,
    *,
    username: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    query: Mapping[str, IterableStrs | str] | None = None,
    poolclass: type[Pool] | None = NullPool,
) -> Engine:
    """Create a SQLAlchemy engine."""
    if query is None:
        kwargs = {}
    else:

        def func(x: str | IterableStrs, /) -> list[str] | str:
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
            case Dialect.postgresql:  # pragma: no cover
                return 32767
            case Dialect.sqlite:
                return 100
            case _ as never:  # type: ignore[]
                assert_never(never)


def ensure_engine(engine: Engine | str, /) -> Engine:
    """Ensure the object is an Engine."""
    if isinstance(engine, Engine):
        return engine
    return parse_engine(engine)


def ensure_tables_created(
    engine: Engine, /, *tables_or_mapped_classes: Table | type[Any]
) -> None:
    """Ensure a table/set of tables is/are created."""
    match dialect := get_dialect(engine):
        case Dialect.mysql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.postgresql:  # pragma: no cover
            match = "relation .* already exists"
        case Dialect.mssql:  # pragma: no cover
            match = "There is already an object named .* in the database"
        case Dialect.oracle:  # pragma: no cover
            match = "ORA-00955: name is already used by an existing object"
        case Dialect.sqlite:
            match = "table .* already exists"
        case _ as never:  # type: ignore[]
            assert_never(never)

    for table_or_mapped_class in tables_or_mapped_classes:
        table = get_table(table_or_mapped_class)
        with engine.begin() as conn:
            try:
                table.create(conn)
            except DatabaseError as error:
                if not search(match, ensure_str(one(error.args))):
                    raise  # pragma: no cover


def ensure_tables_dropped(
    engine: Engine, *tables_or_mapped_classes: Table | type[Any]
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    match = get_table_does_not_exist_message(engine)
    for table_or_mapped_class in tables_or_mapped_classes:
        table = get_table(table_or_mapped_class)
        with engine.begin() as conn:
            try:
                table.drop(conn)
            except DatabaseError as error:
                if not search(match, ensure_str(one(error.args))):
                    raise  # pragma: no cover


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
    if isinstance(dialect, mssql_dialect):  # pragma: no cover
        return Dialect.mssql
    if isinstance(dialect, mysql_dialect):  # pragma: no cover
        return Dialect.mysql
    if isinstance(dialect, oracle_dialect):  # pragma: no cover
        return Dialect.oracle
    if isinstance(dialect, postgresql_dialect):  # pragma: no cover
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


def get_table_does_not_exist_message(engine: Engine, /) -> str:
    """Get the message for a non-existent table."""
    match dialect := get_dialect(engine):
        case Dialect.mysql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.postgresql:  # pragma: no cover
            return "table .* does not exist"
        case Dialect.mssql:  # pragma: no cover
            return (
                "Cannot drop the table .*, because it does not exist or you do "
                "not have permission"
            )
        case Dialect.oracle:  # pragma: no cover
            return "ORA-00942: table or view does not exist"
        case Dialect.sqlite:
            return "no such table"
        case _ as never:  # type: ignore[]
            assert_never(never)


def get_table_updated_column(
    table_or_mapped_class: Table | type[Any], /, *, pattern: str = "updated"
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


def get_table_name(table_or_mapped_class: Table | type[Any], /) -> str:
    """Get the table name from a Table or mapped class."""
    return get_table(table_or_mapped_class).name


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
        values = item.values  # noqa: PD011
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
            for chunk in chunked(values, chunk_size):
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


class _InsertItemsCollectError(Exception): ...


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


class _InsertItemsCollectIterableError(Exception): ...


def _insert_items_collect_valid(obj: Any, /) -> TypeGuard[_InsertItemValues]:
    """Check if an item being collected is valid."""
    return isinstance(obj, tuple) or (
        isinstance(obj, dict) and all(isinstance(key, str) for key in obj)
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


def parse_engine(engine: str, /) -> Engine:
    """Parse a string into an Engine."""
    with redirect_error(ArgumentError, ParseEngineError(f"{engine=}")):
        return _create_engine(engine, poolclass=NullPool)


class ParseEngineError(Exception): ...


@overload
def postgres_upsert(
    item: Table | type[DeclarativeBase],
    /,
    *,
    values: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    selected_or_all: Literal["selected", "all"] = ...,
) -> Insert: ...
@overload
def postgres_upsert(
    item: DeclarativeBase | Sequence[DeclarativeBase],
    /,
    *,
    values: None = None,
    selected_or_all: Literal["selected", "all"] = ...,
) -> Insert: ...
def postgres_upsert(  # pragma: ci-in-environ
    item: Any,
    /,
    *,
    values: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> Insert:
    """Upsert statement for a database (postgres only).

    These can be:

    - tuple[Table, Mapping[str, Any]]
    - tuple[Table, Sequence[Mapping[str, Any]]]
    - Model
    - Sequence[Model]
    """
    if (
        isinstance(item, Table)
        or (isinstance(item, type) and issubclass(item, DeclarativeBase))
    ) and (values is not None):
        return _postgres_upsert_core(item, values, selected_or_all=selected_or_all)
    if is_mapped_class(item) and (values is None):
        table = get_table(item)
        mappings = [mapped_class_to_dict(item)]
    elif (
        is_iterable_not_str(item)
        and all(map(is_mapped_class, item))
        and (values is None)
    ):
        table = one(set(map(get_table, item)))
        mappings = map(mapped_class_to_dict, item)
    else:
        raise PostgresUpsertError(item=item, values=values)
    mappings2 = [{k: v for k, v in m.items() if v is not None} for m in mappings]
    return _postgres_upsert_core(table, mappings2, selected_or_all=selected_or_all)


def _postgres_upsert_core(  # pragma: ci-in-environ
    table_or_mapped_class: Table | type[DeclarativeBase],
    values: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    /,
    *,
    selected_or_all: Literal["selected", "all"] = "selected",
) -> Insert:
    table = get_table(table_or_mapped_class)
    if (updated_col := get_table_updated_column(table)) is not None:
        updated_mapping = {updated_col: get_now()}
        values = _postgres_upsert_add_updated(values, updated_mapping)
    constraint = cast(Any, table.primary_key)
    ins = postgresql_insert(table).values(values)
    if selected_or_all == "selected":
        if isinstance(values, Mapping):
            columns = set(values)
        else:
            all_columns = set(map(frozenset, values))
            columns = one(all_columns)
    elif selected_or_all == "all":
        columns = {c.name for c in ins.excluded}
    else:
        assert_never(selected_or_all)
    set_ = {c: getattr(ins.excluded, c) for c in columns}
    return ins.on_conflict_do_update(constraint=constraint, set_=set_)


def _postgres_upsert_add_updated(  # pragma: ci-in-environ
    values: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    updated: Mapping[str, dt.datetime],
    /,
) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
    if isinstance(values, Mapping):
        return _postgres_upsert_add_updated_to_mapping(values, updated)
    return [_postgres_upsert_add_updated_to_mapping(v, updated) for v in values]


def _postgres_upsert_add_updated_to_mapping(  # pragma: ci-in-environ
    value: Mapping[str, Any], updated_at: Mapping[str, dt.datetime], /
) -> Mapping[str, Any]:
    return {**value, **updated_at}


@dataclass(kw_only=True)
class PostgresUpsertError(Exception):
    item: Any
    values: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None

    @override
    def __str__(self) -> str:  # pragma: ci-in-environ
        return f"Unsupported item and values; got {self.item} and {self.values}"


def reflect_table(
    table_or_mapped_class: Table | type[Any],
    engine: Engine,
    /,
    *,
    schema: str | None = None,
) -> Table:
    """Reflect a table from a database."""
    name = get_table_name(table_or_mapped_class)
    metadata = MetaData(schema=schema)
    with engine.begin() as conn:
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


__all__ = [
    "CHUNK_SIZE_FRAC",
    "CheckEngineError",
    "Dialect",
    "GetDialectError",
    "GetTableError",
    "ParseEngineError",
    "PostgresUpsertError",
    "TablenameMixin",
    "check_engine",
    "check_table_against_reflection",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "ensure_engine",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "get_chunk_size",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table",
    "get_table_does_not_exist_message",
    "get_table_name",
    "get_table_updated_column",
    "insert_items",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "parse_engine",
    "postgres_upsert",
    "serialize_engine",
]
