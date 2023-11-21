from __future__ import annotations

import enum
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from enum import auto
from functools import reduce
from itertools import chain
from math import floor, isclose
from operator import ge, itemgetter, le
from typing import Any, NoReturn, cast

import sqlalchemy
from sqlalchemy import (
    URL,
    Boolean,
    Column,
    Connection,
    DateTime,
    Engine,
    Float,
    Interval,
    LargeBinary,
    MetaData,
    Numeric,
    Select,
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
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import (
    ArgumentError,
    DatabaseError,
    DuplicateColumnError,
    NoSuchTableError,
    OperationalError,
)
from sqlalchemy.orm import InstrumentedAttribute, class_mapper, declared_attr
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.pool import NullPool, Pool
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from typing_extensions import assert_never

from utilities.class_name import get_class_name
from utilities.errors import redirect_error
from utilities.itertools import (
    EmptyIterableError,
    IterableContainsDuplicatesError,
    MultipleElementsError,
    check_duplicates,
    chunked,
    is_iterable_not_str,
    one,
)
from utilities.math import FloatNonNeg, IntNonNeg
from utilities.text import ensure_str, snake_case, snake_case_mappings
from utilities.typing import IterableStrs


def check_dataframe_schema_against_table(
    df_schema: Mapping[str, Any],
    table_or_mapped_class: Table | type[Any],
    check_dtype: Callable[[Any, type], bool],
    /,
    *,
    snake: bool = False,
) -> dict[str, str]:
    table_schema = {
        col.name: col.type.python_type
        for col in get_columns(get_table(table_or_mapped_class))
    }
    out: dict[str, str] = {}
    for sr_name, sr_dtype in df_schema.items():
        with suppress(SeriesMatchesAgainstNoColumnError):
            out[sr_name] = _check_series_against_against_table(
                sr_name, sr_dtype, table_schema, check_dtype, snake=snake
            )
    return out


def _check_series_against_against_table(
    sr_name: str,
    sr_dtype: Any,
    table_schema: Mapping[str, type],
    check_dtype: Callable[[Any, type], bool],
    /,
    *,
    snake: bool = False,
) -> str:
    db_name, db_type = _match_series_name_to_table_column(
        sr_name, table_schema, snake=snake
    )
    if not check_dtype(sr_dtype, db_type):
        msg = f"{sr_dtype=}, {db_type=}"
        raise SeriesAndTableColumnIncompatibleError(msg)
    return db_name


class SeriesAndTableColumnIncompatibleError(Exception):
    """Raised when a Series and table column are incompatible."""


def _match_series_name_to_table_column(
    sr_name: str, table_schema: Mapping[str, type], /, *, snake: bool = False
) -> tuple[str, type]:
    items = table_schema.items()
    try:
        if snake:
            return one((n, t) for n, t in items if snake_case(n) == snake_case(sr_name))
        return one((n, t) for n, t in items if n == sr_name)
    except EmptyIterableError:
        msg = f"{sr_name=}, {table_schema=}"
        raise SeriesMatchesAgainstNoColumnError(msg) from None
    except MultipleElementsError:
        msg = f"{sr_name=}, {table_schema=}"
        raise SeriesMatchesAgainstMultipleColumnsError(msg) from None


class SeriesMatchesAgainstNoColumnError(Exception):
    """Raised when a Series matches against no column."""


class SeriesMatchesAgainstMultipleColumnsError(Exception):
    """Raised when a Series matches against multiple columns."""


def check_selectable_for_duplicate_columns(sel: Select[Any], /) -> None:
    """Check a selectable for duplicate columns."""
    columns: ReadOnlyColumnCollection = cast(Any, sel).selected_columns
    names = [col.name for col in columns]
    try:
        check_duplicates(names)
    except IterableContainsDuplicatesError:
        msg = f"{names=}"
        raise DuplicateColumnError(msg) from None


def check_table_against_reflection(
    table_or_mapped_class: Table | type[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    schema: str | None = None,
    snake_table: bool = False,
    snake_columns: bool = False,
    allow_permutations_columns: bool = False,
    primary_key: bool = True,
) -> None:
    """Check that a table equals its reflection."""
    reflected = _reflect_table(table_or_mapped_class, engine_or_conn, schema=schema)
    check_tables_equal(
        reflected,
        table_or_mapped_class,
        snake_table=snake_table,
        allow_permutations_columns=allow_permutations_columns,
        snake_columns=snake_columns,
        primary_key=primary_key,
    )


def _reflect_table(
    table_or_mapped_class: Table | type[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    schema: str | None = None,
) -> Table:
    """Reflect a table from a database."""
    name = get_table_name(table_or_mapped_class)
    metadata = MetaData(schema=schema)
    with yield_connection(engine_or_conn) as conn:
        return Table(name, metadata, autoload_with=conn)


def check_tables_equal(
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
    x, y = (str(i) if isinstance(i, quoted_name) else i for i in [x, y])
    msg = f"{x=}, {y=}"
    if snake and (snake_case(x) != snake_case(y)):
        raise UnequalTableOrColumnSnakeCaseNamesError(msg)
    if (not snake) and (x != y):
        raise UnequalTableOrColumnNamesError(msg)


class UnequalTableOrColumnNamesError(Exception):
    """Raised when two table/columns' names differ."""


class UnequalTableOrColumnSnakeCaseNamesError(Exception):
    """Raised when two table/columns' snake case names differ."""


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
    cols_x, cols_y = (list(cast(Iterable[Column[Any]], i)) for i in [x, y])
    name_to_col_x, name_to_col_y = (
        {ensure_str(col.name): col for col in i} for i in [cols_x, cols_y]
    )
    if len(name_to_col_x) != len(name_to_col_y):
        msg = f"{x=}, {y=}"
        raise UnequalNumberOfColumnsError(msg)
    if snake:
        snake_to_name_x, snake_to_name_y = (
            snake_case_mappings(i, inverse=True) for i in [name_to_col_x, name_to_col_y]
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
        raise UnequalSetOfColumnsError(msg)
    for x_i, y_i in zip(cols_to_check_x, cols_to_check_y, strict=True):
        _check_columns_equal(x_i, y_i, snake=snake, primary_key=primary_key)


class UnequalNumberOfColumnsError(Exception):
    """Raised when two column collections' lengths differ."""


class UnequalSetOfColumnsError(Exception):
    """Raised when two column collections' set of columns differ."""


def _check_columns_equal(
    x: Column[Any], y: Column[Any], /, *, snake: bool = False, primary_key: bool = True
) -> None:
    """Check that a pair of columns are equal."""
    _check_table_or_column_names_equal(x.name, y.name, snake=snake)
    _check_column_types_equal(x.type, y.type)
    if primary_key and (x.primary_key != y.primary_key):
        msg = f"{x.primary_key=}, {y.primary_key=}"
        raise UnequalPrimaryKeyStatusError(msg)
    if x.nullable != y.nullable:
        msg = f"{x.nullable=}, {y.nullable=}"
        raise UnequalNullableStatusError(msg)


class UnequalPrimaryKeyStatusError(Exception):
    """Raised when two columns differ in primary key status."""


class UnequalNullableStatusError(Exception):
    """Raised when two columns differ in nullable status."""


def _check_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of column types are equal."""
    x_inst, y_inst = (i() if isinstance(i, type) else i for i in [x, y])
    x_cls, y_cls = (i._type_affinity for i in [x_inst, y_inst])  # noqa: SLF001
    msg = f"{x=}, {y=}"
    if not (isinstance(x_inst, y_cls) and isinstance(y_inst, x_cls)):
        raise UnequalColumnTypesError(msg)
    if isinstance(x_inst, Boolean) and isinstance(y_inst, Boolean):
        _check_boolean_column_types_equal(x_inst, y_inst)
    if isinstance(x_inst, sqlalchemy.Enum) and isinstance(y_inst, sqlalchemy.Enum):
        _check_enum_column_types_equal(x_inst, y_inst)
    if (
        isinstance(x_inst, Float | Numeric)
        and isinstance(y_inst, Float | Numeric)
        and (x_inst.asdecimal is not y_inst.asdecimal)
    ):
        raise UnequalFloatColumnAsDecimalError(msg)
    if (
        isinstance(x_inst, DateTime)
        and isinstance(y_inst, DateTime)
        and (x_inst.timezone is not y_inst.timezone)
    ):
        raise UnequalDateTimeColumnTimezoneError(msg)
    if isinstance(x_inst, Float | Numeric) and isinstance(y_inst, Float | Numeric):
        _check_float_column_types_equal(x_inst, y_inst)
    if isinstance(x_inst, Interval) and isinstance(y_inst, Interval):
        _check_interval_column_types_equal(x_inst, y_inst)
    if (
        isinstance(x_inst, LargeBinary)
        and isinstance(y_inst, LargeBinary)
        and (x_inst.length != y_inst.length)
    ):
        raise UnequalLargeBinaryColumnLengthError(msg)
    if isinstance(x_inst, String | Unicode | UnicodeText) and isinstance(
        y_inst, String | Unicode | UnicodeText
    ):
        _check_string_column_types_equal(x_inst, y_inst)
    if isinstance(x_inst, Uuid) and isinstance(y_inst, Uuid):
        _check_uuid_column_types_equal(x_inst, y_inst)


class UnequalColumnTypesError(Exception):
    """Raised when two columns' types differ."""


def _check_boolean_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of boolean column types are equal."""
    msg = f"{x=}, {y=}"
    if x.create_constraint is not y.create_constraint:
        raise UnequalBooleanColumnCreateConstraintError(msg)
    if x.name != y.name:
        raise UnequalBooleanColumnNameError(msg)


class UnequalBooleanColumnCreateConstraintError(Exception):
    """Raised when two boolean columns' create constraints differ."""


class UnequalBooleanColumnNameError(Exception):
    """Raised when two boolean columns' names differ."""


class UnequalDateTimeColumnTimezoneError(Exception):
    """Raised when two datetime columns' timezones differ."""


def _check_enum_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of enum column types are equal."""
    x_enum, y_enum = (i.enum_class for i in [x, y])
    msg = f"{x=}, {y=}"
    if ((x_enum is None) and (y_enum is not None)) or (
        (x_enum is not None)
        and (y_enum is None)
        or (
            (x_enum is not None)
            and (y_enum is not None)
            and not (issubclass(x_enum, y_enum) and issubclass(y_enum, x_enum))
        )
    ):
        raise UnequalEnumColumnTypesError(msg)
    if x.create_constraint is not y.create_constraint:
        raise UnequalEnumColumnCreateConstraintError(msg)
    if x.native_enum is not y.native_enum:
        raise UnequalEnumColumnNativeEnumError(msg)
    if x.length != y.length:
        raise UnequalEnumColumnLengthError(msg)
    if x.inherit_schema is not y.inherit_schema:
        raise UnequalEnumColumnInheritSchemaError(msg)


class UnequalEnumColumnTypesError(Exception):
    """Raised when two enum columns' types differ."""


class UnequalEnumColumnCreateConstraintError(Exception):
    """Raised when two enum columns' create constraints differ."""


class UnequalEnumColumnNativeEnumError(Exception):
    """Raised when two enum columns' native enums differ."""


class UnequalEnumColumnLengthError(Exception):
    """Raised when two enum columns' lengths differ."""


class UnequalEnumColumnInheritSchemaError(Exception):
    """Raised when two enum columns' inherit schemas differ."""


def _check_float_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of float column types are equal."""
    msg = f"{x=}, {y=}"
    if x.precision != y.precision:
        raise UnequalFloatColumnPrecisionsError(msg)
    if x.decimal_return_scale != y.decimal_return_scale:
        raise UnequalFloatColumnDecimalReturnScaleError(msg)
    if x.scale != y.scale:
        raise UnequalNumericScaleError(msg)


class UnequalFloatColumnPrecisionsError(Exception):
    """Raised when two float columns' precisions differ."""


class UnequalFloatColumnAsDecimalError(Exception):
    """Raised when two float columns' asdecimal differ."""


class UnequalFloatColumnDecimalReturnScaleError(Exception):
    """Raised when two float columns' decimal return scales differ."""


def _check_interval_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of interval column types are equal."""
    msg = f"{x=}, {y=}"
    if x.native is not y.native:
        raise UnequalIntervalColumnNativeError(msg)
    if x.second_precision != y.second_precision:
        raise UnequalIntervalColumnSecondPrecisionError(msg)
    if x.day_precision != y.day_precision:
        raise UnequalIntervalColumnDayPrecisionError(msg)


class UnequalIntervalColumnNativeError(Exception):
    """Raised when two intervals columns' native differ."""


class UnequalIntervalColumnSecondPrecisionError(Exception):
    """Raised when two intervals columns' second precisions differ."""


class UnequalIntervalColumnDayPrecisionError(Exception):
    """Raised when two intervals columns' day precisions differ."""


class UnequalLargeBinaryColumnLengthError(Exception):
    """Raised when two large binary columns' lengths differ."""


class UnequalNumericScaleError(Exception):
    """Raised when two numeric columns' scales differ."""


def _check_string_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of string column types are equal."""
    msg = f"{x=}, {y=}"
    if x.length != y.length:
        raise UnequalStringLengthError(msg)
    if x.collation != y.collation:
        raise UnequalStringCollationError(msg)


class UnequalStringLengthError(Exception):
    """Raised when two string columns' lengths differ."""


class UnequalStringCollationError(Exception):
    """Raised when two string columns' collations differ."""


def _check_uuid_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of UUID column types are equal."""
    msg = f"{x=}, {y=}"
    if x.as_uuid is not y.as_uuid:
        raise UnequalUUIDAsUUIDError(msg)
    if x.native_uuid is not y.native_uuid:
        raise UnequalUUIDNativeUUIDError(msg)


class UnequalUUIDAsUUIDError(Exception):
    """Raised when two UUID columns' as_uuid differ."""


class UnequalUUIDNativeUUIDError(Exception):
    """Raised when two UUID columns' native UUID differ."""


def check_engine(
    engine: Engine,
    /,
    *,
    num_tables: IntNonNeg | None = None,
    rel_tol: FloatNonNeg | None = None,
    abs_tol: IntNonNeg | None = None,
) -> None:
    """Check that an engine can connect.

    Optionally query for the number of tables, or the number of columns in
    such a table.
    """
    dialect = get_dialect(engine)
    match dialect:
        case Dialect.mssql | Dialect.mysql | Dialect.postgresql:  # pragma: no cover
            query = "select * from information_schema.tables"
        case Dialect.oracle:  # pragma: no cover
            query = "select * from all_objects"
        case Dialect.sqlite:
            query = "select * from sqlite_master where type='table'"
        case _:  # pragma: no cover  # type: ignore
            assert_never(dialect)

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(query)).all()
    except OperationalError as error:
        redirect_error(error, "unable to open database file", EngineError)
    if num_tables is not None:
        n_rows = len(rows)
        if (rel_tol is None) and (abs_tol is None):
            if n_rows != num_tables:
                msg = f"{len(rows)=}, {num_tables=}"
                raise IncorrectNumberOfTablesError(msg)
        else:
            rel_tol_use = 1e-9 if rel_tol is None else rel_tol
            abs_tol_use = 0.0 if abs_tol is None else abs_tol
            if not isclose(
                n_rows, num_tables, rel_tol=rel_tol_use, abs_tol=abs_tol_use
            ):
                msg = f"{len(rows)=}, {num_tables=}, {rel_tol=}, {abs_tol=}"
                raise IncorrectNumberOfTablesError(msg)


class EngineError(Exception):
    """Raised when an Engine cannot connect."""


class IncorrectNumberOfTablesError(Exception):
    """Raised when there are an incorrect number of tables."""


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
        kwargs = {"query": {k: _map_value(v) for k, v in query.items()}}
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


def _map_value(x: IterableStrs | str, /) -> list[str] | str:
    return x if isinstance(x, str) else list(x)


def ensure_engine(engine: Engine | str, /) -> Engine:
    """Ensure the object is an Engine."""
    if isinstance(engine, Engine):
        return engine
    return parse_engine(engine)


def ensure_tables_created(
    engine_or_conn: Engine | Connection, /, *tables_or_mapped_classes: Table | type[Any]
) -> None:
    """Ensure a table/set of tables is/are created."""
    for table_or_mapped_class in tables_or_mapped_classes:
        table = get_table(table_or_mapped_class)
        with yield_connection(engine_or_conn) as conn:
            try:
                table.create(conn)
            except DatabaseError as error:
                with suppress(TableAlreadyExistsError):
                    redirect_to_table_already_exists_error(conn, error)


def ensure_tables_dropped(
    engine_or_conn: Engine | Connection, /, *tables_or_mapped_classes: Table | type[Any]
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    for table_or_mapped_class in tables_or_mapped_classes:
        table = get_table(table_or_mapped_class)
        with yield_connection(engine_or_conn) as conn:
            try:
                table.drop(conn)
            except DatabaseError as error:
                with suppress(NoSuchTableError):
                    redirect_to_no_such_table_error(conn, error)


def get_column_names(table_or_mapped_class: Table | type[Any], /) -> list[str]:
    """Get the column names from a table or model."""
    return [col.name for col in get_columns(table_or_mapped_class)]


def get_columns(table_or_mapped_class: Table | type[Any], /) -> list[Column[Any]]:
    """Get the columns from a table or model."""
    return list(get_table(table_or_mapped_class).columns)


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
    raise UnsupportedDialectError(msg)  # pragma: no cover


class UnsupportedDialectError(Exception):
    """Raised when a dialect is unsupported."""


def get_table(table_or_mapped_class: Table | type[Any], /) -> Table:
    """Get the table from a Table or mapped class."""
    if isinstance(table_or_mapped_class, Table):
        return table_or_mapped_class
    if is_mapped_class(table_or_mapped_class):
        return cast(Any, table_or_mapped_class).__table__
    msg = f"{table_or_mapped_class=}"
    raise NotATableOrAMappedClassError(msg)


class NotATableOrAMappedClassError(Exception):
    """Raised when an object is neither a Table nor a mapped class."""


def get_table_name(table_or_mapped_class: Table | type[Any], /) -> str:
    """Get the table name from a Table or mapped class."""
    return get_table(table_or_mapped_class).name


_InsertItemValues = tuple[Any, ...] | dict[str, Any]


@dataclass
class _InsertionItem:
    values: _InsertItemValues
    table: Table


CHUNK_SIZE_FRAC = 0.95


def insert_items(
    engine_or_conn: Engine | Connection,
    *items: Any,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
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
    max_params = dialect.max_params
    to_insert: dict[Table, list[_InsertItemValues]] = defaultdict(list)
    lengths: set[int] = set()
    for item in chain(*map(_insert_items_collect, items)):
        values = item.values
        to_insert[item.table].append(values)
        lengths.add(len(values))
    max_length = max(lengths, default=1)
    chunk_size = floor(chunk_size_frac * max_params / max_length)
    with yield_connection(engine_or_conn) as conn:
        for table, values in to_insert.items():
            ins = insert(table)
            for chunk in chunked(values, n=chunk_size):
                if dialect is Dialect.oracle:  # pragma: no cover
                    _ = conn.execute(ins, cast(list[Any], chunk))
                else:
                    _ = conn.execute(ins.values(chunk))


def _insert_items_collect(item: Any, /) -> Iterator[_InsertionItem]:
    """Collect the insertion items."""
    if isinstance(item, tuple):
        try:
            data, table_or_mapped_class = item
        except ValueError:
            msg = f"{item=}"
            raise TupleNotAPairError(msg) from None
        if not is_table_or_mapped_class(table_or_mapped_class):
            msg = f"{table_or_mapped_class=}"
            raise SecondArgumentNotATableOrMappedClassError(msg)
        if _insert_items_collect_valid(data):
            yield _InsertionItem(values=data, table=get_table(table_or_mapped_class))
        elif is_iterable_not_str(data):
            yield from _insert_items_collect_iterable(data, table_or_mapped_class)
        else:
            msg = f"{data=}"
            raise FirstArgumentInvalidError(msg)
    elif is_iterable_not_str(item):
        for i in item:
            yield from _insert_items_collect(i)
    elif is_mapped_class(cls := type(item)):
        yield _InsertionItem(values=mapped_class_to_dict(item), table=get_table(cls))
    else:
        msg = f"{item=}"
        raise InvalidItemError(msg)


def _insert_items_collect_iterable(
    obj: Iterable[Any], table_or_mapped_class: Table | type[Any], /
) -> Iterator[_InsertionItem]:
    table = get_table(table_or_mapped_class)
    for datum in obj:
        if _insert_items_collect_valid(datum):
            yield _InsertionItem(values=datum, table=table)
        else:
            msg = f"{datum=}"
            raise InvalidItemInIterableError(msg)


def _insert_items_collect_valid(obj: Any, /) -> bool:
    return isinstance(obj, tuple) or (
        isinstance(obj, dict) and all(isinstance(key, str) for key in obj)
    )


class TupleNotAPairError(Exception):
    """Raised when the tuple is not a pair."""


class SecondArgumentNotATableOrMappedClassError(Exception):
    """Raised when the second argument is not a table or mapped class."""


class FirstArgumentListItemInvalidError(Exception):
    """Raised when the first argument contains an invalid item."""


class FirstArgumentInvalidError(Exception):
    """Raised when ths first argument is invalid."""


class InvalidItemError(Exception):
    """Raised when the item is invalid."""


class InvalidItemInIterableError(Exception):
    """Raised when the item in the iterable is invalid."""


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


def parse_engine(engine: str, /) -> Engine:
    """Parse a string into an Engine."""
    try:
        return _create_engine(engine, poolclass=NullPool)
    except ArgumentError:
        raise ParseEngineError from None


class ParseEngineError(Exception):
    """Raised when an `Engine` cannot be parsed."""


def redirect_to_no_such_table_error(
    engine_or_conn: Engine | Connection, error: DatabaseError, /
) -> NoReturn:
    """Redirect to the `NoSuchTableError`."""
    dialect = get_dialect(engine_or_conn)
    match dialect:
        case Dialect.mysql | Dialect.postgresql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.mssql:  # pragma: no cover
            pattern = (
                "Cannot drop the table .*, because it does not exist or you do "
                "not have permission"
            )
        case Dialect.oracle:  # pragma: no cover
            pattern = "ORA-00942: table or view does not exist"
        case Dialect.sqlite:
            pattern = "no such table"
        case _:  # pragma: no cover  # type: ignore
            assert_never(dialect)
    return redirect_error(error, pattern, NoSuchTableError)


def redirect_to_table_already_exists_error(
    engine_or_conn: Engine | Connection, error: DatabaseError, /
) -> NoReturn:
    """Redirect to the `TableAlreadyExistsError`."""
    dialect = get_dialect(engine_or_conn)
    match dialect:
        case Dialect.mssql | Dialect.postgresql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.mysql:  # pragma: no cover
            pattern = "There is already an object named .* in the database"
        case Dialect.oracle:  # pragma: no cover
            pattern = "ORA-00955: name is already used by an existing object"
        case Dialect.sqlite:
            pattern = "table .* already exists"
        case _:  # pragma: no cover  # type: ignore
            assert_never(dialect)
    return redirect_error(error, pattern, TableAlreadyExistsError)


class TableAlreadyExistsError(Exception):
    """Raised when a table already exists."""


def serialize_engine(engine: Engine, /) -> str:
    """Serialize an Engine."""
    return engine.url.render_as_string(hide_password=False)


class TablenameMixin:
    """Mix-in for an auto-generated tablename."""

    @cast(Any, declared_attr)
    def __tablename__(cls) -> str:  # noqa: N805
        return get_class_name(cls, snake=True)


@contextmanager
def yield_connection(engine_or_conn: Engine | Connection, /) -> Iterator[Connection]:
    """Yield a connection."""
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield conn
    else:
        yield engine_or_conn


__all__ = [
    "check_dataframe_schema_against_table",
    "check_engine",
    "check_selectable_for_duplicate_columns",
    "check_table_against_reflection",
    "check_tables_equal",
    "CHUNK_SIZE_FRAC",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "Dialect",
    "EngineError",
    "ensure_engine",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "FirstArgumentInvalidError",
    "FirstArgumentListItemInvalidError",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table_name",
    "get_table",
    "IncorrectNumberOfTablesError",
    "insert_items",
    "InvalidItemError",
    "InvalidItemInIterableError",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "NotATableOrAMappedClassError",
    "parse_engine",
    "ParseEngineError",
    "redirect_to_no_such_table_error",
    "redirect_to_table_already_exists_error",
    "SecondArgumentNotATableOrMappedClassError",
    "serialize_engine",
    "SeriesAndTableColumnIncompatibleError",
    "SeriesMatchesAgainstMultipleColumnsError",
    "SeriesMatchesAgainstNoColumnError",
    "TableAlreadyExistsError",
    "TablenameMixin",
    "TupleNotAPairError",
    "UnequalBooleanColumnCreateConstraintError",
    "UnequalBooleanColumnNameError",
    "UnequalColumnTypesError",
    "UnequalDateTimeColumnTimezoneError",
    "UnequalEnumColumnCreateConstraintError",
    "UnequalEnumColumnInheritSchemaError",
    "UnequalEnumColumnLengthError",
    "UnequalEnumColumnNativeEnumError",
    "UnequalEnumColumnTypesError",
    "UnequalFloatColumnAsDecimalError",
    "UnequalFloatColumnDecimalReturnScaleError",
    "UnequalFloatColumnPrecisionsError",
    "UnequalIntervalColumnDayPrecisionError",
    "UnequalIntervalColumnNativeError",
    "UnequalIntervalColumnSecondPrecisionError",
    "UnequalLargeBinaryColumnLengthError",
    "UnequalNullableStatusError",
    "UnequalNumberOfColumnsError",
    "UnequalNumericScaleError",
    "UnequalPrimaryKeyStatusError",
    "UnequalSetOfColumnsError",
    "UnequalStringCollationError",
    "UnequalStringLengthError",
    "UnequalTableOrColumnNamesError",
    "UnequalTableOrColumnSnakeCaseNamesError",
    "UnequalUUIDAsUUIDError",
    "UnequalUUIDNativeUUIDError",
    "UnsupportedDialectError",
    "yield_connection",
]
