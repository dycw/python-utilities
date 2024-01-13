from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from functools import reduce
from operator import ge, itemgetter, le
from typing import Any, cast

import sqlalchemy
from sqlalchemy import (
    URL,
    Boolean,
    Column,
    DateTime,
    Engine,
    Float,
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
    quoted_name,
    text,
)
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.exc import ArgumentError, DatabaseError
from sqlalchemy.orm import declared_attr
from sqlalchemy.pool import NullPool, Pool
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from typing_extensions import assert_never

from utilities._sqlalchemy.common import (
    CHUNK_SIZE_FRAC,
    Dialect,
    GetDialectError,
    GetTableError,
    ensure_tables_created,
    get_chunk_size,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    insert_items,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
)
from utilities.errors import redirect_error
from utilities.humps import snake_case, snake_case_mappings
from utilities.iterables import CheckLengthError, check_length
from utilities.math import FloatFinNonNeg, IntNonNeg
from utilities.text import ensure_str
from utilities.types import IterableStrs, get_class_name


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
        raise _CheckColumnCollectionsEqualError(msg)
    if snake:
        snake_to_name_x, snake_to_name_y = (
            snake_case_mappings(i).inv for i in [name_to_col_x, name_to_col_y]
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


class _CheckColumnCollectionsEqualError(Exception):
    ...


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


class _CheckColumnsEqualError(Exception):
    ...


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


class _CheckColumnTypesEqualError(Exception):
    ...


def _check_column_types_boolean_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of boolean column types are equal."""
    msg = f"{x=}, {y=}"
    if x.create_constraint is not y.create_constraint:
        raise _CheckColumnTypesBooleanEqualError(msg)
    if x.name != y.name:
        raise _CheckColumnTypesBooleanEqualError(msg)


class _CheckColumnTypesBooleanEqualError(Exception):
    ...


def _check_column_types_datetime_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of datetime column types are equal."""
    if x.timezone is not y.timezone:
        msg = f"{x=}, {y=}"
        raise _CheckColumnTypesDateTimeEqualError(msg)


class _CheckColumnTypesDateTimeEqualError(Exception):
    ...


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


class _CheckColumnTypesEnumEqualError(Exception):
    ...


def _check_column_types_float_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of float column types are equal."""
    msg = f"{x=}, {y=}"
    if x.precision != y.precision:
        raise _CheckColumnTypesFloatEqualError(msg)
    if x.asdecimal is not y.asdecimal:
        raise _CheckColumnTypesFloatEqualError(msg)
    if x.decimal_return_scale != y.decimal_return_scale:
        raise _CheckColumnTypesFloatEqualError(msg)


class _CheckColumnTypesFloatEqualError(Exception):
    ...


def _check_column_types_interval_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of interval column types are equal."""
    msg = f"{x=}, {y=}"
    if x.native is not y.native:
        raise _CheckColumnTypesIntervalEqualError(msg)
    if x.second_precision != y.second_precision:
        raise _CheckColumnTypesIntervalEqualError(msg)
    if x.day_precision != y.day_precision:
        raise _CheckColumnTypesIntervalEqualError(msg)


class _CheckColumnTypesIntervalEqualError(Exception):
    ...


def _check_column_types_large_binary_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of large binary column types are equal."""
    if x.length != y.length:
        msg = f"{x=}, {y=}"
        raise _CheckColumnTypesLargeBinaryEqualError(msg)


class _CheckColumnTypesLargeBinaryEqualError(Exception):
    ...


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


class _CheckColumnTypesNumericEqualError(Exception):
    ...


def _check_column_types_string_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of string column types are equal."""
    msg = f"{x=}, {y=}"
    if x.length != y.length:
        raise _CheckColumnTypesStringEqualError(msg)
    if x.collation != y.collation:
        raise _CheckColumnTypesStringEqualError(msg)


class _CheckColumnTypesStringEqualError(Exception):
    ...


def _check_column_types_uuid_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of UUID column types are equal."""
    msg = f"{x=}, {y=}"
    if x.as_uuid is not y.as_uuid:
        raise _CheckColumnTypesUuidEqualError(msg)
    if x.native_uuid is not y.native_uuid:
        raise _CheckColumnTypesUuidEqualError(msg)


class _CheckColumnTypesUuidEqualError(Exception):
    ...


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
        case _ as never:  # type: ignore
            assert_never(never)
    statement = text(query)
    with engine.begin() as conn:
        rows = conn.execute(statement).all()
    if num_tables is not None:
        with redirect_error(
            CheckLengthError, CheckEngineError(f"{engine=}, {num_tables=}")
        ):
            check_length(rows, equal_or_approx=num_tables)


class CheckEngineError(Exception):
    ...


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
    x, y = (str(i) if isinstance(i, quoted_name) else i for i in [x, y])
    msg = f"{x=}, {y=}"
    if (not snake) and (x != y):
        raise _CheckTableOrColumnNamesEqualError(msg)
    if snake and (snake_case(x) != snake_case(y)):
        raise _CheckTableOrColumnNamesEqualError(msg)


class _CheckTableOrColumnNamesEqualError(Exception):
    ...


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


def ensure_engine(engine: Engine | str, /) -> Engine:
    """Ensure the object is an Engine."""
    if isinstance(engine, Engine):
        return engine
    return parse_engine(engine)


def ensure_tables_dropped(
    engine: Engine, *tables_or_mapped_classes: Table | type[Any]
) -> None:
    """Ensure a table/set of tables is/are dropped."""
    for table_or_mapped_class in tables_or_mapped_classes:
        table = get_table(table_or_mapped_class)
        with suppress(TableDoesNotExistError), redirect_table_does_not_exist(
            engine
        ), engine.begin() as conn:
            table.drop(conn)


def get_table_name(table_or_mapped_class: Table | type[Any], /) -> str:
    """Get the table name from a Table or mapped class."""
    return get_table(table_or_mapped_class).name


def parse_engine(engine: str, /) -> Engine:
    """Parse a string into an Engine."""
    with redirect_error(ArgumentError, ParseEngineError(f"{engine=}")):
        return _create_engine(engine, poolclass=NullPool)


class ParseEngineError(Exception):
    ...


@contextmanager
def redirect_table_does_not_exist(engine: Engine, /) -> Iterator[None]:
    """Redirect to the `TableDoesNotExistError`."""
    match dialect := get_dialect(engine):
        case Dialect.mysql | Dialect.postgresql:  # pragma: no cover
            raise NotImplementedError(dialect)
        case Dialect.mssql:  # pragma: no cover
            match = (
                "Cannot drop the table .*, because it does not exist or you do "
                "not have permission"
            )
        case Dialect.oracle:  # pragma: no cover
            match = "ORA-00942: table or view does not exist"
        case Dialect.sqlite:
            match = "no such table"
        case _ as never:  # type: ignore
            assert_never(never)
    with redirect_error(DatabaseError, TableDoesNotExistError, match=match):
        yield


class TableDoesNotExistError(Exception):
    ...


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
        return snake_case(get_class_name(cls))


__all__ = [
    "CHUNK_SIZE_FRAC",
    "CheckEngineError",
    "Dialect",
    "GetDialectError",
    "GetTableError",
    "ParseEngineError",
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
    "get_table_name",
    "insert_items",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "parse_engine",
    "redirect_table_does_not_exist",
    "serialize_engine",
]


try:
    from utilities._sqlalchemy.polars import (
        InsertDataFrameError,
        SelectToDataFrameError,
        insert_dataframe,
        select_to_dataframe,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "InsertDataFrameError",
        "SelectToDataFrameError",
        "insert_dataframe",
        "select_to_dataframe",
    ]

try:
    from utilities._sqlalchemy.timeout_decorator import (
        NextFromSequenceError,
        next_from_sequence,
        redirect_next_from_sequence_error,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "NextFromSequenceError",
        "next_from_sequence",
        "redirect_next_from_sequence_error",
    ]
