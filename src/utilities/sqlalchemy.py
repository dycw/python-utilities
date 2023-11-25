from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from functools import reduce
from math import isclose
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
from sqlalchemy.exc import (
    ArgumentError,
    DatabaseError,
    NoSuchTableError,
    OperationalError,
)
from sqlalchemy.orm import declared_attr
from sqlalchemy.pool import NullPool, Pool
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from typing_extensions import assert_never

from utilities._sqlalchemy.common import (
    INSERT_ITEMS_CHUNK_SIZE_FRAC,
    CheckSeriesAgainstTableColumnError,
    CheckSeriesAgainstTableSchemaError,
    Dialect,
    GetDialectError,
    GetTableError,
    InsertItemsCollectError,
    InsertItemsCollectIterableError,
    check_dataframe_schema_against_table,
    check_selectable_for_duplicate_columns,
    check_series_against_table_column,
    check_series_against_table_schema,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    insert_items,
    insert_items_collect,
    insert_items_collect_iterable,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
    yield_connection,
)
from utilities.class_name import get_class_name
from utilities.errors import redirect_error
from utilities.math import FloatNonNeg, IntNonNeg
from utilities.text import ensure_str, snake_case, snake_case_mappings
from utilities.typing import IterableStrs


def check_column_collections_equal(
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
        raise CheckColumnCollectionsEqualError(msg)
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
        raise CheckColumnCollectionsEqualError(msg)
    for x_i, y_i in zip(cols_to_check_x, cols_to_check_y, strict=True):
        check_columns_equal(x_i, y_i, snake=snake, primary_key=primary_key)


class CheckColumnCollectionsEqualError(Exception):
    ...


def check_columns_equal(
    x: Column[Any], y: Column[Any], /, *, snake: bool = False, primary_key: bool = True
) -> None:
    """Check that a pair of columns are equal."""
    check_table_or_column_names_equal(x.name, y.name, snake=snake)
    check_column_types_equal(x.type, y.type)
    if primary_key and (x.primary_key != y.primary_key):
        msg = f"{x.primary_key=}, {y.primary_key=}"
        raise CheckColumnsEqualError(msg)
    if x.nullable != y.nullable:
        msg = f"{x.nullable=}, {y.nullable=}"
        raise CheckColumnsEqualError(msg)


class CheckColumnsEqualError(Exception):
    ...


def check_column_types_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of column types are equal."""
    x_inst, y_inst = (i() if isinstance(i, type) else i for i in [x, y])
    x_cls, y_cls = (i._type_affinity for i in [x_inst, y_inst])  # noqa: SLF001
    msg = f"{x=}, {y=}"
    if not (isinstance(x_inst, y_cls) and isinstance(y_inst, x_cls)):
        raise CheckColumnTypesEqualError(msg)
    if isinstance(x_inst, Boolean) and isinstance(y_inst, Boolean):
        check_column_types_boolean_equal(x_inst, y_inst)
    if isinstance(x_inst, DateTime) and isinstance(y_inst, DateTime):
        check_column_types_datetime_equal(x_inst, y_inst)
    if isinstance(x_inst, sqlalchemy.Enum) and isinstance(y_inst, sqlalchemy.Enum):
        check_column_types_enum_equal(x_inst, y_inst)
    if isinstance(x_inst, Float) and isinstance(y_inst, Float):
        check_column_types_float_equal(x_inst, y_inst)
    if isinstance(x_inst, Interval) and isinstance(y_inst, Interval):
        check_column_types_interval_equal(x_inst, y_inst)
    if isinstance(x_inst, LargeBinary) and isinstance(y_inst, LargeBinary):
        check_column_types_large_binary_equal(x_inst, y_inst)
    if isinstance(x_inst, Numeric) and isinstance(y_inst, Numeric):
        check_column_types_numeric_equal(x_inst, y_inst)
    if isinstance(x_inst, String | Unicode | UnicodeText) and isinstance(
        y_inst, String | Unicode | UnicodeText
    ):
        check_column_types_string_equal(x_inst, y_inst)
    if isinstance(x_inst, Uuid) and isinstance(y_inst, Uuid):
        check_column_types_uuid_equal(x_inst, y_inst)


class CheckColumnTypesEqualError(Exception):
    ...


def check_column_types_boolean_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of boolean column types are equal."""
    msg = f"{x=}, {y=}"
    if x.create_constraint is not y.create_constraint:
        raise CheckColumnTypesBooleanEqualError(msg)
    if x.name != y.name:
        raise CheckColumnTypesBooleanEqualError(msg)


class CheckColumnTypesBooleanEqualError(Exception):
    ...


def check_column_types_datetime_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of datetime column types are equal."""
    if x.timezone is not y.timezone:
        msg = f"{x=}, {y=}"
        raise CheckColumnTypesDateTimeEqualError(msg)


class CheckColumnTypesDateTimeEqualError(Exception):
    ...


def check_column_types_enum_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of enum column types are equal."""
    x_enum, y_enum = (i.enum_class for i in [x, y])
    msg = f"{x=}, {y=}"
    if not (issubclass(x_enum, y_enum) and issubclass(y_enum, x_enum)):
        raise CheckColumnTypesEnumEqualError(msg)
    if x.create_constraint is not y.create_constraint:
        raise CheckColumnTypesEnumEqualError(msg)
    if x.native_enum is not y.native_enum:
        raise CheckColumnTypesEnumEqualError(msg)
    if x.length != y.length:
        raise CheckColumnTypesEnumEqualError(msg)
    if x.inherit_schema is not y.inherit_schema:
        raise CheckColumnTypesEnumEqualError(msg)


class CheckColumnTypesEnumEqualError(Exception):
    ...


def check_column_types_float_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of float column types are equal."""
    msg = f"{x=}, {y=}"
    if x.precision != y.precision:
        raise CheckColumnTypesFloatEqualError(msg)
    if x.asdecimal is not y.asdecimal:
        raise CheckColumnTypesFloatEqualError(msg)
    if x.decimal_return_scale != y.decimal_return_scale:
        raise CheckColumnTypesFloatEqualError(msg)


class CheckColumnTypesFloatEqualError(Exception):
    ...


def check_column_types_interval_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of interval column types are equal."""
    msg = f"{x=}, {y=}"
    if x.native is not y.native:
        raise CheckColumnTypesIntervalEqualError(msg)
    if x.second_precision != y.second_precision:
        raise CheckColumnTypesIntervalEqualError(msg)
    if x.day_precision != y.day_precision:
        raise CheckColumnTypesIntervalEqualError(msg)


class CheckColumnTypesIntervalEqualError(Exception):
    ...


def check_column_types_large_binary_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of large binary column types are equal."""
    if x.length != y.length:
        msg = f"{x=}, {y=}"
        raise CheckColumnTypesLargeBinaryEqualError(msg)


class CheckColumnTypesLargeBinaryEqualError(Exception):
    ...


def check_column_types_numeric_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of numeric column types are equal."""
    msg = f"{x=}, {y=}"
    if x.precision != y.precision:
        raise CheckColumnTypesNumericEqualError(msg)
    if x.scale != y.scale:
        raise CheckColumnTypesNumericEqualError(msg)
    if x.asdecimal != y.asdecimal:
        raise CheckColumnTypesNumericEqualError(msg)
    if x.decimal_return_scale != y.decimal_return_scale:
        raise CheckColumnTypesNumericEqualError(msg)


class CheckColumnTypesNumericEqualError(Exception):
    ...


def check_column_types_string_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of string column types are equal."""
    msg = f"{x=}, {y=}"
    if x.length != y.length:
        raise CheckColumnTypesStringEqualError(msg)
    if x.collation != y.collation:
        raise CheckColumnTypesStringEqualError(msg)


class CheckColumnTypesStringEqualError(Exception):
    ...


def check_column_types_uuid_equal(x: Any, y: Any, /) -> None:
    """Check that a pair of UUID column types are equal."""
    msg = f"{x=}, {y=}"
    if x.as_uuid is not y.as_uuid:
        raise CheckColumnTypesUuidEqualError(msg)
    if x.native_uuid is not y.native_uuid:
        raise CheckColumnTypesUuidEqualError(msg)


class CheckColumnTypesUuidEqualError(Exception):
    ...


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
        redirect_error(error, "unable to open database file", CheckEngineError)
    if num_tables is not None:
        n_rows = len(rows)
        if (rel_tol is None) and (abs_tol is None):
            if n_rows != num_tables:
                msg = f"{len(rows)=}, {num_tables=}"
                raise CheckEngineError(msg)
        else:
            rel_tol_use = 1e-9 if rel_tol is None else rel_tol
            abs_tol_use = 0.0 if abs_tol is None else abs_tol
            if not isclose(
                n_rows, num_tables, rel_tol=rel_tol_use, abs_tol=abs_tol_use
            ):
                msg = f"{len(rows)=}, {num_tables=}, {rel_tol=}, {abs_tol=}"
                raise CheckEngineError(msg)


class CheckEngineError(Exception):
    ...


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
    reflected = reflect_table(table_or_mapped_class, engine_or_conn, schema=schema)
    check_tables_equal(
        reflected,
        table_or_mapped_class,
        snake_table=snake_table,
        allow_permutations_columns=allow_permutations_columns,
        snake_columns=snake_columns,
        primary_key=primary_key,
    )


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
    check_table_or_column_names_equal(x_t.name, y_t.name, snake=snake_table)
    check_column_collections_equal(
        x_t.columns,
        y_t.columns,
        snake=snake_columns,
        allow_permutations=allow_permutations_columns,
        primary_key=primary_key,
    )


def check_table_or_column_names_equal(
    x: str | quoted_name, y: str | quoted_name, /, *, snake: bool = False
) -> None:
    """Check that a pair of table/columns' names are equal."""
    x, y = (str(i) if isinstance(i, quoted_name) else i for i in [x, y])
    msg = f"{x=}, {y=}"
    if (not snake) and (x != y):
        raise CheckTableOrColumnNamesEqualError(msg)
    if snake and (snake_case(x) != snake_case(y)):
        raise CheckTableOrColumnNamesEqualError(msg)


class CheckTableOrColumnNamesEqualError(Exception):
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


def get_table_name(table_or_mapped_class: Table | type[Any], /) -> str:
    """Get the table name from a Table or mapped class."""
    return get_table(table_or_mapped_class).name


def parse_engine(engine: str, /) -> Engine:
    """Parse a string into an Engine."""
    try:
        return _create_engine(engine, poolclass=NullPool)
    except ArgumentError:
        raise ParseEngineError from None


class ParseEngineError(Exception):
    ...


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


def reflect_table(
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


def serialize_engine(engine: Engine, /) -> str:
    """Serialize an Engine."""
    return engine.url.render_as_string(hide_password=False)


class TablenameMixin:
    """Mix-in for an auto-generated tablename."""

    @cast(Any, declared_attr)
    def __tablename__(cls) -> str:  # noqa: N805
        return get_class_name(cls, snake=True)


__all__ = [
    "check_column_collections_equal",
    "check_column_types_boolean_equal",
    "check_column_types_datetime_equal",
    "check_column_types_enum_equal",
    "check_column_types_equal",
    "check_column_types_float_equal",
    "check_column_types_interval_equal",
    "check_column_types_large_binary_equal",
    "check_column_types_numeric_equal",
    "check_column_types_uuid_equal",
    "check_columns_equal",
    "check_dataframe_schema_against_table",
    "check_engine",
    "check_selectable_for_duplicate_columns",
    "check_series_against_table_column",
    "check_series_against_table_schema",
    "check_table_against_reflection",
    "check_table_or_column_names_equal",
    "check_tables_equal",
    "CheckColumnCollectionsEqualError",
    "CheckColumnsEqualError",
    "CheckColumnTypesBooleanEqualError",
    "CheckColumnTypesDateTimeEqualError",
    "CheckColumnTypesEnumEqualError",
    "CheckColumnTypesEqualError",
    "CheckColumnTypesFloatEqualError",
    "CheckColumnTypesIntervalEqualError",
    "CheckColumnTypesNumericEqualError",
    "CheckColumnTypesStringEqualError",
    "CheckColumnTypesUuidEqualError",
    "CheckEngineError",
    "CheckSeriesAgainstTableColumnError",
    "CheckSeriesAgainstTableSchemaError",
    "CheckTableOrColumnNamesEqualError",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "Dialect",
    "Dialect",
    "ensure_engine",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "get_column_names",
    "get_column_names",
    "get_columns",
    "get_columns",
    "get_dialect",
    "get_dialect",
    "get_table_name",
    "get_table",
    "get_table",
    "GetDialectError",
    "GetTableError",
    "INSERT_ITEMS_CHUNK_SIZE_FRAC",
    "insert_items_collect_iterable",
    "insert_items_collect",
    "insert_items",
    "InsertItemsCollectError",
    "InsertItemsCollectIterableError",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "parse_engine",
    "ParseEngineError",
    "redirect_to_no_such_table_error",
    "redirect_to_table_already_exists_error",
    "serialize_engine",
    "TableAlreadyExistsError",
    "TablenameMixin",
    "yield_connection",
]


try:
    from utilities._sqlalchemy.fastparquet import select_to_parquet
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["select_to_parquet"]


try:
    from utilities._sqlalchemy.pandas import (
        InsertPandasDataFrameError,
        StreamDataFramesError,
        TableColumnToDTypeError,
        insert_pandas_dataframe,
        select_to_pandas_dataframe,
        stream_dataframes,
        table_column_to_dtype,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "insert_pandas_dataframe",
        "InsertPandasDataFrameError",
        "select_to_pandas_dataframe",
        "stream_dataframes",
        "StreamDataFramesError",
        "table_column_to_dtype",
        "TableColumnToDTypeError",
    ]


try:
    from utilities._sqlalchemy.polars import (
        InsertPolarsDataFrameError,
        TableColumnToExprError,
        insert_polars_dataframe,
        select_to_polars_dataframe,
        table_column_to_expr,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "insert_polars_dataframe",
        "InsertPolarsDataFrameError",
        "select_to_polars_dataframe",
        "table_column_to_expr",
        "TableColumnToExprError",
    ]

try:
    from utilities._sqlalchemy.timeout_decorator import (
        NextFromSequenceError,
        RedirectToNextFromSequenceSQLiteError,
        next_from_sequence,
        redirect_to_next_from_sequence_error,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "next_from_sequence",
        "NextFromSequenceError",
        "redirect_to_next_from_sequence_error",
        "RedirectToNextFromSequenceSQLiteError",
    ]
