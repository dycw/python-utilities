from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from functools import reduce
from operator import ge, le
from typing import Any, Literal, NoReturn, Optional, Union

from beartype import beartype
from more_itertools import chunked
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
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
    quoted_name,
    text,
)
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.dialects.mssql import dialect as mssql_dialect
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.exc import (
    DatabaseError,
    NoSuchTableError,
)
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.pool import NullPool, Pool
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from utilities.errors import redirect_error
from utilities.inflection import snake_case
from utilities.more_itertools import one
from utilities.typing import never


@beartype
def check_table_against_reflection(
    table_or_model: Any,
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    schema: Optional[str] = None,
) -> None:
    """Check that a table equals its reflection."""
    reflected = _reflect_table(table_or_model, engine_or_conn, schema=schema)
    check_tables_equal(reflected, table_or_model)


@beartype
def _reflect_table(
    table_or_model: Any,
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    schema: Optional[str] = None,
) -> Table:
    """Reflect a table from a database."""
    name = get_table_name(table_or_model)
    metadata = MetaData(schema=schema)
    with yield_connection(engine_or_conn) as conn:
        return Table(name, metadata, autoload_with=conn)


@beartype
def check_tables_equal(
    x: Any,
    y: Any,
    /,
    *,
    snake_table: bool = False,
    snake_columns: bool = False,
) -> None:
    """Check that a pair of tables are equal."""
    x_t, y_t = map(get_table, [x, y])
    _check_table_or_column_names_equal(x_t.name, y_t.name, snake=snake_table)
    _check_column_collections_equal(
        x_t.columns,
        y_t.columns,
        snake=snake_columns,
    )


@beartype
def _check_table_or_column_names_equal(
    x: Union[str, quoted_name],
    y: Union[str, quoted_name],
    /,
    *,
    snake: bool = False,
) -> None:
    """Check that a pair of table/columns' names are equal."""
    x, y = (str(i) if isinstance(i, quoted_name) else i for i in [x, y])
    msg = f"{x=}, {y=}"
    if snake and (snake_case(x) != snake_case(y)):
        raise UnequalTableOrColumnSnakeCaseNamesError(msg)
    if (not snake) and (x != y):
        raise UnequalTableOrColumnNamesError(msg)


class UnequalTableOrColumnNamesError(ValueError):
    """Raised when two table/columns' names differ."""


class UnequalTableOrColumnSnakeCaseNamesError(ValueError):
    """Raised when two table/columns' snake case names differ."""


@beartype
def _check_column_collections_equal(
    x: ReadOnlyColumnCollection[Any, Any],
    y: ReadOnlyColumnCollection[Any, Any],
    /,
    *,
    snake: bool = False,
) -> None:
    """Check that a pair of column collections are equal."""
    msg = f"{x=}, {y=}"
    if len(x) != len(y):
        raise UnequalNumberOfColumnsError(msg)
    for x_i, y_i in zip(x, y):
        _check_columns_equal(x_i, y_i, snake=snake)


class UnequalNumberOfColumnsError(ValueError):
    """Raised when two column collections' lengths differ."""


@beartype
def _check_columns_equal(
    x: Column[Any],
    y: Column[Any],
    /,
    *,
    snake: bool = False,
) -> None:
    """Check that a pair of columns are equal."""
    _check_table_or_column_names_equal(x.name, y.name, snake=snake)
    _check_column_types_equal(x.type, y.type)
    if x.primary_key != y.primary_key:
        msg = f"{x.primary_key=}, {y.primary_key=}"
        raise UnequalPrimaryKeyStatusError(msg)
    if x.nullable != y.nullable:
        msg = f"{x.nullable=}, {y.nullable=}"
        raise UnequalNullableStatusError(msg)


class UnequalPrimaryKeyStatusError(ValueError):
    """Raised when two columns differ in primary key status."""


class UnequalNullableStatusError(ValueError):
    """Raised when two columns differ in nullable status."""


@beartype
def _check_column_types_equal(  # noqa: C901,PLR0912,PLR0915
    x: Any,
    y: Any,
    /,
) -> None:
    """Check that a pair of column types are equal."""
    x_inst, y_inst = (i() if isinstance(i, type) else i for i in [x, y])
    x_cls, y_cls = (i._type_affinity for i in [x_inst, y_inst])  # noqa: SLF001
    msg = f"{x=}, {y=}"
    if not (isinstance(x_inst, y_cls) and isinstance(y_inst, x_cls)):
        raise UnequalColumnTypesError(msg)
    if isinstance(x_inst, Boolean) and isinstance(y_inst, Boolean):
        if x_inst.create_constraint is not y_inst.create_constraint:
            raise UnequalBooleanColumnCreateConstraintError(msg)
        if x_inst.name != y_inst.name:
            raise UnequalBooleanColumnNameError(msg)
    if isinstance(x_inst, Enum) and isinstance(y_inst, Enum):
        x_enum, y_enum = (i.enum_class for i in [x_inst, y_inst])
        if ((x_enum is None) and (y_enum is not None)) or (
            (x_enum is not None)
            and (y_enum is None)
            or (
                (x_enum is not None)
                and (y_enum is not None)
                and not (
                    issubclass(x_enum, y_enum) and issubclass(y_enum, x_enum)
                )
            )
        ):
            raise UnequalEnumColumnTypesError(msg)
        if x_inst.create_constraint is not y_inst.create_constraint:
            raise UnequalEnumColumnCreateConstraintError(msg)
        if x_inst.native_enum is not y_inst.native_enum:
            raise UnequalEnumColumnNativeEnumError(msg)
        if x_inst.length != y_inst.length:
            raise UnequalEnumColumnLengthError(msg)
        if x_inst.inherit_schema is not y_inst.inherit_schema:
            raise UnequalEnumColumnInheritSchemaError(msg)
    if (
        isinstance(x_inst, (Float, Numeric))
        and isinstance(y_inst, (Float, Numeric))
        and (x_inst.asdecimal is not y_inst.asdecimal)
    ):
        raise UnequalFloatColumnAsDecimalError(msg)
    if (
        isinstance(x_inst, DateTime)
        and isinstance(y_inst, DateTime)
        and (x_inst.timezone is not y_inst.timezone)
    ):
        raise UnequalDateTimeColumnTimezoneError(msg)
    if isinstance(x_inst, (Float, Numeric)) and isinstance(
        y_inst,
        (Float, Numeric),
    ):
        if x_inst.precision != y_inst.precision:
            raise UnequalFloatColumnPrecisionsError(msg)
        if x_inst.decimal_return_scale != y_inst.decimal_return_scale:
            raise UnequalFloatColumnDecimalReturnScaleError(msg)
        if x_inst.scale != y_inst.scale:
            raise UnequalNumericScaleError(msg)
    if isinstance(x_inst, Interval) and isinstance(y_inst, Interval):
        if x_inst.native is not y_inst.native:
            raise UnequalIntervalColumnNativeError(msg)
        if x_inst.second_precision != y_inst.second_precision:
            raise UnequalIntervalColumnSecondPrecisionError(msg)
        if x_inst.day_precision != y_inst.day_precision:
            raise UnequalIntervalColumnDayPrecisionError(msg)
    if (
        isinstance(x_inst, LargeBinary)
        and isinstance(y_inst, LargeBinary)
        and (x_inst.length != y_inst.length)
    ):
        raise UnequalLargeBinaryColumnLengthError(msg)
    if isinstance(x_inst, (String, Unicode, UnicodeText)) and isinstance(
        y_inst,
        (String, Unicode, UnicodeText),
    ):
        if x_inst.length != y_inst.length:
            raise UnequalStringLengthError(msg)
        if x_inst.collation != y_inst.collation:
            raise UnequalStringCollationError(msg)
    if isinstance(x_inst, Uuid) and isinstance(y_inst, Uuid):
        if x_inst.as_uuid is not y_inst.as_uuid:
            raise UnequalUUIDAsUUIDError(msg)
        if x_inst.native_uuid is not y_inst.native_uuid:
            raise UnequalUUIDNativeUUIDError(msg)


class UnequalColumnTypesError(TypeError):
    """Raised when two columns' types differ."""


class UnequalBooleanColumnCreateConstraintError(TypeError):
    """Raised when two boolean columns' create constraints differ."""


class UnequalBooleanColumnNameError(TypeError):
    """Raised when two boolean columns' names differ."""


class UnequalDateTimeColumnTimezoneError(TypeError):
    """Raised when two datetime columns' timezones differ."""


class UnequalEnumColumnTypesError(TypeError):
    """Raised when two enum columns' types differ."""


class UnequalEnumColumnCreateConstraintError(TypeError):
    """Raised when two enum columns' create constraints differ."""


class UnequalEnumColumnNativeEnumError(TypeError):
    """Raised when two enum columns' native enums differ."""


class UnequalEnumColumnLengthError(TypeError):
    """Raised when two enum columns' lengths differ."""


class UnequalEnumColumnInheritSchemaError(TypeError):
    """Raised when two enum columns' inherit schemas differ."""


class UnequalFloatColumnPrecisionsError(TypeError):
    """Raised when two float columns' precisions differ."""


class UnequalFloatColumnAsDecimalError(TypeError):
    """Raised when two float columns' asdecimal differ."""


class UnequalFloatColumnDecimalReturnScaleError(TypeError):
    """Raised when two float columns' decimal return scales differ."""


class UnequalIntervalColumnNativeError(TypeError):
    """Raised when two intervals columns' native differ."""


class UnequalIntervalColumnSecondPrecisionError(TypeError):
    """Raised when two intervals columns' second precisions differ."""


class UnequalIntervalColumnDayPrecisionError(TypeError):
    """Raised when two intervals columns' day precisions differ."""


class UnequalLargeBinaryColumnLengthError(TypeError):
    """Raised when two large binary columns' lengths differ."""


class UnequalNumericScaleError(TypeError):
    """Raised when two numeric columns' scales differ."""


class UnequalStringLengthError(TypeError):
    """Raised when two string columns' lengths differ."""


class UnequalStringCollationError(TypeError):
    """Raised when two string columns' collations differ."""


class UnequalUUIDAsUUIDError(TypeError):
    """Raised when two UUID columns' as_uuid differ."""


class UnequalUUIDNativeUUIDError(TypeError):
    """Raised when two UUID columns' native UUID differ."""


@beartype
def check_engine(
    engine: Engine,
    /,
    *,
    num_tables: Optional[int] = None,
    num_columns: Optional[int] = None,
) -> None:
    """Check that an engine can connect.

    Optionally query for the number of tables, or the number of columns in
    such a table.
    """
    dialect = get_dialect(engine)
    if (  # pragma: no cover
        (dialect == "mssql")
        or (dialect == "mysql")
        or (dialect == "postgresql")
    ):
        query = "select * from information_schema.tables"  # pragma: no cover
    elif dialect == "oracle":  # pragma: no cover
        query = "select * from all_objects"
    elif dialect == "sqlite":
        query = "select * from sqlite_master where type='table'"
    else:
        return never(dialect)  # pragma: no cover
    with engine.begin() as conn:
        rows = conn.execute(text(query)).all()
    if (num_tables is not None) and (len(rows) != num_tables):
        msg = f"{len(rows)=}, {num_tables=}"
        raise IncorrectNumberOfTablesError(msg)
    if num_columns is not None:
        if len(rows) == 0:
            msg = f"{engine=}"
            raise NoTablesError(msg)
        if len(rows[0]) != num_columns:
            msg = f"{len(rows[0])=}, {num_columns=}"
            raise IncorrectNumberOfColumnsError(msg)
    return None


class IncorrectNumberOfTablesError(ValueError):
    """Raised when there are an incorrect number of tables."""


class NoTablesError(ValueError):
    """Raised when there are no tables."""


class IncorrectNumberOfColumnsError(ValueError):
    """Raised when there are an incorrect number of columns."""


@beartype
def columnwise_max(*columns: Any) -> Any:
    """Compute the columnwise max of a number of columns."""
    return _columnwise_minmax(*columns, op=ge)


@beartype
def columnwise_min(*columns: Any) -> Any:
    """Compute the columnwise min of a number of columns."""
    return _columnwise_minmax(*columns, op=le)


@beartype
def _columnwise_minmax(*columns: Any, op: Callable[[Any, Any], Any]) -> Any:
    """Compute the columnwise min of a number of columns."""

    @beartype
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
            value
            for col in [x, y]
            if (value := getattr(col, "name", None)) is not None
        }
        try:
            (name,) = names
        except ValueError:
            return col
        else:
            return col.label(name)

    return reduce(func, columns)


@beartype
def create_engine(
    drivername: str,
    /,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    poolclass: Optional[type[Pool]] = NullPool,
) -> Engine:
    """Create a SQLAlchemy engine."""
    url = URL.create(
        drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    return _create_engine(url, future=True, poolclass=poolclass)


Dialect = Literal["mssql", "mysql", "oracle", "postgresql", "sqlite"]


@beartype
def get_dialect(engine_or_conn: Union[Engine, Connection], /) -> Dialect:
    """Get the dialect of a database."""
    if isinstance(dialect := engine_or_conn.dialect, mssql_dialect):
        return "mssql"
    if isinstance(dialect, mysql_dialect):
        return "mysql"
    if isinstance(dialect, oracle_dialect):
        return "oracle"
    if isinstance(dialect, postgresql_dialect):
        return "postgresql"
    if isinstance(dialect, sqlite_dialect):
        return "sqlite"
    msg = f"{dialect=}"  # pragma: no cover
    raise UnsupportedDialectError(msg)  # pragma: no cover


class UnsupportedDialectError(TypeError):
    """Raised when a dialect is unsupported."""


@beartype
def ensure_table_created(
    table_or_model: Any,
    engine_or_connection: Union[Engine, Connection],
    /,
) -> None:
    """Ensure a table is created."""
    table = get_table(table_or_model)
    try:
        with yield_connection(engine_or_connection) as conn:
            table.create(conn)
    except DatabaseError as error:
        with suppress(TableAlreadyExistsError):
            redirect_to_table_already_exists_error(engine_or_connection, error)


@beartype
def ensure_table_dropped(
    table_or_model: Any,
    engine_or_conn: Union[Engine, Connection],
    /,
) -> None:
    """Ensure a table is dropped."""
    table = get_table(table_or_model)
    try:
        with yield_connection(engine_or_conn) as conn:
            table.drop(conn)
    except DatabaseError as error:
        with suppress(NoSuchTableError):
            redirect_to_no_such_table_error(engine_or_conn, error)


@beartype
def get_column_names(table_or_model: Any, /) -> list[str]:
    """Get the column names from a table or model."""
    return [col.name for col in get_columns(table_or_model)]


@beartype
def get_columns(table_or_model: Any, /) -> list[Column[Any]]:
    """Get the columns from a table or model."""
    return list(get_table(table_or_model).columns)


@beartype
def get_table(table_or_model: Any, /) -> Table:
    """Get the table from a ORM model."""
    if isinstance(table_or_model, Table):
        return table_or_model
    return table_or_model.__table__


@beartype
def get_table_name(table_or_model: Any, /) -> str:
    """Get the table name from a ORM model."""
    return get_table(table_or_model).name


@beartype
def model_to_dict(obj: Any, /) -> dict[str, Any]:
    """Construct a dictionary of elements for insertion."""
    cls = type(obj)

    @beartype
    def is_attr(attr: str, key: str, /) -> Optional[str]:
        if isinstance(value := getattr(cls, attr), InstrumentedAttribute) and (
            value.name == key
        ):
            return attr
        return None

    @beartype
    def yield_items() -> Iterator[tuple[str, Any]]:
        for key in get_column_names(cls):
            attr = one(
                attr for attr in dir(cls) if is_attr(attr, key) is not None
            )
            yield key, getattr(obj, attr)

    return dict(yield_items())


@beartype
def redirect_to_no_such_table_error(
    engine_or_conn: Union[Engine, Connection],
    error: DatabaseError,
    /,
) -> NoReturn:
    """Redirect to the `NoSuchTableError`."""
    dialect = get_dialect(engine_or_conn)
    if (  # pragma: no cover
        dialect == "mssql" or dialect == "mysql" or dialect == "postgresql"
    ):
        raise NotImplementedError(dialect)  # pragma: no cover
    if dialect == "oracle":  # pragma: no cover
        pattern = "ORA-00942: table or view does not exist"
    elif dialect == "sqlite":
        pattern = "no such table"
    else:
        return never(dialect)  # pragma: no cover
    return redirect_error(error, pattern, NoSuchTableError)


@beartype
def redirect_to_table_already_exists_error(
    engine_or_conn: Union[Engine, Connection],
    error: DatabaseError,
    /,
) -> NoReturn:
    """Redirect to the `TableAlreadyExistsError`."""
    dialect = get_dialect(engine_or_conn)
    if (  # pragma: no cover
        dialect == "mssql" or dialect == "mysql" or dialect == "postgresql"
    ):
        raise NotImplementedError(dialect)  # pragma: no cover
    if dialect == "oracle":  # pragma: no cover
        pattern = "ORA-00955: name is already used by an existing object"
    elif dialect == "sqlite":
        pattern = "table .* already exists"
    else:
        return never(dialect)  # pragma: no cover
    return redirect_error(error, pattern, TableAlreadyExistsError)


class TableAlreadyExistsError(Exception):
    """Raised when a table already exists."""


@contextmanager
@beartype
def yield_connection(
    engine_or_conn: Union[Engine, Connection],
    /,
) -> Iterator[Connection]:
    """Yield a connection."""
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield conn
    else:
        yield engine_or_conn


@beartype
def yield_in_clause_rows(
    sel: Select,
    column: Any,
    values: Iterable[Any],
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    chunk_size: Optional[int] = None,
    frac: float = 0.95,
) -> Iterator[Any]:
    """Yield the rows from an `in` clause."""
    if chunk_size is None:
        dialect = get_dialect(engine_or_conn)
        if dialect == "mssql":  # pragma: no cover
            max_params = 2100
        elif dialect == "mysql":  # pragma: no cover
            max_params = 65535
        elif dialect == "oracle":  # pragma: no cover
            max_params = 1000
        elif dialect == "postgresql":  # pragma: no cover
            max_params = 32767
        elif dialect == "sqlite":
            max_params = 100
        else:
            return never(dialect)  # pragma: no cover
        chunk_size_use = round(frac * max_params)
    else:
        chunk_size_use = chunk_size
    with yield_connection(engine_or_conn) as conn:
        for values_i in chunked(values, chunk_size_use):
            sel_i = sel.where(column.in_(values_i))
            yield from conn.execute(sel_i).all()
    return None
