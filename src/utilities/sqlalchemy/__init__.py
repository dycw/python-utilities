from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from functools import reduce
from operator import ge, le
from typing import Any, Literal, NoReturn, Optional, Union

from beartype import beartype
from more_itertools import chunked
from sqlalchemy import Column, Select, Table, and_, case
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.dialects.mssql import dialect as mssql_dialect
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.oracle import dialect as oracle_dialect
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.exc import DatabaseError, NoSuchTableError
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.pool import NullPool, Pool

from utilities.errors import redirect_error
from utilities.more_itertools import one
from utilities.typing import never


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
