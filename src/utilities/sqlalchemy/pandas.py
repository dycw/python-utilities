import datetime as dt
from collections.abc import Iterable
from collections.abc import Iterator
from decimal import Decimal
from typing import Any
from typing import Optional
from typing import Union
from typing import overload

from beartype import beartype
from pandas import DataFrame
from pandas import Series
from sqlalchemy import Column
from sqlalchemy import insert
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.engine import Row
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import Select

from utilities.bidict import snake_case_mappings
from utilities.iterables import IterableContainsDuplicatesError
from utilities.iterables import check_duplicates
from utilities.more_itertools import EmptyIterableError
from utilities.more_itertools import one
from utilities.numpy import datetime64ns
from utilities.numpy import has_dtype
from utilities.pandas import Int64
from utilities.pandas import boolean
from utilities.pandas import datetime64nsutc
from utilities.pandas import string
from utilities.pandas import timestamp_to_date
from utilities.pandas import timestamp_to_datetime
from utilities.sqlalchemy import get_column_names
from utilities.sqlalchemy import get_columns
from utilities.sqlalchemy import get_dialect
from utilities.sqlalchemy import get_table
from utilities.sqlalchemy import model_to_dict
from utilities.sqlalchemy import yield_connection
from utilities.text import ensure_str
from utilities.text import snake_case


@beartype
def insert_dataframe(
    df: DataFrame,
    table_or_model: Any,
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    snake: bool = False,
    allow_naive_datetimes: bool = False,
) -> None:
    """Insert a DataFrame into a database."""
    return insert_items(
        [(df, table_or_model)],
        engine_or_conn,
        snake=snake,
        allow_naive_datetimes=allow_naive_datetimes,
    )


@beartype
def insert_items(
    items: Iterable[Any],
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    snake: bool = False,
    allow_naive_datetimes: bool = False,
) -> None:
    """Insert a set of items into a database.

    These can be either a:
     - ([tuple], table) pair, or
     - (DataFrame, table) pair, or
     - Model instance.
    """
    dialect = get_dialect(engine_or_conn)
    with yield_connection(engine_or_conn) as conn:
        for item in items:
            if isinstance(item, tuple):
                first, table_or_model = item
                if isinstance(first, list):
                    values = first
                elif isinstance(first, DataFrame):
                    values = list(
                        _yield_dataframe_rows_as_dicts(
                            first,
                            table_or_model,
                            snake=snake,
                            allow_naive_datetimes=allow_naive_datetimes,
                        ),
                    )
                else:
                    msg = f"Invalid type: {first=}"
                    raise TypeError(msg)
            else:
                table_or_model = item
                values = [model_to_dict(item)]
            ins = insert(get_table(table_or_model))
            if len(values) >= 1:
                if dialect == "oracle":  # pragma: no cover
                    _ = conn.execute(ins, values)
                else:
                    _ = conn.execute(ins.values(values))


@beartype
def _yield_dataframe_rows_as_dicts(
    df: DataFrame,
    table_or_model: Any,
    /,
    *,
    snake: bool = False,
    allow_naive_datetimes: bool = False,
) -> Iterator[dict[str, Any]]:
    """Yield the rows of a DataFrame as dicts, ready for insertion."""
    parsed = [
        _parse_series_against_table(
            sr,
            table_or_model,
            snake=snake,
            allow_naive_datetimes=allow_naive_datetimes,
        )
        for _, sr in df.items()
    ]
    keys = [key for key, _ in parsed]
    for row in zip(*(it for _, it in parsed)):
        yield dict(zip(keys, row))


@beartype
def _parse_series_against_table(
    series: Series,
    table_or_model: Any,
    /,
    *,
    snake: bool = False,
    allow_naive_datetimes: bool = False,
) -> tuple[str, Iterator[Any]]:
    """Parse a series against a table.

    In particular:
     - check the column which it will insert into, and
     - yield the elements for insertion.
    """
    series_name = ensure_str(series.name)
    if snake:
        column_names = map(snake_case, get_column_names(table_or_model))
        target_name = snake_case(series_name)
        error = SeriesNameSnakeCaseNotInTableError
    else:
        column_names = get_column_names(table_or_model)
        target_name = snake_case(series_name)
        error = SeriesNameNotInTableError
    try:
        column = one(
            col
            for name, col in zip(column_names, get_columns(table_or_model))
            if name == target_name
        )
    except EmptyIterableError:
        msg = f"Unable to map {series_name!r} to {table_or_model}"
        raise error(msg) from None
    _check_series_against_table_column(
        series,
        column,
        allow_naive_datetimes=allow_naive_datetimes,
    )
    return column.name, _yield_insertion_elements(series)


class SeriesNameSnakeCaseNotInTableError(ValueError):
    """Raised when a Series name is not in a table, modulo snake case."""


class SeriesNameNotInTableError(ValueError):
    """Raised when a Series name is not in a table."""


@beartype
def _check_series_against_table_column(
    series: Series,
    table_column: Column[Any],
    /,
    *,
    allow_naive_datetimes: bool = False,
) -> None:
    """Check if a series can be inserted into a column."""
    py_type = table_column.type.python_type
    if not (
        (has_dtype(series, (bool, boolean)) and issubclass(py_type, (bool, int)))
        or (has_dtype(series, float) and issubclass(py_type, float))
        or (
            has_dtype(series, datetime64ns)
            and issubclass(py_type, dt.date)
            and not issubclass(py_type, dt.datetime)
        )
        or (has_dtype(series, datetime64nsutc) and issubclass(py_type, dt.datetime))
        or (
            allow_naive_datetimes
            and has_dtype(series, datetime64ns)
            and issubclass(py_type, dt.datetime)
        )
        or (has_dtype(series, (int, Int64)) and (py_type, int))
        or (has_dtype(series, string) and (py_type, str))
    ):
        msg = f"{series=}, {table_column=}"
        raise SeriesAgainstTableColumnError(msg)


class SeriesAgainstTableColumnError(TypeError):
    """Raised when a series has incompatible dtype with a table column."""


@beartype
def _yield_insertion_elements(series: Series, /) -> Iterator[Any]:
    """Yield the elements for insertion."""
    if has_dtype(series, (bool, boolean)):
        cast = bool
    elif has_dtype(series, datetime64ns):
        if (series.notna() & (series != series.dt.normalize())).any():
            raise DatesWithTimeComponentsError(str(series))
        cast = timestamp_to_date
    elif has_dtype(series, datetime64nsutc):
        cast = timestamp_to_datetime
    elif has_dtype(series, float):
        cast = float
    elif has_dtype(series, (int, Int64)):
        cast = int
    elif has_dtype(series, string):
        cast = str
    else:
        msg = f"Invalid dtype: {series=}"
        raise TypeError(msg)
    return (None if n else cast(v) for n, v in zip(series.isna(), series))


class DatesWithTimeComponentsError(ValueError):
    """Raised when dates with time components are encountered."""


@overload
def select_to_dataframe(
    sel: Select,
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    snake: bool = False,
    stream: int,
) -> Iterator[DataFrame]:
    ...


@overload
def select_to_dataframe(
    sel: Select,
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    snake: bool = False,
    stream: None = None,
) -> DataFrame:
    ...


@beartype
def select_to_dataframe(
    sel: Select,
    engine_or_conn: Union[Engine, Connection],
    /,
    *,
    snake: bool = False,
    stream: Optional[int] = None,
) -> Union[DataFrame, Iterator[DataFrame]]:
    """Read a table from a database into a DataFrame.

    Optionally stream it in chunks.
    """
    _check_select_for_duplicates(sel)
    if stream is None:
        with yield_connection(engine_or_conn) as conn:
            rows = conn.execute(sel).all()
            return _rows_to_dataframe(sel, rows, snake=snake)
    return _stream_dataframes(sel, engine_or_conn, stream, snake=snake)


@beartype
def _check_select_for_duplicates(sel: Select, /) -> None:
    """Check a select statement contains no duplicates."""
    col_names = [col.name for col in sel.selected_columns.values()]
    try:
        check_duplicates(col_names)
    except IterableContainsDuplicatesError:
        msg = f"{col_names=}"
        raise DuplicateColumnError(msg) from None


@beartype
def _rows_to_dataframe(
    sel: Select,
    rows: Iterable[Row],
    /,
    *,
    snake: bool = False,
) -> DataFrame:
    """Convert a set of rows into a DataFrame."""
    dtypes = {
        col.name: _table_column_to_dtype(col) for col in sel.selected_columns.values()
    }
    df = DataFrame(rows, columns=list(dtypes)).astype(dtypes)
    if snake:
        return _dataframe_columns_to_snake(df)
    return df


@beartype
def _table_column_to_dtype(column: ColumnElement[Any], /) -> Any:
    """Map a table column to a DataFrame dtype."""
    py_type = column.type.python_type
    if py_type is bool:
        return boolean
    if (py_type is float) or (py_type is Decimal):
        return float
    if py_type is int:
        return Int64
    if py_type is str:
        return string
    if issubclass(py_type, dt.date):
        return datetime64ns
    msg = f"Invalid type: {py_type=}"  # pragma: no cover
    raise TypeError(msg)  # pragma: no cover


@beartype
def _dataframe_columns_to_snake(df: DataFrame, /) -> DataFrame:
    """Convert the columns of a DataFrame to snake case."""
    columns = [c for c in df.columns if isinstance(c, str)]
    mapping = snake_case_mappings(columns)
    return df.rename(columns=mapping)


@beartype
def _stream_dataframes(
    sel: Select,
    engine_or_conn: Union[Engine, Connection],
    stream: int,
    /,
    *,
    snake: bool = False,
) -> Iterator[DataFrame]:
    if stream <= 0:
        raise NonPositiveStreamError(str(stream))
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield from _stream_dataframes(sel, conn, stream, snake=snake)
    else:
        for rows in (
            engine_or_conn.execution_options(yield_per=stream).execute(sel).partitions()
        ):
            yield _rows_to_dataframe(sel, rows, snake=snake)


class NonPositiveStreamError(ValueError):
    """Raised when the `stream` arguemnt is non-positive."""
