from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Iterator
from datetime import timezone
from decimal import Decimal
from functools import partial
from typing import Any, cast, overload

from numpy import int64
from pandas import DataFrame, DatetimeTZDtype, Series, concat
from sqlalchemy import Column, Connection, Engine, Row, Select, Table
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from utilities.datetime import UTC
from utilities.numpy import datetime64ns
from utilities.pandas import (
    Int64,
    astype,
    boolean,
    datetime64nsutc,
    string,
    timestamp_to_date,
    timestamp_to_datetime,
)
from utilities.sqlalchemy import (
    CHUNK_SIZE_FRAC,
    check_dataframe_schema_against_table,
    check_selectable_for_duplicate_columns,
    insert_items,
    yield_connection,
)
from utilities.text import snake_case_mappings


def insert_pandas_dataframe(
    engine_or_conn: Engine | Connection,
    df: DataFrame,
    table_or_mapped_class: Table | type[Any],
    /,
    *,
    allow_naive_datetimes: bool = False,
    snake: bool = False,
    chunk_size_frac: float = CHUNK_SIZE_FRAC,
) -> None:
    """Insert a DataFrame into a database."""
    check_dtype = partial(
        _check_pandas_series, allow_naive_datetimes=allow_naive_datetimes
    )
    mapping = check_dataframe_schema_against_table(
        dict(df.dtypes), table_or_mapped_class, check_dtype, snake=snake
    )
    df_items = df[list(mapping)].rename(columns=mapping)
    for name, col in df_items.items():
        col_obj = col.astype(object).where(col.notna(), None)
        if col.dtype == datetime64ns:
            is_not_null = col.notnull()
            values = col.loc[is_not_null]
            if (values == values.dt.normalize()).all():
                func = timestamp_to_date
            else:
                func = timestamp_to_datetime
            col_obj.loc[is_not_null] = values.map(func)
        df_items[name] = col_obj
    items = df_items.to_dict(orient="records")
    if (len(df) > 0) and (len(items) == 0):
        msg = f"{df=}, {items=}"
        raise PandasDataFrameYieldsNoRowsError(msg)
    return insert_items(
        engine_or_conn, (items, table_or_mapped_class), chunk_size_frac=chunk_size_frac
    )


class PandasDataFrameYieldsNoRowsError(Exception):
    """Raised when a DataFrame yields no rows to insert."""


def _check_pandas_series(
    dtype: Any, py_type: type, /, *, allow_naive_datetimes: bool = False
) -> bool:
    is_bool = (dtype == bool) or (dtype == boolean)  # noqa: PLR1714
    is_int = (dtype == int) or (dtype == int64) or (dtype == Int64)  # noqa: PLR1714
    return (
        (is_bool and issubclass(py_type, bool))
        or ((dtype == datetime64ns) and issubclass(py_type, dt.date))
        or ((dtype == datetime64nsutc) and issubclass(py_type, dt.datetime))
        or (
            allow_naive_datetimes
            and (dtype == datetime64ns)
            and issubclass(py_type, dt.datetime)
        )
        or ((dtype == float) and issubclass(py_type, float))
        or (is_int and issubclass(py_type, int))
        or ((dtype == string) and issubclass(py_type, str))
    )


@overload
def select_to_pandas_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    stream: int,
    time_zone: timezone = ...,
    snake: bool = ...,
) -> Iterator[DataFrame]:
    ...


@overload
def select_to_pandas_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    stream: None = ...,
    time_zone: timezone = ...,
    snake: bool = False,
) -> DataFrame:
    ...


def select_to_pandas_dataframe(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    /,
    *,
    stream: int | None = None,
    time_zone: timezone = UTC,
    snake: bool = False,
) -> DataFrame | Iterator[DataFrame]:
    """Read a table from a database into a DataFrame.

    Optionally stream it in chunks.
    """
    check_selectable_for_duplicate_columns(sel)
    if stream is None:
        with yield_connection(engine_or_conn) as conn:
            rows = conn.execute(sel).all()
            return _rows_to_dataframe(sel, rows, time_zone=time_zone, snake=snake)
    return _stream_dataframes(
        sel, engine_or_conn, stream, time_zone=time_zone, snake=snake
    )


def _rows_to_dataframe(
    sel: Select[Any],
    rows: Iterable[Row[Any]],
    /,
    *,
    time_zone: timezone = UTC,
    snake: bool = False,
) -> DataFrame:
    """Convert a set of rows into a DataFrame."""
    columns: ReadOnlyColumnCollection = cast(Any, sel).selected_columns
    dtypes = {
        col.name: _table_column_to_dtype(col, time_zone=time_zone)
        for col in columns.values()
    }
    rows = list(rows)
    if len(rows) >= 1:
        by_cols = zip(*rows, strict=True)
        series = (
            Series(data, dtype=dtype, name=name)
            for data, (name, dtype) in zip(by_cols, dtypes.items(), strict=True)
        )
        df = concat(series, axis=1)
    else:
        df = DataFrame(columns=list(dtypes))
        df = astype(df, dtypes)
    return _dataframe_columns_to_snake(df) if snake else df


def _table_column_to_dtype(column: Column[Any], /, *, time_zone: timezone = UTC) -> Any:
    """Map a table column to a DataFrame dtype."""
    db_type = column.type
    py_type = db_type.python_type
    if issubclass(py_type, bool):
        return boolean
    if issubclass(py_type, dt.date) and not issubclass(py_type, dt.datetime):
        return datetime64ns
    if issubclass(py_type, dt.datetime):
        has_tz: bool = cast(Any, db_type).timezone
        return DatetimeTZDtype(tz=time_zone) if has_tz else datetime64ns
    if issubclass(py_type, float | Decimal):
        return float
    if issubclass(py_type, int):
        return Int64
    if issubclass(py_type, str):
        return string
    msg = f"{column=}"
    raise ColumnToPandasDTypeError(msg)


class ColumnToPandasDTypeError(Exception):
    """Raised when an invalid column type is encountered."""


def _dataframe_columns_to_snake(df: DataFrame, /) -> DataFrame:
    """Convert the columns of a DataFrame to snake case."""
    mapping = snake_case_mappings(list(df.columns))
    return df.rename(columns=mapping)


def _stream_dataframes(
    sel: Select[Any],
    engine_or_conn: Engine | Connection,
    stream: int,
    /,
    *,
    time_zone: timezone = UTC,
    snake: bool = False,
) -> Iterator[DataFrame]:
    if stream <= 0:
        raise NonPositiveStreamError(str(stream))
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield from _stream_dataframes(
                sel, conn, stream, time_zone=time_zone, snake=snake
            )
    else:
        for rows in (
            engine_or_conn.execution_options(yield_per=stream).execute(sel).partitions()
        ):
            yield _rows_to_dataframe(sel, rows, time_zone=time_zone, snake=snake)


class NonPositiveStreamError(Exception):
    """Raised when the `stream` arguemnt is non-positive."""


__all__ = [
    "ColumnToPandasDTypeError",
    "insert_pandas_dataframe",
    "NonPositiveStreamError",
    "PandasDataFrameYieldsNoRowsError",
    "select_to_pandas_dataframe",
]
