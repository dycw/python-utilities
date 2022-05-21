import datetime as dt
from collections.abc import Iterator
from typing import Any

from pandas import DataFrame
from pandas import Series
from sqlalchemy.engine import Engine

from dycw_utilities.numpy import datetime64ns
from dycw_utilities.numpy import has_dtype
from dycw_utilities.pandas import Int64
from dycw_utilities.pandas import boolean
from dycw_utilities.pandas import string
from dycw_utilities.pandas import timestamp_to_date
from dycw_utilities.pandas import timestamp_to_datetime
from dycw_utilities.sqlalchemy import get_column_names


def insert_dataframe(
    df: DataFrame, table_or_model: Any, engine: Engine, /
) -> None:
    """Insert a DataFrame into a database."""

    table_cols = get_column_names(table_or_model)
    df = df[[col for col in df.columns if col in table_cols]]


def nativize_column(series: Series, column: Any, /) -> Iterator[Any]:
    """Check the columns of the DataFrame form a subset of the columns of the
    table.
    """

    py_type = column.type.python_type
    if (
        (has_dtype(series, (bool, boolean)) and (py_type in {bool, int}))
        or (has_dtype(series, float) and (py_type is float))
        or (has_dtype(series, (int, Int64)) and (py_type is int))
        or (has_dtype(series, string) and (py_type is str))
    ):
        values = series.tolist()
    elif has_dtype(series, datetime64ns) and (py_type is dt.date):
        values = series.map(timestamp_to_date)
    elif has_dtype(series, datetime64ns) and (py_type is dt.datetime):
        values = series.map(timestamp_to_datetime)
    else:
        raise ValueError(f"{series=}, {py_type=}")
    for is_null, native in zip(series.isna(), values):
        yield None if is_null else native
