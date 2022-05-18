from typing import Any

from pandas import DataFrame
from pandas import Series
from sqlalchemy.engine import Engine

from dycw_utilities.numpy import has_dtype
from dycw_utilities.pandas import boolean
from dycw_utilities.sqlalchemy import get_column_names


def insert_dataframe(
    df: DataFrame, table_or_model: Any, engine: Engine, /
) -> None:
    """Insert a DataFrame into a database."""

    table_cols = get_column_names(table_or_model)
    df = df[[col for col in df.columns if col in table_cols]]


def nativize_column(series: Series, column: Any, /) -> None:
    """Check the columns of the DataFrame form a subset of the columns of the
    table.
    """

    py_type = column.type.py_type
    if has_dtype(series, (bool, boolean)) and (py_type in {bool, int}):
        pass
