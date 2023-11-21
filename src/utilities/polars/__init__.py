from __future__ import annotations

from utilities.polars.polars import (
    DataFrameColumnsError,
    DataFrameDTypesError,
    DataFrameHeightError,
    DataFrameMaxHeightError,
    DataFrameMinHeightError,
    DataFrameSchemaError,
    DataFrameShapeError,
    DataFrameSortedError,
    DataFrameUniqueError,
    DataFrameWidthError,
    EmptyDataFrameError,
    check_dataframe,
    join,
    set_first_row_as_columns,
)

__all__ = [
    "check_dataframe",
    "DataFrameColumnsError",
    "DataFrameDTypesError",
    "DataFrameHeightError",
    "DataFrameMaxHeightError",
    "DataFrameMinHeightError",
    "DataFrameSchemaError",
    "DataFrameShapeError",
    "DataFrameSortedError",
    "DataFrameUniqueError",
    "DataFrameWidthError",
    "EmptyDataFrameError",
    "join",
    "set_first_row_as_columns",
]


try:
    from utilities.polars.bs4 import MultipleTHRowsError, yield_tables
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["MultipleTHRowsError", "yield_tables"]
