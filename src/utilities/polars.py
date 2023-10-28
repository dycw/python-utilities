from __future__ import annotations

from polars import DataFrame
from polars.type_aliases import PolarsDataType, SchemaDict

from utilities.typing import SequenceStrs


def check_dataframe(
    df: DataFrame,
    /,
    *,
    columns: SequenceStrs | None = None,
    dtypes: list[PolarsDataType] | None = None,
    height: int | None = None,
    schema: SchemaDict | None = None,
    shape: tuple[int, int] | None = None,
    width: int | None = None,
) -> None:
    if (columns is not None) and (df.columns != list(columns)):
        msg = f"{df=}, {columns=}"
        raise DataFrameColumnsError(msg)
    if (dtypes is not None) and (df.dtypes != dtypes):
        msg = f"{df=}, {dtypes=}"
        raise DataFrameDTypesError(msg)
    if (height is not None) and (df.height != height):
        msg = f"{df=}"
        raise DataFrameHeightError(msg)
    if (schema is not None) and (df.schema != schema):
        msg = f"{df=}"
        raise DataFrameSchemaError(msg)
    if (shape is not None) and (df.shape != shape):
        msg = f"{df=}"
        raise DataFrameShapeError(msg)
    if (width is not None) and (df.width != width):
        msg = f"{df=}"
        raise DataFrameWidthError(msg)


class DataFrameColumnsError(ValueError):
    """Raised when a DataFrame has the incorrect columns."""


class DataFrameDTypesError(ValueError):
    """Raised when a DataFrame has the incorrect dtypes."""


class DataFrameHeightError(ValueError):
    """Raised when a DataFrame has the incorrect height."""


class DataFrameSchemaError(ValueError):
    """Raised when a DataFrame has the incorrect schema."""


class DataFrameShapeError(ValueError):
    """Raised when a DataFrame has the incorrect shape."""


class DataFrameWidthError(ValueError):
    """Raised when a DataFrame has the incorrect width."""
