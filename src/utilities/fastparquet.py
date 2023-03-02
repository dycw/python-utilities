from collections.abc import Hashable, Mapping, Sequence
from typing import Any, Literal, Optional, Union, cast, overload

from beartype import beartype
from fastparquet import ParquetFile, write
from pandas import DataFrame, Series

from utilities.atomicwrites import writer
from utilities.iterables import is_iterable_not_str
from utilities.numpy import datetime64ns, has_dtype
from utilities.pandas import Int64, category, check_range_index, string
from utilities.pathlib import PathLike

_Compression = Literal["gzip", ",snappy", "brotli", "lz4", "zstandard"]
Compression = Union[_Compression, Mapping[Hashable, Optional[_Compression]]]
_Op = Literal["==", "=", ">", ">=", "<", "<=", "!=", "in", "not in"]
_Filter = tuple[Hashable, _Op, Any]
Filters = Union[Sequence[_Filter], Sequence[Sequence[_Filter]]]
_PARQUET_DTYPES = [bool, datetime64ns, float, Int64, string]


@beartype
def count_rows(path: PathLike, /, *, filters: Optional[Filters] = None) -> int:
    """Count the number of rows in a Parquet file."""
    return _get_parquet_file(path).count(filters=filters, **_maybe_row_filter(filters))


@beartype
def get_columns(path: PathLike, /) -> list[Hashable]:
    """Get the columns in a Parquet file."""
    return _get_parquet_file(path).columns


@beartype
def get_dtypes(path: PathLike, /) -> dict[Hashable, Any]:
    """Get the dtypes in a Parquet file.

    Note that we store strings as categoricals, so we will report `string`
    instead of category here.
    """
    dtypes = cast(dict[Hashable, Any], _get_parquet_file(path).dtypes)
    return {k: string if v == category else v for k, v in dtypes.items()}


@overload
def read_parquet(
    path: PathLike,
    /,
    *,
    head: Optional[int] = None,
    columns: Hashable,
    filters: Optional[Filters] = None,
) -> Series:
    ...


@overload
def read_parquet(
    path: PathLike,
    /,
    *,
    head: Optional[int] = None,
    columns: Optional[Sequence[Hashable]] = None,
    filters: Optional[Filters] = None,
) -> DataFrame:
    ...


@beartype
def read_parquet(
    path: PathLike,
    /,
    *,
    head: Optional[int] = None,
    columns: Optional[Union[Hashable, Sequence[Hashable]]] = None,
    filters: Optional[Filters] = None,
) -> Union[Series, DataFrame]:
    """Read a Parquet file into a Series/DataFrame."""
    file = _get_parquet_file(path)
    as_df = (columns is None) or is_iterable_not_str(columns)
    columns_use = columns if as_df else [columns]
    kwargs = _maybe_row_filter(filters)
    if head is None:
        df = file.to_pandas(columns=columns_use, filters=filters, **kwargs)
    else:
        df = file.head(head, columns=columns_use, filters=filters, **kwargs)
    dtypes = {k: string for k, v in df.items() if has_dtype(v, [category, object])}
    df = df.astype(dtypes).reset_index(drop=True)
    return df if as_df else df[columns]


@beartype
def _get_parquet_file(path: PathLike, /) -> ParquetFile:
    """Read a Parquet file."""
    return ParquetFile(path, verify=True)


@beartype
def _maybe_row_filter(filters: Optional[Filters], /) -> dict[str, bool]:
    """Add the `row_filter` argument if necessary."""
    return {} if filters is None else {"row_filter": True}


@beartype
def write_parquet(
    df: DataFrame,
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    compression: Optional[Compression] = "gzip",
) -> None:
    """Atomically write a DataFrame to a Parquet file."""
    if len(df) == 0:
        msg = f"DataFrame is empty: {df=}"
        raise EmptyDataFrameError(msg)
    check_range_index(df)
    for _, column in df.items():
        if not has_dtype(column, _PARQUET_DTYPES):
            msg = f"Invalid dtype: {column=}"
            raise TypeError(msg)
    dtypes = {k: category for k, v in df.dtypes.items() if v == string}
    df = df.astype(dtypes)
    with writer(path, overwrite=overwrite) as temp:
        write(temp, df, compression=compression)


class EmptyDataFrameError(ValueError):
    """Raised when a DataFrame is empty."""
