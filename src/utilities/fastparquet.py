from __future__ import annotations

from collections.abc import Hashable, Iterable, Mapping, Sequence
from typing import Any, Literal, Optional, Union, cast, overload

from fastparquet import ParquetFile, write
from pandas import DataFrame, Series

from utilities.atomicwrites import writer
from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.math.typing import IntNonNeg
from utilities.numpy import datetime64ns, has_dtype
from utilities.pandas import Int64, check_range_index, string
from utilities.pathlib import PathLike

_Compression = Literal["gzip", ",snappy", "brotli", "lz4", "zstandard"]
Compression = Union[_Compression, Mapping[Hashable, Optional[_Compression]]]
_Op = Literal["==", "=", ">", ">=", "<", "<=", "!=", "in", "not in"]
_Filter = tuple[Hashable, _Op, Any]
Filters = Union[Sequence[_Filter], Sequence[Sequence[_Filter]]]
_PARQUET_DTYPES = {bool, cast(Hashable, datetime64ns), float, Int64, string}


def count_rows(path: PathLike, /, *, filters: Filters | None = None) -> int:
    """Count the number of rows in a Parquet file."""
    return _get_parquet_file(path).count(
        filters=filters, **_maybe_row_filter(filters)
    )


def get_columns(path: PathLike, /) -> list[Hashable]:
    """Get the columns in a Parquet file."""
    return _get_parquet_file(path).columns


def get_dtypes(path: PathLike, /) -> dict[Hashable, Any]:
    """Get the dtypes in a Parquet file.

    Note that we store strings as categoricals, so we will report `string`
    instead of category here.
    """
    dtypes = cast(dict[Hashable, Any], _get_parquet_file(path).dtypes)
    return {k: string if v == object else v for k, v in dtypes.items()}


def get_num_row_groups(path: PathLike, /) -> IntNonNeg:
    """Get the number of row groups in a Parquet file."""
    return len(_get_parquet_file(path).row_groups)


@overload
def read_parquet(
    path: PathLike,
    /,
    *,
    head: IntNonNeg | None = None,
    row_group: IntNonNeg | None = None,
    columns: Hashable,
    filters: Filters | None = None,
) -> Series[Any]:
    ...


@overload
def read_parquet(
    path: PathLike,
    /,
    *,
    head: IntNonNeg | None = None,
    row_group: IntNonNeg | None = None,
    columns: Sequence[Hashable] | None = None,
    filters: Filters | None = None,
) -> DataFrame:
    ...


def read_parquet(
    path: PathLike,
    /,
    *,
    head: IntNonNeg | None = None,
    row_group: IntNonNeg | None = None,
    columns: Hashable | Sequence[Hashable] | None = None,
    filters: Filters | None = None,
) -> Series[Any] | DataFrame:
    """Read a Parquet file into a Series/DataFrame."""
    file = _get_parquet_file(path, row_group=row_group)
    as_df = (columns is None) or is_iterable_not_str(columns)
    columns_use = columns if as_df else [columns]
    kwargs = _maybe_row_filter(filters)
    if head is None:
        df = file.to_pandas(columns=columns_use, filters=filters, **kwargs)
    else:
        df = file.head(head, columns=columns_use, filters=filters, **kwargs)
    dtypes = {k: string for k, v in df.items() if has_dtype(v, object)}
    df = df.astype(dtypes).reset_index(drop=True)
    return df if as_df else df[columns]


def _get_parquet_file(
    path: PathLike, /, *, row_group: IntNonNeg | None = None
) -> ParquetFile:
    """Read a Parquet file."""
    try:
        file = ParquetFile(path, verify=True)
    except TypeError as error:
        msg = f"{path=}"
        new = FileNotFoundError(msg)
        redirect_error(
            error, "argument of type 'PosixPath' is not iterable", new
        )
    if row_group is None:
        return file
    try:
        return file[row_group]
    except IndexError as error:
        msg = f"{path=}, {row_group=}"
        redirect_error(
            error, "list index out of range", InvalidRowGroupIndexError(msg)
        )


class InvalidRowGroupIndexError(IndexError):
    """Raised when a row group index is invalid."""


def _maybe_row_filter(filters: Filters | None, /) -> dict[str, bool]:
    """Add the `row_filter` argument if necessary."""
    return {} if filters is None else {"row_filter": True}


def write_parquet(
    df: DataFrame | Iterable[DataFrame],
    path: PathLike,
    /,
    *,
    extra_dtypes: Mapping[Hashable, Any] | None = None,
    overwrite: bool = False,
    row_group_offsets: IntNonNeg | list[IntNonNeg] | None = None,
    compression: Compression | None = "gzip",
) -> None:
    """Atomically write a DataFrame to a Parquet file."""
    with writer(path, overwrite=overwrite) as temp:
        if isinstance(df, DataFrame):
            _write_parquet_core(
                df,
                temp,
                extra_dtypes=extra_dtypes,
                row_group_offsets=row_group_offsets,
                compression=compression,
            )
        else:
            for i, df_i in enumerate(df):
                _write_parquet_core(
                    df_i,
                    temp,
                    extra_dtypes=extra_dtypes,
                    row_group_offsets=row_group_offsets,
                    compression=compression,
                    append=i >= 1,
                )


def _write_parquet_core(
    df: DataFrame,
    path: PathLike,
    /,
    *,
    extra_dtypes: Mapping[Hashable, Any] | None = None,
    row_group_offsets: IntNonNeg | list[IntNonNeg] | None = None,
    compression: Compression | None = "gzip",
    append: bool = False,
) -> None:
    """Atomically write a DataFrame to a Parquet file."""
    if len(df) == 0:
        msg = f"{df=}"
        raise EmptyDataFrameError(msg)
    check_range_index(df)
    for name, column in df.items():
        allowed_dtypes = _PARQUET_DTYPES
        if extra_dtypes is not None:
            try:
                extra_dtype = extra_dtypes[name]
            except KeyError:
                pass
            else:
                allowed_dtypes = allowed_dtypes | {extra_dtype}
        if not has_dtype(column, allowed_dtypes):
            msg = f"{column=}"
            raise InvalidDTypeError(msg)
    write(
        path,
        df,
        row_group_offsets=row_group_offsets,
        compression=compression,
        append=append,
    )


class EmptyDataFrameError(ValueError):
    """Raised when a DataFrame is empty."""


class InvalidDTypeError(TypeError):
    """Raised when an invalid dtype is encountered."""
