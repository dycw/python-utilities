from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from functools import partial
from itertools import product
from re import search
from typing import TYPE_CHECKING, Any, Literal, NoReturn, assert_never, cast

import redis
import redis.asyncio
from redis import ResponseError
from typing_extensions import override

from utilities.datetime import (
    milliseconds_since_epoch,
    milliseconds_since_epoch_to_datetime,
)
from utilities.errors import ImpossibleCaseError
from utilities.functions import get_class_name
from utilities.iterables import always_iterable, one
from utilities.text import ensure_str
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
    from zoneinfo import ZoneInfo

    from polars import DataFrame
    from polars.datatypes import DataType
    from redis.commands.timeseries import TimeSeries
    from redis.typing import KeyT, Number

    from utilities.iterables import MaybeIterable

DuplicatePolicy = Literal["block", "first", "last", "min", "max", "sum"]
_HOST = "localhost"
_PORT = 6379
_KEY = "key"
_TIMESTAMP = "timestamp"
_VALUE = "value"
_SPLIT = "|"


def ensure_time_series_created(
    ts: TimeSeries,
    /,
    *keys: KeyT,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> None:
    """Ensure a time series/set of time series is/are created."""
    for key in set(keys):  # skipif-ci-and-not-linux
        try:
            _ = ts.create(
                key,
                retention_msecs=retention_msecs,
                uncompressed=uncompressed,
                labels=labels,
                chunk_size=chunk_size,
                duplicate_policy=duplicate_policy,
                ignore_max_time_diff=ignore_max_time_diff,
                ignore_max_val_diff=ignore_max_val_diff,
            )
        except ResponseError as error:
            _ensure_time_series_created_maybe_reraise(error)


async def ensure_time_series_created_async(
    ts: TimeSeries,
    /,
    *keys: KeyT,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> None:
    """Ensure a time series/set of time series is/are created."""
    for key in set(keys):  # skipif-ci-and-not-linux
        try:
            _ = await ts.create(
                key,
                retention_msecs=retention_msecs,
                uncompressed=uncompressed,
                labels=labels,
                chunk_size=chunk_size,
                duplicate_policy=duplicate_policy,
                ignore_max_time_diff=ignore_max_time_diff,
                ignore_max_val_diff=ignore_max_val_diff,
            )
        except ResponseError as error:
            _ensure_time_series_created_maybe_reraise(error)


def _ensure_time_series_created_maybe_reraise(error: ResponseError, /) -> None:
    """Re-raise the error if it does not match the required statement."""
    if not search(  # skipif-ci-and-not-linux
        "TSDB: key already exists", ensure_str(one(error.args))
    ):
        raise error  # pragma: no cover


def time_series_add(
    ts: TimeSeries,
    key: KeyT,
    timestamp: dt.datetime,
    value: Number,
    /,
    *,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: float | None = None,
    on_duplicate: str | None = None,
) -> int:
    """Append a sample to a time series."""
    milliseconds = milliseconds_since_epoch(  # skipif-ci-and-not-linux
        timestamp, strict=True
    )
    try:  # skipif-ci-and-not-linux
        return ts.add(
            key,
            milliseconds,
            value,
            retention_msecs=retention_msecs,
            uncompressed=uncompressed,
            labels=labels,
            chunk_size=chunk_size,
            duplicate_policy=duplicate_policy,
            ignore_max_time_diff=ignore_max_time_diff,
            ignore_max_val_diff=ignore_max_val_diff,
            on_duplicate=on_duplicate,
        )
    except ResponseError as error:  # skipif-ci-and-not-linux
        _time_series_add_classify_error(error, timestamp, value)


async def time_series_add_async(
    ts: TimeSeries,
    key: KeyT,
    timestamp: dt.datetime,
    value: Number,
    /,
    *,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: float | None = None,
    on_duplicate: str | None = None,
) -> int:
    """Append a sample to a time series."""
    milliseconds = milliseconds_since_epoch(  # skipif-ci-and-not-linux
        timestamp, strict=True
    )
    try:  # skipif-ci-and-not-linux
        return await ts.add(
            key,
            milliseconds,
            value,
            retention_msecs=retention_msecs,
            uncompressed=uncompressed,
            labels=labels,
            chunk_size=chunk_size,
            duplicate_policy=duplicate_policy,
            ignore_max_time_diff=ignore_max_time_diff,
            ignore_max_val_diff=ignore_max_val_diff,
            on_duplicate=on_duplicate,
        )
    except ResponseError as error:  # skipif-ci-and-not-linux
        _time_series_add_classify_error(error, timestamp, value)


def _time_series_add_classify_error(
    error: ResponseError, timestamp: dt.datetime, value: Number, /
) -> NoReturn:
    """Re-direct the response error from adding a time series."""
    match _classify_response_error(error):  # skipif-ci-and-not-linux
        case "error at upsert":
            raise _TimeSeriesAddErrorAtUpsertError(
                timestamp=timestamp, value=value
            ) from None
        case "invalid timestamp":
            raise _TimeSeriesAddInvalidTimestampError(
                timestamp=timestamp, value=value
            ) from None
        case "invalid value":
            raise _TimeSeriesAddInvalidValueError(
                timestamp=timestamp, value=value
            ) from None
        case "invalid key":  # pragma: no cover
            raise error


@dataclass(kw_only=True)
class TimeSeriesAddError(Exception):
    timestamp: dt.datetime
    value: float


@dataclass(kw_only=True)
class _TimeSeriesAddErrorAtUpsertError(TimeSeriesAddError):
    @override
    def __str__(self) -> str:
        return (  # skipif-ci-and-not-linux
            f"Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got {self.timestamp}"
        )


@dataclass(kw_only=True)
class _TimeSeriesAddInvalidTimestampError(TimeSeriesAddError):
    @override
    def __str__(self) -> str:
        return (  # skipif-ci-and-not-linux
            f"Timestamp must be at least the Epoch; got {self.timestamp}"
        )


@dataclass(kw_only=True)
class _TimeSeriesAddInvalidValueError(TimeSeriesAddError):
    @override
    def __str__(self) -> str:
        return f"Invalid value; got {self.value}"  # skipif-ci-and-not-linux


def time_series_add_dataframe(
    ts: TimeSeries,
    df: DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    assume_time_series_exist: bool = False,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> None:
    """Append a DataFrame of time series."""
    from polars import Utf8  # skipif-ci-and-not-linux
    from polars.selectors import float, integer  # skipif-ci-and-not-linux

    from utilities.polars import (  # skipif-ci-and-not-linux
        CheckZonedDTypeOrSeriesError,
        check_zoned_dtype_or_series,
    )

    if key not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameKeyMissingError(df=df, key=key)
    if timestamp not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameTimestampMissingError(df=df, timestamp=timestamp)
    if not isinstance(key_dtype := df.schema[key], Utf8):  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameKeyIsNotUtf8Error(df=df, key=key, dtype=key_dtype)
    timestamp_dtype = df.schema[timestamp]  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        check_zoned_dtype_or_series(timestamp_dtype)
    except CheckZonedDTypeOrSeriesError:  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameTimestampIsNotAZonedDatetimeError(
            df=df, timestamp=timestamp, dtype=timestamp_dtype
        ) from None

    for selector in [integer(), float()]:  # skipif-ci-and-not-linux
        _time_series_add_dataframe_one_selector(
            ts,
            df.select(key, timestamp, selector),
            key=key,
            timestamp=timestamp,
            assume_time_series_exist=assume_time_series_exist,
            retention_msecs=retention_msecs,
            uncompressed=uncompressed,
            labels=labels,
            chunk_size=chunk_size,
            duplicate_policy=duplicate_policy,
            ignore_max_time_diff=ignore_max_time_diff,
            ignore_max_val_diff=ignore_max_val_diff,
        )


async def time_series_add_dataframe_async(
    ts: TimeSeries,
    df: DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    assume_time_series_exist: bool = False,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> None:
    """Append a DataFrame of time series."""
    from polars import Utf8  # skipif-ci-and-not-linux
    from polars.selectors import float, integer  # skipif-ci-and-not-linux

    from utilities.polars import (  # skipif-ci-and-not-linux
        CheckZonedDTypeOrSeriesError,
        check_zoned_dtype_or_series,
    )

    if key not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameKeyMissingError(df=df, key=key)
    if timestamp not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameTimestampMissingError(df=df, timestamp=timestamp)
    if not isinstance(key_dtype := df.schema[key], Utf8):  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameKeyIsNotUtf8Error(df=df, key=key, dtype=key_dtype)
    timestamp_dtype = df.schema[timestamp]  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        check_zoned_dtype_or_series(timestamp_dtype)
    except CheckZonedDTypeOrSeriesError:  # skipif-ci-and-not-linux
        raise _TimeSeriesAddDataFrameTimestampIsNotAZonedDatetimeError(
            df=df, timestamp=timestamp, dtype=timestamp_dtype
        ) from None

    for selector in [integer(), float()]:  # skipif-ci-and-not-linux
        await _time_series_add_dataframe_one_selector_async(
            ts,
            df.select(key, timestamp, selector),
            key=key,
            timestamp=timestamp,
            assume_time_series_exist=assume_time_series_exist,
            retention_msecs=retention_msecs,
            uncompressed=uncompressed,
            labels=labels,
            chunk_size=chunk_size,
            duplicate_policy=duplicate_policy,
            ignore_max_time_diff=ignore_max_time_diff,
            ignore_max_val_diff=ignore_max_val_diff,
        )


def _time_series_add_dataframe_one_selector(
    ts: TimeSeries,
    df: DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    assume_time_series_exist: bool = False,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> None:
    """Append a DataFrame of time series, one selector."""
    import polars as pl  # skipif-ci-and-not-linux

    df_long = (  # skipif-ci-and-not-linux
        df.unpivot(index=[key, timestamp])
        .with_columns(pl.format(f"{{}}{_SPLIT}{{}}", key, "variable").alias(f"_{_KEY}"))
        .drop(key, "variable")
    )
    if df_long.is_empty():  # skipif-ci-and-not-linux
        return

    _ = time_series_madd(  # skipif-ci-and-not-linux
        ts,
        df_long,
        key=f"_{_KEY}",
        timestamp=timestamp,
        assume_time_series_exist=assume_time_series_exist,
        retention_msecs=retention_msecs,
        uncompressed=uncompressed,
        labels=labels,
        chunk_size=chunk_size,
        duplicate_policy=duplicate_policy,
        ignore_max_time_diff=ignore_max_time_diff,
        ignore_max_val_diff=ignore_max_val_diff,
    )


async def _time_series_add_dataframe_one_selector_async(
    ts: TimeSeries,
    df: DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    assume_time_series_exist: bool = False,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> None:
    """Append a DataFrame of time series, one selector."""
    import polars as pl  # skipif-ci-and-not-linux

    df_long = (  # skipif-ci-and-not-linux
        df.unpivot(index=[key, timestamp])
        .with_columns(pl.format(f"{{}}{_SPLIT}{{}}", key, "variable").alias(f"_{_KEY}"))
        .drop(key, "variable")
    )
    if df_long.is_empty():  # skipif-ci-and-not-linux
        return

    _ = await time_series_madd_async(  # skipif-ci-and-not-linux
        ts,
        df_long,
        key=f"_{_KEY}",
        timestamp=timestamp,
        assume_time_series_exist=assume_time_series_exist,
        retention_msecs=retention_msecs,
        uncompressed=uncompressed,
        labels=labels,
        chunk_size=chunk_size,
        duplicate_policy=duplicate_policy,
        ignore_max_time_diff=ignore_max_time_diff,
        ignore_max_val_diff=ignore_max_val_diff,
    )


@dataclass(kw_only=True)
class TimeSeriesAddDataFrameError(Exception):
    df: DataFrame


@dataclass(kw_only=True)
class _TimeSeriesAddDataFrameKeyMissingError(TimeSeriesAddDataFrameError):
    key: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have a {self.key!r} column; got {self.df.columns}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesAddDataFrameTimestampMissingError(TimeSeriesAddDataFrameError):
    timestamp: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have a {self.timestamp!r} column; got {self.df.columns}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesAddDataFrameKeyIsNotUtf8Error(TimeSeriesAddDataFrameError):
    df: DataFrame
    key: str
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"The {self.key!r} column must be Utf8; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesAddDataFrameTimestampIsNotAZonedDatetimeError(
    TimeSeriesAddDataFrameError
):
    df: DataFrame
    timestamp: str
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"The {self.timestamp!r} column must be a zoned Datetime; got {self.dtype}"  # skipif-ci-and-not-linux


def time_series_get(
    ts: TimeSeries, key: KeyT, /, *, latest: bool | None = False
) -> tuple[dt.datetime, float]:
    """Get the last sample of a time series."""
    milliseconds, value = ts.get(key, latest=latest)  # skipif-ci-and-not-linux
    return _time_series_parse_millliseconds(  # skipif-ci-and-not-linux
        milliseconds, value
    )


async def time_series_get_async(
    ts: TimeSeries, key: KeyT, /, *, latest: bool | None = False
) -> tuple[dt.datetime, float]:
    """Get the last sample of a time series."""
    milliseconds, value = await ts.get(key, latest=latest)  # skipif-ci-and-not-linux
    return _time_series_parse_millliseconds(  # skipif-ci-and-not-linux
        milliseconds, value
    )


def _time_series_parse_millliseconds(
    milliseconds: int, value: float, /
) -> tuple[dt.datetime, float]:
    """Get the last sample of a time series."""
    timestamp = milliseconds_since_epoch_to_datetime(  # skipif-ci-and-not-linux
        milliseconds
    )
    return timestamp, value  # skipif-ci-and-not-linux


def time_series_madd(
    ts: TimeSeries,
    values_or_df: Iterable[tuple[KeyT, dt.datetime, Number]] | DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    value: str = _VALUE,
    assume_time_series_exist: bool = False,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> list[int]:
    """Append new samples to one or more time series."""
    from polars import DataFrame  # skipif-ci-and-not-linux

    if isinstance(values_or_df, DataFrame):  # skipif-ci-and-not-linux
        triples = _time_series_madd_prepare_dataframe(
            values_or_df, key=key, timestamp=timestamp, value=value
        )
    else:  # skipif-ci-and-not-linux
        values_or_df = list(values_or_df)
        triples = list(_time_series_madd_prepare_triples(values_or_df))
    if not assume_time_series_exist:  # skipif-ci-and-not-linux
        ensure_time_series_created(
            ts,
            *{key for key, _, _ in triples},
            retention_msecs=retention_msecs,
            uncompressed=uncompressed,
            labels=labels,
            chunk_size=chunk_size,
            duplicate_policy=duplicate_policy,
            ignore_max_time_diff=ignore_max_time_diff,
            ignore_max_val_diff=ignore_max_val_diff,
        )
    result: list[int | ResponseError] = ts.madd(  # skipif-ci-and-not-linux
        list(triples)
    )
    try:  # skipif-ci-and-not-linux
        i, error = next(
            (i, r) for i, r in enumerate(result) if isinstance(r, ResponseError)
        )
    except StopIteration:  # skipif-ci-and-not-linux
        return cast(list[int], result)
    if isinstance(values_or_df, DataFrame):  # skipif-ci-and-not-linux
        error_key, error_timestamp, error_value = values_or_df.row(i)
    else:  # skipif-ci-and-not-linux
        error_key, error_timestamp, error_value = values_or_df[i]
    match _classify_response_error(error):  # skipif-ci-and-not-linux
        case "invalid key":
            raise _TimeSeriesMAddInvalidKeyError(
                values_or_df=values_or_df, key=error_key
            )
        case "invalid timestamp":
            raise _TimeSeriesMAddInvalidTimestampError(
                values_or_df=values_or_df, timestamp=error_timestamp
            )
        case "invalid value":
            raise _TimeSeriesMAddInvalidValueError(
                values_or_df=values_or_df, value=error_value
            )
        case _:  # pragma: no cover
            raise error


async def time_series_madd_async(
    ts: TimeSeries,
    values_or_df: Iterable[tuple[KeyT, dt.datetime, Number]] | DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    value: str = _VALUE,
    assume_time_series_exist: bool = False,
    retention_msecs: int | None = None,
    uncompressed: bool | None = False,
    labels: dict[str, str] | None = None,
    chunk_size: int | None = None,
    duplicate_policy: DuplicatePolicy | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: Number | None = None,
) -> list[int]:
    """Append new samples to one or more time series."""
    from polars import DataFrame  # skipif-ci-and-not-linux

    if isinstance(values_or_df, DataFrame):  # skipif-ci-and-not-linux
        triples = _time_series_madd_prepare_dataframe(
            values_or_df, key=key, timestamp=timestamp, value=value
        )
    else:  # skipif-ci-and-not-linux
        values_or_df = list(values_or_df)
        triples = list(_time_series_madd_prepare_triples(values_or_df))
    if not assume_time_series_exist:  # skipif-ci-and-not-linux
        await ensure_time_series_created_async(
            ts,
            *{key for key, _, _ in triples},
            retention_msecs=retention_msecs,
            uncompressed=uncompressed,
            labels=labels,
            chunk_size=chunk_size,
            duplicate_policy=duplicate_policy,
            ignore_max_time_diff=ignore_max_time_diff,
            ignore_max_val_diff=ignore_max_val_diff,
        )
    result: list[int | ResponseError] = await ts.madd(  # skipif-ci-and-not-linux
        list(triples)
    )
    try:  # skipif-ci-and-not-linux
        i, error = next(
            (i, r) for i, r in enumerate(result) if isinstance(r, ResponseError)
        )
    except StopIteration:  # skipif-ci-and-not-linux
        return cast(list[int], result)
    if isinstance(values_or_df, DataFrame):  # skipif-ci-and-not-linux
        error_key, error_timestamp, error_value = values_or_df.row(i)
    else:  # skipif-ci-and-not-linux
        error_key, error_timestamp, error_value = values_or_df[i]
    match _classify_response_error(error):  # skipif-ci-and-not-linux
        case "invalid key":
            raise _TimeSeriesMAddInvalidKeyError(
                values_or_df=values_or_df, key=error_key
            )
        case "invalid timestamp":
            raise _TimeSeriesMAddInvalidTimestampError(
                values_or_df=values_or_df, timestamp=error_timestamp
            )
        case "invalid value":
            raise _TimeSeriesMAddInvalidValueError(
                values_or_df=values_or_df, value=error_value
            )
        case _:  # pragma: no cover
            raise error


def _time_series_madd_prepare_dataframe(
    df: DataFrame,
    /,
    *,
    key: str = _KEY,
    timestamp: str = _TIMESTAMP,
    value: str = _VALUE,
) -> Sequence[tuple[KeyT, int, Number]]:
    """Prepare a DataFrame for `madd`."""
    import polars as pl  # skipif-ci-and-not-linux
    from polars import Float64, Int64, Utf8, col  # skipif-ci-and-not-linux

    from utilities.polars import (  # skipif-ci-and-not-linux
        CheckZonedDTypeOrSeriesError,
        check_zoned_dtype_or_series,
        zoned_datetime,
    )

    if key not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesMAddKeyMissingError(df=df, key=key)
    if timestamp not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesMAddTimestampMissingError(df=df, timestamp=timestamp)
    if value not in df.columns:  # skipif-ci-and-not-linux
        raise _TimeSeriesMAddValueMissingError(df=df, value=value)
    df = df.select(key, timestamp, value)  # skipif-ci-and-not-linux
    if not isinstance(key_dtype := df.schema[key], Utf8):  # skipif-ci-and-not-linux
        raise _TimeSeriesMAddKeyIsNotUtf8Error(df=df, key=key, dtype=key_dtype)
    timestamp_dtype = df.schema[timestamp]  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        check_zoned_dtype_or_series(timestamp_dtype)
    except CheckZonedDTypeOrSeriesError:  # skipif-ci-and-not-linux
        raise _TimeSeriesMAddTimestampIsNotAZonedDatetimeError(
            df=df, timestamp=timestamp, dtype=timestamp_dtype
        ) from None
    df = df.with_columns(  # skipif-ci-and-not-linux
        col(timestamp)
        .cast(zoned_datetime(time_unit="ms", time_zone=UTC))
        .dt.epoch(time_unit="ms")
    )
    if not isinstance(  # skipif-ci-and-not-linux
        value_dtype := df.schema[value], Int64 | Float64
    ):
        raise _TimeSeriesMAddValueIsNotNumericError(
            df=df, value=value, dtype=value_dtype
        )
    cls_name = get_class_name(value_dtype)  # skipif-ci-and-not-linux
    return df.with_columns(  # skipif-ci-and-not-linux
        pl.format(f"{{}}{_SPLIT}{cls_name}", key)
    ).rows()


def _time_series_madd_prepare_triples(
    values: Iterable[tuple[KeyT, dt.datetime, Number]], /
) -> Iterator[tuple[KeyT, int, Number]]:
    """Prepare a set of triples for `madd`."""
    from polars import Float64, Int64  # skipif-ci-and-not-linux

    for key, timestamp, value in values:  # skipif-ci-and-not-linux
        milliseconds = milliseconds_since_epoch(timestamp, strict=True)
        dtype = Int64 if isinstance(value, int) else Float64
        cls_name = get_class_name(dtype)
        full_key = f"{key}{_SPLIT}{cls_name}"
        yield full_key, milliseconds, value


@dataclass(kw_only=True)
class TimeSeriesMAddError(Exception): ...


@dataclass(kw_only=True)
class _TimeSeriesMAddKeyMissingError(TimeSeriesMAddError):
    df: DataFrame
    key: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have a {self.key!r} column; got {self.df.columns}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddTimestampMissingError(TimeSeriesMAddError):
    df: DataFrame
    timestamp: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have a {self.timestamp!r} column; got {self.df.columns}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddValueMissingError(TimeSeriesMAddError):
    df: DataFrame
    value: str

    @override
    def __str__(self) -> str:
        return f"DataFrame must have a {self.value!r} column; got {self.df.columns}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddKeyIsNotUtf8Error(TimeSeriesMAddError):
    df: DataFrame
    key: str
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"The {self.key!r} column must be Utf8; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddTimestampIsNotAZonedDatetimeError(TimeSeriesMAddError):
    df: DataFrame
    timestamp: str
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"The {self.timestamp!r} column must be a zoned Datetime; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddValueIsNotNumericError(TimeSeriesMAddError):
    df: DataFrame
    value: str
    dtype: DataType

    @override
    def __str__(self) -> str:
        return f"The {self.value!r} column must be numeric; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddInvalidKeyError(TimeSeriesMAddError):
    values_or_df: Sequence[tuple[KeyT, dt.datetime, Number]] | DataFrame
    key: KeyT

    @override
    def __str__(self) -> str:
        return f"The key {self.key!r} must exist"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddInvalidTimestampError(TimeSeriesMAddError):
    values_or_df: Sequence[tuple[KeyT, dt.datetime, Number]] | DataFrame
    timestamp: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Timestamps must be at least the Epoch; got {self.timestamp}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddInvalidValueError(TimeSeriesMAddError):
    values_or_df: Sequence[tuple[KeyT, dt.datetime, Number]] | DataFrame
    value: float

    @override
    def __str__(self) -> str:
        return f"The value {self.value} is invalid"  # skipif-ci-and-not-linux


def time_series_range(
    ts: TimeSeries,
    key: MaybeIterable[KeyT],
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
    output_value: str = _VALUE,
) -> DataFrame:
    """Get a range in forward direction."""
    from polars import concat  # skipif-ci-and-not-linux

    key = list(always_iterable(key))  # skipif-ci-and-not-linux)
    dfs = (  # skipif-ci-and-not-linux
        _time_series_range_one_key(
            ts,
            k,
            from_time=from_time,
            to_time=to_time,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts,
            filter_by_min_value=filter_by_min_value,
            filter_by_max_value=filter_by_max_value,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
            output_key=output_key,
            output_timestamp=output_timestamp,
            output_time_zone=output_time_zone,
            output_value=output_value,
        )
        for k in key
    )
    try:  # skipif-ci-and-not-linux
        return concat(dfs)
    except ValueError:  # skipif-ci-and-not-linux
        raise _TimeSeriesRangeNoKeysRequestedError(key=key) from None


async def time_series_range_async(
    ts: TimeSeries,
    key: MaybeIterable[KeyT],
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
    output_value: str = _VALUE,
) -> DataFrame:
    """Get a range in forward direction."""
    from polars import concat  # skipif-ci-and-not-linux

    key = list(always_iterable(key))  # skipif-ci-and-not-linux)
    dfs = [  # skipif-ci-and-not-linux
        await _time_series_range_one_key_async(
            ts,
            k,
            from_time=from_time,
            to_time=to_time,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts,
            filter_by_min_value=filter_by_min_value,
            filter_by_max_value=filter_by_max_value,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
            output_key=output_key,
            output_timestamp=output_timestamp,
            output_time_zone=output_time_zone,
            output_value=output_value,
        )
        for k in key
    ]
    try:  # skipif-ci-and-not-linux
        return concat(dfs)
    except ValueError:  # skipif-ci-and-not-linux
        raise _TimeSeriesRangeNoKeysRequestedError(key=key) from None


def _time_series_range_one_key(
    ts: TimeSeries,
    key: KeyT,
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
    output_value: str = _VALUE,
) -> DataFrame:
    """Get a range for one key."""
    res_int64, res_float64 = (  # skipif-ci-and-not-linux
        _time_series_range_one_key_one_dtype(
            ts,
            key,
            cast(Literal["Int64", "Float64"], dtype),
            from_time=from_time,
            to_time=to_time,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts,
            filter_by_min_value=filter_by_min_value,
            filter_by_max_value=filter_by_max_value,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
            output_key=output_key,
            output_timestamp=output_timestamp,
            output_time_zone=output_time_zone,
            output_value=output_value,
        )
        for dtype in ["Int64", "Float64"]
    )
    if (res_int64 is None) and (res_float64 is None):  # skipif-ci-and-not-linux
        raise _TimeSeriesRangeInvalidKeyError(key=key)
    if (res_int64 is not None) and (res_float64 is None):  # skipif-ci-and-not-linux
        return res_int64
    if (res_int64 is None) and (res_float64 is not None):  # skipif-ci-and-not-linux
        return res_float64
    if (res_int64 is not None) and (res_float64 is not None):  # skipif-ci-and-not-linux
        raise _TimeSeriesRangeKeyWithInt64AndFloat64Error(key=key)
    raise ImpossibleCaseError(  # pragma: no cover
        case=[f"{res_int64=}", f"{res_float64=}"]
    )


async def _time_series_range_one_key_async(
    ts: TimeSeries,
    key: KeyT,
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
    output_value: str = _VALUE,
) -> DataFrame:
    """Get a range for one key."""
    res_int64, res_float64 = [  # skipif-ci-and-not-linux
        await _time_series_range_one_key_one_dtype_async(
            ts,
            key,
            cast(Literal["Int64", "Float64"], dtype),
            from_time=from_time,
            to_time=to_time,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts,
            filter_by_min_value=filter_by_min_value,
            filter_by_max_value=filter_by_max_value,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
            output_key=output_key,
            output_timestamp=output_timestamp,
            output_time_zone=output_time_zone,
            output_value=output_value,
        )
        for dtype in ["Int64", "Float64"]
    ]
    if (res_int64 is None) and (res_float64 is None):  # skipif-ci-and-not-linux
        raise _TimeSeriesRangeInvalidKeyError(key=key)
    if (res_int64 is not None) and (res_float64 is None):  # skipif-ci-and-not-linux
        return res_int64
    if (res_int64 is None) and (res_float64 is not None):  # skipif-ci-and-not-linux
        return res_float64
    if (res_int64 is not None) and (res_float64 is not None):  # skipif-ci-and-not-linux
        raise _TimeSeriesRangeKeyWithInt64AndFloat64Error(key=key)
    raise ImpossibleCaseError(  # pragma: no cover
        case=[f"{res_int64=}", f"{res_float64=}"]
    )


def _time_series_range_one_key_one_dtype(
    ts: TimeSeries,
    key: KeyT,
    dtype: Literal["Int64", "Float64"],
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
    output_value: str = _VALUE,
) -> DataFrame | None:
    """Get a range for one key, one dtype."""
    from polars import (  # skipif-ci-and-not-linux
        DataFrame,
        Float64,
        Int64,
        Utf8,
        from_epoch,
        lit,
    )

    from utilities.polars import DatetimeUTC, zoned_datetime  # skipif-ci-and-not-linux

    ms_since_epoch = partial(  # skipif-ci-and-not-linux
        milliseconds_since_epoch, strict=True
    )
    from_time_use = (  # skipif-ci-and-not-linux
        "-" if from_time is None else ms_since_epoch(from_time)
    )
    to_time_use = (  # skipif-ci-and-not-linux
        "+" if to_time is None else ms_since_epoch(to_time)
    )
    filter_by_ts_use = (  # skipif-ci-and-not-linux
        None if filter_by_ts is None else list(map(ms_since_epoch, filter_by_ts))
    )
    filter_by_min_value_use = (  # skipif-ci-and-not-linux
        None if filter_by_min_value is None else ms_since_epoch(filter_by_min_value)
    )
    filter_by_max_value_use = (  # skipif-ci-and-not-linux
        None if filter_by_max_value is None else ms_since_epoch(filter_by_max_value)
    )
    output_dtype = zoned_datetime(time_zone=output_time_zone)  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        values = ts.range(
            f"{key}{_SPLIT}{dtype}",
            from_time_use,
            to_time_use,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts_use,
            filter_by_min_value=filter_by_min_value_use,
            filter_by_max_value=filter_by_max_value_use,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
        )
    except ResponseError as error:  # skipif-ci-and-not-linux
        match _classify_response_error(error):
            case "invalid key":
                return None
            case _:  # pragma: no cover
                raise
    match dtype:  # skipif-ci-and-not-linux
        case "Int64":
            dtype_use = Int64
        case "Float64":
            dtype_use = Float64
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    return DataFrame(  # skipif-ci-and-not-linux
        values, schema={output_timestamp: Int64, output_value: dtype_use}, orient="row"
    ).select(
        lit(key, dtype=Utf8).alias(output_key),
        from_epoch(output_timestamp, time_unit="ms")
        .cast(DatetimeUTC)
        .cast(output_dtype),
        output_value,
    )


async def _time_series_range_one_key_one_dtype_async(
    ts: TimeSeries,
    key: KeyT,
    dtype: Literal["Int64", "Float64"],
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
    output_value: str = _VALUE,
) -> DataFrame | None:
    """Get a range for one key, one dtype."""
    from polars import (  # skipif-ci-and-not-linux
        DataFrame,
        Float64,
        Int64,
        Utf8,
        from_epoch,
        lit,
    )

    from utilities.polars import DatetimeUTC, zoned_datetime  # skipif-ci-and-not-linux

    ms_since_epoch = partial(  # skipif-ci-and-not-linux
        milliseconds_since_epoch, strict=True
    )
    from_time_use = (  # skipif-ci-and-not-linux
        "-" if from_time is None else ms_since_epoch(from_time)
    )
    to_time_use = (  # skipif-ci-and-not-linux
        "+" if to_time is None else ms_since_epoch(to_time)
    )
    filter_by_ts_use = (  # skipif-ci-and-not-linux
        None if filter_by_ts is None else list(map(ms_since_epoch, filter_by_ts))
    )
    filter_by_min_value_use = (  # skipif-ci-and-not-linux
        None if filter_by_min_value is None else ms_since_epoch(filter_by_min_value)
    )
    filter_by_max_value_use = (  # skipif-ci-and-not-linux
        None if filter_by_max_value is None else ms_since_epoch(filter_by_max_value)
    )
    output_dtype = zoned_datetime(time_zone=output_time_zone)  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        values = await ts.range(
            f"{key}{_SPLIT}{dtype}",
            from_time_use,
            to_time_use,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts_use,
            filter_by_min_value=filter_by_min_value_use,
            filter_by_max_value=filter_by_max_value_use,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
        )
    except ResponseError as error:  # skipif-ci-and-not-linux
        match _classify_response_error(error):
            case "invalid key":
                return None
            case _:  # pragma: no cover
                raise
    match dtype:  # skipif-ci-and-not-linux
        case "Int64":
            dtype_use = Int64
        case "Float64":
            dtype_use = Float64
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    return DataFrame(  # skipif-ci-and-not-linux
        values, schema={output_timestamp: Int64, output_value: dtype_use}, orient="row"
    ).select(
        lit(key, dtype=Utf8).alias(output_key),
        from_epoch(output_timestamp, time_unit="ms")
        .cast(DatetimeUTC)
        .cast(output_dtype),
        output_value,
    )


@dataclass(kw_only=True)
class TimeSeriesRangeError(Exception): ...


@dataclass(kw_only=True)
class _TimeSeriesRangeNoKeysRequestedError(TimeSeriesRangeError):
    key: Sequence[KeyT]

    @override
    def __str__(self) -> str:
        return f"At least 1 key must be requested; got {self.key}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesRangeInvalidKeyError(TimeSeriesRangeError):
    key: KeyT

    @override
    def __str__(self) -> str:
        return f"The key {self.key!r} must exist"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesRangeKeyWithInt64AndFloat64Error(TimeSeriesRangeError):
    key: KeyT

    @override
    def __str__(self) -> str:
        return f"The key {self.key!r} contains both Int64 and Float64 data"  # skipif-ci-and-not-linux


def time_series_read_dataframe(
    ts: TimeSeries,
    keys: MaybeIterable[KeyT],
    columns: MaybeIterable[str],
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
) -> DataFrame:
    """Read a DataFrame of time series."""
    from polars import Float64, Int64  # skipif-ci-and-not-linux

    keys = list(always_iterable(keys))  # skipif-ci-and-not-linux
    if len(keys) == 0:  # skipif-ci-and-not-linux
        raise _TimeSeriesReadDataFrameNoKeysRequestedError(keys=keys)
    columns = list(always_iterable(columns))  # skipif-ci-and-not-linux
    if len(columns) == 0:  # skipif-ci-and-not-linux
        raise _TimeSeriesReadDataFrameNoColumnsRequestedError(columns=columns)
    pairs = list(  # skipif-ci-and-not-linux
        product(always_iterable(keys), always_iterable(columns))
    )
    dfs = [  # skipif-ci-and-not-linux
        time_series_range(
            ts,
            f"{key}{_SPLIT}{column}",
            from_time=from_time,
            to_time=to_time,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts,
            filter_by_min_value=filter_by_min_value,
            filter_by_max_value=filter_by_max_value,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
            output_key=f"_{_KEY}",
            output_timestamp=output_timestamp,
            output_time_zone=output_time_zone,
        )
        for key, column in pairs
    ]
    df_int, df_float = (  # skipif-ci-and-not-linux
        _time_series_read_dataframe_concat(
            (df for df in dfs if isinstance(df["value"].dtype, dtype)),
            output_key=output_key,
            output_timestamp=output_timestamp,
        )
        for dtype in [Int64, Float64]
    )
    if (df_int is not None) and (df_float is None):  # skipif-ci-and-not-linux
        return df_int
    if (df_int is None) and (df_float is not None):  # skipif-ci-and-not-linux
        return df_float
    if (df_int is not None) and (df_float is not None):  # skipif-ci-and-not-linux
        return (  # skipif-ci-and-not-linux
            df_int.join(df_float, on=[output_key, output_timestamp], how="full")
            .drop(f"{output_key}_right", f"{output_timestamp}_right")
            .select(output_key, output_timestamp, *columns)
        )
    raise ImpossibleCaseError(case=[f"{df_int=}", f"{df_float=}"])  # pragma: no cover


async def time_series_read_dataframe_async(
    ts: TimeSeries,
    keys: MaybeIterable[KeyT],
    columns: MaybeIterable[str],
    /,
    *,
    from_time: dt.datetime | None = None,
    to_time: dt.datetime | None = None,
    count: int | None = None,
    aggregation_type: str | None = None,
    bucket_size_msec: int | None = 0,
    filter_by_ts: list[dt.datetime] | None = None,
    filter_by_min_value: dt.datetime | None = None,
    filter_by_max_value: dt.datetime | None = None,
    align: int | str | None = None,
    latest: bool | None = False,
    bucket_timestamp: str | None = None,
    empty: bool | None = False,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
    output_time_zone: ZoneInfo = UTC,
) -> DataFrame:
    """Read a DataFrame of time series."""
    from polars import Float64, Int64  # skipif-ci-and-not-linux

    keys = list(always_iterable(keys))  # skipif-ci-and-not-linux
    if len(keys) == 0:  # skipif-ci-and-not-linux
        raise _TimeSeriesReadDataFrameNoKeysRequestedError(keys=keys)
    columns = list(always_iterable(columns))  # skipif-ci-and-not-linux
    if len(columns) == 0:  # skipif-ci-and-not-linux
        raise _TimeSeriesReadDataFrameNoColumnsRequestedError(columns=columns)
    pairs = list(  # skipif-ci-and-not-linux
        product(always_iterable(keys), always_iterable(columns))
    )
    dfs = [  # skipif-ci-and-not-linux
        await time_series_range_async(
            ts,
            f"{key}{_SPLIT}{column}",
            from_time=from_time,
            to_time=to_time,
            count=count,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_msec,
            filter_by_ts=filter_by_ts,
            filter_by_min_value=filter_by_min_value,
            filter_by_max_value=filter_by_max_value,
            align=align,
            latest=latest,
            bucket_timestamp=bucket_timestamp,
            empty=empty,
            output_key=f"_{_KEY}",
            output_timestamp=output_timestamp,
            output_time_zone=output_time_zone,
        )
        for key, column in pairs
    ]
    df_int, df_float = (  # skipif-ci-and-not-linux
        _time_series_read_dataframe_concat(
            (df for df in dfs if isinstance(df["value"].dtype, dtype)),
            output_key=output_key,
            output_timestamp=output_timestamp,
        )
        for dtype in [Int64, Float64]
    )
    if (df_int is not None) and (df_float is None):  # skipif-ci-and-not-linux
        return df_int
    if (df_int is None) and (df_float is not None):  # skipif-ci-and-not-linux
        return df_float
    if (df_int is not None) and (df_float is not None):  # skipif-ci-and-not-linux
        return (  # skipif-ci-and-not-linux
            df_int.join(df_float, on=[output_key, output_timestamp], how="full")
            .drop(f"{output_key}_right", f"{output_timestamp}_right")
            .select(output_key, output_timestamp, *columns)
        )
    raise ImpossibleCaseError(case=[f"{df_int=}", f"{df_float=}"])  # pragma: no cover


def _time_series_read_dataframe_concat(
    dfs: Iterable[DataFrame],
    /,
    *,
    output_key: str = _KEY,
    output_timestamp: str = _TIMESTAMP,
) -> DataFrame | None:
    """Concat the key/column DataFrame parts."""
    from polars import col, concat  # skipif-ci-and-not-linux

    try:  # skipif-ci-and-not-linux
        df = concat(dfs)
    except ValueError:  # skipif-ci-and-not-linux
        return None
    df = df.with_columns(  # skipif-ci-and-not-linux
        col(f"_{_KEY}")
        .str.split_exact(_SPLIT, 1)
        .struct.rename_fields([output_key, "_variable"])
    )
    return df.unnest(f"_{_KEY}").pivot(  # skipif-ci-and-not-linux
        "_variable", index=[output_key, output_timestamp]
    )


@dataclass(kw_only=True)
class TimeSeriesReadDataFrameError(Exception): ...


@dataclass(kw_only=True)
class _TimeSeriesReadDataFrameNoKeysRequestedError(TimeSeriesReadDataFrameError):
    keys: Sequence[KeyT]

    @override
    def __str__(self) -> str:
        return f"At least 1 key must be requested; got {self.keys}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesReadDataFrameNoColumnsRequestedError(TimeSeriesReadDataFrameError):
    columns: Sequence[str]

    @override
    def __str__(self) -> str:
        return f"At least 1 column must be requested; got {self.columns}"  # skipif-ci-and-not-linux


@contextmanager
def yield_client(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: int = 0,
    password: str | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> Iterator[redis.Redis]:
    """Yield a synchronous client."""
    client = redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=decode_responses,
        **kwargs,
    )
    try:
        yield client
    finally:
        client.close()


@asynccontextmanager
async def yield_client_async(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: str | int = 0,
    password: str | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> AsyncIterator[redis.asyncio.Redis]:
    """Yield an asynchronous client."""
    client = redis.asyncio.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=decode_responses,
        **kwargs,
    )
    try:
        yield client
    finally:
        await client.aclose()


@contextmanager
def yield_time_series(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: int = 0,
    password: str | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> Iterator[TimeSeries]:
    """Yield a synchronous time series client."""
    with yield_client(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=decode_responses,
        **kwargs,
    ) as client:
        yield client.ts()


@asynccontextmanager
async def yield_time_series_async(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: int = 0,
    password: str | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> AsyncIterator[TimeSeries]:
    """Yield an asynchronous time series client."""
    async with yield_client_async(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=decode_responses,
        **kwargs,
    ) as client:
        yield client.ts()


_ResponseErrorKind = Literal[
    "error at upsert", "invalid key", "invalid timestamp", "invalid value"
]


def _classify_response_error(error: ResponseError, /) -> _ResponseErrorKind:
    msg = ensure_str(one(error.args))  # skipif-ci-and-not-linux
    if (  # skipif-ci-and-not-linux
        msg
        == "TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode"
    ):
        return "error at upsert"
    if msg in {  # skipif-ci-and-not-linux
        "TSDB: the key does not exist",
        "TSDB: the key is not a TSDB key",
    }:
        return "invalid key"
    if (  # skipif-ci-and-not-linux
        msg == "TSDB: invalid timestamp, must be a nonnegative integer"
    ):
        return "invalid timestamp"
    if msg == "TSDB: invalid value":  # skipif-ci-and-not-linux
        return "invalid value"
    raise ImpossibleCaseError(case=[f"{msg=}"])  # pragma: no cover


__all__ = [
    "TimeSeriesAddDataFrameError",
    "TimeSeriesAddError",
    "TimeSeriesMAddError",
    "TimeSeriesRangeError",
    "TimeSeriesReadDataFrameError",
    "ensure_time_series_created",
    "ensure_time_series_created_async",
    "time_series_add",
    "time_series_add_async",
    "time_series_add_dataframe",
    "time_series_add_dataframe_async",
    "time_series_get",
    "time_series_get_async",
    "time_series_madd",
    "time_series_madd_async",
    "time_series_range",
    "time_series_range_async",
    "time_series_read_dataframe",
    "time_series_read_dataframe_async",
    "yield_client",
    "yield_client_async",
    "yield_time_series",
    "yield_time_series_async",
]
