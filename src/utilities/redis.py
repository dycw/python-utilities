from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from functools import partial
from re import search
from typing import TYPE_CHECKING, Any, Literal, cast

import redis
import redis.asyncio
from redis import ResponseError
from typing_extensions import override

from utilities.datetime import (
    milliseconds_since_epoch,
    milliseconds_since_epoch_to_datetime,
)
from utilities.errors import ImpossibleCaseError
from utilities.iterables import one
from utilities.more_itertools import always_iterable
from utilities.text import ensure_str

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncIterator, Iterable, Iterator, Sequence

    from polars import DataFrame
    from polars._typing import PolarsDataType
    from redis.commands.timeseries import TimeSeries
    from redis.typing import KeyT, Number
    from sqlalchemy.util.typing import resolve_name_to_real_class_name

    from utilities.iterables import MaybeIterable

DuplicatePolicy = Literal["block", "first", "last", "min", "max", "sum"]
_HOST = "localhost"
_PORT = 6379


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
    """Ensure a set of time series are created."""
    for key in keys:
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
    for key in keys:
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


def _ensure_time_series_created_maybe_reraise(
    error: resolve_name_to_real_class_name, /
) -> None:
    """Re-raise the error if it does not match the required statement."""
    if not search("TSDB: key already exists", ensure_str(one(error.args))):
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
        match _classify_response_error(error):
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
                raise


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


def time_series_get(
    ts: TimeSeries, key: KeyT, /, *, latest: bool | None = False
) -> tuple[dt.datetime, float]:
    """Get the last sample of a time series."""
    milliseconds, value = ts.get(key, latest=latest)  # skipif-ci-and-not-linux
    timestamp = milliseconds_since_epoch_to_datetime(  # skipif-ci-and-not-linux
        milliseconds
    )
    return timestamp, value  # skipif-ci-and-not-linux


def time_series_madd(
    ts: TimeSeries,
    values_or_df: Iterable[tuple[KeyT, dt.datetime, Number]] | DataFrame,
    /,
) -> list[int]:
    """Append new samples to one or more time series."""
    from polars import (  # skipif-ci-and-not-linux
        DataFrame,
        Datetime,
        Float64,
        Int64,
        Utf8,
        col,
    )

    ktv_tuples: Sequence[tuple[KeyT, int, Number]]  # skipif-ci-and-not-linux
    if isinstance(values_or_df, DataFrame):  # skipif-ci-and-not-linux
        df = values_or_df.select("key", "timestamp", "value")
        key_dtype, timestamp_dtype, value_dtype = df.dtypes
        if not isinstance(key_dtype, Utf8):
            raise _TimeSeriesMAddKeyIsNotUtf8Error(df=df, dtype=key_dtype)
        if not isinstance(timestamp_dtype, Datetime):
            raise _TimeSeriesMAddTimestampIsNotDatetimeError(
                df=df, dtype=timestamp_dtype
            )
        df = df.with_columns(
            timestamp=col("timestamp")
            .cast(Datetime(time_unit="ms", time_zone="UTC"))
            .dt.epoch(time_unit="ms")
        )
        if not isinstance(value_dtype, Float64 | Int64):
            raise _TimeSeriesMAddValueIsNotNumericError(df=df, dtype=value_dtype)
        ktv_tuples = df.rows()
    else:  # skipif-ci-and-not-linux
        values_or_df = list(values_or_df)
        ktv_tuples = [
            (key, milliseconds_since_epoch(timestamp, strict=True), value)
            for key, timestamp, value in values_or_df
        ]
    result: list[int | ResponseError] = ts.madd(  # skipif-ci-and-not-linux
        list(ktv_tuples)
    )
    try:  # skipif-ci-and-not-linux
        i, error = next(
            (i, r) for i, r in enumerate(result) if isinstance(r, ResponseError)
        )
    except StopIteration:  # skipif-ci-and-not-linux
        return cast(list[int], result)
    if isinstance(values_or_df, DataFrame):  # skipif-ci-and-not-linux
        key, timestamp, value = values_or_df.row(i)
    else:  # skipif-ci-and-not-linux
        key, timestamp, value = values_or_df[i]
    match _classify_response_error(error):  # skipif-ci-and-not-linux
        case "invalid key":
            raise _TimeSeriesMAddInvalidKeyError(values_or_df=values_or_df, key=key)
        case "invalid timestamp":
            raise _TimeSeriesMAddInvalidTimestampError(
                values_or_df=values_or_df, timestamp=timestamp
            )
        case "invalid value":
            raise _TimeSeriesMAddInvalidValueError(
                values_or_df=values_or_df, value=value
            )
        case _:  # pragma: no cover
            raise error


@dataclass(kw_only=True)
class TimeSeriesMAddError(Exception): ...


@dataclass(kw_only=True)
class _TimeSeriesMAddKeyIsNotUtf8Error(TimeSeriesMAddError):
    df: DataFrame
    dtype: PolarsDataType

    @override
    def __str__(self) -> str:
        return f"The 'key' column must be Utf8; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddTimestampIsNotDatetimeError(TimeSeriesMAddError):
    df: DataFrame
    dtype: PolarsDataType

    @override
    def __str__(self) -> str:
        return f"The 'timestamp' column must be Datetime; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddValueIsNotNumericError(TimeSeriesMAddError):
    df: DataFrame
    dtype: PolarsDataType

    @override
    def __str__(self) -> str:
        return f"The 'value' column must be numeric; got {self.dtype}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class _TimeSeriesMAddInvalidKeyError(TimeSeriesMAddError):
    values_or_df: Sequence[tuple[KeyT, dt.datetime, Number]] | DataFrame
    key: KeyT

    @override
    def __str__(self) -> str:
        return f"Invalid key; got {self.key!r}"  # skipif-ci-and-not-linux


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
        return f"Invalid value; got {self.value}"  # skipif-ci-and-not-linux


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
) -> DataFrame:
    """Get a range in forward direction."""
    from polars import (  # skipif-ci-and-not-linux
        DataFrame,
        Datetime,
        Float64,
        Int64,
        Utf8,
        concat,
        from_epoch,
        lit,
    )

    keys = list(always_iterable(key))  # skipif-ci-and-not-linux
    if len(keys) >= 2:  # skipif-ci-and-not-linux
        dfs = (
            time_series_range(
                ts,
                key,
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
            )
            for key in keys
        )
        return concat(dfs)
    key = one(keys)  # skipif-ci-and-not-linux
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
    values = ts.range(  # skipif-ci-and-not-linux
        key,
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
    return DataFrame(  # skipif-ci-and-not-linux
        values, schema={"timestamp": Int64, "value": Float64}, orient="row"
    ).select(
        key=lit(key, dtype=Utf8),
        timestamp=from_epoch("timestamp", time_unit="ms").cast(
            Datetime(time_unit="us", time_zone="UTC")
        ),
        value="value",
    )


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
    if msg == "TSDB: the key is not a TSDB key":  # skipif-ci-and-not-linux
        return "invalid key"
    if (  # skipif-ci-and-not-linux
        msg == "TSDB: invalid timestamp, must be a nonnegative integer"
    ):
        return "invalid timestamp"
    if msg == "TSDB: invalid value":  # skipif-ci-and-not-linux
        return "invalid value"
    raise ImpossibleCaseError(case=[f"{msg=}"])  # pragma: no cover


__all__ = [
    "TimeSeriesMAddError",
    "ensure_time_series_created",
    "time_series_add",
    "time_series_get",
    "time_series_madd",
    "time_series_range",
    "yield_client",
    "yield_client_async",
    "yield_time_series",
    "yield_time_series_async",
]
