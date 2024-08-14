from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from functools import partial
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

    from utilities.iterables import MaybeIterable


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
    duplicate_policy: str | None = None,
    ignore_max_time_diff: int | None = None,
    ignore_max_val_diff: float | None = None,
    on_duplicate: str | None = None,
) -> int:
    """Append a sample to a time series."""
    milliseconds = milliseconds_since_epoch(timestamp, strict=True)
    try:
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
    except ResponseError as error:
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


@dataclass(kw_only=True)
class TimeSeriesAddError(Exception):
    timestamp: dt.datetime
    value: float


@dataclass(kw_only=True)
class _TimeSeriesAddErrorAtUpsertError(TimeSeriesAddError):
    @override
    def __str__(self) -> str:
        return (
            f"Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got {self.timestamp}"
        )


@dataclass(kw_only=True)
class _TimeSeriesAddInvalidTimestampError(TimeSeriesAddError):
    @override
    def __str__(self) -> str:
        return f"Timestamp must be a non-negative integer; got {self.timestamp}"


@dataclass(kw_only=True)
class _TimeSeriesAddInvalidValueError(TimeSeriesAddError):
    @override
    def __str__(self) -> str:
        return f"Invalid value; got {self.value}"


def time_series_get(
    ts: TimeSeries, key: KeyT, /, *, latest: bool | None = False
) -> tuple[dt.datetime, float]:
    """Get the last sample of a time series."""
    milliseconds, value = ts.get(key, latest=latest)
    timestamp = milliseconds_since_epoch_to_datetime(milliseconds)
    return timestamp, value


def time_series_madd(
    ts: TimeSeries,
    values_or_df: Iterable[tuple[KeyT, dt.datetime, Number]] | DataFrame,
    /,
) -> list[int]:
    """Append new samples to one or more time series."""
    from polars import DataFrame, Datetime, Float64, Int64, Utf8, col

    ktv_tuples: Iterable[tuple[KeyT, int, Number]]
    if isinstance(values_or_df, DataFrame):
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
        ktv_tuples = df.iter_rows()
    else:
        values_or_df = list(values_or_df)
        ktv_tuples = (
            (key, milliseconds_since_epoch(timestamp, strict=True), value)
            for key, timestamp, value in values_or_df
        )
    result: list[int | ResponseError] = ts.madd(list(ktv_tuples))
    try:
        error = next(r for r in result if isinstance(r, ResponseError))
    except StopIteration:
        return cast(list[int], result)
    match _classify_response_error(error):
        case "invalid timestamp":
            raise _TimeSeriesMAddInvalidTimestampError(values_or_df=values_or_df)
        case "invalid value":
            raise _TimeSeriesMAddInvalidValueError(values_or_df=values_or_df)
        case _:
            raise error


@dataclass(kw_only=True)
class TimeSeriesMAddError(Exception): ...


@dataclass(kw_only=True)
class _TimeSeriesMAddKeyIsNotUtf8Error(TimeSeriesMAddError):
    df: DataFrame
    dtype: PolarsDataType

    @override
    def __str__(self) -> str:
        return f"The 'key' column must be Utf8; got {self.dtype}"


@dataclass(kw_only=True)
class _TimeSeriesMAddTimestampIsNotDatetimeError(TimeSeriesMAddError):
    df: DataFrame
    dtype: PolarsDataType

    @override
    def __str__(self) -> str:
        return f"The 'timestamp' column must be Datetime; got {self.dtype}"


@dataclass(kw_only=True)
class _TimeSeriesMAddValueIsNotNumericError(TimeSeriesMAddError):
    df: DataFrame
    dtype: PolarsDataType

    @override
    def __str__(self) -> str:
        return f"The 'value' column must be numeric; got {self.dtype}"


@dataclass(kw_only=True)
class _TimeSeriesMAddInvalidTimestampError(TimeSeriesMAddError):
    values_or_df: Sequence[tuple[KeyT, dt.datetime, Number]] | DataFrame

    @override
    def __str__(self) -> str:
        return f"Timestamps must be non-negative integers; got {self.values_or_df}"


@dataclass(kw_only=True)
class _TimeSeriesMAddInvalidValueError(TimeSeriesMAddError):
    values_or_df: Sequence[tuple[KeyT, dt.datetime, Number]] | DataFrame

    @override
    def __str__(self) -> str:
        return f"Invalid value(s); got {self.values_or_df}"


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
    from polars import (
        DataFrame,
        Datetime,
        Float64,
        Int64,
        Utf8,
        concat,
        from_epoch,
        lit,
    )

    keys = list(always_iterable(key))
    if len(keys) >= 2:
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
    key = one(keys)
    ms_since_epoch = partial(milliseconds_since_epoch, strict=True)
    from_time_use = "-" if from_time is None else ms_since_epoch(from_time)
    to_time_use = "+" if to_time is None else ms_since_epoch(to_time)
    filter_by_ts_use = (
        None if filter_by_ts is None else list(map(ms_since_epoch, filter_by_ts))
    )
    filter_by_min_value_use = (
        None if filter_by_min_value is None else ms_since_epoch(filter_by_min_value)
    )
    filter_by_max_value_use = (
        None if filter_by_max_value is None else ms_since_epoch(filter_by_max_value)
    )
    values = ts.range(
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
    return DataFrame(
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
    host: str = "localhost",
    port: int = 6379,
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
    host: str = "localhost",
    port: int = 6379,
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


_ResponseErrorKind = Literal["invalid timestamp", "invalid value", "error at upsert"]


def _classify_response_error(error: ResponseError, /) -> _ResponseErrorKind:
    msg = ensure_str(one(error.args))
    if msg == "TSDB: invalid timestamp, must be a nonnegative integer":
        return "invalid timestamp"
    if msg == "TSDB: invalid value":
        return "invalid value"
    if (
        msg
        == "TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode"
    ):
        return "error at upsert"
    raise ImpossibleCaseError(case=[f"{msg=}"])  # pragma: no cover


__all__ = [
    "TimeSeriesMAddError",
    "time_series_add",
    "time_series_get",
    "time_series_madd",
    "time_series_range",
    "yield_client",
    "yield_client_async",
]
