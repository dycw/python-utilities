from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from functools import partial
from typing import TYPE_CHECKING, Any

import redis
import redis.asyncio

from utilities.datetime import (
    milliseconds_since_epoch,
    milliseconds_since_epoch_to_datetime,
)

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncIterator, Iterator

    from polars import DataFrame
    from redis.commands.timeseries import TimeSeries
    from redis.typing import KeyT, Number


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
) -> Any:
    """Append a sample to a time series."""
    milliseconds = milliseconds_since_epoch(timestamp, strict=True)
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


def time_series_get(
    ts: TimeSeries, key: KeyT, /, *, latest: bool | None = False
) -> tuple[dt.datetime, float]:
    """Get the last sample of a time series."""
    milliseconds, value = ts.get(key, latest=latest)
    timestamp = milliseconds_since_epoch_to_datetime(milliseconds)
    return timestamp, value


def time_series_range(
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
) -> DataFrame:
    """Get."""
    from polars import DataFrame, Datetime, Float64, Int64, from_epoch

    ms_since_epoch = partial(milliseconds_since_epoch, strict=True)
    from_time_use = "-" if from_time is None else ms_since_epoch(from_time)
    to_time_use = "+" if to_time is None else ms_since_epoch(to_time)
    if filter_by_ts is None:
        filter_by_ts_use = None
    else:
        filter_by_ts_use = list(map(ms_since_epoch, filter_by_ts))
    if filter_by_min_value is None:
        filter_by_min_value_use = None
    else:
        filter_by_min_value_use = ms_since_epoch(filter_by_min_value)
    if filter_by_max_value is None:
        filter_by_max_value_use = None
    else:
        filter_by_max_value_use = ms_since_epoch(filter_by_max_value)
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
    ).with_columns(
        from_epoch("timestamp", time_unit="ms").cast(
            Datetime(time_unit="us", time_zone="UTC")
        )
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


__all__ = [
    "time_series_add",
    "time_series_get",
    "time_series_range",
    "yield_client",
    "yield_client_async",
]
