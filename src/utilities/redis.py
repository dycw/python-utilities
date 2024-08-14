from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from re import search
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

    from redis.commands.timeseries import TimeSeries
    from redis.typing import Number


def add_timestamp(
    ts: TimeSeries,
    key: bytes | str | memoryview,
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
    milliseconds = round(milliseconds_since_epoch(timestamp))
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


def get_timestamp(
    ts: TimeSeries, key: bytes | str | memoryview, /, *, latest: bool | None = False
) -> tuple[dt.datetime, float]:
    milliseconds, value_str = ts.get(key, latest=latest)
    timestamp = milliseconds_since_epoch_to_datetime(milliseconds)
    value = float(value_str) if search(r"\.", value_str) else int(value_str)
    return timestamp, value


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


__all__ = ["add_timestamp", "yield_client", "yield_client_async"]
