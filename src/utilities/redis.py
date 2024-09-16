from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, assert_never

import redis
import redis.asyncio

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncIterator, Iterator
    from uuid import UUID

    from redis.commands.timeseries import TimeSeries


_HOST = "localhost"
_PORT = 6379


_TRedis = TypeVar("_TRedis", redis.Redis, redis.asyncio.Redis)


@dataclass(repr=False, frozen=True, kw_only=True)
class RedisContainer(Generic[_TRedis]):
    """A container for a client; for testing purposes only."""

    client: _TRedis
    timestamp: dt.datetime
    uuid: UUID
    key: str

    @property
    def ts(self) -> TimeSeries:
        return self.client.ts()  # skipif-ci-and-not-linux


@contextmanager
def yield_client(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: int = 0,
    password: str | None = None,
    connection_pool: redis.ConnectionPool | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> Iterator[redis.Redis]:
    """Yield a synchronous client."""
    client = redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        connection_pool=connection_pool,
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
    connection_pool: redis.asyncio.ConnectionPool | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> AsyncIterator[redis.asyncio.Redis]:
    """Yield an asynchronous client."""
    client = redis.asyncio.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        connection_pool=connection_pool,
        decode_responses=decode_responses,
        **kwargs,
    )
    try:
        yield client
    finally:
        match client.connection_pool:
            case redis.ConnectionPool() as pool:
                pool.disconnect(inuse_connections=False)  # pragma: no cover
            case redis.asyncio.ConnectionPool() as pool:
                await pool.disconnect(inuse_connections=False)
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)


__all__ = ["RedisContainer", "yield_client", "yield_client_async"]
