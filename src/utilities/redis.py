from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, assert_never

import redis
import redis.asyncio

from utilities.text import ensure_bytes

from utilities.text import ensure_bytes

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncIterator, Iterator
    from uuid import UUID

    from redis.commands.timeseries import TimeSeries
    from redis.typing import ResponseT


_HOST = "localhost"
_PORT = 6379


_T = TypeVar("_T")
_TRedis = TypeVar("_TRedis", redis.Redis, redis.asyncio.Redis)


@dataclass(repr=False, frozen=True, kw_only=True)
class RedisContainer(Generic[_TRedis]):
    """A container for a client; for testing purposes only."""

    client: _TRedis
    timestamp: dt.datetime
    uuid: UUID
    key: str


@dataclass(frozen=True, kw_only=True)
class RedisKey(Generic[_T]):
    """A key in a redis store."""

    name: str
    type: type[_T]

    def get(
        self,
        *,
        client: redis.Redis | None = None,
        host: str = _HOST,
        port: int = _PORT,
        db: int = 0,
        password: str | None = None,
        connection_pool: redis.ConnectionPool | None = None,
        decode_responses: bool = False,
        **kwargs: Any,
    ) -> _T | None:
        """Get a value from `redis`."""
        from utilities.orjson import deserialize

        with yield_client(
            client=client,
            host=host,
            port=port,
            db=db,
            password=password,
            connection_pool=connection_pool,
            decode_responses=decode_responses,
            **kwargs,
        ) as client_use:
            maybe_ser = ensure_bytes(client_use.get(self.name), nullable=True)
        return None if maybe_ser is None else deserialize(maybe_ser)

    def set(
        self,
        value: _T,
        /,
        *,
        client: redis.Redis | None = None,
        host: str = _HOST,
        port: int = _PORT,
        db: int = 0,
        password: str | None = None,
        connection_pool: redis.ConnectionPool | None = None,
        decode_responses: bool = False,
        **kwargs: Any,
    ) -> ResponseT:
        """Set a value in `redis`."""
        from utilities.orjson import serialize

        ser = serialize(value)
        with yield_client(
            client=client,
            host=host,
            port=port,
            db=db,
            password=password,
            connection_pool=connection_pool,
            decode_responses=decode_responses,
            **kwargs,
        ) as client_use:
            return client_use.set(self.name, ser)

    async def get_async(
        self,
        *,
        client: redis.asyncio.Redis | None = None,
        host: str = _HOST,
        port: int = _PORT,
        db: str | int = 0,
        password: str | None = None,
        connection_pool: redis.asyncio.ConnectionPool | None = None,
        decode_responses: bool = False,
        **kwargs: Any,
    ) -> _T | None:
        """Get a value from `redis` asynchronously."""
        from utilities.orjson import deserialize

        async with yield_client_async(
            client=client,
            host=host,
            port=port,
            db=db,
            password=password,
            connection_pool=connection_pool,
            decode_responses=decode_responses,
            **kwargs,
        ) as client_use:
            maybe_ser = ensure_bytes(await client_use.get(self.name), nullable=True)
        return None if maybe_ser is None else deserialize(maybe_ser)

    async def set_async(
        self,
        value: _T,
        /,
        *,
        client: redis.asyncio.Redis | None = None,
        host: str = _HOST,
        port: int = _PORT,
        db: str | int = 0,
        password: str | None = None,
        connection_pool: redis.asyncio.ConnectionPool | None = None,
        decode_responses: bool = False,
        **kwargs: Any,
    ) -> ResponseT:
        """Set a value in `redis` asynchronously."""
        from utilities.orjson import serialize

        ser = serialize(value)
        async with yield_client_async(
            client=client,
            host=host,
            port=port,
            db=db,
            password=password,
            connection_pool=connection_pool,
            decode_responses=decode_responses,
            **kwargs,
        ) as client_use:
            return await client_use.set(self.name, ser)


@contextmanager
def yield_client(
    *,
    client: redis.Redis | None = None,
    host: str = _HOST,
    port: int = _PORT,
    db: int = 0,
    password: str | None = None,
    connection_pool: redis.ConnectionPool | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> Iterator[redis.Redis]:
    """Yield a synchronous client."""
    if client is None:
        client_use = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            connection_pool=connection_pool,
            decode_responses=decode_responses,
            **kwargs,
        )
    else:
        client_use = client
    try:
        yield client_use
    finally:
        client_use.close()


@asynccontextmanager
async def yield_client_async(
    *,
    client: redis.asyncio.Redis | None = None,
    host: str = _HOST,
    port: int = _PORT,
    db: str | int = 0,
    password: str | None = None,
    connection_pool: redis.asyncio.ConnectionPool | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> AsyncIterator[redis.asyncio.Redis]:
    """Yield an asynchronous client."""
    if client is None:
        client_use = redis.asyncio.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            connection_pool=connection_pool,
            decode_responses=decode_responses,
            **kwargs,
        )
    else:
        client_use = client
    try:
        yield client_use
    finally:
        match client_use.connection_pool:
            case redis.ConnectionPool() as pool:
                pool.disconnect(inuse_connections=False)  # pragma: no cover
            case redis.asyncio.ConnectionPool() as pool:
                await pool.disconnect(inuse_connections=False)
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)


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


__all__ = [
    "RedisContainer",
    "RedisKey",
    "yield_client",
    "yield_client_async",
    "yield_time_series",
    "yield_time_series_async",
]
