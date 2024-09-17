from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import redis
import redis.asyncio

from utilities.text import ensure_bytes
from utilities.types import ensure_int

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID


_HOST = "localhost"
_PORT = 6379


_K = TypeVar("_K")
_T = TypeVar("_T")
_V = TypeVar("_V")
_TRedis = TypeVar("_TRedis", redis.Redis, redis.asyncio.Redis)


@dataclass(repr=False, frozen=True, kw_only=True)
class RedisContainer(Generic[_TRedis]):
    """A container for a client; for testing purposes only."""

    client: _TRedis
    timestamp: dt.datetime
    uuid: UUID
    key: str


@dataclass(frozen=True, kw_only=True)
class RedisHashMapKey(Generic[_K, _V]):
    """A hashmap key in a redis store."""

    name: str
    key: type[_K]
    value: type[_V]

    def hget(
        self,
        key: _K,
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
    ) -> _V | None:
        """Get a value from a hashmap in `redis`."""
        from utilities.orjson import deserialize, serialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        ser = serialize(key)  # skipif-ci-and-not-linux
        maybe_ser = client_use.hget(  # skipif-ci-and-not-linux
            self.name, cast(Any, ser)
        )
        if maybe_ser is None:  # skipif-ci-and-not-linux
            return None
        return deserialize(ensure_bytes(maybe_ser))  # skipif-ci-and-not-linux

    def hset(
        self,
        key: _K,
        value: _V,
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
    ) -> int:
        """Set a value in a hashmap in `redis`."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        ser_key = serialize(key)  # skipif-ci-and-not-linux
        ser_value = serialize(value)  # skipif-ci-and-not-linux
        response = client_use.hset(  # skipif-ci-and-not-linux
            self.name, key=cast(Any, ser_key), value=cast(Any, ser_value)
        )
        return ensure_int(response)  # skipif-ci-and-not-linux

    async def hget_async(
        self,
        key: _K,
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
    ) -> _V | None:
        """Get a value from a hashmap in `redis` asynchronously."""
        from utilities.orjson import deserialize, serialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.asyncio.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        ser = serialize(key)  # skipif-ci-and-not-linux
        maybe_ser = await cast(  # skipif-ci-and-not-linux
            Awaitable[Any], client_use.hget(self.name, cast(Any, ser))
        )
        if maybe_ser is None:  # skipif-ci-and-not-linux
            return None
        return deserialize(ensure_bytes(maybe_ser))  # skipif-ci-and-not-linux

    async def hset_async(
        self,
        key: _K,
        value: _V,
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
    ) -> int:
        """Set a value in a hashmap in `redis` asynchronously."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.asyncio.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        ser_key = serialize(key)  # skipif-ci-and-not-linux
        ser_value = serialize(value)  # skipif-ci-and-not-linux
        response = await cast(  # skipif-ci-and-not-linux
            Awaitable[int],
            client_use.hset(
                self.name, key=cast(Any, ser_key), value=cast(Any, ser_value)
            ),
        )
        return ensure_int(response)  # skipif-ci-and-not-linux


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
        from utilities.orjson import deserialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        maybe_ser = client_use.get(self.name)  # skipif-ci-and-not-linux
        if maybe_ser is None:  # skipif-ci-and-not-linux
            return None
        return deserialize(ensure_bytes(maybe_ser))  # skipif-ci-and-not-linux

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
    ) -> int:
        """Set a value in `redis`."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        ser = serialize(value)  # skipif-ci-and-not-linux
        response = client_use.set(self.name, ser)  # skipif-ci-and-not-linux
        return ensure_int(response)  # skipif-ci-and-not-linux

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
        from utilities.orjson import deserialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.asyncio.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        maybe_ser = await client_use.get(self.name)  # skipif-ci-and-not-linux
        if maybe_ser is None:  # skipif-ci-and-not-linux
            return None
        return deserialize(ensure_bytes(maybe_ser))  # skipif-ci-and-not-linux

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
    ) -> int:
        """Set a value in `redis` asynchronously."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        if client is None:  # skipif-ci-and-not-linux
            client_use = redis.asyncio.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                connection_pool=connection_pool,
                decode_responses=decode_responses,
                **kwargs,
            )
        else:  # skipif-ci-and-not-linux
            client_use = client
        ser = serialize(value)  # skipif-ci-and-not-linux
        return await client_use.set(self.name, ser)  # skipif-ci-and-not-linux


__all__ = ["RedisContainer", "RedisHashMapKey", "RedisKey"]
