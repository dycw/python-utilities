from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypedDict,
    TypeVar,
    cast,
    overload,
)
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.typing import EncodableT

from utilities.asyncio import timeout_dur
from utilities.datetime import MILLISECOND, SECOND, duration_to_float, get_now
from utilities.errors import ImpossibleCaseError
from utilities.iterables import always_iterable
from utilities.types import Duration, ensure_int

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import ResponseT

    from utilities.iterables import MaybeIterable


_K = TypeVar("_K")
_K1 = TypeVar("_K1")
_K2 = TypeVar("_K2")
_K3 = TypeVar("_K3")
_T = TypeVar("_T")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")
_V = TypeVar("_V")
_V1 = TypeVar("_V1")
_V2 = TypeVar("_V2")
_V3 = TypeVar("_V3")


class _RedisMessageSubscribe(TypedDict):
    type: Literal["subscribe", "psubscribe", "message", "pmessage"]
    pattern: str | None
    channel: bytes
    data: bytes


class _RedisMessageUnsubscribe(TypedDict):
    type: Literal["unsubscribe", "punsubscribe"]
    pattern: str | None
    channel: bytes
    data: int


_HOST = "localhost"
_PORT = 6379
_SUBSCRIBE_TIMEOUT = SECOND
_SUBSCRIBE_SLEEP = 10 * MILLISECOND


@dataclass(kw_only=True)
class _RedisHashMapKey(Generic[_K, _V]):
    """A hashmap key in a redis store."""

    name: str
    key: type[_K]
    key_serializer: Callable[[_K], bytes] | None = None
    value: type[_V]
    value_serializer: Callable[[_V], bytes] | None = None
    value_deserializer: Callable[[bytes], _V] | None = None

    async def delete(
        self, redis: Redis, key: _K, /, *, timeout: Duration | None = None
    ) -> int:
        """Delete a key from a hashmap in `redis`."""
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            return await cast(Awaitable[int], redis.hdel(self.name, cast(str, key)))

    async def exists(
        self, redis: Redis, key: _K, /, *, timeout: Duration | None = None
    ) -> bool:
        """Check if the key exists in a hashmap in `redis`."""
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            return await cast(Awaitable[bool], redis.hexists(self.name, cast(str, key)))

    async def get(
        self, redis: Redis, key: _K, /, *, timeout: Duration | None = None
    ) -> _V | None:
        """Get a value from a hashmap in `redis`."""
        ser_key = self._serialize_key(key)  # skipif-ci-and-not-linux
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            result = await cast(
                Awaitable[Any], redis.hget(self.name, cast(Any, ser_key))
            )
        match result:  # skipif-ci-and-not-linux
            case None:
                return None
            case bytes() as data:
                if self.value_deserializer is None:
                    from utilities.orjson import deserialize

                    return deserialize(data)
                return self.value_deserializer(data)
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{redis=}"])

    async def set(
        self, redis: Redis, key: _K, value: _V, /, *, timeout: Duration | None = None
    ) -> int:
        """Set a value in a hashmap in `redis`."""
        ser_key = self._serialize_key(key)  # skipif-ci-and-not-linux
        if self.value_serializer is None:  # skipif-ci-and-not-linux
            from utilities.orjson import serialize

            ser_value = serialize(value)
        else:  # skipif-ci-and-not-linux
            ser_value = self.value_serializer(value)
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            return await cast(
                Awaitable[int],
                redis.hset(
                    self.name, key=cast(Any, ser_key), value=cast(Any, ser_value)
                ),
            )

    def _serialize_key(self, key: _K, /) -> bytes:
        """Serialize the key."""
        if self.key_serializer is None:  # skipif-ci-and-not-linux
            from utilities.orjson import serialize

            return serialize(key)
        return self.key_serializer(key)  # skipif-ci-and-not-linux


@overload
def redis_hash_map_key(
    name: str,
    key: type[_K],
    value: type[_V],
    /,
    *,
    key_serializer: Callable[[_K], bytes] | None = ...,
    value_serializer: Callable[[_V], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V] | None = ...,
) -> _RedisHashMapKey[_K, _V]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: type[_K],
    value: tuple[type[_V1], type[_V2]],
    /,
    *,
    key_serializer: Callable[[_K], bytes] | None = ...,
    value_serializer: Callable[[_V1 | _V2], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V1 | _V2] | None = ...,
) -> _RedisHashMapKey[_K, _V1 | _V2]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: type[_K],
    value: tuple[type[_V1], type[_V2], type[_V3]],
    /,
    *,
    key_serializer: Callable[[_K], bytes] | None = ...,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = ...,
) -> _RedisHashMapKey[_K, _V1 | _V2 | _V3]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2]],
    value: type[_V],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2], bytes] | None = ...,
    value_serializer: Callable[[_V], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V] | None = ...,
) -> _RedisHashMapKey[_K1 | _K2, _V]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2]],
    value: tuple[type[_V1], type[_V2]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2], bytes] | None = ...,
    value_serializer: Callable[[_V1 | _V2], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V1 | _V2] | None = ...,
) -> _RedisHashMapKey[_K1 | _K2, _V1 | _V2]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2]],
    value: tuple[type[_V1], type[_V2], type[_V3]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2], bytes] | None = ...,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = ...,
) -> _RedisHashMapKey[_K1 | _K2, _V1 | _V2 | _V3]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2], type[_K3]],
    value: type[_V],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = ...,
    value_serializer: Callable[[_V], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V] | None = ...,
) -> _RedisHashMapKey[_K1 | _K2 | _K3, _V]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2], type[_K3]],
    value: tuple[type[_V1], type[_V2]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = ...,
    value_serializer: Callable[[_V1 | _V2], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V1 | _V2] | None = ...,
) -> _RedisHashMapKey[_K1 | _K2 | _K3, _V1 | _V2]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2], type[_K3]],
    value: tuple[type[_V1], type[_V2], type[_V3]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = ...,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = ...,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = ...,
) -> _RedisHashMapKey[_K1 | _K2 | _K3, _V1 | _V2 | _V3]: ...
def redis_hash_map_key(
    name: str,
    key: Any,
    value: Any,
    /,
    *,
    key_serializer: Callable[[Any], bytes] | None = None,
    value_serializer: Callable[[Any], bytes] | None = None,
    value_deserializer: Callable[[bytes], Any] | None = None,
) -> _RedisHashMapKey[Any, Any]:
    """Create a redis key."""
    return _RedisHashMapKey(  # skipif-ci-and-not-linux
        name=name,
        key=key,
        key_serializer=key_serializer,
        value=value,
        value_serializer=value_serializer,
        value_deserializer=value_deserializer,
    )


@dataclass(kw_only=True)
class _RedisKey(Generic[_T]):
    """A key in a redis store."""

    name: str
    type: type[_T]
    serializer: Callable[[_T], bytes] | None = None
    deserializer: Callable[[bytes], _T] | None = None

    async def delete(self, redis: Redis, /, *, timeout: Duration | None = None) -> int:
        """Delete the key from `redis`."""
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            return ensure_int(await redis.delete(self.name))

    async def exists(self, redis: Redis, /, *, timeout: Duration | None = None) -> bool:
        """Check if the key exists in `redis`."""
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            result = await redis.exists(self.name)
        match ensure_int(result):  # skipif-ci-and-not-linux
            case 0 | 1 as value:
                return bool(value)
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{redis=}"])

    async def get(
        self, redis: Redis, /, *, timeout: Duration | None = None
    ) -> _T | None:
        """Get a value from `redis`."""
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            result = await redis.get(self.name)
        match result:  # skipif-ci-and-not-linux
            case None:
                return None
            case bytes() as data:
                if self.deserializer is None:
                    from utilities.orjson import deserialize

                    return deserialize(data)
                return self.deserializer(data)
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{redis=}"])

    async def set(
        self, redis: Redis, value: _T, /, *, timeout: Duration | None = None
    ) -> int:
        """Set a value in `redis`."""
        if self.serializer is None:  # skipif-ci-and-not-linux
            from utilities.orjson import serialize

            value_use = serialize(value)
        else:  # skipif-ci-and-not-linux
            value_use = self.serializer(value)
        async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
            result = await redis.set(self.name, value_use)
        return ensure_int(result)  # skipif-ci-and-not-linux


@overload
def redis_key(
    name: str,
    type_: type[_T],
    /,
    *,
    serializer: Callable[[_T], bytes] | None = ...,
    deserializer: Callable[[bytes], _T] | None = ...,
) -> _RedisKey[_T]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2]],
    /,
    *,
    serializer: Callable[[_T1 | _T2], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2] | None = None,
) -> _RedisKey[_T1 | _T2]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3] | None = None,
) -> _RedisKey[_T1 | _T2 | _T3]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3], type[_T4]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4] | None = None,
) -> _RedisKey[_T1 | _T2 | _T3 | _T4]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4 | _T5], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4 | _T5] | None = None,
) -> _RedisKey[_T1 | _T2 | _T3 | _T4 | _T5]: ...
def redis_key(
    name: str,
    type_: Any,
    /,
    *,
    serializer: Callable[[Any], bytes] | None = None,
    deserializer: Callable[[bytes], Any] | None = None,
) -> _RedisKey[Any]:
    """Create a redis key."""
    return _RedisKey(  # skipif-ci-and-not-linux
        name=name, type=type_, serializer=serializer, deserializer=deserializer
    )


@overload
async def publish(
    redis: Redis, channel: str, data: _T, /, *, serializer: Callable[[_T], EncodableT]
) -> ResponseT: ...
@overload
async def publish(
    redis: Redis,
    channel: str,
    data: EncodableT,
    /,
    *,
    serializer: Callable[[EncodableT], EncodableT] | None = None,
) -> ResponseT: ...
async def publish(
    redis: Redis,
    channel: str,
    data: Any,
    /,
    *,
    serializer: Callable[[Any], EncodableT] | None = None,
) -> ResponseT:
    """Publish an object to a channel."""
    data_use = (  # skipif-ci-and-not-linux
        cast(EncodableT, data) if serializer is None else serializer(data)
    )
    return await redis.publish(channel, data_use)  # skipif-ci-and-not-linux


@overload
def subscribe(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    deserializer: Callable[[bytes], _T],
    timeout: Duration | None = ...,
    sleep: Duration = ...,
) -> AsyncIterator[_T]: ...
@overload
def subscribe(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    deserializer: None = None,
    timeout: Duration | None = ...,
    sleep: Duration = ...,
) -> AsyncIterator[bytes]: ...
async def subscribe(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    deserializer: Callable[[bytes], _T] | None = None,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[Any]:
    """Subscribe to the data of a given channel(s)."""
    channels = list(always_iterable(channels))  # skipif-ci-and-not-linux
    messages = subscribe_messages(  # skipif-ci-and-not-linux
        pubsub, channels, timeout=timeout, sleep=sleep
    )
    if deserializer is None:  # skipif-ci-and-not-linux
        async for message in messages:
            yield message["data"]
    else:  # skipif-ci-and-not-linux
        async for message in messages:
            yield deserializer(message["data"])


async def subscribe_messages(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[_RedisMessageSubscribe]:
    """Subscribe to the messages of a given channel(s)."""
    channels = list(always_iterable(channels))  # skipif-ci-and-not-linux
    for channel in channels:  # skipif-ci-and-not-linux
        await pubsub.subscribe(channel)
    channels_bytes = [c.encode() for c in channels]  # skipif-ci-and-not-linux
    timeout_use = (  # skipif-ci-and-not-linux
        None if timeout is None else duration_to_float(timeout)
    )
    sleep_use = duration_to_float(sleep)  # skipif-ci-and-not-linux
    while True:  # skipif-ci-and-not-linux
        message = cast(
            _RedisMessageSubscribe | _RedisMessageUnsubscribe | None,
            await pubsub.get_message(timeout=timeout_use),
        )
        if (
            (message is not None)
            and (message["type"] in {"subscribe", "psubscribe", "message", "pmessage"})
            and (message["channel"] in channels_bytes)
            and isinstance(message["data"], bytes)
        ):
            yield cast(_RedisMessageSubscribe, message)
        else:
            await asyncio.sleep(sleep_use)


@asynccontextmanager
async def yield_redis(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: str | int = 0,
    password: str | None = None,
    socket_timeout: float | None = None,
    socket_connect_timeout: float | None = None,
    socket_keepalive: bool | None = None,
    socket_keepalive_options: Mapping[int, int | bytes] | None = None,
    connection_pool: ConnectionPool | None = None,
    decode_responses: bool = False,
    **kwargs: Any,
) -> AsyncIterator[Redis]:
    """Yield an asynchronous redis client."""
    redis = Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        socket_keepalive_options=socket_keepalive_options,
        connection_pool=connection_pool,
        decode_responses=decode_responses,
        **kwargs,
    )
    try:
        yield redis
    finally:
        await redis.aclose()


@dataclass(repr=False, kw_only=True, slots=True)
class _TestRedis:
    """A container for a redis client; for testing purposes only."""

    redis: Redis
    timestamp: dt.datetime = field(default_factory=get_now)
    uuid: UUID = field(default_factory=uuid4)
    key: str


_ = _TestRedis


__all__ = [
    "publish",
    "redis_hash_map_key",
    "redis_key",
    "subscribe",
    "subscribe_messages",
    "yield_redis",
]
