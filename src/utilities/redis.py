from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypedDict,
    TypeVar,
    assert_never,
    cast,
    overload,
    override,
)
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.typing import EncodableT

from utilities.asyncio import QueueProcessor, timeout_dur
from utilities.datetime import (
    MILLISECOND,
    SECOND,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    get_now,
)
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_int
from utilities.iterables import always_iterable, one

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Callable,
        Iterable,
        Mapping,
        Sequence,
    )

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import ResponseT

    from utilities.iterables import MaybeIterable
    from utilities.types import Duration, TypeLike


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


##


@dataclass(kw_only=True)
class RedisHashMapKey(Generic[_K, _V]):
    """A hashmap key in a redis store."""

    name: str
    key: TypeLike[_K]
    key_serializer: Callable[[_K], bytes] | None = None
    key_deserializer: Callable[[bytes], _K] | None = None
    value: TypeLike[_V]
    value_serializer: Callable[[_V], bytes] | None = None
    value_deserializer: Callable[[bytes], _V] | None = None
    timeout: Duration | None = None
    error: type[Exception] = TimeoutError
    ttl: Duration | None = None

    async def delete(self, redis: Redis, key: _K, /) -> int:
        """Delete a key from a hashmap in `redis`."""
        ser = _serialize(  # skipif-ci-and-not-linux
            key, serializer=self.key_serializer
        ).decode()
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            return await cast("Awaitable[int]", redis.hdel(self.name, ser))
        raise ImpossibleCaseError(case=[f"{redis=}", f"{key=}"])  # pragma: no cover

    async def exists(self, redis: Redis, key: _K, /) -> bool:
        """Check if the key exists in a hashmap in `redis`."""
        ser = _serialize(  # skipif-ci-and-not-linux
            key, serializer=self.key_serializer
        ).decode()
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            return await cast("Awaitable[bool]", redis.hexists(self.name, ser))

    async def get(self, redis: Redis, key: _K, /) -> _V:
        """Get a value from a hashmap in `redis`."""
        result = one(await self.get_many(redis, [key]))  # skipif-ci-and-not-linux
        if result is None:  # skipif-ci-and-not-linux
            raise KeyError(self.name, key)
        return result  # skipif-ci-and-not-linux

    async def get_all(self, redis: Redis, /) -> Mapping[_K, _V]:
        """Get a value from a hashmap in `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = await cast(  # skipif-ci-and-not-linux
                "Awaitable[Mapping[bytes, bytes]]", redis.hgetall(self.name)
            )
        return {  # skipif-ci-and-not-linux
            _deserialize(key, deserializer=self.key_deserializer): _deserialize(
                value, deserializer=self.value_deserializer
            )
            for key, value in result.items()
        }

    async def get_many(
        self, redis: Redis, keys: Iterable[_K], /
    ) -> Sequence[_V | None]:
        """Get multiple values from a hashmap in `redis`."""
        keys = list(keys)  # skipif-ci-and-not-linux
        if len(keys) == 0:  # skipif-ci-and-not-linux
            return []
        ser = [  # skipif-ci-and-not-linux
            _serialize(key, serializer=self.key_serializer) for key in keys
        ]
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = await cast(  # skipif-ci-and-not-linux
                "Awaitable[Sequence[bytes | None]]", redis.hmget(self.name, ser)
            )
        return [  # skipif-ci-and-not-linux
            None
            if data is None
            else _deserialize(data, deserializer=self.value_deserializer)
            for data in result
        ]

    async def keys(self, redis: Redis, /) -> Sequence[_K]:
        """Get the keys of a hashmap in `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = await cast("Awaitable[Sequence[bytes]]", redis.hkeys(self.name))
        return [  # skipif-ci-and-not-linux
            _deserialize(data, deserializer=self.key_deserializer) for data in result
        ]

    async def length(self, redis: Redis, /) -> int:
        """Get the length of a hashmap in `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            return await cast("Awaitable[int]", redis.hlen(self.name))

    async def set(self, redis: Redis, key: _K, value: _V, /) -> int:
        """Set a value in a hashmap in `redis`."""
        return await self.set_many(redis, {key: value})  # skipif-ci-and-not-linux

    async def set_many(self, redis: Redis, mapping: Mapping[_K, _V], /) -> int:
        """Set multiple value(s) in a hashmap in `redis`."""
        if len(mapping) == 0:  # skipif-ci-and-not-linux
            return 0
        ser = {  # skipif-ci-and-not-linux
            _serialize(key, serializer=self.key_serializer): _serialize(
                value, serializer=self.value_serializer
            )
            for key, value in mapping.items()
        }
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = await cast(
                "Awaitable[int]", redis.hset(self.name, mapping=cast("Any", ser))
            )
            if self.ttl is not None:
                await redis.pexpire(self.name, datetime_duration_to_timedelta(self.ttl))
        return result  # skipif-ci-and-not-linux

    async def values(self, redis: Redis, /) -> Sequence[_V]:
        """Get the values of a hashmap in `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = await cast("Awaitable[Sequence[bytes]]", redis.hvals(self.name))
        return [  # skipif-ci-and-not-linux
            _deserialize(data, deserializer=self.value_deserializer) for data in result
        ]


@overload
def redis_hash_map_key(
    name: str,
    key: type[_K],
    value: type[_V],
    /,
    *,
    key_serializer: Callable[[_K], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K, _V]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: type[_K],
    value: tuple[type[_V1], type[_V2]],
    /,
    *,
    key_serializer: Callable[[_K], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K, _V1 | _V2]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: type[_K],
    value: tuple[type[_V1], type[_V2], type[_V3]],
    /,
    *,
    key_serializer: Callable[[_K], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K, _V1 | _V2 | _V3]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2]],
    value: type[_V],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K1 | _K2, _V]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2]],
    value: tuple[type[_V1], type[_V2]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K1 | _K2, _V1 | _V2]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2]],
    value: tuple[type[_V1], type[_V2], type[_V3]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K1 | _K2, _V1 | _V2 | _V3]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2], type[_K3]],
    value: type[_V],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K1 | _K2 | _K3, _V]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2], type[_K3]],
    value: tuple[type[_V1], type[_V2]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K1 | _K2 | _K3, _V1 | _V2]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: tuple[type[_K1], type[_K2], type[_K3]],
    value: tuple[type[_V1], type[_V2], type[_V3]],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K1 | _K2 | _K3, _V1 | _V2 | _V3]: ...
@overload
def redis_hash_map_key(
    name: str,
    key: TypeLike[_K],
    value: TypeLike[_V],
    /,
    *,
    key_serializer: Callable[[_K1 | _K2 | _K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[_V1 | _V2 | _V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], _V1 | _V2 | _V3] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisHashMapKey[_K, _V]: ...
def redis_hash_map_key(
    name: str,
    key: TypeLike[_K],
    value: TypeLike[_V],
    /,
    *,
    key_serializer: Callable[[Any], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[Any], bytes] | None = None,
    value_deserializer: Callable[[bytes], Any] | None = None,
    timeout: Duration | None = None,
    ttl: Duration | None = None,
    error: type[Exception] = TimeoutError,
) -> RedisHashMapKey[_K, _V]:
    """Create a redis key."""
    return RedisHashMapKey(  # skipif-ci-and-not-linux
        name=name,
        key=key,
        key_serializer=key_serializer,
        key_deserializer=key_deserializer,
        value=value,
        value_serializer=value_serializer,
        value_deserializer=value_deserializer,
        timeout=timeout,
        error=error,
        ttl=ttl,
    )


##


@dataclass(kw_only=True)
class RedisKey(Generic[_T]):
    """A key in a redis store."""

    name: str
    type: TypeLike[_T]
    serializer: Callable[[_T], bytes] | None = None
    deserializer: Callable[[bytes], _T] | None = None
    timeout: Duration | None = None
    error: type[Exception] = TimeoutError
    ttl: Duration | None = None

    async def delete(self, redis: Redis, /) -> int:
        """Delete the key from `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            return ensure_int(await redis.delete(self.name))

    async def exists(self, redis: Redis, /) -> bool:
        """Check if the key exists in `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = cast("Literal[0, 1]", await redis.exists(self.name))
        match result:  # skipif-ci-and-not-linux
            case 0 | 1 as value:
                return bool(value)
            case _ as never:
                assert_never(never)

    async def get(self, redis: Redis, /) -> _T:
        """Get a value from `redis`."""
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = cast("bytes | None", await redis.get(self.name))
        if result is None:  # skipif-ci-and-not-linux
            raise KeyError(self.name)
        return _deserialize(  # skipif-ci-and-not-linux
            result, deserializer=self.deserializer
        )

    async def set(self, redis: Redis, value: _T, /) -> int:
        """Set a value in `redis`."""
        ser = _serialize(value, serializer=self.serializer)  # skipif-ci-and-not-linux
        ttl = (  # skipif-ci-and-not-linux
            None
            if self.ttl is None
            else round(1000 * datetime_duration_to_float(self.ttl))
        )
        async with timeout_dur(  # skipif-ci-and-not-linux
            duration=self.timeout, error=self.error
        ):
            result = await redis.set(  # skipif-ci-and-not-linux
                self.name, ser, px=ttl
            )
        return ensure_int(result)  # skipif-ci-and-not-linux


@overload
def redis_key(
    name: str,
    type_: type[_T],
    /,
    *,
    serializer: Callable[[_T], bytes] | None = None,
    deserializer: Callable[[bytes], _T] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2]],
    /,
    *,
    serializer: Callable[[_T1 | _T2], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T1 | _T2]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T1 | _T2 | _T3]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3], type[_T4]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T1 | _T2 | _T3 | _T4]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4 | _T5], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4 | _T5] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T1 | _T2 | _T3 | _T4 | _T5]: ...
@overload
def redis_key(
    name: str,
    type_: TypeLike[_T],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4 | _T5], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4 | _T5] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T]: ...
def redis_key(
    name: str,
    type_: TypeLike[_T],
    /,
    *,
    serializer: Callable[[Any], bytes] | None = None,
    deserializer: Callable[[bytes], Any] | None = None,
    timeout: Duration | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[_T]:
    """Create a redis key."""
    return RedisKey(  # skipif-ci-and-not-linux
        name=name,
        type=type_,
        serializer=serializer,
        deserializer=deserializer,
        timeout=timeout,
        error=error,
        ttl=ttl,
    )


##


_PUBLISH_TIMEOUT: Duration = SECOND


@overload
async def publish(
    redis: Redis,
    channel: str,
    data: _T,
    /,
    *,
    serializer: Callable[[_T], EncodableT],
    timeout: Duration = _PUBLISH_TIMEOUT,
) -> ResponseT: ...
@overload
async def publish(
    redis: Redis,
    channel: str,
    data: EncodableT,
    /,
    *,
    serializer: Callable[[EncodableT], EncodableT] | None = None,
    timeout: Duration = _PUBLISH_TIMEOUT,
) -> ResponseT: ...
async def publish(
    redis: Redis,
    channel: str,
    data: Any,
    /,
    *,
    serializer: Callable[[Any], EncodableT] | None = None,
    timeout: Duration = _PUBLISH_TIMEOUT,
) -> ResponseT:
    """Publish an object to a channel."""
    data_use = (  # skipif-ci-and-not-linux
        cast("EncodableT", data) if serializer is None else serializer(data)
    )
    async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
        return await redis.publish(channel, data_use)  # skipif-ci-and-not-linux


##


@dataclass(kw_only=True)
class Publisher(QueueProcessor[tuple[str, EncodableT]]):
    """Publish a set of messages to Redis."""

    redis: Redis
    serializer: Callable[[Any], EncodableT] | None = None
    timeout: Duration = _PUBLISH_TIMEOUT

    @override
    async def _process_item(self, item: tuple[str, EncodableT], /) -> None:
        channel, data = item  # skipif-ci-and-not-linux
        _ = await publish(  # skipif-ci-and-not-linux
            self.redis, channel, data, serializer=self.serializer, timeout=self.timeout
        )


##


_SUBSCRIBE_TIMEOUT: Duration = SECOND
_SUBSCRIBE_SLEEP: Duration = 10 * MILLISECOND


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
        None if timeout is None else datetime_duration_to_float(timeout)
    )
    sleep_use = datetime_duration_to_float(sleep)  # skipif-ci-and-not-linux
    while True:  # skipif-ci-and-not-linux
        message = cast(
            "_RedisMessageSubscribe | _RedisMessageUnsubscribe | None",
            await pubsub.get_message(timeout=timeout_use),
        )
        if (
            (message is not None)
            and (message["type"] in {"subscribe", "psubscribe", "message", "pmessage"})
            and (message["channel"] in channels_bytes)
            and isinstance(message["data"], bytes)
        ):
            yield cast("_RedisMessageSubscribe", message)
        else:
            await asyncio.sleep(sleep_use)


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


##


_HOST = "localhost"
_PORT = 6379


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


##


def _serialize(obj: _T, /, *, serializer: Callable[[_T], bytes] | None = None) -> bytes:
    if serializer is None:  # skipif-ci-and-not-linux
        from utilities.orjson import serialize as serializer_use
    else:  # skipif-ci-and-not-linux
        serializer_use = serializer
    return serializer_use(obj)  # skipif-ci-and-not-linux


def _deserialize(
    data: bytes, /, *, deserializer: Callable[[bytes], _T] | None = None
) -> _T:
    if deserializer is None:  # skipif-ci-and-not-linux
        from utilities.orjson import deserialize as deserializer_use
    else:  # skipif-ci-and-not-linux
        deserializer_use = deserializer
    return deserializer_use(data)  # skipif-ci-and-not-linux


##


@dataclass(repr=False, kw_only=True, slots=True)
class _TestRedis:
    """A container for a redis client; for testing purposes only."""

    redis: Redis
    timestamp: dt.datetime = field(default_factory=get_now)
    uuid: UUID = field(default_factory=uuid4)
    key: str


_ = _TestRedis


__all__ = [
    "Publisher",
    "RedisHashMapKey",
    "RedisKey",
    "publish",
    "redis_hash_map_key",
    "redis_key",
    "subscribe",
    "subscribe_messages",
    "yield_redis",
]
