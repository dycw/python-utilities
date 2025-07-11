from __future__ import annotations

from asyncio import CancelledError, Queue, Task, TaskGroup, create_task
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypedDict,
    TypeGuard,
    assert_never,
    cast,
    overload,
    override,
)

from redis.asyncio import Redis

from utilities.asyncio import sleep_td, timeout_td
from utilities.contextlib import enhanced_async_context_manager
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_int, identity
from utilities.iterables import always_iterable, one
from utilities.whenever import MILLISECOND, SECOND, to_milliseconds, to_seconds

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Collection, Iterable

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import EncodableT

    from utilities.iterables import MaybeIterable
    from utilities.types import Delta, MaybeListStr, MaybeSequence, MaybeType, TypeLike


_PUBLISH_TIMEOUT: Delta = SECOND


##


@dataclass(kw_only=True)
class RedisHashMapKey[K, V]:
    """A hashmap key in a redis store."""

    name: str
    key: TypeLike[K]
    key_serializer: Callable[[K], bytes] | None = None
    key_deserializer: Callable[[bytes], K] | None = None
    value: TypeLike[V]
    value_serializer: Callable[[V], bytes] | None = None
    value_deserializer: Callable[[bytes], V] | None = None
    timeout: Delta | None = None
    error: MaybeType[BaseException] = TimeoutError
    ttl: Delta | None = None

    async def delete(self, redis: Redis, key: K, /) -> int:
        """Delete a key from a hashmap in `redis`."""
        ser = _serialize(  # skipif-ci-and-not-linux
            key, serializer=self.key_serializer
        ).decode()
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            return await cast("Awaitable[int]", redis.hdel(self.name, ser))
        raise ImpossibleCaseError(case=[f"{redis=}", f"{key=}"])  # pragma: no cover

    async def exists(self, redis: Redis, key: K, /) -> bool:
        """Check if the key exists in a hashmap in `redis`."""
        ser = _serialize(  # skipif-ci-and-not-linux
            key, serializer=self.key_serializer
        ).decode()
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            return await cast("Awaitable[bool]", redis.hexists(self.name, ser))

    async def get(self, redis: Redis, key: K, /) -> V:
        """Get a value from a hashmap in `redis`."""
        result = one(await self.get_many(redis, [key]))  # skipif-ci-and-not-linux
        if result is None:  # skipif-ci-and-not-linux
            raise KeyError(self.name, key)
        return result  # skipif-ci-and-not-linux

    async def get_all(self, redis: Redis, /) -> Mapping[K, V]:
        """Get a value from a hashmap in `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
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

    async def get_many(self, redis: Redis, keys: Iterable[K], /) -> Sequence[V | None]:
        """Get multiple values from a hashmap in `redis`."""
        keys = list(keys)  # skipif-ci-and-not-linux
        if len(keys) == 0:  # skipif-ci-and-not-linux
            return []
        ser = [  # skipif-ci-and-not-linux
            _serialize(key, serializer=self.key_serializer) for key in keys
        ]
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
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

    async def keys(self, redis: Redis, /) -> Sequence[K]:
        """Get the keys of a hashmap in `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = await cast("Awaitable[Sequence[bytes]]", redis.hkeys(self.name))
        return [  # skipif-ci-and-not-linux
            _deserialize(data, deserializer=self.key_deserializer) for data in result
        ]

    async def length(self, redis: Redis, /) -> int:
        """Get the length of a hashmap in `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            return await cast("Awaitable[int]", redis.hlen(self.name))

    async def set(self, redis: Redis, key: K, value: V, /) -> int:
        """Set a value in a hashmap in `redis`."""
        return await self.set_many(redis, {key: value})  # skipif-ci-and-not-linux

    async def set_many(self, redis: Redis, mapping: Mapping[K, V], /) -> int:
        """Set multiple value(s) in a hashmap in `redis`."""
        if len(mapping) == 0:  # skipif-ci-and-not-linux
            return 0
        ser = {  # skipif-ci-and-not-linux
            _serialize(key, serializer=self.key_serializer): _serialize(
                value, serializer=self.value_serializer
            )
            for key, value in mapping.items()
        }
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = await cast(
                "Awaitable[int]", redis.hset(self.name, mapping=cast("Any", ser))
            )
            if self.ttl is not None:
                await redis.pexpire(self.name, to_milliseconds(self.ttl))
        return result  # skipif-ci-and-not-linux

    async def values(self, redis: Redis, /) -> Sequence[V]:
        """Get the values of a hashmap in `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = await cast("Awaitable[Sequence[bytes]]", redis.hvals(self.name))
        return [  # skipif-ci-and-not-linux
            _deserialize(data, deserializer=self.value_deserializer) for data in result
        ]


@overload
def redis_hash_map_key[K, V](
    name: str,
    key: type[K],
    value: type[V],
    /,
    *,
    key_serializer: Callable[[K], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V], bytes] | None = None,
    value_deserializer: Callable[[bytes], V] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K, V]: ...
@overload
def redis_hash_map_key[K, V1, V2](
    name: str,
    key: type[K],
    value: tuple[type[V1], type[V2]],
    /,
    *,
    key_serializer: Callable[[K], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K, V1 | V2]: ...
@overload
def redis_hash_map_key[K, V1, V2, V3](
    name: str,
    key: type[K],
    value: tuple[type[V1], type[V2], type[V3]],
    /,
    *,
    key_serializer: Callable[[K], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2 | V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2 | V3] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K, V1 | V2 | V3]: ...
@overload
def redis_hash_map_key[K1, K2, V](
    name: str,
    key: tuple[type[K1], type[K2]],
    value: type[V],
    /,
    *,
    key_serializer: Callable[[K1 | K2], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V], bytes] | None = None,
    value_deserializer: Callable[[bytes], V] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K1 | K2, V]: ...
@overload
def redis_hash_map_key[K1, K2, V1, V2](
    name: str,
    key: tuple[type[K1], type[K2]],
    value: tuple[type[V1], type[V2]],
    /,
    *,
    key_serializer: Callable[[K1 | K2], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K1 | K2, V1 | V2]: ...
@overload
def redis_hash_map_key[K1, K2, V1, V2, V3](
    name: str,
    key: tuple[type[K1], type[K2]],
    value: tuple[type[V1], type[V2], type[V3]],
    /,
    *,
    key_serializer: Callable[[K1 | K2], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2 | V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2 | V3] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K1 | K2, V1 | V2 | V3]: ...
@overload
def redis_hash_map_key[K1, K2, K3, V](
    name: str,
    key: tuple[type[K1], type[K2], type[K3]],
    value: type[V],
    /,
    *,
    key_serializer: Callable[[K1 | K2 | K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V], bytes] | None = None,
    value_deserializer: Callable[[bytes], V] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K1 | K2 | K3, V]: ...
@overload
def redis_hash_map_key[K1, K2, K3, V1, V2](
    name: str,
    key: tuple[type[K1], type[K2], type[K3]],
    value: tuple[type[V1], type[V2]],
    /,
    *,
    key_serializer: Callable[[K1 | K2 | K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K1 | K2 | K3, V1 | V2]: ...
@overload
def redis_hash_map_key[K1, K2, K3, V1, V2, V3](
    name: str,
    key: tuple[type[K1], type[K2], type[K3]],
    value: tuple[type[V1], type[V2], type[V3]],
    /,
    *,
    key_serializer: Callable[[K1 | K2 | K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2 | V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2 | V3] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K1 | K2 | K3, V1 | V2 | V3]: ...
@overload
def redis_hash_map_key[K, K1, K2, K3, V, V1, V2, V3](
    name: str,
    key: TypeLike[K],
    value: TypeLike[V],
    /,
    *,
    key_serializer: Callable[[K1 | K2 | K3], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[V1 | V2 | V3], bytes] | None = None,
    value_deserializer: Callable[[bytes], V1 | V2 | V3] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisHashMapKey[K, V]: ...
def redis_hash_map_key[K, V](
    name: str,
    key: TypeLike[K],
    value: TypeLike[V],
    /,
    *,
    key_serializer: Callable[[Any], bytes] | None = None,
    key_deserializer: Callable[[bytes], Any] | None = None,
    value_serializer: Callable[[Any], bytes] | None = None,
    value_deserializer: Callable[[bytes], Any] | None = None,
    timeout: Delta | None = None,
    ttl: Delta | None = None,
    error: type[Exception] = TimeoutError,
) -> RedisHashMapKey[K, V]:
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
class RedisKey[T]:
    """A key in a redis store."""

    name: str
    type: TypeLike[T]
    serializer: Callable[[T], bytes] | None = None
    deserializer: Callable[[bytes], T] | None = None
    timeout: Delta | None = None
    error: MaybeType[BaseException] = TimeoutError
    ttl: Delta | None = None

    async def delete(self, redis: Redis, /) -> int:
        """Delete the key from `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            response = await redis.delete(self.name)
        return ensure_int(response)

    async def exists(self, redis: Redis, /) -> bool:
        """Check if the key exists in `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = cast("Literal[0, 1]", await redis.exists(self.name))
        match result:  # skipif-ci-and-not-linux
            case 0 | 1 as value:
                return bool(value)
            case _ as never:
                assert_never(never)

    async def get(self, redis: Redis, /) -> T:
        """Get a value from `redis`."""
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = cast("bytes | None", await redis.get(self.name))
        if result is None:  # skipif-ci-and-not-linux
            raise KeyError(self.name)
        return _deserialize(  # skipif-ci-and-not-linux
            result, deserializer=self.deserializer
        )

    async def set(self, redis: Redis, value: T, /) -> int:
        """Set a value in `redis`."""
        ser = _serialize(value, serializer=self.serializer)  # skipif-ci-and-not-linux
        ttl = (  # skipif-ci-and-not-linux
            None if self.ttl is None else to_milliseconds(self.ttl)
        )
        async with timeout_td(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            response = await redis.set(  # skipif-ci-and-not-linux
                self.name, ser, px=ttl
            )
        return ensure_int(response)  # skipif-ci-and-not-linux


@overload
def redis_key[T](
    name: str,
    type_: type[T],
    /,
    *,
    serializer: Callable[[T], bytes] | None = None,
    deserializer: Callable[[bytes], T] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T]: ...
@overload
def redis_key[T1, T2](
    name: str,
    type_: tuple[type[T1], type[T2]],
    /,
    *,
    serializer: Callable[[T1 | T2], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T1 | T2]: ...
@overload
def redis_key[T1, T2, T3](
    name: str,
    type_: tuple[type[T1], type[T2], type[T3]],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T1 | T2 | T3]: ...
@overload
def redis_key[T1, T2, T3, T4](
    name: str,
    type_: tuple[type[T1], type[T2], type[T3], type[T4]],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3 | T4], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3 | T4] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T1 | T2 | T3 | T4]: ...
@overload
def redis_key[T1, T2, T3, T4, T5](
    name: str,
    type_: tuple[type[T1], type[T2], type[T3], type[T4], type[T5]],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3 | T4 | T5], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3 | T4 | T5] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T1 | T2 | T3 | T4 | T5]: ...
@overload
def redis_key[T, T1, T2, T3, T4, T5](
    name: str,
    type_: TypeLike[T],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3 | T4 | T5], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3 | T4 | T5] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T]: ...
def redis_key[T](
    name: str,
    type_: TypeLike[T],
    /,
    *,
    serializer: Callable[[Any], bytes] | None = None,
    deserializer: Callable[[bytes], Any] | None = None,
    timeout: Delta | None = None,
    error: type[Exception] = TimeoutError,
    ttl: Delta | None = None,
) -> RedisKey[T]:
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


@overload
async def publish[T](
    redis: Redis,
    channel: str,
    data: T,
    /,
    *,
    serializer: Callable[[T], EncodableT],
    timeout: Delta = _PUBLISH_TIMEOUT,
) -> int: ...
@overload
async def publish(
    redis: Redis,
    channel: str,
    data: bytes | str,
    /,
    *,
    serializer: None = None,
    timeout: Delta = _PUBLISH_TIMEOUT,
) -> int: ...
@overload
async def publish[T](
    redis: Redis,
    channel: str,
    data: bytes | str | T,
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Delta = _PUBLISH_TIMEOUT,
) -> int: ...
async def publish[T](
    redis: Redis,
    channel: str,
    data: bytes | str | T,
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Delta = _PUBLISH_TIMEOUT,
) -> int:
    """Publish an object to a channel."""
    match data, serializer:  # skipif-ci-and-not-linux
        case bytes() | str() as data_use, _:
            ...
        case _, None:
            raise PublishError(data=data)
        case _, Callable():
            data_use = serializer(data)
        case _ as never:
            assert_never(never)
    async with timeout_td(timeout):  # skipif-ci-and-not-linux
        response = await redis.publish(channel, data_use)  # skipif-ci-and-not-linux
    return ensure_int(response)  # skipif-ci-and-not-linux


@dataclass(kw_only=True, slots=True)
class PublishError(Exception):
    data: Any

    @override
    def __str__(self) -> str:
        return f"Unable to publish data {self.data!r} with no serializer"


##


async def publish_many[T](
    redis: Redis,
    channel: str,
    data: MaybeSequence[bytes | T] | MaybeListStr,
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Delta = _PUBLISH_TIMEOUT,
) -> Sequence[bool]:
    """Publish an object/multiple objects to a channel."""
    async with TaskGroup() as tg:
        tasks = [
            tg.create_task(
                _try_publish(
                    redis,
                    channel,
                    d,
                    serializer=cast("Callable[[Any], EncodableT]", serializer),
                    timeout=timeout,
                )
            )
            for d in always_iterable(data)
        ]
    return [t.result() for t in tasks]


async def _try_publish[T](
    redis: Redis,
    channel: str,
    data: bytes | str | T,
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Delta = _PUBLISH_TIMEOUT,
) -> bool:
    try:
        _ = await publish(redis, channel, data, serializer=serializer, timeout=timeout)
    except TimeoutError:
        return False
    return True


##


_SUBSCRIBE_TIMEOUT: Delta = SECOND
_SUBSCRIBE_SLEEP: Delta = MILLISECOND


@overload
@enhanced_async_context_manager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_RedisMessage],
    /,
    *,
    timeout: Delta | None = _SUBSCRIBE_TIMEOUT,
    sleep: Delta = _SUBSCRIBE_SLEEP,
    output: Literal["raw"],
    filter_: Callable[[_RedisMessage], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@overload
@enhanced_async_context_manager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[bytes],
    /,
    *,
    timeout: Delta | None = _SUBSCRIBE_TIMEOUT,
    sleep: Delta = _SUBSCRIBE_SLEEP,
    output: Literal["bytes"],
    filter_: Callable[[bytes], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@overload
@enhanced_async_context_manager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[str],
    /,
    *,
    timeout: Delta | None = _SUBSCRIBE_TIMEOUT,
    sleep: Delta = _SUBSCRIBE_SLEEP,
    output: Literal["text"] = "text",
    filter_: Callable[[str], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@overload
@enhanced_async_context_manager
def subscribe[T](
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[T],
    /,
    *,
    timeout: Delta | None = _SUBSCRIBE_TIMEOUT,
    sleep: Delta = _SUBSCRIBE_SLEEP,
    output: Callable[[bytes], T],
    filter_: Callable[[T], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@enhanced_async_context_manager
async def subscribe[T](
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_RedisMessage] | Queue[bytes] | Queue[T],
    /,
    *,
    timeout: Delta | None = _SUBSCRIBE_TIMEOUT,
    sleep: Delta = _SUBSCRIBE_SLEEP,
    output: Literal["raw", "bytes", "text"] | Callable[[bytes], T] = "text",
    filter_: Callable[[Any], bool] | None = None,
) -> AsyncIterator[Task[None]]:
    """Subscribe to the data of a given channel(s)."""
    channels = list(always_iterable(channels))  # skipif-ci-and-not-linux
    match output:  # skipif-ci-and-not-linux
        case "raw":
            transform = cast("Any", identity)
        case "bytes":
            transform = cast("Any", itemgetter("data"))
        case "text":

            def transform(message: _RedisMessage, /) -> str:  # pyright: ignore[reportRedeclaration]
                return message["data"].decode()

        case Callable() as deserialize:

            def transform(message: _RedisMessage, /) -> T:
                return deserialize(message["data"])

        case _ as never:
            assert_never(never)

    task = create_task(  # skipif-ci-and-not-linux
        _subscribe_core(
            redis,
            channels,
            transform,
            queue,
            timeout=timeout,
            sleep=sleep,
            filter_=filter_,
        )
    )
    try:  # skipif-ci-and-not-linux
        yield task
    finally:  # skipif-ci-and-not-linux
        try:
            _ = task.cancel()
        except RuntimeError as error:  # pragma: no cover
            from utilities.pytest import is_pytest

            if (not is_pytest()) or (error.args[0] != "Event loop is closed"):
                raise
        with suppress(CancelledError):
            await task


async def _subscribe_core(
    redis: Redis,
    channels: MaybeIterable[str],
    transform: Callable[[_RedisMessage], Any],
    queue: Queue[Any],
    /,
    *,
    timeout: Delta | None = _SUBSCRIBE_TIMEOUT,
    sleep: Delta = _SUBSCRIBE_SLEEP,
    filter_: Callable[[Any], bool] | None = None,
) -> None:
    timeout_use = (  # skipif-ci-and-not-linux
        None if timeout is None else to_seconds(timeout)
    )
    is_subscribe_message = partial(  # skipif-ci-and-not-linux
        _is_message, channels={c.encode() for c in channels}
    )
    async with yield_pubsub(redis, channels) as pubsub:  # skipif-ci-and-not-linux
        while True:
            message = await pubsub.get_message(timeout=timeout_use)
            if is_subscribe_message(message):
                transformed = transform(message)
                if (filter_ is None) or filter_(transformed):
                    queue.put_nowait(transformed)
            else:
                await sleep_td(sleep)


def _is_message(
    message: Any, /, *, channels: Collection[bytes]
) -> TypeGuard[_RedisMessage]:
    return (
        isinstance(message, Mapping)
        and ("type" in message)
        and (message["type"] in {"subscribe", "psubscribe", "message", "pmessage"})
        and ("pattern" in message)
        and ((message["pattern"] is None) or isinstance(message["pattern"], str))
        and ("channel" in message)
        and (message["channel"] in channels)
        and ("data" in message)
        and isinstance(message["data"], bytes)
    )


class _RedisMessage(TypedDict):
    type: Literal["subscribe", "psubscribe", "message", "pmessage"]
    pattern: str | None
    channel: bytes
    data: bytes


##


@enhanced_async_context_manager
async def yield_pubsub(
    redis: Redis, channels: MaybeIterable[str], /
) -> AsyncIterator[PubSub]:
    """Yield a PubSub instance subscribed to some channels."""
    pubsub = redis.pubsub()  # skipif-ci-and-not-linux
    channels = list(always_iterable(channels))  # skipif-ci-and-not-linux
    await pubsub.subscribe(*channels)  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        yield pubsub
    finally:  # skipif-ci-and-not-linux
        await pubsub.unsubscribe(*channels)
        await pubsub.aclose()


##


_HOST = "localhost"
_PORT = 6379


@enhanced_async_context_manager
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


def _serialize[T](
    obj: T, /, *, serializer: Callable[[T], bytes] | None = None
) -> bytes:
    if serializer is None:  # skipif-ci-and-not-linux
        from utilities.orjson import serialize as serializer_use
    else:  # skipif-ci-and-not-linux
        serializer_use = serializer
    return serializer_use(obj)  # skipif-ci-and-not-linux


def _deserialize[T](
    data: bytes, /, *, deserializer: Callable[[bytes], T] | None = None
) -> T:
    if deserializer is None:  # skipif-ci-and-not-linux
        from utilities.orjson import deserialize as deserializer_use
    else:  # skipif-ci-and-not-linux
        deserializer_use = deserializer
    return deserializer_use(data)  # skipif-ci-and-not-linux


__all__ = [
    "RedisHashMapKey",
    "RedisKey",
    "publish",
    "publish_many",
    "redis_hash_map_key",
    "redis_key",
    "subscribe",
    "yield_pubsub",
    "yield_redis",
]
