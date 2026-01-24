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

import utilities.asyncio
from utilities.asyncio import timeout
from utilities.constants import MILLISECOND, SECOND
from utilities.contextlib import enhanced_async_context_manager
from utilities.core import (
    always_iterable,
    async_sleep,
    duration_to_milliseconds,
    duration_to_seconds,
    identity,
    is_pytest,
    one,
)
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_int
from utilities.math import safe_round
from utilities.typing import is_instance_gen

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Collection, Iterable

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import EncodableT

    from utilities.types import (
        Duration,
        MaybeIterable,
        MaybeSequence,
        MaybeType,
        TypeLike,
    )


_PUBLISH_TIMEOUT: Duration = SECOND


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
    timeout: Duration | None = None
    error: MaybeType[BaseException] = TimeoutError
    ttl: Duration | None = None

    async def delete(self, redis: Redis, key: K, /) -> int:
        """Delete a key from a hashmap in `redis`."""
        ser = _serialize(  # skipif-ci-and-not-linux
            key, serializer=self.key_serializer
        ).decode()
        async with timeout(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            return await cast("Awaitable[int]", redis.hdel(self.name, ser))
        raise ImpossibleCaseError(case=[f"{redis=}", f"{key=}"])  # pragma: no cover

    async def exists(self, redis: Redis, key: K, /) -> bool:
        """Check if the key exists in a hashmap in `redis`."""
        ser = _serialize(  # skipif-ci-and-not-linux
            key, serializer=self.key_serializer
        ).decode()
        async with timeout(  # skipif-ci-and-not-linux
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
        async with timeout(  # skipif-ci-and-not-linux
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
        async with timeout(  # skipif-ci-and-not-linux
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
        async with timeout(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = await cast("Awaitable[Sequence[bytes]]", redis.hkeys(self.name))
        return [  # skipif-ci-and-not-linux
            _deserialize(data, deserializer=self.key_deserializer) for data in result
        ]

    async def length(self, redis: Redis, /) -> int:
        """Get the length of a hashmap in `redis`."""
        async with timeout(  # skipif-ci-and-not-linux
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
        async with timeout(  # skipif-ci-and-not-linux
            self.timeout, error=self.error
        ):
            result = await cast(
                "Awaitable[int]", redis.hset(self.name, mapping=cast("Any", ser))
            )
            if self.ttl is not None:
                await redis.pexpire(
                    self.name, safe_round(duration_to_milliseconds(self.ttl))
                )
        return result  # skipif-ci-and-not-linux

    async def values(self, redis: Redis, /) -> Sequence[V]:
        """Get the values of a hashmap in `redis`."""
        async with timeout(  # skipif-ci-and-not-linux
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = None,
    ttl: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
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
    timeout: Duration | None = None
    error: MaybeType[BaseException] = TimeoutError
    ttl: Duration | None = None

    async def delete(self, redis: Redis, /) -> int:
        """Delete the key from `redis`."""
        async with timeout(self.timeout, error=self.error):  # skipif-ci-and-not-linux
            response = await redis.delete(self.name)
        return ensure_int(response)

    async def exists(self, redis: Redis, /) -> bool:
        """Check if the key exists in `redis`."""
        async with timeout(self.timeout, error=self.error):  # skipif-ci-and-not-linux
            result = cast("Literal[0, 1]", await redis.exists(self.name))
        match result:  # skipif-ci-and-not-linux
            case 0 | 1 as value:
                return bool(value)
            case never:
                assert_never(never)

    async def get(self, redis: Redis, /) -> T:
        """Get a value from `redis`."""
        async with timeout(self.timeout, error=self.error):  # skipif-ci-and-not-linux
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
            None if self.ttl is None else safe_round(duration_to_milliseconds(self.ttl))
        )
        async with timeout(  # skipif-ci-and-not-linux
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
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[T]: ...
@overload
def redis_key[T1, T2](
    name: str,
    type_: tuple[type[T1], type[T2]],
    /,
    *,
    serializer: Callable[[T1 | T2], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2] | None = None,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[T1 | T2]: ...
@overload
def redis_key[T1, T2, T3](
    name: str,
    type_: tuple[type[T1], type[T2], type[T3]],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3] | None = None,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[T1 | T2 | T3]: ...
@overload
def redis_key[T1, T2, T3, T4](
    name: str,
    type_: tuple[type[T1], type[T2], type[T3], type[T4]],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3 | T4], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3 | T4] | None = None,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[T1 | T2 | T3 | T4]: ...
@overload
def redis_key[T1, T2, T3, T4, T5](
    name: str,
    type_: tuple[type[T1], type[T2], type[T3], type[T4], type[T5]],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3 | T4 | T5], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3 | T4 | T5] | None = None,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[T1 | T2 | T3 | T4 | T5]: ...
@overload
def redis_key[T, T1, T2, T3, T4, T5](
    name: str,
    type_: TypeLike[T],
    /,
    *,
    serializer: Callable[[T1 | T2 | T3 | T4 | T5], bytes] | None = None,
    deserializer: Callable[[bytes], T1 | T2 | T3 | T4 | T5] | None = None,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
) -> RedisKey[T]: ...
def redis_key[T](
    name: str,
    type_: TypeLike[T],
    /,
    *,
    serializer: Callable[[Any], bytes] | None = None,
    deserializer: Callable[[bytes], Any] | None = None,
    timeout: Duration | None = None,
    error: MaybeType[BaseException] = TimeoutError,
    ttl: Duration | None = None,
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
    timeout: Duration | None = _PUBLISH_TIMEOUT,
) -> int: ...
@overload
async def publish(
    redis: Redis,
    channel: str,
    data: bytes | str,
    /,
    *,
    serializer: None = None,
    timeout: Duration | None = _PUBLISH_TIMEOUT,
) -> int: ...
@overload
async def publish[T](
    redis: Redis,
    channel: str,
    data: bytes | str | T,
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Duration | None = _PUBLISH_TIMEOUT,
) -> int: ...
async def publish[T](
    redis: Redis,
    channel: str,
    data: bytes | str | T,
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Duration | None = _PUBLISH_TIMEOUT,
) -> int:
    """Publish an object to a channel."""
    match data, serializer:  # skipif-ci-and-not-linux
        case bytes() | str() as data_use, _:
            ...
        case _, None:
            raise PublishError(data=data)
        case _, Callable():
            data_use = serializer(data)
        case never:
            assert_never(never)
    async with utilities.asyncio.timeout(timeout):  # skipif-ci-and-not-linux
        response = await redis.publish(channel, data_use)
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
    data: MaybeSequence[bytes | str | T],
    /,
    *,
    serializer: Callable[[T], EncodableT] | None = None,
    timeout: Duration | None = _PUBLISH_TIMEOUT,
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
    timeout: Duration | None = _PUBLISH_TIMEOUT,
) -> bool:
    try:
        _ = await publish(redis, channel, data, serializer=serializer, timeout=timeout)
    except TimeoutError:
        return False
    return True


##


_SUBSCRIBE_TIMEOUT: Duration = SECOND
_SUBSCRIBE_SLEEP: Duration = MILLISECOND


@overload
@enhanced_async_context_manager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_RedisMessage],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    output: Literal["raw"],
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[bytes], bool] | None = None,
    error_filter: Callable[[bytes, Exception], None] | None = None,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[Task[None]]: ...
@overload
@enhanced_async_context_manager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[bytes],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    output: Literal["bytes"],
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[bytes], bool] | None = None,
    error_filter: Callable[[bytes, Exception], None] | None = None,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[Task[None]]: ...
@overload
@enhanced_async_context_manager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[str],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    output: Literal["text"] = "text",
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[str], bool] | None = None,
    error_filter: Callable[[str, Exception], None] | None = None,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[Task[None]]: ...
@overload
@enhanced_async_context_manager
def subscribe[T](
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[T],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    output: Callable[[bytes], T],
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[T], bool] | None = None,
    error_filter: Callable[[T, Exception], None] | None = None,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[Task[None]]: ...
@enhanced_async_context_manager
async def subscribe[T](
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_RedisMessage] | Queue[bytes] | Queue[T],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    output: Literal["raw", "bytes", "text"] | Callable[[bytes], T] = "text",
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[T], bool] | None = None,
    error_filter: Callable[[T, Exception], None] | None = None,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[Task[None]]:
    """Subscribe to the data of a given channel(s)."""
    channels = list(always_iterable(channels))  # skipif-ci-and-not-linux
    match output:  # skipif-ci-and-not-linux
        case "raw":
            transform = cast("Callable[[_RedisMessage], T]", identity)
        case "bytes":
            transform = cast("Callable[[_RedisMessage], T]", itemgetter("data"))
        case "text":
            transform = cast("Callable[[_RedisMessage], T]", _decoded_data)
        case Callable() as deserialize:

            def transform(message: _RedisMessage, /) -> T:
                return deserialize(message["data"])

        case never:
            assert_never(never)

    task = create_task(  # skipif-ci-and-not-linux
        _subscribe_core(
            redis,
            channels,
            transform,
            queue,
            timeout=timeout,
            error_transform=error_transform,
            filter_=filter_,
            error_filter=error_filter,
            sleep=sleep,
        )
    )
    try:  # skipif-ci-and-not-linux
        yield task
    finally:  # skipif-ci-and-not-linux
        try:
            _ = task.cancel()
        except RuntimeError as error:  # pragma: no cover
            if (not is_pytest()) or (error.args[0] != "Event loop is closed"):
                raise
        with suppress(CancelledError):
            await task


def _decoded_data(message: _RedisMessage, /) -> str:
    return message["data"].decode()


async def _subscribe_core[T](
    redis: Redis,
    channels: MaybeIterable[str],
    transform: Callable[[_RedisMessage], T],
    queue: Queue[Any],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[T], bool] | None = None,
    error_filter: Callable[[T, Exception], None] | None = None,
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> None:
    timeout_use = (  # skipif-ci-and-not-linux
        None if timeout is None else duration_to_seconds(timeout)
    )
    is_subscribe_message = partial(  # skipif-ci-and-not-linux
        _is_message, channels={c.encode() for c in channels}
    )
    async with yield_pubsub(redis, channels) as pubsub:  # skipif-ci-and-not-linux
        while True:
            message = await pubsub.get_message(timeout=timeout_use)
            if is_subscribe_message(message):
                _handle_message(
                    message,
                    transform,
                    queue,
                    error_transform=error_transform,
                    filter_=filter_,
                    error_filter=error_filter,
                )
            else:
                await async_sleep(sleep)


def _is_message(
    message: Any, /, *, channels: Collection[bytes]
) -> TypeGuard[_RedisMessage]:
    return is_instance_gen(message, _RedisMessage) and (message["channel"] in channels)


def _handle_message[T](
    message: _RedisMessage,
    transform: Callable[[_RedisMessage], T],
    queue: Queue[Any],
    /,
    *,
    error_transform: Callable[[_RedisMessage, Exception], None] | None = None,
    filter_: Callable[[T], bool] | None = None,
    error_filter: Callable[[T, Exception], None] | None = None,
) -> None:
    try:
        transformed = transform(message)
    except Exception as error:  # noqa: BLE001
        if error_transform is not None:
            error_transform(message, error)
        return
    if filter_ is None:
        queue.put_nowait(transformed)
        return
    try:
        if filter_(transformed):
            queue.put_nowait(transformed)
    except Exception as error:  # noqa: BLE001
        if error_filter is not None:
            error_filter(transformed, error)


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
        import utilities.orjson

        serializer_use = utilities.orjson.serialize
    else:  # skipif-ci-and-not-linux
        serializer_use = serializer
    return serializer_use(obj)  # skipif-ci-and-not-linux


def _deserialize[T](
    data: bytes, /, *, deserializer: Callable[[bytes], T] | None = None
) -> T:
    if deserializer is None:  # skipif-ci-and-not-linux
        import utilities.orjson

        deserializer_use = utilities.orjson.deserialize
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
