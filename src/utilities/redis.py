from __future__ import annotations

import asyncio
from asyncio import CancelledError, Event, Queue, Task, create_task
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from functools import partial
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Self,
    TypedDict,
    TypeGuard,
    TypeVar,
    assert_never,
    cast,
    overload,
    override,
)

from redis.asyncio import Redis
from redis.typing import EncodableT

from utilities.asyncio import (
    EnhancedQueue,
    InfiniteQueueLooper,
    Looper,
    LooperTimeoutError,
    timeout_dur,
)
from utilities.contextlib import suppress_super_object_attribute_error
from utilities.datetime import (
    MILLISECOND,
    SECOND,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
)
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_int, get_class_name, identity
from utilities.iterables import always_iterable, one
from utilities.orjson import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Collection,
        Iterable,
        Iterator,
        Sequence,
    )
    from types import TracebackType

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import ResponseT

    from utilities.iterables import MaybeIterable
    from utilities.types import Duration, MaybeType, TypeLike


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


_PUBLISH_TIMEOUT: Duration = SECOND


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
    data: bytes | str,
    /,
    *,
    serializer: None = None,
    timeout: Duration = _PUBLISH_TIMEOUT,
) -> ResponseT: ...
@overload
async def publish(
    redis: Redis,
    channel: str,
    data: bytes | str | _T,
    /,
    *,
    serializer: Callable[[_T], EncodableT] | None = None,
    timeout: Duration = _PUBLISH_TIMEOUT,
) -> ResponseT: ...
async def publish(
    redis: Redis,
    channel: str,
    data: bytes | str | _T,
    /,
    *,
    serializer: Callable[[_T], EncodableT] | None = None,
    timeout: Duration = _PUBLISH_TIMEOUT,
) -> ResponseT:
    """Publish an object to a channel."""
    match data, serializer:  # skipif-ci-and-not-linux
        case bytes() | str() as data_use, _:
            ...
        case _, None:
            raise PublishError(data=data, serializer=serializer)
        case _, Callable():
            data_use = serializer(data)
        case _ as never:
            assert_never(never)
    async with timeout_dur(duration=timeout):  # skipif-ci-and-not-linux
        return await redis.publish(channel, data_use)  # skipif-ci-and-not-linux


@dataclass(kw_only=True, slots=True)
class PublishError(Exception):
    data: Any
    serializer: Callable[[Any], EncodableT] | None = None

    @override
    def __str__(self) -> str:
        return (
            f"Unable to publish data {self.data!r} with serializer {self.serializer!r}"
        )


##


@dataclass(kw_only=True)
class Publisher(InfiniteQueueLooper[None, tuple[str, EncodableT]]):
    """Publish a set of messages to Redis."""

    redis: Redis
    serializer: Callable[[Any], EncodableT] | None = None
    timeout: Duration = _PUBLISH_TIMEOUT

    @override
    async def _process_queue(self) -> None:
        for item in self._queue.get_all_nowait():  # skipif-ci-and-not-linux
            channel, data = item
            _ = await publish(
                self.redis,
                channel,
                data,
                serializer=self.serializer,
                timeout=self.timeout,
            )

    @override
    def _yield_events_and_exceptions(
        self,
    ) -> Iterator[tuple[None, MaybeType[Exception]]]:
        yield (None, PublisherError)  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class PublisherError(Exception):
    publisher: Publisher

    @override
    def __str__(self) -> str:
        return f"Error running {get_class_name(self.publisher)!r}"  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class PublishService(Looper[tuple[str, _T]]):
    """Service to publish items to Redis."""

    # base
    freq: Duration = field(default=MILLISECOND, repr=False)
    backoff: Duration = field(default=SECOND, repr=False)
    empty_upon_exit: bool = field(default=True, repr=False)
    # self
    redis: Redis
    serializer: Callable[[_T], EncodableT] = serialize
    publish_timeout: Duration = _PUBLISH_TIMEOUT

    @override
    async def core(self) -> None:
        await super().core()  # skipif-ci-and-not-linux
        while not self.empty():  # skipif-ci-and-not-linux
            channel, data = self.get_left_nowait()
            _ = await publish(
                self.redis,
                channel,
                data,
                serializer=self.serializer,
                timeout=self.publish_timeout,
            )


##


@dataclass(kw_only=True)
class PublishServiceMixin(Generic[_T]):
    """Mix-in for the publish service."""

    # base - looper
    publish_service_freq: Duration = field(default=MILLISECOND, repr=False)
    publish_service_backoff: Duration = field(default=SECOND, repr=False)
    publish_service_empty_upon_exit: bool = field(default=False, repr=False)
    publish_service_logger: str | None = field(default=None, repr=False)
    publish_service_timeout: Duration | None = field(default=None, repr=False)
    publish_service_timeout_error: type[Exception] = field(
        default=LooperTimeoutError, repr=False
    )
    publish_service_debug: bool = field(default=False, repr=False)
    _is_pending_restart: Event = field(default_factory=Event, init=False, repr=False)
    # base - publish service
    publish_service_redis: Redis
    publish_service_serializer: Callable[[_T], EncodableT] = serialize
    publish_service_publish_timeout: Duration = _PUBLISH_TIMEOUT
    # self
    _publish_service: PublishService[_T] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        with suppress_super_object_attribute_error():  # skipif-ci-and-not-linux
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self._publish_service = PublishService(  # skipif-ci-and-not-linux
            # looper
            freq=self.publish_service_freq,
            backoff=self.publish_service_backoff,
            empty_upon_exit=self.publish_service_empty_upon_exit,
            logger=self.publish_service_logger,
            timeout=self.publish_service_timeout,
            timeout_error=self.publish_service_timeout_error,
            _debug=self.publish_service_debug,
            # publish service
            redis=self.publish_service_redis,
            serializer=self.publish_service_serializer,
            publish_timeout=self.publish_service_publish_timeout,
        )

    def _yield_sub_loopers(self) -> Iterator[Looper[Any]]:
        with suppress_super_object_attribute_error():  # skipif-ci-and-not-linux
            yield from super()._yield_sub_loopers()  # pyright: ignore[reportAttributeAccessIssue]
        yield self._publish_service  # skipif-ci-and-not-linux


##


_SUBSCRIBE_TIMEOUT: Duration = SECOND
_SUBSCRIBE_SLEEP: Duration = MILLISECOND


@overload
@asynccontextmanager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_RedisMessage],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
    output: Literal["raw"],
    filter_: Callable[[_RedisMessage], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@overload
@asynccontextmanager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[bytes],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
    output: Literal["bytes"],
    filter_: Callable[[bytes], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@overload
@asynccontextmanager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[str],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
    output: Literal["text"] = "text",
    filter_: Callable[[str], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@overload
@asynccontextmanager
def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_T],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
    output: Callable[[bytes], _T],
    filter_: Callable[[_T], bool] | None = None,
) -> AsyncIterator[Task[None]]: ...
@asynccontextmanager
async def subscribe(
    redis: Redis,
    channels: MaybeIterable[str],
    queue: Queue[_RedisMessage] | Queue[bytes] | Queue[_T],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
    output: Literal["raw", "bytes", "text"] | Callable[[bytes], _T] = "text",
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

            def transform(message: _RedisMessage, /) -> _T:
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
        _ = task.cancel()
        with suppress(CancelledError):
            await task


async def _subscribe_core(
    redis: Redis,
    channels: MaybeIterable[str],
    transform: Callable[[_RedisMessage], Any],
    queue: Queue[Any],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,
    sleep: Duration = _SUBSCRIBE_SLEEP,
    filter_: Callable[[Any], bool] | None = None,
) -> None:
    timeout_use = (  # skipif-ci-and-not-linux
        None if timeout is None else datetime_duration_to_float(timeout)
    )
    sleep_use = datetime_duration_to_float(sleep)  # skipif-ci-and-not-linux
    is_subscribe_message = partial(  # skipif-ci-and-not-linux
        _is_message, channels={c.encode() for c in channels}
    )
    async with yield_pubsub(redis, channels) as pubsub:  # skipif-ci-and-not-linux
        while True:
            message = await pubsub.get_message(timeout=timeout_use)
            if is_subscribe_message(message):
                transformed = transform(message)
                if (filter_ is None) or filter_(transformed):
                    if isinstance(queue, EnhancedQueue):
                        queue.put_right_nowait(transformed)
                    else:
                        queue.put_nowait(transformed)
            else:
                await asyncio.sleep(sleep_use)


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


@dataclass(kw_only=True)
class SubscribeService(Looper[_T]):
    """Service to subscribe to Redis."""

    # base
    freq: Duration = field(default=MILLISECOND, repr=False)
    backoff: Duration = field(default=SECOND, repr=False)
    logger: str | None = field(default=__name__, repr=False)
    # self
    redis: Redis
    channel: str
    deserializer: Callable[[bytes], _T] = deserialize
    subscribe_timeout: Duration | None = _SUBSCRIBE_TIMEOUT
    subscribe_sleep: Duration = _SUBSCRIBE_SLEEP
    filter_: Callable[[_T], bool] | None = None
    _is_subscribed: Event = field(default_factory=Event, init=False, repr=False)

    @override
    async def __aenter__(self) -> Self:
        _ = await super().__aenter__()  # skipif-ci-and-not-linux
        match self._is_subscribed.is_set():  # skipif-ci-and-not-linux
            case True:
                _ = self._debug and self._logger.debug("%s: already subscribing", self)
            case False:
                _ = self._debug and self._logger.debug(
                    "%s: starting subscription...", self
                )
                self._is_subscribed.set()
                _ = await self._stack.enter_async_context(
                    subscribe(
                        self.redis,
                        self.channel,
                        self._queue,
                        timeout=self.subscribe_timeout,
                        sleep=self.subscribe_sleep,
                        output=self.deserializer,
                        filter_=self.filter_,
                    )
                )
            case _ as never:
                assert_never(never)
        return self  # skipif-ci-and-not-linux

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await super().__aexit__(  # skipif-ci-and-not-linux
            exc_type=exc_type, exc_value=exc_value, traceback=traceback
        )
        match self._is_subscribed.is_set():  # skipif-ci-and-not-linux
            case True:
                _ = self._debug and self._logger.debug(
                    "%s: stopping subscription...", self
                )
                self._is_subscribed.clear()
            case False:
                _ = self._debug and self._logger.debug(
                    "%s: already stopped subscription", self
                )
            case _ as never:
                assert_never(never)


##


@dataclass(kw_only=True)
class SubscribeServiceMixin(Generic[_T]):
    """Mix-in for the subscribe service."""

    # base - looper
    subscribe_service_freq: Duration = field(default=MILLISECOND, repr=False)
    subscribe_service_backoff: Duration = field(default=SECOND, repr=False)
    subscribe_service_empty_upon_exit: bool = field(default=False, repr=False)
    subscribe_service_logger: str | None = field(default=None, repr=False)
    subscribe_service_timeout: Duration | None = field(default=None, repr=False)
    subscribe_service_timeout_error: type[Exception] = field(
        default=LooperTimeoutError, repr=False
    )
    subscribe_service_debug: bool = field(default=False, repr=False)
    # base - looper
    subscribe_service_redis: Redis
    subscribe_service_channel: str
    subscribe_service_deserializer: Callable[[bytes], _T] = deserialize
    subscribe_service_subscribe_sleep: Duration = _SUBSCRIBE_SLEEP
    subscribe_service_subscribe_timeout: Duration | None = _SUBSCRIBE_TIMEOUT
    # self
    _subscribe_service: SubscribeService[_T] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        with suppress_super_object_attribute_error():  # skipif-ci-and-not-linux
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self._subscribe_service = SubscribeService(  # skipif-ci-and-not-linux
            # looper
            freq=self.subscribe_service_freq,
            backoff=self.subscribe_service_backoff,
            empty_upon_exit=self.subscribe_service_empty_upon_exit,
            logger=self.subscribe_service_logger,
            timeout=self.subscribe_service_timeout,
            timeout_error=self.subscribe_service_timeout_error,
            _debug=self.subscribe_service_debug,
            # subscribe service
            redis=self.subscribe_service_redis,
            channel=self.subscribe_service_channel,
            deserializer=self.subscribe_service_deserializer,
            subscribe_sleep=self.subscribe_service_subscribe_sleep,
            subscribe_timeout=self.subscribe_service_subscribe_timeout,
        )

    def _yield_sub_loopers(self) -> Iterator[Looper[Any]]:
        with suppress_super_object_attribute_error():  # skipif-ci-and-not-linux
            yield from super()._yield_sub_loopers()  # pyright: ignore[reportAttributeAccessIssue]
        yield self._subscribe_service  # skipif-ci-and-not-linux


##


@asynccontextmanager
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


__all__ = [
    "PublishService",
    "PublishServiceMixin",
    "Publisher",
    "PublisherError",
    "RedisHashMapKey",
    "RedisKey",
    "SubscribeService",
    "SubscribeServiceMixin",
    "publish",
    "redis_hash_map_key",
    "redis_key",
    "subscribe",
    "yield_pubsub",
    "yield_redis",
]
