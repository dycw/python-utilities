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
from utilities.iterables import always_iterable
from utilities.tenacity import MaybeAttemptContextManager, yield_timeout_attempts

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import ResponseT
    from tenacity.retry import RetryBaseT
    from tenacity.stop import StopBaseT
    from tenacity.wait import WaitBaseT

    from utilities.iterables import MaybeIterable
    from utilities.types import Duration


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
    key: type[_K]
    key_serializer: Callable[[_K], bytes] | None = None
    value: type[_V]
    value_serializer: Callable[[_V], bytes] | None = None
    value_deserializer: Callable[[bytes], _V] | None = None
    stop: StopBaseT | None = None
    wait: WaitBaseT | None = None
    retry: RetryBaseT | None = None
    timeout: Duration | None = None
    ttl: Duration | None = None

    async def delete(self, redis: Redis, key: _K, /) -> int:
        """Delete a key from a hashmap in `redis`."""
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await cast(
                    "Awaitable[int]", redis.hdel(self.name, cast("str", key))
                )
        raise ImpossibleCaseError(case=[f"{redis=}", f"{key=}"])  # pragma: no cover

    async def exists(self, redis: Redis, key: _K, /) -> bool:
        """Check if the key exists in a hashmap in `redis`."""
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await cast(
                    "Awaitable[bool]", redis.hexists(self.name, cast("str", key))
                )
        raise ImpossibleCaseError(case=[f"{redis=}", f"{key=}"])  # pragma: no cover

    async def get(self, redis: Redis, key: _K, /) -> _V | None:
        """Get a value from a hashmap in `redis`."""
        ser_key = self._serialize_key(key)  # skipif-ci-and-not-linux
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await self._get_core(redis, cast("Any", ser_key))
        raise ImpossibleCaseError(case=[f"{redis=}", f"{key=}"])  # pragma: no cover

    async def _get_core(self, redis: Redis, ser_key: bytes, /) -> _V | None:
        result = await cast(  # skipif-ci-and-not-linux
            "Awaitable[Any]", redis.hget(self.name, cast("Any", ser_key))
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
                raise ImpossibleCaseError(case=[f"{result=}"])

    async def set(self, redis: Redis, key: _K, value: _V, /) -> int:
        """Set a value in a hashmap in `redis`."""
        ser_key = self._serialize_key(key)  # skipif-ci-and-not-linux
        if self.value_serializer is None:  # skipif-ci-and-not-linux
            from utilities.orjson import serialize

            ser_value = serialize(value)
        else:  # skipif-ci-and-not-linux
            ser_value = self.value_serializer(value)

        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await self._set_core(redis, ser_key, ser_value)
        raise ImpossibleCaseError(case=[f"{self=}"])  # pragma: no cover

    async def _set_core(self, redis: Redis, ser_key: bytes, ser_value: bytes, /) -> int:
        result = await cast(  # skipif-ci-and-not-linux
            "Awaitable[int]",
            redis.hset(
                self.name, key=cast("Any", ser_key), value=cast("Any", ser_value)
            ),
        )
        if self.ttl is not None:  # skipif-ci-and-not-linux
            await redis.pexpire(self.name, datetime_duration_to_timedelta(self.ttl))
        return result  # skipif-ci-and-not-linux

    def _serialize_key(self, key: _K, /) -> bytes:
        """Serialize the key."""
        if self.key_serializer is None:  # skipif-ci-and-not-linux
            from utilities.orjson import serialize

            return serialize(key)
        return self.key_serializer(key)  # skipif-ci-and-not-linux

    def _yield_timeout_attempts(self) -> AsyncIterator[MaybeAttemptContextManager]:
        return yield_timeout_attempts(  # skipif-ci-and-not-linux
            stop=self.stop, wait=self.wait, retry=self.retry, timeout=self.timeout
        )


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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K, _V]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K, _V1 | _V2]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K, _V1 | _V2 | _V3]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K1 | _K2, _V]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K1 | _K2, _V1 | _V2]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K1 | _K2, _V1 | _V2 | _V3]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K1 | _K2 | _K3, _V]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K1 | _K2 | _K3, _V1 | _V2]: ...
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
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisHashMapKey[_K1 | _K2 | _K3, _V1 | _V2 | _V3]: ...
def redis_hash_map_key(
    name: str,
    key: Any,
    value: Any,
    /,
    *,
    key_serializer: Callable[[Any], bytes] | None = None,
    value_serializer: Callable[[Any], bytes] | None = None,
    value_deserializer: Callable[[bytes], Any] | None = None,
    stop: StopBaseT | None = None,
    wait: WaitBaseT | None = None,
    retry: RetryBaseT | None = None,
    timeout: Duration | None = None,
    ttl: Duration | None = None,
) -> RedisHashMapKey[Any, Any]:
    """Create a redis key."""
    return RedisHashMapKey(  # skipif-ci-and-not-linux
        name=name,
        key=key,
        key_serializer=key_serializer,
        value=value,
        value_serializer=value_serializer,
        value_deserializer=value_deserializer,
        stop=stop,
        wait=wait,
        retry=retry,
        timeout=timeout,
        ttl=ttl,
    )


##


@dataclass(kw_only=True)
class RedisKey(Generic[_T]):
    """A key in a redis store."""

    name: str
    type: type[_T]
    serializer: Callable[[_T], bytes] | None = None
    deserializer: Callable[[bytes], _T] | None = None
    stop: StopBaseT | None = None
    wait: WaitBaseT | None = None
    retry: RetryBaseT | None = None
    timeout: Duration | None = None
    ttl: Duration | None = None

    async def delete(self, redis: Redis, /) -> int:
        """Delete the key from `redis`."""
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return ensure_int(await redis.delete(self.name))
        raise ImpossibleCaseError(case=[f"{redis=}"])  # pragma: no cover

    async def exists(self, redis: Redis, /) -> bool:
        """Check if the key exists in `redis`."""
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await self._exists_core(redis)
        raise ImpossibleCaseError(case=[f"{redis=}"])  # pragma: no cover

    async def _exists_core(self, redis: Redis, /) -> bool:
        result = await redis.exists(self.name)  # skipif-ci-and-not-linux
        match ensure_int(result):  # skipif-ci-and-not-linux
            case 0 | 1 as value:
                return bool(value)
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{redis=}"])

    async def get(self, redis: Redis, /) -> _T | None:
        """Get a value from `redis`."""
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await self._get_core(redis)
        raise ImpossibleCaseError(case=[f"{redis=}"])  # pragma: no cover

    async def _get_core(self, redis: Redis, /) -> _T | None:
        result = await redis.get(self.name)  # skipif-ci-and-not-linux
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

    async def set(self, redis: Redis, value: _T, /) -> int:
        """Set a value in `redis`."""
        if self.serializer is None:  # skipif-ci-and-not-linux
            from utilities.orjson import serialize

            ser_value = serialize(value)
        else:  # skipif-ci-and-not-linux
            ser_value = self.serializer(value)
        ttl = (  # skipif-ci-and-not-linux
            None
            if self.ttl is None
            else round(1000 * datetime_duration_to_float(self.ttl))
        )
        async for attempt in self._yield_timeout_attempts():  # skipif-ci-and-not-linux
            async with attempt:
                return await self._set_core(redis, ser_value, ttl=ttl)
        raise ImpossibleCaseError(case=[f"{redis=}", f"{value=}"])  # pragma: no cover

    async def _set_core(
        self, redis: Redis, ser_value: bytes, /, *, ttl: int | None = None
    ) -> int:
        result = await redis.set(  # skipif-ci-and-not-linux
            self.name, ser_value, px=ttl
        )
        return ensure_int(result)  # skipif-ci-and-not-linux

    def _yield_timeout_attempts(self) -> AsyncIterator[MaybeAttemptContextManager]:
        return yield_timeout_attempts(  # skipif-ci-and-not-linux
            stop=self.stop, wait=self.wait, retry=self.retry, timeout=self.timeout
        )


@overload
def redis_key(
    name: str,
    type_: type[_T],
    /,
    *,
    serializer: Callable[[_T], bytes] | None = ...,
    deserializer: Callable[[bytes], _T] | None = ...,
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisKey[_T]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2]],
    /,
    *,
    serializer: Callable[[_T1 | _T2], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2] | None = None,
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisKey[_T1 | _T2]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3] | None = None,
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisKey[_T1 | _T2 | _T3]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3], type[_T4]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4] | None = None,
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisKey[_T1 | _T2 | _T3 | _T4]: ...
@overload
def redis_key(
    name: str,
    type_: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]],
    /,
    *,
    serializer: Callable[[_T1 | _T2 | _T3 | _T4 | _T5], bytes] | None = None,
    deserializer: Callable[[bytes], _T1 | _T2 | _T3 | _T4 | _T5] | None = None,
    stop: StopBaseT | None = ...,
    wait: WaitBaseT | None = ...,
    retry: RetryBaseT | None = ...,
    timeout: Duration | None = ...,
    ttl: Duration | None = ...,
) -> RedisKey[_T1 | _T2 | _T3 | _T4 | _T5]: ...
def redis_key(
    name: str,
    type_: Any,
    /,
    *,
    serializer: Callable[[Any], bytes] | None = None,
    deserializer: Callable[[bytes], Any] | None = None,
    stop: StopBaseT | None = None,
    wait: WaitBaseT | None = None,
    retry: RetryBaseT | None = None,
    timeout: Duration | None = None,
    ttl: Duration | None = None,
) -> RedisKey[Any]:
    """Create a redis key."""
    return RedisKey(  # skipif-ci-and-not-linux
        name=name,
        type=type_,
        serializer=serializer,
        deserializer=deserializer,
        stop=stop,
        wait=wait,
        retry=retry,
        timeout=timeout,
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
