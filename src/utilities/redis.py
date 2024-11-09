from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
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

from utilities.datetime import MILLISECOND, SECOND, duration_to_float, get_now
from utilities.iterables import always_iterable
from utilities.orjson import deserialize
from utilities.text import ensure_bytes
from utilities.types import Duration, ensure_int

if TYPE_CHECKING:
    import datetime as dt

    from redis.asyncio import ConnectionPool
    from redis.asyncio.client import PubSub
    from redis.typing import ResponseT

    from utilities.iterables import MaybeIterable


_K = TypeVar("_K")
_T = TypeVar("_T")
_V = TypeVar("_V")


class RedisMessageSubscribe(TypedDict):
    type: Literal["subscribe", "psubscribe", "message", "pmessage"]
    pattern: str | None
    channel: bytes
    data: bytes


class RedisMessageUnsubscribe(TypedDict):
    type: Literal["unsubscribe", "punsubscribe"]
    pattern: str | None
    channel: bytes
    data: int


_HOST = "localhost"
_PORT = 6379
_SUBSCRIBE_TIMEOUT = SECOND
_SUBSCRIBE_SLEEP = 10 * MILLISECOND


@dataclass(repr=False, kw_only=True, slots=True)
class TestRedis:
    """A container for a redis client; for testing purposes only."""

    redis: Redis
    timestamp: dt.datetime = field(default_factory=get_now)
    uuid: UUID = field(default_factory=uuid4)
    key: str


@dataclass(kw_only=True)
class RedisHashMapKey(Generic[_K, _V]):
    """A hashmap key in a redis store."""

    name: str
    key: type[_K]
    value: type[_V]

    async def hget(self, redis: Redis, key: _K, /) -> _V | None:
        """Get a value from a hashmap in `redis`."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        ser = serialize(key)  # skipif-ci-and-not-linux
        maybe_ser = await cast(  # skipif-ci-and-not-linux
            Awaitable[Any], redis.hget(self.name, cast(Any, ser))
        )
        if maybe_ser is None:  # skipif-ci-and-not-linux
            return None
        return deserialize(ensure_bytes(maybe_ser))  # skipif-ci-and-not-linux

    async def hset(self, redis: Redis, key: _K, value: _V, /) -> int:
        """Set a value in a hashmap in `redis`."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        ser_key = serialize(key)  # skipif-ci-and-not-linux
        ser_value = serialize(value)  # skipif-ci-and-not-linux
        response = await cast(  # skipif-ci-and-not-linux
            Awaitable[int],
            redis.hset(self.name, key=cast(Any, ser_key), value=cast(Any, ser_value)),
        )
        return ensure_int(response)  # skipif-ci-and-not-linux


@dataclass(kw_only=True)
class RedisKey(Generic[_T]):
    """A key in a redis store."""

    name: str
    type: type[_T]

    async def get(self, redis: Redis, /) -> _T | None:
        """Get a value from `redis`."""
        maybe_ser = await redis.get(self.name)  # skipif-ci-and-not-linux
        if maybe_ser is None:  # skipif-ci-and-not-linux
            return None
        return deserialize(ensure_bytes(maybe_ser))  # skipif-ci-and-not-linux

    async def set(self, redis: Redis, value: _T, /) -> int:
        """Set a value in `redis`."""
        from utilities.orjson import serialize  # skipif-ci-and-not-linux

        ser = serialize(value)  # skipif-ci-and-not-linux
        return await redis.set(self.name, ser)  # skipif-ci-and-not-linux


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
async def subscribe(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    deserializer: Callable[[bytes], _T],
    timeout: Duration | None = ...,  # noqa: ASYNC109
    sleep: Duration = ...,
) -> AsyncIterator[_T]: ...
@overload
async def subscribe(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    deserializer: None = None,
    timeout: Duration | None = ...,  # noqa: ASYNC109
    sleep: Duration = ...,
) -> AsyncIterator[bytes]: ...
async def subscribe(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    deserializer: Callable[[bytes], _T] | None = None,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,  # noqa: ASYNC109
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[_T] | AsyncIterator[bytes]:
    """Subscribe to the data of a given channel(s)."""
    channels = list(always_iterable(channels))  # skipif-ci-and-not-linux
    messages = subscribe_messages(  # skipif-ci-and-not-linux
        pubsub, channels, timeout=timeout, sleep=sleep
    )
    if deserializer is None:
        async for message in messages:
            yield message["data"]
    else:
        async for message in messages:
            yield deserializer(message["data"])


async def subscribe_messages(
    pubsub: PubSub,
    channels: MaybeIterable[str],
    /,
    *,
    timeout: Duration | None = _SUBSCRIBE_TIMEOUT,  # noqa: ASYNC109
    sleep: Duration = _SUBSCRIBE_SLEEP,
) -> AsyncIterator[RedisMessageSubscribe]:
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
            RedisMessageSubscribe | RedisMessageUnsubscribe | None,
            await pubsub.get_message(timeout=timeout_use),
        )
        if (
            (message is not None)
            and (message["type"] in {"subscribe", "psubscribe", "message", "pmessage"})
            and (message["channel"] in channels_bytes)
            and isinstance(message["data"], bytes)
        ):
            yield cast(RedisMessageSubscribe, message)
        else:
            await asyncio.sleep(sleep_use)


@asynccontextmanager
async def yield_redis(
    *,
    host: str = _HOST,
    port: int = _PORT,
    db: str | int = 0,
    password: str | None = None,
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
        connection_pool=connection_pool,
        decode_responses=decode_responses,
        **kwargs,
    )
    try:
        yield redis
    finally:
        await redis.aclose()


__all__ = [
    "RedisHashMapKey",
    "RedisKey",
    "TestRedis",
    "publish",
    "subscribe",
    "subscribe_messages",
    "yield_redis",
]
