from __future__ import annotations

from asyncio import Queue
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import (
    DataObject,
    binary,
    booleans,
    data,
    dictionaries,
    lists,
    permutations,
    sampled_from,
)
from pytest import mark, param, raises
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_objects.objects import objects
from utilities.asyncio import get_items_nowait, sleep_td
from utilities.hypothesis import int64s, pairs, text_ascii
from utilities.operator import is_equal
from utilities.orjson import deserialize, serialize
from utilities.redis import (
    PublishError,
    _is_message,
    _RedisMessage,
    publish,
    publish_many,
    redis_hash_map_key,
    redis_key,
    subscribe,
    yield_pubsub,
    yield_redis,
)
from utilities.sentinel import SENTINEL_REPR, Sentinel, sentinel
from utilities.text import unique_str
from utilities.whenever import MICROSECOND, SECOND

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from whenever import TimeDelta


_PUB_SUB_SLEEP: TimeDelta = 0.1 * SECOND


class TestIsMessage:
    @mark.parametrize(
        ("message", "channels", "expected"),
        [
            param(
                {
                    "type": "message",
                    "pattern": None,
                    "channel": b"channel",
                    "data": b"data",
                },
                [b"channel"],
                True,
            ),
            param(None, [], False),
            param({"type": "invalid"}, [], False),
            param({"type": "message"}, [], False),
            param({"type": "message", "pattern": False}, [], False),
            param({"type": "message", "pattern": None}, [], False),
            param(
                {"type": "message", "pattern": None, "channel": b"channel1"},
                [b"channel2"],
                False,
            ),
            param(
                {"type": "message", "pattern": None, "channel": b"channel"},
                [b"channel"],
                False,
            ),
            param(
                {
                    "type": "message",
                    "pattern": None,
                    "channel": b"channel",
                    "data": None,
                },
                [b"channel"],
                False,
            ),
        ],
    )
    def test_main(
        self, *, message: Any, channels: Sequence[bytes], expected: bool
    ) -> None:
        result = _is_message(message, channels=channels)
        assert result is expected


class TestPublish:
    @given(data=lists(binary(min_size=1), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_bytes(self, *, test_redis: Redis, data: Sequence[bytes]) -> None:
        channel = unique_str()
        queue: Queue[bytes] = Queue()
        async with subscribe(test_redis, channel, queue, output="bytes"):
            await sleep_td(_PUB_SUB_SLEEP)
            for datum in data:
                _ = await publish(test_redis, channel, datum)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(data)
        results = get_items_nowait(queue)
        for result, datum in zip(results, data, strict=True):
            assert isinstance(result, bytes)
            assert result == datum

    @given(objects=lists(objects(), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_serializer(
        self, *, test_redis: Redis, objects: Sequence[Any]
    ) -> None:
        channel = unique_str()
        queue: Queue[Any] = Queue()
        async with subscribe(test_redis, channel, queue, output=deserialize):
            await sleep_td(_PUB_SUB_SLEEP)
            for obj in objects:
                _ = await publish(test_redis, channel, obj, serializer=serialize)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(objects)
        results = get_items_nowait(queue)
        for result, obj in zip(results, objects, strict=True):
            assert is_equal(result, obj)

    @given(messages=lists(text_ascii(min_size=1), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_text(self, *, test_redis: Redis, messages: Sequence[str]) -> None:
        channel = f"test_{unique_str()}"
        queue: Queue[str] = Queue()
        async with subscribe(test_redis, channel, queue):
            await sleep_td(_PUB_SUB_SLEEP)
            for message in messages:
                _ = await publish(test_redis, channel, message)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, str)
            assert result == message

    async def test_error(self, *, test_redis: Redis) -> None:
        with raises(
            PublishError, match="Unable to publish data None with no serializer"
        ):
            _ = await publish(test_redis, "channel", None)


class TestPublishMany:
    @given(
        data=lists(binary(min_size=1) | text_ascii(min_size=1) | objects(), min_size=1)
    )
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(self, *, test_redis: Redis, data: Sequence[Any]) -> None:
        result = await publish_many(
            test_redis, unique_str(), data, serializer=serialize
        )
        expected = list(repeat(object=True, times=len(data)))
        assert result == expected

    @given(messages=lists(text_ascii(min_size=1), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_timeout(self, *, test_redis: Redis, messages: Sequence[str]) -> None:
        result = await publish_many(
            test_redis, unique_str(), messages, timeout=MICROSECOND
        )
        expected = list(repeat(object=False, times=len(messages)))
        assert result == expected


class TestRedisHashMapKey:
    @given(key=int64s(), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_bool(
        self, *, test_redis: Redis, key: int, value: bool
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.get(test_redis, key) is value

    @given(key=booleans() | int64s(), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_union_key(
        self, *, test_redis: Redis, key: bool | int, value: bool
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), (bool, int), bool)
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.get(test_redis, key) is value

    @given(value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_sentinel_key(
        self, *, test_redis: Redis, value: bool
    ) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        hm_key = redis_hash_map_key(
            unique_str(), Sentinel, bool, key_serializer=serializer
        )
        _ = await hm_key.set(test_redis, sentinel, value)
        assert await hm_key.get(test_redis, sentinel) is value

    @given(key=int64s(), value=int64s() | booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_union_value(
        self, *, test_redis: Redis, key: int, value: bool | int
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, (bool, int))
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.get(test_redis, key) == value

    @given(key=int64s())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_sentinel_value(
        self, *, test_redis: Redis, key: int
    ) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        def deserializer(data: bytes, /) -> Sentinel:
            assert data == SENTINEL_REPR.encode()
            return sentinel

        hm_key = redis_hash_map_key(
            unique_str(),
            int,
            Sentinel,
            value_serializer=serializer,
            value_deserializer=deserializer,
        )
        _ = await hm_key.set(test_redis, key, sentinel)
        assert await hm_key.get(test_redis, key) is sentinel

    @given(data=data(), mapping=dictionaries(int64s(), booleans()))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_many(
        self, *, test_redis: Redis, data: DataObject, mapping: Mapping[int, bool]
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set_many(test_redis, mapping)
        if len(mapping) == 0:
            keys = []
        else:
            keys = data.draw(lists(sampled_from(list(mapping))))
        expected = [mapping[k] for k in keys]
        assert await hm_key.get_many(test_redis, keys) == expected

    @given(key=int64s(), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_delete(self, *, test_redis: Redis, key: int, value: bool) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.get(test_redis, key) is value
        _ = await hm_key.delete(test_redis, key)
        with raises(KeyError):
            _ = await hm_key.get(test_redis, key)

    @given(key=pairs(int64s()), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_delete_compound(
        self, *, test_redis: Redis, key: tuple[int, int], value: bool
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), tuple[int, int], bool)
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.get(test_redis, key) is value
        _ = await hm_key.delete(test_redis, key)
        with raises(KeyError):
            _ = await hm_key.get(test_redis, key)

    @given(key=int64s(), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_exists(self, *, test_redis: Redis, key: int, value: bool) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        assert not (await hm_key.exists(test_redis, key))
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.exists(test_redis, key)

    @given(key=pairs(int64s()), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_exists_compound(
        self, *, test_redis: Redis, key: tuple[int, int], value: bool
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), tuple[int, int], bool)
        assert not (await hm_key.exists(test_redis, key))
        _ = await hm_key.set(test_redis, key, value)
        assert await hm_key.exists(test_redis, key)

    @given(mapping=dictionaries(int64s(), booleans()))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_all(
        self, *, test_redis: Redis, mapping: Mapping[int, bool]
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set_many(test_redis, mapping)
        assert await hm_key.get_all(test_redis) == mapping

    @given(mapping=dictionaries(int64s(), booleans()))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_keys(
        self, *, test_redis: Redis, mapping: Mapping[int, bool]
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set_many(test_redis, mapping)
        assert await hm_key.keys(test_redis) == list(mapping)

    @given(mapping=dictionaries(int64s(), booleans()))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_length(
        self, *, test_redis: Redis, mapping: Mapping[int, bool]
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set_many(test_redis, mapping)
        assert await hm_key.length(test_redis) == len(mapping)

    @given(key=int64s(), value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_ttl(self, *, test_redis: Redis, key: int, value: bool) -> None:
        delta = 0.1 * SECOND
        hm_key = redis_hash_map_key(unique_str(), int, bool, ttl=2 * delta)
        _ = await hm_key.set(test_redis, key, value)
        await sleep_td(delta)  # else next line may not work
        assert await hm_key.exists(test_redis, key)
        await sleep_td(2 * delta)
        assert not await test_redis.exists(hm_key.name)

    @given(mapping=dictionaries(int64s(), booleans()))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_values(
        self, *, test_redis: Redis, mapping: Mapping[int, bool]
    ) -> None:
        hm_key = redis_hash_map_key(unique_str(), int, bool)
        _ = await hm_key.set_many(test_redis, mapping)
        assert await hm_key.values(test_redis) == list(mapping.values())


class TestRedisKey:
    @given(value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_bool(self, *, test_redis: Redis, value: bool) -> None:
        key = redis_key(unique_str(), bool)
        _ = await key.set(test_redis, value)
        assert await key.get(test_redis) is value

    @given(value=booleans() | int64s())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_get_and_set_union(
        self, *, test_redis: Redis, value: bool | int
    ) -> None:
        key = redis_key(unique_str(), (bool, int))
        _ = await key.set(test_redis, value)
        assert await key.get(test_redis) == value

    @mark.flaky
    async def test_get_and_set_sentinel_with_serialize(
        self, *, test_redis: Redis
    ) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        def deserializer(data: bytes, /) -> Sentinel:
            assert data == SENTINEL_REPR.encode()
            return sentinel

        red_key = redis_key(
            unique_str(), Sentinel, serializer=serializer, deserializer=deserializer
        )
        _ = await red_key.set(test_redis, sentinel)
        assert await red_key.get(test_redis) is sentinel

    @given(value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_delete(self, *, test_redis: Redis, value: bool) -> None:
        key = redis_key(unique_str(), bool)
        _ = await key.set(test_redis, value)
        assert await key.get(test_redis) is value
        _ = await key.delete(test_redis)
        with raises(KeyError):
            _ = await key.get(test_redis)

    @given(value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_exists(self, *, test_redis: Redis, value: bool) -> None:
        key = redis_key(unique_str(), bool)
        assert not (await key.exists(test_redis))
        _ = await key.set(test_redis, value)
        assert await key.exists(test_redis)

    @given(value=booleans())
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_ttl(self, *, test_redis: Redis, value: bool) -> None:
        delta = 0.1 * SECOND
        key = redis_key(unique_str(), bool, ttl=2 * delta)
        _ = await key.set(test_redis, value)
        await sleep_td(delta)  # else next line may not work
        assert await key.exists(test_redis)
        await sleep_td(2 * delta)
        assert not await key.exists(test_redis)


class TestSubscribe:
    @given(messages=lists(binary(min_size=1), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_bytes(self, *, test_redis: Redis, messages: Sequence[bytes]) -> None:
        channel = unique_str()
        queue: Queue[bytes] = Queue()
        async with subscribe(test_redis, channel, queue, output="bytes"):
            await sleep_td(_PUB_SUB_SLEEP)
            for message in messages:
                await test_redis.publish(channel, message)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, bytes)
            assert result == message

    @given(objs=lists(objects(), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_deserialize(self, *, test_redis: Redis, objs: Sequence[Any]) -> None:
        channel = unique_str()
        queue: Queue[Any] = Queue()
        async with subscribe(test_redis, channel, queue, output=deserialize):
            await sleep_td(_PUB_SUB_SLEEP)
            for obj in objs:
                await test_redis.publish(channel, serialize(obj))
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(objs)
        results = get_items_nowait(queue)
        for result, obj in zip(results, objs, strict=True):
            assert is_equal(result, obj)

    @given(
        data=data(),
        short_messages=lists(text_ascii(max_size=4), min_size=1),
        long_messages=lists(text_ascii(min_size=6), min_size=1),
    )
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_filter(
        self,
        *,
        test_redis: Redis,
        data: DataObject,
        short_messages: Sequence[str],
        long_messages: Sequence[str],
    ) -> None:
        channel = unique_str()
        messages = data.draw(permutations(list(chain(short_messages, long_messages))))
        queue: Queue[str] = Queue()
        async with subscribe(
            test_redis, channel, queue, filter_=lambda text: len(text) >= 6
        ):
            await sleep_td(_PUB_SUB_SLEEP)
            for message in messages:
                await test_redis.publish(channel, message)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(long_messages)
        results = get_items_nowait(queue)
        for result in results:
            assert isinstance(result, str)
            assert len(result) >= 3

    @given(messages=lists(text_ascii(min_size=1), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_raw(self, *, test_redis: Redis, messages: Sequence[str]) -> None:
        channel = f"test_{unique_str()}"
        queue: Queue[_RedisMessage] = Queue()
        async with subscribe(test_redis, channel, queue, output="raw"):
            await sleep_td(_PUB_SUB_SLEEP)
            for message in messages:
                await test_redis.publish(channel, message)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, dict)
            assert result["type"] == "message"
            assert result["pattern"] is None
            assert result["channel"] == channel.encode()
            assert result["data"] == message.encode()

    @given(messages=lists(text_ascii(min_size=1), min_size=1))
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_text(self, *, test_redis: Redis, messages: Sequence[str]) -> None:
        channel = f"test_{unique_str()}"
        queue: Queue[_RedisMessage] = Queue()
        async with subscribe(test_redis, channel, queue, output="raw"):
            await sleep_td(_PUB_SUB_SLEEP)
            for message in messages:
                await test_redis.publish(channel, message)
            await sleep_td(_PUB_SUB_SLEEP)  # remain in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, dict)
            assert result["type"] == "message"
            assert result["pattern"] is None
            assert result["channel"] == channel.encode()
            assert result["data"] == message.encode()


class TestYieldRedis:
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self) -> None:
        async with yield_redis() as client:
            assert isinstance(client, Redis)


class TestYieldPubSub:
    async def test_main(self, *, test_redis: Redis) -> None:
        async with yield_pubsub(test_redis, unique_str()) as pubsub:
            assert isinstance(pubsub, PubSub)
