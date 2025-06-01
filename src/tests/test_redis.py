from __future__ import annotations

from asyncio import Queue, sleep
from os import getpid
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    binary,
    booleans,
    composite,
    data,
    dictionaries,
    lists,
    sampled_from,
    uuids,
)
from pytest import LogCaptureFixture, mark, param, raises
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_operator import make_objects
from utilities.asyncio import get_items_nowait
from utilities.datetime import serialize_compact
from utilities.hypothesis import (
    int64s,
    pairs,
    settings_with_reduced_examples,
    text_ascii,
    yield_test_redis,
)
from utilities.iterables import one
from utilities.operator import is_equal
from utilities.orjson import deserialize, serialize
from utilities.redis import (
    Publisher,
    PublisherError,
    PublishError,
    PublishService,
    SubscribeService,
    _is_message,
    _RedisMessage,
    publish,
    redis_hash_map_key,
    redis_key,
    subscribe,
    yield_pubsub,
    yield_redis,
)
from utilities.sentinel import SENTINEL_REPR, Sentinel, sentinel
from utilities.tzlocal import get_now_local

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path


_PUB_SUB_SLEEP = 0.1


@composite
def channels(draw: DrawFn, /) -> str:
    now = serialize_compact(get_now_local())
    key = draw(uuids())
    pid = getpid()
    return f"test_{now}_{key}_{pid}"


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
    @given(channel=channels(), data=lists(binary(min_size=1), min_size=1, max_size=5))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_bytes(self, *, data: Sequence[bytes], channel: str) -> None:
        queue: Queue[bytes] = Queue()
        async with (
            yield_redis() as redis,
            subscribe(redis, channel, queue, output="bytes"),
        ):
            await sleep(_PUB_SUB_SLEEP)
            for datum in data:
                _ = await publish(redis, channel, datum)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(data)
        results = get_items_nowait(queue)
        for result, datum in zip(results, data, strict=True):
            assert isinstance(result, bytes)
            assert result == datum

    @given(channel=channels(), objects=lists(make_objects(), min_size=1, max_size=5))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_serializer(self, *, channel: str, objects: Sequence[Any]) -> None:
        queue: Queue[Any] = Queue()
        async with (
            yield_redis() as redis,
            subscribe(redis, channel, queue, output=deserialize),
        ):
            await sleep(_PUB_SUB_SLEEP)
            for obj in objects:
                _ = await publish(redis, channel, obj, serializer=serialize)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(objects)
        results = get_items_nowait(queue)
        for result, obj in zip(results, objects, strict=True):
            assert is_equal(result, obj)

    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=5),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_text(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[str] = Queue()
        async with yield_redis() as redis, subscribe(redis, channel, queue):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                _ = await publish(redis, channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, str)
            assert result == message

    async def test_error(self) -> None:
        async with yield_redis() as redis:
            with raises(
                PublishError, match="Unable to publish data None with serializer None"
            ):
                _ = await publish(redis, "channel", None)


class TestPublisher:
    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=5),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[str] = Queue()
        async with (
            yield_redis() as redis,
            Publisher(duration=1.0, redis=redis, sleep_core=0.1) as publisher,
            subscribe(redis, channel, queue),
        ):
            await sleep(_PUB_SUB_SLEEP)
            publisher.put_right_nowait(*((channel, m) for m in messages))
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, str)
            assert result == message

    @given(data=data())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_error(self, *, data: DataObject) -> None:
        async with yield_test_redis(data) as test:
            publisher = Publisher(redis=test.redis)
            with raises(PublisherError, match="Error running 'Publisher'"):
                raise PublisherError(publisher=publisher)

    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=5),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main_service(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[str] = Queue()
        async with (
            yield_redis() as redis,
            PublishService(freq=0.1, timeout=1.0, redis=redis) as service,
            subscribe(redis, channel, queue),
        ):
            await sleep(_PUB_SUB_SLEEP)
            service.put_right_nowait(*((channel, m) for m in messages))
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, str)
            assert result == message


class TestRedisHashMapKey:
    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_bool(
        self, *, data: DataObject, key: int, value: bool
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.get(test.redis, key) is value

    @given(data=data(), key=booleans() | int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_union_key(
        self, *, data: DataObject, key: bool | int, value: bool
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, (bool, int), bool)
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.get(test.redis, key) is value

    @given(data=data(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_sentinel_key(
        self, *, data: DataObject, value: bool
    ) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(
                test.key, Sentinel, bool, key_serializer=serializer
            )
            _ = await hm_key.set(test.redis, sentinel, value)
            assert await hm_key.get(test.redis, sentinel) is value

    @given(data=data(), key=int64s(), value=int64s() | booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_union_value(
        self, *, data: DataObject, key: int, value: bool | int
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, (bool, int))
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.get(test.redis, key) == value

    @given(data=data(), key=int64s())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_sentinel_value(
        self, *, data: DataObject, key: int
    ) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        def deserializer(data: bytes, /) -> Sentinel:
            assert data == SENTINEL_REPR.encode()
            return sentinel

        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(
                test.key,
                int,
                Sentinel,
                value_serializer=serializer,
                value_deserializer=deserializer,
            )
            _ = await hm_key.set(test.redis, key, sentinel)
            assert await hm_key.get(test.redis, key) is sentinel

    @given(data=data(), mapping=dictionaries(int64s(), booleans()))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_many(
        self, *, data: DataObject, mapping: Mapping[int, bool]
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set_many(test.redis, mapping)
            if len(mapping) == 0:
                keys = []
            else:
                keys = data.draw(lists(sampled_from(list(mapping))))
            expected = [mapping[k] for k in keys]
            assert await hm_key.get_many(test.redis, keys) == expected

    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_delete(self, *, data: DataObject, key: int, value: bool) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.get(test.redis, key) is value
            _ = await hm_key.delete(test.redis, key)
            with raises(KeyError):
                _ = await hm_key.get(test.redis, key)

    @given(data=data(), key=pairs(int64s()), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_delete_compound(
        self, *, data: DataObject, key: tuple[int, int], value: bool
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, tuple[int, int], bool)
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.get(test.redis, key) is value
            _ = await hm_key.delete(test.redis, key)
            with raises(KeyError):
                _ = await hm_key.get(test.redis, key)

    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_exists(self, *, data: DataObject, key: int, value: bool) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            assert not (await hm_key.exists(test.redis, key))
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.exists(test.redis, key)

    @given(data=data(), key=pairs(int64s()), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_exists_compound(
        self, *, data: DataObject, key: tuple[int, int], value: bool
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, tuple[int, int], bool)
            assert not (await hm_key.exists(test.redis, key))
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.exists(test.redis, key)

    @given(data=data(), mapping=dictionaries(int64s(), booleans()))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_all(
        self, *, data: DataObject, mapping: Mapping[int, bool]
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set_many(test.redis, mapping)
            assert await hm_key.get_all(test.redis) == mapping

    @given(data=data(), mapping=dictionaries(int64s(), booleans()))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_keys(self, *, data: DataObject, mapping: Mapping[int, bool]) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set_many(test.redis, mapping)
            assert await hm_key.keys(test.redis) == list(mapping)

    @given(data=data(), mapping=dictionaries(int64s(), booleans()))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_length(
        self, *, data: DataObject, mapping: Mapping[int, bool]
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set_many(test.redis, mapping)
            assert await hm_key.length(test.redis) == len(mapping)

    @given(data=data(), key=int64s(), value=booleans())
    @settings(max_examples=1, phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_ttl(self, *, data: DataObject, key: int, value: bool) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool, ttl=0.05)
            _ = await hm_key.set(test.redis, key, value)
            await sleep(0.025)  # else next line may not work
            assert await hm_key.exists(test.redis, key)
            await sleep(0.05)
            assert not await test.redis.exists(hm_key.name)

    @given(data=data(), mapping=dictionaries(int64s(), booleans()))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_values(
        self, *, data: DataObject, mapping: Mapping[int, bool]
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set_many(test.redis, mapping)
            assert await hm_key.values(test.redis) == list(mapping.values())


class TestRedisKey:
    @given(data=data(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_bool(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(test.key, bool)
            _ = await key.set(test.redis, value)
            assert await key.get(test.redis) is value

    @given(data=data(), value=booleans() | int64s())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_union(
        self, *, data: DataObject, value: bool | int
    ) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(test.key, (bool, int))
            _ = await key.set(test.redis, value)
            assert await key.get(test.redis) == value

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_sentinel_with_serialize(
        self, *, data: DataObject
    ) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        def deserializer(data: bytes, /) -> Sentinel:
            assert data == SENTINEL_REPR.encode()
            return sentinel

        async with yield_test_redis(data) as test:
            key = redis_key(
                test.key, Sentinel, serializer=serializer, deserializer=deserializer
            )
            _ = await key.set(test.redis, sentinel)
            assert await key.get(test.redis) is sentinel

    @given(data=data(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_delete(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(test.key, bool)
            _ = await key.set(test.redis, value)
            assert await key.get(test.redis) is value
            _ = await key.delete(test.redis)
            with raises(KeyError):
                _ = await key.get(test.redis)

    @given(data=data(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_exists(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(test.key, bool)
            assert not (await key.exists(test.redis))
            _ = await key.set(test.redis, value)
            assert await key.exists(test.redis)

    @given(data=data(), value=booleans())
    @settings(max_examples=1, phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_ttl(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(test.key, bool, ttl=0.05)
            _ = await key.set(test.redis, value)
            await sleep(0.025)  # else next line may not work
            assert await key.exists(test.redis)
            await sleep(0.05)
            assert not await key.exists(test.redis)


class TestSubscribe:
    @given(
        channel=channels(), messages=lists(binary(min_size=1), min_size=1, max_size=5)
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_bytes(self, *, channel: str, messages: Sequence[bytes]) -> None:
        queue: Queue[bytes] = Queue()
        async with (
            yield_redis() as redis,
            subscribe(redis, channel, queue, output="bytes"),
        ):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                await redis.publish(channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, bytes)
            assert result == message

    @given(channel=channels(), objs=lists(make_objects(), min_size=1, max_size=5))
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_deserialize(self, *, channel: str, objs: Sequence[Any]) -> None:
        queue: Queue[Any] = Queue()
        async with (
            yield_redis() as redis,
            subscribe(redis, channel, queue, output=deserialize),
        ):
            await sleep(_PUB_SUB_SLEEP)
            for obj in objs:
                await redis.publish(channel, serialize(obj))
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(objs)
        results = get_items_nowait(queue)
        for result, obj in zip(results, objs, strict=True):
            assert is_equal(result, obj)

    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=5),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_raw(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[_RedisMessage] = Queue()
        async with (
            yield_redis() as redis,
            subscribe(redis, channel, queue, output="raw"),
        ):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                await redis.publish(channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, dict)
            assert result["type"] == "message"
            assert result["pattern"] is None
            assert result["channel"] == channel.encode()
            assert result["data"] == message.encode()

    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=5),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_text(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[_RedisMessage] = Queue()
        async with (
            yield_redis() as redis,
            subscribe(redis, channel, queue, output="raw"),
        ):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                await redis.publish(channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, dict)
            assert result["type"] == "message"
            assert result["pattern"] is None
            assert result["channel"] == channel.encode()
            assert result["data"] == message.encode()


class TestSubscribeService:
    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=5),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, channel: str, messages: list[str]) -> None:
        async with (
            yield_redis() as redis,
            SubscribeService(timeout=1.0, redis=redis, channel=channel) as service,
        ):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                await redis.publish(channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert service.qsize() == len(messages)
        results = service.get_all_nowait()
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, str)
            assert result == message

    @given(channel=channels())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_context_manager_already_subscribing(
        self, *, channel: str, caplog: LogCaptureFixture
    ) -> None:
        async with yield_redis() as redis:
            looper = SubscribeService(
                timeout=1.0, _debug=True, redis=redis, channel=channel
            )
            async with looper, looper:
                ...
            _ = one(m for m in caplog.messages if search(": already subscribing$", m))
            _ = one(
                m
                for m in caplog.messages
                if search(": already stopped subscription$", m)
            )


class TestYieldClient:
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self) -> None:
        async with yield_redis() as client:
            assert isinstance(client, Redis)


class TestYieldPubSub:
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, tmp_path: Path) -> None:
        channel = str(tmp_path)
        async with yield_redis() as redis, yield_pubsub(redis, channel) as pubsub:
            assert isinstance(pubsub, PubSub)
