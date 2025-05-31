from __future__ import annotations

from asyncio import Queue, create_task, sleep
from io import BytesIO, StringIO
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    booleans,
    composite,
    data,
    dictionaries,
    lists,
    sampled_from,
    uuids,
)
from pytest import mark, raises
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_operator import make_objects
from utilities.asyncio import EnhancedTaskGroup, get_items_nowait
from utilities.datetime import serialize_compact
from utilities.functions import get_class_name
from utilities.hypothesis import (
    int64s,
    pairs,
    settings_with_reduced_examples,
    text_ascii,
    yield_test_redis,
)
from utilities.operator import is_equal
from utilities.orjson import deserialize, serialize
from utilities.redis import (
    Publisher,
    PublisherError,
    PublishService,
    SubscribeService,
    _RedisMessageSubscribe,
    publish,
    redis_hash_map_key,
    redis_key,
    subscribe,
    yield_message_queue,
    yield_pubsub,
    yield_redis,
)
from utilities.sentinel import SENTINEL_REPR, Sentinel, sentinel
from utilities.tzlocal import get_now_local

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from pytest import CaptureFixture


_PUB_SUB_SLEEP = 0.1


@composite
def channels(draw: DrawFn, /) -> str:
    now = serialize_compact(get_now_local())
    key = draw(uuids())
    return f"test_{now}_{key}"


@mark.skip
class TestPublishAndSubscribe:
    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_all_objects_with_serialize_{c}"
        ),
        obj=make_objects(),
    )
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_all_objects_with_serialize(
        self, *, capsys: CaptureFixture, data: DataObject, channel: str, obj: Any
    ) -> None:
        async with yield_test_redis(data) as test:

            async def listener() -> None:
                async for msg in subscribe(
                    test.redis.pubsub(), channel, deserializer=deserialize
                ):
                    print(msg)  # noqa: T201

            task = create_task(listener())
            await sleep(0.05)
            _ = await publish(test.redis, channel, obj, serializer=serialize)
            await sleep(0.05)
            try:
                out = capsys.readouterr().out
                expected = f"{obj}\n"
                assert out == expected
            finally:
                _ = task.cancel()

    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_text_without_serialize_{c}"
        ),
        text=text_ascii(min_size=1),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_text_without_serialize(
        self, *, capsys: CaptureFixture, data: DataObject, channel: str, text: str
    ) -> None:
        async with yield_test_redis(data) as test:

            async def listener() -> None:
                async for msg in subscribe(test.redis.pubsub(), channel):
                    print(msg)  # noqa: T201

            task = create_task(listener())
            await sleep(0.05)
            _ = await publish(test.redis, channel, text)
            await sleep(0.05)
            try:
                out = capsys.readouterr().out
                expected = f"{text.encode()}\n"
                assert out == expected
            finally:
                _ = task.cancel()


class TestPublisher:
    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublisher)}_main_{c}"
        ),
        obj=make_objects(),
    )
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, data: DataObject, channel: str, obj: Any) -> None:
        buffer = StringIO()
        async with (
            yield_test_redis(data) as test,
            Publisher(
                duration=1.0, redis=test.redis, serializer=serialize, sleep_core=0.1
            ) as publisher,
        ):

            async def listener() -> None:
                async for obj_i in subscribe(
                    test.redis.pubsub(), channel, deserializer=deserialize
                ):
                    _ = buffer.write(str(obj_i))

            async def sleep_then_put() -> None:
                await sleep(0.1)
                publisher.put_right_nowait((channel, obj))

            with raises(ExceptionGroup):  # noqa: PT012
                async with EnhancedTaskGroup(timeout=1.0) as tg:
                    _ = tg.create_task(listener())
                    _ = tg.create_task(sleep_then_put())

        assert buffer.getvalue() == str(obj)

    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublisher)}_text_without_serialize_{c}"
        ),
        text=text_ascii(min_size=1),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_text_without_serialize(
        self, *, data: DataObject, channel: str, text: str
    ) -> None:
        buffer = BytesIO()
        async with (
            yield_test_redis(data) as test,
            Publisher(duration=1.0, redis=test.redis, sleep_core=0.1) as publisher,
        ):

            async def listener() -> None:
                async for bytes_i in subscribe(test.redis.pubsub(), channel):
                    _ = buffer.write(bytes_i)

            async def sleep_then_put() -> None:
                await sleep(0.1)
                publisher.put_right_nowait((channel, text))

            with raises(ExceptionGroup):  # noqa: PT012
                async with EnhancedTaskGroup(timeout=1.0) as tg:
                    _ = tg.create_task(listener())
                    _ = tg.create_task(sleep_then_put())

        assert buffer.getvalue() == text.encode()

    @given(data=data())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_error(self, *, data: DataObject) -> None:
        async with yield_test_redis(data) as test:
            publisher = Publisher(redis=test.redis)
            with raises(PublisherError, match="Error running 'Publisher'"):
                raise PublisherError(publisher=publisher)

    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublisher)}_main_{c}_service"
        ),
        obj=make_objects(),
    )
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main_service(
        self, *, data: DataObject, channel: str, obj: Any
    ) -> None:
        buffer = StringIO()
        async with (
            yield_test_redis(data) as test,
            PublishService(
                freq=0.1, timeout=1.0, redis=test.redis, serializer=serialize
            ) as service,
        ):

            async def listener() -> None:
                async for obj_i in subscribe(
                    test.redis.pubsub(), channel, deserializer=deserialize
                ):
                    _ = buffer.write(str(obj_i))

            async def sleep_then_put() -> None:
                await sleep(0.1)
                service.put_right_nowait((channel, obj))

            with raises(ExceptionGroup):  # noqa: PT012
                async with EnhancedTaskGroup(timeout=1.0) as tg:
                    _ = tg.create_task(listener())
                    _ = tg.create_task(sleep_then_put())

        assert buffer.getvalue() == str(obj)

    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublisher)}_text_without_serialize_{c}_service"
        ),
        text=text_ascii(min_size=1),
    )
    @mark.flaky
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_text_without_serialize_service(
        self, *, data: DataObject, channel: str, text: str
    ) -> None:
        buffer = BytesIO()
        async with (
            yield_test_redis(data) as test,
            PublishService(freq=0.1, timeout=1.0, redis=test.redis) as service,
        ):

            async def listener() -> None:
                async for bytes_i in subscribe(test.redis.pubsub(), channel):
                    _ = buffer.write(bytes_i)

            async def sleep_then_put() -> None:
                await sleep(0.1)
                service.put_right_nowait((channel, text))

            with raises(ExceptionGroup):  # noqa: PT012
                async with EnhancedTaskGroup(timeout=1.0) as tg:
                    _ = tg.create_task(listener())
                    _ = tg.create_task(sleep_then_put())

        assert buffer.getvalue() == text.encode()


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


@mark.only
class TestSubscribe:
    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=10),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_bytes(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[bytes] = Queue()
        async with yield_redis() as redis, subscribe(redis, channel, queue):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                await redis.publish(channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert queue.qsize() == len(messages)
        results = get_items_nowait(queue)
        for result, message in zip(results, messages, strict=True):
            assert isinstance(result, bytes)
            assert result.decode() == message

    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=10),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_raw(self, *, channel: str, messages: Sequence[str]) -> None:
        queue: Queue[_RedisMessageSubscribe] = Queue()
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

    @given(channel=channels(), objs=lists(make_objects(), min_size=1, max_size=10))
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


@mark.only
class TestSubscribeService:
    @given(
        channel=channels(),
        messages=lists(text_ascii(min_size=1), min_size=1, max_size=10),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, channel: str, messages: list[str]) -> None:
        async with (
            yield_redis() as redis,
            SubscribeService(timeout=4.0, redis=redis, channel=channel) as service,
        ):
            await sleep(_PUB_SUB_SLEEP)
            for message in messages:
                await redis.publish(channel, message)
            await sleep(_PUB_SUB_SLEEP)  # keep in context
        assert service.qsize() == len(messages)
        results = service.get_all_nowait()
        for result, message in zip(results, messages, strict=True):
            assert result.decode() == message


class TestYieldClient:
    async def test_main(self) -> None:
        async with yield_redis() as client:
            assert isinstance(client, Redis)


@mark.skip
class TestYieldMessageQueue:
    @given(obj=make_objects())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_deserialize(self, *, obj: Any, tmp_path: Path) -> None:
        channel = str(tmp_path)
        async with (
            yield_redis() as redis,
            yield_message_queue(redis, channel, output=deserialize) as queue,
        ):
            assert isinstance(queue, Queue)
            await redis.publish(channel, serialize(obj))
            await sleep(_PUB_SUB_SLEEP)
            assert queue.qsize() == 1
            result = queue.get_nowait()
        assert is_equal(result, obj)


class TestYieldPubSub:
    async def test_main(self, *, tmp_path: Path) -> None:
        channel = str(tmp_path)
        async with yield_redis() as redis, yield_pubsub(redis, channel) as pubsub:
            assert isinstance(pubsub, PubSub)
