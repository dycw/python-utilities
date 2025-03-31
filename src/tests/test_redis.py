from __future__ import annotations

from asyncio import create_task, get_running_loop, sleep
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import DataObject, booleans, data
from redis.asyncio import Redis
from tenacity import stop_after_delay

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_operator import make_objects
from utilities.functions import get_class_name
from utilities.hypothesis import (
    int64s,
    settings_with_reduced_examples,
    text_ascii,
    yield_test_redis,
)
from utilities.orjson import deserialize, serialize
from utilities.redis import (
    Publisher,
    publish,
    redis_hash_map_key,
    redis_key,
    subscribe,
    subscribe_messages,
    yield_redis,
)
from utilities.sentinel import SENTINEL_REPR, Sentinel, sentinel
from utilities.tenacity import wait_exponential_jitter

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestPublishAndSubscribe:
    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_obj_ser_{c}"
        ),
        obj=make_objects(),
    )
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
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_text_no_ser_{c}"
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
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_obj_ser_{c}"
        ),
        obj=make_objects(),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(
        self, *, capsys: CaptureFixture, data: DataObject, channel: str, obj: Any
    ) -> None:
        async with yield_test_redis(data) as test:

            async def listener() -> None:
                async for msg in subscribe(
                    test.redis.pubsub(), channel, deserializer=deserialize
                ):
                    print(msg)  # noqa: T201

            task = create_task(listener())
            await sleep(0.1)

            async with Publisher(redis=test.redis, serializer=serialize) as publisher:
                publisher.enqueue((channel, obj))
                await sleep(0.1)

            try:
                out = capsys.readouterr().out
                expected = f"{obj}\n"
                assert out == expected
            finally:
                _ = task.cancel()

    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_text_no_ser_{c}"
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
            await sleep(0.1)
            _ = await publish(test.redis, channel, text)
            await sleep(0.1)
            try:
                out = capsys.readouterr().out
                expected = f"{text.encode()}\n"
                assert out == expected
            finally:
                _ = task.cancel()


class TestSubscribeMessages:
    @given(
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestSubscribeMessages)}_{c}"
        ),
        message=text_ascii(min_size=1),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(
        self, *, capsys: CaptureFixture, channel: str, message: str
    ) -> None:
        client = Redis()

        async def listener() -> None:
            async for msg in subscribe_messages(client.pubsub(), channel):
                print(msg)  # noqa: T201

        task = get_running_loop().create_task(listener())
        await sleep(0.05)
        _ = await client.publish(channel, message)
        await sleep(0.05)
        try:
            out = capsys.readouterr().out
            expected = f"{{'type': 'message', 'pattern': None, 'channel': b'{channel}', 'data': b'{message}'}}\n"
            assert out == expected
        finally:
            _ = task.cancel()


class TestRedisHashMapKey:
    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_bool(
        self, *, data: DataObject, key: int, value: bool
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            assert await hm_key.get(test.redis, key) is None
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
            assert await hm_key.get(test.redis, key) is None
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
            assert await hm_key.get(test.redis, sentinel) is None
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
            assert await hm_key.get(test.redis, key) is None
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
            assert await hm_key.get(test.redis, key) is None
            _ = await hm_key.set(test.redis, key, sentinel)
            assert await hm_key.get(test.redis, key) is sentinel

    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_delete(self, *, data: DataObject, key: int, value: bool) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.get(test.redis, key) is value
            _ = await hm_key.delete(test.redis, key)
            assert await hm_key.get(test.redis, key) is None

    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_exists(self, *, data: DataObject, key: int, value: bool) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(test.key, int, bool)
            assert not (await hm_key.exists(test.redis, key))
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.exists(test.redis, key)

    @given(data=data(), key=int64s(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_stop_and_wait(
        self, *, data: DataObject, key: int, value: bool
    ) -> None:
        async with yield_test_redis(data) as test:
            hm_key = redis_hash_map_key(
                test.key,
                int,
                bool,
                stop=stop_after_delay(1),
                wait=wait_exponential_jitter(),
            )
            _ = await hm_key.set(test.redis, key, value)
            assert await hm_key.exists(test.redis, key)

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


class TestRedisKey:
    @given(data=data(), value=booleans())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_get_and_set_bool(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(test.key, bool)
            assert await key.get(test.redis) is None
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
            assert await key.get(test.redis) is None
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
            assert await key.get(test.redis) is None
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
            assert await key.get(test.redis) is None

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
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_stop_and_wait(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = redis_key(
                test.key, bool, stop=stop_after_delay(1), wait=wait_exponential_jitter()
            )
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


class TestYieldClient:
    async def test_sync(self) -> None:
        async with yield_redis() as client:
            assert isinstance(client, Redis)
