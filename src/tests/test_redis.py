from __future__ import annotations

from asyncio import create_task, get_running_loop, sleep
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import DataObject, booleans, data
from redis.asyncio import Redis

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_orjson2 import _Object, objects
from utilities.functions import get_class_name
from utilities.hypothesis import int64s, text_ascii, yield_test_redis
from utilities.orjson import SerializeError, deserialize, serialize
from utilities.redis import (
    RedisHashMapKey,
    RedisKey,
    publish,
    subscribe,
    subscribe_messages,
    yield_redis,
)
from utilities.sentinel import SENTINEL_REPR, Sentinel, sentinel

if TYPE_CHECKING:
    from pytest import CaptureFixture

from pytest import raises


class TestRedisKey:
    @given(data=data(), value=booleans())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_bool(self, *, data: DataObject, value: bool) -> None:
        async with yield_test_redis(data) as test:
            key = RedisKey(name=test.key, type=bool)
            assert await key.get(test.redis) is None
            _ = await key.set(test.redis, value)
            assert await key.get(test.redis) is value

    @given(data=data())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_sentinel_with_serialize(self, *, data: DataObject) -> None:
        def serializer(sentinel: Sentinel, /) -> bytes:
            return repr(sentinel).encode()

        def deserializer(data: bytes, /) -> Sentinel:
            assert data == SENTINEL_REPR.encode()
            return sentinel

        async with yield_test_redis(data) as test:
            key = RedisKey(
                name=test.key,
                type=Sentinel,
                serializer=serializer,
                deserializer=deserializer,
            )
            assert await key.get(test.redis) is None
            _ = await key.set(test.redis, sentinel)
            assert await key.get(test.redis) is sentinel

    @given(data=data())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_sentinel_without_serialize(self, *, data: DataObject) -> None:
        async with yield_test_redis(data) as test:
            key = RedisKey(name=test.key, type=Sentinel)
            with raises(
                SerializeError, match="Unable to serialize object of type 'Sentinel'"
            ):
                _ = await key.set(test.redis, sentinel)


class TestRedisHashMapKey:
    @given(data=data(), key=int64s(), value=booleans())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, data: DataObject, key: int, value: bool) -> None:
        async with yield_test_redis(data) as test:
            hm_key = RedisHashMapKey(name=test.key, key=int, value=bool)
            assert await hm_key.hget(test.redis, key) is None
            _ = await hm_key.hset(test.redis, key, value)
            assert await hm_key.hget(test.redis, key) is value


class TestPublishAndSubscribe:
    @given(
        data=data(),
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestPublishAndSubscribe)}_obj_ser_{c}"
        ),
        obj=objects,
    )
    @settings(
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_all_objects_with_serialize(
        self, *, capsys: CaptureFixture, data: DataObject, channel: str, obj: _Object
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


class TestYieldClient:
    async def test_sync(self) -> None:
        async with yield_redis() as client:
            assert isinstance(client, Redis)
