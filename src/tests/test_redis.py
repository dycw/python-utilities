from __future__ import annotations

from asyncio import get_running_loop, sleep
from typing import TYPE_CHECKING

import redis
import redis.asyncio
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import DataObject, booleans, data

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_orjson2 import _Object, objects
from utilities.functions import get_class_name
from utilities.hypothesis import int64s, redis_cms, text_ascii
from utilities.orjson import deserialize, serialize
from utilities.redis import (
    RedisHashMapKey,
    RedisKey,
    publish,
    subscribe,
    subscribe_messages,
)

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestRedisKey:
    @given(data=data(), value=booleans())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, data: DataObject, value: bool) -> None:
        async with redis_cms(data) as container:
            key = RedisKey(name=container.key, type=bool)
            match container.client:
                case redis.Redis():
                    assert key.get(db=15) is None
                    _ = key.set(value, db=15)
                    assert key.get(db=15) is value
                case redis.asyncio.Redis():
                    assert await key.get_async(db=15) is None
                    _ = await key.set_async(value, db=15)
                    assert await key.get_async(db=15) is value

    @given(data=data(), value=booleans())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_using_client(self, *, data: DataObject, value: bool) -> None:
        async with redis_cms(data) as container:
            key = RedisKey(name=container.key, type=bool)
            match container.client:
                case redis.Redis() as client:
                    assert key.get(client=client) is None
                    _ = key.set(value, client=client)
                    assert key.get(client=client) is value
                case redis.asyncio.Redis() as client:
                    assert await key.get_async(client=client) is None
                    _ = await key.set_async(value, client=client)
                    assert await key.get_async(client=client) is value


class TestRedisHashMapKey:
    @given(data=data(), key=int64s(), value=booleans())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, data: DataObject, key: int, value: bool) -> None:
        async with redis_cms(data) as container:
            hash_map_key = RedisHashMapKey(name=container.key, key=int, value=bool)
            match container.client:
                case redis.Redis():
                    assert hash_map_key.hget(key, db=15) is None
                    _ = hash_map_key.hset(key, value, db=15)
                    assert hash_map_key.hget(key, db=15) is value
                case redis.asyncio.Redis():
                    assert await hash_map_key.hget_async(key, db=15) is None
                    _ = await hash_map_key.hset_async(key, value, db=15)
                    assert await hash_map_key.hget_async(key, db=15) is value

    @given(data=data(), key=int64s(), value=booleans())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_using_client(
        self, *, data: DataObject, key: int, value: bool
    ) -> None:
        async with redis_cms(data) as container:
            hash_map_key = RedisHashMapKey(name=container.key, key=int, value=bool)
            match container.client:
                case redis.Redis() as client:
                    assert hash_map_key.hget(key, client=client) is None
                    _ = hash_map_key.hset(key, value, client=client)
                    assert hash_map_key.hget(key, client=client) is value
                case redis.asyncio.Redis() as client:
                    assert await hash_map_key.hget_async(key, client=client) is None
                    _ = await hash_map_key.hset_async(key, value, client=client)
                    assert await hash_map_key.hget_async(key, client=client) is value


class TestPublishAndSubscribe:
    @given(
        channel=text_ascii(min_size=1).map(
            lambda c: f"{get_class_name(TestSubscribeMessages)}_{c}"
        ),
        obj=objects,
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(
        self, *, capsys: CaptureFixture, channel: str, obj: _Object
    ) -> None:
        client = redis.asyncio.Redis()

        async def listener() -> None:
            async for msg in subscribe(
                channel, pubsub=client.pubsub(), deserializer=deserialize
            ):
                print(msg)  # noqa: T201

        task = get_running_loop().create_task(listener())
        await sleep(0.01)
        _ = await publish(channel, obj, redis=client, serializer=serialize)
        await sleep(0.01)
        try:
            out = capsys.readouterr().out
            expected = f"{obj}\n"
            assert out == expected
        finally:
            _ = task.cancel()
            await client.aclose()


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
    async def test_main(
        self, *, capsys: CaptureFixture, channel: str, message: str
    ) -> None:
        client = redis.asyncio.Redis()

        async def listener() -> None:
            async for msg in subscribe_messages(channel, pubsub=client.pubsub()):
                print(msg)  # noqa: T201

        task = get_running_loop().create_task(listener())
        await sleep(0.01)
        _ = await client.publish(channel, message)
        await sleep(0.01)
        try:
            out = capsys.readouterr().out
            expected = f"{{'type': 'message', 'pattern': None, 'channel': b'{channel}', 'data': b'{message}'}}\n"
            assert out == expected
        finally:
            _ = task.cancel()
            await client.aclose()
