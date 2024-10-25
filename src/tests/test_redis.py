from __future__ import annotations

from asyncio import get_running_loop, sleep

import redis
import redis.asyncio
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import DataObject, booleans, data
from pytest import CaptureFixture, mark

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from utilities.hypothesis import int64s, redis_cms, text_ascii
from utilities.redis import RedisHashMapKey, RedisKey, subscribe_messages


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


@mark.only
class TestSubscribeMessages:
    @given(
        channel=text_ascii(min_size=1).map(lambda c: f"test_{c}"),
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
        pubsub = client.pubsub()

        async def listener() -> None:
            async for msg in subscribe_messages(channel, pubsub=pubsub):
                print(msg)  # noqa: T201

        loop = get_running_loop()
        task = loop.create_task(listener())
        await sleep(0.01)
        await client.publish(channel, message)
        await sleep(0.01)
        try:
            out = capsys.readouterr().out
            expected = f"{{'type': 'message', 'pattern': None, 'channel': b'{channel}', 'data': b'{message}'}}\n"
            assert out == expected
        finally:
            _ = task.cancel()
            await client.aclose()
