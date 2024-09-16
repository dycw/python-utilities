from __future__ import annotations

import redis
import redis.asyncio
from hypothesis import given
from hypothesis.strategies import DataObject, booleans, data

from utilities.hypothesis import redis_cms
from utilities.redis import RedisKey, yield_client, yield_client_async


class TestRedisKey:
    @given(data=data(), value=booleans())
    async def test_main(self, *, data: DataObject, value: bool) -> None:
        async with redis_cms(data) as container:
            key = RedisKey(name=container.key, type=bool)
            match container.client:
                case redis.Redis():
                    assert key.get() is None
                    _ = key.set(value)
                    assert key.get() is value
                case redis.asyncio.Redis():
                    assert await key.get_async() is None
                    _ = await key.set_async(value)
                    assert await key.get_async() is value


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
