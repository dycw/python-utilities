from __future__ import annotations

import redis
import redis.asyncio
from dts.redis.clients import yield_redis_client
from dts.redis.store import RedisItem, _RedisItem
from hypothesis import given
from hypothesis.strategies import booleans, dictionaries
from pytest import mark

from utilities.hypothesis import int64s, text_ascii
from utilities.redis import yield_client, yield_client_async


class TestRedisKey:
    @mark.parametrize("key", RedisItem, ids=str)
    def test_enum_name_and_key_name_agree(self, *, key: RedisItem) -> None:
        assert key.name == key.value.name

    @mark.parametrize("key", RedisItem, ids=str)
    async def test_getter(self, *, key: RedisItem) -> None:
        _ = await key.value.get()

    @mark.parametrize("key", RedisItem, ids=str)
    def test_values_are_data(self, *, key: RedisItem) -> None:
        assert isinstance(key.value, _RedisItem)

    @given(value=booleans())
    async def test_bool(self, *, value: bool) -> None:
        async with yield_redis_client(db=15) as client:
            await client.delete(RedisItem._test_bool.name)  # noqa: SLF001
        get_test_bool = RedisItem._test_bool.value.get  # noqa: SLF001
        assert await get_test_bool() is None
        await RedisItem._test_bool.value.set(value)  # noqa: SLF001
        assert await get_test_bool() is value

    @given(value=dictionaries(text_ascii(), int64s()))
    async def test_dict(self, *, value: dict[str, int]) -> None:
        async with yield_redis_client(db=15) as client:
            await client.delete(RedisItem._test_dict.name)  # noqa: SLF001
        get_test_dict = RedisItem._test_dict.value.get  # noqa: SLF001
        assert await get_test_dict() is None
        await RedisItem._test_dict.value.set(value)  # noqa: SLF001
        assert await get_test_dict() == value

    @given(n=int64s())
    async def test_int(self, *, n: int) -> None:
        async with yield_redis_client(db=15) as client:
            await client.delete(RedisItem._test_int.name)  # noqa: SLF001
        get_test_int = RedisItem._test_int.value.get  # noqa: SLF001
        assert await get_test_int() is None
        await RedisItem._test_int.value.set(n)  # noqa: SLF001
        assert await get_test_int() == n

    @given(text=text_ascii())
    async def test_str(self, *, text: str) -> None:
        async with yield_redis_client(db=15) as client:
            await client.delete(RedisItem._test_str.name)  # noqa: SLF001
        get_test_str = RedisItem._test_str.value.get  # noqa: SLF001
        assert await get_test_str() is None
        await RedisItem._test_str.value.set(text)  # noqa: SLF001
        assert await get_test_str() == text


class TestYieldClient:
    def test_sync_default(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    def test_sync_client(self) -> None:
        with yield_client() as client1, yield_client(client=client1) as client2:
            assert isinstance(client2, redis.Redis)

    async def test_async_default(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)

    async def test_async_client(self) -> None:
        async with (
            yield_client_async() as client1,
            yield_client_async(client=client1) as client2,
        ):
            assert isinstance(client2, redis.asyncio.Redis)
