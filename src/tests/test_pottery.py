from __future__ import annotations

from asyncio import TaskGroup, sleep
from typing import TYPE_CHECKING

from hypothesis import given

from tests.test_redis import yield_test_redis
from utilities.hypothesis import unique_strs
from utilities.pottery import yield_locked_resource
from utilities.timer import Timer

if TYPE_CHECKING:
    from redis.asyncio import Redis


class TestYieldLockedResource:
    @given(key=unique_strs())
    async def test_main(self, *, key: str) -> None:
        async def coroutine(redis: Redis) -> None:
            async with yield_locked_resource(redis, key):
                await sleep(0.1)

        with Timer():
            async with TaskGroup() as tg, yield_test_redis() as redis:
                _ = [tg.create_task(coroutine(redis)) for i in range(2)]
