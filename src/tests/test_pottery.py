from __future__ import annotations

from asyncio import TaskGroup, sleep
from typing import TYPE_CHECKING

from hypothesis import given

from tests.test_redis import channels
from utilities.pottery import yield_locked_resource
from utilities.redis import yield_redis
from utilities.timer import Timer

if TYPE_CHECKING:
    from redis.asyncio import Redis


class TestYieldLockedResource:
    @given(key=channels())
    async def test_main(self, *, key: str) -> None:
        async def coroutine(redis: Redis) -> None:
            async with yield_locked_resource(redis, key):
                await sleep(0.1)

        with Timer() as t, yield_redis(db=15):
            async with TaskGroup() as tg:
                _ = [
                    tg.create_task(coroutine(host=f"job{j}-test{i}")) for i in range(10)
                ]
        t
