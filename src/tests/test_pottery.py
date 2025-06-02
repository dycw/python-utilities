from __future__ import annotations

from asyncio import TaskGroup, sleep
from typing import TYPE_CHECKING

from hypothesis import Phase, given
from pytest import approx

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_redis import yield_test_redis
from utilities.hypothesis import settings_with_reduced_examples, unique_strs
from utilities.pottery import yield_locked_resource
from utilities.timer import Timer

if TYPE_CHECKING:
    from redis.asyncio import Redis


class TestYieldLockedResource:
    @given(key=unique_strs())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, key: str) -> None:
        async def coroutine(redis: Redis) -> None:
            async with yield_locked_resource(redis, key):
                await sleep(0.1)

        with Timer() as timer:
            async with TaskGroup() as tg, yield_test_redis() as redis:
                _ = [tg.create_task(coroutine(redis)) for _ in range(10)]
        assert float(timer) == approx(1.0, rel=0.5)
