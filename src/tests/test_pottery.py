from __future__ import annotations

from asyncio import TaskGroup, sleep

from hypothesis import Phase, given

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_redis import yield_test_redis
from utilities.hypothesis import settings_with_reduced_examples, unique_strs
from utilities.pottery import yield_locked_resource
from utilities.timer import Timer


async def _coroutine(key: str) -> None:
    async with yield_test_redis() as redis, yield_locked_resource(redis, key):
        await sleep(0.1)


async def _asyncio_runner(num_tasks: int, key: str) -> None:
    async with TaskGroup() as tg:
        _ = [tg.create_task(_coroutine(key)) for _ in range(num_tasks)]


class TestYieldLockedResource:
    @given(key=unique_strs())
    @settings_with_reduced_examples(phases={Phase.generate})
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_single_process(self, *, key: str) -> None:
        with Timer() as timer:
            await _asyncio_runner(3, key)
        min_time = 0.3
        assert min_time <= float(timer) <= 3 * min_time
