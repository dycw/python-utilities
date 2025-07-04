from __future__ import annotations

from asyncio import TaskGroup
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_redis import yield_test_redis
from utilities.asyncio import sleep_td
from utilities.pottery import (
    _YieldAccessNumLocksError,
    _YieldAccessUnableToAcquireLockError,
    yield_access,
)
from utilities.text import unique_str
from utilities.timer import Timer
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from whenever import TimeDelta


_DELTA: TimeDelta = 0.1 * SECOND


async def _func_access(num_tasks: int, key: str, /, *, num_locks: int = 1) -> None:
    async def coroutine() -> None:
        async with yield_test_redis() as redis, yield_access(redis, key, num=num_locks):
            await sleep_td(_DELTA)

    async with TaskGroup() as tg:
        _ = [tg.create_task(coroutine()) for _ in range(num_tasks)]


class TestYieldAccess:
    @SKIPIF_CI_AND_NOT_LINUX
    @mark.parametrize(
        ("num_tasks", "num_locks", "min_multiple"),
        [
            param(1, 1, 1),
            param(1, 2, 1),
            param(1, 3, 1),
            param(2, 1, 2),
            param(2, 2, 1),
            param(2, 3, 1),
            param(2, 4, 1),
            param(2, 5, 1),
            param(3, 1, 3),
            param(3, 2, 2),
            param(3, 3, 1),
            param(3, 4, 1),
            param(3, 5, 1),
            param(4, 1, 4),
            param(4, 2, 2),
            param(4, 3, 2),
            param(4, 4, 1),
            param(4, 5, 1),
        ],
    )
    async def test_main(
        self, *, num_tasks: int, num_locks: int, min_multiple: int
    ) -> None:
        with Timer() as timer:
            await _func_access(num_tasks, unique_str(), num_locks=num_locks)
        assert (min_multiple * _DELTA) <= timer <= (5 * min_multiple * _DELTA)

    async def test_error_num_locks(self) -> None:
        key = unique_str()
        with raises(
            _YieldAccessNumLocksError,
            match=r"Number of locks for '\w+' must be positive; got 0",
        ):
            async with yield_test_redis() as redis, yield_access(redis, key, num=0):
                ...

    @SKIPIF_CI_AND_NOT_LINUX
    async def test_error_unable_to_acquire_lock(self) -> None:
        key = unique_str()
        delta = 0.1 * SECOND

        async def coroutine(redis: Redis, key: str, /) -> None:
            async with yield_access(
                redis, key, num=1, timeout_acquire=delta, throttle=5 * delta
            ):
                await sleep_td(delta)

        with raises(ExceptionGroup) as exc_info:
            async with yield_test_redis() as redis, TaskGroup() as tg:
                _ = tg.create_task(coroutine(redis, key))
                _ = tg.create_task(coroutine(redis, key))
        assert exc_info.group_contains(
            _YieldAccessUnableToAcquireLockError,
            match=r"Unable to acquire any 1 of 1 locks for '\w+' after .*",
        )
