from __future__ import annotations

from asyncio import TaskGroup, sleep
from re import search
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from tests.test_redis import yield_test_redis
from utilities.iterables import one
from utilities.pottery import (
    _YieldAccessNumLocksError,
    _YieldAccessUnableToAcquireLockError,
    yield_access,
)
from utilities.text import unique_str
from utilities.timer import Timer

if TYPE_CHECKING:
    from redis.asyncio import Redis


async def _func_access(num_tasks: int, key: str, /, *, num_locks: int = 1) -> None:
    async def coroutine() -> None:
        async with yield_test_redis() as redis, yield_access(redis, key, num=num_locks):
            await sleep(0.1)

    async with TaskGroup() as tg:
        _ = [tg.create_task(coroutine()) for _ in range(num_tasks)]


class TestYieldAccess:
    @SKIPIF_CI_AND_NOT_LINUX
    @mark.parametrize(
        ("num_tasks", "num_locks", "min_time"),
        [
            param(1, 1, 0.1),
            param(1, 2, 0.1),
            param(1, 3, 0.1),
            param(2, 1, 0.2),
            param(2, 2, 0.1),
            param(2, 3, 0.1),
            param(2, 4, 0.1),
            param(2, 5, 0.1),
            param(3, 1, 0.3),
            param(3, 2, 0.2),
            param(3, 3, 0.1),
            param(3, 4, 0.1),
            param(3, 5, 0.1),
            param(4, 1, 0.4),
            param(4, 2, 0.2),
            param(4, 3, 0.2),
            param(4, 4, 0.1),
            param(4, 5, 0.1),
        ],
    )
    async def test_main(
        self, *, num_tasks: int, num_locks: int, min_time: float
    ) -> None:
        with Timer() as timer:
            await _func_access(num_tasks, unique_str(), num_locks=num_locks)
        assert min_time <= float(timer) <= 3 * min_time

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

        async def coroutine(redis: Redis, key: str, /) -> None:
            async with yield_access(
                redis, key, num=1, timeout_acquire=0.1, throttle=0.5
            ):
                await sleep(0.1)

        with raises(ExceptionGroup) as exc_info:  # noqa: PT012
            async with yield_test_redis() as redis, TaskGroup() as tg:
                _ = tg.create_task(coroutine(redis, key))
                _ = tg.create_task(coroutine(redis, key))
        error = one(exc_info.value.exceptions)
        assert isinstance(error, _YieldAccessUnableToAcquireLockError)
        assert search(
            r"Unable to acquire any 1 of 1 locks for '\w+' after 0\.1", str(error)
        )
