from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING, Any, cast

from arq.connections import ArqRedis
from hypothesis import given
from hypothesis.strategies import integers

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from utilities.arq import Worker, cron_raw, job_enqueuer
from utilities.iterables import one

if TYPE_CHECKING:
    from collections.abc import Sequence

    from arq.typing import WorkerCoroutine

    from utilities.types import CallableCoroutine1


class TestCronRaw:
    @given(x=integers(), y=integers())
    async def test_main(self, *, x: int, y: int) -> None:
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        job = cron_raw(func, args=(x, y))
        result = await job.coroutine({})
        assert result == (x + y)


class TestJobEnqueuer:
    @given(x=integers(), y=integers())
    @SKIPIF_CI_AND_NOT_LINUX
    async def test_main(self, *, x: int, y: int) -> None:
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        redis = ArqRedis(db=15)
        _ = await job_enqueuer.settings(queue_name="test")(redis, func, x, y)


class TestWorker:
    @given(x=integers(), y=integers())
    async def test_main(self, *, x: int, y: int) -> None:
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        class Example(Worker):
            functions_raw: Sequence[CallableCoroutine1[Any]] = [func]

        func_use = cast("WorkerCoroutine", one(Example.functions))
        result = await func_use({}, x, y)
        assert result == (x + y)
