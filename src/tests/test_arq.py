from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING, Any, cast

from hypothesis import given
from hypothesis.strategies import integers

from utilities.arq import Worker, cron_raw
from utilities.iterables import one

if TYPE_CHECKING:
    from collections.abc import Sequence

    from arq.cron import CronJob
    from arq.typing import WorkerCoroutine

    from utilities.types import CallableCoroutine1


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

    @given(x=integers(), y=integers())
    async def test_cron(self, *, x: int, y: int) -> None:
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        class Example(Worker):
            cron_jobs: Sequence[CronJob] | None = [cron_raw(func, args=(x, y))]

        assert Example.cron_jobs is not None
        cron_job = one(Example.cron_jobs)
        result = await cron_job.coroutine({})
        assert result == (x + y)
