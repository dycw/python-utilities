from __future__ import annotations

from asyncio import sleep
from typing import ClassVar

from utilities.aiolimiter import get_async_limiter
from utilities.text import unique_str
from utilities.timer import Timer
from utilities.whenever import SECOND


class TestGetAsyncLimiter:
    async def test_main(self) -> None:
        counter = 0

        async def increment() -> None:
            nonlocal counter
            counter += 1
            await sleep(0.01)

        name = unique_str()
        with Timer() as timer:
            for _ in range(2):
                async with get_async_limiter(name, rate=0.5):
                    await increment()
        assert timer >= 0.48 * SECOND

    shared: ClassVar[str] = unique_str()

    async def test_shared1(self) -> None:
        async with get_async_limiter(self.shared):
            await sleep(0.01)

    async def test_shared2(self) -> None:
        async with get_async_limiter(self.shared):
            await sleep(0.01)

    async def test_shared3(self) -> None:
        async with get_async_limiter(self.shared):
            await sleep(0.01)
