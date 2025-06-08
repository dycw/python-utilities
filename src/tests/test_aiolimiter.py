from __future__ import annotations

from asyncio import sleep

from utilities.aiolimiter import get_async_limiter
from utilities.text import unique_str
from utilities.timer import Timer


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
                async with get_async_limiter(name, max_rate=2):
                    await increment()
        assert timer >= 0.5
