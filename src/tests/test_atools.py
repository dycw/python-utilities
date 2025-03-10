from __future__ import annotations

from asyncio import sleep

from utilities.atools import call_memoized


class TestCallMemoized:
    async def test_main(self) -> None:
        counter = 0

        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for i in range(1, 3):
            assert (await call_memoized(increment)) == i
            assert counter == i

    async def test_refresh(self) -> None:
        counter = 0

        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for _ in range(2):
            assert (await call_memoized(increment, 0.05)) == 1
            assert counter == 1
        await sleep(0.1)
        for _ in range(2):
            assert (await call_memoized(increment, 0.05)) == 2
            assert counter == 2
