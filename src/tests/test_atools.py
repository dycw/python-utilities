from __future__ import annotations

from utilities.asyncio import sleep_dur
from utilities.atools import call_memoized
from utilities.whenever2 import SECOND


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
        delta = 0.05 * SECOND

        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for _ in range(2):
            assert (await call_memoized(increment, delta)) == 1
            assert counter == 1
        await sleep_dur(duration=2 * delta)
        for _ in range(2):
            assert (await call_memoized(increment, delta)) == 2
            assert counter == 2
