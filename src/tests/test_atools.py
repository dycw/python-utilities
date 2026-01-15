from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.asyncio import sleep
from utilities.atools import call_memoized, memoize
from utilities.constants import SECOND

if TYPE_CHECKING:
    from whenever import TimeDelta


_DURATION: TimeDelta = 0.05 * SECOND


class TestCallMemoized:
    async def test_main(self) -> None:
        counter = 0

        async def increment() -> int:
            await sleep()
            nonlocal counter
            counter += 1
            return counter

        for i in range(1, 11):
            assert (await call_memoized(increment)) == i
            assert counter == i

    async def test_refresh(self) -> None:
        counter = 0

        async def increment() -> int:
            await sleep()
            nonlocal counter
            counter += 1
            return counter

        for _ in range(2):
            assert (await call_memoized(increment, _DURATION)) == 1
            assert counter == 1
        await sleep(2 * _DURATION)
        for _ in range(2):
            assert (await call_memoized(increment, _DURATION)) == 2
            assert counter == 2


class TestMemoize:
    async def test_main(self) -> None:
        counter = 0

        @memoize
        async def increment() -> int:
            await sleep()
            nonlocal counter
            counter += 1
            return counter

        for _ in range(10):
            assert await increment() == 1
            assert counter == 1

    async def test_with_arguments(self) -> None:
        counter = 0

        @memoize(duration=_DURATION)
        async def increment() -> int:
            await sleep()
            nonlocal counter
            counter += 1
            return counter

        assert await increment() == 1
        assert counter == 1
        await sleep(2 * _DURATION)
        assert await increment() == 2
        assert counter == 2
