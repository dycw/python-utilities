from __future__ import annotations

from utilities.asyncio import sleep_td
from utilities.atools import call_memoized, memoize
from utilities.whenever import SECOND

_DELTA = 0.1 * SECOND


class TestCallMemoized:
    async def test_main(self) -> None:
        counter = 0

        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for i in range(1, 11):
            assert (await call_memoized(increment)) == i
            assert counter == i

    async def test_refresh(self) -> None:
        counter = 0

        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for _ in range(2):
            assert (await call_memoized(increment, _DELTA)) == 1
            assert counter == 1
        await sleep_td(2 * _DELTA)
        for _ in range(2):
            assert (await call_memoized(increment, _DELTA)) == 2
            assert counter == 2


class TestMemoize:
    async def test_main(self) -> None:
        counter = 0

        @memoize
        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for _ in range(10):
            assert await increment() == 1
            assert counter == 1

    async def test_with_arguments(self) -> None:
        counter = 0

        @memoize(duration=_DELTA)
        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        assert await increment() == 1
        assert counter == 1
        await sleep_td(2 * _DELTA)
        assert await increment() == 2
        assert counter == 2
