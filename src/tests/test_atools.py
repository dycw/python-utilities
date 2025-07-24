from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.asyncio import sleep_td
from utilities.atools import call_memoized, memoize
from utilities.whenever import SECOND


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
        delta = 0.1 * SECOND

        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for _ in range(2):
            assert (await call_memoized(increment, delta)) == 1
            assert counter == 1
        await sleep_td(2 * delta)
        for _ in range(2):
            assert (await call_memoized(increment, delta)) == 2
            assert counter == 2


class TestMemoize:
    async def test_no_arguments(self) -> None:
        counter = 0

        @memoize
        async def increment() -> int:
            nonlocal counter
            counter += 1
            return counter

        for i in range(1, 3):
            assert await increment() == i
            assert counter == i

    @given(max_size=integers(1, 10))
    async def test_with_arguments(self, *, max_size: int, typed: bool) -> None:
        counter = 0

        @memoize(max_size=max_size, typed=typed)
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        for _ in range(2):
            assert func(0) == 0
        assert counter == 1
