from __future__ import annotations

from asyncio import sleep

from pytest import mark, raises

from utilities.atools import RefreshMemoizedError, memoize, refresh_memoized


class TestMemoize:
    @mark.asyncio
    async def test_main(self) -> None:
        i = 0

        @memoize
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        for _ in range(2):
            assert (await increment()) == 1

    @mark.asyncio
    async def test_with_duration(self) -> None:
        i = 0

        @memoize(duration=1)
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        for _ in range(2):
            assert (await increment()) == 1
        await sleep(1)
        for _ in range(2):
            assert (await increment()) == 2


class TestRefreshMemoized:
    @mark.asyncio
    async def test_main(self) -> None:
        i = 0

        @memoize(duration=1)
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        for _ in range(2):
            assert (await increment()) == 1
        await sleep(1)
        for _ in range(2):
            assert (await increment()) == 2
        assert await refresh_memoized(increment) == 3

    @mark.asyncio
    async def test_error(self) -> None:
        async def none() -> None:
            return None

        with raises(
            RefreshMemoizedError, match="Asynchronous function .* must be memoized"
        ):
            await refresh_memoized(none)
