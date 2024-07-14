from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING

from pytest import mark, raises

from utilities.atools import RefreshMemoizedError, memoize, refresh_memoized

if TYPE_CHECKING:
    from collections.abc import Hashable


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

    @mark.asyncio
    async def test_with_keygen(self) -> None:
        i = 0

        def _keygen(*, j: int, _ignore: bool) -> tuple[Hashable, ...]:
            return (j,)

        @memoize(keygen=_keygen)
        async def increment(j: int, /, *, _ignore: bool) -> int:
            nonlocal i
            i += j
            _ = _ignore
            return i

        for j in [True, False]:
            assert (await increment(1, _ignore=j)) == 1
        for j in [True, False]:
            assert (await increment(2, _ignore=j)) == 3


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
