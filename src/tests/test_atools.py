from __future__ import annotations

from asyncio import sleep

from pytest import mark

from utilities.atools import memoize


class TestMemoize:
    @mark.asyncio
    async def test_main(self) -> None:
        i = 0

        @memoize
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        assert i == 0
        for _ in range(2):
            assert (await increment()) == 1
            assert i == 1

    @mark.asyncio
    async def test_with_duration(self) -> None:
        i = 0

        @memoize(duration=1)
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        assert i == 0
        for _ in range(2):
            assert (await increment()) == 1
            assert i == 1
        await sleep(1)
        for _ in range(2):
            assert (await increment()) == 2
            assert i == 2
