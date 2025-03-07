from __future__ import annotations

from asyncio import sleep

from utilities.atools import call_memoized


class TestCallMemoized:
    async def test_main(self) -> None:
        i = 0

        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        assert (await call_memoized(increment)) == 1
        assert (await call_memoized(increment)) == 2
        for _ in range(2):
            assert (await call_memoized(increment, 0.01)) == 3
        await sleep(0.01)
        assert (await call_memoized(increment)) == 4
        assert (await call_memoized(increment)) == 5
        for _ in range(2):
            assert (await call_memoized(increment, 0.01)) == 6
