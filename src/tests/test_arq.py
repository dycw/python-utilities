from __future__ import annotations

from asyncio import sleep

from hypothesis import given
from hypothesis.strategies import integers

from utilities.arq import lift


class TestLift:
    @given(x=integers(), y=integers())
    async def test_main(self, *, x: int, y: int) -> None:
        @lift
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        assert func({}, x, y) == (x + y)
