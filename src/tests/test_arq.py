from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING, Any, cast

from hypothesis import given
from hypothesis.strategies import integers

from utilities.arq import Worker, lift
from utilities.iterables import one

if TYPE_CHECKING:
    from collections.abc import Sequence

    from arq.typing import WorkerCoroutine

    from utilities.types import CallableAwaitable


class TestLift:
    @given(x=integers(), y=integers())
    async def test_main(self, *, x: int, y: int) -> None:
        @lift
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        result = await func({}, x, y)
        assert result == (x + y)


class TestWorker:
    @given(x=integers(), y=integers())
    async def test_main(self, *, x: int, y: int) -> None:
        async def func(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        class Example(Worker):
            functions_raw: Sequence[CallableAwaitable[Any]] = [func]

        func_use = cast("WorkerCoroutine", one(Example.functions))
        result = await func_use({}, x, y)
        assert result == (x + y)
