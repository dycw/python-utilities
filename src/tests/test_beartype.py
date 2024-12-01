from __future__ import annotations

from asyncio import sleep
from typing import Any, cast

from beartype.roar import BeartypeCallHintReturnViolation
from pytest import raises

from utilities.beartype import beartype_cond


class TestBeartypeCond:
    def test_main(self) -> None:
        @beartype_cond
        def func(a: int, b: int, /) -> int:
            return cast(Any, str(a + b))

        with raises(BeartypeCallHintReturnViolation):
            _ = func(1, 2)

    def test_enable_sync(self) -> None:
        enable = True

        @beartype_cond(enable=lambda: enable)
        def func(a: int, b: int, /) -> int:
            return cast(Any, str(a + b))

        with raises(BeartypeCallHintReturnViolation):
            _ = func(1, 2)
        enable = False
        _ = func(1, 2)
        enable = True
        with raises(BeartypeCallHintReturnViolation):
            _ = func(1, 2)

    async def test_enable_async(self) -> None:
        enable = True

        @beartype_cond(enable=lambda: enable)
        async def func(a: int, b: int, /) -> int:
            await sleep(0.01)
            return cast(Any, str(a + b))

        with raises(BeartypeCallHintReturnViolation):
            _ = await func(1, 2)
        enable = False
        _ = await func(1, 2)
        enable = True
        with raises(BeartypeCallHintReturnViolation):
            _ = await func(1, 2)
