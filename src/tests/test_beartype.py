from __future__ import annotations

from asyncio import sleep
from dataclasses import dataclass
from typing import Any, cast

from beartype.roar import (
    BeartypeCallHintParamViolation,
    BeartypeCallHintReturnViolation,
)
from hypothesis import given
from hypothesis.strategies import booleans
from pytest import raises

from utilities.beartype import beartype_cond


class TestBeartypeCond:
    def test_main(self) -> None:
        @beartype_cond
        def func(a: int, b: int, /) -> int:
            return cast(Any, str(a + b))

        with raises(BeartypeCallHintReturnViolation):
            _ = func(1, 2)

    def test_runtime_sync(self) -> None:
        enable = True

        @beartype_cond(runtime=lambda: enable)
        def func(a: int, b: int, /) -> int:
            return cast(Any, str(a + b))

        with raises(BeartypeCallHintReturnViolation):
            _ = func(1, 2)
        enable = False
        _ = func(1, 2)
        enable = True
        with raises(BeartypeCallHintReturnViolation):
            _ = func(1, 2)

    async def test_runtime_async(self) -> None:
        enable = True

        @beartype_cond(runtime=lambda: enable)
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

    def test_runtime_class_main(self) -> None:
        @beartype_cond
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        with raises(BeartypeCallHintParamViolation):
            _ = Example(x=cast(Any, "0"))
        _ = Example(x=0)

    @given(runtime=booleans())
    def test_runtime_class_enable(self, *, runtime: bool) -> None:
        @beartype_cond(runtime=lambda: runtime)
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        with raises(BeartypeCallHintParamViolation):
            _ = Example(x=cast(Any, "0"))
