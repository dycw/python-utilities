from __future__ import annotations

from asyncio import sleep
from inspect import signature
from typing import Annotated, Any

from pytest import raises

from tests.conftest import FLAKY
from utilities.atools import (
    RefreshMemoizedError,
    _memoize_auto_keygen_is_param,
    memoize,
    no_memoize,
    refresh_memoized,
)


class TestMemoize:
    async def test_main(self) -> None:
        i = 0

        @memoize
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        for _ in range(2):
            assert (await increment()) == 1

    @FLAKY
    async def test_with_duration(self) -> None:
        i = 0

        @memoize(duration=0.01)
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        for _ in range(2):
            assert (await increment()) == 1
        await sleep(0.01)
        for _ in range(2):
            assert (await increment()) == 2

    async def test_with_keygen(self) -> None:
        i = 0

        @memoize
        async def increment(j: int, /, *, ignore: Annotated[bool, no_memoize]) -> int:
            nonlocal i
            i += j
            _ = ignore
            return i

        for j in [True, False]:
            assert (await increment(1, ignore=j)) == 1
        for j in [True, False]:
            assert (await increment(2, ignore=j)) == 3


class TestMemoizeAutoKeygenIsParam:
    def test_no_annotation(self) -> None:
        def func(a, /) -> Any:  # noqa: ANN001 # pyright: ignore[reportMissingParameterType]
            return a

        ann = signature(func).parameters["a"].annotation
        result = _memoize_auto_keygen_is_param(ann)
        assert result is True

    def test_basic_annotation(self) -> None:
        def func(a: int, /) -> int:
            return a

        ann = signature(func).parameters["a"].annotation
        result = _memoize_auto_keygen_is_param(ann)
        assert result is True

    def test_no_memoize(self) -> None:
        def func(a: Annotated[int, no_memoize], /) -> int:
            return a

        ann = signature(func).parameters["a"].annotation
        result = _memoize_auto_keygen_is_param(ann)
        assert result is False


class TestRefreshMemoized:
    @FLAKY
    async def test_main(self) -> None:
        i = 0

        @memoize(duration=0.01)
        async def increment() -> int:
            nonlocal i
            i += 1
            return i

        for _ in range(2):
            assert (await increment()) == 1
        await sleep(0.01)
        for _ in range(2):
            assert (await increment()) == 2
        assert await refresh_memoized(increment) == 3

    async def test_error(self) -> None:
        async def none() -> None:
            return None

        with raises(
            RefreshMemoizedError, match="Asynchronous function .* must be memoized"
        ):
            await refresh_memoized(none)
