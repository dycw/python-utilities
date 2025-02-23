from __future__ import annotations

from asyncio import sleep
from inspect import signature
from typing import Annotated, Any

from pytest import raises

from utilities.atools import (
    RefreshMemoizedError,
    _memoize_auto_keygen_is_param,
    call_memoized,
    memoize,
    no_memoize,
    refresh_memoized,
)


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
