from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING, Any

from pytest import mark, param

from utilities.asyncio import (
    _MaybeAsyncIterable,
    is_awaitable,
    to_list,
    to_set,
    to_sorted,
    try_await,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


def _yield_ints_sync() -> Iterator[int]:
    return iter([1, 3, 5, 2, 4])


async def _yield_ints_async() -> AsyncIterator[int]:
    for i in _yield_ints_sync():
        await sleep(0.01)
        yield i


class TestIsAwaitable:
    @mark.parametrize(
        ("obj", "expected"), [param(sleep(0.01), True), param(None, False)]
    )
    async def test_main(self, *, obj: Any, expected: bool) -> None:
        result = await is_awaitable(obj)
        assert result is expected


class TestToList:
    @mark.parametrize(
        "iterable", [param(_yield_ints_sync()), param(_yield_ints_async())]
    )
    async def test_main(self, *, iterable: _MaybeAsyncIterable[int]) -> None:
        result = await to_list(iterable)
        expected = [1, 3, 5, 2, 4]
        assert result == expected


class TestToSet:
    @mark.parametrize(
        "iterable", [param(_yield_ints_sync()), param(_yield_ints_async())]
    )
    async def test_main(self, *, iterable: _MaybeAsyncIterable[int]) -> None:
        result = await to_set(iterable)
        expected = set(range(1, 6))
        assert result == expected


class TestToSorted:
    @mark.parametrize(
        "iterable", [param(_yield_ints_sync()), param(_yield_ints_async())]
    )
    async def test_main(self, *, iterable: _MaybeAsyncIterable[int]) -> None:
        result = await to_sorted(iterable)
        expected = list(range(1, 6))
        assert result == expected

    @mark.parametrize(
        "iterable", [param(_yield_ints_sync()), param(_yield_ints_async())]
    )
    async def test_key_sync(self, *, iterable: _MaybeAsyncIterable[int]) -> None:
        result = await to_sorted(iterable, key=int)
        expected = list(range(1, 6))
        assert result == expected

    @mark.parametrize(
        "iterable", [param(_yield_ints_sync()), param(_yield_ints_async())]
    )
    async def test_key_async(self, *, iterable: _MaybeAsyncIterable[int]) -> None:
        async def key(n: int, /) -> int:
            await sleep(0.01)
            return n

        result = await to_sorted(iterable, key=key)
        expected = list(range(1, 6))
        assert result == expected

    @mark.parametrize(
        "iterable", [param(_yield_ints_sync()), param(_yield_ints_async())]
    )
    async def test_reverse(self, *, iterable: _MaybeAsyncIterable[int]) -> None:
        result = await to_sorted(iterable, reverse=True)
        expected = list(range(5, 0, -1))
        assert result == expected


class TestTryAwait:
    async def awaitable(self) -> None:
        async def not_async(*, value: bool) -> bool:
            await sleep(0.01)
            return not value

        result = await try_await(not_async(value=True))
        assert result is False

    async def test_non_awaitable(self) -> None:
        result = await try_await(None)
        assert result is None
