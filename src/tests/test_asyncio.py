from __future__ import annotations

from asyncio import sleep
from itertools import repeat
from typing import TYPE_CHECKING, Any

from pytest import mark, param

from utilities.asyncio import (
    _MaybeAwaitableMaybeAsynIterable,
    groupby_async,
    is_awaitable,
    to_list,
    to_set,
    to_sorted,
    try_await,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

_STRS = list("AAAABBBCCDAABB")


def _get_strs_sync() -> Iterable[str]:
    return iter(_STRS)


async def _get_strs_async() -> Iterable[str]:
    return _get_strs_sync()


def _yield_strs_sync() -> Iterator[str]:
    return iter(_get_strs_sync())


async def _yield_strs_async() -> AsyncIterator[str]:
    for i in _get_strs_sync():
        yield i
        await sleep(0.01)


class TestGroupbyAsync:
    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_main(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_list(groupby_async(iterable))
        expected = [
            ("A", list(repeat("A", times=4))),
            ("B", list(repeat("B", times=3))),
            ("C", list(repeat("C", times=2))),
            ("D", list(repeat("D", times=1))),
            ("A", list(repeat("A", times=2))),
            ("B", list(repeat("B", times=2))),
        ]
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_sync(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_list(groupby_async(iterable, key=ord))
        expected = [
            (65, list(repeat("A", times=4))),
            (66, list(repeat("B", times=3))),
            (67, list(repeat("C", times=2))),
            (68, list(repeat("D", times=1))),
            (65, list(repeat("A", times=2))),
            (66, list(repeat("B", times=2))),
        ]
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_async(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        async def key(text: str, /) -> int:
            await sleep(0.01)
            return ord(text)

        result = await to_list(groupby_async(iterable, key=key))
        expected = [
            (65, list(repeat("A", times=4))),
            (66, list(repeat("B", times=3))),
            (67, list(repeat("C", times=2))),
            (68, list(repeat("D", times=1))),
            (65, list(repeat("A", times=2))),
            (66, list(repeat("B", times=2))),
        ]
        assert result == expected


class TestIsAwaitable:
    @mark.parametrize(
        ("obj", "expected"), [param(sleep(0.01), True), param(None, False)]
    )
    async def test_main(self, *, obj: Any, expected: bool) -> None:
        result = await is_awaitable(obj)
        assert result is expected


class TestToList:
    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_main(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_list(iterable)
        assert result == _STRS


class TestToSet:
    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_main(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_set(iterable)
        assert result == set(_STRS)


class TestToSorted:
    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_main(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_sorted(iterable)
        expected = sorted(_STRS)
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_sync(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_sorted(iterable, key=ord)
        expected = sorted(_STRS)
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_async(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        async def key(text: str, /) -> int:
            await sleep(0.01)
            return ord(text)

        result = await to_sorted(iterable, key=key)
        expected = sorted(_STRS)
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_reverse(
        self, *, iterable: _MaybeAwaitableMaybeAsynIterable[str]
    ) -> None:
        result = await to_sorted(iterable, reverse=True)
        expected = sorted(_STRS, reverse=True)
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
