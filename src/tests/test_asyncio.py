from __future__ import annotations

from asyncio import sleep
from dataclasses import dataclass
from itertools import repeat
from typing import TYPE_CHECKING, Any

from pytest import mark, param

from utilities.asyncio import (
    _MaybeAwaitableMaybeAsyncIterable,
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


@dataclass(frozen=True, kw_only=True)
class _Container:
    text: str


def _get_containers_sync() -> Iterable[_Container]:
    return (_Container(text=t) for t in _get_strs_sync())


async def _get_containers_async() -> Iterable[_Container]:
    return _get_containers_sync()


def _yield_containers_sync() -> Iterator[_Container]:
    return iter(_get_containers_sync())


async def _yield_containers_async() -> AsyncIterator[_Container]:
    for i in _get_containers_sync():
        yield i
        await sleep(0.01)


@mark.only
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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        groupby_async(iterable)
        as_list: list[tuple[str, list[str]]] = []
        async for k, v in await groupby_async(iterable):
            assert isinstance(k, str)
            assert isinstance(v, list)
            for v_i in v:
                assert isinstance(v_i, str)
            as_list.append((k, v))
        expected = [
            ("A", list(repeat("A", times=4))),
            ("B", list(repeat("B", times=3))),
            ("C", list(repeat("C", times=2))),
            ("D", list(repeat("D", times=1))),
            ("A", list(repeat("A", times=2))),
            ("B", list(repeat("B", times=2))),
        ]
        assert as_list == expected

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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = groupby_async(iterable, key=ord)
        as_list: list[tuple[int, list[str]]] = []
        async for k, v in await result:
            assert isinstance(k, int)
            assert isinstance(v, list)
            for v_i in v:
                assert isinstance(v_i, str)
            as_list.append((k, v))
        expected = [
            (65, list(repeat("A", times=4))),
            (66, list(repeat("B", times=3))),
            (67, list(repeat("C", times=2))),
            (68, list(repeat("D", times=1))),
            (65, list(repeat("A", times=2))),
            (66, list(repeat("B", times=2))),
        ]
        assert as_list == expected

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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await to_sorted(iterable)
        expected = sorted(_STRS)
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_containers_sync()),
            param(_get_containers_async()),
            param(_yield_containers_sync()),
            param(_yield_containers_async()),
        ],
    )
    async def test_key_sync(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[_Container]
    ) -> None:
        result = await to_sorted(iterable, key=lambda c: c.text)
        expected = [_Container(text=t) for t in sorted(_STRS)]
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_containers_sync()),
            param(_get_containers_async()),
            param(_yield_containers_sync()),
            param(_yield_containers_async()),
        ],
    )
    async def test_key_async(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[_Container]
    ) -> None:
        async def key(container: _Container, /) -> str:
            await sleep(0.01)
            return container.text

        result = await to_sorted(iterable, key=key)
        expected = [_Container(text=t) for t in sorted(_STRS)]
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
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
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
