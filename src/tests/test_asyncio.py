from __future__ import annotations

from asyncio import sleep
from dataclasses import dataclass
from functools import partial
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any, ClassVar

from hypothesis import given
from pytest import mark, param, raises

from utilities.asyncio import (
    ReduceAsyncError,
    _MaybeAwaitableMaybeAsyncIterable,
    groupby_async,
    groupby_async_list,
    is_awaitable,
    reduce_async,
    timeout_dur,
    to_list,
    to_set,
    to_sorted,
    try_await,
)
from utilities.hypothesis import durations

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator, Sequence

    from utilities.types import Duration

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


async def _ord_async(text: str, /) -> int:
    await sleep(0.01)
    return ord(text)


class TestGroupbyAsync:
    exp_no_key: ClassVar[list[tuple[str, list[str]]]] = [
        ("A", list(repeat("A", times=4))),
        ("B", list(repeat("B", times=3))),
        ("C", list(repeat("C", times=2))),
        ("D", list(repeat("D", times=1))),
        ("A", list(repeat("A", times=2))),
        ("B", list(repeat("B", times=2))),
    ]
    exp_with_key: ClassVar[list[tuple[int, list[str]]]] = [
        (65, list(repeat("A", times=4))),
        (66, list(repeat("B", times=3))),
        (67, list(repeat("C", times=2))),
        (68, list(repeat("D", times=1))),
        (65, list(repeat("A", times=2))),
        (66, list(repeat("B", times=2))),
    ]

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_no_key(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = groupby_async(iterable)
        as_list: list[tuple[str, list[str]]] = []
        async for k, v in await result:
            assert isinstance(k, str)
            assert isinstance(v, list)
            for v_i in v:
                assert isinstance(v_i, str)
            as_list.append((k, v))
        assert as_list == self.exp_no_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_no_key_as_list(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await groupby_async_list(iterable)
        assert result == self.exp_no_key

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
            assert all(isinstance(v_i, str) for v_i in v)
            as_list.append((k, v))
        assert as_list == self.exp_with_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_sync_list(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await groupby_async_list(iterable, key=ord)
        assert result == self.exp_with_key

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
        result = groupby_async(iterable, key=_ord_async)
        as_list: list[tuple[int, list[str]]] = []
        async for k, v in await result:
            assert isinstance(k, int)
            assert isinstance(v, list)
            assert all(isinstance(v_i, str) for v_i in v)
            as_list.append((k, v))
        assert as_list == self.exp_with_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_async_list(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await groupby_async_list(iterable, key=_ord_async)
        assert result == self.exp_with_key


class TestIsAwaitable:
    @mark.parametrize(
        ("obj", "expected"), [param(sleep(0.01), True), param(None, False)]
    )
    async def test_main(self, *, obj: Any, expected: bool) -> None:
        result = await is_awaitable(obj)
        assert result is expected


class TestReduceAsync:
    async def test_no_initial(self) -> None:
        async def add(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        result = await reduce_async(add, [1, 2, 3])
        assert result == 6

    async def test_no_initial_with_partial(self) -> None:
        async def add(x: int, y: int, /, *, z: int) -> int:
            await sleep(0.01)
            return x + y + z

        result = await reduce_async(partial(add, z=1), [1, 2, 3])
        assert result == 8

    async def test_with_initial(self) -> None:
        async def collect(x: Iterable[int], y: int, /) -> Sequence[int]:
            await sleep(0.01)
            return list(chain(x, [y]))

        result = await reduce_async(collect, [1, 2, 3], initial=[])
        assert result == [1, 2, 3]

    async def test_with_initial_with_partial(self) -> None:
        async def collect(x: Iterable[int], y: int, /, *, z: int) -> Sequence[int]:
            await sleep(0.01)
            return list(chain(x, [y, z]))

        result = await reduce_async(partial(collect, z=0), [1, 2, 3], initial=[])
        assert result == [1, 0, 2, 0, 3, 0]

    async def test_empty(self) -> None:
        async def add(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        with raises(ReduceAsyncError, match="Empty iterable .* with no initial value"):
            _ = await reduce_async(add, [])


class TestTimeoutDur:
    @given(duration=durations())
    async def test_main(self, *, duration: Duration) -> None:
        async with timeout_dur(duration=duration):
            pass


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
    async def test_maybe_awaitable(self) -> None:
        from utilities.random import SYSTEM_RANDOM

        if SYSTEM_RANDOM.random() <= 0.5:

            async def func(*, value: bool) -> bool:  # pyright: ignore[reportRedeclaration]
                await sleep(0.01)
                return not value

        else:

            def func(*, value: bool) -> bool:
                return not value

        result = await try_await(func(value=True))
        assert result is False

    async def test_awaitable(self) -> None:
        async def func(*, value: bool) -> bool:
            await sleep(0.01)
            return not value

        result = await try_await(func(value=True))
        assert result is False

    async def test_sync(self) -> None:
        def func(*, value: bool) -> bool:
            return not value

        result = await try_await(func(value=True))
        assert result is False
