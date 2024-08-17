from __future__ import annotations

from asyncio import timeout
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Iterable, Sequence
from itertools import groupby
from typing import TYPE_CHECKING, Any, TypeGuard, TypeVar, cast, overload

from utilities.datetime import duration_to_float
from utilities.typing import SupportsRichComparison

if TYPE_CHECKING:
    from asyncio import Timeout
    from collections.abc import Callable

    from utilities.types import Duration


_T = TypeVar("_T")
_U = TypeVar("_U")
_MaybeAsyncIterable = Iterable[_T] | AsyncIterable[_T]
_MaybeAwaitable = _T | Awaitable[_T]
_MaybeAwaitableMaybeAsyncIterable = _MaybeAwaitable[_MaybeAsyncIterable[_T]]
_TSupportsRichComparison = TypeVar(
    "_TSupportsRichComparison", bound=SupportsRichComparison
)


@overload
async def groupby_async(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T], /, *, key: None = None
) -> AsyncIterator[tuple[_T, Sequence[_T]]]: ...
@overload
async def groupby_async(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[_U]],
) -> AsyncIterator[tuple[_U, Sequence[_T]]]: ...
async def groupby_async(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[_U]] | None = None,
) -> AsyncIterator[tuple[_T, Sequence[_T]]] | AsyncIterator[tuple[_U, Sequence[_T]]]:
    """Yield consecutive keys and groups (as lists)."""
    as_list = await to_list(iterable)
    if key is None:

        async def iterator1() -> AsyncIterator[tuple[_T, Sequence[_T]]]:
            for k, group in groupby(as_list):
                yield k, list(group)

        return iterator1()

    async def iterator2() -> AsyncIterator[tuple[_U, Sequence[_T]]]:
        pairs = [(cast(_U, await try_await(key(e))), e) for e in as_list]
        for k, pairs_group in groupby(pairs, key=lambda x: x[0]):
            group = [v for _, v in pairs_group]
            yield k, group

    return iterator2()


@overload
async def groupby_async_list(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T], /, *, key: None = None
) -> Sequence[tuple[_T, Sequence[_T]]]: ...
@overload
async def groupby_async_list(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[_U]],
) -> Sequence[tuple[_U, Sequence[_T]]]: ...
async def groupby_async_list(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[_U]] | None = None,
) -> Sequence[tuple[_T, Sequence[_T]]] | Sequence[tuple[_U, Sequence[_T]]]:
    """Yield consecutive keys and groups (as lists)."""
    if key is None:
        iterator = await groupby_async(iterable)
        return await to_list(iterator)
    iterator = await groupby_async(iterable, key=key)
    return await to_list(iterator)


async def is_awaitable(obj: Any, /) -> TypeGuard[Awaitable[Any]]:
    """Check if an object is awaitable."""
    try:
        await obj
    except TypeError:
        return False
    return True


async def to_list(iterable: _MaybeAwaitableMaybeAsyncIterable[_T], /) -> list[_T]:
    """Reify an asynchronous iterable as a list."""
    value = cast(_MaybeAsyncIterable[_T], await try_await(iterable))
    try:
        return [x async for x in cast(AsyncIterable[_T], value)]
    except TypeError:
        return list(cast(Iterable[_T], value))


async def to_set(iterable: _MaybeAwaitableMaybeAsyncIterable[_T], /) -> set[_T]:
    """Reify an asynchronous iterable as a set."""
    value = cast(_MaybeAsyncIterable[_T], await try_await(iterable))
    try:
        return {x async for x in cast(AsyncIterable[_T], value)}
    except TypeError:
        return set(cast(Iterable[_T], value))


@overload
async def to_sorted(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_TSupportsRichComparison],
    /,
    *,
    key: None = None,
    reverse: bool = ...,
) -> list[_TSupportsRichComparison]: ...
@overload
async def to_sorted(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[SupportsRichComparison]],
    reverse: bool = ...,
) -> list[_T]: ...
async def to_sorted(
    iterable: _MaybeAwaitableMaybeAsyncIterable[_T]
    | _MaybeAwaitableMaybeAsyncIterable[_TSupportsRichComparison],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[SupportsRichComparison]] | None = None,
    reverse: bool = False,
) -> list[_T] | list[_TSupportsRichComparison]:
    """Convert."""
    as_list = await to_list(iterable)
    if key is None:
        as_list = cast(list[_TSupportsRichComparison], as_list)
        return sorted(as_list, reverse=reverse)

    as_list = cast(list[_T], as_list)
    values = [cast(SupportsRichComparison, await try_await(key(e))) for e in as_list]
    sorted_pairs = sorted(zip(as_list, values, strict=True), key=lambda x: x[1])
    return [element for element, _ in sorted_pairs]


def timeout_dur(*, duration: Duration | None = None) -> Timeout:
    """Timeout context manager which accepts durations."""
    delay = None if duration is None else duration_to_float(duration)
    return timeout(delay)


async def try_await(obj: Any, /) -> Any:
    """Try await a value from an object."""
    try:
        return await obj
    except TypeError:
        return obj


__all__ = [
    "groupby_async",
    "groupby_async_list",
    "is_awaitable",
    "timeout_dur",
    "to_list",
    "to_set",
    "to_sorted",
    "try_await",
]
