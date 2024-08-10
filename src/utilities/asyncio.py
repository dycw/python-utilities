from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Iterable
from itertools import groupby
from typing import TYPE_CHECKING, Any, TypeGuard, TypeVar, cast, overload

from utilities.typing import SupportsRichComparison

if TYPE_CHECKING:
    from collections.abc import Callable


_T = TypeVar("_T")
_U = TypeVar("_U")
_MaybeAsyncIterable = Iterable[_T] | AsyncIterable[_T]
_MaybeAwaitable = _T | Awaitable[_T]
_IterableLike = _MaybeAwaitable[_MaybeAsyncIterable[_T]]
_TSupportsRichComparison = TypeVar(
    "_TSupportsRichComparison", bound=SupportsRichComparison
)


@overload
async def groupby_async(
    iterable: _IterableLike[_T], /, *, key: None = None
) -> AsyncIterator[tuple[_T, list[_T]]]: ...
@overload
async def groupby_async(
    iterable: _IterableLike[_T], /, *, key: Callable[[_T], _MaybeAwaitable[_U]]
) -> AsyncIterator[tuple[_U, list[_T]]]: ...
async def groupby_async(
    iterable: _IterableLike[_T],
    /,
    *,
    key: Callable[[_T], _MaybeAwaitable[_U]] | None = None,
) -> AsyncIterator[tuple[_T, list[_T]]] | AsyncIterator[tuple[_U, list[_T]]]:
    """Yield consecutive keys and groups (as lists)."""
    if key is None:
        as_list = await to_list(iterable)
        for k, group in groupby(as_list):
            yield k, list(group)


async def is_awaitable(obj: Any, /) -> TypeGuard[Awaitable[Any]]:
    """Check if an object is awaitable."""
    try:
        await obj
    except TypeError:
        return False
    return True


async def to_list(iterable: _IterableLike[_T], /) -> list[_T]:
    """Reify an asynchronous iterable as a list."""
    value = cast(_MaybeAsyncIterable[_T], await try_await(iterable))
    try:
        return [x async for x in cast(AsyncIterable[_T], value)]
    except TypeError:
        return list(cast(Iterable[_T], value))


async def to_set(iterable: _IterableLike[_T], /) -> set[_T]:
    """Reify an asynchronous iterable as a set."""
    value = cast(_MaybeAsyncIterable[_T], await try_await(iterable))
    try:
        return {x async for x in cast(AsyncIterable[_T], value)}
    except TypeError:
        return set(cast(Iterable[_T], value))


async def to_sorted(
    iterable: _IterableLike[_TSupportsRichComparison],
    /,
    *,
    key: Callable[[_TSupportsRichComparison], _MaybeAwaitable[SupportsRichComparison]]
    | None = None,
    reverse: bool = False,
) -> list[_TSupportsRichComparison]:
    """Convert."""
    as_list = await to_list(iterable)
    if key is None:
        return sorted(as_list, reverse=reverse)

    values = [cast(SupportsRichComparison, await try_await(key(e))) for e in as_list]
    sorted_pairs = sorted(zip(as_list, values, strict=True), key=lambda x: x[1])
    return [element for element, _ in sorted_pairs]


async def try_await(obj: Any, /) -> Any:
    """Try await a value from an object."""
    try:
        return await obj
    except TypeError:
        return obj


__all__ = ["is_awaitable", "to_list", "to_set", "to_sorted", "try_await"]
