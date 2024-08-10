from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Iterable
from typing import TYPE_CHECKING, Any, TypeGuard, TypeVar, cast

from utilities.typing import SupportsRichComparison

if TYPE_CHECKING:
    from collections.abc import Callable


_T = TypeVar("_T")
_MaybeAsyncIterable = Iterable[_T] | AsyncIterable[_T]
_MaybeAwaitable = _T | Awaitable[_T]
_TSupportsRichComparison = TypeVar(
    "_TSupportsRichComparison", bound=SupportsRichComparison
)


async def is_awaitable(obj: Any, /) -> TypeGuard[Awaitable[Any]]:
    """Check if an object is awaitable."""
    try:
        await obj
    except TypeError:
        return False
    return True


async def to_list(iterable: _MaybeAsyncIterable[_T], /) -> list[_T]:
    """Reify an asynchronous iterable as a list."""
    try:
        return [x async for x in cast(AsyncIterable[_T], iterable)]
    except TypeError:
        return list(cast(Iterable[_T], iterable))


async def to_set(iterable: _MaybeAsyncIterable[_T], /) -> set[_T]:
    """Reify an asynchronous iterable as a set."""
    try:
        return {x async for x in cast(AsyncIterable[_T], iterable)}
    except TypeError:
        return set(cast(Iterable[_T], iterable))


async def to_sorted(
    iterable: _MaybeAsyncIterable[_TSupportsRichComparison],
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
