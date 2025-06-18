from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from atools import memoize

from utilities.types import Coro

if TYPE_CHECKING:
    from whenever import TimeDelta


type _Key[**P, T] = tuple[Callable[P, Coro[T]], TimeDelta]
_MEMOIZED_FUNCS: dict[_Key, Callable[..., Coro[Any]]] = {}


async def call_memoized[**P, T](
    func: Callable[P, Coro[T]],
    refresh: TimeDelta | None = None,
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Call an asynchronous function, with possible memoization."""
    if refresh is None:
        return await func(*args, **kwargs)
    key: _Key = (func, refresh)
    memoized_func: Callable[P, Coro[T]]
    try:
        memoized_func = _MEMOIZED_FUNCS[key]
    except KeyError:
        memoized_func = _MEMOIZED_FUNCS[(key)] = memoize(duration=refresh.in_seconds())(
            func
        )
    return await memoized_func(*args, **kwargs)


__all__ = ["call_memoized"]
