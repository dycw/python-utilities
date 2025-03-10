from __future__ import annotations

from asyncio import Lock
from collections.abc import Callable
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from atools import memoize

from utilities.datetime import datetime_duration_to_timedelta
from utilities.types import Coroutine1

if TYPE_CHECKING:
    import datetime as dt

    from utilities.types import Duration


_P = ParamSpec("_P")
_R = TypeVar("_R")
_AsyncFunc = Callable[_P, Coroutine1[_R]]
type _Key = tuple[_AsyncFunc, dt.timedelta]
_MEMOIZED_FUNCS: dict[_Key, _AsyncFunc] = {}
_LOCKS: dict[_Key, Lock] = {}


async def call_memoized(
    func: _AsyncFunc[_P, _R],
    refresh: Duration | None = None,
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _R:
    """Call an asynchronous function, with possible memoization."""
    if refresh is None:
        return await func(*args, **kwargs)
    timedelta = datetime_duration_to_timedelta(refresh)
    key: _Key = (func, timedelta)
    try:
        lock = _LOCKS[key]
    except KeyError:
        lock = _LOCKS[key] = Lock()
    try:
        memoized = _MEMOIZED_FUNCS[key]
    except KeyError:
        memoized = _MEMOIZED_FUNCS[(key)] = memoize(duration=refresh)(func)
    async with lock:
        return await memoized(*args, **kwargs)


__all__ = ["call_memoized"]
