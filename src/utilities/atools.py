from __future__ import annotations

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
_MEMOIZED_FUNCS: dict[tuple[_AsyncFunc, dt.timedelta], _AsyncFunc] = {}


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
    try:
        memoized = _MEMOIZED_FUNCS[(func, timedelta)]
    except KeyError:
        memoized = _MEMOIZED_FUNCS[(func, timedelta)] = memoize(duration=refresh)(func)
    return await memoized(*args, **kwargs)


__all__ = ["call_memoized"]
