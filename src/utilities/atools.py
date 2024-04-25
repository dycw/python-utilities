from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast, overload

from atools import memoize as _memoize

if TYPE_CHECKING:
    import datetime as dt

_P = ParamSpec("_P")
_R = TypeVar("_R")
_AsyncFunc = Callable[_P, Awaitable[_R]]


@overload
def memoize(
    func: _AsyncFunc[_P, _R], /, *, duration: None = ...
) -> _AsyncFunc[_P, _R]: ...
@overload
def memoize(
    func: None = ..., /, *, duration: float | dt.timedelta | None = ...
) -> Callable[[_AsyncFunc[_P, _R]], _AsyncFunc[_P, _R]]: ...


def memoize(
    func: _AsyncFunc[_P, _R] | None = None,
    /,
    *,
    duration: float | dt.timedelta | None = None,
) -> _AsyncFunc[_P, _R] | Callable[[_AsyncFunc[_P, _R]], _AsyncFunc[_P, _R]]:
    return cast(Any, _memoize(func, duration=duration))


__all__ = ["memoize"]
