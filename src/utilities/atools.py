from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar, cast

from atools import memoize as _memoize

_P = ParamSpec("_P")
_R = TypeVar("_R")
_AsyncFunc = Callable[_P, Awaitable[_R]]


def memoize(func: _AsyncFunc[_P, _R], /) -> _AsyncFunc[_P, _R]:
    return cast(Any, _memoize(func))


__all__ = ["memoize"]
