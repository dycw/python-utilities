from __future__ import annotations

from functools import wraps
from itertools import chain
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from utilities.traceback import trace

if TYPE_CHECKING:
    from collections.abc import Callable


_P = ParamSpec("_P")
_R = TypeVar("_R")


def other_decorator(func: Callable[_P, _R], /) -> Callable[_P, _R]:
    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        return func(*args, **kwargs)

    return wrapped


@trace
def func_decorated_sync_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_sync_second(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
def func_decorated_sync_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_sync_third(a, b, *args, c=c, **kwargs)


@trace
@other_decorator
def func_decorated_sync_third(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_sync_fourth(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
@other_decorator
def func_decorated_sync_fourth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_sync_fifth(a, b, *args, c=c, **kwargs)


@other_decorator
@other_decorator
@trace
@other_decorator
@other_decorator
@other_decorator
def func_decorated_sync_fifth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
    return result
