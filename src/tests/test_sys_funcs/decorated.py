from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from itertools import chain
from typing import Any, TypeVar, cast

from utilities.sys import trace

_F = TypeVar("_F", bound=Callable[..., Any])


def other_decorator(func: _F, /) -> _F:
    @wraps(func)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return cast(_F, wrapped)


@trace
def func_decorated_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    return func_decorated_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@other_decorator
@trace(above=1)
def func_decorated_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    return func_decorated_third(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@trace()
@other_decorator
def func_decorated_third(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    return func_decorated_fourth(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@other_decorator
@trace(above=1)
@other_decorator
def func_decorated_fourth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    return func_decorated_fifth(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@other_decorator
@other_decorator
@trace(above=2)
@other_decorator
@other_decorator
@other_decorator
def func_decorated_fifth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result
