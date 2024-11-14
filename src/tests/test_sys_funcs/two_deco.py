from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from itertools import chain
from typing import Any, TypeVar, cast, overload

from utilities.sys import trace

_F = TypeVar("_F", bound=Callable[..., Any])


def other_deco(func: _F, /) -> _F:
    @wraps(func)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return cast(_F, wrapped)


@other_deco
@trace
def func_two_deco_before_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    return func_two_deco_before_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@other_deco
@trace
def func_two_deco_before_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result


@trace
@other_deco
def func_two_deco_after_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    return func_two_deco_after_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@trace
@other_deco
def func_two_deco_after_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result
