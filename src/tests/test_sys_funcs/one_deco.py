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
def func_one_deco_before(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result


@trace
@other_deco
def func_one_deco_after(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result
