from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from itertools import chain
from typing import Any, TypeVar, cast

from utilities.traceback import trace

_F = TypeVar("_F", bound=Callable[..., Any])


def other_decorator(func: _F, /) -> _F:
    @wraps(func)
    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        return await func(*args, **kwargs)

    return cast("_F", wrapped)


@trace
async def func_decorated_async_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_decorated_async_second(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
async def func_decorated_async_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_decorated_async_third(a, b, *args, c=c, **kwargs)


@trace
@other_decorator
async def func_decorated_async_third(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_decorated_async_fourth(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
@other_decorator
async def func_decorated_async_fourth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_decorated_async_fifth(a, b, *args, c=c, **kwargs)


@other_decorator
@other_decorator
@trace
@other_decorator
@other_decorator
@other_decorator
async def func_decorated_async_fifth(
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
