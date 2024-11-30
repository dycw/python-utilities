from __future__ import annotations

from asyncio import sleep
from collections.abc import Callable
from functools import wraps
from inspect import iscoroutinefunction
from itertools import chain
from typing import Any, TypeVar, cast

from utilities.traceback import trace

_F = TypeVar("_F", bound=Callable[..., Any])
dur = 0.01


def other_decorator(func: _F, /) -> _F:
    if not iscoroutinefunction(func):

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return cast(_F, wrapped_sync)

    @wraps(func)
    async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
        return await func(*args, **kwargs)

    return cast(_F, wrapped_async)


@trace
def func_decorated_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_second(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
def func_decorated_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_third(a, b, *args, c=c, **kwargs)


@trace
@other_decorator
def func_decorated_third(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_fourth(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
@other_decorator
def func_decorated_fourth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_decorated_fifth(a, b, *args, c=c, **kwargs)


@other_decorator
@other_decorator
@trace
@other_decorator
@other_decorator
@other_decorator
def func_decorated_fifth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result % 123456789 == 0, f"Result ({result}) must be divisible by 123456789"
    return result


###############################################################################


@trace
async def func_async_decorated_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    await sleep(dur)
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_async_decorated_second(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
async def func_async_decorated_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    await sleep(dur)
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_async_decorated_third(a, b, *args, c=c, **kwargs)


@trace
@other_decorator
async def func_async_decorated_third(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    await sleep(dur)
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_async_decorated_fourth(a, b, *args, c=c, **kwargs)


@other_decorator
@trace
@other_decorator
async def func_async_decorated_fourth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    await sleep(dur)
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return await func_async_decorated_fifth(a, b, *args, c=c, **kwargs)


@other_decorator
@other_decorator
@trace
@other_decorator
@other_decorator
@other_decorator
async def func_async_decorated_fifth(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    await sleep(dur)
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result % 123456789 == 0, f"Result ({result}) must be divisible by 123456789"
    return result
