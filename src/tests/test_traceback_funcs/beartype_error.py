from __future__ import annotations

from itertools import chain
from typing import Any, cast

from beartype import beartype

from utilities.traceback import trace


@trace
@beartype
def func_beartype_error_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    return func_beartype_error_second(a, b, *args, c, **kwargs)


@trace
@beartype
def func_beartype_error_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    return cast(Any, str(result))
