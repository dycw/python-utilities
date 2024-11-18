from __future__ import annotations

from itertools import chain

from beartype import beartype

from utilities.traceback import trace


@trace
@beartype
def func_recursive(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    if a <= 2:
        return func_recursive(a, b, *args, c=c, **kwargs)
    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
    return result
