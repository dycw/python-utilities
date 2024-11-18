from __future__ import annotations

from itertools import chain

from utilities.traceback import trace


@trace
def func_one(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
    return result
