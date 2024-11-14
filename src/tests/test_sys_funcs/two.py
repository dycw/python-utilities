from __future__ import annotations

from itertools import chain

from utilities.sys import trace


@trace
def func_two_first(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    return func_two_second(2 * a, 2 * b, *args, c=2 * c, **kwargs)


@trace
def func_two_second(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result
