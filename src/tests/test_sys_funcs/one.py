from __future__ import annotations

from itertools import chain

from utilities.sys import trace


@trace
def func_one(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result >= 0
    return result
