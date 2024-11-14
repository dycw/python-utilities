from __future__ import annotations

from utilities.sys import trace


@trace
def func_error(a: int, b: int, /, c: int = 0) -> int:
    result = sum([a, b, c])
    assert result > 0, f"Result ({result}) must be positive"
    return result
