from __future__ import annotations

from itertools import chain


def func_zero(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result
