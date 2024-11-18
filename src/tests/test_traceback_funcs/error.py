from __future__ import annotations

from utilities.traceback import trace


@trace
def func_error_sync(a: int, b: int, /) -> int:
    result = sum([a, b])
    assert result > 0, f"Result ({result}) must be positive"
    return result


@trace
async def func_error_async(a: int, b: int, /) -> int:
    result = sum([a, b])
    assert result > 0, f"Result ({result}) must be positive"
    return result
