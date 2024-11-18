from __future__ import annotations

from asyncio import sleep
from itertools import chain

from utilities.traceback import trace


@trace
async def func_async(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
    await sleep(0.01)
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result > 0, f"Result ({result}) must be positive"
    return result
