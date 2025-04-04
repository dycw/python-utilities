from __future__ import annotations

from asyncio import TaskGroup, sleep
from itertools import chain

from utilities.traceback import trace


@trace
async def func_task_group_one_first(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> None:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    async with TaskGroup() as tg:
        _ = tg.create_task(func_task_group_one_second(a, b, *args, c=c, **kwargs))


@trace
async def func_task_group_one_second(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    await sleep(0.01)
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
    return result
