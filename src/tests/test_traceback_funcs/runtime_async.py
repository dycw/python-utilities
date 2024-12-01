from __future__ import annotations

from asyncio import sleep
from contextlib import contextmanager
from itertools import chain
from typing import TYPE_CHECKING

from utilities.traceback import trace

if TYPE_CHECKING:
    from collections.abc import Iterator

_enable = True


def _get_enable() -> bool:
    return _enable


@contextmanager
def disable_trace_for_func_runtime_async() -> Iterator[None]:
    global _enable  # noqa: PLW0603
    _enable = False
    try:
        yield
    finally:
        _enable = True


@trace(runtime=_get_enable)
async def func_runtime_async(
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
