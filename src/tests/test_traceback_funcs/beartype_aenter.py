from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, Self

from beartype import beartype

from utilities.traceback import trace

if TYPE_CHECKING:
    from types import TracebackType


@trace
async def func_beartype_aenter(
    a: int, b: int, /, *args: int, c: int = 0, **kwargs: int
) -> int:
    a *= 2
    b *= 2
    args = tuple(2 * arg for arg in args)
    c *= 2
    kwargs = {k: 2 * v for k, v in kwargs.items()}
    result = sum(chain([a, b], args, [c], kwargs.values()))
    async with ClassBeartypeAsyncContextManager():
        pass
    assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
    return result


@beartype
class ClassBeartypeAsyncContextManager:
    @trace
    async def __aenter__(self) -> Self:
        return self

    @trace
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        _ = (exc_type, exc_value, traceback)
