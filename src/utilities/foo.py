from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING, Any

from utilities.arq import Worker

if TYPE_CHECKING:
    from collections.abc import Sequence

    from utilities.types import CallableAwaitable


async def func(x: int, y: int, /) -> int:
    await sleep(0.01)
    return x + y


class Example(Worker):
    functions_unlifted: Sequence[CallableAwaitable[Any]] = [func]
