from __future__ import annotations

from asyncio import sleep
from functools import wraps
from typing import TYPE_CHECKING

from loguru import logger
from tenacity import retry, wait_fixed

from utilities.functions import is_not_none
from utilities.loguru import LogLevel, log
from utilities.tenacity import before_sleep_log

if TYPE_CHECKING:
    from collections.abc import Callable


@log(exit_=LogLevel.INFO)
async def func_test_exit_async(x: int, /) -> int:
    logger.info("Starting")
    await sleep(0.01)
    return x + 1


@log(exit_=LogLevel.WARNING)
def func_test_exit_custom_level(x: int, /) -> int:
    logger.info("Starting")
    return x + 1


@log(exit_=LogLevel.INFO, exit_predicate=is_not_none)
def func_test_exit_predicate(x: int, /) -> int | None:
    logger.info("Starting")
    return (x + 1) if x % 2 == 0 else None


# test decorated


def make_new(func: Callable[[int], int], /) -> Callable[[int], tuple[int, int]]:
    @wraps(func)
    @log
    def wrapped(x: int, /) -> tuple[int, int]:
        first = func(x)
        second = func(x + 1)
        return first, second

    return wrapped


@make_new
@log(depth=3)
def func_test_decorated(x: int, /) -> int:
    logger.info(f"Starting x={x}")
    return x + 1


# test tenacity


_counter = 0


@retry(wait=wait_fixed(0.01), before_sleep=before_sleep_log())
def func_test_before_sleep_log() -> int:
    global _counter  # noqa: PLW0603
    _counter += 1
    if _counter >= 3:
        return _counter
    raise ValueError(_counter)
