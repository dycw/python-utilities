from __future__ import annotations

from asyncio import sleep

from loguru import logger

from utilities.functions import is_not_none
from utilities.loguru import LogLevel, log

# test entry sync


@log
def func_test_entry_sync_inc(x: int, /) -> int:
    return x + 1


@log
def func_test_entry_sync_dec(x: int, /) -> int:
    return x - 1


@log
def func_test_entry_sync_inc_and_dec(x: int, /) -> tuple[int, int]:
    return func_test_entry_sync_inc(x), func_test_entry_sync_dec(x)


# test entry async


@log
async def func_test_entry_async_inc(x: int, /) -> int:
    await sleep(0.01)
    return x + 1


@log
async def func_test_entry_async_dec(x: int, /) -> int:
    await sleep(0.01)
    return x - 1


@log
async def func_test_entry_async_inc_and_dec(x: int, /) -> tuple[int, int]:
    return (await func_test_entry_async_inc(x), await func_test_entry_async_dec(x))


# test entry disabled


def func_test_entry_disabled(x: int, /) -> int:
    return x + 1


# test custom level


@log(entry=LogLevel.INFO)
def func_test_entry_custom_level(x: int, /) -> int:
    return x + 1


# test completion


@log(exit_=LogLevel.INFO)
def func_test_exit_sync(x: int, /) -> int:
    logger.info("Starting")
    return x + 1


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
