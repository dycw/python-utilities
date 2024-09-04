from __future__ import annotations

from asyncio import sleep

from loguru import logger

from utilities.loguru import LogLevel, log, log_completion

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


@log(entry=LogLevel.INFO)
def func_test_entry_custom_level(x: int, /) -> int:
    return x + 1


@log(entry=None)
async def func_test_entry_disabled_async(x: int, /) -> int:
    await sleep(0.01)
    return x + 1


# test entry custom level


@log(entry=LogLevel.INFO)
def func_test_entry_custom_level(x: int, /) -> int:
    return x + 1


# test error


@log
def func_test_error_sync(x: int, /) -> int | None:
    if x % 2 == 0:
        return x + 1
    msg = f"Got an odd number {x}"
    raise ValueError(msg)

@log_completion
def comp_test_sync(x: int, /) -> int:
    logger.info("Starting")
    return x + 1


@log_completion
async def comp_test_async(x: int, /) -> int:
    logger.info("Starting")
    await sleep(0.01)
    return x + 1


@log_completion(level=LogLevel.WARNING)
def comp_test_custom_level(x: int, /) -> int:
    logger.info("Starting")
    return x + 1


@log_completion(level=LogLevel.WARNING)
def comp_test_nullable(x: int, /) -> int | None:
    logger.info("Starting")
    return (x + 1) if x % 2 == 0 else None
