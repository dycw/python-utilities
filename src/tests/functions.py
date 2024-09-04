from __future__ import annotations

from asyncio import sleep

from loguru import logger

from utilities.loguru import LogLevel, log_call, log_completion

# test sync


@log_call
def add_sync(x: int, y: int, /) -> int:
    return x + y


@log_call
def diff_sync(x: int, y: int, /) -> int:
    return x - y


@log_call
def diff_pairwise_then_add_sync(x: int, y: int, z: int, w: int, /) -> int:
    first = diff_sync(x, y)
    second = diff_sync(z, w)
    return add_sync(first, second)


# test async


@log_call
async def add_async(x: int, y: int, /) -> int:
    await sleep(0.01)
    return x + y


@log_call
async def diff_async(x: int, y: int, /) -> int:
    await sleep(0.01)
    return x - y


@log_call
async def diff_pairwise_then_add_async(x: int, y: int, z: int, w: int, /) -> int:
    first = await diff_async(x, y)
    second = await diff_async(z, w)
    return await add_async(first, second)


# test custom level


@log_call(level=LogLevel.INFO)
def add_sync_info(x: int, y: int, /) -> int:
    return x + y


# test completion


@log_completion
def add_sync_comp(x: int, y: int, /) -> int:
    logger.info("middle")
    return x + y


@log_completion
async def add_async_comp(x: int, y: int, /) -> int:
    logger.info("middle")
    await sleep(0.01)
    return x + y
