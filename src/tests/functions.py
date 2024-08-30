from __future__ import annotations

from asyncio import sleep

from utilities.loguru import log_call


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
