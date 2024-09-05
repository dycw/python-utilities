from __future__ import annotations

from asyncio import sleep
from functools import wraps
from typing import TYPE_CHECKING

from loguru import logger

from utilities.functions import is_not_none
from utilities.loguru import LogLevel, log

if TYPE_CHECKING:
    from collections.abc import Callable


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


@log(entry=None)
def func_test_entry_disabled_sync(x: int, /) -> int:
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


class Remainder1Error(Exception): ...


class Remainder2Error(Exception): ...


@log
def func_test_error_sync(x: int, /) -> int | None:
    if x % 2 == 0:
        return x + 1
    msg = f"Got an odd number {x}"
    raise ValueError(msg)


@log
def func_test_error_chain_outer_sync(x: int, /) -> int | None:
    try:
        return func_test_error_chain_inner_sync(x)
    except Remainder1Error:
        return x + 1


@log(error_expected=Remainder1Error)
def func_test_error_chain_inner_sync(x: int, /) -> int | None:
    if x % 3 == 0:
        return x + 1
    if x % 3 == 1:
        msg = "Got a remainder of 1"
        raise Remainder1Error(msg)
    msg = "Got a remainder of 2"
    raise Remainder2Error(msg)


@log
async def func_test_error_async(x: int, /) -> int | None:
    await sleep(0.01)
    if x % 2 == 0:
        return x + 1
    msg = f"Got an odd number {x}"
    raise ValueError(msg)


@log
async def func_test_error_chain_outer_async(x: int, /) -> int | None:
    try:
        return await func_test_error_chain_inner_async(x)
    except Remainder1Error:
        return x + 1


@log(error_expected=Remainder1Error)
async def func_test_error_chain_inner_async(x: int, /) -> int | None:
    if x % 3 == 0:
        return x + 1
    if x % 3 == 1:
        msg = "Got a remainder of 1"
        raise Remainder1Error(msg)
    msg = "Got a remainder of 2"
    raise Remainder2Error(msg)


# test exit


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
