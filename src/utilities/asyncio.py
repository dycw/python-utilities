from __future__ import annotations

from asyncio import (
    StreamReader,
    create_subprocess_shell,
    create_task,
    gather,
    sleep,
    timeout,
)
from collections.abc import AsyncIterable, Awaitable, Coroutine, Iterable
from re import search
from subprocess import PIPE
from sys import stderr, stdout
from typing import TYPE_CHECKING, Any, TextIO, TypeGuard, TypeVar, cast

from utilities.datetime import duration_to_float
from utilities.functions import ensure_not_none
from utilities.iterables import OneError, one
from utilities.text import EnsureStrError, ensure_str

if TYPE_CHECKING:
    from asyncio import Timeout
    from asyncio.subprocess import Process

    from utilities.types import Duration

_T = TypeVar("_T")
Coroutine1 = Coroutine[Any, Any, _T]
MaybeAwaitable = _T | Awaitable[_T]
MaybeCoroutine1 = _T | Coroutine1[_T]
_MaybeAsyncIterable = Iterable[_T] | AsyncIterable[_T]
_MaybeAwaitableMaybeAsyncIterable = MaybeAwaitable[_MaybeAsyncIterable[_T]]


async def is_awaitable(obj: Any, /) -> TypeGuard[Awaitable[Any]]:
    """Check if an object is awaitable."""
    try:
        await obj
    except TypeError:
        return False
    return True


async def sleep_dur(*, duration: Duration | None = None) -> None:
    """Sleep which accepts durations."""
    if duration is None:
        return
    await sleep(duration_to_float(duration))


async def stream_command(cmd: str, /) -> Process:
    """Run a shell command asynchronously and stream its output in real time."""
    process = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    tasks = [
        create_task(_stream_one(ensure_not_none(process.stdout), stdout)),
        create_task(_stream_one(ensure_not_none(process.stderr), stderr)),
    ]
    _ = await process.wait()
    _ = await gather(*tasks)
    return process


async def _stream_one(input_: StreamReader, output: TextIO, /) -> None:
    """Asynchronously read from a stream and write to the target output stream."""
    while True:
        line = await input_.readline()
        if line:
            _ = output.write(line.decode())
            output.flush()
        else:
            break


def timeout_dur(*, duration: Duration | None = None) -> Timeout:
    """Timeout context manager which accepts durations."""
    delay = None if duration is None else duration_to_float(duration)
    return timeout(delay)


async def to_list(iterable: _MaybeAwaitableMaybeAsyncIterable[_T], /) -> list[_T]:
    """Reify an asynchronous iterable as a list."""
    value = await try_await(iterable)
    try:
        return [x async for x in cast(AsyncIterable[_T], value)]
    except TypeError:
        return list(cast(Iterable[_T], value))


async def try_await(obj: MaybeAwaitable[_T], /) -> _T:
    """Try await a value from an object."""
    try:
        return await cast(Awaitable[_T], obj)
    except TypeError as error:
        try:
            text = ensure_str(one(error.args))
        except (EnsureStrError, OneError):
            raise error from None
        if search("object .* can't be used in 'await' expression", text):
            return cast(_T, obj)
        raise


__all__ = [
    "Coroutine1",
    "MaybeAwaitable",
    "MaybeCoroutine1",
    "is_awaitable",
    "sleep_dur",
    "stream_command",
    "timeout_dur",
    "to_list",
]
