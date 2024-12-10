from __future__ import annotations

from asyncio import (
    Queue,
    QueueEmpty,
    StreamReader,
    TaskGroup,
    create_subprocess_shell,
    sleep,
    timeout,
)
from collections.abc import AsyncIterable, Awaitable, Coroutine, Iterable
from dataclasses import dataclass
from io import StringIO
from re import search
from subprocess import PIPE
from sys import stderr, stdout
from typing import TYPE_CHECKING, Any, TextIO, TypeAlias, TypeGuard, TypeVar, cast

from utilities.datetime import duration_to_float
from utilities.functions import ensure_not_none
from utilities.iterables import OneError, one
from utilities.text import EnsureStrError, ensure_str
from utilities.types import ensure_int

if TYPE_CHECKING:
    from asyncio import Timeout
    from asyncio.subprocess import Process

    from utilities.types import Duration

_T = TypeVar("_T")
Coroutine1: TypeAlias = Coroutine[Any, Any, _T]
MaybeAwaitable: TypeAlias = _T | Awaitable[_T]
MaybeCoroutine1: TypeAlias = _T | Coroutine1[_T]
_MaybeAsyncIterable: TypeAlias = Iterable[_T] | AsyncIterable[_T]
_MaybeAwaitableMaybeAsyncIterable: TypeAlias = MaybeAwaitable[_MaybeAsyncIterable[_T]]


async def get_items(queue: Queue[_T], /) -> list[_T]:
    """Get all the items from a queue; if empty then wait."""
    items = [await queue.get()]
    items.extend(get_items_nowait(queue))
    return items


def get_items_nowait(queue: Queue[_T], /) -> list[_T]:
    """Get all the items from a queue; no waiting."""
    items: list[_T] = []
    while True:
        try:
            items.append(queue.get_nowait())
        except QueueEmpty:
            break
    return items


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


@dataclass(kw_only=True, slots=True)
class StreamCommandOutput:
    process: Process
    stdout: str
    stderr: str

    @property
    def return_code(self) -> int:
        return ensure_int(self.process.returncode)  # skipif-not-windows


async def stream_command(cmd: str, /) -> StreamCommandOutput:
    """Run a shell command asynchronously and stream its output in real time."""
    process = await create_subprocess_shell(  # skipif-not-windows
        cmd, stdout=PIPE, stderr=PIPE
    )
    proc_stdout = ensure_not_none(  # skipif-not-windows
        process.stdout, desc="process.stdout"
    )
    proc_stderr = ensure_not_none(  # skipif-not-windows
        process.stderr, desc="process.stderr"
    )
    ret_stdout = StringIO()  # skipif-not-windows
    ret_stderr = StringIO()  # skipif-not-windows
    async with TaskGroup() as tg:  # skipif-not-windows
        _ = tg.create_task(_stream_one(proc_stdout, stdout, ret_stdout))
        _ = tg.create_task(_stream_one(proc_stderr, stderr, ret_stderr))
    _ = await process.wait()  # skipif-not-windows
    return StreamCommandOutput(  # skipif-not-windows
        process=process, stdout=ret_stdout.getvalue(), stderr=ret_stderr.getvalue()
    )


async def _stream_one(
    input_: StreamReader, out_stream: TextIO, ret_stream: StringIO, /
) -> None:
    """Asynchronously read from a stream and write to the target output stream."""
    while True:  # skipif-not-windows
        line = await input_.readline()
        if not line:
            break
        decoded = line.decode()
        _ = out_stream.write(decoded)
        out_stream.flush()
        _ = ret_stream.write(decoded)


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
    "StreamCommandOutput",
    "is_awaitable",
    "sleep_dur",
    "stream_command",
    "timeout_dur",
    "to_list",
]
