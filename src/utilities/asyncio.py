from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import (
    Lock,
    Queue,
    QueueEmpty,
    Semaphore,
    StreamReader,
    Task,
    TaskGroup,
    create_subprocess_shell,
    create_task,
    sleep,
    timeout,
)
from contextlib import suppress
from dataclasses import dataclass, field
from io import StringIO
from subprocess import PIPE
from sys import stderr, stdout
from typing import TYPE_CHECKING, Any, Generic, TextIO, TypeVar, cast

from typing_extensions import override

from utilities.datetime import datetime_duration_to_float
from utilities.functions import ensure_int, ensure_not_none

if TYPE_CHECKING:
    from asyncio import Timeout, _CoroutineLike
    from asyncio.subprocess import Process
    from contextvars import Context

    from utilities.types import Duration

_T = TypeVar("_T")


class BoundedTaskGroup(TaskGroup):
    """Task group with an internal limiter."""

    _semaphore: Semaphore | None

    @override
    def __init__(self, *, max_tasks: int | None = None) -> None:
        super().__init__()
        self._semaphore = None if max_tasks is None else Semaphore(max_tasks)

    @override
    def create_task(
        self,
        coro: _CoroutineLike[_T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> Task[_T]:
        if self._semaphore is None:
            return super().create_task(coro, name=name, context=context)

        async def wrapped(semaphore: Semaphore, coro: _CoroutineLike[_T], /) -> _T:
            async with semaphore:
                return await cast(Any, coro)

        return super().create_task(
            wrapped(self._semaphore, coro), name=name, context=context
        )


##


@dataclass(kw_only=True, slots=True)
class QueueProcessor(ABC, Generic[_T]):
    """Process a set of items in a queue."""

    _queue: Queue[_T] = field(default_factory=Queue, repr=False)
    _running: bool = False
    _task: Task[None] = field(init=False)

    def __del__(self) -> None:
        with suppress(AttributeError, RuntimeError):  # pragma: no cover
            _ = self._task.cancel()

    async def run_forever(self) -> None:
        self._running = True
        self._task = create_task(self._loop())

    def enqueue(self, *items: _T) -> None:
        """Enqueue a set items."""
        if self._running:
            for item in items:
                self._queue.put_nowait(item)
        else:
            msg = "Process is not accepting any more tasks"
            raise ValueError(msg)

    async def stop(self) -> None:
        """Stop the processor."""
        self._running = False
        items = await get_items_nowait(self._queue)
        await self._run(*items)

    async def _loop(self, /) -> None:
        """Loop the processor."""
        while self._running:
            items = await get_items(self._queue)
            await self._run(*items)

    @abstractmethod
    async def _run(self, *items: _T) -> None:
        """Run the processor once."""
        raise NotImplementedError(*items)


##


async def get_items(queue: Queue[_T], /, *, lock: Lock | None = None) -> list[_T]:
    """Get all the items from a queue; if empty then wait."""
    items = [await queue.get()]
    items.extend(await get_items_nowait(queue, lock=lock))
    return items


async def get_items_nowait(
    queue: Queue[_T], /, *, lock: Lock | None = None
) -> list[_T]:
    """Get all the items from a queue; no waiting."""
    if lock is None:
        return _get_items_nowait_core(queue)
    async with lock:
        return _get_items_nowait_core(queue)


def _get_items_nowait_core(queue: Queue[_T], /) -> list[_T]:
    """Get all the items from a queue; no waiting."""
    items: list[_T] = []
    while True:
        try:
            items.append(queue.get_nowait())
        except QueueEmpty:
            break
    return items


##


async def sleep_dur(*, duration: Duration | None = None) -> None:
    """Sleep which accepts durations."""
    if duration is None:
        return
    await sleep(datetime_duration_to_float(duration))


##


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


##


def timeout_dur(*, duration: Duration | None = None) -> Timeout:
    """Timeout context manager which accepts durations."""
    delay = None if duration is None else datetime_duration_to_float(duration)
    return timeout(delay)


__all__ = [
    "BoundedTaskGroup",
    "QueueProcessor",
    "StreamCommandOutput",
    "get_items",
    "get_items_nowait",
    "sleep_dur",
    "stream_command",
    "timeout_dur",
]
