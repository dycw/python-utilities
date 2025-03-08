from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import (
    CancelledError,
    Lock,
    PriorityQueue,
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
from collections.abc import Hashable
from contextlib import suppress
from dataclasses import dataclass, field
from io import StringIO
from subprocess import PIPE
from sys import stderr, stdout
from typing import TYPE_CHECKING, Any, Generic, Self, TextIO, TypeVar, cast

from typing_extensions import override

from utilities.datetime import datetime_duration_to_float
from utilities.functions import ensure_int, ensure_not_none
from utilities.types import SupportsRichComparison

if TYPE_CHECKING:
    from asyncio import Timeout, _CoroutineLike
    from asyncio.subprocess import Process
    from collections.abc import Sequence
    from contextvars import Context
    from types import TracebackType

    from utilities.types import Duration

_T = TypeVar("_T")
_THashable = TypeVar("_THashable", bound=Hashable)
_TSupportsRichComparison = TypeVar(
    "_TSupportsRichComparison", bound=SupportsRichComparison
)


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


@dataclass(kw_only=True)
class QueueProcessor(ABC, Generic[_T]):
    """Process a set of items in a queue."""

    queue_type: type[Queue[_T]] = field(default=Queue, repr=False)
    queue_max_size: int | None = field(default=None, repr=False)
    _lock: Lock = field(init=False, repr=False)
    _queue: Queue[_T] = field(init=False, repr=False)
    _task: Task[None] | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._lock = Lock()
        self._queue = self.queue_type(
            maxsize=0 if self.queue_max_size is None else self.queue_max_size
        )
        self._task = None

    async def __aenter__(self) -> Self:
        """Start the server."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Stop the server."""
        _ = (exc_type, exc_value, traceback)
        await self.stop()

    def __del__(self) -> None:
        try:
            task = self._task
        except AttributeError:
            return
        if task is None:
            return
        with suppress(RuntimeError):
            _ = task.cancel()

    def __len__(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def enqueue(self, *items: _T) -> None:
        """Enqueue a set items."""
        for item in items:
            self._queue.put_nowait(item)

    @classmethod
    async def new(cls, *args: _T, **kwargs: Any) -> Self:
        """Create and start ."""
        self = cls(**kwargs)
        self.enqueue(*args)
        await self.start()
        return self

    async def run_until_empty(self) -> None:
        """Run the processor until the queue is empty."""
        while not self.empty():
            _ = await self._get_and_run()

    async def start(self) -> None:
        """Start the processor."""
        if self._task is None:
            self._task = create_task(self._loop())

    async def stop(self) -> None:
        """Stop the processor."""
        if self._task is None:
            return
        _ = self._task.cancel()
        await self.run_until_empty()
        with suppress(CancelledError):
            await self._task
        self._task = None

    async def _get_and_run(self) -> None:
        """Get the next item and run the processor."""
        (item,) = await self._get_items(max_size=1)
        await self._run(item)

    async def _get_items(self, *, max_size: int | None = None) -> Sequence[_T]:
        """Get items from the queue; if empty then wait."""
        try:
            return await get_items(self._queue, max_size=max_size, lock=self._lock)
        except RuntimeError as error:  # pragma: no cover
            if error.args[0] == "Event loop is closed":
                return []
            raise

    async def _get_items_nowait(self, *, max_size: int | None = None) -> Sequence[_T]:
        """Get items from the queue; no waiting."""
        return await get_items_nowait(self._queue, max_size=max_size, lock=self._lock)

    async def _loop(self, /) -> None:
        """Loop the processor."""
        while True:
            try:
                await self._get_and_run()
            except RuntimeError as error:  # pragma: no cover
                if error.args[0] == "Event loop is closed":
                    return
                raise

    @abstractmethod
    async def _run(self, item: _T, /) -> None:
        """Run the processor on the first item."""
        raise NotImplementedError(item)  # pragma: no cover


##


class UniquePriorityQueue(PriorityQueue[tuple[_TSupportsRichComparison, _THashable]]):
    """Priority queue with unique tasks."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._set: set[_THashable] = set()

    @override
    def _get(self) -> tuple[_TSupportsRichComparison, _THashable]:
        item = super()._get()
        _, value = item
        self._set.remove(value)
        return item

    @override
    def _put(self, item: tuple[_TSupportsRichComparison, _THashable]) -> None:
        _, value = item
        if value not in self._set:
            super()._put(item)
            self._set.add(value)


class UniqueQueue(Queue[_THashable]):
    """Queue with unique tasks."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._set: set[_THashable] = set()

    @override
    def _get(self) -> _THashable:
        item = super()._get()
        self._set.remove(item)
        return item

    @override
    def _put(self, item: _THashable) -> None:
        if item not in self._set:
            super()._put(item)
            self._set.add(item)


##


async def get_items(
    queue: Queue[_T], /, *, max_size: int | None = None, lock: Lock | None = None
) -> list[_T]:
    """Get items from a queue; if empty then wait."""
    items = [await queue.get()]
    max_size_use = None if max_size is None else (max_size - 1)
    if lock is None:
        items.extend(await get_items_nowait(queue, max_size=max_size_use))
    else:
        async with lock:
            items.extend(await get_items_nowait(queue, max_size=max_size_use))
    return items


async def get_items_nowait(
    queue: Queue[_T], /, *, max_size: int | None = None, lock: Lock | None = None
) -> list[_T]:
    """Get items from a queue; no waiting."""
    if lock is None:
        return _get_items_nowait_core(queue, max_size=max_size)
    async with lock:
        return _get_items_nowait_core(queue, max_size=max_size)


def _get_items_nowait_core(
    queue: Queue[_T], /, *, max_size: int | None = None
) -> list[_T]:
    """Get all the items from a queue; no waiting."""
    items: list[_T] = []
    if max_size is None:
        while True:
            try:
                items.append(queue.get_nowait())
            except QueueEmpty:
                break
    else:
        while len(items) < max_size:
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
    "UniquePriorityQueue",
    "UniqueQueue",
    "get_items",
    "get_items_nowait",
    "sleep_dur",
    "stream_command",
    "timeout_dur",
]
