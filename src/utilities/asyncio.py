from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import (
    CancelledError,
    Event,
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
from contextlib import (
    AsyncExitStack,
    _AsyncGeneratorContextManager,
    asynccontextmanager,
    suppress,
)
from dataclasses import dataclass, field
from io import StringIO
from subprocess import PIPE
from sys import stderr, stdout
from typing import TYPE_CHECKING, Generic, Self, TextIO, TypeVar, override

from utilities.datetime import MILLISECOND, datetime_duration_to_float
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_int, ensure_not_none
from utilities.types import THashable, TSupportsRichComparison

if TYPE_CHECKING:
    from asyncio import _CoroutineLike
    from asyncio.subprocess import Process
    from collections.abc import AsyncIterator, Sequence
    from contextvars import Context
    from types import TracebackType

    from utilities.types import Duration

_T = TypeVar("_T")


##


@dataclass(kw_only=True)
class AsyncService(ABC):
    """A long-running, asynchronous service."""

    duration: Duration | None = None
    _await_upon_aenter: bool = field(default=True, init=False, repr=False)
    _event: Event = field(default_factory=Event, init=False, repr=False)
    _stack: AsyncExitStack = field(
        default_factory=AsyncExitStack, init=False, repr=False
    )
    _state: bool = field(default=False, init=False, repr=False)
    _task: Task[None] | None = field(default=None, init=False, repr=False)
    _depth: int = field(default=0, init=False, repr=False)

    async def __aenter__(self) -> Self:
        """Context manager entry."""
        if (self._task is None) and (self._depth == 0):
            _ = await self._stack.__aenter__()
            self._task = create_task(self._start_runner())
            if self._await_upon_aenter:
                with suppress(CancelledError):
                    await self._task
        elif (self._task is not None) and (self._depth >= 1):
            ...
        else:
            raise ImpossibleCaseError(  # pragma: no cover
                case=[f"{self._task=}", f"{self._depth=}"]
            )
        self._depth += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Context manager exit."""
        _ = (exc_type, exc_value, traceback)
        if (self._task is None) or (self._depth == 0):
            raise ImpossibleCaseError(  # pragma: no cover
                case=[f"{self._task=}", f"{self._depth=}"]
            )
        self._state = False
        self._depth -= 1
        if self._depth == 0:
            _ = await self._stack.__aexit__(exc_type, exc_value, traceback)
            await self.stop()
            with suppress(CancelledError):
                await self._task
            self._task = None

    @abstractmethod
    async def _start(self) -> None:
        """Start the service."""

    async def _start_runner(self) -> None:
        """Coroutine to start the service."""
        if self.duration is None:
            _ = await self._start()
            _ = await self._event.wait()
        else:
            try:
                async with timeout_dur(duration=self.duration):
                    _ = await self._start()
            except TimeoutError:
                await self.stop()

    async def stop(self) -> None:
        """Stop the service."""
        if self._task is None:
            raise ImpossibleCaseError(case=[f"{self._task=}"])  # pragma: no cover
        with suppress(CancelledError):
            _ = self._task.cancel()


##


@dataclass(kw_only=True)
class AsyncLoopingService(AsyncService):
    """A long-running, asynchronous service which loops a core function."""

    sleep: Duration = MILLISECOND
    _await_upon_aenter: bool = field(default=True, init=False, repr=False)

    @abstractmethod
    async def _run(self) -> None:
        """Run the core function once."""
        raise NotImplementedError  # pragma: no cover

    async def _run_failure(self, error: Exception, /) -> None:
        """Process the failure."""
        raise error

    @override
    async def _start(self) -> None:
        """Start the service, assuming no task is present."""
        while True:
            try:
                await self._run()
                await sleep_dur(duration=self.sleep)
            except CancelledError:
                await self.stop()
                break
            except Exception as error:  # noqa: BLE001
                await self._run_failure(error)


##


class EnhancedTaskGroup(TaskGroup):
    """Task group with enhanced features."""

    _semaphore: Semaphore | None
    _timeout: Duration | None
    _error: type[Exception]
    _timeout_cm: _AsyncGeneratorContextManager[None] | None

    @override
    def __init__(
        self,
        *,
        max_tasks: int | None = None,
        timeout: Duration | None = None,
        error: type[Exception] = TimeoutError,
    ) -> None:
        super().__init__()
        self._semaphore = None if max_tasks is None else Semaphore(max_tasks)
        self._timeout = timeout
        self._error = error
        self._timeout_cm = None

    @override
    def create_task(
        self,
        coro: _CoroutineLike[_T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> Task[_T]:
        if self._semaphore is None:
            coroutine = coro
        else:
            coroutine = self._wrap_with_semaphore(self._semaphore, coro)
        coroutine = self._wrap_with_timeout(coroutine)
        return super().create_task(coroutine, name=name, context=context)

    async def _wrap_with_semaphore(
        self, semaphore: Semaphore, coroutine: _CoroutineLike[_T], /
    ) -> _T:
        async with semaphore:
            return await coroutine

    async def _wrap_with_timeout(self, coroutine: _CoroutineLike[_T], /) -> _T:
        async with timeout_dur(duration=self._timeout, error=self._error):
            return await coroutine


##


@dataclass(kw_only=True)
class QueueProcessor(AsyncService, Generic[_T]):
    """Process a set of items in a queue."""

    queue_type: type[Queue[_T]] = field(default=Queue, repr=False)
    queue_max_size: int | None = field(default=None, repr=False)
    sleep: Duration = MILLISECOND
    _await_upon_aenter: bool = field(default=False, init=False, repr=False)
    _queue: Queue[_T] = field(init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self._queue = self.queue_type(
            maxsize=0 if self.queue_max_size is None else self.queue_max_size
        )

    def __len__(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def enqueue(self, *items: _T) -> None:
        """Enqueue a set items."""
        for item in items:
            self._queue.put_nowait(item)

    async def run_until_empty(self) -> None:
        """Run the processor until the queue is empty."""
        while not self.empty():
            await self._run()
            await sleep_dur(duration=self.sleep)

    async def _get_items_nowait(self, *, max_size: int | None = None) -> Sequence[_T]:
        """Get items from the queue; no waiting."""
        return await get_items_nowait(self._queue, max_size=max_size, lock=self._lock)

    @abstractmethod
    async def _process_item(self, item: _T, /) -> None:
        """Process the first item."""
        raise NotImplementedError(item)  # pragma: no cover

    async def _process_item_failure(self, item: _T, error: Exception, /) -> None:
        """Process the failure."""
        _ = item
        raise error

    async def _run(self) -> None:
        """Run the processer."""
        try:
            (item,) = await self._get_items_nowait(max_size=1)
        except ValueError:
            raise QueueEmpty from None
        try:
            await self._process_item(item)
        except Exception as error:  # noqa: BLE001
            await self._process_item_failure(item, error)

    @override
    async def _start(self) -> None:
        """Start the processor."""
        while True:
            try:
                await self._run()
            except QueueEmpty:
                await sleep_dur(duration=self.sleep)
            except CancelledError:
                await self.stop()
                break
            else:
                await sleep_dur(duration=self.sleep)

    @override
    async def stop(self) -> None:
        """Stop the processor."""
        await self.run_until_empty()
        await super().stop()


@dataclass(kw_only=True)
class ExceptionProcessor(QueueProcessor[Exception | type[Exception]]):
    """Raise an exception in a queue."""

    queue_max_size: int | None = field(default=1, repr=False)

    @override
    async def _process_item(self, item: Exception | type[Exception], /) -> None:
        """Run the processor on the first item."""
        raise item


##


class UniquePriorityQueue(PriorityQueue[tuple[TSupportsRichComparison, THashable]]):
    """Priority queue with unique tasks."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._set: set[THashable] = set()

    @override
    def _get(self) -> tuple[TSupportsRichComparison, THashable]:
        item = super()._get()
        _, value = item
        self._set.remove(value)
        return item

    @override
    def _put(self, item: tuple[TSupportsRichComparison, THashable]) -> None:
        _, value = item
        if value not in self._set:
            super()._put(item)
            self._set.add(value)


class UniqueQueue(Queue[THashable]):
    """Queue with unique tasks."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._set: set[THashable] = set()

    @override
    def _get(self) -> THashable:
        item = super()._get()
        self._set.remove(item)
        return item

    @override
    def _put(self, item: THashable) -> None:
        if item not in self._set:
            super()._put(item)
            self._set.add(item)


##


async def get_items(
    queue: Queue[_T], /, *, max_size: int | None = None, lock: Lock | None = None
) -> list[_T]:
    """Get items from a queue; if empty then wait."""
    try:
        items = [await queue.get()]
    except RuntimeError as error:  # pragma: no cover
        if error.args[0] == "Event loop is closed":
            return []
        raise
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


@asynccontextmanager
async def timeout_dur(
    *, duration: Duration | None = None, error: type[Exception] = TimeoutError
) -> AsyncIterator[None]:
    """Timeout context manager which accepts durations."""
    delay = None if duration is None else datetime_duration_to_float(duration)
    try:
        async with timeout(delay):
            yield
    except TimeoutError:
        raise error from None


__all__ = [
    "AsyncLoopingService",
    "AsyncService",
    "EnhancedTaskGroup",
    "ExceptionProcessor",
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
