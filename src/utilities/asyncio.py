from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import (
    CancelledError,
    Event,
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
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping
from contextlib import (
    AsyncExitStack,
    _AsyncGeneratorContextManager,
    asynccontextmanager,
    suppress,
)
from dataclasses import dataclass, field
from io import StringIO
from logging import getLogger
from subprocess import PIPE
from sys import stderr, stdout
from typing import (
    TYPE_CHECKING,
    Generic,
    NoReturn,
    Self,
    TextIO,
    TypeVar,
    assert_never,
    overload,
    override,
)

from utilities.datetime import MILLISECOND, MINUTE, SECOND, datetime_duration_to_float
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_int, ensure_not_none, get_class_name
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    MaybeCallableEvent,
    MaybeType,
    THashable,
    TSupportsRichComparison,
)

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
            except CancelledError:
                await self.stop()
                break
            except Exception as error:  # noqa: BLE001
                await self._run_failure(error)
                await sleep_dur(duration=self.sleep)
            else:
                await sleep_dur(duration=self.sleep)


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

    def _get_items_nowait(self, *, max_size: int | None = None) -> Sequence[_T]:
        """Get items from the queue; no waiting."""
        return get_items_nowait(self._queue, max_size=max_size)

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
            (item,) = self._get_items_nowait(max_size=1)
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


@dataclass(kw_only=True, unsafe_hash=True)
class InfiniteLooper(ABC, Generic[THashable]):
    """An infinite loop which can throw exceptions by setting events."""

    sleep_core: Duration = SECOND
    sleep_restart: Duration = MINUTE
    logger: str | None = None
    _events: Mapping[THashable, Event] = field(
        default_factory=dict, init=False, repr=False, hash=False
    )

    def __post_init__(self) -> None:
        self._events = {
            event: Event() for event, _ in self._yield_events_and_exceptions()
        }

    async def __call__(self) -> None:
        """Create a coroutine to run the looper."""
        loopers = list(self._yield_loopers())
        if len(loopers) == 0:
            return await self._run_looper()
        return await self._run_multiple_loopers(*loopers)

    async def _run_looper(self) -> None:
        """Run the looper by itself."""
        while True:
            try:
                self._reset_events()
                try:
                    await self._initialize()
                except Exception as error:  # noqa: BLE001
                    self._error_upon_initialize(error)
                    await sleep_dur(duration=self.sleep_restart)
                else:
                    while True:
                        try:
                            event = next(
                                key
                                for (key, value) in self._events.items()
                                if value.is_set()
                            )
                        except StopIteration:
                            await self._core()
                            await sleep_dur(duration=self.sleep_core)
                        else:
                            self._raise_error(event)
            except InfiniteLooperError:
                raise
            except Exception as error:  # noqa: BLE001
                self._error_upon_core(error)
                await sleep_dur(duration=self.sleep_restart)

    async def _run_multiple_loopers(self, *loopers: InfiniteLooper) -> None:
        """Run multiple loopers."""
        while True:
            self._reset_events()
            try:
                async with TaskGroup() as tg:
                    _ = tg.create_task(self._run_looper())
                    _ = [tg.create_task(looper()) for looper in loopers]
            except Exception as error:  # noqa: BLE001
                self._error_upon_core(error)  # pragma: no cover
                await sleep_dur(duration=self.sleep_restart)  # pragma: no cover

    async def _initialize(self) -> None:
        """Initialize the loop."""

    async def _core(self) -> None:
        """Run the core part of the loop."""

    def _error_upon_initialize(self, error: Exception, /) -> None:
        """Handle any errors upon initializing the looper."""
        if self.logger is not None:
            getLogger(name=self.logger).error(
                "Error initializing %r due to %s; sleeping for %s...",
                get_class_name(self),
                error,
                self.sleep_restart,
            )

    def _error_upon_core(self, error: Exception, /) -> None:
        """Handle any errors upon running the core function."""
        if self.logger is not None:
            getLogger(name=self.logger).error(
                "Error running core part of %r due to %s; sleeping for %s...",
                get_class_name(self),
                error,
                self.sleep_restart,
            )

    def _raise_error(self, event: THashable, /) -> NoReturn:
        """Raise the error corresponding to given event."""
        mapping = dict(self._yield_events_and_exceptions())
        error = mapping.get(event, InfiniteLooperError)
        raise error

    def _reset_events(self) -> None:
        """Reset the events."""
        self._events = {
            event: Event() for event, _ in self._yield_events_and_exceptions()
        }

    def _set_event(self, event: THashable, /) -> None:
        """Set the given event."""
        try:
            event_obj = self._events[event]
        except KeyError:
            raise InfiniteLooperError(event=event) from None
        event_obj.set()

    def _yield_events_and_exceptions(
        self,
    ) -> Iterator[tuple[THashable, MaybeType[BaseException]]]:
        """Yield the events & exceptions."""
        yield from []

    def _yield_loopers(self) -> Iterator[InfiniteLooper]:
        """Yield any other infinite loopers which must also be run."""
        yield from []


@dataclass(kw_only=True, slots=True)
class InfiniteLooperError(Exception):
    event: Hashable

    @override
    def __str__(self) -> str:
        return f"No event {self.event!r} found"


##


@dataclass(kw_only=True)
class InfiniteQueueLooper(InfiniteLooper[THashable], Generic[THashable, _T]):
    """An infinite loop which processes a queue."""

    queue_type: type[Queue[_T]] = field(default=Queue, repr=False)
    _queue: Queue[_T] = field(init=False)
    _current: Queue[_T] = field(init=False)

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self._queue = self.queue_type()
        self._current = self.queue_type()

    @override
    async def _core(self) -> None:
        """Run the core part of the loop."""
        items = await get_items(self._queue)
        _ = get_items_nowait(self._current)
        put_items_nowait(items, self._current)
        try:
            await self._process_items(*items)
        except Exception:
            put_items_nowait(items, self._queue)
            raise

    @abstractmethod
    async def _process_items(self, *items: _T) -> None:
        """Process the items."""

    def put_items_nowait(self, *items: _T) -> None:
        """Put items into the queue."""
        put_items_nowait(items, self._queue)


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


@overload
def get_event(*, event: MaybeCallableEvent) -> Event: ...
@overload
def get_event(*, event: None) -> None: ...
@overload
def get_event(*, event: Sentinel) -> Sentinel: ...
@overload
def get_event(*, event: MaybeCallableEvent | Sentinel) -> Event | Sentinel: ...
@overload
def get_event(
    *, event: MaybeCallableEvent | None | Sentinel = sentinel
) -> Event | None | Sentinel: ...
def get_event(
    *, event: MaybeCallableEvent | None | Sentinel = sentinel
) -> Event | None | Sentinel:
    """Get the event."""
    match event:
        case Event() | None | Sentinel():
            return event
        case Callable() as func:
            return get_event(event=func())
        case _ as never:
            assert_never(never)


##


async def get_items(queue: Queue[_T], /, *, max_size: int | None = None) -> list[_T]:
    """Get items from a queue; if empty then wait."""
    try:
        items = [await queue.get()]
    except RuntimeError as error:  # pragma: no cover
        if error.args[0] == "Event loop is closed":
            return []
        raise
    max_size_use = None if max_size is None else (max_size - 1)
    items.extend(get_items_nowait(queue, max_size=max_size_use))
    return items


def get_items_nowait(queue: Queue[_T], /, *, max_size: int | None = None) -> list[_T]:
    """Get items from a queue; no waiting."""
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


async def put_items(items: Iterable[_T], queue: Queue[_T], /) -> None:
    """Put items into a queue; if full then wait."""
    for item in items:
        await queue.put(item)


def put_items_nowait(items: Iterable[_T], queue: Queue[_T], /) -> None:
    """Put items into a queue; no waiting."""
    for item in items:
        queue.put_nowait(item)


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
    "InfiniteLooper",
    "InfiniteLooperError",
    "InfiniteQueueLooper",
    "QueueProcessor",
    "StreamCommandOutput",
    "UniquePriorityQueue",
    "UniqueQueue",
    "get_event",
    "get_items",
    "get_items_nowait",
    "put_items",
    "put_items_nowait",
    "sleep_dur",
    "stream_command",
    "timeout_dur",
]
