from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from asyncio import (
    Event,
    PriorityQueue,
    Queue,
    QueueEmpty,
    Semaphore,
    StreamReader,
    Task,
    TaskGroup,
    create_subprocess_shell,
    sleep,
    timeout,
)
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from dataclasses import dataclass, field
from io import StringIO
from logging import getLogger
from subprocess import PIPE
from sys import stderr, stdout
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    NoReturn,
    TextIO,
    TypeVar,
    assert_never,
    overload,
    override,
)

from utilities.datetime import (
    MINUTE,
    SECOND,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    get_now,
    round_datetime,
)
from utilities.errors import repr_error
from utilities.functions import ensure_int, ensure_not_none, get_class_name
from utilities.reprlib import get_repr
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    Coroutine1,
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

    from utilities.types import Duration


_T = TypeVar("_T")


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


type _DurationOrEvery = Duration | tuple[Literal["every"], Duration]


@dataclass(kw_only=True, unsafe_hash=True)
class InfiniteLooper(ABC, Generic[THashable]):
    """An infinite loop which can throw exceptions by setting events."""

    sleep_core: _DurationOrEvery = SECOND
    sleep_restart: _DurationOrEvery = MINUTE
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
        coroutines = list(self._yield_coroutines())
        if len(coroutines) == 0:
            return await self._run_looper()
        return await self._run_looper_with_coroutines(*coroutines)

    async def _run_looper(self) -> None:
        """Run the looper by itself."""
        while True:
            try:
                self._reset_events()
                try:
                    await self._initialize()
                except Exception as error:  # noqa: BLE001
                    self._error_upon_initialize(error)
                    await self._run_sleep(self.sleep_restart)
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
                            await self._run_sleep(self.sleep_core)
                        else:
                            self._raise_error(event)
            except InfiniteLooperError:
                raise
            except Exception as error:  # noqa: BLE001
                self._error_upon_core(error)
                await self._run_sleep(self.sleep_restart)

    async def _run_looper_with_coroutines(
        self, *coroutines: Callable[[], Coroutine1[None]]
    ) -> None:
        """Run multiple loopers."""
        while True:
            self._reset_events()
            try:
                async with TaskGroup() as tg:
                    _ = tg.create_task(self._run_looper())
                    _ = [tg.create_task(c()) for c in coroutines]
            except ExceptionGroup as error:
                self._error_group_upon_coroutines(error)
                await self._run_sleep(self.sleep_restart)

    async def _initialize(self) -> None:
        """Initialize the loop."""

    async def _core(self) -> None:
        """Run the core part of the loop."""

    def _error_upon_initialize(self, error: Exception, /) -> None:
        """Handle any errors upon initializing the looper."""
        if self.logger is not None:
            getLogger(name=self.logger).error(
                "%r encountered %r whilst initializing; sleeping %s...",
                get_class_name(self),
                repr_error(error),
                self._sleep_restart_desc,
            )

    def _error_upon_core(self, error: Exception, /) -> None:
        """Handle any errors upon running the core function."""
        if self.logger is not None:
            getLogger(name=self.logger).error(
                "%r encountered %r; sleeping %s...",
                get_class_name(self),
                repr_error(error),
                self._sleep_restart_desc,
            )

    def _error_group_upon_coroutines(self, group: ExceptionGroup, /) -> None:
        """Handle any errors upon running the core function."""
        if self.logger is not None:
            errors = group.exceptions
            n = len(errors)
            msgs = [f"{get_class_name(self)!r} encountered {n} error(s):"]
            msgs.extend(
                f"- Error #{i}/{n}: {repr_error(e)}"
                for i, e in enumerate(errors, start=1)
            )
            msgs.append(f"Sleeping {self._sleep_restart_desc}...")
            getLogger(name=self.logger).error("\n".join(msgs))

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

    async def _run_sleep(self, sleep: _DurationOrEvery, /) -> None:
        """Sleep until the next part of the loop."""
        match sleep:
            case int() | float() | dt.timedelta() as duration:
                await sleep_dur(duration=duration)
            case "every", (int() | float() | dt.timedelta()) as duration:
                await sleep_until_rounded(duration)
            case _ as never:
                assert_never(never)

    @property
    def _sleep_restart_desc(self) -> str:
        """Get a description of the sleep until restart."""
        match self.sleep_restart:
            case int() | float() | dt.timedelta() as duration:
                timedelta = datetime_duration_to_timedelta(duration)
                return f"for {timedelta}"
            case "every", (int() | float() | dt.timedelta()) as duration:
                timedelta = datetime_duration_to_timedelta(duration)
                return f"until next {timedelta}"
            case _ as never:
                assert_never(never)

    def _set_event(self, event: THashable, /) -> None:
        """Set the given event."""
        try:
            event_obj = self._events[event]
        except KeyError:
            raise InfiniteLooperError(looper=self, event=event) from None
        event_obj.set()

    def _yield_coroutines(self) -> Iterator[Callable[[], Coroutine1[None]]]:
        """Yield any other coroutines which must also be run."""
        yield from []

    def _yield_events_and_exceptions(
        self,
    ) -> Iterator[tuple[THashable, MaybeType[BaseException]]]:
        """Yield the events & exceptions."""
        yield from []


@dataclass(kw_only=True, slots=True)
class InfiniteLooperError(Exception):
    looper: InfiniteLooper[Any]
    event: Hashable

    @override
    def __str__(self) -> str:
        return f"{get_class_name(self.looper)!r} does not have an event {self.event!r}"


##


@dataclass(kw_only=True)
class InfiniteQueueLooper(InfiniteLooper[THashable], Generic[THashable, _T]):
    """An infinite loop which processes a queue."""

    queue_type: type[Queue[_T]] = field(default=Queue, repr=False)
    _queue: Queue[_T] = field(init=False)

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self._queue = self.queue_type()

    def __len__(self) -> int:
        return self._queue.qsize()

    @override
    async def _core(self) -> None:
        """Run the core part of the loop."""
        items = await get_items(self._queue)
        try:
            await self._process_items(*items)
        except Exception as error:  # noqa: BLE001
            raise InfiniteQueueLooperError(
                looper=self, items=items, error=error
            ) from None

    @abstractmethod
    async def _process_items(self, *items: _T) -> None:
        """Process the items."""

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def put_items_nowait(self, *items: _T) -> None:
        """Put items into the queue."""
        put_items_nowait(items, self._queue)

    async def run_until_empty(self) -> None:
        """Run until the queue is empty."""
        while not self.empty():
            await self._process_items(*get_items_nowait(self._queue))

    @override
    def _error_upon_core(self, error: Exception, /) -> None:
        """Handle any errors upon running the core function."""
        if self.logger is not None:
            if isinstance(error, InfiniteQueueLooperError):
                getLogger(name=self.logger).error(
                    "%r encountered %s whilst processing %d item(s) %s; sleeping for %s...",
                    get_class_name(self),
                    repr_error(error.error),
                    len(error.items),
                    get_repr(error.items),
                    self.sleep_restart,
                )
            else:
                super()._error_upon_core(error)  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class InfiniteQueueLooperError(Exception, Generic[_T]):
    looper: InfiniteQueueLooper[Any, Any]
    items: Sequence[_T]
    error: Exception

    @override
    def __str__(self) -> str:
        return f"{get_class_name(self.looper)!r} encountered {repr_error(self.error)} whilst processing {len(self.items)} item(s): {get_repr(self.items)}"


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


async def sleep_until(datetime: dt.datetime, /) -> None:
    """Sleep until a given time."""
    await sleep_dur(duration=datetime - get_now())


##


async def sleep_until_rounded(
    duration: Duration, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> None:
    """Sleep until a rounded time; accepts durations."""
    datetime = round_datetime(
        get_now(), duration, mode="ceil", rel_tol=rel_tol, abs_tol=abs_tol
    )
    await sleep_until(datetime)


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
    "EnhancedTaskGroup",
    "InfiniteLooper",
    "InfiniteLooperError",
    "InfiniteQueueLooper",
    "InfiniteQueueLooperError",
    "StreamCommandOutput",
    "UniquePriorityQueue",
    "UniqueQueue",
    "get_event",
    "get_items",
    "get_items_nowait",
    "put_items",
    "put_items_nowait",
    "sleep_dur",
    "sleep_until",
    "sleep_until_rounded",
    "stream_command",
    "timeout_dur",
]
