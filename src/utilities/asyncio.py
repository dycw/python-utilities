from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from asyncio import (
    CancelledError,
    Event,
    PriorityQueue,
    Queue,
    QueueEmpty,
    QueueFull,
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
    AbstractAsyncContextManager,
    AsyncExitStack,
    _AsyncGeneratorContextManager,
    asynccontextmanager,
    suppress,
)
from dataclasses import dataclass, field
from io import StringIO
from itertools import chain
from logging import getLogger
from subprocess import PIPE
from sys import stderr, stdout
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    NoReturn,
    Self,
    TextIO,
    TypeVar,
    assert_never,
    overload,
    override,
)

from typing_extensions import deprecated

from utilities.datetime import (
    MINUTE,
    SECOND,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    get_now,
    round_datetime,
)
from utilities.errors import ImpossibleCaseError, repr_error
from utilities.functions import ensure_int, ensure_not_none, get_class_name
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    Coroutine1,
    DurationOrEveryDuration,
    MaybeCallableEvent,
    MaybeType,
    THashable,
    TSupportsRichComparison,
)

if TYPE_CHECKING:
    from asyncio import _CoroutineLike
    from asyncio.subprocess import Process
    from collections import deque
    from collections.abc import AsyncIterator, Sequence
    from contextvars import Context
    from types import TracebackType

    from utilities.types import Duration


_T = TypeVar("_T")


class EnhancedQueue(Queue[_T]):
    """An asynchronous deque."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize=maxsize)
        self._finished: Event
        self._getters: deque[Any]
        self._putters: deque[Any]
        self._queue: deque[_T]
        self._unfinished_tasks: int

    @override
    @deprecated("Use `get_left`/`get_right` instead")
    async def get(self) -> _T:
        raise RuntimeError  # pragma: no cover

    @override
    @deprecated("Use `get_left_nowait`/`get_right_nowait` instead")
    def get_nowait(self) -> _T:
        raise RuntimeError  # pragma: no cover

    @override
    @deprecated("Use `put_left`/`put_right` instead")
    async def put(self, item: _T) -> None:
        raise RuntimeError(item)  # pragma: no cover

    @override
    @deprecated("Use `put_left_nowait`/`put_right_nowait` instead")
    def put_nowait(self, item: _T) -> None:
        raise RuntimeError(item)  # pragma: no cover

    # get all

    async def get_all(self, *, reverse: bool = False) -> Sequence[_T]:
        """Remove and return all items from the queue."""
        first = await (self.get_right() if reverse else self.get_left())
        return list(chain([first], self.get_all_nowait(reverse=reverse)))

    def get_all_nowait(self, *, reverse: bool = False) -> Sequence[_T]:
        """Remove and return all items from the queue without blocking."""
        items: Sequence[_T] = []
        while True:
            try:
                items.append(
                    self.get_right_nowait() if reverse else self.get_left_nowait()
                )
            except QueueEmpty:
                return items

    # get left/right

    async def get_left(self) -> _T:
        """Remove and return an item from the start of the queue."""
        return await self._get_left_or_right(self._get)

    async def get_right(self) -> _T:
        """Remove and return an item from the end of the queue."""
        return await self._get_left_or_right(self._get_right)

    def get_left_nowait(self) -> _T:
        """Remove and return an item from the start of the queue without blocking."""
        return self._get_left_or_right_nowait(self._get)

    def get_right_nowait(self) -> _T:
        """Remove and return an item from the end of the queue without blocking."""
        return self._get_left_or_right_nowait(self._get_right)

    # put left/right

    async def put_left(self, *items: _T) -> None:
        """Put items into the queue at the start."""
        return await self._put_left_or_right(self._put_left, *items)

    async def put_right(self, *items: _T) -> None:
        """Put items into the queue at the end."""
        return await self._put_left_or_right(self._put, *items)

    def put_left_nowait(self, *items: _T) -> None:
        """Put items into the queue at the start without blocking."""
        self._put_left_or_right_nowait(self._put_left, *items)

    def put_right_nowait(self, *items: _T) -> None:
        """Put items into the queue at the end without blocking."""
        self._put_left_or_right_nowait(self._put, *items)

    # private

    def _put_left(self, item: _T) -> None:
        self._queue.appendleft(item)

    def _get_right(self) -> _T:
        return self._queue.pop()

    async def _get_left_or_right(self, getter_use: Callable[[], _T], /) -> _T:
        while self.empty():  # pragma: no cover
            getter = self._get_loop().create_future()  # pyright: ignore[reportAttributeAccessIssue]
            self._getters.append(getter)
            try:
                await getter
            except:
                getter.cancel()
                with suppress(ValueError):
                    self._getters.remove(getter)
                if not self.empty() and not getter.cancelled():
                    self._wakeup_next(self._getters)  # pyright: ignore[reportAttributeAccessIssue]
                raise
        return getter_use()

    def _get_left_or_right_nowait(self, getter: Callable[[], _T], /) -> _T:
        if self.empty():
            raise QueueEmpty
        item = getter()
        self._wakeup_next(self._putters)  # pyright: ignore[reportAttributeAccessIssue]
        return item

    async def _put_left_or_right(
        self, putter_use: Callable[[_T], None], /, *items: _T
    ) -> None:
        """Put an item into the queue."""
        for item in items:
            await self._put_left_or_right_one(putter_use, item)

    async def _put_left_or_right_one(
        self, putter_use: Callable[[_T], None], item: _T, /
    ) -> None:
        """Put an item into the queue."""
        while self.full():  # pragma: no cover
            putter = self._get_loop().create_future()  # pyright: ignore[reportAttributeAccessIssue]
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()
                with suppress(ValueError):
                    self._putters.remove(putter)
                if not self.full() and not putter.cancelled():
                    self._wakeup_next(self._putters)  # pyright: ignore[reportAttributeAccessIssue]
                raise
        return putter_use(item)

    def _put_left_or_right_nowait(
        self, putter: Callable[[_T], None], /, *items: _T
    ) -> None:
        for item in items:
            self._put_left_or_right_nowait_one(putter, item)

    def _put_left_or_right_nowait_one(
        self, putter: Callable[[_T], None], item: _T, /
    ) -> None:
        if self.full():  # pragma: no cover
            raise QueueFull
        putter(item)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)  # pyright: ignore[reportAttributeAccessIssue]


##


class EnhancedTaskGroup(TaskGroup):
    """Task group with enhanced features."""

    _semaphore: Semaphore | None
    _timeout: Duration | None
    _error: type[Exception]
    _stack: AsyncExitStack
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
        self._stack = AsyncExitStack()
        self._timeout_cm = None

    @override
    async def __aenter__(self) -> Self:
        _ = await self._stack.__aenter__()
        return await super().__aenter__()

    @override
    async def __aexit__(
        self,
        et: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = await self._stack.__aexit__(et, exc, tb)
        _ = await super().__aexit__(et, exc, tb)

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

    def create_task_context(self, cm: AbstractAsyncContextManager[_T], /) -> Task[_T]:
        """Have the TaskGroup start an asynchronous context manager."""
        _ = self._stack.push_async_callback(cm.__aexit__, None, None, None)
        return self.create_task(cm.__aenter__())

    async def _wrap_with_semaphore(
        self, semaphore: Semaphore, coroutine: _CoroutineLike[_T], /
    ) -> _T:
        async with semaphore:
            return await coroutine

    async def _wrap_with_timeout(self, coroutine: _CoroutineLike[_T], /) -> _T:
        async with timeout_dur(duration=self._timeout, error=self._error):
            return await coroutine


##


@dataclass(kw_only=True, unsafe_hash=True)
class InfiniteLooper(ABC, Generic[THashable]):
    """An infinite loop which can throw exceptions by setting events."""

    sleep_core: DurationOrEveryDuration = field(default=SECOND, repr=False)
    sleep_restart: DurationOrEveryDuration = field(default=MINUTE, repr=False)
    duration: Duration | None = field(default=None, repr=False)
    logger: str | None = field(default=None, repr=False)
    _await_upon_aenter: bool = field(default=True, init=False, repr=False)
    _depth: int = field(default=0, init=False, repr=False)
    _events: Mapping[THashable | None, Event] = field(
        default_factory=dict, init=False, repr=False, hash=False
    )
    _stack: AsyncExitStack = field(
        default_factory=AsyncExitStack, init=False, repr=False
    )
    _task: Task[None] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self._events = {
            event: Event() for event, _ in self._yield_events_and_exceptions()
        }

    async def __aenter__(self) -> Self:
        """Context manager entry."""
        if self._depth == 0:
            self._task = create_task(self._run_looper())
            if self._await_upon_aenter:
                with suppress(CancelledError):
                    await self._task
            _ = await self._stack.__aenter__()
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
        self._depth = max(self._depth - 1, 0)
        if (self._depth == 0) and (self._task is not None):
            with suppress(CancelledError):
                await self._task
            self._task = None
            try:
                await self._teardown()
            except Exception as error:  # noqa: BLE001
                self._error_upon_teardown(error)
            _ = await self._stack.__aexit__(exc_type, exc_value, traceback)

    async def stop(self) -> None:
        """Stop the service."""
        if self._task is None:
            raise ImpossibleCaseError(case=[f"{self._task=}"])  # pragma: no cover
        with suppress(CancelledError):
            _ = self._task.cancel()

    async def _run_looper(self) -> None:
        """Run the looper."""
        match self.duration:
            case None:
                await self._run_looper_without_timeout()
            case int() | float() | dt.timedelta() as duration:
                try:
                    async with timeout_dur(duration=duration):
                        return await self._run_looper_without_timeout()
                except TimeoutError:
                    await self.stop()
            case _ as never:
                assert_never(never)
        return None

    async def _run_looper_without_timeout(self) -> None:
        """Run the looper without a timeout."""
        coroutines = list(self._yield_coroutines())
        loopers = list(self._yield_loopers())
        if (len(coroutines) == 0) and (len(loopers) == 0):
            return await self._run_looper_by_itself()
        return await self._run_looper_with_others(coroutines, loopers)

    async def _run_looper_by_itself(self) -> None:
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
                try:
                    await self._teardown()
                except Exception as error:  # noqa: BLE001
                    self._error_upon_teardown(error)
                finally:
                    await self._run_sleep(self.sleep_restart)

    async def _run_looper_with_others(
        self,
        coroutines: Iterable[Callable[[], Coroutine1[None]]],
        loopers: Iterable[InfiniteLooper[Any]],
        /,
    ) -> None:
        """Run multiple loopers."""
        while True:
            self._reset_events()
            try:
                async with TaskGroup() as tg, AsyncExitStack() as stack:
                    _ = tg.create_task(self._run_looper_by_itself())
                    _ = [tg.create_task(c()) for c in coroutines]
                    _ = [
                        tg.create_task(stack.enter_async_context(lo)) for lo in loopers
                    ]
            except ExceptionGroup as error:
                self._error_group_upon_others(error)
                await self._run_sleep(self.sleep_restart)

    async def _initialize(self) -> None:
        """Initialize the loop."""

    async def _core(self) -> None:
        """Run the core part of the loop."""

    async def _teardown(self) -> None:
        """Tear down the loop."""

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

    def _error_upon_teardown(self, error: Exception, /) -> None:
        """Handle any errors upon tearing down the looper."""
        if self.logger is not None:
            getLogger(name=self.logger).error(
                "%r encountered %r whilst tearing down; sleeping %s...",
                get_class_name(self),
                repr_error(error),
                self._sleep_restart_desc,
            )

    def _error_group_upon_others(self, group: ExceptionGroup, /) -> None:
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

    def _raise_error(self, event: THashable | None, /) -> NoReturn:
        """Raise the error corresponding to given event."""
        mapping = dict(self._yield_events_and_exceptions())
        error = mapping.get(event, InfiniteLooperError)
        raise error

    def _reset_events(self) -> None:
        """Reset the events."""
        self._events = {
            event: Event() for event, _ in self._yield_events_and_exceptions()
        }

    async def _run_sleep(self, sleep: DurationOrEveryDuration, /) -> None:
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

    def _set_event(self, *, event: THashable | None = None) -> None:
        """Set the given event."""
        try:
            event_obj = self._events[event]
        except KeyError:
            raise _InfiniteLooperNoSuchEventError(looper=self, event=event) from None
        event_obj.set()

    def _yield_events_and_exceptions(
        self,
    ) -> Iterator[tuple[THashable | None, MaybeType[Exception]]]:
        """Yield the events & exceptions."""
        yield (None, _InfiniteLooperDefaultEventError(looper=self))

    def _yield_coroutines(self) -> Iterator[Callable[[], Coroutine1[None]]]:
        """Yield any other coroutines which must also be run."""
        yield from []

    def _yield_loopers(self) -> Iterator[InfiniteLooper[Any]]:
        """Yield any other loopers which must also be run."""
        yield from []


@dataclass(kw_only=True, slots=True)
class InfiniteLooperError(Exception):
    looper: InfiniteLooper[Any]


@dataclass(kw_only=True, slots=True)
class _InfiniteLooperNoSuchEventError(InfiniteLooperError):
    event: Hashable

    @override
    def __str__(self) -> str:
        return f"{get_class_name(self.looper)!r} does not have an event {self.event!r}"


@dataclass(kw_only=True, slots=True)
class _InfiniteLooperDefaultEventError(InfiniteLooperError):
    @override
    def __str__(self) -> str:
        return f"{get_class_name(self.looper)!r} default event error"


##


@dataclass(kw_only=True)
class InfiniteQueueLooper(InfiniteLooper[THashable], Generic[THashable, _T]):
    """An infinite loop which processes a queue."""

    _await_upon_aenter: bool = field(default=False, init=False, repr=False)
    _queue: EnhancedQueue[_T] = field(init=False, repr=False)

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self._queue = EnhancedQueue()

    def __len__(self) -> int:
        return self._queue.qsize()

    @override
    async def _core(self) -> None:
        """Run the core part of the loop."""
        first = await self._queue.get_left()
        self._queue.put_left_nowait(first)
        await self._process_queue()

    @abstractmethod
    async def _process_queue(self) -> None:
        """Process the queue."""

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def put_left_nowait(self, *items: _T) -> None:
        """Put items into the queue at the start without blocking."""
        self._queue.put_left_nowait(*items)  # pragma: no cover

    def put_right_nowait(self, *items: _T) -> None:
        """Put items into the queue at the end without blocking."""
        self._queue.put_right_nowait(*items)  # pragma: no cover

    async def run_until_empty(self) -> None:
        """Run until the queue is empty."""
        while not self.empty():
            await self._process_queue()
        await self.stop()


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
    "EnhancedQueue",
    "EnhancedTaskGroup",
    "InfiniteLooper",
    "InfiniteLooperError",
    "InfiniteQueueLooper",
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
