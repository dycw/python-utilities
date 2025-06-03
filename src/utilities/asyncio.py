from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from asyncio import (
    CancelledError,
    Event,
    Lock,
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
from logging import DEBUG, Logger, getLogger
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

from utilities.dataclasses import replace_non_sentinel
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
from utilities.random import SYSTEM_RANDOM
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
    from random import Random
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
        whitelisted = tuple(self._yield_whitelisted_errors())
        blacklisted = tuple(self._yield_blacklisted_errors())
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
            except BaseException as error1:
                match error1:
                    case Exception():
                        if isinstance(error1, blacklisted):
                            raise
                    case BaseException():
                        if not isinstance(error1, whitelisted):
                            raise
                    case _ as never:
                        assert_never(never)
                self._error_upon_core(error1)
                try:
                    await self._teardown()
                except BaseException as error2:  # noqa: BLE001
                    self._error_upon_teardown(error2)
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

    def _error_upon_core(self, error: BaseException, /) -> None:
        """Handle any errors upon running the core function."""
        if self.logger is not None:
            getLogger(name=self.logger).error(
                "%r encountered %r; sleeping %s...",
                get_class_name(self),
                repr_error(error),
                self._sleep_restart_desc,
            )

    def _error_upon_teardown(self, error: BaseException, /) -> None:
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

    def _yield_blacklisted_errors(self) -> Iterator[type[Exception]]:
        """Yield any exceptions which the looper ought to catch terminate upon."""
        yield from []

    def _yield_whitelisted_errors(self) -> Iterator[type[BaseException]]:
        """Yield any exceptions which the looper ought to catch and allow running."""
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
    _queue: EnhancedQueue[_T] = field(
        default_factory=EnhancedQueue, init=False, repr=False
    )

    def __len__(self) -> int:
        return self._queue.qsize()

    @override
    async def _core(self) -> None:
        """Run the core part of the loop."""
        if self.empty():
            return
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

    def qsize(self) -> int:
        """Get the number of items in the queue."""
        return self._queue.qsize()

    async def run_until_empty(self, *, stop: bool = False) -> None:
        """Run until the queue is empty."""
        while not self.empty():
            await self._process_queue()
        if stop:
            await self.stop()


##


@dataclass(kw_only=True, slots=True)
class LooperError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _LooperNoTaskError(LooperError):
    looper: Looper

    @override
    def __str__(self) -> str:
        return f"{self.looper} has no running task"


@dataclass(kw_only=True, unsafe_hash=True)
class Looper(Generic[_T]):
    """A looper of a core coroutine, handling errors."""

    auto_start: bool = field(default=False, repr=False)
    freq: Duration = field(default=SECOND, repr=False)
    backoff: Duration = field(default=10 * SECOND, repr=False)
    empty_upon_exit: bool = field(default=False, repr=False)
    logger: str | None = field(default=None, repr=False)
    timeout: Duration | None = field(default=None, repr=False)
    # settings
    _backoff: float = field(init=False, repr=False)
    _debug: bool = field(default=False, repr=False)
    _freq: float = field(init=False, repr=False)
    # counts
    _entries: int = field(default=0, init=False, repr=False)
    _core_attempts: int = field(default=0, init=False, repr=False)
    _core_successes: int = field(default=0, init=False, repr=False)
    _core_failures: int = field(default=0, init=False, repr=False)
    _initialization_attempts: int = field(default=0, init=False, repr=False)
    _initialization_successes: int = field(default=0, init=False, repr=False)
    _initialization_failures: int = field(default=0, init=False, repr=False)
    _tear_down_attempts: int = field(default=0, init=False, repr=False)
    _tear_down_successes: int = field(default=0, init=False, repr=False)
    _tear_down_failures: int = field(default=0, init=False, repr=False)
    _restart_attempts: int = field(default=0, init=False, repr=False)
    _restart_successes: int = field(default=0, init=False, repr=False)
    _restart_failures: int = field(default=0, init=False, repr=False)
    _stops: int = field(default=0, init=False, repr=False)
    # flags
    _is_entered: Event = field(default_factory=Event, init=False, repr=False)
    _is_initialized: Event = field(default_factory=Event, init=False, repr=False)
    _is_initializing: Event = field(default_factory=Event, init=False, repr=False)
    _is_pending_restart: Event = field(default_factory=Event, init=False, repr=False)
    _is_pending_stop: Event = field(default_factory=Event, init=False, repr=False)
    _is_pending_stop_when_empty: Event = field(
        default_factory=Event, init=False, repr=False
    )
    _is_stopped: Event = field(default_factory=Event, init=False, repr=False)
    _is_tearing_down: Event = field(default_factory=Event, init=False, repr=False)
    # internal objects
    _lock: Lock = field(default_factory=Lock, init=False, repr=False, hash=False)
    _logger: Logger = field(init=False, repr=False, hash=False)
    _queue: EnhancedQueue[_T] = field(
        default_factory=EnhancedQueue, init=False, repr=False, hash=False
    )
    _stack: AsyncExitStack = field(
        default_factory=AsyncExitStack, init=False, repr=False, hash=False
    )
    _task: Task[None] | None = field(default=None, init=False, repr=False, hash=False)

    def __post_init__(self) -> None:
        self._backoff = datetime_duration_to_float(self.backoff)
        self._freq = datetime_duration_to_float(self.freq)
        self._logger = getLogger(name=self.logger)
        self._logger.setLevel(DEBUG)

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        match self._is_entered.is_set():
            case True:
                _ = self._debug and self._logger.debug("%s: already entered", self)
            case False:
                _ = self._debug and self._logger.debug("%s: entering context...", self)
                self._is_entered.set()
                async with self._lock:
                    self._entries += 1
                    self._task = create_task(self.run_looper())
                for looper in self._yield_sub_loopers():
                    _ = self._debug and self._logger.debug(
                        "%s: adding sub-looper %s", self, looper
                    )
                    _ = await self._stack.enter_async_context(looper)
                if self.auto_start:
                    _ = self._debug and self._logger.debug("%s: auto-starting...", self)
                    with suppress(TimeoutError):
                        await self._task
            case _ as never:
                assert_never(never)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Exit the context manager."""
        match self._is_entered.is_set():
            case True:
                _ = self._debug and self._logger.debug("%s: exiting context...", self)
                self._is_entered.clear()
                if (
                    (exc_type is not None)
                    and (exc_value is not None)
                    and (traceback is not None)
                ):
                    _ = self._debug and self._logger.warning(
                        "%s: encountered %s whilst in context",
                        self,
                        repr_error(exc_value),
                    )
                _ = await self._stack.__aexit__(exc_type, exc_value, traceback)
                await self.stop()
                if self.empty_upon_exit:
                    await self.run_until_empty()
            case False:
                _ = self._debug and self._logger.debug("%s: already exited", self)
            case _ as never:
                assert_never(never)

    def __await__(self) -> Any:
        if (task := self._task) is None:  # cannot use match
            raise _LooperNoTaskError(looper=self)
        return task.__await__()

    def __len__(self) -> int:
        return self._queue.qsize()

    async def core(self) -> None:
        """Core part of running the looper."""

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def get_all_nowait(self, *, reverse: bool = False) -> Sequence[_T]:
        """Remove and return all items from the queue without blocking."""
        return self._queue.get_all_nowait(reverse=reverse)

    def get_left_nowait(self) -> _T:
        """Remove and return an item from the start of the queue without blocking."""
        return self._queue.get_left_nowait()

    def get_right_nowait(self) -> _T:
        """Remove and return an item from the end of the queue without blocking."""
        return self._queue.get_right_nowait()

    async def initialize(self, *, sleep_if_failure: bool) -> Exception | None:
        """Initialize the looper."""
        match self._is_initializing.is_set():
            case True:
                _ = self._debug and self._logger.debug("%s: already initializing", self)
                return None
            case False:
                _ = self._debug and self._logger.debug("%s: initializing...", self)
                self._is_initializing.set()
                self._is_initialized.clear()
                async with self._lock:
                    self._initialization_attempts += 1
                try:
                    await self._initialize_core()
                except Exception as error:  # noqa: BLE001
                    async with self._lock:
                        self._initialization_failures += 1
                    ret = error
                    match sleep_if_failure:
                        case True:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst initializing; sleeping for %s...",
                                self,
                                repr_error(error),
                                self.backoff,
                            )
                            await sleep(self._backoff)
                        case False:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst initializing",
                                self,
                                repr_error(error),
                            )
                        case _ as never:
                            assert_never(never)
                else:
                    _ = self._debug and self._logger.debug(
                        "%s: finished initializing", self
                    )
                    self._is_initialized.set()
                    async with self._lock:
                        self._initialization_successes += 1
                    ret = None
                finally:
                    self._is_initializing.clear()
                return ret
            case _ as never:
                assert_never(never)

    async def _initialize_core(self) -> None:
        """Core part of initializing the looper."""

    def put_left_nowait(self, *items: _T) -> None:
        """Put items into the queue at the start without blocking."""
        self._queue.put_left_nowait(*items)

    def put_right_nowait(self, *items: _T) -> None:
        """Put items into the queue at the end without blocking."""
        self._queue.put_right_nowait(*items)

    def qsize(self) -> int:
        """Get the number of items in the queue."""
        return self._queue.qsize()

    def replace(
        self,
        *,
        auto_start: bool | Sentinel = sentinel,
        empty_upon_exit: bool | Sentinel = sentinel,
        freq: Duration | Sentinel = sentinel,
        backoff: Duration | Sentinel = sentinel,
        logger: str | None | Sentinel = sentinel,
        timeout: Duration | None | Sentinel = sentinel,
        _debug: bool | Sentinel = sentinel,
        **kwargs: Any,
    ) -> Self:
        """Replace elements of the looper."""
        return replace_non_sentinel(
            self,
            auto_start=auto_start,
            empty_upon_exit=empty_upon_exit,
            freq=freq,
            backoff=backoff,
            logger=logger,
            timeout=timeout,
            _debug=_debug,
            **kwargs,
        )

    def request_restart(self) -> None:
        """Request the looper to restart."""
        match self._is_pending_restart.is_set():
            case True:
                _ = self._debug and self._logger.debug(
                    "%s: already requested restart", self
                )
            case False:
                _ = self._debug and self._logger.debug(
                    "%s: requesting restart...", self
                )
                self._is_pending_restart.set()
            case _ as never:
                assert_never(never)

    def request_stop(self) -> None:
        """Request the looper to stop."""
        match self._is_pending_stop.is_set():
            case True:
                _ = self._debug and self._logger.debug(
                    "%s: already requested stop", self
                )
            case False:
                _ = self._debug and self._logger.debug("%s: requesting stop...", self)
                self._is_pending_stop.set()
            case _ as never:
                assert_never(never)

    def request_stop_when_empty(self) -> None:
        """Request the looper to stop when the queue is empty."""
        match self._is_pending_stop_when_empty.is_set():
            case True:
                _ = self._debug and self._logger.debug(
                    "%s: already requested stop when empty", self
                )
            case False:
                _ = self._debug and self._logger.debug(
                    "%s: requesting stop when empty...", self
                )
                self._is_pending_stop_when_empty.set()
            case _ as never:
                assert_never(never)

    async def restart(self, *, sleep_if_failure: bool) -> None:
        """Restart the looper."""
        _ = self._debug and self._logger.debug("%s: restarting...", self)
        self._is_pending_restart.clear()
        async with self._lock:
            self._restart_attempts += 1
        tear_down = await self.tear_down(sleep_if_failure=False)
        initialization = await self.initialize(sleep_if_failure=False)
        match tear_down, initialization, sleep_if_failure:
            case None, None, bool():
                _ = self._debug and self._logger.debug("%s: finished restarting", self)
                async with self._lock:
                    self._restart_successes += 1
            case Exception(), None, True:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s whilst restarting (tear down); sleeping for %s...",
                    self,
                    repr_error(tear_down),
                    self.backoff,
                )
                await sleep(self._backoff)
            case Exception(), None, False:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s whilst restarting (tear down)",
                    self,
                    repr_error(tear_down),
                )
            case None, Exception(), True:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s whilst restarting (initialize); sleeping for %s...",
                    self,
                    repr_error(initialization),
                    self.backoff,
                )
                await sleep(self._backoff)
            case None, Exception(), False:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s whilst restarting (initialize)",
                    self,
                    repr_error(initialization),
                )
            case Exception(), Exception(), True:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s (tear down) and then %s (initialization) whilst restarting; sleeping for %s...",
                    self,
                    repr_error(tear_down),
                    repr_error(initialization),
                    self.backoff,
                )
                await sleep(self._backoff)
            case Exception(), Exception(), False:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s (tear down) and then %s (initialization) whilst restarting",
                    self,
                    repr_error(tear_down),
                    repr_error(initialization),
                )
            case _ as never:
                assert_never(never)

    async def run_looper(self) -> None:
        """Run the looper."""
        try:
            async with timeout_dur(duration=self.timeout):
                while True:
                    if self._is_stopped.is_set():
                        _ = self._debug and self._logger.debug("%s: stopped", self)
                        return
                    if (self._is_pending_stop.is_set()) or (
                        self._is_pending_stop_when_empty.is_set() and self.empty()
                    ):
                        await self.stop()
                    elif self._is_pending_restart.is_set():
                        await self.restart(sleep_if_failure=True)
                    elif not self._is_initialized.is_set():
                        _ = await self.initialize(sleep_if_failure=True)
                    else:
                        _ = self._debug and self._logger.debug(
                            "%s: running core...", self
                        )
                        async with self._lock:
                            self._core_attempts += 1
                        try:
                            await self.core()
                        except Exception as error:  # noqa: BLE001
                            _ = self._logger.warning(
                                "%s: encountered %s whilst running core...",
                                self,
                                repr_error(error),
                            )
                            async with self._lock:
                                self._core_failures += 1
                            self.request_restart()
                            await sleep(self._backoff)
                        else:
                            async with self._lock:
                                self._core_successes += 1
                            await sleep(self._freq)
        except RuntimeError as error:  # pragma: no cover
            if error.args[0] == "generator didn't stop after athrow()":
                return
            raise
        except TimeoutError:
            pass

    async def run_until_empty(self) -> None:
        """Run until the queue is empty."""
        while not self.empty():
            await self.core()
            if not self.empty():
                await sleep(self._freq)

    @property
    def stats(self) -> _LooperStats:
        """Return the statistics."""
        return _LooperStats(
            entries=self._entries,
            core_attempts=self._core_attempts,
            core_successes=self._core_successes,
            core_failures=self._core_failures,
            initialization_attempts=self._initialization_attempts,
            initialization_successes=self._initialization_successes,
            initialization_failures=self._initialization_failures,
            tear_down_attempts=self._tear_down_attempts,
            tear_down_successes=self._tear_down_successes,
            tear_down_failures=self._tear_down_failures,
            restart_attempts=self._restart_attempts,
            restart_successes=self._restart_successes,
            restart_failures=self._restart_failures,
            stops=self._stops,
        )

    async def stop(self) -> None:
        """Stop the looper."""
        match self._is_stopped.is_set():
            case True:
                _ = self._debug and self._logger.debug("%s: already stopped", self)
            case False:
                _ = self._debug and self._logger.debug("%s: stopping...", self)
                self._is_pending_stop.clear()
                self._is_stopped.set()
                async with self._lock:
                    self._stops += 1
                _ = self._debug and self._logger.debug("%s: stopped", self)
            case _ as never:
                assert_never(never)

    async def tear_down(self, *, sleep_if_failure: bool) -> Exception | None:
        """Tear down the looper."""
        match self._is_tearing_down.is_set():
            case True:
                _ = self._debug and self._logger.debug("%s: already tearing down", self)
                return None
            case False:
                _ = self._debug and self._logger.debug("%s: tearing down...", self)
                self._is_tearing_down.set()
                async with self._lock:
                    self._tear_down_attempts += 1
                try:
                    await self._tear_down_core()
                except Exception as error:  # noqa: BLE001
                    async with self._lock:
                        self._tear_down_failures += 1
                    ret = error
                    match sleep_if_failure:
                        case True:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst tearing down; sleeping for %s...",
                                self,
                                repr_error(error),
                                self.backoff,
                            )
                            await sleep(self._backoff)
                        case False:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst tearing down",
                                self,
                                repr_error(error),
                            )
                        case _ as never:
                            assert_never(never)
                else:
                    _ = self._debug and self._logger.debug(
                        "%s: finished tearing down", self
                    )
                    async with self._lock:
                        self._tear_down_successes += 1
                    ret = None
                finally:
                    self._is_tearing_down.clear()
                return ret
            case _ as never:
                assert_never(never)

    async def _tear_down_core(self) -> None:
        """Core part of tearing down the looper."""

    @property
    def with_auto_start(self) -> Self:
        """Replace the auto start flag of the looper."""
        return self.replace(auto_start=True)

    def _yield_sub_loopers(self) -> Iterator[Looper]:
        """Yield all sub-loopers."""
        yield from []


@dataclass(kw_only=True, slots=True)
class _LooperStats:
    entries: int = 0
    core_attempts: int = 0
    core_successes: int = 0
    core_failures: int = 0
    initialization_attempts: int = 0
    initialization_successes: int = 0
    initialization_failures: int = 0
    tear_down_attempts: int = 0
    tear_down_successes: int = 0
    tear_down_failures: int = 0
    restart_attempts: int = 0
    restart_successes: int = 0
    restart_failures: int = 0
    stops: int = 0


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


async def sleep_max_dur(
    *, duration: Duration | None = None, random: Random = SYSTEM_RANDOM
) -> None:
    """Sleep which accepts max durations."""
    if duration is None:
        return
    await sleep(random.uniform(0.0, datetime_duration_to_float(duration)))


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
    "Looper",
    "LooperError",
    "StreamCommandOutput",
    "UniquePriorityQueue",
    "UniqueQueue",
    "get_event",
    "get_items",
    "get_items_nowait",
    "put_items",
    "put_items_nowait",
    "sleep_dur",
    "sleep_max_dur",
    "sleep_until",
    "sleep_until_rounded",
    "stream_command",
    "timeout_dur",
]
