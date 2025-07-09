from __future__ import annotations

import asyncio
from asyncio import (
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
)
from collections.abc import (
    Callable,
    Hashable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    ValuesView,
)
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
    ClassVar,
    Self,
    TextIO,
    assert_never,
    overload,
    override,
)

from typing_extensions import deprecated

from utilities.dataclasses import replace_non_sentinel
from utilities.errors import repr_error
from utilities.functions import ensure_int, ensure_not_none, to_bool
from utilities.random import SYSTEM_RANDOM
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    Delta,
    MaybeCallableBool,
    SupportsKeysAndGetItem,
    SupportsRichComparison,
)
from utilities.whenever import SECOND, get_now, round_date_or_date_time, to_nanoseconds

if TYPE_CHECKING:
    from asyncio import _CoroutineLike
    from asyncio.subprocess import Process
    from collections import deque
    from collections.abc import AsyncIterator, Sequence
    from contextvars import Context
    from random import Random
    from types import TracebackType

    from whenever import TimeDelta, ZonedDateTime

    from utilities.types import MaybeCallableEvent, MaybeType


class AsyncDict[K, V]:
    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, map: SupportsKeysAndGetItem[K, V], /) -> None: ...
    @overload
    def __init__(self, iterable: Iterable[tuple[K, V]], /) -> None: ...
    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self._dict = dict[K, V](*args, **kwargs)
        self._lock = Lock()

    async def __aenter__(self) -> dict[K, V]:
        await self._lock.__aenter__()
        return self._dict

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
        /,
    ) -> None:
        await self._lock.__aexit__(exc_type, exc, tb)

    def __contains__(self, key: Any, /) -> bool:
        return key in self._dict

    @override
    def __eq__(self, other: Any, /) -> bool:
        return self._dict == other

    __hash__: ClassVar[None] = None  # pyright: ignore[reportIncompatibleMethodOverride]

    def __getitem__(self, key: K, /) -> V:
        return self._dict[key]

    def __iter__(self) -> Iterator[K]:
        yield from self._dict

    def __len__(self) -> int:
        return len(self._dict)

    @override
    def __repr__(self) -> str:
        return repr(self._dict)

    def __reversed__(self) -> Iterator[K]:
        return reversed(self._dict)

    @override
    def __str__(self) -> str:
        return str(self._dict)

    @property
    def empty(self) -> bool:
        return len(self) == 0

    @classmethod
    @overload
    def fromkeys[T](
        cls, iterable: Iterable[T], value: None = None, /
    ) -> AsyncDict[T, Any | None]: ...
    @classmethod
    @overload
    def fromkeys[K2, V2](
        cls, iterable: Iterable[K2], value: V2, /
    ) -> AsyncDict[K2, V2]: ...
    @classmethod
    def fromkeys(
        cls, iterable: Iterable[Any], value: Any = None, /
    ) -> AsyncDict[Any, Any]:
        return cls(dict.fromkeys(iterable, value))

    async def clear(self) -> None:
        async with self._lock:
            self._dict.clear()

    def copy(self) -> Self:
        return type(self)(self._dict.items())

    async def del_(self, key: K, /) -> None:
        async with self._lock:
            del self._dict[key]

    @overload
    def get(self, key: K, default: None = None, /) -> V | None: ...
    @overload
    def get(self, key: K, default: V, /) -> V: ...
    @overload
    def get[V2](self, key: K, default: V2, /) -> V | V2: ...
    def get(self, key: K, default: Any = sentinel, /) -> Any:
        match default:
            case Sentinel():
                return self._dict.get(key)
            case _:
                return self._dict.get(key, default)

    def keys(self) -> KeysView[K]:
        return self._dict.keys()

    def items(self) -> ItemsView[K, V]:
        return self._dict.items()

    @overload
    async def pop(self, key: K, /) -> V: ...
    @overload
    async def pop(self, key: K, default: V, /) -> V: ...
    @overload
    async def pop[V2](self, key: K, default: V2, /) -> V | V2: ...
    async def pop(self, key: K, default: Any = sentinel, /) -> Any:
        async with self._lock:
            match default:
                case Sentinel():
                    return self._dict.pop(key)
                case _:
                    return self._dict.pop(key, default)

    async def popitem(self) -> tuple[K, V]:
        async with self._lock:
            return self._dict.popitem()

    async def set(self, key: K, value: V, /) -> None:
        async with self._lock:
            self._dict[key] = value

    async def setdefault(self, key: K, default: V, /) -> V:
        async with self._lock:
            return self._dict.setdefault(key, default)

    @overload
    async def update(self, m: SupportsKeysAndGetItem[K, V], /) -> None: ...
    @overload
    async def update(self, m: Iterable[tuple[K, V]], /) -> None: ...
    async def update(self, *args: Any, **kwargs: V) -> None:
        async with self._lock:
            self._dict.update(*args, **kwargs)

    def values(self) -> ValuesView[V]:
        return self._dict.values()


##


class EnhancedQueue[T](Queue[T]):
    """An asynchronous deque."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize=maxsize)
        self._finished: Event
        self._getters: deque[Any]
        self._putters: deque[Any]
        self._queue: deque[T]
        self._unfinished_tasks: int

    @override
    @deprecated("Use `get_left`/`get_right` instead")
    async def get(self) -> T:
        raise RuntimeError  # pragma: no cover

    @override
    @deprecated("Use `get_left_nowait`/`get_right_nowait` instead")
    def get_nowait(self) -> T:
        raise RuntimeError  # pragma: no cover

    @override
    @deprecated("Use `put_left`/`put_right` instead")
    async def put(self, item: T) -> None:
        raise RuntimeError(item)  # pragma: no cover

    @override
    @deprecated("Use `put_left_nowait`/`put_right_nowait` instead")
    def put_nowait(self, item: T) -> None:
        raise RuntimeError(item)  # pragma: no cover

    # get all

    async def get_all(self, *, reverse: bool = False) -> Sequence[T]:
        """Remove and return all items from the queue."""
        first = await (self.get_right() if reverse else self.get_left())
        return list(chain([first], self.get_all_nowait(reverse=reverse)))

    def get_all_nowait(self, *, reverse: bool = False) -> Sequence[T]:
        """Remove and return all items from the queue without blocking."""
        items: Sequence[T] = []
        while True:
            try:
                items.append(
                    self.get_right_nowait() if reverse else self.get_left_nowait()
                )
            except QueueEmpty:
                return items

    # get left/right

    async def get_left(self) -> T:
        """Remove and return an item from the start of the queue."""
        return await self._get_left_or_right(self._get)

    async def get_right(self) -> T:
        """Remove and return an item from the end of the queue."""
        return await self._get_left_or_right(self._get_right)

    def get_left_nowait(self) -> T:
        """Remove and return an item from the start of the queue without blocking."""
        return self._get_left_or_right_nowait(self._get)

    def get_right_nowait(self) -> T:
        """Remove and return an item from the end of the queue without blocking."""
        return self._get_left_or_right_nowait(self._get_right)

    # put left/right

    async def put_left(self, *items: T) -> None:
        """Put items into the queue at the start."""
        return await self._put_left_or_right(self._put_left, *items)

    async def put_right(self, *items: T) -> None:
        """Put items into the queue at the end."""
        return await self._put_left_or_right(self._put, *items)

    def put_left_nowait(self, *items: T) -> None:
        """Put items into the queue at the start without blocking."""
        self._put_left_or_right_nowait(self._put_left, *items)

    def put_right_nowait(self, *items: T) -> None:
        """Put items into the queue at the end without blocking."""
        self._put_left_or_right_nowait(self._put, *items)

    # private

    def _put_left(self, item: T) -> None:
        self._queue.appendleft(item)

    def _get_right(self) -> T:
        return self._queue.pop()

    async def _get_left_or_right(self, getter_use: Callable[[], T], /) -> T:
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

    def _get_left_or_right_nowait(self, getter: Callable[[], T], /) -> T:
        if self.empty():
            raise QueueEmpty
        item = getter()
        self._wakeup_next(self._putters)  # pyright: ignore[reportAttributeAccessIssue]
        return item

    async def _put_left_or_right(
        self, putter_use: Callable[[T], None], /, *items: T
    ) -> None:
        """Put an item into the queue."""
        for item in items:
            await self._put_left_or_right_one(putter_use, item)

    async def _put_left_or_right_one(
        self, putter_use: Callable[[T], None], item: T, /
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
        self, putter: Callable[[T], None], /, *items: T
    ) -> None:
        for item in items:
            self._put_left_or_right_nowait_one(putter, item)

    def _put_left_or_right_nowait_one(
        self, putter: Callable[[T], None], item: T, /
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

    _max_tasks: int | None
    _semaphore: Semaphore | None
    _timeout: Delta | None
    _error: MaybeType[BaseException]
    _debug: MaybeCallableBool
    _stack: AsyncExitStack
    _timeout_cm: _AsyncGeneratorContextManager[None] | None

    @override
    def __init__(
        self,
        *,
        max_tasks: int | None = None,
        timeout: Delta | None = None,
        error: MaybeType[BaseException] = TimeoutError,
        debug: MaybeCallableBool = False,
    ) -> None:
        super().__init__()
        self._max_tasks = max_tasks
        if (max_tasks is None) or (max_tasks <= 0):
            self._semaphore = None
        else:
            self._semaphore = Semaphore(max_tasks)
        self._timeout = timeout
        self._error = error
        self._debug = debug
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
        match self._is_debug():
            case True:
                with suppress(Exception):
                    _ = await super().__aexit__(et, exc, tb)
            case False:
                _ = await super().__aexit__(et, exc, tb)
            case _ as never:
                assert_never(never)

    @override
    def create_task[T](
        self,
        coro: _CoroutineLike[T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> Task[T]:
        if self._semaphore is None:
            coroutine = coro
        else:
            coroutine = self._wrap_with_semaphore(self._semaphore, coro)
        coroutine = self._wrap_with_timeout(coroutine)
        return super().create_task(coroutine, name=name, context=context)

    def create_task_context[T](self, cm: AbstractAsyncContextManager[T], /) -> Task[T]:
        """Have the TaskGroup start an asynchronous context manager."""
        _ = self._stack.push_async_callback(cm.__aexit__, None, None, None)
        return self.create_task(cm.__aenter__())

    async def run_or_create_many_tasks[**P, T](
        self,
        make_coro: Callable[P, _CoroutineLike[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T | Sequence[Task[T]]:
        match self._is_debug(), self._max_tasks:
            case (True, _) | (False, None):
                return await make_coro(*args, **kwargs)
            case False, int():
                return [
                    self.create_task(make_coro(*args, **kwargs))
                    for _ in range(self._max_tasks)
                ]
            case _ as never:
                assert_never(never)

    async def run_or_create_task[T](
        self,
        coro: _CoroutineLike[T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> T | Task[T]:
        match self._is_debug():
            case True:
                return await coro
            case False:
                return self.create_task(coro, name=name, context=context)
            case _ as never:
                assert_never(never)

    def _is_debug(self) -> bool:
        return to_bool(bool_=self._debug) or (
            (self._max_tasks is not None) and (self._max_tasks <= 0)
        )

    async def _wrap_with_semaphore[T](
        self, semaphore: Semaphore, coroutine: _CoroutineLike[T], /
    ) -> T:
        async with semaphore:
            return await coroutine

    async def _wrap_with_timeout[T](self, coroutine: _CoroutineLike[T], /) -> T:
        async with timeout_td(self._timeout, error=self._error):
            return await coroutine


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
class Looper[T]:
    """A looper of a core coroutine, handling errors."""

    auto_start: bool = field(default=False, repr=False)
    freq: TimeDelta = field(default=SECOND, repr=False)
    backoff: TimeDelta = field(default=10 * SECOND, repr=False)
    empty_upon_exit: bool = field(default=False, repr=False)
    logger: str | None = field(default=None, repr=False)
    timeout: TimeDelta | None = field(default=None, repr=False)
    # settings
    _debug: bool = field(default=False, repr=False)
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
    _is_pending_back_off: Event = field(default_factory=Event, init=False, repr=False)
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
    _queue: EnhancedQueue[T] = field(
        default_factory=EnhancedQueue, init=False, repr=False, hash=False
    )
    _stack: AsyncExitStack = field(
        default_factory=AsyncExitStack, init=False, repr=False, hash=False
    )
    _task: Task[None] | None = field(default=None, init=False, repr=False, hash=False)

    def __post_init__(self) -> None:
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

    async def _apply_back_off(self) -> None:
        """Apply a back off period."""
        await sleep_td(self.backoff)
        self._is_pending_back_off.clear()

    async def core(self) -> None:
        """Core part of running the looper."""

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    def get_all_nowait(self, *, reverse: bool = False) -> Sequence[T]:
        """Remove and return all items from the queue without blocking."""
        return self._queue.get_all_nowait(reverse=reverse)

    def get_left_nowait(self) -> T:
        """Remove and return an item from the start of the queue without blocking."""
        return self._queue.get_left_nowait()

    def get_right_nowait(self) -> T:
        """Remove and return an item from the end of the queue without blocking."""
        return self._queue.get_right_nowait()

    async def initialize(
        self, *, skip_sleep_if_failure: bool = False
    ) -> Exception | None:
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
                    match skip_sleep_if_failure:
                        case True:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst initializing",
                                self,
                                repr_error(error),
                            )
                        case False:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst initializing; sleeping for %s...",
                                self,
                                repr_error(error),
                                self.backoff,
                            )
                            await self._apply_back_off()
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

    def put_left_nowait(self, *items: T) -> None:
        """Put items into the queue at the start without blocking."""
        self._queue.put_left_nowait(*items)

    def put_right_nowait(self, *items: T) -> None:
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
        freq: TimeDelta | Sentinel = sentinel,
        backoff: TimeDelta | Sentinel = sentinel,
        logger: str | None | Sentinel = sentinel,
        timeout: TimeDelta | None | Sentinel = sentinel,
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

    def request_back_off(self) -> None:
        """Request the looper to back off."""
        match self._is_pending_back_off.is_set():
            case True:
                _ = self._debug and self._logger.debug(
                    "%s: already requested back off", self
                )
            case False:
                _ = self._debug and self._logger.debug(
                    "%s: requesting back off...", self
                )
                self._is_pending_back_off.set()
            case _ as never:
                assert_never(never)

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
        self.request_back_off()

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

    async def restart(self) -> None:
        """Restart the looper."""
        _ = self._debug and self._logger.debug("%s: restarting...", self)
        self._is_pending_restart.clear()
        async with self._lock:
            self._restart_attempts += 1
        tear_down = await self.tear_down(skip_sleep_if_failure=True)
        initialization = await self.initialize(skip_sleep_if_failure=True)
        match tear_down, initialization:
            case None, None:
                _ = self._debug and self._logger.debug("%s: finished restarting", self)
                async with self._lock:
                    self._restart_successes += 1
            case Exception(), None:
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s whilst restarting (tear down); sleeping for %s...",
                    self,
                    repr_error(tear_down),
                    self.backoff,
                )
                await self._apply_back_off()
            case None, Exception():
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s whilst restarting (initialize); sleeping for %s...",
                    self,
                    repr_error(initialization),
                    self.backoff,
                )
                await self._apply_back_off()
            case Exception(), Exception():
                async with self._lock:
                    self._restart_failures += 1
                _ = self._logger.warning(
                    "%s: encountered %s (tear down) and then %s (initialization) whilst restarting; sleeping for %s...",
                    self,
                    repr_error(tear_down),
                    repr_error(initialization),
                    self.backoff,
                )
                await self._apply_back_off()
            case _ as never:
                assert_never(never)

    async def run_looper(self) -> None:
        """Run the looper."""
        try:
            async with timeout_td(self.timeout):
                while True:
                    if self._is_stopped.is_set():
                        _ = self._debug and self._logger.debug("%s: stopped", self)
                        return
                    if (self._is_pending_stop.is_set()) or (
                        self._is_pending_stop_when_empty.is_set() and self.empty()
                    ):
                        await self.stop()
                    elif self._is_pending_back_off.is_set():
                        await self._apply_back_off()
                    elif self._is_pending_restart.is_set():
                        await self.restart()
                    elif not self._is_initialized.is_set():
                        _ = await self.initialize()
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
                        else:
                            async with self._lock:
                                self._core_successes += 1
                            await sleep_td(self.freq)
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
                await sleep_td(self.freq)

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

    async def tear_down(
        self, *, skip_sleep_if_failure: bool = False
    ) -> Exception | None:
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
                    match skip_sleep_if_failure:
                        case True:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst tearing down",
                                self,
                                repr_error(error),
                            )
                        case False:
                            _ = self._logger.warning(
                                "%s: encountered %s whilst tearing down; sleeping for %s...",
                                self,
                                repr_error(error),
                                self.backoff,
                            )
                            await self._apply_back_off()
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


class UniquePriorityQueue[T: SupportsRichComparison, U: Hashable](
    PriorityQueue[tuple[T, U]]
):
    """Priority queue with unique tasks."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._set: set[U] = set()

    @override
    def _get(self) -> tuple[T, U]:
        item = super()._get()
        _, value = item
        self._set.remove(value)
        return item

    @override
    def _put(self, item: tuple[T, U]) -> None:
        _, value = item
        if value not in self._set:
            super()._put(item)
            self._set.add(value)


class UniqueQueue[T: Hashable](Queue[T]):
    """Queue with unique tasks."""

    @override
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._set: set[T] = set()

    @override
    def _get(self) -> T:
        item = super()._get()
        self._set.remove(item)
        return item

    @override
    def _put(self, item: T) -> None:
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


async def get_items[T](queue: Queue[T], /, *, max_size: int | None = None) -> list[T]:
    """Get items from a queue; if empty then wait."""
    try:
        items = [await queue.get()]
    except RuntimeError as error:  # pragma: no cover
        from utilities.pytest import is_pytest

        if (not is_pytest()) or (error.args[0] != "Event loop is closed"):
            return []
        raise
    max_size_use = None if max_size is None else (max_size - 1)
    items.extend(get_items_nowait(queue, max_size=max_size_use))
    return items


def get_items_nowait[T](queue: Queue[T], /, *, max_size: int | None = None) -> list[T]:
    """Get items from a queue; no waiting."""
    items: list[T] = []
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


async def put_items[T](items: Iterable[T], queue: Queue[T], /) -> None:
    """Put items into a queue; if full then wait."""
    for item in items:
        await queue.put(item)


def put_items_nowait[T](items: Iterable[T], queue: Queue[T], /) -> None:
    """Put items into a queue; no waiting."""
    for item in items:
        queue.put_nowait(item)


##


async def sleep_max(
    sleep: Delta | None = None, /, *, random: Random = SYSTEM_RANDOM
) -> None:
    """Sleep which accepts deltas."""
    if sleep is None:
        return
    await asyncio.sleep(random.uniform(0.0, to_nanoseconds(sleep) / 1e9))


##


async def sleep_rounded(delta: Delta, /) -> None:
    """Sleep until a rounded time."""
    await sleep_until(round_date_or_date_time(get_now(), delta, mode="ceil"))


##


async def sleep_td(delta: Delta | None = None, /) -> None:
    """Sleep which accepts deltas."""
    if delta is None:
        return
    await sleep(to_nanoseconds(delta) / 1e9)


##


async def sleep_until(datetime: ZonedDateTime, /) -> None:
    """Sleep until a given time."""
    await sleep_td(datetime - get_now())


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
async def timeout_td(
    timeout: Delta | None = None, /, *, error: MaybeType[BaseException] = TimeoutError
) -> AsyncIterator[None]:
    """Timeout context manager which accepts deltas."""
    timeout_use = None if timeout is None else (to_nanoseconds(timeout) / 1e9)
    try:
        async with asyncio.timeout(timeout_use):
            yield
    except TimeoutError:
        raise error from None


__all__ = [
    "AsyncDict",
    "EnhancedQueue",
    "EnhancedTaskGroup",
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
    "sleep_max",
    "sleep_rounded",
    "sleep_td",
    "sleep_until",
    "stream_command",
    "timeout_td",
]
