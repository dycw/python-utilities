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
from dataclasses import dataclass
from io import StringIO
from itertools import chain
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

from utilities.functions import ensure_int, ensure_not_none, to_bool
from utilities.random import SYSTEM_RANDOM
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    Delta,
    MaybeCallableBool,
    SupportsKeysAndGetItem,
    SupportsRichComparison,
)
from utilities.whenever import get_now, round_date_or_date_time, to_nanoseconds

if TYPE_CHECKING:
    from asyncio import _CoroutineLike
    from asyncio.subprocess import Process
    from collections import deque
    from collections.abc import AsyncIterator, Sequence
    from contextvars import Context
    from random import Random
    from types import TracebackType

    from whenever import ZonedDateTime

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
            raise
        return []
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
