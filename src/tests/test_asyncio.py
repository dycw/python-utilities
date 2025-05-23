from __future__ import annotations

from asyncio import CancelledError, Event, Queue, run, sleep
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import partial
from itertools import chain, count
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast, override

from hypothesis import HealthCheck, Phase, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    integers,
    just,
    lists,
    none,
    permutations,
    sampled_from,
)
from pytest import LogCaptureFixture, mark, param, raises

from utilities.asyncio import (
    EnhancedQueue,
    EnhancedTaskGroup,
    InfiniteLooper,
    InfiniteQueueLooper,
    UniquePriorityQueue,
    UniqueQueue,
    _InfiniteLooperDefaultEventError,
    _InfiniteLooperNoSuchEventError,
    get_event,
    get_items,
    get_items_nowait,
    put_items,
    put_items_nowait,
    sleep_dur,
    sleep_until,
    sleep_until_rounded,
    stream_command,
    timeout_dur,
)
from utilities.dataclasses import replace_non_sentinel
from utilities.datetime import (
    MILLISECOND,
    MINUTE,
    datetime_duration_to_timedelta,
    get_now,
)
from utilities.hypothesis import sentinels, text_ascii
from utilities.iterables import one, unique_everseen
from utilities.pytest import skipif_windows
from utilities.sentinel import Sentinel, sentinel
from utilities.timer import Timer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from utilities.types import (
        Coroutine1,
        Duration,
        DurationOrEveryDuration,
        MaybeCallableEvent,
        MaybeType,
    )


class TestEnhancedQueue:
    @given(
        xs=lists(integers()),
        wait=booleans(),
        put_all=booleans(),
        get_reverse=booleans(),
    )
    async def test_left(
        self, *, xs: list[int], wait: int, put_all: bool, get_reverse: bool
    ) -> None:
        _ = assume(not ((len(xs) == 0) and wait))
        deq: deque[int] = deque()
        for x in xs:
            deq.appendleft(x)
        queue: EnhancedQueue[int] = EnhancedQueue()
        if put_all:
            if wait:
                await queue.put_left(*xs)
            else:
                queue.put_left_nowait(*xs)
        else:
            for i, x in enumerate(xs, start=1):
                if wait:
                    await queue.put_left(x)
                else:
                    queue.put_left_nowait(x)
                assert queue.qsize() == i
        assert list(deq) == xs[::-1]
        if wait:
            res = await queue.get_all(reverse=get_reverse)
        else:
            res = queue.get_all_nowait(reverse=get_reverse)
        expected = xs if get_reverse else xs[::-1]
        assert res == expected

    @given(
        xs=lists(integers()),
        wait=booleans(),
        put_all=booleans(),
        get_reverse=booleans(),
    )
    async def test_right(
        self, *, xs: list[int], wait: int, put_all: bool, get_reverse: bool
    ) -> None:
        _ = assume(not ((len(xs) == 0) and wait))
        deq: deque[int] = deque()
        for x in xs:
            deq.append(x)
        queue: EnhancedQueue[int] = EnhancedQueue()
        if put_all:
            if wait:
                await queue.put_right(*xs)
            else:
                queue.put_right_nowait(*xs)
            assert queue.qsize() == len(xs)
        else:
            for i, x in enumerate(xs, start=1):
                if wait:
                    await queue.put_right(x)
                else:
                    queue.put_right_nowait(x)
                assert queue.qsize() == i
        assert list(deq) == xs
        if wait:
            res = await queue.get_all(reverse=get_reverse)
        else:
            res = queue.get_all_nowait(reverse=get_reverse)
        expected = xs[::-1] if get_reverse else xs
        assert res == expected


class TestEnhancedTaskGroup:
    async def test_create_task_context_coroutine(self) -> None:
        flag: bool = False

        @asynccontextmanager
        async def yield_true() -> AsyncIterator[None]:
            nonlocal flag
            try:
                flag = True
                yield
            finally:
                flag = False

        assert not flag
        async with EnhancedTaskGroup(timeout=0.1) as tg:
            _ = tg.create_task_context(yield_true())
            await sleep(0.05)
            assert flag
        assert not flag

    async def test_create_task_context_looper(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            running: bool = False

            @override
            async def _initialize(self) -> None:
                self.running = True

            @override
            async def _teardown(self) -> None:
                self.running = False

        looper = Example(duration=0.1)
        assert not looper.running
        async with EnhancedTaskGroup(timeout=0.1) as tg:
            assert not looper.running
            _ = tg.create_task_context(looper)
            await sleep(0.05)
            assert looper.running
        assert not looper.running

    async def test_max_tasks_disabled(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup() as tg:
                for _ in range(10):
                    _ = tg.create_task(sleep(0.01))
        assert timer <= 0.05

    async def test_max_tasks_enabled(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(max_tasks=2) as tg:
                for _ in range(10):
                    _ = tg.create_task(sleep(0.01))
        assert timer >= 0.05

    async def test_timeout_pass(self) -> None:
        async with EnhancedTaskGroup(timeout=0.2) as tg:
            _ = tg.create_task(sleep(0.1))

    async def test_timeout_fail(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            async with EnhancedTaskGroup(timeout=0.05) as tg:
                _ = tg.create_task(sleep(0.1))
        assert len(exc_info.value.exceptions) == 1
        error = one(exc_info.value.exceptions)
        assert isinstance(error, TimeoutError)

    async def test_custom_error(self) -> None:
        class CustomError(Exception): ...

        with raises(ExceptionGroup) as exc_info:
            async with EnhancedTaskGroup(timeout=0.05, error=CustomError) as tg:
                _ = tg.create_task(sleep(0.1))
        assert len(exc_info.value.exceptions) == 1
        error = one(exc_info.value.exceptions)
        assert isinstance(error, CustomError)


class TestGetEvent:
    def test_event(self) -> None:
        event = Event()
        assert get_event(event=event) is event

    @given(event=none() | sentinels())
    def test_none_or_sentinel(self, *, event: None | Sentinel) -> None:
        assert get_event(event=event) is event

    def test_replace_non_sentinel(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            event: Event = field(default_factory=Event)

            def replace(
                self, *, event: MaybeCallableEvent | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, event=get_event(event=event))

        event1, event2, event3 = Event(), Event(), Event()
        obj = Example(event=event1)
        assert obj.event is event1
        assert obj.replace().event is event1
        assert obj.replace(event=event2).event is event2
        assert obj.replace(event=lambda: event3).event is event3

    def test_callable(self) -> None:
        event = Event()
        assert get_event(event=lambda: event) is event


class TestGetItems:
    @given(
        xs=lists(integers(), min_size=1),
        max_size=integers(1, 10) | none(),
        wait=booleans(),
    )
    async def test_main(
        self, *, xs: list[int], max_size: int | None, wait: bool
    ) -> None:
        queue: Queue[int] = Queue()
        put_items_nowait(xs, queue)
        if wait:
            result = await get_items(queue, max_size=max_size)
        else:
            result = get_items_nowait(queue, max_size=max_size)
        assert result == xs[:max_size]


class TestInfiniteLooper:
    sleep_restart_cases: ClassVar[list[Any]] = [
        param(60.0, "for 0:01:00"),
        param(MINUTE, "for 0:01:00"),
        param(("every", 60), "until next 0:01:00"),
        param(("every", MINUTE), "until next 0:01:00"),
    ]

    async def test_main_no_errors(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1

        async with timeout_dur(duration=1.0), Example(sleep_core=0.05) as looper:
            pass
        assert 15 <= looper.counter <= 25

    async def test_main_with_errors(self) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            initializations: int = 0
            counter: int = 0
            teardowns: int = 0

            @override
            async def _initialize(self) -> None:
                self.initializations += 1
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= 5:
                    raise CustomError

            @override
            async def _teardown(self) -> None:
                self.teardowns += 1

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.05, sleep_restart=0.05) as looper,
        ):
            pass
        assert 3 <= looper.initializations <= 5
        assert 0 <= looper.counter <= 5
        assert 3 <= looper.teardowns <= 5

    async def test_cancelled_error(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            counter: int = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= 5:
                    raise CancelledError

        async with Example(sleep_core=0.05) as service:
            pass
        assert 5 <= service.counter <= 15

    async def test_duration(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1

        async with Example(duration=1.0, sleep_core=0.05) as looper:
            pass
        assert 15 <= looper.counter <= 25

    async def test_hashable(self) -> None:
        @dataclass(kw_only=True, unsafe_hash=True)
        class Example(InfiniteLooper[None]): ...

        looper = Example(sleep_core=0.1)
        _ = hash(looper)

    async def test_nested_context_manager(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            running: bool = False

            @override
            async def _initialize(self) -> None:
                self.running = True

            @override
            async def _teardown(self) -> None:
                self.running = False

        looper = Example()
        for _ in range(2):
            assert not looper.running
            async with timeout_dur(duration=0.2), looper:
                assert looper.running
                async with timeout_dur(duration=0.1), looper:
                    assert looper.running
                assert looper.running
            assert not looper.running

    def test_repr(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            counter: int = 0

        looper = Example()
        result = repr(looper)
        expected = "TestInfiniteLooper.test_repr.<locals>.Example(counter=0)"
        assert result == expected

    @given(n=integers(10, 11))
    async def test_setting_events(self, *, n: int) -> None:
        class TrueError(Exception): ...

        class FalseError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[bool]):
            counter: int = 0
            true_counter: int = 0
            false_counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= n:
                    self._set_event(event=n % 2 == 0)

            @override
            def _error_upon_core(self, error: Exception, /) -> None:
                if isinstance(error, TrueError):
                    self.true_counter += 1
                elif isinstance(error, FalseError):
                    self.false_counter += 1

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[bool, MaybeType[Exception]]]:
                yield (True, TrueError)
                yield (False, FalseError)

        async with timeout_dur(duration=1.0), Example(sleep_core=0.05) as looper:
            ...
        match n % 2 == 0:
            case True:
                assert looper.true_counter >= 1, looper
                assert looper.false_counter == 0
            case False:
                assert looper.true_counter == 0
                assert looper.false_counter >= 1

    async def test_with_coroutine_self_set_event(self) -> None:
        external: int = 0

        async def inc_external(obj: Example, /) -> None:
            nonlocal external
            for _ in range(100):  # infinite doesn't work on CI
                external += 1
                obj.counter += 1
                await sleep(0.05)

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            initializations: int = 0
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.initializations += 1
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= 5:
                    self._set_event()

            @override
            def _yield_coroutines(self) -> Iterator[Callable[[], Coroutine1[None]]]:
                yield partial(inc_external, self)

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.05, sleep_restart=0.05) as looper,
        ):
            ...
        assert 3 <= looper.initializations <= 7
        assert 0 <= looper.counter <= 8
        assert 13 <= external <= 22

    async def test_with_coroutine_self_error(self) -> None:
        class CustomError(Exception): ...

        async def dummy() -> None:
            _ = await Event().wait()

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            initializations: int = 0
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.initializations += 1
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= 5:
                    raise CustomError

            @override
            def _yield_coroutines(self) -> Iterator[Callable[[], Coroutine1[None]]]:
                yield dummy

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.05, sleep_restart=0.05) as looper,
        ):
            ...
        assert 3 <= looper.initializations <= 5
        assert 0 <= looper.counter <= 5

    async def test_with_looper(self) -> None:
        @dataclass(kw_only=True)
        class Child(InfiniteLooper[None]):
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1

        @dataclass(kw_only=True)
        class Parent(InfiniteLooper[None]):
            counter: int = 0
            child: Child = field(init=False, repr=False)

            @override
            def __post_init__(self) -> None:
                super().__post_init__()
                self.child = Child(sleep_core=self.sleep_core)

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1

            @override
            def _yield_loopers(self) -> Iterator[InfiniteLooper]:
                yield self.child

        async with timeout_dur(duration=1.0), Parent(sleep_core=0.05) as parent:
            ...
        assert 15 <= parent.counter <= 25
        assert 15 <= parent.child.counter <= 25

    async def test_error_default_event(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]): ...

        looper = Example()

        with raises(
            _InfiniteLooperDefaultEventError, match=r"'Example' default event error"
        ):
            raise _InfiniteLooperDefaultEventError(looper=looper)

    @given(logger=just("logger") | none())
    @mark.parametrize(("sleep_restart", "desc"), sleep_restart_cases)
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_upon_initialize(
        self,
        *,
        sleep_restart: DurationOrEveryDuration,
        desc: str,
        logger: str | None,
        caplog: LogCaptureFixture,
    ) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            @override
            async def _initialize(self) -> None:
                raise CustomError

            @override
            async def _core(self) -> None:
                raise NotImplementedError

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.1, sleep_restart=sleep_restart, logger=logger),
        ):
            ...
        if logger is not None:
            message = caplog.messages[0]
            expected = f"'Example' encountered 'CustomError()' whilst initializing; sleeping {desc}..."
            assert message == expected

    @given(logger=just("logger") | none())
    @mark.parametrize(("sleep_restart", "desc"), sleep_restart_cases)
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_upon_core(
        self,
        *,
        sleep_restart: DurationOrEveryDuration,
        desc: str,
        logger: str | None,
        caplog: LogCaptureFixture,
    ) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            @override
            async def _core(self) -> None:
                raise CustomError

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.1, sleep_restart=sleep_restart, logger=logger),
        ):
            ...
        if logger is not None:
            message = caplog.messages[0]
            expected = f"'Example' encountered 'CustomError()'; sleeping {desc}..."
            assert message == expected

    @given(logger=just("logger") | none())
    @mark.parametrize(("sleep_restart", "desc"), sleep_restart_cases)
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_upon_teardown(
        self,
        *,
        sleep_restart: DurationOrEveryDuration,
        desc: str,
        logger: str | None,
        caplog: LogCaptureFixture,
    ) -> None:
        class Custom1Error(Exception): ...

        class Custom2Error(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            counter: int = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= 5:
                    self._set_event()

            @override
            async def _teardown(self) -> None:
                raise Custom2Error

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[None, MaybeType[Exception]]]:
                yield (None, Custom1Error)

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.1, sleep_restart=sleep_restart, logger=logger),
        ):
            ...
        if logger is not None:
            expected = f"'Example' encountered 'Custom2Error()' whilst tearing down; sleeping {desc}..."
            assert expected in caplog.messages

    @given(logger=just("logger") | none())
    @mark.parametrize(("sleep_restart", "desc"), sleep_restart_cases)
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_group_upon_others(
        self,
        *,
        sleep_restart: DurationOrEveryDuration,
        desc: str,
        logger: str | None,
        caplog: LogCaptureFixture,
    ) -> None:
        class CustomError(Exception): ...

        async def dummy() -> None:
            for i in count():
                if i >= 5:
                    raise CustomError
                await sleep(0.05)

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            initializations: int = 0
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.initializations += 1
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1

            @override
            def _yield_coroutines(self) -> Iterator[Callable[[], Coroutine1[None]]]:
                yield dummy

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.05, sleep_restart=sleep_restart, logger=logger),
        ):
            ...
        if logger is not None:
            message = caplog.messages[0]
            expected = f"""\
'Example' encountered 1 error(s):
- Error #1/1: CustomError()
Sleeping {desc}..."""
            assert message == expected

    async def test_error_no_event_found(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteLooper[None]):
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= 10:
                    self._set_event(event=cast("Any", "invalid"))

        with raises(
            _InfiniteLooperNoSuchEventError,
            match="'Example' does not have an event 'invalid'",
        ):
            async with Example():
                ...


class TestInfiniteQueueLooper:
    async def test_main(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            counter: int = 0

            @override
            async def _process_queue(self) -> None:
                self.counter += len(self._queue.get_all_nowait())

        async with timeout_dur(duration=1.0), Example(sleep_core=0.05) as looper:
            await sleep(0.1)
            for i in range(10):
                looper.put_right_nowait(i)
                await sleep(0.05)

            assert looper.counter == 10

    @given(n=integers(1, 10))
    def test_len_and_empty(self, *, n: int) -> None:
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_queue(self) -> None:
                self.output.update(self._queue.get_all_nowait())

        looper = Example(sleep_core=0.05)
        assert len(looper) == 0
        assert looper.empty()
        looper.put_right_nowait(*range(n))
        assert len(looper) == n
        assert not looper.empty()

    async def test_run_until_empty(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_queue(self) -> None:
                self.output.update(self._queue.get_all_nowait())

        looper = Example(sleep_core=0.05)
        looper.put_right_nowait(*range(10))
        async with looper:
            await looper.run_until_empty()
        assert looper.empty()

    @given(logger=just("logger") | none())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_process_items(
        self, *, logger: str | None, caplog: LogCaptureFixture
    ) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_queue(self) -> None:
                raise CustomError

        async with (
            timeout_dur(duration=1.0),
            Example(sleep_core=0.05, logger=logger) as looper,
        ):
            looper.put_left_nowait(1)
        if logger is not None:
            message = caplog.messages[0]
            expected = "'Example' encountered 'CustomError()'; sleeping for 0:01:00..."
            assert message == expected


class TestPutItems:
    @given(xs=lists(integers(), min_size=1), wait=booleans())
    async def test_main(self, *, xs: list[int], wait: bool) -> None:
        queue: Queue[int] = Queue()
        if wait:
            put_items_nowait(xs, queue)
        else:
            await put_items(xs, queue)
        result: list[int] = []
        while not queue.empty():
            result.append(await queue.get())
        assert result == xs


class TestUniquePriorityQueue:
    @given(data=data(), texts=lists(text_ascii(min_size=1), min_size=1, unique=True))
    async def test_main(self, *, data: DataObject, texts: list[str]) -> None:
        items = list(enumerate(texts))
        extra = data.draw(lists(sampled_from(items)))
        items_use = data.draw(permutations(list(chain(items, extra))))
        queue: UniquePriorityQueue[int, str] = UniquePriorityQueue()
        assert queue._set == set()
        for item in items_use:
            await queue.put(item)
        assert queue._set == set(texts)
        result = await get_items(queue)
        assert result == items
        assert queue._set == set()


class TestUniqueQueue:
    @given(x=lists(integers(), min_size=1))
    async def test_main(self, *, x: list[int]) -> None:
        queue: UniqueQueue[int] = UniqueQueue()
        assert queue._set == set()
        for x_i in x:
            await queue.put(x_i)
        assert queue._set == set(x)
        result = await get_items(queue)
        expected = list(unique_everseen(x))
        assert result == expected
        assert queue._set == set()


class TestSleepDur:
    @given(duration=sampled_from([0.1, 10 * MILLISECOND]))
    @settings(phases={Phase.generate})
    async def test_main(self, *, duration: Duration) -> None:
        with Timer() as timer:
            await sleep_dur(duration=duration)
        assert timer >= datetime_duration_to_timedelta(duration / 2)

    async def test_none(self) -> None:
        with Timer() as timer:
            await sleep_dur()
        assert timer <= 0.01


class TestSleepUntil:
    async def test_main(self) -> None:
        await sleep_until(get_now() + 10 * MILLISECOND)


class TestSleepUntilRounded:
    async def test_main(self) -> None:
        await sleep_until_rounded(10 * MILLISECOND)


class TestStreamCommand:
    @skipif_windows
    async def test_main(self) -> None:
        output = await stream_command(
            'echo "stdout message" && sleep 0.1 && echo "stderr message" >&2'
        )
        await sleep(0.01)
        assert output.return_code == 0
        assert output.stdout == "stdout message\n"
        assert output.stderr == "stderr message\n"

    @skipif_windows
    async def test_error(self) -> None:
        output = await stream_command("this-is-an-error")
        await sleep(0.01)
        assert output.return_code == 127
        assert output.stdout == ""
        assert search(
            r"^/bin/sh: (1: )?this-is-an-error: (command )?not found$", output.stderr
        )


class TestTimeoutDur:
    async def test_pass(self) -> None:
        async with timeout_dur(duration=0.2):
            await sleep(0.1)

    async def test_fail(self) -> None:
        with raises(TimeoutError):
            async with timeout_dur(duration=0.05):
                await sleep(0.1)

    async def test_custom_error(self) -> None:
        class CustomError(Exception): ...

        with raises(CustomError):
            async with timeout_dur(duration=0.05, error=CustomError):
                await sleep(0.1)


if __name__ == "__main__":
    _ = run(
        stream_command('echo "stdout message" && sleep 2 && echo "stderr message" >&2')
    )
