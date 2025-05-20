from __future__ import annotations

from asyncio import CancelledError, Event, Queue, TaskGroup, run, sleep, timeout
from dataclasses import dataclass, field
from functools import partial
from itertools import chain, count
from re import search
from typing import TYPE_CHECKING, Self, override

from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import (
    DataObject,
    data,
    integers,
    just,
    lists,
    none,
    permutations,
    sampled_from,
)
from pytest import LogCaptureFixture, mark, raises

from utilities.asyncio import (
    EnhancedTaskGroup,
    InfiniteLooper,
    InfiniteLooperError,
    InfiniteQueueLooper,
    InfiniteQueueLooperError,
    UniquePriorityQueue,
    UniqueQueue,
    _DurationOrEvery,
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
    from collections.abc import Callable, Iterator

    from utilities.types import Coroutine1, Duration, MaybeCallableEvent, MaybeType


class TestEnhancedTaskGroup:
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


class TestInfiniteLooper:
    @given(n=integers(10, 11), sleep_core=sampled_from([0.1, ("every", 0.1)]))
    async def test_main(self, *, n: int, sleep_core: _DurationOrEvery) -> None:
        class TrueError(BaseException): ...

        class FalseError(BaseException): ...

        @dataclass(kw_only=True)
        class Example(InfiniteLooper[bool]):
            counter: int = 0

            @override
            async def _initialize(self) -> None:
                self.counter = 0

            @override
            async def _core(self) -> None:
                self.counter += 1
                if self.counter >= n:
                    self._set_event(n % 2 == 0)

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[bool, MaybeType[BaseException]]]:
                yield (True, TrueError)
                yield (False, FalseError)

        looper = Example(sleep_core=sleep_core)
        match n % 2 == 0:
            case True:
                with raises(TrueError):
                    _ = await looper()
            case False:
                with raises(FalseError):
                    _ = await looper()

    async def test_hashable(self) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True, unsafe_hash=True)
        class Example(InfiniteLooper[None]):
            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[None, MaybeType[Exception]]]:
                yield (None, CustomError)

        looper = Example(sleep_core=0.1)
        _ = hash(looper)

    async def test_with_coroutine_self_set_event(self) -> None:
        external: int = 0

        async def inc_external(obj: Example, /) -> None:
            nonlocal external
            while True:
                external += 1
                obj.counter += 1
                await sleep(0.05)

        class CustomError(Exception): ...

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
                    self._set_event(None)

            @override
            def _yield_coroutines(self) -> Iterator[Callable[[], Coroutine1[None]]]:
                yield partial(inc_external, self)

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[None, MaybeType[BaseException]]]:
                yield (None, CustomError)

        looper = Example(sleep_core=0.05, sleep_restart=0.05)
        with raises(TimeoutError):
            async with timeout_dur(duration=1.0):
                await looper()
        assert 4 <= looper.initializations <= 6
        assert 0 <= looper.counter <= 7
        assert 16 <= external <= 21

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

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[None, MaybeType[BaseException]]]:
                yield (None, CustomError)

        looper = Example(sleep_core=0.05, sleep_restart=0.05)
        with raises(TimeoutError):
            async with timeout_dur(duration=1.0):
                await looper()
        assert 3 <= looper.initializations <= 5
        assert 0 <= looper.counter <= 5

    @given(logger=just("logger") | none())
    async def test_with_coroutine_other_coroutine_error(
        self, *, logger: str | None
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

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[None, MaybeType[BaseException]]]:
                yield (None, CustomError)

        looper = Example(sleep_core=0.05, sleep_restart=0.05, logger=logger)
        with raises(CancelledError):
            async with timeout_dur(duration=1.0):
                await looper()
        assert 3 <= looper.initializations <= 5
        assert 1 <= looper.counter <= 6

    @given(logger=just("logger") | none())
    @mark.parametrize(
        ("sleep_restart", "desc"),
        [
            (60.0, "for 0:01:00"),
            (MINUTE, "for 0:01:00"),
            (("every", 60), "until next 0:01:00"),
            (("every", MINUTE), "until next 0:01:00"),
        ],
    )
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_upon_initialize(
        self,
        *,
        sleep_restart: _DurationOrEvery,
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

        looper = Example(sleep_core=0.1, sleep_restart=sleep_restart, logger=logger)
        with raises(TimeoutError):
            async with timeout_dur(duration=0.5):
                _ = await looper()
        if logger is not None:
            message = caplog.messages[0]
            expected = f"'Example' encountered 'CustomError()' whilst initializing; sleeping {desc}..."
            assert message == expected

    @given(logger=just("logger") | none())
    @mark.parametrize(
        ("sleep_restart", "desc"),
        [
            (60.0, "for 0:01:00"),
            (MINUTE, "for 0:01:00"),
            (("every", 60), "until next 0:01:00"),
            (("every", MINUTE), "until next 0:01:00"),
        ],
    )
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_group_upon_coroutines(
        self,
        *,
        sleep_restart: _DurationOrEvery,
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

            @override
            def _yield_events_and_exceptions(
                self,
            ) -> Iterator[tuple[None, MaybeType[BaseException]]]:
                yield (None, CustomError)

        looper = Example(sleep_core=0.1, sleep_restart=sleep_restart, logger=logger)
        with raises(TimeoutError):
            async with timeout_dur(duration=0.5):
                _ = await looper()
        if logger is not None:
            message = caplog.messages[0]
            expected = f"'Example' encountered 'CustomError()'; sleeping {desc}..."
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
                    self._set_event(None)

        looper = Example(sleep_core=0.1)
        with raises(InfiniteLooperError, match="'Example' does not have an event None"):
            _ = await looper()


class TestInfiniteQueueLooper:
    async def test_main(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_items(self, *items: int) -> None:
                self.output.update(items)

        looper = Example(sleep_core=0.05)

        async def add_items() -> None:
            for i in count():
                looper.put_items_nowait(i)
                await sleep(0.05)

        with raises(ExceptionGroup):  # noqa: PT012
            async with EnhancedTaskGroup(timeout=1.0) as tg:
                _ = tg.create_task(looper())
                _ = tg.create_task(add_items())
        assert 15 <= len(looper.output) <= 20

    @given(n=integers(1, 10))
    def test_len_and_empty(self, *, n: int) -> None:
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_items(self, *items: int) -> None:
                self.output.update(items)

        looper = Example(sleep_core=0.05)
        assert len(looper) == 0
        assert looper.empty()
        looper.put_items_nowait(*range(n))
        assert len(looper) == n
        assert not looper.empty()

    async def test_no_items(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_items(self, *items: int) -> None:
                self.output.update(items)

        looper = Example(sleep_core=0.05)
        with raises(TimeoutError):
            async with timeout_dur(duration=0.5):
                _ = await looper()

    async def test_run_until_empty(self) -> None:
        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_items(self, *items: int) -> None:
                self.output.update(items)

        looper = Example(sleep_core=0.5)

        async def add_items() -> None:
            for i in count():
                looper.put_items_nowait(i)
                await sleep(0.01)

        with raises(ExceptionGroup):  # noqa: PT012
            async with EnhancedTaskGroup(timeout=1.0) as tg:
                _ = tg.create_task(looper())
                _ = tg.create_task(add_items())

        tasks = len(looper)
        assert tasks >= 1
        await sleep(0.1)
        assert len(looper) == tasks
        await looper.run_until_empty()
        assert looper.empty()

    @given(logger=just("logger") | none())
    async def test_error_process_items(self, *, logger: str | None) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _process_items(self, *items: int) -> None:
                raise CustomError(*items)

        looper = Example(sleep_core=0.05, logger=logger)
        looper.put_items_nowait(1)
        with raises(TimeoutError):
            async with timeout_dur(duration=0.5):
                _ = await looper()

    async def test_error_infinite_queue_looper(self) -> None:
        class CustomError(Exception): ...

        @dataclass(kw_only=True)
        class Example(InfiniteQueueLooper[None, int]):
            @override
            async def _process_items(self, *items: int) -> None:
                raise CustomError(*items)

        looper = Example(sleep_core=0.1)
        looper.put_items_nowait(1)
        with raises(
            InfiniteQueueLooperError,
            match=r"'Example' encountered CustomError\(1\) whilst processing 1 item\(s\): \[1\]",
        ):
            _ = await looper._core()


class TestPutAndGetItems:
    @given(xs=lists(integers(), min_size=1), max_size=integers(1, 10) | none())
    async def test_put_then_get(self, *, xs: list[int], max_size: int | None) -> None:
        queue: Queue[int] = Queue()
        await put_items(xs, queue)
        result = await get_items(queue, max_size=max_size)
        if max_size is None:
            assert result == xs
        else:
            assert result == xs[:max_size]

    @given(xs=lists(integers(), min_size=1), max_size=integers(1, 10) | none())
    async def test_get_then_put(self, *, xs: list[int], max_size: int | None) -> None:
        queue: Queue[int] = Queue()

        async def put() -> None:
            await sleep(0.01)
            await put_items(xs, queue)

        async with TaskGroup() as tg:
            task = tg.create_task(get_items(queue, max_size=max_size))
            _ = tg.create_task(put())
        result = task.result()
        if max_size is None:
            assert result == xs
        else:
            assert result == xs[:max_size]

    async def test_empty(self) -> None:
        queue: Queue[int] = Queue()
        with raises(TimeoutError):  # noqa: PT012
            async with timeout(0.01), TaskGroup() as tg:
                _ = tg.create_task(get_items(queue))
                _ = tg.create_task(sleep(0.02))


class TestPutAndGetItemsNoWait:
    @given(xs=lists(integers(), min_size=1), max_size=integers(1, 10) | none())
    def test_main(self, *, xs: list[int], max_size: int | None) -> None:
        queue: Queue[int] = Queue()
        put_items_nowait(xs, queue)
        result = get_items_nowait(queue, max_size=max_size)
        if max_size is None:
            assert result == xs
        else:
            assert result == xs[:max_size]


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
