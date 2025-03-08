from __future__ import annotations

from asyncio import Lock, PriorityQueue, Queue, TaskGroup, run, sleep, timeout
from dataclasses import dataclass, field
from gc import collect
from itertools import chain
from re import search
from typing import TYPE_CHECKING

from hypothesis import Phase, given
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
from more_itertools import unique_everseen
from pytest import mark, raises
from typing_extensions import override

from utilities.asyncio import (
    BoundedTaskGroup,
    QueueProcessor,
    UniquePriorityQueue,
    UniqueQueue,
    get_items,
    get_items_nowait,
    sleep_dur,
    stream_command,
    timeout_dur,
)
from utilities.datetime import MILLISECOND, ZERO_TIME, datetime_duration_to_timedelta
from utilities.hypothesis import (
    datetime_durations,
    settings_with_reduced_examples,
    text_ascii,
)
from utilities.iterables import one
from utilities.pytest import skipif_windows
from utilities.timer import Timer

if TYPE_CHECKING:
    from utilities.types import Duration


class TestBoundedTaskGroup:
    async def test_with(self) -> None:
        with Timer() as timer:
            async with BoundedTaskGroup(max_tasks=2) as tg:
                for _ in range(10):
                    _ = tg.create_task(sleep(0.01))
        assert timer >= 0.05

    async def test_without(self) -> None:
        with Timer() as timer:
            async with BoundedTaskGroup() as tg:
                for _ in range(10):
                    _ = tg.create_task(sleep(0.01))
        assert timer <= 0.02


class TestGetItems:
    @given(
        xs=lists(integers(), min_size=1),
        max_size=integers(1, 10) | none(),
        lock=just(Lock()) | none(),
    )
    async def test_put_then_get(
        self, *, xs: list[int], max_size: int | None, lock: Lock | None
    ) -> None:
        queue: Queue[int] = Queue()
        for x in xs:
            await queue.put(x)
        result = await get_items(queue, max_size=max_size, lock=lock)
        if max_size is None:
            assert result == xs
        else:
            assert result == xs[:max_size]

    @given(
        xs=lists(integers(), min_size=1),
        max_size=integers(1, 10) | none(),
        lock=just(Lock()) | none(),
    )
    async def test_get_then_put(
        self, *, xs: list[int], max_size: int | None, lock: Lock | None
    ) -> None:
        queue: Queue[int] = Queue()

        async def put() -> None:
            await sleep(0.01)
            for x in xs:
                await queue.put(x)

        async with TaskGroup() as tg:
            task = tg.create_task(get_items(queue, max_size=max_size, lock=lock))
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


class TestGetItemsNoWait:
    @given(
        xs=lists(integers(), min_size=1),
        max_size=integers(1, 10) | none(),
        lock=just(Lock()) | none(),
    )
    async def test_main(
        self, *, xs: list[int], max_size: int | None, lock: Lock | None
    ) -> None:
        queue: Queue[int] = Queue()
        for x in xs:
            await queue.put(x)
        result = await get_items_nowait(queue, max_size=max_size, lock=lock)
        if max_size is None:
            assert result == xs
        else:
            assert result == xs[:max_size]


class TestQueueProcessor:
    @given(n=integers(1, 10))
    async def test_one_processor_slow_tasks(self, *, n: int) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, item: int, /) -> None:
                self.output.add(item)

        processor = Example()

        async def yield_tasks() -> None:
            await processor.start()
            await sleep(0.01)
            for i in range(n):
                processor.enqueue(i)
                await sleep(0.01)
            await sleep(0.01)

        async with TaskGroup() as tg:
            _ = tg.create_task(yield_tasks())
            _ = tg.create_task(processor.run_until_empty())
        assert len(processor.output) == n
        await processor.stop()

    @given(n=integers(1, 10))
    async def test_one_processor_slow_run(self, *, n: int) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, item: int, /) -> None:
                self.output.add(item)
                await sleep(0.01)

        processor = Example()
        processor.enqueue(*range(n))
        async with TaskGroup() as tg:
            _ = tg.create_task(processor.run_until_empty())
        assert len(processor.output) == n
        await processor.stop()

    @given(n=integers(1, 10))
    async def test_one_processor_continually_adding(self, *, n: int) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, item: int, /) -> None:
                self.output.add(item)

        processor = Example()
        await processor.start()
        for i in range(n):
            processor.enqueue(i)
            await sleep(0.01)
        assert len(processor.output) == n

    @given(n=integers(0, 10))
    async def test_two_processors(self, *, n: int) -> None:
        @dataclass(kw_only=True)
        class First(QueueProcessor[int]):
            second: Second
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, item: int, /) -> None:
                self.second.enqueue(item)
                self.output.add(item)

        @dataclass(kw_only=True)
        class Second(QueueProcessor[int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, item: int, /) -> None:
                self.output.add(item)

        second = await Second.new()
        first = await First.new(second=second)

        async def yield_tasks() -> None:
            first.enqueue(*range(n))
            await first.run_until_empty()

        async with TaskGroup() as tg:
            _ = tg.create_task(yield_tasks())

        assert len(first.output) == n
        assert len(second.output) == n

    @given(n=integers(0, 10))
    async def test_context_manager(self, *, n: int) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, item: int, /) -> None:
                self.output.add(item)

        processor = Example()
        processor.enqueue(*range(n))
        assert len(processor.output) == 0
        async with processor:
            pass
        assert len(processor.output) == n

    async def test_del_with_task(self) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int, /) -> None:
                _ = item

        processor = await Example.new()
        assert processor._task is not None
        await sleep(0.01)
        del processor
        _ = collect()

    async def test_del_with_none_task(self) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int, /) -> None:
                _ = item

        processor = Example()
        assert processor._task is None
        del processor
        _ = collect()

    async def test_del_without_task_attribute(self) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[int]):
            x: int

            @override
            async def _run(self, item: int, /) -> None:
                _ = item

        with raises(TypeError, match="missing 1 required keyword-only argument: 'x'"):
            _ = Example()  # pyright: ignore[reportCallIssue]

    async def test_empty(self) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int, /) -> None:
                _ = item

        processor = Example()
        assert processor.empty()
        processor.enqueue(0)
        assert not processor.empty()

    @given(n=integers(0, 10))
    async def test_get_items_nowait(self, *, n: int) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[int]):
            output: set[int] = field(default_factory=set)

            @override
            async def _run(self, _: int, /) -> None:
                items = await self._get_items_nowait()
                self.output.add(len(items))

        processor = Example()
        processor.enqueue(*range(n + 1))
        await processor._get_and_run()
        result = one(processor.output)
        assert result == n

    @given(n=integers(0, 10))
    async def test_len(self, *, n: int) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int) -> None:
                _ = item

        processor = Example()
        assert len(processor) == 0
        processor.enqueue(*range(n))
        assert len(processor) == n

    @given(n=integers(0, 10))
    async def test_new(self, *, n: int) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int) -> None:
                _ = item

        processor = await Example.new(*range(n))
        assert len(processor) == n
        assert processor._task is not None

    @given(data=data(), texts=lists(text_ascii(min_size=1), min_size=1))
    async def test_priority_queue(self, *, data: DataObject, texts: list[str]) -> None:
        @dataclass(kw_only=True)
        class Example(QueueProcessor[tuple[int, str]]):
            output: set[str] = field(default_factory=set)

            @override
            async def _run(self, item: tuple[int, str]) -> None:
                _, text = item
                self.output.add(text)

        processor = Example(queue_type=PriorityQueue)
        items = data.draw(permutations(list(enumerate(texts))))
        processor.enqueue(*items)
        await processor._get_and_run()
        result = one(processor.output)
        assert result == texts[0]

    def test_repr(self) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int) -> None:
                _ = item

        queue = Example()
        result = repr(queue)
        expected = "TestQueueProcessor.test_repr.<locals>.Example()"
        assert result == expected

    async def test_start_with_task(self) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int) -> None:
                _ = item

        processor = Example()
        assert processor._task is None
        await processor.start()
        assert processor._task is not None
        await processor.start()
        assert processor._task is not None
        await processor.stop()
        assert processor._task is None

    async def test_stop_without_task(self) -> None:
        class Example(QueueProcessor[int]):
            @override
            async def _run(self, item: int) -> None:
                _ = item

        processor = Example()
        assert processor._task is None
        await processor.start()
        assert processor._task is not None
        await processor.stop()
        assert processor._task is None
        await processor.stop()
        assert processor._task is None


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
    @given(
        duration=datetime_durations(
            min_number=0.1,
            max_number=0.2,
            min_timedelta=100 * MILLISECOND,
            max_timedelta=200 * MILLISECOND,
        )
    )
    @mark.flaky
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_main(self, *, duration: Duration) -> None:
        with Timer() as timer:
            await sleep_dur(duration=duration)
        assert timer >= datetime_duration_to_timedelta(duration / 2)

    async def test_none(self) -> None:
        with Timer() as timer:
            await sleep_dur()
        assert timer <= 0.01


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
    @given(
        duration=datetime_durations(
            min_number=0.0,
            max_number=0.01,
            min_timedelta=ZERO_TIME,
            max_timedelta=10 * MILLISECOND,
        )
    )
    @mark.flaky
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_main(self, *, duration: Duration) -> None:
        with raises(TimeoutError):
            async with timeout_dur(duration=duration):
                await sleep_dur(duration=2 * duration)


if __name__ == "__main__":
    _ = run(
        stream_command('echo "stdout message" && sleep 2 && echo "stderr message" >&2')
    )
