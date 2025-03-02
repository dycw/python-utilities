from __future__ import annotations

from asyncio import Lock, Queue, TaskGroup, run, sleep, timeout
from re import search
from typing import TYPE_CHECKING

from hypothesis import Phase, given, settings
from hypothesis.strategies import integers, lists
from pytest import raises

from utilities.asyncio import (
    BoundedTaskGroup,
    get_items,
    get_items_nowait,
    sleep_dur,
    stream_command,
    timeout_dur,
)
from utilities.datetime import MILLISECOND, ZERO_TIME, datetime_duration_to_timedelta
from utilities.hypothesis import datetime_durations
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
    @given(xs=lists(integers(), min_size=1))
    async def test_put_then_get(self, *, xs: list[int]) -> None:
        queue: Queue[int] = Queue()
        for x in xs:
            queue.put_nowait(x)
        result = await get_items(queue)
        assert result == xs

    @given(xs=lists(integers(), min_size=1))
    async def test_get_then_put(self, *, xs: list[int]) -> None:
        queue: Queue[int] = Queue()

        async def put() -> None:
            await sleep(0.01)
            for x in xs:
                queue.put_nowait(x)

        async with TaskGroup() as tg:
            task = tg.create_task(get_items(queue))
            _ = tg.create_task(put())
        result = task.result()
        assert result == xs

    async def test_empty(self) -> None:
        queue: Queue[int] = Queue()
        with raises(TimeoutError):  # noqa: PT012
            async with timeout(0.01), TaskGroup() as tg:
                _ = tg.create_task(get_items(queue))
                _ = tg.create_task(sleep(0.02))


class TestGetItemsNoWait:
    @given(xs=lists(integers()))
    async def test_main(self, *, xs: list[int]) -> None:
        queue: Queue[int] = Queue()
        for x in xs:
            queue.put_nowait(x)
        result = await get_items_nowait(queue)
        assert result == xs

    @given(xs=lists(integers()))
    async def test_lock(self, *, xs: list[int]) -> None:
        queue: Queue[int] = Queue()
        for x in xs:
            queue.put_nowait(x)
        lock = Lock()
        result = await get_items_nowait(queue, lock=lock)
        assert result == xs


class TestSleepDur:
    @given(
        duration=datetime_durations(
            min_number=0.0,
            max_number=0.01,
            min_timedelta=ZERO_TIME,
            max_timedelta=10 * MILLISECOND,
        )
    )
    @settings(max_examples=1, phases={Phase.generate})
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
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main(self, *, duration: Duration) -> None:
        with raises(TimeoutError):
            async with timeout_dur(duration=duration):
                await sleep_dur(duration=2 * duration)


if __name__ == "__main__":
    _ = run(
        stream_command('echo "stdout message" && sleep 2 && echo "stderr message" >&2')
    )
