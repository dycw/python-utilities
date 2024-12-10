from __future__ import annotations

import datetime as dt
from asyncio import Lock, Queue, TaskGroup, run, sleep, timeout
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import Phase, given, settings
from hypothesis.strategies import integers, lists
from pytest import mark, param, raises

from tests.conftest import FLAKY
from utilities.asyncio import (
    _MaybeAwaitableMaybeAsyncIterable,
    get_items,
    get_items_nowait,
    is_awaitable,
    sleep_dur,
    stream_command,
    timeout_dur,
    to_list,
    try_await,
)
from utilities.datetime import MILLISECOND, duration_to_timedelta
from utilities.hypothesis import durations
from utilities.pytest import skipif_windows
from utilities.timer import Timer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

    from utilities.types import Duration

_STRS = list("AAAABBBCCDAABB")


def _get_strs_sync() -> Iterable[str]:
    return iter(_STRS)


async def _get_strs_async() -> Iterable[str]:
    return _get_strs_sync()


def _yield_strs_sync() -> Iterator[str]:
    return iter(_get_strs_sync())


async def _yield_strs_async() -> AsyncIterator[str]:
    for i in _get_strs_sync():
        yield i
        await sleep(0.01)


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


class TestIsAwaitable:
    @mark.parametrize(
        ("obj", "expected"), [param(sleep(0.01), True), param(None, False)]
    )
    async def test_main(self, *, obj: Any, expected: bool) -> None:
        result = await is_awaitable(obj)
        assert result is expected


class TestSleepDur:
    @FLAKY
    @given(
        duration=durations(
            min_number=0.0,
            max_number=0.01,
            min_timedelta=dt.timedelta(0),
            max_timedelta=10 * MILLISECOND,
        )
    )
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main(self, *, duration: Duration) -> None:
        with Timer() as timer:
            await sleep_dur(duration=duration)
        assert timer >= duration_to_timedelta(duration / 2)

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
    @FLAKY
    @given(
        duration=durations(
            min_number=0.0,
            max_number=0.01,
            min_timedelta=dt.timedelta(0),
            max_timedelta=10 * MILLISECOND,
        )
    )
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main(self, *, duration: Duration) -> None:
        with raises(TimeoutError):
            async with timeout_dur(duration=duration):
                await sleep_dur(duration=2 * duration)


class TestToList:
    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_main(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await to_list(iterable)
        assert result == _STRS


class TestTryAwait:
    async def test_sync(self) -> None:
        def func(*, value: bool) -> bool:
            return not value

        result = await try_await(func(value=True))
        assert result is False

    async def test_async(self) -> None:
        async def func(*, value: bool) -> bool:
            await sleep(0.01)
            return not value

        result = await try_await(func(value=True))
        assert result is False

    @mark.parametrize("cls", [param(ValueError), param(TypeError)], ids=str)
    async def test_error_std_msg_sync(self, *, cls: type[Exception]) -> None:
        def func(*, value: bool) -> bool:
            if not value:
                msg = f"Value must be True; got {value}"
                raise cls(msg)
            return not value

        with raises(cls, match="Value must be True; got False"):
            _ = await try_await(func(value=False))

    @mark.parametrize("cls", [param(ValueError), param(TypeError)], ids=str)
    async def test_error_std_msg_async(self, *, cls: type[Exception]) -> None:
        async def func(*, value: bool) -> bool:
            if not value:
                msg = f"Value must be True; got {value}"
                raise cls(msg)
            await sleep(0.01)
            return not value

        with raises(cls, match="Value must be True; got False"):
            _ = await try_await(func(value=False))

    @mark.parametrize("cls", [param(ValueError), param(TypeError)], ids=str)
    async def test_error_non_std_msg_sync(self, *, cls: type[Exception]) -> None:
        def func(*, value: bool) -> bool:
            if not value:
                raise cls(value)
            return not value

        with raises(cls, match="False"):
            _ = await try_await(func(value=False))

    @mark.parametrize("cls", [param(ValueError), param(TypeError)], ids=str)
    async def test_error_non_std_msg_async(self, *, cls: type[Exception]) -> None:
        async def func(*, value: bool) -> bool:
            if not value:
                raise cls(value)
            await sleep(0.01)
            return not value

        with raises(cls, match="False"):
            _ = await try_await(func(value=False))


if __name__ == "__main__":
    _ = run(
        stream_command('echo "stdout message" && sleep 2 && echo "stderr message" >&2')
    )
