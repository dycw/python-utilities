from __future__ import annotations

from asyncio import Queue, run
from collections.abc import ItemsView, KeysView, ValuesView
from contextlib import asynccontextmanager
from re import search
from typing import TYPE_CHECKING, ClassVar

from hypothesis import given
from hypothesis.strategies import booleans, dictionaries, integers, lists, none
from pytest import RaisesGroup, raises

from utilities.asyncio import (
    AsyncDict,
    EnhancedTaskGroup,
    get_items,
    get_items_nowait,
    loop_until_succeed,
    put_items,
    put_items_nowait,
    sleep_max,
    sleep_rounded,
    sleep_td,
    sleep_until,
    stream_command,
    timeout_td,
)
from utilities.hypothesis import pairs, text_ascii
from utilities.pytest import skipif_windows
from utilities.timer import Timer
from utilities.whenever import MILLISECOND, SECOND, get_now

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from whenever import TimeDelta


async_dicts = dictionaries(text_ascii(), integers()).map(AsyncDict)


class TestAsyncDict:
    @given(dict_=async_dicts)
    async def test_aenter(self, *, dict_: AsyncDict[str, int]) -> None:
        async with dict_:
            ...

    @given(dict_=async_dicts)
    async def test_clear(self, *, dict_: AsyncDict[str, int]) -> None:
        await dict_.clear()
        assert len(dict_) == 0

    @given(dict_=async_dicts, key=text_ascii())
    def test_contains(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        assert isinstance(key in dict_, bool)

    @given(dict_=async_dicts)
    def test_copy(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(dict_.copy(), AsyncDict)

    @given(dict_=async_dicts, key=text_ascii())
    async def test_del(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        if key in dict_:
            await dict_.del_(key)
        else:
            with raises(KeyError):
                await dict_.del_(key)

    @given(dict_=async_dicts)
    def test_empty(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(dict_.empty, bool)

    @given(dicts=pairs(async_dicts))
    def test_eq(
        self, *, dicts: tuple[AsyncDict[str, int], AsyncDict[str, int]]
    ) -> None:
        first, second = dicts
        assert isinstance(first == second, bool)

    @given(keys=lists(text_ascii()))
    def test_fromkeys(self, *, keys: list[str]) -> None:
        dict_ = AsyncDict.fromkeys(keys)
        assert isinstance(dict_, AsyncDict)

    @given(dict_=async_dicts, key=text_ascii())
    def test_get(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        if key in dict_:
            assert isinstance(dict_.get(key), int)
        else:
            assert dict_.get(key) is None

    @given(dict_=async_dicts, key=text_ascii())
    def test_get_default(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        value = dict_.get(key, None)
        assert isinstance(value, int) or (value is None)

    @given(dict_=async_dicts, key=text_ascii())
    def test_getitem(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        if key in dict_:
            assert isinstance(dict_[key], int)
        else:
            with raises(KeyError):
                _ = dict_[key]

    @given(dict_=async_dicts)
    def test_items(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(dict_.items(), ItemsView)
        for key, value in dict_.items():
            assert isinstance(key, str)
            assert isinstance(value, int)

    @given(dict_=async_dicts)
    def test_iter(self, *, dict_: AsyncDict[str, int]) -> None:
        for key in dict_:
            assert isinstance(key, str)

    @given(dict_=async_dicts)
    def test_keys(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(dict_.keys(), KeysView)
        for key in dict_.keys():  # noqa: SIM118
            assert isinstance(key, str)

    @given(dict_=async_dicts)
    def test_len(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(len(dict_), int)

    @given(dict_=async_dicts, key=text_ascii())
    async def test_pop(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        if key in dict_:
            assert isinstance(await dict_.pop(key), int)
        else:
            with raises(KeyError):
                _ = await dict_.pop(key)

    @given(dict_=async_dicts, key=text_ascii())
    async def test_pop_default(self, *, dict_: AsyncDict[str, int], key: str) -> None:
        value = await dict_.pop(key, None)
        assert isinstance(value, int) or (value is None)

    @given(dict_=async_dicts)
    async def test_popitem(self, *, dict_: AsyncDict[str, int]) -> None:
        if len(dict_) >= 1:
            key, value = await dict_.popitem()
            assert isinstance(key, str)
            assert isinstance(value, int)
        else:
            with raises(KeyError):
                _ = await dict_.popitem()

    @given(dict_=async_dicts)
    def test_repr(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(repr(dict_), str)

    @given(dict_=async_dicts)
    def test_reversed(self, *, dict_: AsyncDict[str, int]) -> None:
        for key in reversed(dict_):
            assert isinstance(key, str)

    @given(dict_=async_dicts, key=text_ascii(), value=integers())
    async def test_set(
        self, *, dict_: AsyncDict[str, int], key: str, value: int
    ) -> None:
        await dict_.set(key, value)

    @given(dict_=async_dicts, key=text_ascii(), value=integers())
    async def test_setdefault(
        self, *, dict_: AsyncDict[str, int], key: str, value: int
    ) -> None:
        assert isinstance(await dict_.setdefault(key, value), int)

    @given(dict_=async_dicts)
    def test_str(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(str(dict_), str)

    @given(dicts=pairs(async_dicts))
    async def test_update(
        self, *, dicts: tuple[AsyncDict[str, int], AsyncDict[str, int]]
    ) -> None:
        first, second = dicts
        await first.update(second)

    @given(dict_=async_dicts)
    def test_values(self, *, dict_: AsyncDict[str, int]) -> None:
        assert isinstance(dict_.values(), ValuesView)
        for value in dict_.values():
            assert isinstance(value, int)


class TestEnhancedTaskGroup:
    delta: ClassVar[TimeDelta] = 0.05 * SECOND

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
        async with EnhancedTaskGroup(timeout=2 * self.delta) as tg:
            _ = tg.create_task_context(yield_true())
            await sleep_td(self.delta)
            assert flag
        assert not flag

    async def test_max_tasks_disabled(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup() as tg:
                for _ in range(10):
                    _ = tg.create_task(sleep_td(self.delta))
        assert timer <= 2 * self.delta

    async def test_max_tasks_enabled(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(max_tasks=2) as tg:
                for _ in range(10):
                    _ = tg.create_task(sleep_td(self.delta))
        assert timer >= 5 * self.delta

    async def test_run_or_create_many_tasks_parallel_with_max_tasks_two(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(max_tasks=2) as tg:
                assert not tg._is_debug()
                for _ in range(10):
                    _ = await tg.run_or_create_many_tasks(sleep_td, self.delta)
        assert timer >= 5 * self.delta

    async def test_run_or_create_many_tasks_serial_with_debug(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(debug=True) as tg:
                assert tg._is_debug()
                for _ in range(10):
                    _ = await tg.run_or_create_many_tasks(sleep_td, self.delta)
        assert timer >= 10 * self.delta

    async def test_run_or_create_task_parallel_with_max_tasks_none(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup() as tg:
                assert not tg._is_debug()
                for _ in range(10):
                    _ = await tg.run_or_create_task(sleep_td(self.delta))
        assert timer <= 2 * self.delta

    async def test_run_or_create_task_parallel_with_max_tasks_two(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(max_tasks=2) as tg:
                assert not tg._is_debug()
                for _ in range(10):
                    _ = await tg.run_or_create_task(sleep_td(self.delta))
        assert timer >= 5 * self.delta

    async def test_run_or_create_task_serial_with_max_tasks_negative(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(max_tasks=-1) as tg:
                assert tg._is_debug()
                for _ in range(10):
                    _ = await tg.run_or_create_task(sleep_td(self.delta))
        assert timer >= 10 * self.delta

    async def test_run_or_create_task_serial_with_debug(self) -> None:
        with Timer() as timer:
            async with EnhancedTaskGroup(debug=True) as tg:
                assert tg._is_debug()
                for _ in range(10):
                    _ = await tg.run_or_create_task(sleep_td(self.delta))
        assert timer >= 10 * self.delta

    async def test_timeout_pass(self) -> None:
        async with EnhancedTaskGroup(timeout=2 * self.delta) as tg:
            _ = tg.create_task(sleep_td(self.delta))

    async def test_timeout_fail(self) -> None:
        with RaisesGroup(TimeoutError):
            async with EnhancedTaskGroup(timeout=self.delta) as tg:
                _ = tg.create_task(sleep_td(2 * self.delta))

    async def test_custom_error(self) -> None:
        class CustomError(Exception): ...

        with RaisesGroup(CustomError):
            async with EnhancedTaskGroup(timeout=self.delta, error=CustomError) as tg:
                _ = tg.create_task(sleep_td(2 * self.delta))


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


class TestLoopUntilSucceed:
    async def test_main(self) -> None:
        counter = 0

        async def func() -> None:
            nonlocal counter
            counter += 1
            if counter <= 3:
                raise ValueError

        _ = await loop_until_succeed(lambda: func())
        assert counter == 4

    async def test_error(self) -> None:
        counter = 0
        errors: list[Exception] = []

        async def func() -> None:
            nonlocal counter
            counter += 1
            if counter <= 3:
                raise ValueError

        def error(error: Exception, /) -> None:
            errors.append(error)

        _ = await loop_until_succeed(lambda: func(), error=error)
        assert counter == 4
        assert len(errors) == 3


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


class TestSleepMaxDur:
    delta: ClassVar[TimeDelta] = 0.05 * SECOND

    async def test_main(self) -> None:
        with Timer() as timer:
            await sleep_max(self.delta)
        assert timer <= 2 * self.delta

    async def test_none(self) -> None:
        with Timer() as timer:
            await sleep_max()
        assert timer <= self.delta


class TestSleepTD:
    delta: ClassVar[TimeDelta] = 0.05 * SECOND

    async def test_main(self) -> None:
        with Timer() as timer:
            await sleep_td(self.delta)
        assert timer <= 2 * self.delta

    async def test_none(self) -> None:
        with Timer() as timer:
            await sleep_td()
        assert timer <= self.delta


class TestSleepUntil:
    async def test_main(self) -> None:
        await sleep_until(get_now() + 0.05 * SECOND)


class TestSleepUntilRounded:
    async def test_main(self) -> None:
        await sleep_rounded(10 * MILLISECOND)


class TestStreamCommand:
    delta: ClassVar[TimeDelta] = 0.05 * SECOND

    @skipif_windows
    async def test_main(self) -> None:
        output = await stream_command(
            'echo "stdout message" && sleep 0.1 && echo "stderr message" >&2'
        )
        await sleep_td(self.delta)
        assert output.return_code == 0
        assert output.stdout == "stdout message\n"
        assert output.stderr == "stderr message\n"

    @skipif_windows
    async def test_error(self) -> None:
        output = await stream_command("this-is-an-error")
        await sleep_td(self.delta)
        assert output.return_code == 127
        assert output.stdout == ""
        assert search(
            r"^/bin/sh: (1: )?this-is-an-error: (command )?not found$", output.stderr
        )


class TestTimeoutTD:
    delta: ClassVar[TimeDelta] = 0.05 * SECOND

    async def test_pass(self) -> None:
        async with timeout_td(2 * self.delta):
            await sleep_td(self.delta)

    async def test_fail(self) -> None:
        with raises(TimeoutError):
            async with timeout_td(self.delta):
                await sleep_td(2 * self.delta)

    async def test_custom_error(self) -> None:
        class CustomError(Exception): ...

        with raises(CustomError):
            async with timeout_td(self.delta, error=CustomError):
                await sleep_td(2 * self.delta)


if __name__ == "__main__":
    _ = run(
        stream_command('echo "stdout message" && sleep 2 && echo "stderr message" >&2')
    )
