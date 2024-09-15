from __future__ import annotations

from asyncio import sleep
from dataclasses import dataclass
from functools import partial
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any, ClassVar

from hypothesis import given
from pytest import CaptureFixture, mark, param, raises

from utilities.asyncio import (
    ReduceAsyncError,
    _MaybeAwaitableMaybeAsyncIterable,
    groupby_async,
    groupby_async_list,
    is_awaitable,
    reduce_async,
    send_and_next_async,
    start_async_generator_coroutine,
    timeout_dur,
    to_list,
    to_set,
    to_sorted,
    try_await,
)
from utilities.functions import ensure_not_none
from utilities.hypothesis import durations

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Iterable,
        Iterator,
        Sequence,
    )

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


@dataclass(frozen=True, kw_only=True)
class _Container:
    text: str


def _get_containers_sync() -> Iterable[_Container]:
    return (_Container(text=t) for t in _get_strs_sync())


async def _get_containers_async() -> Iterable[_Container]:
    return _get_containers_sync()


def _yield_containers_sync() -> Iterator[_Container]:
    return iter(_get_containers_sync())


async def _yield_containers_async() -> AsyncIterator[_Container]:
    for i in _get_containers_sync():
        yield i
        await sleep(0.01)


async def _ord_async(text: str, /) -> int:
    await sleep(0.01)
    return ord(text)


class TestGroupbyAsync:
    exp_no_key: ClassVar[list[tuple[str, list[str]]]] = [
        ("A", list(repeat("A", times=4))),
        ("B", list(repeat("B", times=3))),
        ("C", list(repeat("C", times=2))),
        ("D", list(repeat("D", times=1))),
        ("A", list(repeat("A", times=2))),
        ("B", list(repeat("B", times=2))),
    ]
    exp_with_key: ClassVar[list[tuple[int, list[str]]]] = [
        (65, list(repeat("A", times=4))),
        (66, list(repeat("B", times=3))),
        (67, list(repeat("C", times=2))),
        (68, list(repeat("D", times=1))),
        (65, list(repeat("A", times=2))),
        (66, list(repeat("B", times=2))),
    ]

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_no_key(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = groupby_async(iterable)
        as_list: list[tuple[str, list[str]]] = []
        async for k, v in await result:
            assert isinstance(k, str)
            assert isinstance(v, list)
            for v_i in v:
                assert isinstance(v_i, str)
            as_list.append((k, v))
        assert as_list == self.exp_no_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_no_key_as_list(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await groupby_async_list(iterable)
        assert result == self.exp_no_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_sync(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = groupby_async(iterable, key=ord)
        as_list: list[tuple[int, list[str]]] = []
        async for k, v in await result:
            assert isinstance(k, int)
            assert isinstance(v, list)
            assert all(isinstance(v_i, str) for v_i in v)
            as_list.append((k, v))
        assert as_list == self.exp_with_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_sync_list(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await groupby_async_list(iterable, key=ord)
        assert result == self.exp_with_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_async(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = groupby_async(iterable, key=_ord_async)
        as_list: list[tuple[int, list[str]]] = []
        async for k, v in await result:
            assert isinstance(k, int)
            assert isinstance(v, list)
            assert all(isinstance(v_i, str) for v_i in v)
            as_list.append((k, v))
        assert as_list == self.exp_with_key

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_key_async_list(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await groupby_async_list(iterable, key=_ord_async)
        assert result == self.exp_with_key


class TestIsAwaitable:
    @mark.parametrize(
        ("obj", "expected"), [param(sleep(0.01), True), param(None, False)]
    )
    async def test_main(self, *, obj: Any, expected: bool) -> None:
        result = await is_awaitable(obj)
        assert result is expected


class TestReduceAsync:
    async def test_no_initial(self) -> None:
        async def add(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        result = await reduce_async(add, [1, 2, 3])
        assert result == 6

    async def test_no_initial_with_partial(self) -> None:
        async def add(x: int, y: int, /, *, z: int) -> int:
            await sleep(0.01)
            return x + y + z

        result = await reduce_async(partial(add, z=1), [1, 2, 3])
        assert result == 8

    async def test_with_initial(self) -> None:
        async def collect(x: Iterable[int], y: int, /) -> Sequence[int]:
            await sleep(0.01)
            return list(chain(x, [y]))

        result = await reduce_async(collect, [1, 2, 3], initial=[])
        assert result == [1, 2, 3]

    async def test_with_initial_with_partial(self) -> None:
        async def collect(x: Iterable[int], y: int, /, *, z: int) -> Sequence[int]:
            await sleep(0.01)
            return list(chain(x, [y, z]))

        result = await reduce_async(partial(collect, z=0), [1, 2, 3], initial=[])
        assert result == [1, 0, 2, 0, 3, 0]

    async def test_empty(self) -> None:
        async def add(x: int, y: int, /) -> int:
            await sleep(0.01)
            return x + y

        with raises(ReduceAsyncError, match="Empty iterable .* with no initial value"):
            _ = await reduce_async(add, [])


class TestSendAndNextAsync:
    async def test_main(self, *, capsys: CaptureFixture) -> None:
        @start_async_generator_coroutine
        async def func() -> AsyncGenerator[int | None, float | None]:
            print("Initial")  # noqa: T201
            while True:
                input_ = ensure_not_none((yield))
                output = round(input_)
                if output >= 0:
                    print(f"Received {input_}, yielding {output}")  # noqa: T201
                    yield output
                else:
                    break

        generator = await func()
        out = capsys.readouterr().out
        assert out == "Initial\n", out
        result = await send_and_next_async(0.1, generator)
        assert result == 0
        out = capsys.readouterr().out
        assert out == "Received 0.1, yielding 0\n", out
        result = await send_and_next_async(0.9, generator)
        assert result == 1
        out = capsys.readouterr().out
        assert out == "Received 0.9, yielding 1\n", out
        result = await send_and_next_async(1.1, generator)
        assert result == 1
        out = capsys.readouterr().out
        assert out == "Received 1.1, yielding 1\n", out
        with raises(StopAsyncIteration) as exc:
            _ = await send_and_next_async(-0.9, generator)
        assert exc.value.args == ()


class TestStartAsyncGeneratorCoroutine:
    async def test_main(self, *, capsys: CaptureFixture) -> None:
        @start_async_generator_coroutine
        async def func() -> AsyncGenerator[int, float]:
            print("Pre-initial")  # noqa: T201
            x = yield 0
            print(f"Post-initial; x={x}")  # noqa: T201
            while x >= 0:
                print(f"Pre-yield; x={x}")  # noqa: T201
                x = yield round(x)
                print(f"Post-yield; x={x}")  # noqa: T201
                await sleep(0.01)

        generator = await func()
        out = capsys.readouterr().out
        assert out == "Pre-initial\n", out
        assert await generator.asend(0.1) == 0
        out = capsys.readouterr().out
        assert out == "Post-initial; x=0.1\nPre-yield; x=0.1\n", out
        assert await generator.asend(0.9) == 1
        out = capsys.readouterr().out
        assert out == "Post-yield; x=0.9\nPre-yield; x=0.9\n", out
        assert await generator.asend(1.1) == 1
        out = capsys.readouterr().out
        assert out == "Post-yield; x=1.1\nPre-yield; x=1.1\n", out
        with raises(StopAsyncIteration) as exc:
            _ = await generator.asend(-0.9)
        assert exc.value.args == ()


class TestTimeoutDur:
    @given(duration=durations())
    async def test_main(self, *, duration: Duration) -> None:
        async with timeout_dur(duration=duration):
            pass


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


class TestToSet:
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
        result = await to_set(iterable)
        assert result == set(_STRS)


class TestToSorted:
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
        result = await to_sorted(iterable)
        expected = sorted(_STRS)
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_containers_sync()),
            param(_get_containers_async()),
            param(_yield_containers_sync()),
            param(_yield_containers_async()),
        ],
    )
    async def test_key_sync(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[_Container]
    ) -> None:
        result = await to_sorted(iterable, key=lambda c: c.text)
        expected = [_Container(text=t) for t in sorted(_STRS)]
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_containers_sync()),
            param(_get_containers_async()),
            param(_yield_containers_sync()),
            param(_yield_containers_async()),
        ],
    )
    async def test_key_async(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[_Container]
    ) -> None:
        async def key(container: _Container, /) -> str:
            await sleep(0.01)
            return container.text

        result = await to_sorted(iterable, key=key)
        expected = [_Container(text=t) for t in sorted(_STRS)]
        assert result == expected

    @mark.parametrize(
        "iterable",
        [
            param(_get_strs_sync()),
            param(_get_strs_async()),
            param(_yield_strs_sync()),
            param(_yield_strs_async()),
        ],
    )
    async def test_reverse(
        self, *, iterable: _MaybeAwaitableMaybeAsyncIterable[str]
    ) -> None:
        result = await to_sorted(iterable, reverse=True)
        expected = sorted(_STRS, reverse=True)
        assert result == expected


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
