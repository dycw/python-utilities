from __future__ import annotations

from asyncio import sleep
from collections.abc import Callable
from functools import wraps
from io import StringIO
from logging import StreamHandler, getLogger
from re import search
from typing import TYPE_CHECKING, Literal, ParamSpec, TypeVar

from eventkit import Event
from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import raises

from utilities.eventkit import (
    LiftedEvent,
    LiftListenerError,
    TypedEvent,
    add_listener,
    lift_listener,
)
from utilities.hypothesis import temp_paths, text_ascii

if TYPE_CHECKING:
    from pathlib import Path


_P = ParamSpec("_P")
_R = TypeVar("_R")


class TestAddListener:
    @given(sync_or_async=sampled_from(["sync", "async"]))
    async def test_main(self, *, sync_or_async: Literal["sync", "async"]) -> None:
        event = Event()
        called = False
        match sync_or_async:
            case "sync":

                def listener_sync() -> None:
                    nonlocal called
                    called |= True

                _ = add_listener(event, listener_sync)
            case "async":

                async def listener_async() -> None:
                    nonlocal called
                    called |= True
                    await sleep(0.01)

                _ = add_listener(event, listener_async)

        event.emit()
        await sleep(0.01)
        assert called

    @given(root=temp_paths(), sync_or_async=sampled_from(["sync", "async"]))
    async def test_no_error_handler_but_run_into_error(
        self, *, root: Path, sync_or_async: Literal["sync", "async"]
    ) -> None:
        logger = getLogger(str(root))
        logger.addHandler(StreamHandler(buffer := StringIO()))
        event = Event()

        match sync_or_async:
            case "sync":

                def listener_sync() -> None: ...

                _ = add_listener(event, listener_sync, logger=str(root))
            case "async":

                async def listener_async() -> None:
                    await sleep(0.01)

                _ = add_listener(event, listener_async, logger=str(root))

        event.emit(None)
        await sleep(0.01)
        pattern = r"listener_a?sync\(\) takes 0 positional arguments but 1 was given"
        contents = buffer.getvalue()
        assert search(pattern, contents)

    @given(
        name=text_ascii(min_size=1), case=sampled_from(["sync", "async/sync", "async"])
    )
    async def test_with_error_handler(
        self, *, name: str, case: Literal["sync", "async/sync", "async"]
    ) -> None:
        event = Event(name=name)
        assert event.name() == name
        called = False
        log: set[tuple[str, type[BaseException]]] = set()

        def listener_sync(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
            else:
                raise ValueError

        def error_sync(event: Event, exception: BaseException, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))

        async def listener_async(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
                await sleep(0.01)
            else:
                raise ValueError

        async def error_async(event: Event, exception: BaseException, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))
            await sleep(0.01)

        match case:
            case "sync":
                _ = add_listener(event, listener_sync, error=error_sync)
            case "async/sync":
                _ = add_listener(event, listener_async, error=error_sync)
            case "async":
                _ = add_listener(event, listener_async, error=error_async)
        event.emit(True)  # noqa: FBT003
        await sleep(0.01)
        assert called
        assert log == set()
        event.emit(False)  # noqa: FBT003
        await sleep(0.01)
        assert log == {(name, ValueError)}

    @given(
        case=sampled_from([
            "no/sync",
            "no/async",
            "have/sync",
            "have/async/sync",
            "have/async",
        ])
    )
    async def test_ignore(
        self,
        *,
        case: Literal[
            "no/sync", "no/async", "have/sync", "have/async/sync", "have/async"
        ],
    ) -> None:
        event = Event()
        called = False
        log: set[tuple[str, type[BaseException]]] = set()

        def listener_sync(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
            else:
                raise ValueError

        def error_sync(event: Event, exception: BaseException, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))

        async def listener_async(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
                await sleep(0.01)
            else:
                raise ValueError

        async def error_async(event: Event, exception: BaseException, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))
            await sleep(0.01)

        match case:
            case "no/sync":
                _ = add_listener(event, listener_sync, ignore=ValueError)
            case "no/async":
                _ = add_listener(event, listener_async, ignore=ValueError)
            case "have/sync":
                _ = add_listener(
                    event, listener_sync, error=error_sync, ignore=ValueError
                )
            case "have/async/sync":
                _ = add_listener(
                    event, listener_async, error=error_sync, ignore=ValueError
                )
            case "have/async":
                _ = add_listener(
                    event, listener_async, error=error_async, ignore=ValueError
                )
        event.emit(True)  # noqa: FBT003
        await sleep(0.01)
        assert called
        assert log == set()
        event.emit(False)  # noqa: FBT003
        await sleep(0.01)
        assert log == set()

    def test_decorators(self) -> None:
        event = Event()
        counter = 0

        def listener() -> None:
            nonlocal counter
            counter += 1

        def increment(func: Callable[_P, _R], /) -> Callable[_P, _R]:
            @wraps(func)
            def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
                nonlocal counter
                counter += 1
                return func(*args, **kwargs)

            return wrapped

        _ = add_listener(event, listener, decorators=increment)
        event.emit()
        assert counter == 2


class TestLiftListener:
    def test_error(self) -> None:
        def listener() -> None:
            pass

        async def error(event: Event, exception: BaseException, /) -> None:
            _ = (event, exception)
            await sleep(0.01)

        with raises(
            LiftListenerError,
            match="Synchronous listener .* cannot be paired with an asynchronous error handler .*",
        ):
            _ = lift_listener(listener, Event(), error=error)


class TestLiftedEvent:
    def test_main(self) -> None:
        event1 = Event()
        counter = 0

        def listener() -> None:
            nonlocal counter
            counter += 1

        _ = event1.connect(listener)
        event1.emit()
        assert counter == 1

        event2 = Event()

        class Example(LiftedEvent[Callable[[], None]]): ...

        lifted = Example(event=event2)
        _ = lifted.connect(listener)
        lifted.emit()
        assert counter == 2

        def incorrect(x: int, /) -> None:
            assert x >= 0

        _ = lifted.connect(incorrect)  # pyright: ignore[reportArgumentType]


class TestTypedEvent:
    def test_main(self) -> None:
        class Example(TypedEvent[Callable[[int], None]]): ...

        event = Example()

        def correct(x: int, /) -> None:
            assert x >= 0

        _ = event.connect(correct)

        def incorrect(x: int, y: int, /) -> None:
            assert x >= 0
            assert y >= 0

        _ = event.connect(incorrect)  # pyright: ignore[reportArgumentType]
