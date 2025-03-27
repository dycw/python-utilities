from __future__ import annotations

from asyncio import sleep
from functools import wraps
from io import StringIO
from logging import DEBUG, StreamHandler, getLogger
from re import search
from typing import TYPE_CHECKING, Literal, ParamSpec, TypeVar

from eventkit import Event
from hypothesis import HealthCheck, given
from hypothesis.strategies import integers, sampled_from
from pytest import CaptureFixture, mark

from utilities.eventkit import add_listener
from utilities.functions import identity
from utilities.hypothesis import settings_with_reduced_examples, text_ascii

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


_P = ParamSpec("_P")
_R = TypeVar("_R")

    @mark.only
    @given(sync_or_async=sampled_from(["sync", "async"]), n=integers())
    async def test_main(
        self, *, sync_or_async: Literal["sync", "async"], n: int
    ) -> None:
        event = Event()
        counter = 0
        match sync_or_async:
            case "sync":

                def listener_sync(n: int, /) -> None:
                    nonlocal counter
                    counter += n

                _ = add_listener(event, listener_sync)
            case "async":

                async def listener_async(n: int, /) -> None:
                    nonlocal counter
                    counter += n
                    await sleep(0.01)

                _ = add_listener(event, listener_async)

        event.emit(n)
        await sleep(0.01)
        assert counter == n

    @given(name=text_ascii(min_size=1), n=integers(min_value=1))
    @mark.only
    def test_custom_error_handler(self, *, name: str, n: int) -> None:
        event = Event(name=name)
        assert event.name() == name
        counter = 0
        log: set[tuple[str, type[Exception]]] = set()

        def listener(n: int, /) -> None:
            if n >= 0:
                nonlocal counter
                counter += n
            else:
                msg = "'n' must be non-negative"
                raise ValueError(msg)

        def error(event: Event, exception: Exception, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))

        _ = add_listener(event, listener, error=error)
        event.emit(n)
        assert counter == n
        assert log == set()
        event.emit(-n)
        assert counter == n
        assert log == {(name, ValueError)}

    @given(
        name=text_ascii(min_size=1), case=sampled_from(["sync", "async/sync", "async"])
    )
    async def test_with_error_handler(
        self, *, name: str, case: Literal["sync", "async/sync", "async"]
    ) -> None:
        event = Event(name=name)
        assert event.name() == name
        called = False
        log: set[tuple[str, type[Exception]]] = set()

        def listener_sync(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
            else:
                raise ValueError

        def error_sync(event: Event, exception: Exception, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))

        async def listener_async(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
                await sleep(0.01)
            else:
                raise ValueError

        async def error_async(event: Event, exception: Exception, /) -> None:
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
        log: set[tuple[str, type[Exception]]] = set()

        def listener_sync(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
            else:
                raise ValueError

        def error_sync(event: Event, exception: Exception, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))

        async def listener_async(is_success: bool, /) -> None:  # noqa: FBT001
            if is_success:
                nonlocal called
                called |= True
                await sleep(0.01)
            else:
                raise ValueError

        async def error_async(event: Event, exception: Exception, /) -> None:
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

    def test_error(self) -> None:
        event = Event()
        counter = 0
        log: set[tuple[str, type[Exception]]] = set()

        def listener(n: int, /) -> None:
            if n >= 0:
                nonlocal counter
                counter += n
            else:
                msg = "'n' must be non-negative"
                raise ValueError(msg)

        async def error(event: Event, exception: Exception, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))
            await sleep(0.01)

        with raises(
            AddListenerError,
            match="Synchronous listener .* cannot be paired with an asynchronous error handler .*",
        ):
            _ = add_listener(event, listener, error=error)
