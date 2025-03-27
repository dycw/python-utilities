from __future__ import annotations

from asyncio import sleep
from io import StringIO
from logging import DEBUG, StreamHandler, getLogger
from re import search
from typing import TYPE_CHECKING, Literal

from eventkit import Event
from hypothesis import given
from hypothesis.strategies import integers, sampled_from
from pytest import raises

from utilities.eventkit import AddListenerError, add_listener
from utilities.hypothesis import temp_paths, text_ascii

if TYPE_CHECKING:
    from pathlib import Path


class TestAddListener:
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

    @given(root=temp_paths(), sync_or_async=sampled_from(["sync", "async"]))
    async def test_no_error_handler_but_run_into_error(
        self, *, root: Path, sync_or_async: Literal["sync", "async"]
    ) -> None:
        logger = getLogger(str(root))
        logger.setLevel(DEBUG)
        handler = StreamHandler(buffer := StringIO())
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
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
        name=text_ascii(min_size=1),
        case=sampled_from(["sync", "async/sync", "async"]),
        n=integers(min_value=1),
    )
    async def test_with_error_handler(
        self, *, name: str, case: Literal["sync", "async/sync", "async"], n: int
    ) -> None:
        event = Event(name=name)
        assert event.name() == name
        counter = 0
        log: set[tuple[str, type[Exception]]] = set()

        def listener_sync(n: int, /) -> None:
            if n >= 0:
                nonlocal counter
                counter += n
            else:
                msg = "'n' must be non-negative"
                raise ValueError(msg)

        def error_sync(event: Event, exception: Exception, /) -> None:
            nonlocal log
            log.add((event.name(), type(exception)))

        async def listener_async(n: int, /) -> None:
            if n >= 0:
                nonlocal counter
                counter += n
                await sleep(0.01)
            else:
                msg = "'n' must be non-negative"
                raise ValueError(msg)

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
        event.emit(n)
        await sleep(0.01)
        assert counter == n
        assert log == set()
        event.emit(-n)
        await sleep(0.01)
        assert counter == n
        assert log == {(name, ValueError)}

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

        with raises(AddListenerError, match="asdf"):
            _ = add_listener(event, listener, error=error)
