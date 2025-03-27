from __future__ import annotations

from asyncio import sleep
from io import StringIO
from logging import DEBUG, StreamHandler, getLogger
from re import search
from typing import TYPE_CHECKING, ClassVar, Literal

from eventkit import Event
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers, sampled_from
from pytest import CaptureFixture, mark

from utilities.eventkit import add_listener
from utilities.functions import identity
from utilities.hypothesis import settings_with_reduced_examples, temp_paths, text_ascii

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import CaptureFixture


class TestAddListener:
    datetime: ClassVar[str] = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| "

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

    @given(root=temp_paths(), sync_or_async=sampled_from(["sync", "async"]))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    @mark.only
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

                _ = add_listener(event, listener_sync, error_logger=str(root))
            case "async":

                async def listener_async() -> None:
                    await sleep(0.01)

                _ = add_listener(event, listener_async, error_logger=str(root))

        event.emit(None)
        await sleep(0.01)
        pattern = r"listener_a?sync\(\) takes 0 positional arguments but 1 was given"
        contents = buffer.getvalue()
        assert search(pattern, contents)

    @given(name=text_ascii(min_size=1), n=integers(min_value=1))
    @mark.only
    def test_with_error_handler(self, *, name: str, n: int) -> None:
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

    @given(n=integers())
    @settings_with_reduced_examples(
        suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    async def test_error_ignore(self, *, capsys: CaptureFixture, n: int) -> None:
        event = Event()
        _ = add_listener(event, identity, error_ignore=TypeError)
        event.emit(n, n)
        out = capsys.readouterr().out
        expected = ""
        assert out == expected
