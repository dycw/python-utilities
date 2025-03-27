from __future__ import annotations

from asyncio import sleep
from re import search
from typing import TYPE_CHECKING, ClassVar, Literal

from eventkit import Event
from hypothesis import HealthCheck, given
from hypothesis.strategies import integers, sampled_from
from pytest import CaptureFixture, mark

from utilities.eventkit import add_listener
from utilities.functions import identity
from utilities.hypothesis import settings_with_reduced_examples, text_ascii

if TYPE_CHECKING:
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

    @given(n=integers())
    @settings_with_reduced_examples(
        suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    async def test_error_stdout(self, *, capsys: CaptureFixture, n: int) -> None:
        event = Event()
        _ = add_listener(event, identity)
        event.emit(n, n)
        out = capsys.readouterr().out
        (line1, line2, line3) = out.splitlines()
        assert line1 == "Raised a TypeError whilst running 'Event':"
        pattern2 = (
            r"^event=Event<Event, \[\[None, None, <function identity at .*>\]\]>$"
        )
        assert search(pattern2, line2)
        assert (
            line3
            == "exception=TypeError('identity() takes 1 positional argument but 2 were given')"
        )

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
