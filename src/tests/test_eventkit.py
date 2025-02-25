from __future__ import annotations

from asyncio import sleep
from re import search
from typing import TYPE_CHECKING, ClassVar, Literal

from eventkit import Event
from hypothesis import HealthCheck, given
from hypothesis.strategies import integers, sampled_from
from pytest import CaptureFixture

from utilities.eventkit import add_listener
from utilities.functions import identity
from utilities.hypothesis import settings_with_reduced_examples

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestAddListener:
    datetime: ClassVar[str] = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| "

    @given(sync_or_async=sampled_from(["sync", "async"]), n=integers())
    async def test_main(
        self, *, sync_or_async: Literal["sync", "async"], n: int
    ) -> None:
        event = Event()
        match sync_or_async:
            case "sync":

                def listener_sync(n: int, /) -> None:
                    print(f"n={n}")  # noqa: T201

                _ = add_listener(event, listener_sync)
            case "async":

                async def listener_async(n: int, /) -> None:
                    await sleep(0.01)
                    print(f"n={n}")  # noqa: T201

                _ = add_listener(event, listener_async)

        event.emit(n)

    @given(n=integers())
    @settings_with_reduced_examples(
        suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_custom_error_handler(self, *, capsys: CaptureFixture, n: int) -> None:
        event = Event()

        def error(event: Event, exception: Exception, /) -> None:
            _ = (event, exception)
            print("Custom handler")  # noqa: T201

        _ = add_listener(event, identity, error=error)
        event.emit(n, n)
        out = capsys.readouterr().out
        *_, line = out.splitlines()
        assert line == "Custom handler"

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
