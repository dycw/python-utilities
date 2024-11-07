from __future__ import annotations

import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, cast

from eventkit import Event
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers
from loguru import logger
from pytest import CaptureFixture

from utilities.eventkit import add_listener
from utilities.functions import identity
from utilities.loguru import HandlerConfiguration, LogLevel

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestAddListener:
    datetime: ClassVar[str] = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| "

    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_main(self, *, n: int) -> None:
        event = Event()
        _ = add_listener(event, identity)
        event.emit(n)

    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_custom_error_handler(
        self, *, capsys: CaptureFixture, n: int
    ) -> None:
        def error(event: Event, exception: Exception, /) -> None:
            _ = (event, exception)
            print("Custom handler")  # noqa: T201

        event = Event()
        _ = add_listener(event, identity, error=error, _stdout=False, _loguru=False)
        event.emit(n, n)
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        assert line == "Custom handler"

    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_loguru(self, *, capsys: CaptureFixture, n: int) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        event = Event()
        _ = add_listener(event, identity, _stdout=False, _loguru=True)
        event.emit(n, n)
        out = capsys.readouterr().out
        (line1, line2, line3, *_, last) = out.splitlines()
        expected1 = r"ERROR    \| utilities\.eventkit:_add_listener_error:\d+ - Raised a TypeError whilst running 'Event':$"
        assert search(expected1, line1), line1
        pattern2 = r"^Event<Event, \[\[None, None, <function identity at .*>\]\]>$"
        assert search(pattern2, line2)
        assert line3 == "Traceback (most recent call last):"
        assert (
            last == "TypeError: identity() takes 1 positional argument but 2 were given"
        )

    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error_stdout(self, *, capsys: CaptureFixture, n: int) -> None:
        event = Event()
        _ = add_listener(event, identity, keep_ref=True, _stdout=True, _loguru=False)
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
