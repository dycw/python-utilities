from __future__ import annotations

import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, cast

from eventkit import Event
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers
from loguru import logger
from pytest import CaptureFixture

from tests.test_loguru_functions import func_test_eventkit
from utilities.eventkit import add_listener
from utilities.loguru import HandlerConfiguration, LogLevel

if TYPE_CHECKING:
    from pytest import CaptureFixture


class TestAddListener:
    datetime: ClassVar[str] = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| "

    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_main(self, *, capsys: CaptureFixture, n: int) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        event = Event()
        _ = add_listener(event, func_test_eventkit)
        event.emit(n)
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        expected = (
            self.datetime
            + r"TRACE    \| tests\.test_loguru_functions:func_test_eventkit:\d+ - n=-?\d+$"
        )
        assert search(expected, line), line

    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_error(self, *, capsys: CaptureFixture, n: int) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        event = Event()
        _ = add_listener(event, func_test_eventkit)
        event.emit(n, n)
        out = capsys.readouterr().out
        (line1, line2, *_, last) = out.splitlines()
        expected1 = r"ERROR    \| utilities\.eventkit:_add_listener_error:\d+ - Error running Event<.*>$"
        assert search(expected1, line1), line1
        assert line2 == "Traceback (most recent call last):"
        assert (
            last
            == "TypeError: func_test_eventkit() takes 1 positional argument but 2 were given"
        )
