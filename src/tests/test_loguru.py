from __future__ import annotations

import datetime as dt
import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, cast

from loguru import logger
from loguru._defaults import LOGURU_FORMAT
from pytest import CaptureFixture, mark, param, raises

from utilities.loguru import (
    InterceptHandler,
    LogLevel,
    logged_sleep_async,
    logged_sleep_sync,
    make_catch_hook,
)
from utilities.text import ensure_str, strip_and_dedent

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture

    from utilities.types import Duration


class TestGetLoggingLevel:
    @mark.parametrize(
        ("level", "expected"),
        [
            param(LogLevel.TRACE, 5),
            param(LogLevel.DEBUG, 10),
            param(LogLevel.INFO, 20),
            param(LogLevel.SUCCESS, 25),
            param(LogLevel.WARNING, 30),
            param(LogLevel.ERROR, 40),
            param(LogLevel.CRITICAL, 50),
        ],
    )
    def test_main(self, *, level: str, expected: int) -> None:
        assert get_logging_level(level) == expected

    def test_error(self) -> None:
        with raises(GetLoggingLevelError, match="Invalid logging level: 'invalid'"):
            _ = get_logging_level("invalid")


class TestInterceptHandler:
    def test_main(self) -> None:
        _ = InterceptHandler()


class TestLogCall:
    def test_sync(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert diff_pairwise_then_add_sync(1000, 100, 10, 1) == 909
        out = capsys.readouterr().out
        line1, line2, line3, line4 = out.splitlines()
        head = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| "
        expected1 = (
            head
            + r"tests\.test_loguru:test_sync:\d+ -  \| {'x': 1000, 'y': 100, 'z': 10, 'w': 1}"
        )
        assert search(expected1, line1)
        head_mid = head + r"tests\.functions:diff_pairwise_then_add_sync:\d+ -  \| "
        expected2 = head_mid + "{'x': 1000, 'y': 100}"
        assert search(expected2, line2)
        expected3 = head_mid + "{'x': 10, 'y': 1}"
        assert search(expected3, line3)
        expected4 = head_mid + "{'x': 900, 'y': 9}"
        assert search(expected4, line4)

    async def test_async(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert await diff_pairwise_then_add_async(1000, 100, 10, 1) == 909
        out = capsys.readouterr().out
        line1, line2, line3, line4 = out.splitlines()
        head = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| "
        expected1 = (
            head
            + r"tests\.test_loguru:test_async:\d+ -  \| {'x': 1000, 'y': 100, 'z': 10, 'w': 1}"
        )
        assert search(expected1, line1)
        head_mid = head + r"tests\.functions:diff_pairwise_then_add_async:\d+ -  \| "
        expected2 = head_mid + "{'x': 1000, 'y': 100}"
        assert search(expected2, line2)
        expected3 = head_mid + "{'x': 10, 'y': 1}"
        assert search(expected3, line3)
        expected4 = head_mid + "{'x': 900, 'y': 9}"
        assert search(expected4, line4)

    def test_custom_level(self, *, capsys: CaptureFixture) -> None:
        default_format = ensure_str(LOGURU_FORMAT)
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "level": LogLevel.TRACE,
            "format": f"{default_format} | {{extra}}",
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert add_sync_info(1, 2) == 3
        out = capsys.readouterr().out
        (line,) = out.splitlines()
        expected = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| INFO     \| tests\.test_loguru:test_custom_level:\d+ -  \| {'x': 1, 'y': 2}"
        assert search(expected, line)


class TestLoggedSleep:
    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    def test_sync(self, *, duration: Duration) -> None:
        logged_sleep_sync(duration)

    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    async def test_async(self, *, duration: Duration) -> None:
        await logged_sleep_async(duration)


class TestMakeCatchHook:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        catch_on_error = make_catch_hook()

        @logger.catch(onerror=catch_on_error)
        def divide_by_zero(x: float, /) -> float:
            return x / 0

        _ = divide_by_zero(1.0)

        out = capsys.readouterr().out
        lines = out.splitlines()
        exp_first = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| ERROR    \| tests\.test_loguru:test_main:\d+ - An error has been caught in function 'test_main', process 'MainProcess' \(\d+\), thread 'MainThread' \(\d+\)"
        assert search(exp_first, lines[0])
        exp_last = strip_and_dedent("""
                return x / 0
                       └ 1.0

            ZeroDivisionError: float division by zero
        """)
        assert search(exp_last, "\n".join(lines[-4:]))

    def test_without_catch(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        def divide_by_zero(x: float, /) -> float:
            return x / 0

        with raises(ZeroDivisionError):
            _ = divide_by_zero(1.0)

        out = capsys.readouterr().out
        expected = ""
        assert out == expected
