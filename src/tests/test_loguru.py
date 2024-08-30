from __future__ import annotations

import datetime as dt
import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, cast

from loguru import logger
from loguru._defaults import LOGURU_FORMAT
from pytest import mark, param, raises

from tests.functions import (
    add_sync_info,
    diff_pairwise_then_add_async,
    diff_pairwise_then_add_sync,
)
from utilities.loguru import (
    GetLoggingLevelError,
    HandlerConfiguration,
    InterceptHandler,
    LogLevel,
    get_logging_level,
    logged_sleep_async,
    logged_sleep_sync,
)
from utilities.text import ensure_str

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
