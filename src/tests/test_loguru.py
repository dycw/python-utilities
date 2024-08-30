from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.loguru import (
    GetLoggingLevelError,
    InterceptHandler,
    LogLevel,
    get_logging_level,
    logged_sleep_async,
    logged_sleep_sync,
)

if TYPE_CHECKING:
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


class TestLoggedSleep:
    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    def test_sync(self, *, duration: Duration) -> None:
        logged_sleep_sync(duration)

    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    async def test_async(self, *, duration: Duration) -> None:
        await logged_sleep_async(duration)
