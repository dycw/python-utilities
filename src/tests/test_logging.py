from __future__ import annotations

from logging import getLogger

from pytest import mark, param, raises

from utilities.logging import (
    GetLoggingLevelNumberError,
    LogLevel,
    basic_config,
    get_logging_level_number,
)


class TestBasicConfig:
    def test_main(self) -> None:
        basic_config()
        logger = getLogger(__name__)
        logger.info("message")


class TestGetLoggingLevelNumber:
    @mark.parametrize(
        ("level", "expected"),
        [
            param(LogLevel.DEBUG, 10),
            param(LogLevel.INFO, 20),
            param(LogLevel.WARNING, 30),
            param(LogLevel.ERROR, 40),
            param(LogLevel.CRITICAL, 50),
        ],
    )
    def test_main(self, *, level: str, expected: int) -> None:
        assert get_logging_level_number(level) == expected

    def test_error(self) -> None:
        with raises(
            GetLoggingLevelNumberError, match="Invalid logging level: 'invalid'"
        ):
            _ = get_logging_level_number("invalid")


class TestLogLevel:
    def test_main(self) -> None:
        assert len(LogLevel) == 5
