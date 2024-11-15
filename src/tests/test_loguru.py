from __future__ import annotations

import datetime as dt
import sys  # do use `from sys import ...`
from re import search
from typing import TYPE_CHECKING, Any, cast

from loguru import logger
from loguru._recattrs import RecordFile, RecordLevel, RecordProcess, RecordThread
from pytest import CaptureFixture, fixture, mark, param, raises

from utilities.loguru import (
    LEVEL_CONFIGS,
    GetLoggingLevelNumberError,
    HandlerConfiguration,
    InterceptHandler,
    LogLevel,
    _GetLoggingLevelNameEmptyError,
    _GetLoggingLevelNameNonUniqueError,
    get_logging_level_name,
    get_logging_level_number,
)

if TYPE_CHECKING:
    from loguru import Record
    from pytest import CaptureFixture


@fixture
def record() -> Record:
    record = {
        "elapsed": dt.timedelta(seconds=11, microseconds=635587),
        "exception": None,
        "extra": {"x": 1, "y": 2},
        "file": RecordFile(
            name="1723464958.py",
            path="/var/folders/z2/t3tvc2yn33j0zdd910j7805r0000gn/T/ipykernel_98745/1723464958.py",
        ),
        "function": "<module>",
        "level": RecordLevel(name="INFO", no=20, icon="ℹ️ "),  # noqa: RUF001
        "line": 1,
        "message": "l2",
        "module": "1723464958",
        "name": "__main__",
        "process": RecordProcess(id_=98745, name="MainProcess"),
        "thread": RecordThread(id_=8420429632, name="MainThread"),
        "time": dt.datetime(
            2024,
            8,
            31,
            14,
            3,
            52,
            388537,
            tzinfo=dt.timezone(dt.timedelta(seconds=32400), "JST"),
        ),
    }
    return cast(Any, record)


class TestGetLoggingLevelNameAndNumber:
    @mark.parametrize(
        ("name", "number"),
        [
            param(LogLevel.TRACE, 5),
            param(LogLevel.DEBUG, 10),
            param(LogLevel.INFO, 20),
            param(LogLevel.SUCCESS, 25),
            param(LogLevel.WARNING, 30),
            param(LogLevel.ERROR, 40),
            param(LogLevel.CRITICAL, 50),
        ],
        ids=str,
    )
    def test_main(self, *, name: str, number: int) -> None:
        assert get_logging_level_number(name) == number
        assert get_logging_level_name(number) == name

    def test_error_name_empty(self) -> None:
        with raises(
            _GetLoggingLevelNameEmptyError, match="There is no level with severity 0"
        ):
            _ = get_logging_level_name(0)

    def test_error_name_non_unique(self) -> None:
        _ = logger.level("TEST-1", no=99)
        _ = logger.level("TEST-2", no=99)
        with raises(
            _GetLoggingLevelNameNonUniqueError,
            match="There must be exactly one level with severity 99; got 'TEST-1', 'TEST-2' and perhaps more",
        ):
            _ = get_logging_level_name(99)

    def test_error_number(self) -> None:
        with raises(
            GetLoggingLevelNumberError, match="Invalid logging level: 'invalid'"
        ):
            _ = get_logging_level_number("invalid")


class TestHandlerConfiguration:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        logger.trace("message 1")
        out1 = capsys.readouterr().out
        assert out1 == ""

        handler: HandlerConfiguration = {"sink": sys.stdout, "level": LogLevel.TRACE}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        logger.trace("message 2")
        out2 = capsys.readouterr().out
        expected = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| TRACE    \| tests\.test_loguru:test_main:\d+ - message 2$"
        assert search(expected, out2), out2


class TestInterceptHandler:
    def test_main(self) -> None:
        _ = InterceptHandler()


class TestLevelConfiguration:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {
            "sink": sys.stdout,
            "format": "<level>{message}</level>",
            "colorize": True,
        }
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        logger.info("message 1")
        out1 = capsys.readouterr().out
        expected1 = "\x1b[1mmessage 1\x1b[0m\n"
        assert out1 == expected1

        _ = logger.configure(levels=LEVEL_CONFIGS)

        logger.info("message 2")
        out2 = capsys.readouterr().out
        expected2 = "\x1b[32m\x1b[1mmessage 2\x1b[0m\n"
        assert out2 == expected2
