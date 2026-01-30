from __future__ import annotations

from dataclasses import dataclass, field
from logging import (
    CRITICAL,
    DEBUG,
    ERROR,
    INFO,
    WARNING,
    Logger,
    LoggerAdapter,
    StreamHandler,
    getLogger,
)
from re import search
from typing import TYPE_CHECKING, Any, cast

from pytest import LogCaptureFixture, mark, param, raises
from whenever import ZonedDateTime

from utilities.constants import HOSTNAME
from utilities.core import (
    EnhancedLogRecord,
    GetLoggingLevelNumberError,
    add_adapter,
    add_filters,
    get_logging_level_number,
    log_critical,
    log_debug,
    log_error,
    log_exception,
    log_info,
    log_warning,
    one,
    set_up_logging,
    to_logger,
    unique_str,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from pytest import LogCaptureFixture

    from utilities.types import LogLevel


class TestAddAdapter:
    def test_main(self, *, logger: Logger, caplog: LogCaptureFixture) -> None:
        def process(msg: str, x: int, /) -> str:
            return f"x={x}: {msg}"

        @dataclass
        class Example:
            x: int = 0
            logger: LoggerAdapter = field(init=False)

            def __post_init__(self) -> None:
                self.logger = add_adapter(logger, process, self.x)
                self.logger.info("Initializing...")

        _ = Example()
        record = one(r for r in caplog.records if r.name == logger.name)
        assert record.message == "x=0: Initializing..."


class TestAddFilters:
    def test_logger_filter_retained(
        self, *, logger: Logger, caplog: LogCaptureFixture
    ) -> None:
        add_filters(logger, lambda _: True)
        assert len(logger.filters) == 1
        logger.info("message")
        record = one(r for r in caplog.records if r.name == logger.name)
        assert record.message == "message"

    def test_logger_filter_removed(
        self, *, logger: Logger, caplog: LogCaptureFixture
    ) -> None:
        add_filters(logger, lambda _: False)
        assert len(logger.filters) == 1
        logger.info("message")
        records = [r for r in caplog.records if r.name == logger.name]
        assert len(records) == 0

    def test_handler(self, *, logger: Logger) -> None:
        logger.addHandler(handler := StreamHandler())
        add_filters(handler, lambda _: False)
        assert len(handler.filters) == 1


class TestGetLoggingLevelNumber:
    @mark.parametrize(
        ("level", "expected"),
        [
            param("DEBUG", 10),
            param("INFO", 20),
            param("WARNING", 30),
            param("ERROR", 40),
            param("CRITICAL", 50),
        ],
    )
    def test_main(self, *, level: LogLevel, expected: int) -> None:
        assert get_logging_level_number(level) == expected

    def test_error(self) -> None:
        with raises(
            GetLoggingLevelNumberError, match=r"Invalid logging level: 'invalid'"
        ):
            _ = get_logging_level_number(cast("Any", "invalid"))


class TestLogDebugInfoWarningErrorCritical:
    @mark.parametrize(
        ("log_func", "level"),
        [
            param(log_debug, DEBUG),
            param(log_info, INFO),
            param(log_warning, WARNING),
            param(log_error, ERROR),
            param(log_critical, CRITICAL),
        ],
    )
    @mark.parametrize("use_logger", [param(False), param(True)])
    def test_main(
        self,
        *,
        log_func: Callable[..., None],
        use_logger: bool,
        logger: Logger,
        level: int,
        caplog: LogCaptureFixture,
    ) -> None:
        def func() -> None:
            log_func(logger if use_logger else None, "message")

        func()
        records = [r for r in caplog.records if r.name == logger.name]
        if use_logger:
            record = one(records)
            assert record.message == "message"
            assert record.funcName == "func"
            assert record.levelno == level
        else:
            assert len(records) == 0

    @mark.parametrize("use_logger", [param(False), param(True)])
    def test_exception(
        self, *, use_logger: bool, logger: Logger, caplog: LogCaptureFixture
    ) -> None:
        def func() -> None:
            try:
                _ = 1 / 0
            except ZeroDivisionError:
                log_exception(logger if use_logger else None, "message")

        func()
        records = [r for r in caplog.records if r.name == logger.name]
        if use_logger:
            record = one(records)
            assert record.message == "message"
            assert record.funcName == "func"
            assert record.levelno == ERROR
            assert record.exc_info is not None
            assert record.exc_info[0] is ZeroDivisionError
        else:
            assert len(records) == 0


class TestSetUpLogging:
    def test_main(self, *, logger: Logger, caplog: LogCaptureFixture) -> None:
        set_up_logging(logger)
        assert len(logger.handlers) == 1
        logger.info("message")
        record = one(r for r in caplog.records if r.name == logger.name)
        assert isinstance(record, EnhancedLogRecord)
        assert record.hostname == HOSTNAME
        assert record.message == "message"
        assert isinstance(record.zoned_date_time, ZonedDateTime)
        assert search(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,9}\s{0,8}\[[\w\/]+\]$",
            record.zoned_date_time_str,
        )

    def test_files(self, *, logger: Logger, temp_path_not_exist: Path) -> None:
        set_up_logging(logger, files=temp_path_not_exist)
        assert len(logger.handlers) == 4
        assert temp_path_not_exist.is_dir()
        logger.info("message")
        files = {p.name for p in temp_path_not_exist.iterdir() if p.is_file()}
        expected = {"debug.txt", "info.txt", "error.txt"}
        assert files == expected

    def test_nested(self, *, logger: Logger, temp_path_nested_not_exist: Path) -> None:
        set_up_logging(logger, files=temp_path_nested_not_exist)
        assert temp_path_nested_not_exist.is_dir()


class TestToLogger:
    def test_logger(self, *, logger: Logger) -> None:
        assert to_logger(logger) is logger

    def test_str(self) -> None:
        name = unique_str()
        assert to_logger(name).name == name

    @mark.parametrize(
        ("name", "expected"),
        [
            param("foo1", "foo1"),
            param("foo1.bar", "foo1"),
            param("foo1.bar.baz", "foo1"),
            param("foo2.bar.baz", "foo2"),
        ],
    )
    @mark.parametrize("use_logger", [param(False), param(True)])
    def test_root(self, *, name: str, use_logger: bool, expected: str) -> None:
        logger_use = getLogger(name) if use_logger else name
        result = to_logger(logger_use, root=True)
        assert result.name == expected
