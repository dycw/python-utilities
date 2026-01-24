from __future__ import annotations

from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, Logger, getLogger
from typing import TYPE_CHECKING

from pytest import mark, param

from utilities.core import (
    log_critical,
    log_debug,
    log_error,
    log_exception,
    log_info,
    log_warning,
    one,
    to_logger,
    unique_str,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest import LogCaptureFixture


class TestLogDebugInfoWarningError:
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


class TestToLogger:
    def test_default(self) -> None:
        assert to_logger().name == "root"

    def test_logger(self) -> None:
        name = unique_str()
        assert to_logger(getLogger(name)).name == name

    def test_str(self) -> None:
        name = unique_str()
        assert to_logger(name).name == name

    def test_none(self) -> None:
        assert to_logger(None).name == "root"
