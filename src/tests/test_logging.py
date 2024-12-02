from __future__ import annotations

from logging import DEBUG, INFO, NOTSET, FileHandler, Logger, StreamHandler, getLogger
from typing import TYPE_CHECKING, Any, cast

from pytest import LogCaptureFixture, mark, param, raises
from whenever import ZonedDateTime

from utilities.iterables import one
from utilities.logging import (
    GetLoggingLevelNumberError,
    LogLevel,
    _AdvancedLogRecord,
    _setup_logging_default_path,
    basic_config,
    get_logging_level_number,
    setup_logging,
    temp_handler,
    temp_logger,
)
from utilities.pathlib import temp_cwd
from utilities.pytest import skipif_windows
from utilities.typing import get_args

if TYPE_CHECKING:
    from pathlib import Path


class TestBasicConfig:
    def test_main(self) -> None:
        basic_config()
        logger = getLogger(__name__)
        logger.info("message")


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
            GetLoggingLevelNumberError, match="Invalid logging level: 'invalid'"
        ):
            _ = get_logging_level_number(cast(Any, "invalid"))


class TestLogLevel:
    def test_main(self) -> None:
        assert len(get_args(LogLevel)) == 5


class TestSetupLogging:
    @skipif_windows
    def test_main(self, *, tmp_path: Path) -> None:
        name = TestSetupLogging.test_main.__qualname__
        setup_logging(logger_name=name, files_dir=tmp_path)
        logger = getLogger(name)
        assert len(logger.handlers) == 7
        assert len(list(tmp_path.iterdir())) == 7

    @skipif_windows
    def test_files_dir_cwd(self, *, tmp_path: Path) -> None:
        name = TestSetupLogging.test_files_dir_cwd.__qualname__
        with temp_cwd(tmp_path):
            setup_logging(logger_name=name, files_dir=None)
            logger = getLogger(name)
            logger.info("message")
            assert len(list(tmp_path.iterdir())) == 7

    @skipif_windows
    def test_files_dir_callable(self, *, tmp_path: Path) -> None:
        name = TestSetupLogging.test_files_dir_callable.__qualname__
        setup_logging(logger_name=name, files_dir=lambda: tmp_path)
        assert len(list(tmp_path.iterdir())) == 7

    def test_default_path(self) -> None:
        _ = _setup_logging_default_path()

    @skipif_windows
    def test_regular_percent_formatting(
        self, *, caplog: LogCaptureFixture, tmp_path: Path
    ) -> None:
        name = TestSetupLogging.test_regular_percent_formatting.__qualname__
        setup_logging(logger_name=name, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("int: %d, float: %.2f", 1, 12.3456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35"
        assert record.message == expected

    @skipif_windows
    def test_new_brace_formatting(
        self, *, caplog: LogCaptureFixture, tmp_path: Path
    ) -> None:
        name = TestSetupLogging.test_new_brace_formatting.__qualname__
        setup_logging(logger_name=name, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("int: {:d}, float: {:.2f}, percent: {:.2%}", 1, 12.3456, 0.123456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35, percent: 12.35%"
        assert record.message == expected

    @skipif_windows
    def test_no_console(self, *, tmp_path: Path) -> None:
        name = TestSetupLogging.test_no_console.__qualname__
        setup_logging(logger_name=name, console_level=None, files_dir=tmp_path)
        logger = getLogger(name)
        assert len(logger.handlers) == 6

    @skipif_windows
    def test_zoned_datetime(self, *, caplog: LogCaptureFixture, tmp_path: Path) -> None:
        name = TestSetupLogging.test_zoned_datetime.__qualname__
        setup_logging(logger_name=name, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("")
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        assert isinstance(record.zoned_datetime, ZonedDateTime)
        assert isinstance(record.zoned_datetime_str, str)

    @skipif_windows
    def test_extra(self, *, tmp_path: Path) -> None:
        name = TestSetupLogging.test_extra.__qualname__

        def extra(logger: Logger, /) -> None:
            handler = FileHandler(tmp_path.joinpath("extra.log"))
            handler.setLevel(DEBUG)
            logger.addHandler(handler)

        setup_logging(logger_name=name, files_dir=tmp_path, extra=extra)
        logger = getLogger(name)
        logger.info("")
        assert len(list(tmp_path.iterdir())) == 8


class TestTempHandler:
    def test_main(self) -> None:
        name = TestTempHandler.test_main.__qualname__
        logger = getLogger(name)
        logger.addHandler(h1 := StreamHandler())
        logger.addHandler(h2 := StreamHandler())
        assert len(logger.handlers) == 2
        handler = StreamHandler()
        with temp_handler(logger, handler):
            assert len(logger.handlers) == 3
        assert len(logger.handlers) == 2
        assert logger.handlers[0] is h1
        assert logger.handlers[1] is h2


class TestTempLogger:
    def test_disabled(self) -> None:
        name = TestTempLogger.test_disabled.__qualname__
        logger = getLogger(name)
        assert not logger.disabled
        with temp_logger(logger, disabled=True):
            assert logger.disabled
        assert not logger.disabled

    def test_level(self) -> None:
        name = TestTempLogger.test_level.__qualname__
        logger = getLogger(name)
        assert logger.level == NOTSET
        with temp_logger(logger, level="INFO"):
            assert logger.level == INFO
        assert logger.level == NOTSET

    def test_propagate(self) -> None:
        name = TestTempLogger.test_propagate.__qualname__
        logger = getLogger(name)
        assert logger.propagate
        with temp_logger(logger, propagate=False):
            assert not logger.propagate
        assert logger.propagate
