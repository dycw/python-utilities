from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

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
)
from utilities.pathlib import temp_cwd
from utilities.pytest import skipif_windows

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


class TestSetupLogging:
    @skipif_windows
    def test_main(self, *, tmp_path: Path) -> None:
        setup_logging(files_dir=tmp_path)
        assert len(list(tmp_path.iterdir())) == 7

    @skipif_windows
    def test_files_dir_cwd(self, *, tmp_path: Path) -> None:
        with temp_cwd(tmp_path):
            setup_logging(files_dir=None)
            logger = getLogger(__name__)
            logger.info("message")
            assert len(list(tmp_path.iterdir())) == 7

    @skipif_windows
    def test_files_dir_callable(self, *, tmp_path: Path) -> None:
        setup_logging(files_dir=lambda: tmp_path)
        assert len(list(tmp_path.iterdir())) == 7

    def test_default_path(self) -> None:
        _ = _setup_logging_default_path()

    def test_brace_formatting(
        self, *, caplog: LogCaptureFixture, tmp_path: Path
    ) -> None:
        setup_logging(logger_name=__name__, files_dir=tmp_path)
        logger = getLogger(__name__)
        logger.info("int: {:d}, float: {:.2f}, percent: {:.2%}", 1, 12.3456, 0.123456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35, percent: 12.35%"
        assert record.message == expected

    def test_zoned_datetime(self, *, caplog: LogCaptureFixture, tmp_path: Path) -> None:
        setup_logging(logger_name=__name__, files_dir=tmp_path)
        logger = getLogger(__name__)
        logger.info("")
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        assert isinstance(record.zoned_datetime, ZonedDateTime)
        assert isinstance(record.zoned_datetime_str, str)
