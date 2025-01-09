from __future__ import annotations

from logging import DEBUG, NOTSET, FileHandler, Logger, StreamHandler, getLogger
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any, Literal, cast

from pytest import LogCaptureFixture, mark, param, raises
from whenever import ZonedDateTime

from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.untraced import func_untraced
from utilities.iterables import one
from utilities.logging import (
    GetLoggingLevelNumberError,
    LogLevel,
    StandaloneFileHandler,
    _AdvancedLogRecord,
    add_filters,
    basic_config,
    get_default_logging_path,
    get_logger,
    get_logging_level_number,
    setup_logging,
    temp_handler,
    temp_logger,
)
from utilities.pytest import skipif_windows
from utilities.typing import get_args

if TYPE_CHECKING:
    from re import Pattern

    from utilities.types import LoggerOrName


class TestAddFilters:
    def test_main(self) -> None:
        handler = StreamHandler()
        assert len(handler.filters) == 0
        add_filters(handler, filters=[lambda _: True])
        assert len(handler.filters) == 1

    def test_no_handlers(self) -> None:
        handler = StreamHandler()
        assert len(handler.filters) == 0
        add_filters(handler)
        assert len(handler.filters) == 0


class TestBasicConfig:
    def test_main(self) -> None:
        basic_config()
        logger = getLogger(__name__)
        logger.info("message")


class TestGetDefaultLoggingPath:
    def test_main(self) -> None:
        assert isinstance(get_default_logging_path(), Path)


class TestGetLogger:
    def test_logger(self) -> None:
        logger = getLogger(__name__)
        result = get_logger(logger=logger)
        assert result is logger

    def test_str(self) -> None:
        result = get_logger(logger=__name__)
        assert isinstance(result, Logger)
        assert result.name == __name__

    def test_none(self) -> None:
        result = get_logger()
        assert isinstance(result, Logger)
        assert result.name == "root"


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
    def test_decorated(
        self, *, tmp_path: Path, git_ref: str, traceback_func_one: Pattern[str]
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, git_ref=git_ref, files_dir=tmp_path)
        logger = getLogger(name)
        assert len(logger.handlers) == 7
        self.assert_files(tmp_path, "init")
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        self.assert_files(tmp_path, ("post", traceback_func_one))

    @skipif_windows
    def test_undecorated(
        self, *, tmp_path: Path, git_ref: str, traceback_func_untraced: Pattern[str]
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, git_ref=git_ref, files_dir=tmp_path)
        logger = getLogger(name)
        assert len(logger.handlers) == 7
        self.assert_files(tmp_path, "init")
        try:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        self.assert_files(tmp_path, ("post", traceback_func_untraced))

    @skipif_windows
    def test_regular_percent_formatting(
        self, *, tmp_path: Path, git_ref: str, caplog: LogCaptureFixture
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, git_ref=git_ref, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("int: %d, float: %.2f", 1, 12.3456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35"
        assert record.message == expected

    @skipif_windows
    def test_new_brace_formatting(
        self, *, tmp_path: Path, git_ref: str, caplog: LogCaptureFixture
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, git_ref=git_ref, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("int: {:d}, float: {:.2f}, percent: {:.2%}", 1, 12.3456, 0.123456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35, percent: 12.35%"
        assert record.message == expected

    @skipif_windows
    def test_no_console(self, *, tmp_path: Path, git_ref: str) -> None:
        name = str(tmp_path)
        setup_logging(
            logger=name, console_level=None, git_ref=git_ref, files_dir=tmp_path
        )
        logger = getLogger(name)
        assert len(logger.handlers) == 5

    @skipif_windows
    def test_zoned_datetime(
        self, *, tmp_path: Path, git_ref: str, caplog: LogCaptureFixture
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, git_ref=git_ref, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("")
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        assert isinstance(record._zoned_datetime, ZonedDateTime)
        assert isinstance(record._zoned_datetime_str, str)

    @skipif_windows
    def test_extra(self, *, tmp_path: Path, git_ref: str) -> None:
        name = str(tmp_path)

        def extra(logger: LoggerOrName | None, /) -> None:
            handler = FileHandler(tmp_path.joinpath("extra.log"))
            handler.setLevel(DEBUG)
            get_logger(logger=logger).addHandler(handler)

        setup_logging(logger=name, git_ref=git_ref, files_dir=tmp_path, extra=extra)
        logger = getLogger(name)
        logger.info("")
        files = list(tmp_path.iterdir())
        names = {f.name for f in files if not search(r"\.lock", f.name)}
        assert len(names) == 4

    @classmethod
    def assert_files(
        cls, path: Path, check: Literal["init"] | tuple[Literal["post"], Pattern[str]]
    ) -> None:
        files = list(path.iterdir())
        names = {f.name for f in files if not search(r"\.lock", f.name)}
        expected = {"debug.txt", "info.txt", "plain"}
        assert names.issuperset(expected)
        match check:
            case "init":
                pass
            case ("post", pattern):
                if "errors" in names:
                    assert names == (expected | {"errors"})
                    errors = path.joinpath("errors")
                    assert errors.is_dir()
                    files = list(errors.iterdir())
                    assert len(files) == 1
                    with one(files).open() as fh:
                        contents = fh.read()
                    assert pattern.search(contents)


class TestStandaloneFileHandler:
    def test_main(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        handler = StandaloneFileHandler(level=DEBUG, path=tmp_path)
        logger.addHandler(handler)
        logger.setLevel(DEBUG)
        assert len(list(tmp_path.iterdir())) == 0
        logger.info("message")
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        with one(files).open() as fh:
            contents = fh.read()
        assert contents == "message"


class TestTempHandler:
    def test_main(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(h1 := StreamHandler())
        logger.addHandler(h2 := StreamHandler())
        assert len(logger.handlers) == 2
        handler = StreamHandler()
        with temp_handler(handler, logger=logger):
            assert len(logger.handlers) == 3
        assert len(logger.handlers) == 2
        assert logger.handlers[0] is h1
        assert logger.handlers[1] is h2


class TestTempLogger:
    def test_disabled(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        assert not logger.disabled
        with temp_logger(logger, disabled=True):
            assert logger.disabled
        assert not logger.disabled

    def test_level(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        assert logger.level == NOTSET
        with temp_logger(logger, level="DEBUG"):
            assert logger.level == DEBUG
        assert logger.level == NOTSET

    def test_propagate(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        assert logger.propagate
        with temp_logger(logger, propagate=False):
            assert not logger.propagate
        assert logger.propagate
