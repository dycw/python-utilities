from __future__ import annotations

from io import StringIO
from logging import DEBUG, NOTSET, FileHandler, Logger, StreamHandler, getLogger
from pathlib import Path
from re import search
from time import sleep
from typing import TYPE_CHECKING, Any, Literal, cast

from hypothesis import given
from hypothesis.strategies import booleans, integers, none, sampled_from
from pytest import LogCaptureFixture, mark, param, raises
from whenever import ZonedDateTime

from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.untraced import func_untraced
from utilities.datetime import NOW_UTC, SECOND, serialize_compact
from utilities.hypothesis import (
    assume_does_not_raise,
    pairs,
    temp_paths,
    text_ascii,
    zoned_datetimes,
)
from utilities.iterables import one
from utilities.logging import (
    FilterForKeyError,
    GetLoggingLevelNumberError,
    SizeAndTimeRotatingFileHandler,
    StandaloneFileHandler,
    _AdvancedLogRecord,
    _compute_rollover_actions,
    _RotatingLogFile,
    add_filters,
    basic_config,
    filter_for_key,
    get_default_logging_path,
    get_logger,
    get_logging_level_number,
    setup_logging,
    temp_handler,
    temp_logger,
)
from utilities.pytest import skipif_windows
from utilities.types import LogLevel
from utilities.typing import get_args

if TYPE_CHECKING:
    import datetime as dt
    from re import Pattern

    from utilities.types import LoggerOrName


class TestAddFilters:
    @given(root=temp_paths(), expected=booleans())
    def test_main(self, *, root: Path, expected: bool) -> None:
        logger = getLogger(str(root))
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        add_filters(handler, lambda _: expected)
        assert len(handler.filters) == 1
        logger.warning("message")
        result = buffer.getvalue() != ""
        assert result is expected

    def test_no_handlers(self) -> None:
        handler = StreamHandler()
        assert len(handler.filters) == 0
        add_filters(handler)
        assert len(handler.filters) == 0


class TestBasicConfig:
    def test_main(self, *, tmp_path: Path) -> None:
        basic_config()
        logger = getLogger(str(tmp_path))
        logger.info("message")


class TestComputeRolloverActions:
    @skipif_windows
    def test_main(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("log.txt").touch()

        actions = _compute_rollover_actions(tmp_path, "log", ".txt")
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.1\__[\dT]+\.txt$", p.name))

        for _ in range(2):
            sleep(1)
            tmp_path.joinpath("log.txt").touch()
            actions = _compute_rollover_actions(tmp_path, "log", ".txt")
            assert len(actions.deletions) == 1
            assert len(actions.rotations) == 1
            actions.do()
            files = list(tmp_path.iterdir())
            assert len(files) == 1
            assert any(
                p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
            )

    @skipif_windows
    def test_multiple_backups(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("log.txt").touch()

        actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.1\__[\dT]+\.txt$", p.name))

        sleep(1)
        tmp_path.joinpath("log.txt").touch()
        actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 2
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 2
        assert any(
            p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
        )
        assert any(p for p in files if search(r"^log\.2\__[\dT]+\.txt$", p.name))

        sleep(1)
        tmp_path.joinpath("log.txt").touch()
        actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 3
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 3
        assert any(
            p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
        )
        assert any(
            p for p in files if search(r"^log\.2\__[\dT]+__[\dT]+\.txt$", p.name)
        )
        assert all(p for p in files if search(r"^log\.3\__[\dT]+\.txt$", p.name))

        for _ in range(2):
            sleep(1)
            tmp_path.joinpath("log.txt").touch()
            actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
            assert len(actions.deletions) == 1
            assert len(actions.rotations) == 3
            actions.do()
            files = list(tmp_path.iterdir())
            assert len(files) == 3
            assert any(
                p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(
                p for p in files if search(r"^log\.2\__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert all(
                p for p in files if search(r"^log\.3\__[\dT]+__[\dT]+\.txt$", p.name)
            )

    @skipif_windows
    def test_deleting_old_files(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("log.txt").touch()

        actions = _compute_rollover_actions(tmp_path, "log", ".txt")
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.1\__[\dT]+\.txt$", p.name))

        sleep(1)
        tmp_path.joinpath("log.txt").touch()
        now = serialize_compact(NOW_UTC)
        tmp_path.joinpath(f"log.99__{now}__{now}.txt").touch()
        actions = _compute_rollover_actions(tmp_path, "log", ".txt")
        assert len(actions.deletions) == 2
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(
            p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
        )


class TestFilterForKey:
    @given(
        root=temp_paths(),
        key=text_ascii(),
        value=booleans() | none(),
        default=booleans(),
    )
    def test_main(
        self, *, root: Path, key: str, value: bool | None, default: bool
    ) -> None:
        logger = getLogger(str(root))
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        with assume_does_not_raise(FilterForKeyError):
            filter_ = filter_for_key(key, default=default)
        add_filters(handler, filter_)
        match value:
            case bool():
                logger.warning("message", extra={key: value})
                expected = value
            case None:
                logger.warning("message")
                expected = default
        result = buffer.getvalue() != ""
        assert result is expected

    def test_sunder(self) -> None:
        _ = filter_for_key("_key")

    @given(key=sampled_from(["msg", "__dunder__"]))
    def test_error(self, *, key: str) -> None:
        with raises(FilterForKeyError, match="Invalid key: '.*'"):
            _ = filter_for_key(key)


class TestGetDefaultLoggingPath:
    def test_main(self) -> None:
        assert isinstance(get_default_logging_path(), Path)


class TestGetLogger:
    def test_logger(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        result = get_logger(logger=logger)
        assert result is logger

    def test_str(self, *, tmp_path: Path) -> None:
        result = get_logger(logger=str(tmp_path))
        assert isinstance(result, Logger)
        assert result.name == str(tmp_path)

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
            _ = get_logging_level_number(cast("Any", "invalid"))


class TestLogLevel:
    def test_main(self) -> None:
        assert len(get_args(LogLevel)) == 5


class TestRotatingLogFile:
    def test_from_path(self) -> None:
        path = Path("log.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index is None
        assert result.start is None
        assert result.end is None

    @given(index=integers(min_value=1))
    def test_from_path_with_index(self, *, index: int) -> None:
        path = Path(f"log.{index}.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index == index
        assert result.start is None
        assert result.end is None

    @given(
        index=integers(min_value=1),
        end=zoned_datetimes(round_="standard", timedelta=SECOND),
    )
    def test_from_path_with_index_and_end(
        self, *, index: int, end: dt.datetime
    ) -> None:
        path = Path(f"log.{index}__{serialize_compact(end)}.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index == index
        assert result.start is None
        assert result.end == end

    @given(
        index=integers(min_value=1),
        datetimes=pairs(
            zoned_datetimes(round_="standard", timedelta=SECOND), sorted=True
        ),
    )
    def test_from_path_with_index_start_and_end(
        self, *, index: int, datetimes: tuple[dt.datetime, dt.datetime]
    ) -> None:
        start, end = datetimes
        path = Path(
            f"log.{index}__{serialize_compact(start)}__{serialize_compact(end)}.txt"
        )
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index == index
        assert result.start == start
        assert result.end == end

    def test_from_path_none(self) -> None:
        path = Path("invalid.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is None

    def test_path(self, *, tmp_path: Path) -> None:
        file = _RotatingLogFile(directory=tmp_path, stem="log", suffix=".txt")
        assert file.path == tmp_path.joinpath("log.txt")

    @given(index=integers(min_value=1), root=temp_paths())
    def test_path_with_index(self, *, index: int, root: Path) -> None:
        file = _RotatingLogFile(directory=root, stem="log", suffix=".txt", index=index)
        assert file.path == root.joinpath(f"log.{index}.txt")

    @given(
        root=temp_paths(),
        index=integers(min_value=1),
        end=zoned_datetimes(round_="standard", timedelta=SECOND),
    )
    def test_path_with_index_and_end(
        self, *, root: Path, index: int, end: dt.datetime
    ) -> None:
        file = _RotatingLogFile(
            directory=root, stem="log", suffix=".txt", index=index, end=end
        )
        assert file.path == root.joinpath(f"log.{index}__{serialize_compact(end)}.txt")

    @given(
        root=temp_paths(),
        index=integers(min_value=1),
        datetimes=pairs(
            zoned_datetimes(round_="standard", timedelta=SECOND), sorted=True
        ),
    )
    def test_path_with_index_start_and_end(
        self, *, root: Path, index: int, datetimes: tuple[dt.datetime, dt.datetime]
    ) -> None:
        start, end = datetimes
        file = _RotatingLogFile(
            directory=root, stem="log", suffix=".txt", index=index, start=start, end=end
        )
        assert file.path == root.joinpath(
            f"log.{index}__{serialize_compact(start)}__{serialize_compact(end)}.txt"
        )


class TestSetupLogging:
    @skipif_windows
    def test_decorated(
        self, *, tmp_path: Path, traceback_func_one: Pattern[str]
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
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
        self, *, tmp_path: Path, traceback_func_untraced: Pattern[str]
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
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
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("int: %d, float: %.2f", 1, 12.3456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35"
        assert record.message == expected

    @skipif_windows
    def test_new_brace_formatting(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("int: {:d}, float: {:.2f}, percent: {:.2%}", 1, 12.3456, 0.123456)
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        expected = "int: 1, float: 12.35, percent: 12.35%"
        assert record.message == expected

    @skipif_windows
    def test_no_console(self, *, tmp_path: Path) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, console_level=None, files_dir=tmp_path)
        logger = getLogger(name)
        assert len(logger.handlers) == 5

    @skipif_windows
    def test_zoned_datetime(self, *, tmp_path: Path, caplog: LogCaptureFixture) -> None:
        name = str(tmp_path)
        setup_logging(logger=name, files_dir=tmp_path)
        logger = getLogger(name)
        logger.info("")
        record = one(caplog.records)
        assert isinstance(record, _AdvancedLogRecord)
        assert isinstance(record._zoned_datetime, ZonedDateTime)
        assert isinstance(record._zoned_datetime_str, str)

    @skipif_windows
    def test_extra(self, *, tmp_path: Path) -> None:
        name = str(tmp_path)

        def extra(logger: LoggerOrName | None, /) -> None:
            get_logger(logger=logger).addHandler(
                FileHandler(tmp_path.joinpath("extra.log"))
            )

        setup_logging(logger=name, files_dir=tmp_path, extra=extra)
        logger = getLogger(name)
        logger.info("")
        files = list(tmp_path.iterdir())
        names = {f.name for f in files}
        assert len(names) == 4

    @classmethod
    def assert_files(
        cls, path: Path, check: Literal["init"] | tuple[Literal["post"], Pattern[str]]
    ) -> None:
        files = list(path.iterdir())
        names = {f.name for f in files}
        match check:
            case "init":
                assert names == {"debug.txt", "info.txt", "plain"}
            case "post", pattern:
                assert names == {"debug.txt", "info.txt", "errors", "plain"}
                errors = path.joinpath("errors")
                assert errors.is_dir()
                files = list(errors.iterdir())
                assert len(files) == 1
                with one(files).open() as fh:
                    contents = fh.read()
                assert pattern.search(contents)


class TestSizeAndTimeRotatingFileHandler:
    @skipif_windows
    def test_handlers(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        filename = tmp_path.joinpath("log")
        logger.addHandler(SizeAndTimeRotatingFileHandler(filename=filename))
        logger.warning("message")
        with filename.open() as fh:
            content = fh.read()
        assert content == "message\n"

    @skipif_windows
    def test_size(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(
            SizeAndTimeRotatingFileHandler(
                filename=tmp_path.joinpath("log.txt"), maxBytes=100, backupCount=3
            )
        )

        for i in range(1, 3):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 1
            assert any(p for p in files if search(r"^log\.txt$", p.name))

        logger.warning(10 * "message 3")
        files = list(tmp_path.iterdir())
        assert len(files) == 2
        assert any(p for p in files if search(r"^log\.txt$", p.name))
        assert any(p for p in files if search(r"^log\.1__[\dT]+\.txt$", p.name))

        for i in range(4, 6):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 3
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(
                p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(p for p in files if search(r"^log\.2__[\dT]+\.txt$", p.name))

        logger.warning(10 * "message 6")
        files = list(tmp_path.iterdir())
        assert len(files) == 4
        assert any(p for p in files if search(r"^log\.txt$", p.name))
        assert any(p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name))
        assert any(p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name))
        assert any(p for p in files if search(r"^log\.3__[\dT]+\.txt$", p.name))

        for _ in range(2):
            for i in range(7, 9):
                logger.warning("message %d", i)
                files = list(tmp_path.iterdir())
                assert len(files) == 4
                assert any(p for p in files if search(r"^log\.txt$", p.name))
                assert any(
                    p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
                )
                assert any(
                    p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
                )
                assert any(
                    p for p in files if search(r"^log\.3__[\dT]+__[\dT]+\.txt$", p.name)
                )

            logger.warning("message 9")
            files = list(tmp_path.iterdir())
            assert len(files) == 4
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(
                p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(
                p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(
                p for p in files if search(r"^log\.3__[\dT]+__[\dT]+\.txt$", p.name)
            )

    @mark.flaky
    @skipif_windows
    def test_time(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(
            SizeAndTimeRotatingFileHandler(
                filename=tmp_path.joinpath("log.txt"),
                backupCount=3,
                when="S",
                interval=1,
            )
        )

        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.txt$", p.name))

        logger.warning("message 1")
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.txt$", p.name))

        sleep(1.01)
        for i in range(2, 4):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 2
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(p for p in files if search(r"^log\.1__[\dT]+\.txt$", p.name))

        sleep(1.01)
        for i in range(4, 6):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 3
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(
                p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(p for p in files if search(r"^log\.2__[\dT]+\.txt$", p.name))

        sleep(1.01)
        for i in range(6, 8):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 4
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(
                p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(
                p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(p for p in files if search(r"^log\.3__[\dT]+\.txt$", p.name))

        for _ in range(2):
            sleep(1.01)
            for i in range(8, 10):
                logger.warning("message %d", i)
                files = list(tmp_path.iterdir())
                assert len(files) == 4
                assert any(p for p in files if search(r"^log\.txt$", p.name))
                assert any(
                    p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
                )
                assert any(
                    p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
                )
                assert any(
                    p for p in files if search(r"^log\.3__[\dT]+__[\dT]+\.txt$", p.name)
                )

    @skipif_windows
    def test_should_rollover_file_not_found(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        logger = getLogger(str(tmp_path))
        path = tmp_path.joinpath("log")
        logger.addHandler(
            handler := SizeAndTimeRotatingFileHandler(filename=path, maxBytes=1)
        )
        logger.warning("message")
        record = one(caplog.records)
        path.unlink()
        assert not handler._should_rollover(record)


class TestStandaloneFileHandler:
    @skipif_windows
    def test_main(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(StandaloneFileHandler(level=DEBUG, path=tmp_path))
        assert len(list(tmp_path.iterdir())) == 0
        logger.warning("message")
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
